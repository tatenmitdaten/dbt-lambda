import json
import logging
import multiprocessing.pool
import os
import queue
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

# we only import dbt.mp_context and mock dbt.mp_context._MP_CONTEXT before any other dbt imports
import dbt.mp_context
from dbt.artifacts.schemas.run import RunExecutionResult
from dbt_common.events import EventLevel
from dbt_lambda.docs import save_index_html
from dbt_lambda.git import copy_from_repo
from dbt_lambda.git import copy_from_s3
from dbt_lambda.git import default_base_path
from dbt_lambda.secrets import set_snowflake_credentials_to_env

logger = logging.getLogger()
logger.setLevel('INFO')
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)


class CustomThreadPool:
    """
    Custom thread pool replacement with an interface as expected by dbt.task.runnable

    We override the multiprocessing ThreadPool with a ThreadPoolExecutor that doesn't use
    any shared memory semaphore locks. This is necessary because AWS Lambda does not
    provide /dev/shm which is required by the default multiprocessing context.
    """

    def __init__(self, num_threads: int, pool_thread_initializer, invocation_context):
        self.pool = ThreadPoolExecutor(max_workers=num_threads)
        self.pool_thread_initializer = pool_thread_initializer
        self.invocation_context = invocation_context

    # provide the same interface expected by dbt.task.runnable
    def apply_async(self, func, args, callback):
        def future_callback(fut):
            return callback(fut.result())

        self.pool.submit(func, *args).add_done_callback(future_callback)

    # we would need to actually keep a "closed" attribute lying around and properly check it
    def close(self):
        pass

    # shutdown(wait=True) mimics "join", whereas shutdown(wait=False) mimics "terminate"
    def join(self):
        self.pool.shutdown(wait=True)


multiprocessing.pool.ThreadPool = CustomThreadPool  # type: ignore


class ThreadedContext:
    """
    We replace the multiprocessing context with threaded context.
    The objects mostly have the same API.
    """
    Process = threading.Thread
    Lock = threading.Lock
    RLock = threading.RLock
    Queue = queue.Queue


dbt.mp_context._MP_CONTEXT = ThreadedContext()  # type: ignore


@dataclass
class NodeResult:
    node_info: dict
    status: str
    execution_time: float
    failures: int | None

    def __post_init__(self):
        for key in (
                'meta',
                'node_status',
                'node_started_at',
                'node_finished_at',
                'resource_type',
        ):
            if key in self.node_info:
                del self.node_info[key]
        if 'node_relation' in self.node_info:
            if 'relation_name' in self.node_info['node_relation']:
                del self.node_info['node_relation']['relation_name']

    @property
    def as_dict(self):
        return self.__dict__

    @property
    def as_str(self) -> str:
        return self.__str__()

    def __str__(self):
        failures = f'[{self.failures}]' if self.failures else ''
        if self.node_info['materialized'] == 'test':
            name = self.node_info['unique_id']
        else:
            name = '{database}.{schema}.{alias}'.format(**self.node_info['node_relation'])
        return f'{name.ljust(60, ".")}{self.status}{failures} in {self.execution_time:0.2f}s'


@dataclass
class RunnerResult:
    success: bool
    nodes: list[NodeResult]

    @property
    def as_dict(self):
        return {
            'success': self.success,
            'nodes': [node.as_dict for node in self.nodes]
        }

    @property
    def as_json(self) -> str:
        return json.dumps(self.as_dict, default=str)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            success=data['success'],
            nodes=[NodeResult(**node) for node in data['nodes']]
        )

    @property
    def as_str(self) -> str:
        return self.__str__()

    def __str__(self):
        return '\n'.join(node.as_str for node in self.nodes)

    def failed(self) -> 'RunnerResult':
        return RunnerResult(
            success=self.success,
            nodes=[node for node in self.nodes if node.status not in ('success', 'pass')]
        )


def run_single_threaded(
        args: list[str],
        source: str = 'repo',
        base_path: Path | str = default_base_path
) -> RunnerResult:
    """
    Run dbt with the given arguments in a single-threaded context.

    Args:
        args: The dbt arguments to run.
        source: The source of the dbt project. Either 'repo' or 's3'.
        base_path: The base path of the dbt project.

    Returns:
        A RunnerResult object with the success flag and a list of NodeResult objects.
    """
    import dbt.graph.thread_pool

    # Ensure that the threaded context and pook are set
    assert isinstance(dbt.mp_context._MP_CONTEXT, ThreadedContext)
    assert issubclass(dbt.graph.thread_pool.ThreadPool, CustomThreadPool)

    # Import dbt modules after the threaded context has been set
    from dbt.cli.main import dbtRunner, dbtRunnerResult

    os.environ['DBT_SEND_ANONYMOUS_USAGE_STATS'] = 'False'
    base_path = Path(base_path).absolute()

    # Copy the project from the source
    if source == 'repo':
        copy_from_repo(base_path)
    elif source == 's3':
        copy_from_s3(base_path)
    else:
        logger.info(f'No source parameter provided. Using the existing project at {base_path}')

    os.environ['DBT_PROJECT_DIR'] = base_path.__str__()
    logger.info(f'Using project dir: {os.environ["DBT_PROJECT_DIR"]}')
    os.environ['DBT_PROFILES_DIR'] = (base_path / 'profiles').__str__()
    logger.info(f'Using profiles dir: {os.environ["DBT_PROFILES_DIR"]}')

    # Set Snowflake credentials to environment variables
    set_snowflake_credentials_to_env()

    def log_event(event):
        info = event.info
        if info.level in (
                EventLevel.INFO,
                EventLevel.WARN,
                EventLevel.ERROR
        ):
            # remove ANSI color codes
            clean_msg = re.sub(r'\x1b\[[0-9;]*m', '', info.msg)
            logger.info(clean_msg)

    res: dbtRunnerResult = dbtRunner(callbacks=[log_event]).invoke(args + ['--log-level', 'none'])

    if res.exception:
        message = res.exception.__str__()
        raise RuntimeError(f'Failed to run {" ".join(args)}: {message}')

    runner_result = RunnerResult(
        success=res.success,
        nodes=[]
    )
    if 'docs' in args:
        save_index_html()
    if isinstance(res.result, RunExecutionResult):
        for node in res.result.results:
            runner_result.nodes.append(
                NodeResult(
                    node_info=node.node.node_info,
                    status=node.status.lower(),
                    execution_time=node.execution_time,
                    failures=node.failures
                )
            )

    return runner_result
