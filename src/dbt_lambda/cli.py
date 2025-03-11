import json
import logging
import os
import re
from enum import Enum
from tempfile import TemporaryDirectory
from typing import Annotated

import boto3
import botocore.config
import typer
from click import Choice
from typer import Option

from dbt_lambda.app import lambda_handler

logger = logging.getLogger()
logger.setLevel(logging.INFO)
# logger.addHandler(logging.StreamHandler())

cli = typer.Typer(
    add_completion=False,
    pretty_exceptions_enable=False,
    no_args_is_help=True
)


class Env(str, Enum):
    """
    Environment.
    """
    dev = 'dev'
    prod = 'prod'

    def set(self):
        os.environ['APP_ENV'] = self.value

env_ann = Annotated[
    Env, Option(
        '--env', '-e',
        help="target environment"
    )
]


@cli.command()
def cli_execute(
        args: list[str] = typer.Argument(None, help="dbt arguments"),
        source: Annotated[str, Option(
            help="source of dbt project",
            click_type=Choice(['s3', 'repo'])
        )] = 'repo',
        env: env_ann = Env.dev,
        remote: Annotated[bool, Option(help="local or remote execution")] = False,
        test: Annotated[bool, Option(help="run quick test")] = False,
):
    env.set()
    args = args or []
    if len(args) == 1 and ' ' in args[0]:
        args = [a.strip("'") for a in re.findall(r"(?:[^\s']+|'[^']*')+", args[0])]
    if test:
        args.extend(['--target', env.value, '--vars', 'materialized: view'])
    event = {
        'args': args,
        'source': source,
    }
    if remote:
        transform_function_name = f'TransformFunction-{env.value}'
        cfg = botocore.config.Config(
            read_timeout=900,
            connect_timeout=600,
            region_name="eu-central-1",
            retries={'max_attempts': 0},
        )
        payload = json.dumps(event)
        print(payload)
        response = boto3.client('lambda', config=cfg).invoke(
            FunctionName=transform_function_name,
            Payload=payload.encode('utf-8'),
        )
        result = json.loads(response['Payload'].read())
    else:
        if 'SAM_CONFIG_FILE' not in os.environ:
            os.environ['SAM_CONFIG_FILE'] = 'src/samconfig.yaml'
        with TemporaryDirectory() as tmp_dir:
            event['base_path'] = f'{tmp_dir}/dbt-project'
            print(event)
            result = lambda_handler(event, None)

    if 'message' in result:
        print(result['message'])
    else:
        print(result)


if __name__ == '__main__':
    cli()
