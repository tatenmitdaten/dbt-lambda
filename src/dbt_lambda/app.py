import os
from typing import Any

from dbt_lambda.config import set_env_vars
from dbt_lambda.git import copy_from_repo
from dbt_lambda.git import default_base_path
from dbt_lambda.main import run_single_threaded
from dbt_lambda.main import RunnerResult

payload = dict[str, Any]


class DbtTestError(Exception):
    pass


def notify_hook(response: payload) -> str | None:
    if 'error' in response:
        res = RunnerResult.from_dict(response)
        return res.failed().as_str
    return None


def lambda_handler(event, _) -> payload:
    set_env_vars()
    args = event.get('args', [])
    if len(args) == 0:
        return {
            'statusCode': 200,
            'message': 'No args provided. Nothing to do.'
        }

    # check for special args
    match args[0]:
        case 'x-copy':
            return copy_from_repo()
        case 'x-error':
            os.environ['FAIL_ON_ERROR'] = 'False'
            raise DbtTestError('Error')
        case 'x-fail':
            os.environ['FAIL_ON_ERROR'] = 'True'
            raise DbtTestError('Fail')
        case 'x-test':
            os.environ['FAIL_ON_ERROR'] = 'True'
            os.environ['DBT_REPOSITORY_BRANCH'] = 'test'
            args = ['build']
        case 'x-skip':
            return {
                'message': 'Skip dbt execution',
                'success': True,
                'nodes': []
            }

    res: RunnerResult = run_single_threaded(
        args=args,
        source=event.get('source', 'repo'),
        base_path=event.get('base_path', default_base_path)
    )

    response = {
        'message': res.as_str,
        'success': res.success,
        'nodes': [node.as_dict for node in res.nodes]
    }
    if not res.success:
        response['error'] = 'DbtRuntimeError'

    return response
