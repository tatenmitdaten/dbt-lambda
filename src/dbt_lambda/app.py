import json
import os

from dbt_lambda.config import set_env_vars
from dbt_lambda.main import run_single_threaded
from dbt_lambda.main import RunnerResult


class DbtTestError(Exception):
    pass


class DbtRuntimeError(Exception):
    pass


def lambda_handler(event, context):
    set_env_vars()
    args = event.get('args', ['build'])
    if len(args) == 0:
        return {
            'statusCode': 200,
            'message': 'No args provided. Nothing to do.'
        }

    # check for special args
    match args:
        case ['x-error']:
            os.environ['FAIL_ON_ERROR'] = 'False'
            raise DbtTestError('Error')
        case ['x-fail']:
            os.environ['FAIL_ON_ERROR'] = 'True'
            raise DbtTestError('Fail')
        case ['x-test']:
            os.environ['FAIL_ON_ERROR'] = 'True'
            os.environ['DBT_REPOSITORY_BRANCH'] = 'test'
            args = ['build']

    base_path = event.get('base_path', '/tmp/dbt')
    res: RunnerResult = run_single_threaded(args, base_path)

    if not res.success:
        raise DbtRuntimeError(
            json.dumps({
                'name': 'DbtRuntimeError',
                'text': res.failed().as_str
            })
        )

    return {
        'statusCode': 200,
        'message': res.as_str,
        **res.as_dict
    }
