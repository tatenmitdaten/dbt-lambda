import json

from dbt_lambda.config import set_env_vars
from dbt_lambda.main import RunnerResult
from dbt_lambda.main import run_single_threaded


def lambda_handler(event, context):
    set_env_vars()
    args = event.get('args', ['build'])
    if args == ['error']:
        raise RuntimeError('Test Error')

    base_path = event.get('base_path', '/tmp/dbt')
    res: RunnerResult = run_single_threaded(args, base_path)

    if not res.success:
        failed_nodes = [node.__str__() for node in res.nodes if node.status != 'success']
        raise RuntimeError(f'dbt failure: {json.dumps(failed_nodes)}')

    return {
        'statusCode': 200,
        'message': res.__str__(),
        **res.as_dict(),
    }
