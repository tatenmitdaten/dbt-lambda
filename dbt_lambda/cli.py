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
logger.addHandler(logging.StreamHandler())

cli = typer.Typer(
    add_completion=False,
    pretty_exceptions_enable=False
)


class Env(str, Enum):
    """
    Environment.
    """
    dev = 'dev'
    prod = 'prod'


@cli.command(name='exec')
def cli_exec(
        args: list[str] = typer.Argument(None, help="dbt arguments"),
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
        mode: Annotated[str, Option(
            help="local or remote execution",
            click_type=Choice(['remote', 'local'])
        )] = 'local',
):
    if len(args) == 1 and ' ' in args[0]:
        args = [a.strip("'") for a in re.findall(r"(?:[^\s']+|'[^']*')+", args[0])]
    print(args)
    event: dict[str, list[str] | str] = {'args': args}
    if mode == 'local':
        if 'SAM_CONFIG_FILE' not in os.environ:
            os.environ['SAM_CONFIG_FILE'] = 'src/transform/samconfig.yaml'
        with TemporaryDirectory() as tmp_dir:
            event['base_path'] = tmp_dir.__str__()
            res = lambda_handler(event, None)
    elif mode == 'remote':
        transform_function_name = f'TransformFunction-{env.value}'
        cfg = botocore.config.Config(
            retries={'max_attempts': 0},
            read_timeout=900,
            connect_timeout=600,
            region_name="eu-central-1"
        )

        response = boto3.client('lambda', config=cfg).invoke(
            FunctionName=transform_function_name,
            Payload=json.dumps(event).encode('utf-8'),
        )
        res = json.loads(response['Payload'].read())
    else:
        raise ValueError(f'Unknown mode: {mode}')
    print(res['message'])


if __name__ == '__main__':
    cli()
