import json
import logging
import re
from enum import Enum
from typing import Annotated

import boto3
import botocore.config
import typer
from typer import Option

from dbt_lambda.app import lambda_handler

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

cli = typer.Typer(
    pretty_exceptions_enable=False
)


class Env(str, Enum):
    """
    Environment.
    """
    dev = 'dev'
    prod = 'prod'


class Mode(str, Enum):
    """
    Execution mode.
    """
    remote = 'remote'
    local = 'local'


@cli.command(name='exec')
def cli_exec(
        args: list[str] = typer.Argument(None, help="dbt arguments"),
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
        mode: Annotated[Mode, Option(help="local or remote execution")] = Mode.local,
):
    if len(args) == 1 and ' ' in args[0]:
        args = [a.strip("'") for a in re.findall(r"(?:[^\s']+|'[^']*')+", args[0])]
    print(args)
    event = {'args': args}
    if mode == Mode.local:
        payload = lambda_handler(event, None)
    elif mode == Mode.remote:
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
        payload = json.loads(response['Payload'].read())
    else:
        raise ValueError(f'Unknown mode: {mode}')
    print(json.dumps(payload, indent=2))


if __name__ == '__main__':
    cli()
