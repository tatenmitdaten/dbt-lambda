import os
from functools import lru_cache
from logging import getLogger
from pathlib import Path

import yaml

logger = getLogger()


@lru_cache
def get_parameters(env: str | None = None, file: Path | None = None) -> dict:
    if file is None:
        file_str = os.environ.get('SAM_CONFIG_FILE', './samconfig.yaml')
        logger.info(f'Using sam config file "{file_str}"')
        file = Path(file_str)
    if not file.exists():
        raise FileNotFoundError(f'File "{file}" not found. Cannot read parameters.')
    env = env or os.environ.get('APP_ENV', 'dev')
    with file.open() as file:
        config = yaml.safe_load(file)
    params = config[env]['deploy']['parameters']
    overrides = params['parameter_overrides']
    return {
        'profile': params['profile'],
        **dict(p.split('=', 1) for p in overrides)
    }


def set_env_vars():
    parameters = get_parameters()
    os.environ['SNOWFLAKE_SECRET_ARN'] = parameters['SnowflakeSecretArn']
    logger.info(f'Set SNOWFLAKE_SECRET_ARN={parameters["SnowflakeSecretArn"]} environment variable')
    os.environ['DBT_DOCS_BUCKET'] = parameters['DbtDocsBucketStem'] + '-' + os.environ.get('APP_ENV', 'dev')
    logger.info(f'Set DBT_DOCS_BUCKET={os.environ["DBT_DOCS_BUCKET"]} environment variable')
    os.environ['DBT_REPOSITORY_NAME'] = parameters['RepositoryName']
    logger.info(f'Set DBT_REPOSITORY_NAME={os.environ['DBT_REPOSITORY_NAME']} environment variable')
