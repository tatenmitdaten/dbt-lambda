import os
from functools import lru_cache
from logging import getLogger
from pathlib import Path

import yaml

logger = getLogger()


@lru_cache
def get_parameters(env: str | None = None, file: Path | None = None) -> dict:
    if file is None:
        if 'SAM_CONFIG_FILE' in os.environ:
            file = Path(os.environ['SAM_CONFIG_FILE'])
        else:
            file = Path('/var/task/transform/samconfig.yaml')
        logger.info(f'Using sam config file "{file.absolute()}"')
    if not file.exists():
        raise FileNotFoundError(f'File "{file.absolute()}" not found. Cannot read parameters.')
    env = env or os.environ.get('APP_ENV', 'dev')
    with file.open() as buffer:
        config = yaml.safe_load(buffer)
    params = config[env]['deploy']['parameters']
    overrides = params['parameter_overrides']
    return {
        'profile': params['profile'],
        **dict(p.split('=', 1) for p in overrides)
    }


def set_env_vars():
    parameters = get_parameters()
    environ_map = {
        'GITHUB_SECRET_ARN': 'GithubSecretArn',
        'CODECOMMIT_ROLE_ARN': 'CodeCommitRoleArn',
        'SNOWFLAKE_SECRET_ARN': 'SnowflakeSecretArn',
        'DBT_DOCS_BUCKET_STEM': 'DbtDocsBucketStem',
        'DBT_REPOSITORY_NAME': 'DbtRepositoryName'
    }
    for env_var, param_key in environ_map.items():
        os.environ[env_var] = parameters.get(param_key, '')
        logger.info(f'Set {env_var}={os.environ[env_var]} environment variable')
    os.environ['DBT_DOCS_BUCKET'] = os.environ['DBT_DOCS_BUCKET_STEM'] + '-' + os.environ.get('APP_ENV', 'dev')
    logger.info(f'Set DBT_DOCS_BUCKET={os.environ["DBT_DOCS_BUCKET"]} environment variable')
