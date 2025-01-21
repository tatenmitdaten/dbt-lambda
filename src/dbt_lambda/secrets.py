import json
import os
from functools import lru_cache
from typing import Optional

import boto3  # type: ignore
from logging import getLogger

logger = getLogger()
logger.setLevel('INFO')


@lru_cache
def get_secret(secret_id) -> dict:
    """
    Get secret from AWS Secrets Manager

    Args:
        secret_id: SecretsManager ARN or name

    Returns:
        Secret as a dictionary
    """
    client = boto3.client('secretsmanager')
    secret_str = client.get_secret_value(SecretId=secret_id)['SecretString']
    secret = json.loads(secret_str)
    return secret


def set_snowflake_credentials_to_env(secret_id: Optional[str] = None):
    """
    Set Snowflake credentials to environment variables

    Args:
        secret_id: SecretsManager Snowflake credentials ARN or name
    """
    secret_id = secret_id or os.environ.get('SNOWFLAKE_SECRET_ARN')
    if not secret_id:
        raise ValueError('SNOWFLAKE_SECRET_ARN environment variable is not set')

    secret = get_secret(secret_id)
    for key, value in secret.items():
        env_var = f'SNOWFLAKE_{key.upper()}'
        os.environ[env_var] = value
        if key == 'private_key':
            value = '***'
        logger.info(f'Set {env_var}={value} environment variable')


def set_github_token_to_env(secret_id: str | None = None):
    """
    Set GitHub access token to environment variable

    Args:
        secret_id: SecretsManager GitHub token ARN or name
    """
    secret_id = secret_id or os.environ.get('GITHUB_SECRET_ARN')
    if secret_id is None:
        raise ValueError('GITHUB_SECRET_ARN environment variable is not set')
    if secret_id == '':
        logger.info('GITHUB_SECRET_ARN is empty, skipping GitHub token setting')
    else:
        token = get_secret(secret_id)['token']
        os.environ['GITHUB_ACCESS_TOKEN'] = token
        logger.info('Set GITHUB_ACCESS_TOKEN=*** environment variable')
