import json
import logging
import os
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from dbt_lambda.config import get_parameters
from dbt_lambda.config import set_env_vars
from dbt_lambda.docs import load_index_html
from dbt_lambda.main import run_single_threaded

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


@pytest.fixture(scope='session')
def parameters():
    os.environ['SAM_CONFIG_FILE'] = '../samconfig.yaml'
    yield get_parameters('dev')


@pytest.fixture(scope='session')
def env_vars(parameters):
    set_env_vars()


@pytest.fixture(scope='function')
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-central-1"


@pytest.fixture(scope='function')
def mocked_aws(aws_credentials):
    with mock_aws():
        yield


@pytest.fixture(scope='function')
def dbt_docs_bucket(mocked_aws):
    s3 = boto3.client('s3', region_name='eu-central-1')
    bucket_name = 'test-docs-bucket'
    os.environ['DBT_DOCS_BUCKET'] = bucket_name
    s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'eu-central-1'})


def test_run():
    base_path = Path(__file__).parent / 'dbt-project'
    try:
        run_single_threaded(['build'], base_path)
    except RuntimeError as e:
        text = 'dbt failure: '
        message = e.__str__()
        assert message.startswith(text)
        nodes = json.loads(message[len(text):])
        nodes = [node[:-2] for node in nodes]
        assert nodes == [
            'test.test.failing_test......................................fail[1]  in 0.0',
            'test.test.warning_test......................................warn[1]  in 0.0'
        ]


def test_get_parameters(parameters):
    assert parameters['Environment'] == 'dev'
    assert 'profile' in parameters
    assert 'DbtRepositoryName' in parameters
    assert 'SnowflakeSecretArn' in parameters
    assert 'KMSKeyArn' in parameters
    assert 'DbtDocsBucketStem' in parameters
    assert 'DbtDocsAccessToken' in parameters


def test_set_env_vars(env_vars):
    assert 'SNOWFLAKE_SECRET_ARN' in os.environ
    assert 'GITHUB_SECRET_ARN' in os.environ
    assert 'CODECOMMIT_ROLE_ARN' in os.environ
    assert 'DBT_DOCS_BUCKET' in os.environ
    assert 'DBT_REPOSITORY_NAME' in os.environ


def test_docs(dbt_docs_bucket):
    base_path = Path(__file__).parent / 'dbt-project'
    catalog_path = base_path / 'target' / 'catalog.json'
    if catalog_path.exists():
        os.remove(catalog_path)
    run_single_threaded(['docs', 'generate'], base_path)
    load_index_html()
