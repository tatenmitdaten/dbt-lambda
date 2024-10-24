import logging
import os

import boto3
import pytest
from moto import mock_aws

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


@pytest.fixture
def aws_credentials(scope='function'):
    """Mocked AWS Credentials for moto."""
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


@pytest.fixture
def aws_sandbox():
    os.environ['AWS_PROFILE'] = 'sandbox'
    yield
    del os.environ['AWS_PROFILE']
