import json
import logging
import os
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

import dbt_lambda.app as app
import dbt_lambda.docs as docs
from dbt_lambda.config import get_parameters
from dbt_lambda.config import set_env_vars
from dbt_lambda.main import run_single_threaded

logger = logging.getLogger()
logger.setLevel(logging.WARN)
logger.addHandler(logging.StreamHandler())


@pytest.fixture(scope='session')
def parameters():
    if 'SAM_CONFIG_FILE' not in os.environ:
        os.environ['SAM_CONFIG_FILE'] = '../example/transform/samconfig.yaml'
    yield get_parameters('dev')


@pytest.fixture(scope='session')
def env_vars(parameters):
    set_env_vars()


@pytest.fixture(scope='session')
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-central-1"


@pytest.fixture(scope='session')
def mocked_aws(aws_credentials):
    with mock_aws():
        yield


@pytest.fixture(scope='session')
def dbt_docs_bucket(mocked_aws):
    s3 = boto3.client('s3', region_name='eu-central-1')
    bucket_name = 'test-docs-bucket'
    os.environ['DBT_DOCS_BUCKET'] = bucket_name
    s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'eu-central-1'})
    return bucket_name


@pytest.fixture()
def dbt_docs(dbt_docs_bucket):
    base_path = Path(__file__).parent / 'dbt-project'
    catalog_path = base_path / 'target' / 'catalog.json'
    catalog_path.unlink(missing_ok=True)
    run_single_threaded(['docs', 'generate'], base_path)


@pytest.fixture
def base_path():
    return Path(__file__).parent / 'dbt-project'


@pytest.fixture
def dbt_result():
    return {
        'nodes': [
            {
                'execution_time': 0,
                'failures': None,
                'node_info': {
                    'materialized': 'view',
                    'node_name': 'test_model',
                    'node_path': 'testrun/test_model.sql',
                    'node_relation': {
                        'alias': 'test_model',
                        'database': 'memory',
                        'schema': 'main'
                    },
                    'unique_id': 'model.test.test_model'
                },
                'status': 'success'
            },
            {
                'execution_time': 0,
                'failures': 1,
                'node_info': {
                    'materialized': 'test',
                    'node_name': 'failing_test',
                    'node_path': 'failing_test.sql',
                    'node_relation': {
                        'alias': 'failing_test',
                        'database': 'memory',
                        'schema': 'main_dbt_test__audit'
                    },
                    'unique_id': 'test.test.failing_test'
                },
                'status': 'fail'
            },
            {
                'execution_time': 0,
                'failures': 1,
                'node_info': {
                    'materialized': 'test',
                    'node_name': 'warning_test',
                    'node_path': 'warning_test.sql',
                    'node_relation': {
                        'alias': 'warning_test',
                        'database': 'memory',
                        'schema': 'main_dbt_test__audit'
                    },
                    'unique_id': 'test.test.warning_test'
                },
                'status': 'warn'
            },
        ],
        'success': False
    }


def test_run(dbt_result, base_path):
    res = run_single_threaded(['build'], base_path)
    for node in res.nodes:
        node.execution_time = 0
    assert res.as_dict() == dbt_result


def test_successful_app_run(dbt_result, base_path, env_vars):
    event = {'args': ['build', '--select', 'test_model', 'warning_test'], 'base_path': base_path}
    res = app.lambda_handler(event, None)

    # remove execution times
    for node in res['nodes']:
        node['execution_time'] = 0
    res['message'] = '\n'.join(m[:-2] for m in res['message'].split('\n'))

    del dbt_result['nodes'][1]
    assert res == {
        'message': 'memory.main.test_model......................................success in 0.0\n'
                   'test.test.warning_test......................................warn[1] in 0.0',
        'nodes': dbt_result['nodes'],
        'success': True,
        'statusCode': 200
    }


def test_failed_app_run(base_path, env_vars):
    event = {'args': ['build'], 'base_path': base_path}
    with pytest.raises(RuntimeError) as e:
        app.lambda_handler(event, None)
    text = 'dbt failure: '
    message = e.value.__str__()
    assert message.startswith(text)
    nodes = json.loads(message[len(text):])

    # remove execution times
    for i, node in enumerate(nodes):
        nodes[i] = node[:-2]
    assert nodes == [
        'test.test.failing_test......................................fail[1] in 0.0',
        'test.test.warning_test......................................warn[1] in 0.0'
    ]


def test_app_error(base_path, env_vars):
    event = {'args': ['error'], 'base_path': base_path}
    with pytest.raises(RuntimeError) as e:
        app.lambda_handler(event, None)
    message = e.value.__str__()
    assert message == 'Test Error'


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


def test_docs_fails(parameters):
    event = {
        'queryStringParameters': {}
    }
    assert docs.lambda_handler(event, None) == {
        'statusCode': 401,
        'body': 'Unauthorized'
    }


def test_docs_success(parameters, dbt_docs_bucket, dbt_docs, monkeypatch):
    event = {
        'queryStringParameters': {
            'token': parameters['DbtDocsAccessToken']
        }
    }
    monkeypatch.setattr(docs, 'get_dbt_docs_bucket', lambda: boto3.resource('s3').Bucket(dbt_docs_bucket))
    response = docs.lambda_handler(event, None)
    assert response['statusCode'] == 200
    assert response['body'].startswith('<!doctype html><html')
