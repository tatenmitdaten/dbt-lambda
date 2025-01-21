import logging
import os
from pathlib import Path
from tempfile import TemporaryDirectory

import boto3
import dbt_lambda.app as app
import dbt_lambda.main as main
import dbt_lambda.docs as docs
import pytest

from dbt_lambda import git
from dbt_lambda.app import notify_hook
from dbt_lambda.config import get_parameters
from dbt_lambda.config import set_env_vars
from dbt_lambda.main import run_single_threaded
from moto import mock_aws

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


@pytest.fixture()
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-central-1"


@pytest.fixture()
def mocked_aws(aws_credentials):
    with mock_aws():
        yield


@pytest.fixture()
def dbt_docs_bucket(mocked_aws):
    s3 = boto3.client('s3', region_name='eu-central-1')
    bucket_name = 'test-docs-bucket'
    os.environ['DBT_DOCS_BUCKET'] = bucket_name
    s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'eu-central-1'})
    return bucket_name


@pytest.fixture()
def dbt_docs(dbt_docs_bucket, snowflake_credentials):
    base_path = Path(__file__).parent / 'dbt-project'
    catalog_path = base_path / 'target' / 'catalog.json'
    catalog_path.unlink(missing_ok=True)
    run_single_threaded(args=['docs', 'generate'], source='local', base_path=base_path)


@pytest.fixture
def base_path():
    return Path(__file__).parent / 'dbt-project'


@pytest.fixture
def snowflake_credentials(monkeypatch):
    monkeypatch.setattr(main, 'set_snowflake_credentials_to_env', lambda: None)


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


def test_run(dbt_result, base_path, snowflake_credentials):
    res = run_single_threaded(args=['build'], source='local', base_path=base_path)
    for node in res.nodes:
        node.execution_time = 0
    assert res.as_dict == dbt_result


def test_successful_app_run(dbt_result, base_path, snowflake_credentials, env_vars):
    event = {
        'args': ['build', '--select', 'test_model', 'warning_test'],
        'source': 'local',
        'base_path': base_path
    }
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
        'success': True
    }


def test_failed_app_run(base_path, snowflake_credentials, env_vars):
    event = {
        'args': ['build'],
        'source': 'local',
        'base_path': base_path
    }
    result = app.lambda_handler(event, None)
    nodes = result['nodes']
    message = result['message']

    # remove execution times
    message_no_time = '\n'.join(m[:-4] for m in message.split('\n'))
    assert message_no_time == """\
memory.main.test_model......................................success in 0
test.test.failing_test......................................fail[1] in 0
test.test.warning_test......................................warn[1] in 0"""

    # remove execution times
    for node in nodes:
        node['execution_time'] = 0
    assert nodes == [{
        'node_info': {
            'node_path': 'testrun/test_model.sql', 'node_name': 'test_model', 'unique_id': 'model.test.test_model',
            'materialized': 'view', 'node_relation': {'database': 'memory', 'schema': 'main', 'alias': 'test_model'}
        }, 'status': 'success', 'execution_time': 0, 'failures': None
    }, {
        'node_info': {
            'node_path': 'failing_test.sql', 'node_name': 'failing_test', 'unique_id': 'test.test.failing_test',
            'materialized': 'test',
            'node_relation': {'database': 'memory', 'schema': 'main_dbt_test__audit', 'alias': 'failing_test'}
        }, 'status': 'fail', 'execution_time': 0, 'failures': 1
    }, {
        'node_info': {
            'node_path': 'warning_test.sql', 'node_name': 'warning_test', 'unique_id': 'test.test.warning_test',
            'materialized': 'test',
            'node_relation': {'database': 'memory', 'schema': 'main_dbt_test__audit', 'alias': 'warning_test'}
        }, 'status': 'warn', 'execution_time': 0, 'failures': 1
    }]


def test_app_error(base_path, snowflake_credentials, env_vars):
    event = {
        'args': ['error'],
        'source': 'local',
        'base_path': base_path
    }
    with pytest.raises(RuntimeError) as e:
        app.lambda_handler(event, None)
    message = e.value.__str__()
    assert message == "Failed to run error: No such command 'error'."


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
    assert response['body'].startswith('<!doctype html>')


def test_notify_hook(base_path, snowflake_credentials, env_vars):
    event = {
        'args': ['build'],
        'source': 'local',
        'base_path': base_path
    }
    response = app.lambda_handler(event, None)
    message = notify_hook(response)

    # remove execution times
    message_no_time = '\n'.join(m[:-4] for m in message.split('\n'))
    assert message_no_time == """\
test.test.failing_test......................................fail[1] in 0
test.test.warning_test......................................warn[1] in 0"""


def test_copy_to_s3(base_path, parameters, dbt_docs_bucket, monkeypatch):
    bucket = boto3.resource('s3').Bucket(dbt_docs_bucket)
    monkeypatch.setattr(docs, 'get_dbt_docs_bucket', lambda: bucket)
    git.copy_to_s3(base_path)
    with TemporaryDirectory() as tmp_path:
        tmp_path = Path(tmp_path)
        tmp_base_path = tmp_path / 'dbt-project'
        tmp_base_path.mkdir()
        git.copy_from_s3(tmp_base_path)
        files_in_tmp_path = sorted(f.relative_to(tmp_base_path) for f in tmp_base_path.glob('**/*') if f.is_file())

    files_in_base_path = sorted(f.relative_to(base_path) for f in base_path.glob('**/*') if f.is_file())
    assert files_in_base_path == files_in_tmp_path
