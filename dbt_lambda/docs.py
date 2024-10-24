import io
import logging
import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from mypy_boto3_s3.service_resource import Bucket

logger = logging.getLogger()
logger.setLevel("INFO")


def get_dbt_docs_bucket() -> Bucket:
    dbt_docs_bucket_name = os.environ.get('DBT_DOCS_BUCKET')
    if dbt_docs_bucket_name is None:
        raise ValueError('DBT_DOCS_BUCKET environment variable is not set')
    return boto3.resource('s3').Bucket(dbt_docs_bucket_name)


def save_index_html(base_path: Path | None = None):
    if base_path is None:
        project_dir = os.environ.get('DBT_PROJECT_DIR')
        if project_dir is None:
            raise ValueError('DBT_PROJECT_DIR environment variable is not set')
        base_path = Path(project_dir)
    target = base_path / 'target'
    with (target / 'index.html').open() as f:
        index = f.read()
    with (target / 'manifest.json').open() as f:
        json_manifest = f.read()
    with (target / 'catalog.json').open() as f:
        json_catalog = f.read()
    reference_catalog = 'n=[o("manifest","manifest.json"+t),o("catalog","catalog.json"+t)]'
    embedded_catalog = f"""n=[
    {{label: 'manifest', data: {json_manifest}}},
    {{label: 'catalog', data: {json_catalog}}}
    ]"""
    index = index.replace(reference_catalog, embedded_catalog)

    body = index.encode('utf-8')
    bucket = get_dbt_docs_bucket()
    key = 'index.html'
    bucket.put_object(Body=body, Key=key)
    logger.info(f'Written {len(body)} bytes to s3://{bucket.name}/{key}')


def load_index_html(key: str = 'index.html') -> str:
    bucket = get_dbt_docs_bucket()
    stream = io.BytesIO()
    try:
        bucket.download_fileobj(key, stream)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            message = f'Object {key} does not exist in s3://{bucket.name}'
            logger.error(message)
            return message
        raise
    body = stream.getvalue()
    logger.info(f'Loaded {len(body)} bytes from s3://{bucket.name}/{key}')
    return body.decode('utf-8')
