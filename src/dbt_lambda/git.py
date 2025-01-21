import io
import os
import shutil
import zipfile
from concurrent.futures import ThreadPoolExecutor
from logging import getLogger
from pathlib import Path
from tempfile import TemporaryDirectory

import boto3
import requests

from dbt_lambda.docs import get_dbt_docs_bucket
from dbt_lambda.secrets import set_github_token_to_env

logger = getLogger()
logger.setLevel('INFO')

default_base_path = Path('/tmp/dbt-project')


def copy_from_repo(
        base_path: Path = default_base_path,
        repository_name: str | None = None,
        ref: str | None = None,
        upload_to_s3: bool = True,
) -> dict:
    set_github_token_to_env()

    ref = ref or os.environ.get('DBT_REPOSITORY_BRANCH', 'master')
    repository_name = repository_name or os.environ.get('DBT_REPOSITORY_NAME')
    if repository_name is None:
        raise ValueError('DBT_REPOSITORY_NAME environment variable is not set')

    shutil.rmtree(base_path, ignore_errors=True)
    base_path.mkdir()

    if os.environ.get('GITHUB_ACCESS_TOKEN'):
        copy_folder_github(base_path, repository_name=repository_name, ref=ref)
    else:
        role_arn = os.environ.get('CODECOMMIT_ROLE_ARN')
        models_path = base_path / 'models'
        models_path.mkdir()
        copy_folder_codecommit(base_path, repository_name=repository_name, ref=ref, role_arn=role_arn)

    if upload_to_s3:
        copy_to_s3(base_path)

    message = f'Copied project from "{repository_name}" at {ref}'
    logger.info(message)
    return {'message': message}


def copy_folder_github(
        base_path: Path,
        repository_name: str,
        ref: str,
        owner: str = 'tatenmitdaten',
):
    token = os.environ.get('GITHUB_ACCESS_TOKEN')
    if token is None:
        raise ValueError('GITHUB_ACCESS_TOKEN environment variable is not set')
    headers = {
        'Authorization': f'Bearer {token}',
        "Accept": "application/vnd.github.v3+json",
        'X-GitHub-Api-Version': '2022-11-28',
    }
    zip_url = f'https://api.github.com/repos/{owner}/{repository_name}/zipball/{ref}'

    response = requests.get(zip_url, headers=headers, stream=True)

    if response.status_code == 200:
        # Create a ZipFile object from the response content
        z = zipfile.ZipFile(io.BytesIO(response.content))

        # Extract all the contents into the specified directory
        with TemporaryDirectory() as tmp_dir:
            z.extractall(tmp_dir)
            Path(tmp_dir).glob('*').__next__().rename(base_path)

        logger.info(f"Successfully extracted repository contents to {base_path}")
    else:
        logger.error(f"Failed to download repository. Status code: {response.status_code}")
        logger.error(response.text)


def copy_folder_codecommit(
        base_path: Path,
        repository_name: str,
        ref: str,
        role_arn: str | None = None,
):
    ignore = {'.gitignore', 'Makefile', 'make-venv.bat', '.DS_Store', 'README.md', 'docs.py', 'requirements.txt'}

    if role_arn:
        sts_client = boto3.client('sts')
        assumed_role = sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName='AssumeRoleSession'
        )
        credentials = assumed_role['Credentials']
        codecommit_client = boto3.client(
            service_name='codecommit',
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken']
        )
    else:
        codecommit_client = boto3.client('codecommit')

    def get_files(folder_path: str = ''):
        response = codecommit_client.get_folder(
            repositoryName=repository_name,
            folderPath=folder_path,
            **({'commitSpecifier': ref} if ref else {})
        )
        for file in response['files']:
            if file['absolutePath'] not in ignore:
                yield file['absolutePath']
        for folder in response['subFolders']:
            yield from get_files(folder['absolutePath'])

    def download_file(file_path):
        file = codecommit_client.get_file(
            repositoryName=repository_name,
            filePath=file_path,
            **({'commitSpecifier': ref} if ref else {})
        )
        logger.info(f"> {file_path}")
        abs_path = base_path / file_path
        abs_path.parent.mkdir(exist_ok=True, parents=True)
        with abs_path.open('wb') as f:
            f.write(file['fileContent'])

    with ThreadPoolExecutor(9) as executor:
        executor.map(download_file, get_files())


def copy_to_s3(base_path: Path = default_base_path):
    bucket = get_dbt_docs_bucket()
    bucket.Object('dbt-project.zip').delete()

    # Create zip file in memory
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(base_path):
            for file in files:
                file_path = Path(root) / file
                zipf.write(file_path, file_path.relative_to(base_path.parent))

    # Reset buffer position to start
    zip_buffer.seek(0)

    # Upload directly from memory
    bucket.upload_fileobj(zip_buffer, 'dbt-project.zip')
    logger.info(f'Zipped and uploaded {base_path} to s3://{bucket.name}/dbt-project.zip')


def copy_from_s3(base_path: Path = default_base_path):
    bucket = get_dbt_docs_bucket()

    # Download zip file into memory
    zip_buffer = io.BytesIO()
    try:
        bucket.download_fileobj('dbt-project.zip', zip_buffer)
    except bucket.meta.client.exceptions.ClientError as e:
        logger.error(f'Failed to download dbt-project.zip from S3: {str(e)}')
        raise

    # Reset buffer position to start
    zip_buffer.seek(0)

    # Create base directory if it doesn't exist
    base_path.mkdir(parents=True, exist_ok=True)

    # Extract all files from zip
    with zipfile.ZipFile(zip_buffer) as zipf:
        zipf.extractall(base_path.parent)

    logger.info(f'Downloaded and extracted dbt-project.zip to {base_path}')
