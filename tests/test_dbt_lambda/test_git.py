import os
import pathlib
import shutil

import pytest

from dbt_lambda.git import copy_dbt_project


@pytest.fixture
def base_path():
    base_path = pathlib.Path('/tmp/dbt')
    yield base_path
    shutil.rmtree(base_path, ignore_errors=True)


@pytest.mark.skip('test cannot be mocked')
def test_copy_dbt_project(aws_sandbox, base_path):
    copy_dbt_project(base_path)
    project_yml_path = base_path / 'dbt_project.yml'
    assert project_yml_path.exists()
