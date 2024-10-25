venv_path = $(VENV_PATH)/tatenmitdaten/packages/dbt-lambda

venv:
	rm -rf $(venv_path)
	python3.12 -m venv $(venv_path)
	ln -sfn $(venv_path) venv
	$(venv_path)/bin/python -m pip install --upgrade pip-tools pip setuptools

install:
	$(venv_path)/bin/python -m pip install -e .[dev]

setup: venv install

test:
	$(venv_path)/bin/pytest -W "ignore::DeprecationWarning"

check:
	$(venv_path)/bin/flake8 "dbt_lambda" --ignore=E501
	$(venv_path)/bin/mypy "dbt_lambda" --check-untyped-defs --python-executable $(venv_path)/bin/python

.PHONY: setup venv install lock check