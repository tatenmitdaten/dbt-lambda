VENV_PATH = $(VENV_ROOT)/tatenmitdaten/packages/dbt-lambda

venv:
	rm -rf $(VENV_PATH)
	uv venv $(VENV_PATH) --python 3.12
	ln -sfn $(VENV_PATH) venv
	. venv/bin/activate && uv pip install setuptools

install:
	. venv/bin/activate && uv pip install -e .[dev]

setup: venv install

test:
	export SAM_CONFIG_FILE=example/transform/samconfig.yaml && \
	venv/bin/pytest -W "ignore::DeprecationWarning"

check:
	venv/bin/flake8 "dbt_lambda" --ignore=E501
	venv/bin/mypy "dbt_lambda" --check-untyped-defs --python-executable venv/bin/python

lock:
	. venv/bin/activate && uv pip compile -o example/transform-layer/requirements.txt example/transform-layer/requirements.in


.PHONY: setup venv install test check