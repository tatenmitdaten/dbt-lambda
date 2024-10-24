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
	$(venv_path)/bin/flake8 "src" --ignore=F401,E501
	$(venv_path)/bin/mypy "src" --python-executable $(venv_path)/bin/python

lock:
	$(venv_path)/bin/pip-compile --upgrade --strip-extras --build-isolation \
		--output-file src/requirements.txt src/pyproject.toml

.PHONY: setup venv install lock check