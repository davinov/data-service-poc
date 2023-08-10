ifneq ("$(wildcard .env)","")
  ENV_FILE=.env
else
  ENV_FILE=tests/env
endif
# we include the available env file
-include $(ENV_FILE)

PYTEST_CMD=TESTING=true pytest tests -n 4 -vv

# dev aliases format and lint
RUFF=ruff src tests
BLACK=black src tests
MYPY=# mypy src tests # we skip mypy checks for now

SHELL := /bin/bash # Use bash syntax

.PHONY: install
install:  ## Install dependencies (including dev deps) inside current venv.
	pip install -U pip poetry==1.5.1
	poetry install
	pre-commit install

.PHONY: format
format:  ## Reformat project code.
	${RUFF} --fix
	${BLACK}

.PHONY: lint
lint:  ## Lint project code.
	${RUFF}
	${BLACK} --check
	${MYPY}
