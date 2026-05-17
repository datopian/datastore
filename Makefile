.PHONY: run test lint format install check

install:
	pip install -e ".[dev]"

run:
	uvicorn datastore.main:app --reload

test:
	pytest

lint:
	ruff check datastore tests
	mypy datastore

format:
	ruff format datastore tests
	ruff check --fix datastore tests

check: lint test
