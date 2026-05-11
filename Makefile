.PHONY: run test lint format install

install:
	pip install -e ".[dev]"

run:
	uvicorn datastore.main:app --reload

test:
	pytest

lint:
	ruff check src tests
	mypy src

format:
	ruff format src tests
	ruff check --fix src tests
