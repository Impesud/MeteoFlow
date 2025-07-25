.PHONY: venv install test fmt sort lint types security precommit all

venv:
	python3 -m venv .venv

install: venv
	. venv/bin/activate && pip install -e .[test] pre-commit

test:
	pytest

fmt:
	black .

sort:
	isort .

lint:
	flake8

types:
	mypy src/

security:
	bandit -r src/

precommit:
	pre-commit install

all: fmt sort lint types security test

docker-up:
	docker-compose up --build

docker-down:
	docker-compose down -v
