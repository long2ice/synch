checkfiles = synch/ tests/ conftest.py
black_opts = -l 100 -t py38
py_warn = PYTHONDEVMODE=1

up:
	@poetry update

deps:
	@poetry install --no-root

style: deps
	isort -src $(checkfiles)
	black $(black_opts) $(checkfiles)

check: deps
	black --check $(black_opts) $(checkfiles) || (echo "Please run 'make style' to auto-fix style issues" && false)
	flake8 $(checkfiles)
	bandit -x tests -r $(checkfiles) -s B608

test: deps
	$(py_warn) pytest

build: deps
	@poetry build

ci: check test

act:
	@act -P ubuntu-latest=nektos/act-environments-ubuntu:18.04 -b
