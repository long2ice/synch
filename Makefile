checkfiles = synch/ tests/
black_opts = -l 100 -t py38
py_warn = PYTHONDEVMODE=1

help:
	@echo "synch development makefile"
	@echo
	@echo  "usage: make <target>"
	@echo  "Targets:"
	@echo  "    up			Updates dev/test dependencies"
	@echo  "    deps        Ensure dev/test dependencies are installed"
	@echo  "    check		Checks that build is sane"
	@echo  "    lint		Reports all linter violations"
	@echo  "    test		Runs all tests"
	@echo  "    style		Auto-formats the code"

up:
	@poetry update

deps:
	@poetry install --no-root -E mysql -E postgres -E kafka -E sentry

style: deps
	isort -src $(checkfiles)
	black $(black_opts) $(checkfiles)

check: deps
	black --check $(black_opts) $(checkfiles) || (echo "Please run 'make style' to auto-fix style issues" && false)
	flake8 $(checkfiles)
	bandit -x tests -r $(checkfiles)

test: deps
	$(py_warn) local=$(local) pytest

build: deps
	@poetry build

publish: deps
	@poetry publish --build