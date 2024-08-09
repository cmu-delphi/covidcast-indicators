.PHONY = venv, lint, test, clean

dir = $(shell find ./delphi_* -name __init__.py | grep -o 'delphi_[_[:alnum:]]*' | head -1)
venv:
	python3.8 -m venv env
	env/bin/python -m pip install pip==23.0.1

install: venv
	env/bin/pip install -e ../_delphi_utils_python
	env/bin/pip install -e .

install-ci: venv
	env/bin/pip install ../_delphi_utils_python
	env/bin/pip install .

lint:
	env/bin/pylint $(dir) --rcfile=../pyproject.toml
	env/bin/pydocstyle $(dir)

format:
	env/bin/darker $(dir)

test:
	(cd tests && ../env/bin/pytest --cov=$(dir) --cov-report=term-missing)

clean:
	rm -rf env
	rm -f params.json

run:
	env/bin/python -m $(dir)
	env/bin/python -m delphi_utils.validator --dry_run
	env/bin/python -m delphi_utils.archive
