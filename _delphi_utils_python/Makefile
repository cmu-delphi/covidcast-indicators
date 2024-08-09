.PHONY = venv, lint, test, clean, release

venv:
	python3.8 -m venv env
	env/bin/python -m pip install pip==23.0.1

install: venv
	env/bin/pip install -e '.[dev]'

install-ci: venv
	env/bin/pip install '.[dev]'

lint:
	env/bin/pylint delphi_utils --rcfile=../pyproject.toml
	env/bin/pydocstyle delphi_utils

format:
	env/bin/darker delphi_utils

test:
	(cd tests && ../env/bin/pytest --cov=delphi_utils --cov-report=term-missing)

clean:
	rm -rf env
	rm -f params.json

release: lint test
	env/bin/python -m build --sdist --wheel
