.PHONY = venv, lint, test, clean, release

venv:
	python3.8 -m venv env

install: venv
	. env/bin/activate; \
	pip install wheel ; \
	pip install -e '.[dev]'

install-ci: venv
	. env/bin/activate; \
	pip install 'build[virtualenv]' pylint pytest pydocstyle wheel twine ; \
	pip install '.[dev]'

lint:
	. env/bin/activate; pylint delphi_utils --rcfile=../pyproject.toml
	. env/bin/activate; pydocstyle delphi_utils

format:
	. env/bin/activate; darker delphi_utils

test:
	. env/bin/activate ;\
	(cd tests && ../env/bin/pytest --cov=delphi_utils --cov-report=term-missing)

clean:
	rm -rf env
	rm -f params.json

release: lint test
	. env/bin/activate ; \
	pip install 'build[virtualenv]' ; \
	python -m build --sdist --wheel