.PHONY = venv, lint, test, clean

dir = $(shell find ./delphi_* -name __init__.py | grep -o 'delphi_[_[:alnum:]]*' | head -1)
venv:
	python3.8 -m venv env; \
	. env/bin/activate; \
	python -m pip install pip==23.0.1

install: venv
	. env/bin/activate; \
	pip install -e ../_delphi_utils_python ;\
	pip install -e .

install-ci: venv
	. env/bin/activate; \
	pip install ../_delphi_utils_python ;\
	pip install .

lint:
	. env/bin/activate; pylint $(dir) --rcfile=../pyproject.toml
	. env/bin/activate; pydocstyle $(dir)

format:
	. env/bin/activate; darker $(dir)

test:
	. env/bin/activate ;
	(cd tests && ../env/bin/pytest --cov=$(dir) --cov-report=term-missing)

clean:
	rm -rf env
	rm -f params.json

run:
	env/bin/python -m $(dir)
	env/bin/python -m delphi_utils.archive --archive_type s3 --indicator_prefix $(dir)