.PHONY = venv, lint, test, clean

dir = $(shell find ./delphi_* -name __init__.py | grep -o 'delphi_[_[:alnum:]]*' | head -1)
venv:
	python3.8 -m venv env

install: venv
	. env/bin/activate; \
	pip install wheel ; \
	pip install -e ../_delphi_utils_python ;\
	pip install -e .

install-ci: venv
	. env/bin/activate; \
	pip install wheel ; \
	pip install ../_delphi_utils_python ;\
	pip install .

lint:
	. env/bin/activate; pylint $(dir)
	. env/bin/activate; pydocstyle $(dir)

test:
	. env/bin/activate ;\
	(cd tests && ../env/bin/pytest --cov=$(dir) --cov-report=term-missing)

clean:
	rm -rf env
	rm -f params.json
