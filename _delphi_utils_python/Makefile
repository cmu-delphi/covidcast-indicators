.PHONY = venv, lint, test, clean

dir = $(shell find ./delphi_* -name __init__.py |  sed 's/\/__init__.py//g;s/\.\///g')

venv:
	python3 -m venv env

install: venv
	. env/bin/activate; \
	pip install wheel ; \
	pip install -e .

lint:
	. env/bin/activate; \
	pylint $(dir)

test:
	. env/bin/activate ;\
	(cd tests && ../env/bin/pytest --cov=$(dir) --cov-report=term-missing)

clean:
	rm -r env