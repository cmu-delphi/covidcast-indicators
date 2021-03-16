.PHONY = venv, lint, test, clean

venv:
	python3.8 -m venv env

install: venv
	. env/bin/activate; \
	pip install wheel ; \
	pip install -e .

lint:
	. env/bin/activate; pylint delphi_utils
	. env/bin/activate; pydocstyle delphi_utils

test:
	. env/bin/activate ;\
	(cd tests && ../env/bin/pytest --cov=delphi_utils --cov-report=term-missing)

clean:
	rm -rf env
	rm -f params.json
