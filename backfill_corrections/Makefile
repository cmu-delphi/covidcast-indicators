SHELL:=/bin/bash

DOCKER_IMAGE=ghcr.io/cmu-delphi/covidcast-indicators-backfill_corrections
DOCKER_TAG=latest
# Change training options during `make` call via `make <command> OPTIONS="<options>"`
OPTIONS=--train_models --make_predictions

S3_URL=https://backfill_corrections.s3.us-east-2.amazonaws.com
S3_BUCKET=s3://backfill_corrections

TODAY:=$(shell date -u +"%Y-%m-%d")
CURR_TIME:=$(shell date -u +"%Hh%Mm%Ss")
LOG_FILE:=logs/$(TODAY)_$(CURR_TIME).log

default:
	@echo No default implemented yet

install: dev

dev: delphiBackfillCorrection_1.0.tar.gz
	R CMD INSTALL delphiBackfillCorrection_1.0.tar.gz

lib:
	R -e 'roxygen2::roxygenise("delphiBackfillCorrection")'

run-local:
	time Rscript run.R $(OPTIONS) 2>&1 | tee $(LOG_FILE)
	grep "backfill correction completed successfully" $(LOG_FILE)
	grep "scheduled core" $(LOG_FILE) ; \
	[ "$$?" -eq 1 ]

run:
	docker run --rm --pull=always \
	    -v "${PWD}"/logs:/backfill_corrections/logs \
	    -v "${PWD}"/receiving:/backfill_corrections/receiving \
	    -v "${PWD}"/params.json:/backfill_corrections/params.json \
	    -v "${PWD}"/input:/backfill_corrections/input \
		-i "${DOCKER_IMAGE}:${DOCKER_TAG}" \
		/bin/bash -c "make run-local OPTIONS=\"${OPTIONS}\""

deploy: run
	# TODO: assumes receiving is output dir
	aws s3 cp receiving/ $(S3_BUCKET)/ --recursive --exclude "*" --include "*csv" --acl public-read

coverage:
	Rscript -e 'covr::package_coverage("delphiBackfillCorrection")'

# best we can do
lint: coverage

test: delphiBackfillCorrection_1.0.tar.gz
	R CMD check --test-dir=unit-tests $<

delphiBackfillCorrection_1.0.tar.gz: $(wildcard delphiBackfillCorrection/R/*.R)
	R CMD build delphiBackfillCorrection
