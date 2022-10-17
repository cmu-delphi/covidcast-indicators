SHELL:=/bin/bash

DOCKER_IMAGE=ghcr.io/cmu-delphi/covidcast-indicators-backfill_corrections
DOCKER_TAG=latest
# Change training options during `make` call via `make <command> OPTIONS="<options>"`
OPTIONS=--train_models --make_predictions

S3_URL=https://backfill_corrections.s3.us-east-2.amazonaws.com
S3_BUCKET=s3://backfill_corrections

AWS_KEY_ID=aws_access_key_id
AWS_SECRET_KEY=aws_secret_access_key

# System time and date
TODAY:=$(shell date +"%Y-%m-%d")
CURR_TIME:=$(shell date +"%Hh%Mm%Ss")
LOG_FILE:=logs/$(TODAY)_$(CURR_TIME).log

PWD=$(shell pwd)

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
	# Assumes receiving is output dir
	docker run --rm --pull=always \
	    -v "${PWD}"/logs:/backfill_corrections/logs \
	    -v "${PWD}"/receiving:/backfill_corrections/receiving \
	    -v "${PWD}"/input:/backfill_corrections/input \
	    -v "${PWD}"/cache:/backfill_corrections/cache \
	    -v "${PWD}"/params.json:/backfill_corrections/params.json \
		-i "${DOCKER_IMAGE}:${DOCKER_TAG}" \
		/bin/bash -c "make run-local OPTIONS=\"${OPTIONS}\""

deploy: run
	aws configure set aws_access_key_id AWS_KEY_ID
	aws configure set aws_secret_access_key AWS_SECRET_KEY
	# Assumes receiving is output dir
	aws s3 cp receiving/ $(S3_BUCKET)/ --recursive --exclude "*" --include "*csv.gz" --acl public-read

coverage:
	Rscript -e 'covr::package_coverage("delphiBackfillCorrection")'

# best we can do
lint: coverage

test: delphiBackfillCorrection_1.0.tar.gz
	R CMD check --test-dir=unit-tests $<

delphiBackfillCorrection_1.0.tar.gz: $(wildcard delphiBackfillCorrection/R/*.R)
	R CMD build delphiBackfillCorrection
