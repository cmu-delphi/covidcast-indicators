SHELL:=/bin/bash

# Change training options during `make` call via `make <command> OPTIONS="<options>"`
# Allowed OPTIONS flags are `--train_models` and `--make_predictions`
OPTIONS=

PYTHON:=env/bin/python
USR_INPUT_DIR=$(shell $(PYTHON) -m delphi_utils get input_dir)
USR_CACHE_DIR=$(shell $(PYTHON) -m delphi_utils get cache_dir)
USR_EXPORT_DIR=$(shell $(PYTHON) -m delphi_utils get export_dir)

# Gurobi license
GRB_WLSACCESSID=$(shell $(PYTHON) -m delphi_utils get gurobi.GRB_WLSACCESSID)
GRB_WLSSECRET=$(shell $(PYTHON) -m delphi_utils get gurobi.GRB_WLSSECRET)
GRB_LICENSEID=$(shell $(PYTHON) -m delphi_utils get gurobi.GRB_LICENSEID)

# AWS access info
AWS_KEY_ID=$(shell $(PYTHON) -m delphi_utils get post.aws_credentials.aws_access_key_id)
AWS_SECRET_KEY=$(shell $(PYTHON) -m delphi_utils get post.aws_credentials.aws_secret_access_key)
S3_BUCKET=$(shell $(PYTHON) -m delphi_utils get bucket_name)

DOCKER_IMAGE=ghcr.io/cmu-delphi/covidcast-indicators-backfill_corrections
DOCKER_TAG=latest

# Static dir names for use inside Docker container
INPUT_DIR=input
CACHE_DIR=cache
LOG_DIR=logs
EXPORT_DIR=receiving

PWD=$(shell pwd)

# System time and date
TODAY:=$(shell date +"%Y-%m-%d")
CURR_TIME:=$(shell date +"%Hh%Mm%Ss")

LOG_FILE:=$(LOG_DIR)/$(TODAY)_$(CURR_TIME).log

default:
	@echo No default implemented yet

install: install-R install-python

install-R: delphiBackfillCorrection_1.0.tar.gz
	R CMD INSTALL delphiBackfillCorrection_1.0.tar.gz

install-python:
	if [[ `python3 -c 'import sys; print(sys.version_info.minor)'` -lt 8 ]]; then \
	  echo 'python must be version 3.8 or higher'; \
	  exit 1; \
	fi
	python3 -m venv env
	source env/bin/activate && \
	pip install wheel && \
	pip install delphi_utils

lib:
	R -e 'roxygen2::roxygenise("delphiBackfillCorrection")'

run-local:
	time Rscript run.R $(OPTIONS) 2>&1 | tee $(LOG_FILE)
	grep "backfill correction completed successfully" $(LOG_FILE)
	grep "scheduled core" $(LOG_FILE) ; \
	[ "$$?" -eq 1 ]

run:
	@export GRB_WLSACCESSID=$(GRB_WLSACCESSID)
	@export GRB_WLSSECRET=$(GRB_WLSSECRET)
	@export GRB_LICENSEID=$(GRB_LICENSEID)

	docker run --rm --pull=always \
		-v "${PWD}/${LOG_DIR}:/backfill_corrections/${LOG_DIR}" \
		-v "`realpath $(USR_EXPORT_DIR)`:/backfill_corrections/${EXPORT_DIR}" \
		-v "`realpath $(USR_INPUT_DIR)`:/backfill_corrections/${INPUT_DIR}" \
		-v "`realpath $(USR_CACHE_DIR)`:/backfill_corrections/${CACHE_DIR}" \
		-v "${PWD}"/params.json:/backfill_corrections/params.json \
		--env GRB_WLSACCESSID \
		--env GRB_WLSSECRET \
		--env GRB_LICENSEID \
		-i "${DOCKER_IMAGE}:${DOCKER_TAG}" \
		/bin/bash -c "make run-local OPTIONS=\"${OPTIONS}\""

publish:
	aws configure set aws_access_key_id $(AWS_KEY_ID)
	aws configure set aws_secret_access_key $(AWS_SECRET_KEY)
	aws s3 cp $(USR_INPUT_DIR) $(S3_BUCKET)/ --recursive --exclude "*" --include "*.csv.gz" --acl public-read
	echo "SUCCESS: published `ls -1 $(USR_EXPORT_DIR)/*.csv.gz | wc -l` files to the S3 bucket" >> $(LOG_FILE)

pipeline: setup run publish teardown clean

# Make sure all user-specified dirs exist locally; create them if not.
# Reconfigure `params.json` to use fixed dir names, INPUT_DIR, etc, as defined
# above.
#
# This is a convenience for working with Docker. It allows local dirs to all
# be mounted in the Docker container's working directory,
# `/backfill_corrections/` regardless of the actual locations of the local
# dirs and whether their locations are provided in `params.json` as absolute
# or relative paths.
#
# (An alternative approach would be to check if the user-provided dir paths
# are each absolute or relative first, and only concat relative paths with
# `/backfill_corrections/` in the mount step.)
setup:
	[ -f $(USR_INPUT_DIR) ] || mkdir -p $(USR_INPUT_DIR)
	[ -f $(USR_CACHE_DIR) ] || mkdir -p $(USR_CACHE_DIR)
	[ -f $(USR_EXPORT_DIR) ] || mkdir -p $(USR_EXPORT_DIR)
	[ -f $(LOG_DIR) ] || mkdir -p $(LOG_DIR)
	$(PYTHON) -m delphi_utils set input_dir $(INPUT_DIR)
	$(PYTHON) -m delphi_utils set cache_dir $(CACHE_DIR)
	$(PYTHON) -m delphi_utils set export_dir $(EXPORT_DIR)

clean:
	rm -f $(USR_EXPORT_DIR)/*.csv.gz

# Restore dir names in params to user-provided values.
teardown:
	$(PYTHON) -m delphi_utils set input_dir $(USR_INPUT_DIR)
	$(PYTHON) -m delphi_utils set cache_dir $(USR_CACHE_DIR)
	$(PYTHON) -m delphi_utils set export_dir $(USR_EXPORT_DIR)

coverage:
	Rscript -e 'covr::package_coverage("delphiBackfillCorrection")'

# best we can do
lint: coverage

test: delphiBackfillCorrection_1.0.tar.gz
	R CMD check --test-dir=unit-tests $<

delphiBackfillCorrection_1.0.tar.gz: $(wildcard delphiBackfillCorrection/R/*.R)
	R CMD build delphiBackfillCorrection
