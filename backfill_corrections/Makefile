SHELL:=/bin/bash

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

run-R:
	time Rscript run.R 2>&1 | tee $(LOG_FILE)
	grep "backfill correction completed successfully" $(LOG_FILE)
	grep "scheduled core" $(LOG_FILE) ; \
	[ "$$?" -eq 1 ]

coverage:
	Rscript -e 'covr::package_coverage("delphiBackfillCorrection")'

# best we can do
lint: coverage

test: delphiBackfillCorrection_1.0.tar.gz
	R CMD check --test-dir=unit-tests $<

delphiBackfillCorrection_1.0.tar.gz: $(wildcard delphiBackfillCorrection/R/*.R)
	R CMD build delphiBackfillCorrection
