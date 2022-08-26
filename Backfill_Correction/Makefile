SHELL:=/bin/bash --rcfile bash-init.sh

default:
	@echo No default implemented yet

install: dev

dev: delphiBackfillCorrection_1.0.tar.gz
	R CMD INSTALL delphiBackfillCorrection_1.0.tar.gz

lib:
	R -e 'roxygen2::roxygenise("delphiBackfillCorrection")'

run-R:
	rm -rf tmp
	time Rscript run.R 2>&1 | tee tmp
	grep "run_facebook completed successfully" tmp
	grep "scheduled core" tmp ; \
	[ "$$?" -eq 1 ]

coverage:
	Rscript -e 'covr::package_coverage("delphiBackfillCorrection")'

# best we can do
lint: coverage

test: delphiBackfillCorrection_1.0.tar.gz
	R CMD check --test-dir=unit-tests $<

delphiBackfillCorrection_1.0.tar.gz: $(wildcard delphiBackfillCorrection/R/*.R)
	R CMD build delphiBackfillCorrection
