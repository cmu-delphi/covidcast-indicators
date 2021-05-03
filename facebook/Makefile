SHELL:=/bin/bash

# dry-run mode: generate all files, but do not post them anywhere, and disable all emails to outside parties.
DRY:=yes
ifeq ($(DRY),yes)
	OUTSIDE_EMAIL_FLAGS:=-d
	SFTP_POST:=echo -e "Would run: sftp -b <(echo -e \"\$${BATCH}\") -P 2222 fb-automation@ftp.delphi.cmu.edu\n$${BATCH}"
	CONTINGENCY_SFTP_POST:=echo -e "Would run: sftp -b <(echo -e \"\$${BATCH}\") ???@publishing.andrew.cmu.edu\n$${BATCH}"
	DRY_MESSAGE:="[DRY-RUN] "
else
	SFTP_POST:=sftp -b <(echo -e "$${BATCH}") -P 2222 fb-automation@ftp.delphi.cmu.edu
	CONTINGENCY_SFTP_POST:=sftp -b <(echo -e "$${BATCH}") ???@publishing.andrew.cmu.edu
endif

EPOCH:=2020-04-06
TODAY:=$(shell date +"%Y-%m-%d")
YESTERDAY:=$(shell date --date "1 day ago" +"%Y-%m-%d")
ONEWEEK:=$(shell date --date "7 day ago" +"%Y-%m-%d")
THREEWEEK:=$(shell date --date "21 day ago" +"%Y-%m-%d")

MESSAGES:=$(TODAY).messages

PYTHON:=env/bin/python
QUALTRICS=$(shell $(PYTHON) -m delphi_utils get input_dir)
WEIGHTS=$(shell $(PYTHON) -m delphi_utils get weights_in_dir)
CIDS=$(shell $(PYTHON) -m delphi_utils get weights_out_dir)
INDIVIDUAL=$(shell $(PYTHON) -m delphi_utils get individual_dir)
ARCHIVE=$(shell $(PYTHON) -m delphi_utils get archive_dir)
RECEIVING=$(shell $(PYTHON) -m delphi_utils get export_dir)
CONTINGENCY_RECEIVING=$(shell $(PYTHON) -m delphi_utils get contingency.export_dir)
FB_CC=$(shell $(PYTHON) -m delphi_utils get qualtrics.notify-bad-weights)

MAX_WEIGHTED=ls -1 $(WEIGHTS) | grep dap | tail -1 | sed 's/_.*//;s/-//g;'

ANTIJOIN:="antijoin.cids.sorted.txt"
CIDS_DEST:="fb-interchange/cmu_respondent_ids"
INDIVID_DEST:="fb-public-results/"
RAW_DEST:="raw"
CONTINGENCY_DEST:=delphi-web/surveys

default:
	@echo No default implemented yet

tidy: $(RECEIVING)
	rm -rf $@
	mkdir -p $@/$(RECEIVING) $@/$(INDIVIDUAL)
	cp params.json $@
	mv $(RECEIVING)/*.csv $@/$(RECEIVING)
	mv $(INDIVIDUAL)/*.csv* $@/$(INDIVIDUAL)
	tar -czf tidy-`date +"%Y-%m-%d-%H%M%S"`.tgz $@

tidy-%-contingency: $(CONTINGENCY_RECEIVING) post-%-contingency
	rm -rf $@
	mkdir -p $@/$(CONTINGENCY_RECEIVING)
	cp params.json $@
	cp contingency_input.txt $@
	cp $(CONTINGENCY_RECEIVING)/*.csv $@/$(CONTINGENCY_RECEIVING)
	tar -czf tidy-$*-`date +"%Y-%m-%d-%H%M%S"`.tgz $@

clean:
	rm -f $(RECEIVING)/*.csv $(INDIVIDUAL)/*.csv $(CIDS)/*.csv $(CONTINGENCY_RECEIVING)/*.csv

clean-archive:
	rm -f $(ARCHIVE)/*.Rds

install-python:
	python3.8 -m venv env
	source env/bin/activate && \
	pip install -e ../_delphi_utils_python && \
	pip install -e delphiFacebook/python

install-R: dev

install: install-python install-R

$(CIDS) $(CONTINGENCY_RECEIVING):
	[ -f $@ ] || mkdir -p $@

init-qualtrics:
	grep '"token": "..*"' params.json
	git show -s --oneline HEAD >> $(MESSAGES)

$(TODAY):
	[ -f $(QUALTRICS) ] || mkdir -p $(QUALTRICS)
	rm -rf tmp
	touch -d "$(TODAY) 01:00" tmp
	source env/bin/activate && \
	python -m delphi_facebook
	BATCH=""; \
	for f in `find $(QUALTRICS) -maxdepth 1 -newer tmp -type f -name "*.csv"`; do \
	  BATCH="$${BATCH}put $$f $(RAW_DEST)\n"; \
	done; \
	$(SFTP_POST); \
	echo "SUCCESS: $(DRY_MESSAGE)Posted `echo -e $${BATCH} | wc -l` raw files" >> $(MESSAGES)
	mv tmp $@

params.json: $(TODAY)
	PAT=`grep fb-survey params.json | awk 'BEGIN{FS="\""}{print $$2}' | sed 's/ /_/g;s/^/-e /'`; \
	$(PYTHON) -m delphi_utils set \
		debug false \
		end_date $(YESTERDAY) \
		input <(find $(QUALTRICS) -maxdepth 1 -newer $< -type f -name "*.csv" | sort | grep $${PAT}  | tr '\n' ',' | sed 's_$(QUALTRICS)/__g;s/,$$//' ) \
		parallel true \
		output cids,individual,covidalert,archive,community \
		start_date $(YESTERDAY)

%ly-contingency-params: $(WEIGHTS)
	$(PYTHON) -m delphi_utils set \
		contingency.debug false \
		contingency.end_date $(TODAY) \
		contingency.input [] \
		contingency.parallel true \
		contingency.aggregate_range "$*"

$(WEIGHTS): $(TODAY)
	[ -f $(WEIGHTS) ] || mkdir -p $(WEIGHTS)
	cd "$(WEIGHTS)"; \
	BATCH="cd fb-interchange/cmu_respondent_weights\nls -1"; \
	NEW=`LC_ALL=C comm -23 <(sftp -b <(echo -e "$${BATCH}") -P 2222 fb-automation@ftp.delphi.cmu.edu | grep "^202" | LC_ALL=C sort) <(ls -1 | LC_ALL=C sort)`; \
	echo "New weights files:"; \
	echo $${NEW}; \
	for f in $${NEW}; do \
	  BATCH="$${BATCH}\nget $$f"; \
	done; \
	sftp -b <(echo -e "$${BATCH}") -P 2222 fb-automation@ftp.delphi.cmu.edu || exit 90; \
	cd -; \
	MAX_WEIGHTED=`$(MAX_WEIGHTED)`; \
	EXPECTED_MAX_WEIGHTED=`date --date='3 days ago' +'%Y%m%d'`; \
	if [[ $$EXPECTED_MAX_WEIGHTED -gt $$MAX_WEIGHTED ]]; then \
	  MSG="Expected most recent file: $$EXPECTED_MAX_WEIGHTED\nActual most recent file: $$MAX_WEIGHTED"; \
	  echo "WARNING: $${MSG}" | tr "\n" ";" >> $(MESSAGES); \
	  echo -e "$(FB_CC)\n\n$${MSG}" | \
	  s-nail $(OUTSIDE_EMAIL_FLAGS) -t -s "[fb-cmu-cvid] Weights are stale" -. krivard@cs.cmu.edu; \
	fi

dev: delphiFacebook_1.0.tar.gz
	R CMD INSTALL delphiFacebook_1.0.tar.gz

lib:
	R -e 'roxygen2::roxygenise("delphiFacebook")'

run-R: $(CIDS)
	time Rscript run.R

pipeline: init-qualtrics params.json $(WEIGHTS) run-R post-cids post-individual post-done tidy
	grep $(TODAY) params.json
	[ -f $(YESTERDAY) ] && rm $(YESTERDAY) || true
	touch $@
	echo "SUCCESS: $(DRY_MESSAGE)pipeline complete" >> $(MESSAGES)

run-%-contingency: $(CONTINGENCY_RECEIVING) 
	time Rscript contingency_tables.R

%-contingency-pipeline: %-contingency-params run-%-contingency post-%-contingency tidy-%-contingency
	rm contingency_input.txt
	rm $(CONTINGENCY_RECEIVING)/*.csv
	touch $@
	echo "SUCCESS: $(DRY_MESSAGE)$* contingency pipeline complete" >> $(MESSAGES)

coverage:
	Rscript -e 'covr::package_coverage("delphiFacebook")'

# best we can do
lint: coverage

test: delphiFacebook_1.0.tar.gz
	if [ ! -f params.json ]; then cp params.json.template params.json; fi
	grep testthat params.json
	R CMD check $<

delphiFacebook_1.0.tar.gz: $(wildcard delphiFacebook/R/*.R)
	R CMD build delphiFacebook

post-cids: $(TODAY) $(CIDS)
	rm -rf tmp
	touch $(ANTIJOIN)
	POST=`find $(CIDS) -maxdepth 1 -newer $(TODAY) -name "cvid_cids_*.csv"`; \
	[ -n "$${POST}" ]; \
	LC_ALL=C find $(CIDS) -maxdepth 1 -daystart -mtime +0 -name "cvid_cids*.csv" -exec sort -u -o ${ANTIJOIN} {} +; \
	BATCH=""; \
	for f in $${POST}; do \
	    LC_ALL=C comm -23 <(LC_ALL=C sort $$f) ${ANTIJOIN} >tmp; \
	    diff -q tmp $$f || mv $$f $$f.bak; \
	    mv tmp $$f; \
	    ncids=`wc -l $$f | awk '{print $$1}'`; \
	    if [[ $$ncids == "0" ]]; then \
		echo "ERROR: 0 CIDs reported for $$f"; \
		exit 73; \
	    fi; \
	    BATCH="$${BATCH}put $$f ${CIDS_DEST}\n"; \
	done; \
	$(SFTP_POST); \
	echo "SUCCESS: $(DRY_MESSAGE)Posted `echo $${POST} | wc -w` cid files" >> $(MESSAGES)
	touch $@

post-individual: $(TODAY) $(INDIVIDUAL)
	POST=`find $(INDIVIDUAL) -maxdepth 1 -newer $(TODAY) -name "cvid_responses_*.csv"`; \
	[ -n "$${POST}" ]; \
	BATCH=""; \
	for f in $${POST}; do \
	    (grep token $$f; [[ $$? -eq 1 ]]); \
	    gzip -f $$f; \
	    BATCH="$${BATCH}put $${f}.gz ${INDIVID_DEST}\n"; \
	done; \
	$(SFTP_POST); \
	echo "SUCCESS: $(DRY_MESSAGE)Posted `echo $${POST} | wc -w` microresponse files" >> $(MESSAGES)
	touch $@

post-done: post-cids
	touch $(YESTERDAY).done
	BATCH="put $(YESTERDAY).done $(CIDS_DEST)\n"; \
	$(SFTP_POST)
	echo "SUCCESS: $(DRY_MESSAGE)Posted $(YESTERDAY).done" >> $(MESSAGES)

post-%-contingency:
	POST=`find $(CONTINGENCY_RECEIVING) -maxdepth 1 -newer $(TODAY) -name "*.csv"`; \
	[ -n "$${POST}" ]; \
	BATCH=""; \
	for f in $${POST}; do \
	    BATCH="$${BATCH}put $$f \"${CONTINGENCY_DEST}/$*\"\n"; \
	done; \
	$(CONTINGENCY_SFTP_POST); \
	echo "SUCCESS: $(DRY_MESSAGE)Posted `echo $${POST} | wc -w` $* contingency tables" >> $(MESSAGES)
	touch $@

validate-covidcast:
	@echo validate-covidcast not yet implemented

CONTINGENCY_TOUCH:=X-contingency-params run-X-contingency tidy-X-contingency
.PRECIOUS: $(subst X,weekly,$(CONTINGENCY_TOUCH)) $(subst X,monthly,$(CONTINGENCY_TOUCH))
