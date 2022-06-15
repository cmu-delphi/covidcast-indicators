SHELL:=/bin/bash --rcfile bash-init.sh

EPOCH:=2020-04-06
TODAY:=$(shell date +"%Y-%m-%d")
YESTERDAY:=$(shell date --date "$(TODAY) -1 day" +"%Y-%m-%d")
ONEWEEK:=$(shell date --date "$(TODAY) -7 day" +"%Y-%m-%d")
THREEWEEK:=$(shell date --date "$(TODAY) -21 day" +"%Y-%m-%d")
LAST_SATURDAY:=$(shell date -d "last Saturday" +"%Y-%m-%d")
LAST_SUNDAY:=$(shell date -d "$(LAST_SATURDAY) -6 day" +"%Y-%m-%d")
TUESDAY:=$(shell date -d "$(LAST_SATURDAY) +3 day" +"%Y-%m-%d")

MESSAGES:="messages/$(TODAY).messages"

PYTHON:=env/bin/python
QUALTRICS=$(shell $(PYTHON) -m delphi_utils get input_dir)
WEIGHTS=$(shell $(PYTHON) -m delphi_utils get weights_in_dir)
WEEKLY_WEIGHTS=$(shell $(PYTHON) -m delphi_utils get weekly_weights_in_dir)
CIDS=$(shell $(PYTHON) -m delphi_utils get weights_out_dir)
CIDS_EXP=$(shell $(PYTHON) -m delphi_utils get experimental_weights_out_dir)
INDIVIDUAL=$(shell $(PYTHON) -m delphi_utils get individual_dir)
INDIVIDUAL_RACEETH=$(shell $(PYTHON) -m delphi_utils get individual_raceeth_dir)
ARCHIVE=$(shell $(PYTHON) -m delphi_utils get archive_dir)
RECEIVING=$(shell $(PYTHON) -m delphi_utils get export_dir)
FB_CC=$(shell $(PYTHON) -m delphi_utils get qualtrics.notify-bad-weights)
DELPHI_SURVEY_SFTP_PASSWORD=$(shell $(PYTHON) -m delphi_utils get delphi_survey_sftp_password)
DELPHI_SURVEY_SFTP_USER=$(shell $(PYTHON) -m delphi_utils get delphi_survey_sftp_user)
DELPHI_SURVEY_EMAIL_USER=$(shell $(PYTHON) -m delphi_utils get delphi_survey_email_user)
SFTP_OPTIONS=$(shell $(PYTHON) -m delphi_utils get sftp_options)

MAX_WEIGHTED=ls -1 $(WEIGHTS) | grep dap | tail -1 | sed 's/_.*//;s/-//g;'
MAX_WEEKLY_WEIGHTED=ls -1 $(WEEKLY_WEIGHTS) | grep map | tail -1 | sed 's/_.*//;s/-//g;'

ANTIJOIN:="antijoin.cids.sorted.txt"
ANTIJOIN_EXP:="antijoin.experimental.cids.sorted.txt"
CIDS_DEST:="fb-interchange/cmu_respondent_ids"
CIDS_EXP_DEST:="fb-interchange/cmu_respondent_ww_ids"
INDIVID_DEST:="fb-public-results/"
INDIVID_RACEETH_DEST:="protected-race-ethnicity-data/"
RAW_DEST:="raw"

# dry-run mode: generate all files, but do not post them anywhere, and disable all emails to outside parties.
DRY:=yes
ifeq ($(DRY),yes)
	SFTP_POST:=echo -e "Would run: sshpass -p $(DELPHI_SURVEY_SFTP_PASSWORD) sftp $(SFTP_OPTIONS) -b <(echo -e \"\$${BATCH}\") -P 2222 $(DELPHI_SURVEY_SFTP_USER)\n$${BATCH}"
	DRY_MESSAGE:="[DRY-RUN] "
else
	SFTP_POST:=sshpass -p $(DELPHI_SURVEY_SFTP_PASSWORD) sftp $(SFTP_OPTIONS) -b <(echo -e "$${BATCH}") -P 2222 $(DELPHI_SURVEY_SFTP_USER)
endif

ifneq ("$(wildcard params.json)","")
ifeq ($(WEIGHTS),$(WEEKLY_WEIGHTS))
$(error "'weights_in_dir' and 'weekly_weights_in_dir' must be different.")
endif
endif

default:
	@echo No default implemented yet

scratch:
	mkdir scratch
	rm -rf scratch/*

$(INDIVIDUAL) $(INDIVIDUAL_RACEETH):
	mkdir $@

tidy_%: receiving
	rm -rf $@/$(RECEIVING)
	rm -rf $@/$(INDIVIDUAL)
	rm -rf $@/$(INDIVIDUAL_RACEETH)
	rm -f $@/params.json
	mkdir -p $@ $@/$(RECEIVING) $@/$(INDIVIDUAL) $@/$(INDIVIDUAL_RACEETH)
	cp params.json $@/
	# Check for _any_ matching files using https://stackoverflow.com/a/6364244/14401472
	if compgen -G "$(RECEIVING)/*.csv" > /dev/null; then mv $(RECEIVING)/*.csv $@/$(RECEIVING); fi
	mv $(INDIVIDUAL)/*.csv* $@/$(INDIVIDUAL)
	mv $(INDIVIDUAL_RACEETH)/*.csv* $@/$(INDIVIDUAL_RACEETH)
	tar -czf scratch/$@-`date +"%Y-%m-%d-%H%M%S"`.tgz --exclude='tidy*-*.tgz' --exclude='*.done' $@
	mv scratch/*.tgz $@/

clean:
	rm -f $(RECEIVING)/*.csv $(INDIVIDUAL)/*.csv $(INDIVIDUAL_RACEETH)/*.csv $(CIDS)/*.csv $(CIDS_EXP)/*.csv

clean-archive:
	rm -f $(ARCHIVE)/*.Rds

install-python:
	python3.8 -m venv env
	source env/bin/activate && \
	pip install wheel && \
	pip install delphi_utils && \
	pip install -e delphiFacebook/python

install-R:dev

install: install-python install-R

$(CIDS):
	[ -f $(CIDS) ] || mkdir -p $(CIDS)

$(CIDS_EXP):
	[ -f $(CIDS_EXP) ] || mkdir -p $(CIDS_EXP)

init-qualtrics:
	grep '"token": "..*"' params.json

$(TODAY):
	[ -f $(QUALTRICS) ] || mkdir -p $(QUALTRICS)
	rm -rf tmp
	touch -d "$(TODAY) 01:00" tmp
	$(PYTHON) -m delphi_utils set end_date $(YESTERDAY)
	source env/bin/activate && \
	python -m delphi_facebook
	BATCH=""; \
	for f in `find $(QUALTRICS) -maxdepth 1 -newer tmp -type f -name "*.csv"`; do \
	  BATCH="$${BATCH}put $$f $(RAW_DEST)\n"; \
	done; \
	$(SFTP_POST); \
	echo "SUCCESS: $(DRY_MESSAGE)Posted `echo -e $${BATCH} | wc -l` raw files" >> $(MESSAGES)
	touch -d $(YESTERDAY) params.json
	mv tmp $@

params.json: $(TODAY)
	PAT=`grep fb-survey params.json | awk 'BEGIN{FS="\""}{print $$2}' | sed 's/ /_/g;s/^/-e /'`; \
	$(PYTHON) -m delphi_utils set \
		debug false \
		produce_individual_raceeth true \
		end_date $(YESTERDAY) \
		input <(find $(QUALTRICS) -maxdepth 1 -newer $< -type f -name "*.csv" | sort | grep $${PAT}  | tr '\n' ',' | sed 's_$(QUALTRICS)/__g;s/,$$//' ) \
		parallel true \
		output cids,individual,covidalert,archive,community \
		start_date $(YESTERDAY)

params.weekly-weights.json: $(TODAY)
	PAT=`grep fb-survey params.json | awk 'BEGIN{FS="\""}{print $$2}' | sed 's/ /_/g;s/^/-e /'`; \
	$(PYTHON) -m delphi_utils set \
		debug false \
		produce_individual_raceeth true \
		end_date $(YESTERDAY) \
		input <(find $(QUALTRICS) -maxdepth 1 -newer $< -type f -name "*.csv" | sort | grep $${PAT}  | tr '\n' ',' | sed 's_$(QUALTRICS)/__g;s/,$$//' ) \
		parallel true \
		output individual \
		start_date $(LAST_SUNDAY)

$(WEIGHTS): $(TODAY)
	[ -f $(WEIGHTS) ] || mkdir -p $(WEIGHTS)
	cd "$(WEIGHTS)"; \
	BATCH="cd fb-interchange/cmu_respondent_weights\nls -1"; \
	NEW=`LC_ALL=C comm -23 <(sshpass -p $(DELPHI_SURVEY_SFTP_PASSWORD) sftp $(SFTP_OPTIONS) -b <(echo -e "$${BATCH}") -P 2222 $(DELPHI_SURVEY_SFTP_USER) | grep "^202" | LC_ALL=C sort) <(ls -1 | LC_ALL=C sort)`; \
	echo "New weights files:"; \
	echo $${NEW}; \
	for f in $${NEW}; do \
	  BATCH="$${BATCH}\nget $$f"; \
	done; \
	sshpass -p $(DELPHI_SURVEY_SFTP_PASSWORD) sftp $(SFTP_OPTIONS) -b <(echo -e "$${BATCH}") -P 2222 $(DELPHI_SURVEY_SFTP_USER) || exit 90; \
	cd -; \
	MAX_WEIGHTED=`$(MAX_WEIGHTED)`; \
	EXPECTED_MAX_WEIGHTED=`date --date='$(TODAY) -3 day' +'%Y%m%d'`; \
	if [[ $$EXPECTED_MAX_WEIGHTED -gt $$MAX_WEIGHTED ]]; then \
	  MSG="Expected most recent file: $$EXPECTED_MAX_WEIGHTED; Actual most recent file: $$MAX_WEIGHTED"; \
	  echo "WARNING: $${MSG}" >> $(MESSAGES); \
	fi

$(WEEKLY_WEIGHTS): $(TODAY)
# This runs every day as a dependency of `pipeline`. A pipeline run is triggered when new weekly weights files are available.
	[ -f $(WEEKLY_WEIGHTS) ] || mkdir -p $(WEEKLY_WEIGHTS)
	cd "$(WEEKLY_WEIGHTS)"; \
	BATCH="cd fb-interchange/cmu_respondent_ww_weights\nls -1"; \
	NEW=`LC_ALL=C comm -23 <(sshpass -p $(DELPHI_SURVEY_SFTP_PASSWORD) sftp $(SFTP_OPTIONS) -b <(echo -e "$${BATCH}") -P 2222 $(DELPHI_SURVEY_SFTP_USER) | grep "^202" | LC_ALL=C sort) <(ls -1 | LC_ALL=C sort)`; \
	echo "New weekly weights files:"; \
	echo $${NEW}; \
	for f in $${NEW}; do \
	  BATCH="$${BATCH}\nget $$f"; \
	done; \
	sshpass -p $(DELPHI_SURVEY_SFTP_PASSWORD) sftp $(SFTP_OPTIONS) -b <(echo -e "$${BATCH}") -P 2222 $(DELPHI_SURVEY_SFTP_USER) || exit 90; \
	cd -; \
	touch -d $(YESTERDAY) $(WEEKLY_WEIGHTS); \
	EXPECTED_WEEKLY_WEIGHTED=`date --date='$(LAST_SUNDAY)' +'%Y%m%d'`; \
	MIN_NEW_WEEKLY_WEIGHTED=`grep map <<< $${NEW} | head -1 | sed 's/_.*//;s/-//g;'`; \
	if [[ `wc -w <<< $${NEW}` -gt 0 ]] && [[ $$MIN_NEW_WEEKLY_WEIGHTED -ne $$EXPECTED_WEEKLY_WEIGHTED ]]; then \
	  MSG="Expected new weekly weights files to start on: $$EXPECTED_WEEKLY_WEIGHTED; Actual new files starts on: $$MIN_NEW_WEEKLY_WEIGHTED"; \
	  echo "WARNING: $${MSG}" >> $(MESSAGES); \
	fi; \
	MAX_WEEKLY_WEIGHTED=`$(MAX_WEEKLY_WEIGHTED)`; \
	if [[ `date --date='$(TODAY)' +'%Y%m%d'` -gt `date --date='$(TUESDAY)' +'%Y%m%d'` ]] && [[ $$MAX_WEEKLY_WEIGHTED -lt $$EXPECTED_WEEKLY_WEIGHTED ]]; then \
	  MSG="Weekly weights are old; Expected most recent weekly weights file to start on: $$EXPECTED_WEEKLY_WEIGHTED; Actual most recent file starts on: $$MAX_WEEKLY_WEIGHTED"; \
	  echo "WARNING: $${MSG}" >> $(MESSAGES); \
	fi; \
	if [[ ! -f tidy_weekly/$(LAST_SUNDAY)-weekly-weights.done ]] && [[ $$MAX_WEEKLY_WEIGHTED -eq $$EXPECTED_WEEKLY_WEIGHTED ]]; then \
	  if [ -f params.json ]; then cp params.json params.daily.json; fi; \
	  $(MAKE) weekly-weights-pipeline; \
	  if [ -f params.daily.json ]; then \
		cp params.daily.json params.json; \
		rm -f params.daily.json; \
	  fi; \
	fi

dev: delphiFacebook_1.0.tar.gz
	R CMD INSTALL delphiFacebook_1.0.tar.gz

lib:
	R -e 'roxygen2::roxygenise("delphiFacebook")'

run-R: $(CIDS) $(CIDS_EXP)
	rm -rf tmp
	time Rscript run.R 2>&1 |tee tmp
	grep "run_facebook completed successfully" tmp
	grep "scheduled core" tmp ; \
	[ "$$?" -eq 1 ]

pipeline: scratch init-qualtrics params.json $(WEIGHTS) run-R post-cids post-experimental-cids post-individual post-individual-raceeth post-done tidy_daily $(WEEKLY_WEIGHTS)
	grep $(TODAY) params.json
	[ -f $(YESTERDAY) ] && rm $(YESTERDAY) || true
	touch $@
	echo "SUCCESS: $(DRY_MESSAGE)pipeline complete" >> $(MESSAGES)
	chmod o+w $(MESSAGES)

weekly-weights-pipeline: scratch init-qualtrics params.weekly-weights.json run-R post-individual post-individual-raceeth tidy_weekly
	touch $@
	echo "SUCCESS: $(DRY_MESSAGE)completed weekly weights pipeline" >> $(MESSAGES)
	touch tidy_weekly/$(LAST_SUNDAY)-weekly-weights.done

coverage:
	Rscript -e 'covr::package_coverage("delphiFacebook")'

# best we can do
lint: coverage

test: delphiFacebook_1.0.tar.gz
	if [ ! -f params.json ]; then cp params.json.template params.json; fi
	grep testthat params.json
	R CMD check --test-dir=unit-tests $<
	R CMD check --test-dir=integration-tests $<

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

post-experimental-cids: $(TODAY) $(CIDS_EXP)
	rm -rf tmp
	touch $(ANTIJOIN_EXP)
	POST=`find $(CIDS_EXP) -maxdepth 1 -newer $(TODAY) -name "cvid_cids_*.csv"`; \
	[ -n "$${POST}" ]; \
	LC_ALL=C find $(CIDS_EXP) -maxdepth 1 -daystart -mtime +0 -name "cvid_cids*.csv" -exec sort -u -o ${ANTIJOIN_EXP} {} +; \
	BATCH=""; \
	for f in $${POST}; do \
	    LC_ALL=C comm -23 <(LC_ALL=C sort $$f) ${ANTIJOIN_EXP} >tmp; \
	    diff -q tmp $$f || mv $$f $$f.bak; \
	    mv tmp $$f; \
	    ncids=`wc -l $$f | awk '{print $$1}'`; \
	    if [[ $$ncids == "0" ]]; then \
		echo "ERROR: 0 CIDs reported for $$f"; \
		exit 73; \
	    fi; \
	    BATCH="$${BATCH}put $$f ${CIDS_EXP_DEST}\n"; \
	done; \
	$(SFTP_POST); \
	echo "SUCCESS: $(DRY_MESSAGE)Posted `echo $${POST} | wc -w` experimental cid files" >> $(MESSAGES)
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

post-individual-raceeth: $(TODAY) $(INDIVIDUAL_RACEETH)
	POST=`find $(INDIVIDUAL_RACEETH) -maxdepth 1 -newer $(TODAY) -name "cvid_responses_*.csv"`; \
	[ -n "$${POST}" ]; \
	BATCH=""; \
	for f in $${POST}; do \
	    (grep token $$f; [[ $$? -eq 1 ]]); \
	    gzip -f $$f; \
	    BATCH="$${BATCH}put $${f}.gz ${INDIVID_RACEETH_DEST}\n"; \
	done; \
	$(SFTP_POST); \
	echo "SUCCESS: $(DRY_MESSAGE)Posted `echo $${POST} | wc -w` race-ethnicity microresponse files" >> $(MESSAGES)
	touch $@

post-done: post-cids post-experimental-cids
	touch $(YESTERDAY).done
	BATCH="put $(YESTERDAY).done $(CIDS_DEST)\n"; \
	$(SFTP_POST)
	BATCH="put $(YESTERDAY).done $(CIDS_EXP_DEST)\n"; \
	$(SFTP_POST)
	echo "SUCCESS: $(DRY_MESSAGE)Posted $(YESTERDAY).done" >> $(MESSAGES)

validate-covidcast:
	@echo validate-covidcast not yet implemented
