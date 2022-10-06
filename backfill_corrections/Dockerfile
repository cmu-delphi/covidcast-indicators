FROM rocker/tidyverse:latest

# use delphi's timezome
RUN ln -s -f /usr/share/zoneinfo/America/New_York /etc/localtime

RUN install2.r --error \
    roxygen2 \
    arrow \
    evalcast \
    jsonlite \
    lubridate \
    parallel \
    quantgen \
    rlang \
    stats \
    utils \
    zoo \

ADD ./delphiBackfillCorrection /backfill_corrections/delphiBackfillCorrection/
ADD ./Makefile /backfill_corrections/Makefile
ADD ./run.R /backfill_corrections/run.R
ADD ./GITREF backfill_corrections/GITREF
WORKDIR /backfill_corrections/
RUN make lib
RUN make install
