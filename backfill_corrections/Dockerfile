FROM rocker/tidyverse:latest

# use delphi's timezome
RUN ln -s -f /usr/share/zoneinfo/America/New_York /etc/localtime

RUN apt-get update && apt-get install -qq -y \
    libglpk-dev

RUN install2.r --error \
    roxygen2 \
    zoo \
    Rglpk

RUN R -e 'devtools::install_github("cmu-delphi/covidcast", ref = "evalcast", subdir = "R-packages/evalcast")'
RUN R -e 'devtools::install_github(repo="ryantibs/quantgen", subdir="quantgen")'

ADD ./delphiBackfillCorrection /backfill_corrections/delphiBackfillCorrection/
ADD ./Makefile /backfill_corrections/Makefile
ADD ./run.R /backfill_corrections/run.R
WORKDIR /backfill_corrections/
RUN make lib
RUN make install
