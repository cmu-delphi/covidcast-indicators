FROM gurobi/optimizer:9.5.1 as gurobi

## Install R and tidyverse
FROM rocker/tidyverse:latest

WORKDIR /opt/gurobi
COPY --from=gurobi /opt/gurobi .

ENV GUROBI_HOME /opt/gurobi/linux64
ENV PATH $PATH:$GUROBI_HOME/bin
ENV LD_LIBRARY_PATH $GUROBI_HOME/lib

## Install backfill_corrections package and dependencies
# Use delphi's timezome
RUN ln -s -f /usr/share/zoneinfo/America/New_York /etc/localtime

RUN apt-get update && apt-get install -qq -y \
    libglpk-dev \
    python3.8-venv \
    python3.8-dev

RUN install2.r --error \
    roxygen2 \
    zoo \
    Rglpk \
    argparser

RUN R -e 'devtools::install_github("cmu-delphi/covidcast", ref = "evalcast", subdir = "R-packages/evalcast")'
RUN R -e 'devtools::install_github(repo="ryantibs/quantgen", subdir="quantgen")'

ADD ./delphiBackfillCorrection /backfill_corrections/delphiBackfillCorrection/
ADD ./Makefile /backfill_corrections/Makefile
ADD ./run.R /backfill_corrections/run.R
WORKDIR /backfill_corrections/
RUN make lib
RUN make install
