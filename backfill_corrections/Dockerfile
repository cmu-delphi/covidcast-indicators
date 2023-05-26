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
    libglpk-dev\
    python3-venv \
    python3-dev

RUN python3 -m pip install --upgrade pip==22.0.3 

RUN install2.r --error \
    roxygen2 \
    Rglpk \
    argparser

RUN --mount=type=secret,id=GITHUB_TOKEN \
    export GITHUB_PAT="$(cat /run/secrets/GITHUB_TOKEN)" && \
    R -e 'devtools::install_version("bettermc", version = "1.1.2")' && \
    R -e 'devtools::install_github("cmu-delphi/covidcast", ref = "evalcast", subdir = "R-packages/evalcast")' && \
    R -e 'devtools::install_github(repo="ryantibs/quantgen", subdir="quantgen")' && \
    R -e 'install.packages(list.files(path="/opt/gurobi/linux64/R/", pattern="^gurobi_.*[.]tar[.]gz$", full.names = TRUE), repos=NULL)'

WORKDIR /backfill_corrections/
ADD ./delphiBackfillCorrection ./delphiBackfillCorrection/
ADD [ "Makefile", "run.R", "./" ]

RUN make lib && make install
