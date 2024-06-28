FROM gurobi/optimizer:9.5.1 as gurobi

## Install R and tidyverse
FROM rocker/tidyverse:4.2

WORKDIR /opt/gurobi
COPY --from=gurobi /opt/gurobi .

ENV GUROBI_HOME /opt/gurobi/linux64
ENV PATH $PATH:$GUROBI_HOME/bin
ENV LD_LIBRARY_PATH $GUROBI_HOME/lib

## Install backfill_corrections package and dependencies
# Use delphi's timezome
RUN ln -s -f /usr/share/zoneinfo/America/New_York /etc/localtime

RUN apt-get update && apt-get install -qq -y \
    apt-file \
    python3-venv \
    python3-dev \
    python3-pip

RUN R -e 'install.packages("pak", repos = sprintf("https://r-lib.github.io/p/pak/stable/%s/%s/%s", .Platform$pkgType, R.Version()$os, R.Version()$arch))'
RUN R -e 'install.packages(c("rspm"))'
RUN --mount=type=secret,id=GITHUB_TOKEN \
    export GITHUB_PAT="$(cat /run/secrets/GITHUB_TOKEN)" && \
    R -e 'rspm::enable(); pak::pkg_install(c("roxygen2", "Rglpk", "argparser", "gfkse/bettermc@v1.1.2", "cmu-delphi/covidcast/R-packages/evalcast@evalcast", "ryantibs/quantgen/quantgen"))'
RUN R -e 'install.packages(list.files(path="/opt/gurobi/linux64/R/", pattern="^gurobi_.*[.]tar[.]gz$", full.names = TRUE), repos=NULL)'

WORKDIR /backfill_corrections/
ADD ./delphiBackfillCorrection ./delphiBackfillCorrection/
ADD [ "Makefile", "run.R", "./" ]

RUN make lib && make install
