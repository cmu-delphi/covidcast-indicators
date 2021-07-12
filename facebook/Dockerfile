FROM rocker/tidyverse:latest

# use delphi's timezome
RUN ln -s -f /usr/share/zoneinfo/America/New_York /etc/localtime

RUN install2.r --error \
    jsonlite \
    stringr \
    stringi \
    data.table \
    roxygen2 \
    Rcpp
RUN apt-get update && apt-get install -qq -y python3-venv

ADD ./delphiFacebook /facebook/delphiFacebook/
ADD ./static /facebook/static/
ADD ./Makefile /facebook/Makefile
ADD ./run.R /facebook/run.R
ADD ./monthly-files.R /facebook/monthly-files.R
ADD ./contingency_tables.R /facebook/contingency_tables.R
ADD ./contingency-combine.R /facebook/contingency-combine.R
WORKDIR /facebook/
RUN make lib
RUN make install
