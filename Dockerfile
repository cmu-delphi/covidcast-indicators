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

ADD ./facebook/delphiFacebook /facebook/delphiFacebook/
ADD ./facebook/Makefile /facebook/Makefile
WORKDIR /facebook/
RUN make lib
RUN make install
