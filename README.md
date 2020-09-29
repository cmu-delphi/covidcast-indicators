# Covidcast Indicators

In early April 2020, Delphi developed a uniform data schema for [a new Epidata endpoint focused on COVID-19](https://cmu-delphi.github.io/delphi-epidata/api/covidcast.html). Our intent was to provide signals from which the course of the COVID-19 pandemic might be predicted using statistical models and forecasters. Delphi's long history in forecasting influenza made us uniquely situated to provide access to data streams not available anywhere else, including medical claims data, electronic medical records, lab test records, massive public surveys, and internet search trends. We also process commonly-used publicly-available data sources, both for convenience and to provide data versioning for sources that do not track revisions themselves.

Each data stream arrives in a different format using a different delivery technique, be it sftp, an access-controlled API, or an email attachment. The purpose of each pipeline in this repository is to fetch the raw source data, extract aggregate signals useful for forecasting, and output those signals in a common format for upload to the [COVIDcast API](https://cmu-delphi.github.io/delphi-epidata/api/covidcast.html).

A subset of the resulting **Real-time COVID-19 Indicators** have visualizations in the Delphi Group's [**COVIDcast** map](https://covidcast.cmu.edu).

## Organization

Utilities:
* `_delphi_utils_python` - common behaviors
* `_template_python` & `_template_r` - starting points for new data sources
* `ansible` & `jenkins` - automated testing and deployment
* `sir_complainsalot` - a Slack bot to check for failed indicator runs

Indicator pipelines: all remaining directories.

Each indicator pipeline includes its own documentation. 

* Consult README.md for directions to install, lint, test, and run the pipeline for that indicator. 
* Consult REVIEW.md for the checklist to use for code reviews. 
* Consult DETAILS.md (if present) for implementation details, including handling of corner cases.
