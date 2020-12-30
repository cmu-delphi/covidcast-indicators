# Covidcast Indicators

[![License: MIT][mit-image]][mit-url]

In early April 2020, Delphi developed a uniform data schema for [a new Epidata endpoint focused on COVID-19](https://cmu-delphi.github.io/delphi-epidata/api/covidcast.html). Our intent was to provide signals that would track in real-time and in fine geographic granularity all facets of the COVID-19 pandemic, aiding both nowcasting and forecasting. Delphi's long history in tracking and forecasting influenza made us uniquely situated to provide access to data streams not available anywhere else, including medical claims data, electronic medical records, lab test records, massive public surveys, and internet search trends. We also process commonly-used publicly-available data sources, both for user convenience and to provide data versioning for sources that do not track revisions themselves.

Each data stream arrives in a different format using a different delivery technique, be it sftp, an access-controlled API, or an email attachment. The purpose of each pipeline in this repository is to fetch the raw source data, extract informative aggregate signals, and output those signals---which we call **COVID-19 indicators**---in a common format for upload to the [COVIDcast API](https://cmu-delphi.github.io/delphi-epidata/api/covidcast.html). 

For client access to the API, along with a variety of other utilities, see our [R](https://cmu-delphi.github.io/covidcast/covidcastR/) and [Python](https://cmu-delphi.github.io/covidcast/covidcast-py/html/) packages.

For interactive visualizations (of a subset of the available indicators), see our [COVIDcast map](https://covidcast.cmu.edu).

## Organization

Utilities:
* `_delphi_utils_python` - common behaviors
* `_template_python` & `_template_r` - starting points for new data sources
* `ansible` & `jenkins` - automated testing and deployment
* `sir_complainsalot` - a Slack bot to check for missing data

Indicator pipelines: all remaining directories.

Each indicator pipeline includes its own documentation. 

* Consult README.md for directions to install, lint, test, and run the pipeline for that indicator. 
* Consult REVIEW.md for the checklist to use for code reviews. 
* Consult DETAILS.md (if present) for implementation details, including handling of corner cases.


## License

This repository is released under the **MIT License**.

[mit-image]: https://img.shields.io/badge/License-MIT-yellow.svg
[mit-url]: https://opensource.org/licenses/MIT
