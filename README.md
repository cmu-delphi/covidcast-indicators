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

## Development

`prod` reflects what is currently in production. `main` is the staging branch for the next release.

1. Branch from `main` to develop a new change
2. PR into `main` and assign a reviewer (or tag someone) to get feedback on your change. List the issue number under `Fixes` if your change resolves an existing [GitHub Issue](https://github.com/cmu-delphi/covidcast-indicators/issues).
3. Add new commits to your branch in response to feedback.
4. When approved, tag an admin to merge the PR. Let them know if this change should be released immediately, at a set future date, or if it can just go along for the ride whenever the next release happens.

## Release Process

The release process consists of multiple steps which can all be done via the GitHub website:

1. Go to [create_release GitHub Action](https://github.com/cmu-delphi/covidcast-indicators/actions/workflows/create-release.yml) and click the `Run workflow` dropdown button. Leave branch as `main` unless you know what you're doing. Enter the type of release (patch: bugfixes, params file changes, new signals for existing indicators; minor: new indicators, new utilities; major: backwards-incompatible changes requiring substantial refactoring) and GitHub will automatically compute the next version number for you; alternately, specify the version number by hand. Hit the green `Run workflow` button.
2. The action will prepare a new release and generate an associated [Pull Request](https://github.com/cmu-delphi/covidcast-indicators/pulls).
3. Edit the PR description and **list all pull requests included in this release**. This is a manual step to make sure you are aware of 100% of the changes that will be deployed. You can use `#xxx` notation and GitHub will automatically render the title of each PR in Preview mode and when the edit is saved.
4. Verify that CI passes for the PR as a whole and for the most-recent/bottom-most commit in the PR. We're currently having problems where [python-ci does not run on release PRs](https://github.com/cmu-delphi/covidcast-indicators/issues/1310), but if you see a green check next to the most-recent commit you should be fine.
5. Approve the PR, merge it, and delete the branch.
6. Jenkins will automatically deploy the most-recently-built indicator packages to the pipeline servers
7. Another GitHub action will automatically
   1. Create a git tag
   2. Create another [Pull Request](https://github.com/cmu-delphi/covidcast-indicators/pulls) to merge the changes back into the `main` branch
   3. (if `delphi-utils` was updated) Upload the new version of `delphi-utils` to PyPI
8. Approve the sync PR, merge it, and delete the branch
9. Done

You may need to be an admin to perform some of the steps above.

## License

This repository is released under the **MIT License**.

[mit-image]: https://img.shields.io/badge/License-MIT-yellow.svg
[mit-url]: https://opensource.org/licenses/MIT
