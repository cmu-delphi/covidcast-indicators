# Validation for _delphi_utils
This directory is used for one-time validations that would be too expensive 
or wouldn't make sense to be in tests. 

## Check_covidcast_port
### Background
We were previously using [covidcast](https://github.com/cmu-delphi/covidcast) to grab signal and metadata that was not optimized for historical reasons.
 We replaced with [Epidata](https://github.com/cmu-delphi/delphi-epidata) which is what covidcast runs under the hood anyway. To ensure that the results make this script was created to validate the results.

###  Instructions to Run
- requires an API key from Epidata [link](https://cmu-delphi.github.io/delphi-epidata/api/api_keys.html)
- export the api key as an environment variable
- set up virtual environment 
