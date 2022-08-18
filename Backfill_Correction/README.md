Requirement for the input data

Required columns with fixed column names
- geo_value: strings or floating numbers to indicate the location
- time_value: reference date. 
- lag: the number of days between issue date and the reference date
- issue_date: issue date/report, required if lag is not available

Required columns without fixed column names
- num_col: the column for the number of reported counts of the numerator. e.g. the number of COVID claims counts according to the insurance data. 
- denom_col: the column for the number of reported counts of the denominator. e.g. the number of total claims counts according to the insurance data. Required if considering the backfill correction of ratios. 

The scripts except for tooling.R is used to create a pipeline that can help create backfill correction for specified Delphi Covidcast indicators.

The script tooling.R is used to provide a user-friendly way people to crate backfill correction for any dataset that they have in hand before we have the backfill correction officially available in `epiprocess`.
