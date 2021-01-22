library(tibble)
library(delphiFacebook)

# User should add additional desired aggregations here following existing
# format. Names should be descriptive. Listing no groupby vars will implicitly
# compute aggregations at the national level only. Aggregations will not be
# reported if one or more of the metric or grouping variables is missing.
#
# Each row represents one aggregate to report. `name` is the aggregate's base
# column name.`metric` is the column of `df` containing the response value.
# `group_by` is a list of variables used to perform the aggregations over.
# `compute_fn` is the function that computes the aggregate response given many
# rows of data. `post_fn` is applied to the aggregate data and can perform any
# final calculations necessary.
#
# Please verify that any multiple choice and multi-select questions used in the
# aggregations are reformatted to be descriptive in
# `contingency_variables::reformat_responses`.
#
# Compute functions must be one of the `compute_*` set (or another function with
# similar format can be created). Post-processing functions should be one of the
# `jeffreys_*` set or post_convert_count_to_pct or the identity `I`, which does 
# not modify the data.


## Facebook aggregates
weekly_aggs <- tribble(
  ~name, ~metric, ~group_by, ~compute_fn, ~post_fn,
  "freq", "mc_simple_education", c("b_25_or_older", "mc_simple_race", "b_hispanic", "nation"), compute_multiple_choice, post_convert_count_to_pct,
  "freq", "mc_simple_education", c("b_25_or_older", "mc_simple_race", "b_hispanic", "state"), compute_multiple_choice, post_convert_count_to_pct,
)


monthly_aggs <- tribble(
  ~name, ~metric, ~group_by, ~compute_fn, ~post_fn,
)


params <- read_params("contingency_params.json", "contingency_params.json.template")

if (params$aggregate_range == "week") {
  run_contingency_tables(params, weekly_aggs)
} else if (params$aggregate_range == "month") {
  run_contingency_tables(params, monthly_aggs)
} else if (params$aggregate_range == "both") {
  params$aggregate_range <- "week"
  run_contingency_tables(params, weekly_aggs)
  
  params$aggregate_range <- "month"
  run_contingency_tables(params, monthly_aggs)
}

