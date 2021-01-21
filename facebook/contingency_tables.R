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
# `jeffreys_*` set or the identity `I`, which does not modify the data.


## Facebook aggregates
weekly_aggs <- tribble(
  ~name, ~metric, ~group_by, ~compute_fn, ~post_fn,
  ## V9 (concerened about side effects)
  # National
  "pct_concerned_sideeffects", "b_concerned_sideeffects", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary_and_multiselect, jeffreys_binary,
  
  # State
  "pct_concerned_sideeffects", "b_concerned_sideeffects", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary_and_multiselect, jeffreys_binary,
  
  # State marginal
  "pct_concerned_sideeffects", "b_concerned_sideeffects", c("mc_age", "state"), compute_binary_and_multiselect, jeffreys_binary,
  "pct_concerned_sideeffects", "b_concerned_sideeffects", c("mc_gender", "state"), compute_binary_and_multiselect, jeffreys_binary,
  "pct_concerned_sideeffects", "b_concerned_sideeffects", c("mc_race", "b_hispanic", "state"), compute_binary_and_multiselect, jeffreys_binary,

  
  
  ## V9 given V3 (worried about side effects given not willing to get vaccine)
  # National
  "pct_hesitant_sideeffects", "b_concerned_sideeffects", c("b_hesitant_cov_vaccine", "mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary_and_multiselect, jeffreys_binary,
  
  # State
  "pct_hesitant_sideeffects", "b_concerned_sideeffects", c("b_hesitant_cov_vaccine", "mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary_and_multiselect, jeffreys_binary,
  
  # State marginal
  "pct_hesitant_sideeffects", "b_concerned_sideeffects", c("b_hesitant_cov_vaccine", "mc_age", "state"), compute_binary_and_multiselect, jeffreys_binary,
  "pct_hesitant_sideeffects", "b_concerned_sideeffects", c("b_hesitant_cov_vaccine", "mc_gender", "state"), compute_binary_and_multiselect, jeffreys_binary,
  "pct_hesitant_sideeffects", "b_concerned_sideeffects", c("b_hesitant_cov_vaccine", "mc_race", "b_hispanic", "state"), compute_binary_and_multiselect, jeffreys_binary,
  
)

monthly_aggs <- tribble(
  ~name, ~metric, ~group_by, ~compute_fn, ~post_fn,
)


# ## CDC aggregates
# weekly_aggs <- tribble(
#   ~name, ~metric, ~group_by, ~compute_fn, ~post_fn,
#   ## V3 (accept COVID vaccine)
#   # by age
#   "freq_accept_cov_vaccine", "mc_accept_cov_vaccine", c("mc_age", "nation"), compute_multiple_choice, I,
#   
#   # by age and gender
#   "freq_accept_cov_vaccine", "mc_accept_cov_vaccine", c("mc_age", "mc_gender", "nation"), compute_multiple_choice, I,
# 
#   # by age, race, and ethnicity
#   "freq_accept_cov_vaccine", "mc_accept_cov_vaccine", c("mc_age", "mc_race", "b_hispanic", "nation"), compute_multiple_choice, I,
#  
#   # by age and comorbidities
#   "freq_accept_cov_vaccine", "mc_accept_cov_vaccine", c("mc_age", "ms_comorbidities", "nation"), compute_multiple_choice, I,
# 
#   ## V1 (had COVID vaccine)
#   # by age
#   "pct_had_cov_vaccine", "b_had_cov_vaccine", c("mc_age", "nation"), compute_binary_and_multiselect, jeffreys_binary,
# 
#   # by age and gender
#   "pct_had_cov_vaccine", "b_had_cov_vaccine", c("mc_age", "mc_gender", "nation"), compute_binary_and_multiselect, jeffreys_binary,
# 
#   # by age, race, and ethnicity
#   "pct_had_cov_vaccine", "b_had_cov_vaccine", c("mc_age", "mc_race", "b_hispanic", "nation"), compute_binary_and_multiselect, jeffreys_binary,
# 
#   # by age and comorbidities
#   "pct_had_cov_vaccine", "b_had_cov_vaccine", c("mc_age", "ms_comorbidities", "nation"), compute_binary_and_multiselect, jeffreys_binary,
# )
# 
# monthly_aggs <- tribble(
#   ~name, ~metric, ~group_by, ~compute_fn, ~post_fn,
#   ## V3 (accept COVID vaccine)
#   # by age
#   "freq_accept_cov_vaccine", "mc_accept_cov_vaccine", c("mc_age", "state"), compute_multiple_choice, I,
#   "freq_accept_cov_vaccine", "mc_accept_cov_vaccine", c("mc_age", "msa"), compute_multiple_choice, I,
#   
#   # by age and gender
#   "freq_accept_cov_vaccine", "mc_accept_cov_vaccine", c("mc_age", "mc_gender", "state"), compute_multiple_choice, I,
#   "freq_accept_cov_vaccine", "mc_accept_cov_vaccine", c("mc_age", "mc_gender", "msa"), compute_multiple_choice, I,
#   
#   # by age, race, and ethnicity
#   "freq_accept_cov_vaccine", "mc_accept_cov_vaccine", c("mc_age", "mc_race", "b_hispanic", "state"), compute_multiple_choice, I,
#   "freq_accept_cov_vaccine", "mc_accept_cov_vaccine", c("mc_age", "mc_race", "b_hispanic", "msa"), compute_multiple_choice, I,
#   
#   # by age and comorbidities
#   "freq_accept_cov_vaccine", "mc_accept_cov_vaccine", c("mc_age", "ms_comorbidities", "state"), compute_multiple_choice, I,
#   
#   ## V1 (had COVID vaccine)
#   # by age
#   "pct_had_cov_vaccine", "b_had_cov_vaccine", c("mc_age", "state"), compute_binary_and_multiselect, jeffreys_binary,
#   "pct_had_cov_vaccine", "b_had_cov_vaccine", c("mc_age", "msa"), compute_binary_and_multiselect, jeffreys_binary,
#   
#   # by age and gender
#   "pct_had_cov_vaccine", "b_had_cov_vaccine", c("mc_age", "mc_gender", "state"), compute_binary_and_multiselect, jeffreys_binary,
#   "pct_had_cov_vaccine", "b_had_cov_vaccine", c("mc_age", "mc_gender", "msa"), compute_binary_and_multiselect, jeffreys_binary,
#   
#   # by age, race, and ethnicity
#   "pct_had_cov_vaccine", "b_had_cov_vaccine", c("mc_age", "mc_race", "b_hispanic", "state"), compute_binary_and_multiselect, jeffreys_binary,
#   "pct_had_cov_vaccine", "b_had_cov_vaccine", c("mc_age", "mc_race", "b_hispanic", "msa"), compute_binary_and_multiselect, jeffreys_binary,
#   
#   # by age and comorbidities
#   "pct_had_cov_vaccine", "b_had_cov_vaccine", c("mc_age", "ms_comorbidities", "state"), compute_binary_and_multiselect, jeffreys_binary,
# )

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

