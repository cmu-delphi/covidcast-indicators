#### TODO
# - set up to be able to aggregate multiple time periods in series?
#   wrapper function that modifies params.json more likely. Or something like, if
#   want to span multiple time periods, date window is added as a grouping var
# - map response codes to descriptive values? Would need mapping for every
#   individual question
# - How to calculate effective sample size/count respondents per response? Sum
#   of original weights? Butcount.R::line 44 does something completely different
# - Remove dependence on "name" column of `aggregations` so aggregations not sharing
#   grouping variables can have the same name (e.g. might want to report pct_race
#   by state and by county)

library(tibble)
library(delphiFacebook)

# User should add additional desired aggregations here following existing
# format. Names should be unique. Listing no groupby vars will implicitly
# compute aggregations at the national level only.
#
# Each row represents one aggregate to report. `name` is the aggregate's base
# column name; `var_weight` is the column to use for its weights. `metric` is
# the column of `df` containing the response value. `group_by` is a list of
# variables used to perform the aggregations over. `compute_fn` is the function
# that computes the aggregate response given many rows of data. `post_fn` is
# applied to the aggregate data and can perform any final calculations
# necessary.
#
# Compute functions must be one of the `compute_*` set (or another function with
# similar format can be created). Post-processing functions should be one of the
# `jeffreys_*` set or the identity `I`, which does not modify the data.
aggs <- tribble(
  ~name, ~metric, ~group_by, ~compute_fn, ~post_fn,
  "pct_accept_cov_vaccine_1", "mc_accept_cov_vaccine", c("mc_race"), compute_multiple_choice, I,
  "pct_accept_cov_vaccine_2", "mc_accept_cov_vaccine", c("mc_age"), compute_multiple_choice, I,

  # "reasons_tested_14d_pct", "ms_reasons_tested_14d", c("mc_age", "b_tested_14d"), compute_binary_and_multiselect, jeffreys_binary,
  # "tested_pos_14d_pct", "b_tested_pos_14d", c("nation", "mc_age", "b_tested_14d"), compute_binary_and_multiselect, jeffreys_binary,
  # "hh_members_mean", "n_hh_num_total", c("state"), compute_numeric, jeffreys_count,
  #
  # "tested_pos_14d_pct_by_demos", "b_tested_pos_14d", c("state", "mc_age", "mc_race"), compute_binary_and_multiselect, jeffreys_binary,
  # "mean_cli", "b_have_cli", c("state", "mc_age", "mc_race"), compute_binary_and_multiselect, jeffreys_binary,
  # "comorbidity_pct_by_demos", "ms_comorbidities", c("county", "mc_race", "mc_gender"), compute_binary_and_multiselect, jeffreys_binary,
  #
  # "reasons_tested_pct", "ms_reasons_tested_14d", c("county"), compute_binary_and_multiselect, jeffreys_binary,
  # "reasons_not_tested_pct_by_race", "ms_reasons_not_tested_14d", c("mc_race", "b_hispanic"), compute_binary_and_multiselect, jeffreys_binary,
  # "reasons_not_tested_pct_by_age", "ms_reasons_not_tested_14d", c("mc_age"), compute_binary_and_multiselect, I,
  # "reasons_not_tested_pct_by_job", "ms_reasons_not_tested_14d", c("mc_occupational_group"), compute_binary_and_multiselect, jeffreys_binary,
  # "seek_medical_care_pct", "ms_medical_care", c("county"), compute_binary_and_multiselect, jeffreys_binary,
  # "unusual_symptom_pct", "ms_unusual_symptoms", c("b_tested_pos_14d"), compute_binary_and_multiselect, jeffreys_binary,
  #
  # "anxiety_levels_no_groups", "mc_anxiety", c(), compute_multiple_choice, I,
  # "anxiety_levels", "mc_anxiety", c("state"), compute_multiple_choice, I,
)

#Rprof(interval = 0.005)
params <- read_params("contingency_params.json", "contingency_params.json.template")
run_contingency_tables(params, aggs)
