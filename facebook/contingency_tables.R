#### TODO
# - set up to be able to aggregate multiple time periods in series?
#   wrapper function that modifies params.json more likely. Or something like, if
#   want to span multiple time periods, date window is added as a grouping var
# - map response codes to descriptive values? Would need mapping for every
#   individual question 
# - How to calculate effective
#   sample size/count respondents per response? Sum of original weights? But
#   count.R::line 44 does something completely different

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
  ~name, ~var_weight, ~metric, ~group_by, ~skip_mixing, ~compute_fn, ~post_fn,
  "reasons_tested_14d_pct", "weight", "ms_reasons_tested_14d", c("mc_age", "b_tested_14d"), FALSE, compute_binary_and_multiselect, jeffreys_binary,
  "tested_pos_14d_pct", "weight", "b_tested_pos_14d", c("nation", "mc_age", "b_tested_14d"), FALSE, compute_binary_and_multiselect, jeffreys_binary,
  "hh_members_mean", "weight", "n_hh_num_total", c("state"), FALSE, compute_numeric, jeffreys_count,

  "tested_pos_14d_pct_by_demos", "weight", "b_tested_pos_14d", c("state", "mc_age", "mc_race"), FALSE, compute_binary_and_multiselect, jeffreys_binary,
  "mean_cli", "weight", "b_have_cli", c("state", "mc_age", "mc_race"), FALSE, compute_binary_and_multiselect, jeffreys_binary,
  "comorbidity_pct_by_demos", "weight", "ms_comorbidities", c("county", "mc_race", "mc_gender"), FALSE, compute_binary_and_multiselect, jeffreys_binary,

  "reasons_tested_pct", "weight", "ms_reasons_tested_14d", c("county"), FALSE, compute_binary_and_multiselect, jeffreys_binary,
  "reasons_not_tested_pct_by_race", "weight", "ms_reasons_not_tested_14d", c("mc_race", "b_hispanic"), FALSE, compute_binary_and_multiselect, jeffreys_binary,
  "reasons_not_tested_pct_by_age", "weight", "ms_reasons_not_tested_14d", c("mc_age"), FALSE, compute_binary_and_multiselect, I,
  "reasons_not_tested_pct_by_job", "weight", "ms_reasons_not_tested_14d", c("mc_occupational_group"), FALSE, compute_binary_and_multiselect, jeffreys_binary,
  "seek_medical_care_pct", "weight", "ms_medical_care", c("county"), FALSE, compute_binary_and_multiselect, jeffreys_binary,
  "unusual_symptom_pct", "weight", "ms_unusual_symptoms", c("b_tested_pos_14d"), FALSE, compute_binary_and_multiselect, jeffreys_binary,

  "anxiety_levels_no_groups", "weight", "mc_anxiety", c(), FALSE, compute_multiple_choice, I,
  "anxiety_levels", "weight", "mc_anxiety", c("state"), FALSE, compute_multiple_choice, I,
)

#Rprof(interval = 0.005)
params <- read_params("contingency_params.json", "contingency_params.json.template")
run_contingency_tables(params, aggs)
