library(tibble)
library(delphiFacebook)

#Rprof(interval = 0.005)
params <- read_params("params.json", "contingency_params.json.template")


# User should add additional desired aggregations here following existing 
# format. Names should be unique. Listing no groupby vars will implicitly
# compute aggregations at the national level.
# 
# Compute functions must be one of the `compute_*` set (or another function 
# with similar format can be created).
aggs <- tribble(
  ~name, ~var_weight, ~metric, ~group_by, ~skip_mixing, ~compute_fn, ~post_fn,
  "reasons_tested_14d_freq", "weight", "ms_reasons_tested_14d", c("mc_age", "b_tested_14d"), FALSE, compute_prop, jeffreys_binary,
  "tested_pos_14d_freq", "weight", "b_tested_pos_14d", c("national", "mc_age", "b_tested_14d"), FALSE, compute_prop, jeffreys_binary,
  "hh_members_mean", "weight", "n_hh_num_total", c("state"), FALSE, compute_mean, jeffreys_count,
  
  "tested_pos_14d_freq_by_demos", "weight", "b_tested_pos_14d", c("state", "mc_age", "mc_race"), FALSE, compute_prop, jeffreys_binary,
  "mean_cli", "weight", "b_have_cli", c("state", "mc_age", "mc_race"), FALSE, compute_prop, jeffreys_binary,
  "comorbidity_freq_by_demos", "weight", "ms_comorbidities", c("county", "mc_race", "mc_gender"), FALSE, compute_prop, jeffreys_binary,
  
  "reasons_tested_freq", "weight", "ms_reasons_tested_14d", c("county"), FALSE, compute_prop, jeffreys_binary,
  "reasons_not_tested_freq_by_race", "weight", "ms_reasons_not_tested_14d", c("mc_race", "b_hispanic"), FALSE, compute_prop, jeffreys_binary,
  "reasons_not_tested_freq_by_age", "weight", "ms_reasons_not_tested_14d", c("mc_age"), FALSE, compute_prop, I,
  "reasons_not_tested_freq_by_job", "weight", "ms_reasons_not_tested_14d", c("mc_occupational_group"), FALSE, compute_prop, jeffreys_binary,
  "seek_medical_care_freq", "weight", "ms_medical_care", c("county"), FALSE, compute_prop, jeffreys_binary,
  "unusual_symptom_freq", "weight", "ms_unusual_symptoms", c("b_tested_pos_14d"), FALSE, compute_prop, jeffreys_binary,
  
  "anxiety_levels_no_groups", "weight", "mc_anxiety", c(), FALSE, compute_count, I,
  "anxiety_levels", "weight", "mc_anxiety", c("state"), FALSE, compute_count, I,
)


run_contingency_tables(params, aggs)
