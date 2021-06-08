library(tibble)

run_facebook(relativize_params(read_params(test_path("params-test.json"))))
run_facebook(relativize_params(read_params(test_path("params-full.json"))))

aggs <- tribble(
  ~name, ~var_weight, ~metric, ~group_by, ~skip_mixing, ~compute_fn, ~post_fn,
  "reasons_tested_14d_pct", "weight", "ms_reasons_tested_14d", c("mc_age", "b_tested_14d"), FALSE, compute_multiselect, jeffreys_binary,
  "tested_pos_14d_pct", "weight", "b_tested_pos_14d", c("nation", "mc_age", "b_tested_14d"), FALSE, compute_binary, jeffreys_binary,

  "tested_pos_14d_pct_by_demos", "weight", "b_tested_pos_14d", c("state", "mc_age", "mc_race"), FALSE, compute_binary, jeffreys_binary,
  "mean_cli", "weight", "b_have_cli", c("state", "mc_age", "mc_race"), FALSE, compute_binary, jeffreys_binary,
  "comorbidity_pct_by_demos", "weight", "ms_comorbidities", c("county", "mc_race", "mc_gender"), FALSE, compute_multiselect, jeffreys_binary,

  "reasons_tested_pct", "weight", "ms_reasons_tested_14d", c("county"), FALSE, compute_multiselect, jeffreys_binary,
  "reasons_not_tested_pct_by_race", "weight", "ms_reasons_not_tested_14d", c("mc_race", "b_hispanic"), FALSE, compute_multiselect, jeffreys_binary,
  "reasons_not_tested_pct_by_age", "weight", "ms_reasons_not_tested_14d", c("mc_age"), FALSE, compute_multiselect, I,
  "reasons_not_tested_pct_by_job", "weight", "ms_reasons_not_tested_14d", c("mc_occupational_group"), FALSE, compute_multiselect, jeffreys_binary,
  "seek_medical_care_pct", "weight", "ms_medical_care", c("county"), FALSE, compute_multiselect, jeffreys_binary,
  "unusual_symptom_pct", "weight", "ms_unusual_symptoms", c("b_tested_pos_14d"), FALSE, compute_multiselect, jeffreys_binary,

  "anxiety_levels_no_groups", "weight", "mc_anxiety", c(), FALSE, compute_multiple_choice, I,
  "anxiety_levels", "weight", "mc_anxiety", c("state"), FALSE, compute_multiple_choice, I,
)

params <- relativize_params(read_contingency_params(test_path("params-test.json")))
run_contingency_tables_many_periods(params, aggs)

params <- relativize_params(read_contingency_params(test_path("params-full.json")))
run_contingency_tables_many_periods(params, aggs)
