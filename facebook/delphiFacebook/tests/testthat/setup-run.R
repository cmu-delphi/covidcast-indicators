library(tibble)
library(mockr)

mock_hh_count_indicators <- function() {
  ind <- tribble(
    ~name, ~var_weight, ~metric, ~smooth_days, ~compute_fn, ~post_fn,
    "raw_cli", "weight_unif", "hh_p_cli", 0, compute_count_response, jeffreys_count,
    "raw_ili", "weight_unif", "hh_p_ili", 0, compute_count_response, jeffreys_count,
    "raw_wcli", "weight", "hh_p_cli", 0, compute_count_response, jeffreys_count,
    "raw_wili", "weight", "hh_p_ili", 0, compute_count_response, jeffreys_count,
    
    "smoothed_cli", "weight_unif", "hh_p_cli", 6, compute_count_response, jeffreys_count,
    "smoothed_ili", "weight_unif", "hh_p_ili", 6, compute_count_response, jeffreys_count,
    "smoothed_wcli", "weight", "hh_p_cli", 6, compute_count_response, jeffreys_count,
    "smoothed_wili", "weight", "hh_p_ili", 6, compute_count_response, jeffreys_count
  )

  ind$skip_mixing <- FALSE

  return(ind)
}

mock_binary_indicators <- function() {
  ind <- tribble(
    ~name, ~var_weight, ~metric, ~smooth_days, ~compute_fn, ~post_fn,
    "raw_hh_cmnty_cli", "weight_unif", "hh_community_yes", 0, compute_binary_response, jeffreys_binary,
    "raw_nohh_cmnty_cli", "weight_unif", "community_yes", 0, compute_binary_response, jeffreys_binary,
    "raw_whh_cmnty_cli", "weight", "hh_community_yes", 0, compute_binary_response, jeffreys_binary,
    "raw_wnohh_cmnty_cli", "weight", "community_yes", 0, compute_binary_response, jeffreys_binary,
    
    "smoothed_hh_cmnty_cli", "weight_unif", "hh_community_yes", 6, compute_binary_response, jeffreys_binary,
    "smoothed_nohh_cmnty_cli", "weight_unif", "community_yes", 6, compute_binary_response, jeffreys_binary,
    "smoothed_whh_cmnty_cli", "weight", "hh_community_yes", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wnohh_cmnty_cli", "weight", "community_yes", 6, compute_binary_response, jeffreys_binary,
    
    "smoothed_travel_outside_state_5d", "weight_unif", "c_travel_state", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wtravel_outside_state_5d", "weight", "c_travel_state", 6, compute_binary_response, jeffreys_binary,
    
    "wip_smoothed_work_outside_home_5d", "weight_unif", "c_work_outside_5d", 6, compute_binary_response, jeffreys_binary,
    "wip_smoothed_wwork_outside_home_5d", "weight", "c_work_outside_5d", 6, compute_binary_response, jeffreys_binary
  )

  ind$skip_mixing <- TRUE

  return(ind)
}

run_api_pipeline_with_mock <- function(params_path) {
  local_mock("delphiFacebook::get_binary_indicators" = mock_binary_indicators)
  local_mock("delphiFacebook::get_hh_count_indicators" = mock_hh_count_indicators)
  run_facebook(relativize_params(read_params(test_path(params_path))))
}

run_api_pipeline_with_mock("params-test.json")
run_api_pipeline_with_mock("params-full.json")

aggs <- tribble(
  ~name, ~metric, ~group_by, ~compute_fn, ~post_fn,
  "freq_anxiety", "mh_anxious", c("gender"), compute_binary, I,
  "pct_hh_fever", "hh_fever", c("gender"), compute_binary, I,
  "pct_heartdisease", "comorbidheartdisease", c("gender"), compute_binary, I
)

params <- relativize_params(read_contingency_params(test_path("params-test.json")))
run_contingency_tables_many_periods(params, aggs)

params <- relativize_params(read_contingency_params(test_path("params-full.json")))
run_contingency_tables_many_periods(params, aggs)
