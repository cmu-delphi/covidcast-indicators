library(tibble)

run_facebook(relativize_params(read_params(test_path("params-test.json"))))
run_facebook(relativize_params(read_params(test_path("params-full.json"))))

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
