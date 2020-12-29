### As with `test-contingency-gold.R`, this test file relies on `setup-run.R` 
### to run the pipeline. This test checks the configuration listed in 
### `params-contingency-test.json`, which loads `input/responses.csv`, a small 
### selected subset of test responses.

context("Testing the run_contingency_tables function on a small dataset")

test_that("testing lack of existence of csv files", {
  # Since the contingency tables tool removes aggregates that pose a privacy
  # risk, the small test dataset should produce no aggregates at all.
  expected_files <- c()
  actual_files <- dir(test_path("receiving_contingency_test"))
  
  expect_setequal(expected_files, actual_files)
})