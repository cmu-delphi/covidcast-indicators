context("Testing io helper functions")

# Constants
indicator <- "chng"
signal <- "outpatient" 
geo_level <- "state"
signal_suffix <- ""
lambda <- 0.1
lp_solver <- "gurobi"
geo <- "pa"
value_type <- "fraction"
input_dir <- "./input"
export_dir <- "./output"
training_end_date <- as.Date("2022-01-01")

 
test_that("testing exporting the output file", {
  test_data <- data.frame()
  coef_data <- data.frame()

  export_test_result(test_data, coef_data, indicator, signal, 
                     geo_level, signal_suffix, lambda,
                     training_end_date,
                     value_type, export_dir)
  prediction_file <- "./output/prediction_2022-01-01_chng_outpatient_state_lambda0.1_fraction.csv"
  coefs_file <- "./output/coefs_2022-01-01_chng_outpatient_state_lambda0.1_fraction.csv"
  expect_true(file.exists(prediction_file))
  expect_true(file.exists(coefs_file))
  
  # Remove
  file.remove(prediction_file)
  file.remove(coefs_file)
})



