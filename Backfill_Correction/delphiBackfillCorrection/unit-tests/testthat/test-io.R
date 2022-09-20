context("Testing io helper functions")

# Constants
params <- list()
params$input_dir <- "./input"
params$taus <- c(0.01, 0.025, 0.1, 0.25, 0.5, 0.75, 0.9, 0.975, 0.99)

model_path_prefix <- "test"
geo <- "pa"
value_type <- "fraction"


 
test_that("testing exporting the output file", {
  test_data <- data.frame()
  coef_data <- data.frame()
  export_dir <- "./output"
  value_type <- "fraction"
  geo_level <- "state"
  training_end_date <- as.Date("2022-01-01'")

  export_test_result(test_data, coef_data, training_end_date, value_type, export_dir,
                     model_path_prefix)
  prediction_file <- "./output/prediction_2022-01-01_test_fraction.csv"
  coefs_file <- "./output/coefs_2022-01-01_test_fraction.csv"
  expect_true(file.exists(prediction_file))
  expect_true(file.exists(coefs_file))
  
  # Remove
  file.remove(prediction_file)
  file.remove(coefs_file)
})



