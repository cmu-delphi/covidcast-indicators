library(arrow)

context("Testing io helper functions")

# Constants
indicator <- "chng"
signal <- "outpatient" 
geo_level <- "state"
signal_suffix <- ""
lambda <- 0.1
geo <- "pa"
value_type <- "fraction"
date_format = "%Y%m%d"
training_end_date <- as.Date("2022-01-01")

create_dir_not_exist("./input")
create_dir_not_exist("./output")
create_dir_not_exist("./cache")
 
test_that("testing exporting the output file", {
  params <- read_params("params-run.json", "params-run.json.template")

  test_data <- data.frame(test=TRUE)
  coef_data <- data.frame(test=TRUE)
  
  export_test_result(test_data, coef_data, indicator, signal, 
                     geo_level, geo="", signal_suffix, lambda,
                     training_end_date,
                     value_type, params$export_dir)
  prediction_file <- file.path(params$export_dir, "prediction_2022-01-01_chng_outpatient_state_lambda0.1_fraction.csv.gz")
  coefs_file <- file.path(params$export_dir, "coefs_2022-01-01_chng_outpatient_state_lambda0.1_fraction.csv.gz")

  expect_true(file.exists(prediction_file))
  expect_true(file.exists(coefs_file))
  
  # Remove
  file.remove(prediction_file)
  file.remove(coefs_file)
  file.remove("params-run.json")
})


test_that("testing creating file name pattern", {
  params <- read_params("params-run.json", "params-run.json.template")

  daily_pattern <- create_name_pattern(indicator, signal, "daily")
  rollup_pattern <- create_name_pattern(indicator, signal, "rollup")
  
  # Create test files
  daily_data <- data.frame(test=TRUE)
  daily_file_name <- file.path(params$input_dir,
                               str_interp("chng_outpatient_as_of_${format(TODAY-5, date_format)}.parquet"))
  write_parquet(daily_data, daily_file_name)
  
  rollup_file_name <- file.path(params$input_dir,
                                str_interp("chng_outpatient_from_${format(TODAY-15, date_format)}_to_${format(TODAY, date_format)}.parquet"))
  rollup_data <- data.frame(test=TRUE)
  write_parquet(rollup_data, rollup_file_name)
  
  
  filtered_daily_file <- list.files(
    params$input_dir, pattern = daily_pattern, full.names = TRUE)
  expect_equal(filtered_daily_file, daily_file_name)
  
  filtered_rollup_file <- list.files(
    params$input_dir, pattern = rollup_pattern, full.names = TRUE)
  expect_equal(filtered_rollup_file, rollup_file_name)
  
  file.remove(daily_file_name)
  file.remove(rollup_file_name)
  file.remove("params-run.json")
})


test_that("testing the filtration of the files for training and predicting", {
  params <- read_params("params-run.json", "params-run.json.template")

  daily_files_list <- c(file.path(params$input_dir, str_interp("chng_outpatient_as_of_${format(TODAY-15, date_format)}.parquet")),
                        file.path(params$input_dir, str_interp("chng_outpatient_as_of_${format(TODAY-5, date_format)}.parquet")),
                        file.path(params$input_dir, str_interp("chng_outpatient_as_of_${format(TODAY, date_format)}.parquet")))
  daily_valid_files <- subset_valid_files(daily_files_list, "daily", params)
  expect_equal(daily_valid_files, daily_files_list[2])
  
  rollup_files_list <- c(file.path(params$input_dir, str_interp(
    "chng_outpatient_from_${format(TODAY-15, date_format)}_to_${format(TODAY-11, date_format)}.parquet")),
    file.path(params$input_dir, str_interp(
    "chng_outpatient_from_${format(TODAY-15, date_format)}_to_${format(TODAY, date_format)}.parquet")),
    file.path(params$input_dir, str_interp(
      "chng_outpatient_from_${format(TODAY, date_format)}_to_${format(TODAY+3, date_format)}.parquet")))
  rollup_valid_files <- subset_valid_files(rollup_files_list, "rollup", params)
  expect_equal(rollup_valid_files, rollup_files_list[2])

  file.remove("params-run.json")
})

test_that("testing fetching list of files for training and predicting", {
  params <- read_params("params-run.json", "params-run.json.template")

  daily_data <- data.frame(test=TRUE)
  daily_file_name <- file.path(params$input_dir,
                               str_interp("chng_outpatient_as_of_${format(TODAY-5, date_format)}.parquet"))
  write_parquet(daily_data, daily_file_name)
  
  rollup_file_name <- file.path(params$input_dir,
                                str_interp("chng_outpatient_from_${format(TODAY-15, date_format)}_to_${format(TODAY, date_format)}.parquet"))
  rollup_data <- data.frame(test=TRUE)
  write_parquet(rollup_data, rollup_file_name)
  
  
  files <- get_files_list(indicator, signal, params)
  expect_true(all(files == c(daily_file_name, rollup_file_name)))
  
  file.remove(daily_file_name)
  file.remove(rollup_file_name)
  file.remove("params-run.json")
})


