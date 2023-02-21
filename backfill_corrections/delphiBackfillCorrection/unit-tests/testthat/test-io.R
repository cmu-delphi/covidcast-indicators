library(arrow)
library(stringr)

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
training_days <- 7
training_end_date <- as.Date("2022-01-01")
training_start_date <- training_end_date - training_days

create_dir_not_exist("./input")
create_dir_not_exist("./output")
create_dir_not_exist("./cache")
 
test_that("testing exporting the output file", {
  params <- read_params("params-run.json", "params-run.json.template")
  
  expected_col <- c("time_value", "issue_date", "lag", "geo_value", 
                    "target_date", "wis", "predicted_tau0.5")
  test_data <- data.frame(test=TRUE)
  test_data[expected_col] = TRUE
  coef_data <- data.frame(test=TRUE)
  
  components <- c(indicator, signal, signal_suffix)
  signal_dir <- paste(components[components != ""], collapse="_")
  
  export_test_result(test_data, coef_data, indicator, signal, 
                     geo_level, signal_suffix, lambda,
                     training_end_date, training_start_date,
                     value_type, params$export_dir)
  prediction_file <- file.path(params$export_dir, signal_dir,
                               "prediction_20220101_20211225_chng_outpatient_state_lambda0.1_fraction.csv.gz")
  coefs_file <- file.path(params$export_dir, signal_dir,
                          "coefs_20220101_20211225_chng_outpatient_state_lambda0.1_fraction.csv.gz")

  expect_true(file.exists(prediction_file))
  expect_true(file.exists(coefs_file))
  
  # Remove
  unlink(file.path(params$export_dir, signal_dir),recursive = TRUE)
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
  params$train_models <- TRUE

  daily_files_list <- c(file.path(params$input_dir, "chng_outpatient_as_of_20200202.parquet"),
                        file.path(params$input_dir, str_interp("chng_outpatient_as_of_${format(TODAY-15, date_format)}.parquet")),
                        file.path(params$input_dir, str_interp("chng_outpatient_as_of_${format(TODAY-5, date_format)}.parquet")),
                        file.path(params$input_dir, str_interp("chng_outpatient_as_of_${format(TODAY, date_format)}.parquet")))
  daily_valid_files <- subset_valid_files(daily_files_list, "daily", params)
  expect_equal(daily_valid_files, daily_files_list[2:4])
  
  rollup_files_list <- c(file.path(params$input_dir, str_interp(
    "chng_outpatient_from_${format(TODAY-15, date_format)}_to_${format(TODAY-11, date_format)}.parquet")),
    file.path(params$input_dir, str_interp(
    "chng_outpatient_from_${format(TODAY-15, date_format)}_to_${format(TODAY, date_format)}.parquet")),
    file.path(params$input_dir, str_interp(
      "chng_outpatient_from_${format(TODAY, date_format)}_to_${format(TODAY+3, date_format)}.parquet")),
    file.path(params$input_dir, "chng_outpatient_from_20200202_to_20210304.parquet"))
  rollup_valid_files <- subset_valid_files(rollup_files_list, "rollup", params)
  expect_equal(rollup_valid_files, rollup_files_list[1:3])

  file.remove("params-run.json")
})

test_that("testing fetching list of files for training and predicting", {
  params <- read_params("params-run.json", "params-run.json.template",
    train_models = TRUE)

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

test_that("testing read_data type", {
  params <- read_params("params-run.json", "params-run.json.template")
  daily_data <- data.frame(test=c(TRUE, FALSE), num = c(1, 2))
  daily_file_name <- file.path(params$input_dir,
                               str_interp("chng_outpatient_as_of_${format(TODAY-5, date_format)}.parquet"))
  write_parquet(daily_data, daily_file_name)

  expect_true(inherits(read_data(daily_file_name), "data.frame"))

  file.remove(daily_file_name)
  file.remove("params-run.json")
})

test_that("testing conversion of fips to geo_value", {
  geo_value = c("01001", "99999", "12347")
  fips = c("55555", "52390", "00111")

  # Fail if neither `fips` nor `geo_value` exist
  expect_error(fips_to_geovalue(data.frame(empty_col = fips)),
    "Either `fips` or `geo_value` field must be available")

  # Return same df if only `geo_value` exists
  df <- data.frame(geo_value)
  expect_equal(fips_to_geovalue(df), df)

  # Drop `fips` field if both `fips` and `geo_value` exist
  expect_equal(fips_to_geovalue(data.frame(geo_value, fips)), data.frame(geo_value))

  # Rename `fips` to `geo_value` if only `fips` exists
  expect_equal(fips_to_geovalue(data.frame(fips)), data.frame(geo_value = fips))
})

test_that("get_issue_date_range", {
  # Initial call to `get_training_date_range` will return
  # list("training_start_date"=as.Date("2022-01-03"), "training_end_date"=as.Date("2022-01-31"))
  # with these settings.
  params <- list(
    "train_models" = TRUE,
    "testing_window" = 7, # days
    "training_days" = 28,
    "training_end_date" = "2022-01-31",
    "ref_lag" = 14 # days
  )

  # Requested test dates too early compared to requested training dates.
  params$test_dates <- as.Date(c("2022-01-01", "2022-01-02"))
  expect_error(get_issue_date_range(params),
    "training end date must be earlier than the earliest test date")

  params$test_dates <- as.Date(c("2022-06-01", "2022-06-02"))
  result <- get_issue_date_range(params)
  expect_equal(names(result), c("start_issue", "end_issue"))
  expect_equal(
    result,
    list("start_issue"=as.Date("2022-01-03") - 14, "end_issue"=as.Date("2022-06-02"))
  )
})

test_that("get_training_date_range", {
  # train_models = TRUE
  # training_end_date provided
  params <- list(
    "train_models" = TRUE,
    "testing_window" = 7, # days
    "training_days" = 28,
    "training_end_date" = "2022-01-31"
  )
  result <- get_training_date_range(params)
  expect_equal(names(result), c("training_start_date", "training_end_date"))
  expect_equal(
    result,
    list("training_start_date"=as.Date("2022-01-03"), "training_end_date"=as.Date("2022-01-31"))
  )

  # train_models = TRUE
  # training_end_date not provided
  params <- list(
    "train_models" = TRUE,
    "testing_window" = 7, # days
    "training_days" = 28
  )
  expect_equal(
    get_training_date_range(params),
    list("training_start_date"=Sys.Date() - 6 - 28, "training_end_date"=Sys.Date() - 6)
  )

  # train_models = FALSE
  # training_end_date provided or not shouldn't impact result
  # No model files
  tdir <- tempdir() # empty
  params <- list(
    "train_models" = FALSE,
    "cache_dir" = tdir,
    "testing_window" = 7, # days
    "training_days" = 28,
    "indicators" = "all"
  )
  params_tenddate <- list(
    "train_models" = FALSE,
    "cache_dir" = tdir,
    "testing_window" = 7, # days
    "training_days" = 28,
    "training_end_date" = "2022-01-31", # expect to be ignored
    "indicators" = "all"
  )
  expect_equal(
    get_training_date_range(params),
    list("training_start_date"=Sys.Date() - 6 - 28, "training_end_date"=Sys.Date() - 6)
  )
  expect_equal(
    get_training_date_range(params),
    get_training_date_range(params_tenddate)
  )

  # train_models = FALSE
  # training_end_date provided or not shouldn't impact result
  # Some model files
  empty_obj <- list()
  save(empty_obj,
    file=file.path(tdir, "20201031_20130610_changehc_covid_state_lambda0.1_fraction_ny_lag1_tau0.5.model"))
  save(empty_obj,
    file=file.path(tdir, "20201031_20130610_changehc_covid_state_lambda0.1_fraction_ny_lag1_tau0.75.model"))
  expect_equal(
    get_training_date_range(params),
    list("training_start_date"=as.Date("2020-10-03"), "training_end_date"=as.Date("2020-10-31"))
  )
  expect_equal(
    get_training_date_range(params),
    get_training_date_range(params_tenddate)
  )

  # With cached models having mixed training end dates.
  save(empty_obj,
    file=file.path(tdir, "20211031_20130610_changehc_covid_state_lambda0.1_fraction_ny_lag1_tau0.5.model"))
  expect_equal(
    get_training_date_range(params),
    list("training_start_date"=as.Date("2021-10-03"), "training_end_date"=as.Date("2021-10-31"))
  )
  expect_equal(
    get_training_date_range(params),
    get_training_date_range(params_tenddate)
  )

  # When given a specific indicator via `params`, only existing models of that same type are used
  # to fetch training_end_date
  params$indicators <- "flu"
  save(empty_obj,
    file=file.path(tdir, "20221031_20130610_flu_covid_state_lambda0.1_fraction_ny_lag1_tau0.5.model"))
  expect_equal(
    get_training_date_range(params),
    list("training_start_date"=as.Date("2022-10-03"), "training_end_date"=as.Date("2022-10-31"))
  )
})


