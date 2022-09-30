library(lubridate)
library(tibble)

context("Testing utility functions")

test_that("testing update_params command", {
  # Empty input list
  params <- list(
    input = c(),
    use_input_asis = TRUE,
    aggregate_range = "month",
    end_date = "2020-02-01",
    input_dir = "./static" # Using a directory that doesn't contain any valid data files.
  )
  
  expect_error(update_params(params), "no input files to read in")
  
  # Use specified end date
  input_params <- list(
    input = c("full_response.csv"),
    use_input_asis = TRUE,
    aggregate_range = "month",
    end_date = "2020-02-01"
  )
  
  timezone <- "America/Los_Angeles"
  
  expected_output <- list(
    input = c("full_response.csv"),
    use_input_asis = TRUE,
    aggregate_range = "month",
    end_date = ymd("2020-01-31"),
    start_time = ymd_hms("2020-01-01 00:00:00", tz=timezone),
    end_time = ymd_hms("2020-01-31 23:59:59", tz=timezone),
    start_date = ymd("2020-01-01")
    )
  
  out <- update_params(input_params)
  expect_identical(out, expected_output)
})

test_that("testing get_filenames_in_range command", {
  tdir <- tempfile()
  files <- c(
    "2029-01-01.2019-10-30.2019-11-06.Survey_of_COVID-Like_Illness_-_TODEPLOY_......_-_US_Expansion.csv",
    "2029-01-01.2019-12-24.2019-12-31_With_Translations.csv",
    "2029-01-01.2019-12-31.2020-01-06_Wave_4.csv",
    "2029-01-01.2020-01-09.2020-01-16_YouTube.csv",
    "2029-01-01.2020-01-09.2020-01-16_Wave_4.csv",
    "2029-01-01.2020-01-31.2020-02-06_Wave_4.csv",
    "2029-01-01.2020-02-09.2020-02-16_Wave_3.csv"
  )
  
  create_dir_not_exist(tdir)
  for (filename in files) {
    write_csv(data.frame(), file.path(tdir, filename))
  }
  
  params <- list(
    input = c(),
    use_input_asis = FALSE,
    backfill_days = 4,
    input_dir = tdir
  )
  date_range <- list(ymd("2020-01-01"), ymd("2020-01-31"))
  
  expected_output <- c(
    "2029-01-01.2019-12-24.2019-12-31_With_Translations.csv",
    "2029-01-01.2019-12-31.2020-01-06_Wave_4.csv",
    "2029-01-01.2020-01-09.2020-01-16_Wave_4.csv",
    "2029-01-01.2020-01-31.2020-02-06_Wave_4.csv"
  )
  
  out <- get_filenames_in_range(date_range[[1]], date_range[[2]], params)
  expect_equal(out, expected_output)
})


test_that("testing get_sparse_filenames command", {
  tdir <- tempfile()
  files <- c(
    "2021-12-11.2019-12-26.2020-01-01_Wave_4.csv",
    "2021-12-11.2019-12-27.2020-01-02_Wave_4.csv",
    "2021-12-11.2019-12-28.2020-01-03_Wave_4.csv",
    "2021-12-11.2019-12-29.2020-01-04_Wave_4.csv",
    "2021-12-11.2019-12-30.2020-01-05_Wave_4.csv",
    "2021-12-11.2019-12-30.2020-01-05_Wave_5.csv",
    "2021-12-11.2019-12-31.2020-01-06_Wave_4.csv",
    "2021-12-11.2019-12-31.2020-01-06_Wave_5.csv",
    "2021-12-11.2019-01-01.2020-01-07_Wave_4.csv",
    "2021-12-11.2019-01-02.2020-01-08_Wave_4.csv",
    "2021-12-11.2019-01-03.2020-01-09_Wave_4.csv",
    "2021-12-11.2019-01-04.2020-01-10_Wave_4.csv",
    
    "2011-12-11.2019-10-30.2019-11-06.2020-11-06.Survey_of_COVID-Like_Illness_-_TODEPLOY_......_-_US_Expansion.csv",
    "2021-12-11.2020-01-09.2020-01-16_YouTube.csv",
    "2021-12-11.2020-01-09.2020-01-16_Wave_4.csv",
    "2021-12-11.2020-01-31.2020-02-06_Wave_4.csv",
    "2021-12-11.2020-02-09.2020-02-16_Wave_3.csv"
  )
  
  create_dir_not_exist(tdir)
  for (filename in files) {
    write_csv(data.frame(), file.path(tdir, filename))
  }
  
  params <- list(
    input = c(),
    use_input_asis = FALSE,
    backfill_days = 4,
    input_dir = tdir
  )
  date_range <- list(ymd("2020-01-01"), ymd("2020-01-6"))
  
  expected_output <- c(
    "2021-12-11.2019-12-26.2020-01-01_Wave_4.csv",
    "2021-12-11.2019-12-30.2020-01-05_Wave_4.csv",
    "2021-12-11.2019-12-30.2020-01-05_Wave_5.csv",
    "2021-12-11.2019-01-03.2020-01-09_Wave_4.csv",
    "2021-12-11.2019-01-04.2020-01-10_Wave_4.csv"
  )
  
  out <- get_sparse_filenames(date_range[[1]], date_range[[2]], params)
  expect_setequal(out, expected_output)
})


test_that("testing verify_aggs command", {
  # Duplicate rows
  input_aggs <- tribble(
    ~name, ~metric, ~group_by, ~compute_fn, ~post_fn,
    "no_change", "C14", c("geo_id", "mc_race", "DDD1_23"), NULL, NULL,
    "duplicated", "DDD1_23", c("geo_id", "mc_race"), NULL, NULL,
    "duplicated", "DDD1_23", c("geo_id"), NULL, NULL
  )
  
  expected_output <- tribble(
    ~name, ~metric, ~group_by, ~compute_fn, ~post_fn, ~id, ~var_weight,
    "no_change", "C14", c("geo_id", "mc_race", "DDD1_23"), NULL, NULL, "no_change_C14_c(\"geo_id\", \"mc_race\", \"DDD1_23\")", "weight",
    "duplicated", "DDD1_23", c("geo_id", "mc_race"), NULL, NULL, "duplicated_DDD1_23_c(\"geo_id\", \"mc_race\")", "weight",
    "duplicated", "DDD1_23", c("geo_id"), NULL, NULL, "duplicated_DDD1_23_geo_id", "weight",
  )
  
  out <- verify_aggs(input_aggs)
  expect_identical(out, expected_output)
  
  # Missing columns
  input_aggs <- tribble(
    ~name, ~metric, ~group_by,
    "no_change", "C14", c("geo_id", "mc_race", "DDD1_23"),
    "duplicated", "DDD1_23", c("geo_id", "mc_race")
  )
  
  expect_error(verify_aggs(input_aggs), 
               "all expected columns .* must appear in the aggregations table")
})
