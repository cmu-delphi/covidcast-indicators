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
    input_dir = "./input"
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
    end_time = ymd_hms("2020-01-31 23:59:59", tz=timezone),
    start_time = ymd_hms("2020-01-01 00:00:00", tz=timezone),
    start_date = ymd("2020-01-01")
    )
  
  out <- update_params(input_params)
  expect_identical(out, expected_output)
})

test_that("testing get_filenames_in_range command", {
  tdir <- tempfile()
  files <- c(
    "2019-11-06.2019-10-30.2020-11-06.Survey_of_COVID-Like_Illness_-_TODEPLOY_......_-_US_Expansion.csv",
    "2019-12-31.2019-12-24_With_Translations.csv",
    "2020-01-06.2019-12-31_Wave_4.csv",
    "2020-01-16.2020-01-09_YouTube.csv",
    "2020-01-16.2020-01-09_Wave_4.csv",
    "2020-02-06.2020-01-31_Wave_4.csv",
    "2020-02-16.2020-02-09_Wave_3.csv"
  )
  
  create_dir_not_exist(tdir)
  for (filename in files) {
    write_csv(data.frame(), path = file.path(tdir, filename))
  }
  
  params <- list(
    input = c(),
    use_input_asis = FALSE,
    backfill_days = 4,
    input_dir = tdir
  )
  date_range <- list(ymd("2020-01-01"), ymd("2020-01-31"))
  
  expected_output <- c(
    "2019-12-31.2019-12-24_With_Translations.csv",
    "2020-01-06.2019-12-31_Wave_4.csv",
    "2020-01-16.2020-01-09_Wave_4.csv",
    "2020-02-06.2020-01-31_Wave_4.csv"
  )
  
  out <- get_filenames_in_range(date_range[[1]], date_range[[2]], params)
  
  expect_equal(out, expected_output)
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
    ~name, ~metric, ~group_by, ~compute_fn, ~post_fn, ~id, ~var_weight, ~skip_mixing, 
    "no_change", "C14", c("geo_id", "mc_race", "DDD1_23"), NULL, NULL, "no_change_C14_c(\"geo_id\", \"mc_race\", \"DDD1_23\")", "weight", FALSE,
    "duplicated", "DDD1_23", c("geo_id", "mc_race"), NULL, NULL, "duplicated_DDD1_23_c(\"geo_id\", \"mc_race\")", "weight", FALSE,
    "duplicated", "DDD1_23", c("geo_id"), NULL, NULL, "duplicated_DDD1_23_geo_id", "weight", FALSE
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
