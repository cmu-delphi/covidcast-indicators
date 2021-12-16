library(dplyr)
library(mockr)
library(data.table)

context("Verifying the data manipulation correctness of the contingency_tables pipeline")

base_aggs <- tribble(
  ~name, ~metric, ~group_by, ~compute_fn, ~post_fn,
  "freq_anxiety", "mh_anxious", c("gender"), compute_binary, I,
  "pct_hh_fever", "hh_fever", c("gender"), compute_binary, I,
  "pct_heartdisease", "comorbidheartdisease", c("gender"), compute_binary, I,
)

# Suppress loading of archive to keep output predictable.
mock_load_archive <- function(...) {
  return(list(input_data = NULL, seen_tokens = NULL))
}

mock_metadata <- function(data, params, geo_type, groupby_vars) {
  return(data)
}

mock_geo_vars <- function(data, params, geo_type) {
  return(data)
}


# `simple_responses.csv` was created by copying `responses.csv` and modifying
# columns C1 (comorbidities), C8_1 (anxiety), and D1 (gender).
#     - All C1 responses set to "4" (high blood pressure)
#     - All C8_1 responses set to "1" (no anxiety)
#     - All D1 responses set to "2" (female)
#
# `simple_synthetic.csv` was created by copying each row in 
# `simple_responses.csv`100 times for a total of 2000 responses and modifying
# columns C1 (comorbidities), C8_1 (anxiety), and token (for uniqueness).
#     - Obs 11 had C1 response set to "4,12" (high blood pressure + type 1 diabetes)
#     - Obs 1 had C8_1 response set to "4" (anxious all the time)
#     - Tokens were reset to row numbers to prevent errors due to non-uniqueness
#
# Weights are all set to 100.
get_params <- function(output_dir) {
  params <- read_contingency_params("params-full.json")
  params$input <- c("simple_synthetic.csv")
  params$weights_in_dir <- "./weights_simple"
  
  params <- relativize_params(params)
  
  params$export_dir <- output_dir
  return(params)
}


### This test relies on `setup-run.R` to run the pipeline. This test loads
### `input/responses.csv`, a small selected subset of test responses.
test_that("small dataset produces no output", {
  # Since the contingency tables tool removes aggregates that pose a privacy
  # risk, the small test dataset should produce no aggregates at all. In fact,
  # test output directory won't even be created.
  expected_files <- character(0)
  actual_files <- dir(test_path("receiving_contingency_test"))

  expect_setequal(expected_files, actual_files)
  expect_equal(dir.exists(test_path("receiving_contingency_test")), FALSE)
})

### This test relies on `setup-run.R` to run the full pipeline and tests basic
### properties of the output.
test_that("full synthetic dataset produces expected output format", {
  expected_files <- c("20200501_20200531_monthly_nation_gender.csv")
  actual_files <- dir(test_path("receiving_contingency_full"))
  
  out <- read.csv(file.path(params$export_dir, "20200501_20200531_monthly_nation_gender.csv"))
  
  expect_setequal(expected_files, actual_files)
  expect_equal(dir.exists(test_path("receiving_contingency_full")), TRUE)
  expect_equal(nrow(out) > 0, TRUE)
  expect_equal(
    names(out), 
    c("survey_geo", "period_start", "period_end", "period_val", "period_type",
      "geo_type", "aggregation_type", "country", "ISO_3", "GID_0", "region",
      "GID_1", "state", "state_fips", "county", "county_fips",
      "gender", "val_pct_hh_fever", "se_pct_hh_fever", "sample_size_pct_hh_fever",
      "represented_pct_hh_fever", "val_pct_heartdisease",
      "se_pct_heartdisease", "sample_size_pct_heartdisease",
      "represented_pct_heartdisease", "issue_date")
  )
})


### Tests using equal weights

test_that("simple equal-weight dataset produces correct percents", {
  tdir <- tempfile()
  params <- get_params(tdir)
  create_dir_not_exist(params$export_dir)

  local_mock("delphiFacebook::load_archive" = mock_load_archive)
  local_mock("delphiFacebook::add_geo_vars" = mock_geo_vars)
  local_mock("delphiFacebook::add_metadata_vars" = mock_metadata)
  run_contingency_tables_many_periods(params, base_aggs[2,])

  # Expected files
  expect_setequal(!!dir(params$export_dir), c("20200501_20200531_monthly_nation_gender.csv"))

  # Expected file contents
  raw_data <- read.csv(test_path("./input/simple_synthetic.csv"))
  fever_prop <- mean( recode(raw_data[3:nrow(raw_data), "A1_1"], "1"=1, "2"=0) )

  expected_output <- as.data.frame(tribble(
    ~geo_id, ~gender, ~val_pct_hh_fever, ~se_pct_hh_fever, ~sample_size_pct_hh_fever, ~represented_pct_hh_fever,
    "us", "Female", fever_prop * 100, NA, 2000L, 100 * 2000
  ))
  
  df <- read.csv(file.path(params$export_dir, "20200501_20200531_monthly_nation_gender.csv"))
  expect_equivalent(df, expected_output)
})


test_that("testing run with multiple aggregations per group", {
  tdir <- tempfile()
  params <- get_params(tdir)
  create_dir_not_exist(params$export_dir)

  local_mock("delphiFacebook::load_archive" = mock_load_archive)
  local_mock("delphiFacebook::add_geo_vars" = mock_geo_vars)
  local_mock("delphiFacebook::add_metadata_vars" = mock_metadata)
  run_contingency_tables_many_periods(params, base_aggs)

  raw_data <- read.csv(test_path("./input/simple_synthetic.csv"))
  fever_prop <- mean( recode(raw_data[3:nrow(raw_data), "A1_1"], "1"=1, "2"=0) )

  expected <- tibble(
    geo_id = c("us"),
    gender = "Female",
    
    # freq_anxiety would appear here but not defined for the wave used in the
    # synthetic data.
    
    val_pct_hh_fever = fever_prop * 100,
    se_pct_hh_fever = NA,
    sample_size_pct_hh_fever = 2000L,
    represented_pct_hh_fever = 100 * 2000,
    
    val_pct_heartdisease = 0,
    se_pct_heartdisease = NA,
    sample_size_pct_heartdisease = 2000L,
    represented_pct_heartdisease = 100 * 2000,
  )

  out <- read.csv(file.path(params$export_dir, "20200501_20200531_monthly_nation_gender.csv"))
  expect_equivalent(out, expected)
})


### Tests using non-equal weights. `mix_weights` is stubbed (output is fixed) so
### we can ignore the weight normalization process in calculating expected output

# Substitute mix_weights return value so can use in calculation for expected value.
set.seed(0)
rand_weights <- runif(2000)
rand_weights <- rand_weights / sum(rand_weights)

mock_join_weights <- function(data, params, weights = c("step1", "full")) {
  data <- cbind(as.data.table(data), weight=rand_weights)
  return(data)
}

mock_mix_weights <- function(weights, s_mix_coef, s_weight) {
  if ( length(weights) == 1 ) {
    return(list(
      weights=rand_weights[1],
      normalized_preweights=rand_weights[1]
    ))
  } else if ( length(weights) == 1999 ) {
    return(list(
      weights=rand_weights[2:2000],
      normalized_preweights=rand_weights[2:2000]
    ))
  } else {
    return(list(
      weights=rand_weights,
      normalized_preweights=rand_weights
    ))
  }
}

test_that("simple weighted dataset produces correct percents", {
  tdir <- tempfile()
  params <- get_params(tdir)
  create_dir_not_exist(params$export_dir)

  local_mock("delphiFacebook::join_weights" = mock_join_weights)
  local_mock("delphiFacebook::mix_weights" = mock_mix_weights)
  local_mock("delphiFacebook::load_archive" = mock_load_archive)
  local_mock("delphiFacebook::add_geo_vars" = mock_geo_vars)
  local_mock("delphiFacebook::add_metadata_vars" = mock_metadata)
  run_contingency_tables_many_periods(params, base_aggs[2,])

  # Expected files
  expect_equal(!!dir(params$export_dir), c("20200501_20200531_monthly_nation_gender.csv"))

  # Expected file contents
  raw_data <- read.csv(test_path("./input/simple_synthetic.csv"))
  fever_prop <- weighted.mean( recode(raw_data[3:nrow(raw_data), "A1_1"], "1"=1, "2"=0) , rand_weights)

  expected_output <- as.data.frame(tribble(
    ~geo_id, ~gender, ~val_pct_hh_fever, ~se_pct_hh_fever, ~sample_size_pct_hh_fever, ~represented_pct_hh_fever,
    "us", "Female", fever_prop * 100, NA, 2000L, sum(rand_weights)
  ))

  out <- read.csv(file.path(params$export_dir, "20200501_20200531_monthly_nation_gender.csv"))
  expect_equivalent(out, expected_output)
})


### Providing a range of dates to produce aggregates for
test_that("production of historical CSVs for range of dates", {
  tdir <- tempfile()

  params <- get_params(tdir)
  params$aggregate_range <- "week"
  params$n_periods <- 4
  params$input <- c("full_synthetic.csv")
  params$weights_in_dir <- "./weights_full"

  create_dir_not_exist(params$export_dir)

  run_contingency_tables_many_periods(params, base_aggs[2,])
  # Expected files
  expect_equal(!!dir(params$export_dir), c("20200503_20200509_weekly_nation_gender.csv", "20200510_20200516_weekly_nation_gender.csv"))
})


## Megacounties
test_that("county aggs are created correctly", {
  tdir <- tempfile()
  params <- get_params(tdir)
  params$num_filter <- 100
  params$s_mix_coef <- 0.05
  
  agg <- tribble(
    ~name, ~metric, ~group_by, ~compute_fn, ~post_fn, ~id, ~var_weight,
    "pct_hh_fever", "hh_fever", c("gender", "geo_id"), compute_binary, I, "pct_hh_fever", "weight"
  )
  geomap <- tribble(
    ~zip5, ~geo_id,
    "10001", "10001",
    "10004", "10004",
    "20004", "20004",
  )
  input <- as.data.table(
    data.frame(
      gender = 1,
      hh_fever = c(rep(0, 75), rep(1, 30), rep(0, 101)),
      zip5 = c(rep("10001", 75), rep("10004", 30), rep("20004", 101)),
      weight = 100,
      weight_in_location = 1
    )
  )
  
  output <- summarize_aggs(input, geomap, agg, "county", params)
  # "AsIs" class originating from use of identity `I` in `post_fn` causes test
  # failure. Force to common format.
  output[[1]] <- tibble(output[[1]])
  
  expected_output <- list(
    "pct_hh_fever" = tribble(
      ~gender, ~geo_id, ~val, ~se, ~sample_size, ~effective_sample_size, ~represented,
      1, "20004", 0, NA_real_, 101, 101,  100 * 101,
      ## Megacounties are not created.
      # 1, "10000", 30/105 * 100, NA_real_, 105, 105,  NA_real_
    )
  )
  
  expect_equal(output, expected_output)
})
