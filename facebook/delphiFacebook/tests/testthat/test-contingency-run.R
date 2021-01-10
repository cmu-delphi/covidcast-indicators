library(dplyr)
library(mockery)
library(mockr)
library(data.table)

context("Verifying the data manipulation correctness of the contingency_tables pipeline")

# mc_anxiety = C8_1
# mc_gender = D1
# n_hh_num_total = A2b
# b_hh_fever = A1_1
# ms_comorbidities = C1
# zip = A3
base_aggs <- tribble(
  ~name, ~metric, ~group_by, ~compute_fn, ~post_fn,
  "freq_anxiety", "mc_anxiety", c("mc_gender"), compute_multiple_choice, I,
  "avg_hh_size", "n_hh_num_total", c("mc_gender"), compute_numeric, I,
  "pct_hh_fever", "b_hh_fever", c("mc_gender"), compute_binary_and_multiselect, I,
  "pct_comorbidities", "ms_comorbidities", c("mc_gender"), compute_binary_and_multiselect, I,
)

# Suppress loading of archive to keep output predictable.
stub(run_contingency_tables, "load_archive", 
     list(input_data = NULL, seen_tokens = NULL), depth=2)

get_params <- function(output_dir) {
  params <- read_params("params-contingency-full.json")
  params$export_dir <- output_dir
  params$input <- c("simple_synthetic.csv")
  params$weights_in_dir <- "./weights_simple"
  
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


### Tests using equal weights

test_that("simple equal-weight dataset produces correct counts", {
  tdir <- tempfile()
  params <- get_params(tdir)
  create_dir_not_exist(params$export_dir)

  run_contingency_tables(params, base_aggs[1,])

  # Expected files
  expect_setequal(!!dir(params$export_dir), c("20200501_nation_gender_anxiety.csv"))

  # Expected file contents
  expected_output <- as.data.frame(tribble(
    ~geo_id, ~mc_gender, ~mc_anxiety, ~val_freq_anxiety, ~sample_size_freq_anxiety,
    "us", "Female", 1L, 100 * (2000 - 1), 2000L -1L,
    # "us", "Female", 4L, 100 * 1, 1L # censored due to sample size
  ))

  df <- read.csv(file.path(params$export_dir, "20200501_nation_gender_anxiety.csv"))
  expect_equivalent(df, expected_output)
})


test_that("simple equal-weight dataset produces correct unweighted mean", {
  tdir <- tempfile()
  params <- get_params(tdir)
  create_dir_not_exist(params$export_dir)
  
  run_contingency_tables(params, base_aggs[2,])
  
  # Expected files
  expect_equal(!!dir(params$export_dir), c("20200501_nation_gender.csv"))
  
  # Expected file contents
  raw_data <- read.csv("./input/simple_synthetic.csv")
  hh_avg <- mean(as.numeric(raw_data[3:nrow(raw_data), "A2b"]))

  expected_output <- as.data.frame(tribble(
    ~geo_id, ~mc_gender, ~val_avg_hh_size, ~sample_size_avg_hh_size,
    "us", "Female", hh_avg, 2000L
  ))
  
  df <- read.csv(file.path(params$export_dir, "20200501_nation_gender.csv"))
  expect_equivalent(df, expected_output)
})


test_that("simple equal-weight dataset produces correct percents", {
  tdir <- tempfile()
  params <- get_params(tdir)
  create_dir_not_exist(params$export_dir)

  run_contingency_tables(params, base_aggs[3,])

  # Expected files
  expect_setequal(!!dir(params$export_dir), c("20200501_nation_gender.csv"))

  # Expected file contents
  raw_data <- read.csv("./input/simple_synthetic.csv")
  fever_prop <- mean( recode(raw_data[3:nrow(raw_data), "A1_1"], "1"=1, "2"=0) )

  expected_output <- as.data.frame(tribble(
    ~geo_id, ~mc_gender, ~val_pct_hh_fever, ~sample_size_pct_hh_fever,
    "us", "Female", fever_prop * 100, 2000L
  ))

  df <- read.csv(file.path(params$export_dir, "20200501_nation_gender.csv"))
  expect_equivalent(df, expected_output)
})


test_that("simple equal-weight dataset produces correct multiselect binary percents", {
  tdir <- tempfile()
  params <- get_params(tdir)
  create_dir_not_exist(params$export_dir)
  
  run_contingency_tables(params, base_aggs[4,])

  # Expected files
  expect_setequal(!!dir(params$export_dir), c("20200501_nation_gender.csv"))

  # Expected file contents
  expected_output <- as.data.frame(tribble(
    ~geo_id, ~mc_gender, ~val_pct_comorbidities_9, ~sample_size_pct_comorbidities_9, 
    ~val_pct_comorbidities_24, ~sample_size_pct_comorbidities_24,
    "us", "Female", 100, 2000L, 
    1/2000 * 100, 2000L
  ))

  df <- read.csv(file.path(params$export_dir, "20200501_nation_gender.csv"))
  expect_equivalent(df, expected_output)
})


test_that("testing run with multiple aggregations per group", {
  tdir <- tempfile()
  params <- get_params(tdir)
  create_dir_not_exist(params$export_dir)

  run_contingency_tables(params, base_aggs)

  ## freq_anxiety
  expect_setequal(!!dir(params$export_dir), c("20200501_nation_gender.csv",
                                              "20200501_nation_gender_anxiety.csv"))

  # Expected file contents
  ## freq_anxiety
  expected_anxiety <- as.data.frame(tribble(
    ~geo_id, ~mc_gender, ~mc_anxiety, ~val_freq_anxiety, ~sample_size_freq_anxiety,
    "us", "Female", 1L, 100 * (2000 - 1), 2000L -1L,
    # "us", "Female", 4L, 100 * 1, 1L # censored due to sample size
  ))

  df <- read.csv(file.path(params$export_dir, "20200501_nation_gender_anxiety.csv"))
  expect_equivalent(df, expected_anxiety)

  ## all other aggs
  raw_data <- read.csv("./input/simple_synthetic.csv")
  hh_avg <- mean(as.numeric(raw_data[3:nrow(raw_data), "A2b"]))
  fever_prop <- mean( recode(raw_data[3:nrow(raw_data), "A1_1"], "1"=1, "2"=0) )

  expected_other <- as.data.frame(tribble(
    ~geo_id, ~mc_gender, ~val_avg_hh_size, ~sample_size_avg_hh_size,
    ~val_pct_hh_fever, ~sample_size_pct_hh_fever,
    ~val_pct_comorbidities_9, ~sample_size_pct_comorbidities_9, 
    ~val_pct_comorbidities_24, ~sample_size_pct_comorbidities_24,
    "us", "Female", hh_avg, 2000L,
    fever_prop * 100, 2000L,
    100, 2000L, 
    1/2000 * 100, 2000L
  ))

  df <- read.csv(file.path(params$export_dir, "20200501_nation_gender.csv"))
  expect_equivalent(df, expected_other)
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

mock_mix_weights <- function() {
  return(list(
    weights=rand_weights,
    normalized_preweights=rand_weights
  ))
}

test_that("simple weighted dataset produces correct counts", {
  tdir <- tempfile()
  params <- get_params(tdir)
  params$parallel <- FALSE
  create_dir_not_exist(params$export_dir)
  
  with_mock(join_weights = mock_join_weights, {
    with_mock(mix_weights = mock_mix_weights, {
      run_contingency_tables(params, base_aggs[1,])
    })
  })

  # Expected files
  expect_equal(!!dir(params$export_dir), c("20200501_nation_gender_anxiety.csv"))

  # Expected file contents
  raw_data <- read.csv("./input/simple_synthetic.csv")
  anx_freq <- sum( rand_weights[raw_data[3:nrow(raw_data), "C8_1"] == "1"] )

  # Expected file contents
  expected_output <- as.data.frame(tribble(
    ~geo_id, ~mc_gender, ~mc_anxiety, ~val_freq_anxiety, ~sample_size_freq_anxiety,
    "us", "Female", 1L, anx_freq, 2000L - 1L,
    # "us", "Female", 4L, xx, 1L # censored due to sample size
  ))

  df <- read.csv(file.path(params$export_dir, "20200501_nation_gender_anxiety.csv"))
  print("")
  print(df)
  print(expected_output)
  print("")
  expect_equivalent(df, expected_output)
})


test_that("simple weighted dataset produces weighted mean", {
  tdir <- tempfile()
  params <- get_params(tdir)
  create_dir_not_exist(params$export_dir)

  with_mock(join_weights = mock_join_weights, {
    with_mock(mix_weights = mock_mix_weights, {
      run_contingency_tables(params, base_aggs[2,])
    })
  })
  
  # Expected files
  expect_equal(!!dir(params$export_dir), c("20200501_nation_gender.csv"))

  # Expected file contents
  raw_data <- read.csv("./input/simple_synthetic.csv")
  hh_avg <- weighted.mean(as.numeric(raw_data[3:nrow(raw_data), "A2b"]), rand_weights)

  expected_output <- as.data.frame(tribble(
    ~geo_id, ~mc_gender, ~val_avg_hh_size, ~sample_size_avg_hh_size,
    "us", "Female", hh_avg, 2000L
  ))

  df <- read.csv(file.path(params$export_dir, "20200501_nation_gender.csv"))
  print("")
  print(df)
  print(expected_output)
  print("")
  expect_equivalent(df, expected_output)
})


test_that("simple weighted dataset produces correct percents", {
  tdir <- tempfile()
  params <- get_params(tdir)
  create_dir_not_exist(params$export_dir)

  with_mock(join_weights = mock_join_weights, {
    with_mock(mix_weights = mock_mix_weights, {
      run_contingency_tables(params, base_aggs[3,])
    })
  })
  
  # Expected files
  expect_equal(!!dir(params$export_dir), c("20200501_nation_gender.csv"))

  # Expected file contents
  raw_data <- read.csv("./input/simple_synthetic.csv")
  fever_prop <- weighted.mean( recode(raw_data[3:nrow(raw_data), "A1_1"], "1"=1, "2"=0) , rand_weights)

  expected_output <- as.data.frame(tribble(
    ~geo_id, ~mc_gender, ~val_pct_hh_fever, ~sample_size_pct_hh_fever,
    "us", "Female", fever_prop * 100, 2000L
  ))

  df <- read.csv(file.path(params$export_dir, "20200501_nation_gender.csv"))
  print("")
  print(df)
  print(expected_output)
  print("")
  expect_equivalent(df, expected_output)
})


test_that("simple weighted dataset produces correct multiselect binary percents", {
  tdir <- tempfile()
  params <- get_params(tdir)
  create_dir_not_exist(params$export_dir)

  with_mock(join_weights = mock_join_weights, {
    with_mock(mix_weights = mock_mix_weights, {
      run_contingency_tables(params, base_aggs[4,])
    })
  })

  # Expected files
  expect_equal(!!dir(params$export_dir), c("20200501_nation_gender.csv"))

  # Expected file contents
  raw_data <- read.csv("./input/simple_synthetic.csv")
  comorbid_prop <- weighted.mean( recode(raw_data[3:nrow(raw_data), "C1"], "9"=0, .default=1) , rand_weights)

  expected_output <- as.data.frame(tribble(
    ~geo_id, ~mc_gender, ~val_pct_comorbidities_9, ~sample_size_pct_comorbidities_9,
    ~val_pct_comorbidities_24, ~sample_size_pct_comorbidities_24,
    "us", "Female", 100, 2000L,
    comorbid_prop * 100, 2000L
  ))

  df <- read.csv(file.path(params$export_dir, "20200501_nation_gender.csv"))
  print("")
  print(df)
  print(expected_output)
  print("")
  expect_equivalent(df, expected_output)
})
