context("Testing utils helper functions")

TRAINING_DAYS = 10
 
test_that("testing create directory if not exist", {
  # If not exists
  path = "test.test"
  create_dir_not_exist(path)
  expect_true(file.exists(path))
  
  # If already exists
  create_dir_not_exist(path)
  expect_true(file.exists(path))
  
  # Remove
  unlink(path, recursive = TRUE)
  expect_true(!file.exists(path))
})


test_that("testing number of available issue dates for training", {
  start_date <- as.Date("2022-01-01")
  end_date <- as.Date("2022-01-09")
  training_days = 10
  issue_date <- seq(start_date, end_date, by = "days")
  expect_warning(training_days_check(issue_date, training_days = training_days),
                 "Only 9 days are available at most for training.")
  
  end_date <- as.Date("2022-01-10")
  training_days = 10
  issue_date <- seq(start_date, end_date, by = "days")
  expect_silent(training_days_check(issue_date, training_days = training_days))
})

test_that("testing get the top200 populous counties", {
  counties <- get_populous_counties()
    
  expect_true(length(counties) == 200)
  expect_true("06037" %in% counties)
})


test_that("testing the filteration of top200 populous counties", {
  geos = c("06037", "58001")
  expect_true(filter_counties(geos) == "06037")
})


test_that("testing read parameters", {
  # No input file
  expect_error(read_params(path = "params.json", template_path = "params.json.template",
                           train_models = TRUE, make_predictions = TRUE),
               "input_dir must be set in `params` and exist")
  
  # Check parameters
  params <- read_json("params.json", simplifyVector = TRUE)
  # Check initialization
  expect_true(!("export_dir" %in% names(params)))
  expect_true(!("cache_dir" %in% names(params)))

  expect_true(!("parallel" %in% names(params)))
  expect_true(!("parallel_max_cores" %in% names(params)))
  
  
  expect_true(!("taus" %in% names(params)))
  expect_true(!("lambda" %in% names(params)))
  expect_true(!("lp_solver" %in% names(params)))
  
  expect_true(!("taus" %in% names(params)))
  expect_true(!("lambda" %in% names(params)))
  expect_true(!("lp_solver" %in% names(params)))
  
  expect_true(!("num_col" %in% names(params)))
  expect_true(!("denom_col" %in% names(params)))
  expect_true(!("geo_levels" %in% names(params)))
  expect_true(!("value_types" %in% names(params)))
  
  expect_true(!("training_days" %in% names(params)))
  expect_true(!("ref_lag" %in% names(params)))
  expect_true(!("testing_window" %in% names(params)))
  expect_true(!("test_dates" %in% names(params)))
  
  # Create input file
  path = "test.tempt"
  create_dir_not_exist(path)
  expect_silent(params <- read_params(path = "params.json", 
                                      template_path = "params.json.template",
                                      train_models = TRUE, make_predictions = TRUE))
  unlink(path, recursive = TRUE)
  
  
  expect_true("export_dir" %in% names(params))
  expect_true("cache_dir" %in% names(params))
  
  expect_true("parallel" %in% names(params))
  expect_true("parallel_max_cores" %in% names(params))
  
  
  expect_true("taus" %in% names(params))
  expect_true("lambda" %in% names(params))
  expect_true("lp_solver" %in% names(params))
  
  expect_true("taus" %in% names(params))
  expect_true("lambda" %in% names(params))
  expect_true("lp_solver" %in% names(params))
  
  expect_true("num_col" %in% names(params))
  expect_true("denom_col" %in% names(params))
  expect_true("geo_levels" %in% names(params))
  expect_true("value_types" %in% names(params))
  
  expect_true("training_days" %in% names(params))
  expect_true("ref_lag" %in% names(params))
  expect_true("testing_window" %in% names(params))
  expect_true("test_dates" %in% names(params))
  
  expect_true(params$export_dir == "./receiving")
  expect_true(params$cache_dir == "./cache")
  
  expect_true(params$parallel == FALSE)
  expect_true(params$parallel_max_cores == .Machine$integer.max)
  
  expect_true(all(params$taus == TAUS))
  expect_true(params$lambda == LAMBDA)
  expect_true(params$lp_solver == LP_SOLVER)
  
  expect_true(params$num_col == "num")
  expect_true(params$denom_col == "denom")
  expect_true(all(params$geo_levels == c("state", "county")))
  expect_true(all(params$value_types == c("count", "fraction")))
  
  expect_true(params$training_days == TRAINING_DAYS)
  expect_true(params$ref_lag == REF_LAG)
  expect_true(params$testing_window == TESTING_WINDOW)
  start_date <- TODAY - params$testing_window
  end_date <- TODAY - 1
  expect_true(all(params$test_dates == seq(start_date, end_date, by="days")))
  
  expect_silent(file.remove("params.json"))
})


