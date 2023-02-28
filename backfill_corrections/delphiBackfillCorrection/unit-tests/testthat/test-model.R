context("Testing the helper functions for modeling")

library(dplyr)

# Constants
indicator <- "chng"
signal <- "outpatient" 
geo_level <- "state"
signal_suffix <- ""
lambda <- 0.1
test_lag <- 1
model_save_dir <- "./cache"
geo <- "pa"
value_type <- "fraction"
date_format = "%Y%m%d"
training_days <- 7
training_end_date <- as.Date("2022-01-01")
training_start_date <- training_end_date - training_days

# Generate Test Data
main_covariate <- c("log_value_7dav")
null_covariates <- c("value_raw_num", "value_raw_denom",
                     "value_7dav_num", "value_7dav_denom",
                     "value_prev_7dav_num", "value_prev_7dav_denom")
dayofweek_covariates <- c("Mon_ref", "Tue_ref", "Wed_ref", "Thurs_ref", 
                          "Fri_ref", "Sat_ref")
response <- "log_value_target"
train_beta_vs <- log(rbeta(1000, 2, 5))
test_beta_vs <- log(rbeta(61, 2, 5))
train_data <- data.frame(log_value_7dav = train_beta_vs,
                         log_value_target = train_beta_vs)
train_data$value_target_num <- exp(train_beta_vs) * 100
train_data$value_target_denom <- 100
test_data <- data.frame(log_value_7dav = test_beta_vs,
                        log_value_target = test_beta_vs)
for (cov in null_covariates){
  train_data[[cov]] <- 0
  test_data[[cov]] <- 0
}
for (cov in c(dayofweek_covariates, "Sun_ref")){
  train_data[[cov]] <- 1
  test_data[[cov]] <- 1
}
covariates <- c(main_covariate, dayofweek_covariates)
  
  
test_that("testing the generation of model filename prefix", {
  model_file_name <- generate_filename(indicator, signal, 
                                    geo_level, signal_suffix, lambda,
                                    training_end_date=training_end_date,
                                    training_start_date=training_start_date)
  expected <- str_interp(
    "${format(training_end_date, date_format)}_${format(training_start_date, date_format)}_chng_outpatient_state_lambda0.1.model"
  )
  expect_equal(model_file_name, expected)
})

test_that("testing the evaluation", {
  pred_cols = paste0("predicted_tau", TAUS)

  for (tau in TAUS){
    test_data[[paste0("predicted_tau", as.character(tau))]] <- log(quantile(exp(train_beta_vs), tau))
  }
  result <- evaluate(test_data, TAUS)
  expect_true(mean(result$wis) < 0.3)

  # Prediction columns are unchanged
  expect_equal(test_data[, pred_cols], result[, pred_cols])
})

test_that("testing prediction column exponentiation", {
  # Basic example
  input <- data.frame(
    time_value = 5,
    predicted_tau0.1 = c(0, 1, 1),
    predicted_tau0.5 = c(2, 0, 1)
  )
  expected <- data.frame(
    time_value = 5,
    predicted_tau0.1 = c(1, exp(1), exp(1)),
    predicted_tau0.5 = c(exp(2), 1, exp(1))
  )
  expect_equal(expected, exponentiate_preds(input, c(0.1, 0.5)))


  # Realistic test df
  pred_cols = paste0("predicted_tau", TAUS)
  test_data <- mutate(test_data, time_value = as.Date("2022-12-02"))

  for (tau in TAUS){
    test_data[[paste0("predicted_tau", as.character(tau))]] <- log(quantile(exp(train_beta_vs), tau))
  }

  expected <- test_data
  for (col_name in pred_cols){
    expected[[col_name]] <- exp(test_data[[col_name]])
  }

  result <- exponentiate_preds(test_data, TAUS)
  expect_equal(result, expected)
})

test_that("testing generating or loading the model", {
  # Check the model that does not exist
  tau = 0.5
  model_file_name <- generate_filename(indicator, signal, 
                                       geo_level, signal_suffix, lambda,
                                       geo=geo, test_lag=test_lag, tau=tau,
                                       training_end_date=training_end_date,
                                       training_start_date=training_start_date)
  model_path <- file.path(model_save_dir, model_file_name)
  expect_true(!file.exists(model_path))
  
  # Generate the model and check again
  obj <- get_model(model_path, train_data, covariates, tau,
                        lambda, LP_SOLVER, train_models=TRUE) 
  expect_true(file.exists(model_path))
  created <- file.info(model_path)$ctime
  
  # Check that the model was not generated again.
  obj <- get_model(model_path, train_data, covariates, tau,
                        lambda, LP_SOLVER, train_models=FALSE)   
  expect_equal(file.info(model_path)$ctime, created)
  
  expect_silent(file.remove(model_path))
})

test_that("testing model training and testing", {
  result <- model_training_and_testing(train_data, test_data, taus=TAUS, covariates=covariates,
                                       lp_solver=LP_SOLVER, lambda=lambda, test_lag=test_lag,
                                       geo=geo, value_type=value_type, model_save_dir=model_save_dir,
                                       indicator=indicator, signal=signal,
                                       geo_level=geo_level, signal_suffix=signal_suffix,
                                       training_end_date=training_end_date,
                                       training_start_date=training_start_date,
                                       train_models = TRUE,
                                       make_predictions = TRUE) 
  test_result <- result[[1]]
  coef_df <- result[[2]]
  
  for (tau in TAUS){
    cov <- paste0("predicted_tau", as.character(tau))
    expect_true(cov %in% colnames(test_result))
    
    model_file_name <- generate_filename(indicator=indicator, signal=signal, 
                                         geo_level=geo_level, value_type=value_type,
                                         signal_suffix=signal_suffix, lambda=lambda,
                                         geo=geo, test_lag=test_lag, tau=tau,
                                         training_end_date=training_end_date,
                                         training_start_date=training_start_date)
    model_path <- file.path(model_save_dir, model_file_name)
    expect_true(file.exists(model_path))
    
    expect_silent(file.remove(model_path))
  }
  
  for (cov in covariates){
    cov <- paste(cov, "coef", sep="_")
    expect_true(cov %in% colnames(coef_df))
  }
})

test_that("testing adding square root scale", {
  expect_error(result <- add_sqrtscale(train_data, test_data, 1, "value_raw"),
               "value raw does not exist in training data!")
  
  train_data$value_raw <- rbeta(nrow(train_data), 2, 5)
  expect_error(result <- add_sqrtscale(train_data, test_data, 1, "value_raw"),
               "value raw does not exist in testing data!")
  
  test_data$value_raw <- rbeta(nrow(test_data), 2, 5)
  expect_silent(result <- add_sqrtscale(train_data, test_data, 1, "value_raw"))
  
  new_train_data <- result[[1]]
  new_test_data <- result[[2]]
  sqrtscales <- result[[3]]
  expect_true(length(sqrtscales) == 4)
  for (cov in sqrtscales){
    expect_true(cov %in% colnames(new_train_data))
    expect_true(cov %in% colnames(new_test_data))
  }
  expect_true(all(rowSums(new_train_data[sqrtscales]) %in% c(0, 1)))
  expect_true(all(rowSums(new_test_data[sqrtscales]) %in% c(0, 1)))
  
  for (i in 0:2){
    m_l <- max(new_train_data[new_train_data[[paste0("sqrty", as.character(i))]] == 1, "value_raw"])
    m_r <- min(new_train_data[new_train_data[[paste0("sqrty", as.character(i+1))]] == 1, "value_raw"])
    expect_true(m_l <= m_r)
  }
  
})

test_that("testing data filteration", {
  train_data$lag <- rep(0:60, nrow(train_data))[1:nrow(train_data)]
  test_data$lag <- rep(0:60, nrow(test_data))[1:nrow(test_data)]
  
  # When test lag is small
  test_lag <- 5
  result <- data_filteration(test_lag, train_data, test_data, 2)
  train_df <- result[[1]]
  test_df <- result[[2]]
  expect_true(max(train_df$lag) == test_lag+2)
  expect_true(min(train_df$lag) == test_lag-2)
  expect_true(all(test_df$lag == test_lag))
  
  # When test lag is large
  test_lag <- 48
  result <- data_filteration(test_lag, train_data, test_data, 2)
  train_df <- result[[1]]
  test_df <- result[[2]]
  expect_true(max(test_df$lag) == test_lag+7)
  expect_true(min(test_df$lag) == test_lag-6)
  
  # Make sure that all lags are tested
  included_lags = c()
  for (test_lag in c(1:14, 21, 35, 51)){
    result <- data_filteration(test_lag, train_data, test_data, 2)
    test_df <- result[[2]]
    included_lags <- c(included_lags, unique(test_df$lag))
  }
  expect_true(all(1:60 %in% included_lags))
})
