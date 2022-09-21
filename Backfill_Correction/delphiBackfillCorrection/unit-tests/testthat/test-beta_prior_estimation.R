context("Testing helper functions for beta prior estimation")

# Generate Test Data
prior <- c(1, 2)
main_covariate <- c("log_value_7dav")
null_covariates <- c("value_raw_num", "value_raw_denom",
                     "value_7dav_num", "value_7dav_denom",
                     "value_prev_7dav_num", "value_prev_7dav_denom")
dayofweek_covariates <- c("Mon_ref", "Tue_ref", "Wed_ref", "Thurs_ref", 
                          "Fri_ref", "Sat_ref")
response <- "log_value_target"
lp_solver <- "gurobi"
lambda <- 0.1
model_save_dir <- "./model"
geo <- "pa"
value_type <- "fraction"
training_end_date <- as.Date("2022-01-01")
taus <- c(0.01, 0.025, 0.1, 0.25, 0.5, 0.75, 0.9, 0.975, 0.99)

set.seed(2022)
train_beta_vs <- log(rbeta(1000, 2, 5))
test_beta_vs <- log(rbeta(50, 2, 5))
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
prior_test_data <- test_data
covariates <- c(main_covariate, dayofweek_covariates)



test_that("testing the sum of squared error", {
  fit <- c(0, 1, 0)
  actual <- c(1, 1, 1)
  
  expected <- 1^2 + 1^2
  computed <- delta(fit, actual)
  expect_equal(expected, computed)
})


test_that("testing the squared error objection function given the beta prior", {
  theta <- c(log(1), log(2))
  x <- c(0.1, 0.25, 0.5, 0.75, 0.9)
  prob <- qbeta(x, 1, 2)
  
  expected <-0
  computed <- objective(theta, x, prob)
  expect_equal(expected, computed)
})


test_that("testing the prior estimation", {
  dw <- "Sat_ref"
  priors <- est_priors(train_data, prior_test_data, geo, value_type, dw, taus, 
                       covariates, response, lp_solver, lambda, 
                       indicator, signal, geo_level, signal_suffix, 
                       training_end_date, model_save_dir)
  beta <- priors[2]
  alpha <- priors[1] - beta
  expect_true((alpha > 0) & (alpha < 4))
  expect_true((beta > 4) & (beta < 8))
  
  for (idx in 1:length(taus)) {
    tau <- taus[idx]
    model_file_name <- generate_filename(indicator, signal, 
                                         geo_level, signal_suffix, lambda,
                                         geo=geo, dw=dw, tau=tau,
                                         value_type=value_type,
                                         training_end_date=training_end_date,
                                         beta_prior_mode=TRUE)
    model_path <- file.path(model_save_dir, model_file_name)
    expect_true(file.exists(model_path)) 
    file.remove(model_path)
  }
})


test_that("testing the fraction adjustment with pseudo counts", {
  value_raw <- frac_adj_with_pseudo(train_data, NULL, 1, 100, "value_raw_num", "value_raw_denom")
  expect_true(all(value_raw == 1/100))
  
  dw <- "Sat_ref"
  value_raw <- frac_adj_with_pseudo(train_data, dw, 1, 100, "value_raw_num", "value_raw_denom")
  expect_true(all(value_raw == 1/100))
})


test_that("testing the main beta prior adjustment function", {
  set.seed(1)
  updated_data <- frac_adj(train_data, test_data, prior_test_data, 
                           indicator, signal, geo_level, signal_suffix,
                           traning_end_date, model_save_dir, 
                           geo, value_type, taus = taus, lp_solver = lp_solver)
  updated_train_data <- updated_data[[1]]
  updated_test_data <- updated_data[[2]]
  
  for (dw in c(dayofweek_covariates, "Sun_ref")){
    for (idx in 1:length(taus)) {
      tau <- taus[idx]
      model_file_name <- generate_filename(indicator, signal, 
                                           geo_level, signal_suffix, lambda,
                                           geo=geo, dw=dw, tau=tau,
                                           value_type=value_type,
                                           training_end_date=training_end_date,
                                           beta_prior_mode=TRUE)
      model_path <- file.path(model_save_dir, model_file_name)
      expect_true(file.exists(model_path)) 
      file.remove(model_path)
    }
  }
  
  expect_true(unique(updated_train_data$value_raw) == unique(updated_test_data$value_raw))
  expect_true(all(updated_train_data$value_raw < 6/(6+1)))
  expect_true(all(updated_train_data$value_raw > 4/(4+4)))
})

