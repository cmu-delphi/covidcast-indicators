## Functions for Beta Prior Approach.
##
## This is used only for the fraction prediction e.g. fraction of Covid claims,
## percentage of positive tests. We assume that the fraction follows a beta distribution
## that is day-of-week dependent. A quantile regression model is used first with lasso
## penalty for supporting quantile estimation and then a non-linear minimization is used
## for prior estimation.

#' Sum of squared error
#' 
#' @param fit estimated values
#' @param actual actual values
delta <- function(fit, actual) sum((fit-actual)^2)

#' Generate objection function
#' @param theta parameters for the distribution in log scale 
#' @param x vector of quantiles
#' @param prob the expected probabilities
#' @param ... additional arguments
#' 
#' @importFrom stats pbeta
objective <- function(theta, x, prob, ...) {
  ab <- exp(theta) # Parameters are the *logs* of alpha and beta
  fit <- pbeta(x, ab[1], ab[2])
  return (delta(fit, prob))
}

#' Main function for the beta prior approach
#' Estimate the priors for the beta distribution based on data for 
#' a certain day of a week
#' 
#' @template train_data-template
#' @param prior_test_data Data Frame for testing 
#' @template taus-template
#' @template covariates-template
#' @template lp_solver-template
#' @template lambda-template
#' @template geo_level-template
#' @template geo-template
#' @template indicator-template
#' @template signal-template
#' @template signal_suffix-template
#' @template value_type-template
#' @template train_models-template
#' @template make_predictions-template
#' @param dw column name to indicate which day of a week it is
#' @param response the column name of the response variable
#' @param start the initialization of the the points in nlm
#' @param base_pseudo_denom the pseudo counts added to denominator if little data for training
#' @param base_pseudo_num the pseudo counts added to numerator if little data for training
#' @template training_end_date-template
#' @template training_start_date-template
#' @param model_save_dir directory containing trained models
#' 
#' @importFrom stats nlm predict
#' @importFrom dplyr %>% filter
#' @importFrom quantgen quantile_lasso
#' 
est_priors <- function(train_data, prior_test_data, geo, value_type, dw, taus, 
                       covariates, response, lp_solver, lambda, 
                       indicator, signal, geo_level, signal_suffix, 
                       training_end_date, training_start_date,
                       model_save_dir, start=c(0, log(10)),
                       base_pseudo_denom=1000, base_pseudo_num=10,
                       train_models = TRUE, make_predictions = TRUE) {
  sub_train_data <- train_data %>% filter(train_data[[dw]] == 1)
  sub_test_data <- prior_test_data %>% filter(prior_test_data[[dw]] == 1)
  if (nrow(sub_test_data) == 0) {
    pseudo_denom <- base_pseudo_denom
    pseudo_num <- base_pseudo_num
  } else {
    # Using quantile regression to get estimated quantiles at log scale
    quantiles <- list()
    for (idx in 1:length(taus)) {
      tau <- taus[idx]
      model_file_name <- generate_filename(indicator, signal, 
                                           geo_level, signal_suffix, lambda,
                                           geo=geo, dw=dw, tau=tau,
                                           value_type=value_type,
                                           training_end_date=training_end_date,
                                           training_start_date=training_start_date,
                                           beta_prior_mode=TRUE)
      model_path <- file.path(model_save_dir, model_file_name)
      
      obj = get_model(model_path, sub_train_data, covariates, tau = tau,
                      lambda = lambda, lp_solver = lp_solver, train_models)

      y_hat_all <- as.numeric(predict(obj, newx = as.matrix(sub_test_data[covariates])))
      quantiles[idx] <- exp(mean(y_hat_all, na.rm=TRUE)) # back to the actual scale
    }
    quantiles <- as.vector(unlist(quantiles))  
    # Using nlm to estimate priors
    sol <- nlm(objective, start, x=quantiles, prob=taus, lower=0, upper=1,
               typsize=c(1,1), fscale=1e-12, gradtol=1e-12)
    parms <- exp(sol$estimate)  
    # Computing pseudo counts based on beta priors
    pseudo_denom <- parms[1] + parms[2]
    pseudo_num <- parms[1]
  }
  return (c(pseudo_denom, pseudo_num))
}

#' Update fraction based on the pseudo counts for numerators and denominators
#' 
#' @param data Data Frame
#' @param dw character to indicate the day of a week. Can be NULL for all the days
#' @param pseudo_num the estimated counts to be added to numerators
#' @param pseudo_denom the estimated counts to be added to denominators
#' @template num_col-template
#' @template denom_col-template
#' 
#' @export
frac_adj_with_pseudo <- function(data, dw, pseudo_num, pseudo_denom, num_col, denom_col) {
  if (is.null(dw)) {
    num_adj <- data[[num_col]]  + pseudo_num
    denom_adj <- data[[denom_col]]  + pseudo_denom
  } else {
    num_adj <- data[[num_col]][data[[dw]] == 1]  + pseudo_num
    denom_adj <- data[data[[dw]] == 1, denom_col]  + pseudo_denom
  }
  return (num_adj / denom_adj)
}

#' Update fraction using beta prior approach
#' 
#' @template train_data-template
#' @param test_data testing data
#' @param prior_test_data testing data for the lag -1 model
#' @template training_end_date-template
#' @template training_start_date-template
#' @param model_save_dir directory containing trained models
#' @template indicator-template
#' @template signal-template
#' @template geo-template
#' @template signal_suffix-template
#' @template lambda-template
#' @template value_type-template
#' @template geo_level-template
#' @template taus-template
#' @template lp_solver-template
#' @template train_models-template
#' @template make_predictions-template
#' 
#' @export
frac_adj <- function(train_data, test_data, prior_test_data, 
                     indicator, signal, geo_level, signal_suffix,
                     lambda, value_type, geo, 
                     training_end_date, training_start_date,
                     model_save_dir,
                     taus, lp_solver,
                     train_models = TRUE,
                     make_predictions = TRUE) {
  train_data$value_target <- frac_adj_with_pseudo(train_data, NULL, 1, 100, "value_target_num", "value_target_denom")
  train_data$log_value_target <- log(train_data$value_target)

  test_data$value_target <- frac_adj_with_pseudo(test_data, NULL, 1, 100, "value_target_num", "value_target_denom")
  test_data$log_value_target <- log(test_data$value_target)

  train_data$value_7dav <- frac_adj_with_pseudo(train_data, NULL, 1, 100, "value_7dav_num", "value_7dav_denom")
  train_data$log_value_7dav <- log(train_data$value_7dav)

  prior_test_data$value_7dav <- frac_adj_with_pseudo(prior_test_data, NULL, 1, 100, "value_7dav_num", "value_7dav_denom")
  prior_test_data$log_value_7dav <- log(prior_test_data$value_7dav)
  

  pre_covariates = c("Mon_ref", "Tue_ref", "Wed_ref", "Thurs_ref", "Fri_ref", "Sat_ref",
                      "log_value_7dav")
  #For training
  train_data$value_raw = NaN
  train_data$value_7dav = NaN
  train_data$value_prev_7dav = NaN
  
  #For testing
  test_data$value_raw = NaN
  test_data$value_7dav = NaN
  test_data$value_prev_7dav = NaN
  
  test_data$pseudo_num = NaN
  test_data$pseudo_denum = NaN
  
  for (cov in c("Mon_ref", "Tue_ref", "Wed_ref", "Thurs_ref", "Fri_ref", "Sat_ref", "Sun_ref")) {
    pseudo_counts <- est_priors(train_data, prior_test_data, geo, value_type, cov, taus, 
                                pre_covariates, "log_value_target", lp_solver, lambda, 
                                indicator, signal, geo_level, signal_suffix, 
                                training_end_date, training_start_date, model_save_dir,
                                train_models = train_models,
                                make_predictions = make_predictions)
    pseudo_denum = pseudo_counts[1]
    pseudo_num = pseudo_counts[2]
    # update current data
    # For training
    train_data$value_raw[train_data[[cov]] == 1] <- frac_adj_with_pseudo(
      train_data, cov, pseudo_num, pseudo_denum, "value_raw_num", "value_raw_denom")
    train_data$value_7dav[train_data[[cov]] == 1] <- frac_adj_with_pseudo(
      train_data, cov, pseudo_num, pseudo_denum, "value_7dav_num", "value_7dav_denom")
    train_data$value_prev_7dav[train_data[[cov]] == 1] <- frac_adj_with_pseudo(
      train_data, cov, pseudo_num, pseudo_denum, "value_prev_7dav_num", "value_prev_7dav_denom")
    
    #For testing
    test_data$value_raw[test_data[[cov]] == 1] <- frac_adj_with_pseudo(
      test_data, cov, pseudo_num, pseudo_denum, "value_raw_num", "value_raw_denom")
    test_data$value_7dav[test_data[[cov]] == 1] <- frac_adj_with_pseudo(
      test_data, cov, pseudo_num, pseudo_denum, "value_7dav_num", "value_7dav_denom")
    test_data$value_prev_7dav[test_data[[cov]] == 1] <- frac_adj_with_pseudo(
      test_data, cov, pseudo_num, pseudo_denum, "value_prev_7dav_num", "value_prev_7dav_denom")
    
    test_data$pseudo_num[test_data[[cov]] == 1] = pseudo_num
    test_data$pseudo_denum[test_data[[cov]] == 1] = pseudo_denum
  }
  
  train_data$log_value_raw = log(train_data$value_raw)
  train_data$log_value_7dav = log(train_data$value_7dav)
  train_data$log_value_prev_7dav = log(train_data$value_prev_7dav)
  train_data$log_7dav_slope = train_data$log_value_7dav - train_data$log_value_prev_7dav
  
  test_data$log_value_raw = log(test_data$value_raw)
  test_data$log_value_7dav = log(test_data$value_7dav)
  test_data$log_value_prev_7dav = log(test_data$value_prev_7dav)
  test_data$log_7dav_slope = test_data$log_value_7dav - test_data$log_value_prev_7dav
  
  return (list(train_data, test_data))
}
