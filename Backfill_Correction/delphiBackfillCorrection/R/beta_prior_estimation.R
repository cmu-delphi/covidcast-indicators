#' Functions for Beta Prior Approach.
#' This is used only for the ratio prediction e.g. fraction of Covid claims, 
#' percentage of positive tests. We assume that the ratio follows a beta distribution
#' that is day-of-week dependent. A quantile regression model is used first with lasso
#' penalty for supporting quantile estimation and then a non-linear minimization is used
#' for prior estimation. 
lp_solver <- "gurobi"

#' Sum of squared error
#' 
#' @param fit estimated values
#' @param actual actual values
#' 
#' @export
delta <- function(fit, actual) sum((fit-actual)^2)

#' Generate objection function
#' @param theta parameters for the distribution in log scale 
#' @param prob the expected probabilities
#' 
#' @importFrom stats pbeta
#' 
#' @export
objective <- function(theta, x, prob, ...) {
  ab <- exp(theta) # Parameters are the *logs* of alpha and beta
  fit <- pbeta(x, ab[1], ab[2])
  return (delta(fit, prob))
}

#' Main function for the beta prior approach
#' Estimate the priors for the beta distribution based on data for 
#' a certain day of a week
#' 
#' @param train_data Data Frame for training
#' @param prior_test_data Data Frame for testing 
#' @param dw column name to indicate which day of a week it is
#' @param taus vector of considered quantiles
#' @param params_list the list of parameters for training
#' @param response the column name of the response variable
#' @param lp_solver the lp solver used in Quantgen
#' @param labmda the level of lasso penalty
#' @param start the initialization of the the points in nlm
#' @param base_pseudo_denum the pseudo counts added to denominator if little data for training
#' @param base_pseudo_num the pseudo counts added to numerator if little data for training
#' 
#' @import MASS
#' @import stats4
#' @import gurobi
#' @import Matrix
#' @import tidyverse
#' @import dplyr
#' @importFrom quantgen quantile_lasso
#' @importFrom constants lp_solver
est_priors <- function(train_data, prior_test_data, cov, taus, 
                       params_list, response, lp_solver, lambda, 
                       start=c(0, log(10)),
                       base_pseudo_denom=1000, base_pseudo_num=10){
  sub_train_data <- train_data %>% filter(train_data[[cov]] == 1)
  sub_test_data <- prior_test_data %>% filter(prior_test_data[[cov]] == 1)
  if (dim(sub_test_data)[1] == 0) {
    pseudo_denom <- base_pseudo_denom
    pseudo_num <- base_pseudo_num
  } else {
    # Using quantile regression to get estimated quantiles at log scale
    quantiles <- list()
    for (idx in 1:length(taus)){
      tau <- taus[idx]
      obj <- quantile_lasso(as.matrix(sub_train_data[params_list]), 
                           sub_train_data[response], tau = tau,
                           lambda = lambda, stand = FALSE, lp_solver = lp_solver)
      y_hat_all <- as.numeric(predict(obj, newx = as.matrix(sub_test_data[params_list])))
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

#' Update ratio based on the pseudo counts for numerators and denominators 
#' 
#' @param data Data Frame
#' @param dw character to indicate the day of a week. Can be NULL for all the days
#' @param pseudo_num the estimated counts to be added to numerators
#' @param pseudo_denom the estimated counts to be added to denominators
#' @param num_col the column name for the numerator
#' @param denom_col the column name for the denominator
#' 
#' @export
ratio_adj_with_pseudo <- function(data, cov, pseudo_num, pseudo_denom, num_col, denom_col){
  if (is.null(cov)){
    num_adj <- data[[num_col]]  + pseudo_num
    denom_adj <- data[[denom_col]]  + pseudo_denom
  } else {
    num_adj <- data[[num_col]][data[[cov]] == 1]  + pseudo_num
    denom_adj <- data[data[[cov]] == 1, denom_col]  + pseudo_denom
  }
  return (num_adj / denom_adj)
}

#' Update ratio using beta prior approach
#' 
#' @param train_data training data
#' @param test_data testing data
#' @param prior_test_data testing data for the lag -1 model
#' 
#' @importFrom constants taus, dw, lp_solver
#' @export
ratio_adj <- function(train_data, test_data, prior_test_data){
  train_data$value_target <- ratio_adj_with_pseudo(train_data, NULL, 1, 100, "value_target_num", "value_target_denom")
  train_data$value_7dav <- ratio_adj_with_pseudo(train_data, NULL, 1, 100, "value_7dav_num", "value_7dav_denom")
  test_data$value_target <- ratio_adj_with_pseudo(test_data, NULL, 1, 100, "value_target_num", "value_target_denom")
  prior_test_data$value_7dav <- ratio_adj_with_pseudo(prior_test_data, NULL, 1, 100, "value_7dav_num", "value_7dav_denom")
  
  train_data$log_value_target <- log(train_data$value_target)
  train_data$log_value_7dav <- log(train_data$value_7dav)
  test_data$log_value_target <- log(test_data$value_target)
  prior_test_data$log_value_7dav <- log(prior_test_data$value_7dav)
  
  pre_params_list = c("Mon_ref", "Tue_ref", "Wed_ref", "Thurs_ref", "Fri_ref", "Sat_ref",
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
  
  for (cov in c("Mon_ref", "Tue_ref", "Wed_ref", "Thurs_ref", "Fri_ref", "Sat_ref", "Sun_ref")){
    pseudo_counts <- est_priors(train_data, prior_test_data, cov, taus, 
                        pre_params_list, "log_value_target", lp_solver, lambda=0.1)
    pseudo_denum = pseudo_counts[1] + pseudo_counts[2]
    pseudo_num = pseudo_counts[1]
    # update current data
    # For training
    train_data$value_raw[train_data[[cov]] == 1] <- ratio_adj_with_pseudo(
      train_data, cov, pseudo_num, pseudo_denum, "value_raw_num", "value_raw_denom")
    train_data$value_7dav[train_data[[cov]] == 1] <- ratio_adj_with_pseudo(
      train_data, cov, pseudo_num, pseudo_denum, "value_7dav_num", "value_7dav_denom")
    train_data$value_prev_7dav[train_data[[cov]] == 1] <- ratio_adj_with_pseudo(
      train_data, cov, pseudo_num, pseudo_denum, "value_prev_7dav_num", "value_prev_7dav_denom")
    
    #For testing
    test_data$value_raw[test_data[[cov]] == 1] <- ratio_adj_with_pseudo(
      test_data, cov, pseudo_num, pseudo_denum, "value_raw_num", "value_raw_denom")
    test_data$value_7dav[test_data[[cov]] == 1] <- ratio_adj_with_pseudo(
      test_data, cov, pseudo_num, pseudo_denum, "value_7dav_num", "value_7dav_denom")
    test_data$value_prev_7dav[test_data[[cov]] == 1] <- ratio_adj_with_pseudo(
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