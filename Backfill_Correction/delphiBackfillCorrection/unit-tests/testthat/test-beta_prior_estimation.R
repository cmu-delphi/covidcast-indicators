## Functions for Beta Prior Approach.
## This is used only for the ratio prediction e.g. fraction of Covid claims, 
## percentage of positive tests. We assume that the ratio follows a beta distribution
## that is day-of-week dependent. A quantile regression model is used first with lasso
## penalty for supporting quantile estimation and then a non-linear minimization is used
## for prior estimation. 
library(nlm)
library(quantgen) 
library(gurobi)
library(tidyverse)
library(Matrix)
library(dplyr)

lp_solver <- "gurobi"

delta <- function(fit, actual) sum((fit-actual)^2)

objective <- function(theta, x, prob, ...) {
  ab <- exp(theta) # Parameters are the *logs* of alpha and beta
  fit <- pbeta(x, ab[1], ab[2])
  return (delta(fit, prob))
}

## Main function for the beta prior approach
## Estimate the priors for the beta distribution based on data for 
## a certain day of a week
est_priors <- function(train_data, prior_test_data, dw, taus, 
                       params_list, response, lp_solver, lambda, 
                       start=c(0, log(10),
                       base_pseudo_denum=1000, base_pseudo_num=10)){
  sub_train_data <- train_data %>% filter(train_data[dw] == 1)
  sub_test_data <- prior_test_data %>% filter(prior_test_data[dw] == 1)
  if (dim(sub_test_data)[1] == 0) {
    pseudo_denum <- base_pseudo_denum
    pseudo_num <- base_pseudo_num
  } else {
    # Using quantile regressison to get estimated quantiles at log scale
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
  return (pseudo_denom, pseudo_num)
}

## Add pseudo counts for numerators and denominators 
ratio_adj <- function(data, dw, pseudo_num, pseudo_denom, num_col, denom_col){
  num_adj <- data[data[dw] == 1, num_col]  + pseudo_num
  denom_adj <- data[data[dw] == 1, denom_col]  + pseudo_denom
  return (num_adj / denom_adj)
}
