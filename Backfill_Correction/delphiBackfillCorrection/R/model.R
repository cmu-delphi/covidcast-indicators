#' Filtration for training and testing data with different lags
#' 
#' @param test_lag 
#' @param geo_train_data training data for a certain location
#' @param geo_test_data testing data for a certain location
#' 
#' @export
data_filteration <- function(test_lag, geo_train_data, geo_test_data){
  if (test_lag <= 14){
    test_lag_pad=2
    test_lag_pad1=0
    test_lag_pad2=0
  }else if (test_lag < 51){
    test_lag_pad=7
    test_lag_pad1=6
    test_lag_pad2=7
  }else {
    test_lag_pad=9
    test_lag_pad1=8
    test_lag_pad2=9
  }
  train_data = geo_train_data %>% 
    filter(lag >= test_lag-test_lag_pad ) %>%
    filter(lag <= test_lag+test_lag_pad ) 
  test_data = geo_test_data %>%
    filter(lag >= test_lag-test_lag_pad1) %>%
    filter(lag <= test_lag+test_lag_pad2)
  return (list(train_data, test_data))
}


#' Model training and prediction using quantile regression with Lasso penalty
#' The quantile regression uses the quantile_lasso function from quantgen package
#'
#' @param train_data Data frame for training
#' @param test_data Data frame for testing 
#' @param taus vector of considered quantiles
#' @param params_list the list of column names serving as the covariates
#' @param lp_solver the lp solver used in Quantgen
#' @param lambda the level of lasso penalty
#' @param test_date as.Date
#'
#' @export
model_training_and_testing <- function(train_data, test_data, taus, params_list,  
                                       lp_solver, lambda, test_date){
  success = 0
  coefs_result = list()
  coef_list = c("intercept", paste(params_list, '_coef', sep=''))
  for (tau in taus){
    #options(error=NULL)
    tryCatch(
      expr = {
        # Quantile regression
        obj = quantile_lasso(as.matrix(train_data[params_list]), 
                             train_data$log_value_target, tau = tau,
                             lambda = lambda, stand = FALSE, lp_solver = lp_solver)
        
        y_hat_all = as.numeric(predict(obj, newx = as.matrix(test_data[params_list])))
        test_data[paste0("predicted_tau", as.character(tau))] = y_hat_all
        
        coefs_result[[success+1]] = coef(obj)
        coefs_result[[success+1]]$tau = tau
        success = success + 1
      },
      error=function(e) {print(paste(geo, test_date, as.character(tau), sep="_"))}
    )
  }
  if (success < 9){ return (NULL)}
  coef_combined_result = data.frame(tau=taus,
                           issue_date=test_date)
  coef_combined_result[coef_list] = as.matrix(do.call(rbind, coefs_result))
  
  return (list(test_data, coef_combined_result))
}

#' Evaluation of the test results based on WIS score
#' The WIS score calculation is based on the weighted_interval_score function 
#' from the `evalcast` package from Delphi
#' 
#' @param test_data multiple columns for the prediction results of different 
#'    quantiles. Each row represents an update with certain (reference_date, 
#'    issue_date, location) combination.
#' @param taus vector of quantiles interested
#' 
#' @importFrom evalcast weighted_interval_score
#' 
#' @export
evl <- function(test_data, taus){
  n_row = dim(test_data)[1]
  taus_list = as.list(data.frame(matrix(replicate(n_row, taus), ncol=n_row)))
  
  # Calculate WIS
  predicted_all = as.matrix(test_data[c("predicted_tau0.01", "predicted_tau0.025",
                                        "predicted_tau0.1", "predicted_tau0.25",
                                        "predicted_tau0.5", "predicted_tau0.75",
                                        "predicted_tau0.9", "predicted_tau0.975",
                                        "predicted_tau0.99")])
  predicted_all_exp = exp(predicted_all)
  predicted_trans = as.list(data.frame(t(predicted_all - test_data$log_value_target)))
  predicted_trans_exp = as.list(data.frame(t(predicted_all_exp - test_data$value_target)))
  test_data$wis = mapply(weighted_interval_score, taus_list, predicted_trans, 0)
  test_data$wis_exp = mapply(weighted_interval_score, taus_list, predicted_trans_exp, 0)
  
  return (test_data)
}