#' library(tidyverse)
#' library(Matrix)
#' library(stats)
#' library(tidyverse)
#' library(dplyr) 
#' library(lubridate)
#' library(zoo)
#' library(dplyr)
#' library(ggplot2)
#' library(stringr)
#' library(plyr)
#' library(MASS)
#' library(stats4)
#' 
#' library(covidcast)
#' library(evalcast)
#' library(quantgen) 
#' library(gurobi)
#' lp_solver = "gurobi" # LP solver to use in quantile_lasso(); "gurobi" or "glpk"

#' Tempt usage
#' params = list()
#' customize 
#' params$ref_lag: reference lag, after x days, the update is considered to be the response. 60 is a reasonable choice for CHNG outpatient data
#' params$data_path: link to the input data file
#' params$testing_window: the testing window used for saving the runtime. Could set it to be 1 if time allows
#' params$test_dates: list of two elements, the first one is the start date and the second one is the end date
#' params$training_days: set it to be 270 or larger if you have enough data
#' params$num_col: the column name for the counts of the numerator, e.g. the number of COVID claims 
#' params$denom_col: the column name for the counts of the denominator, e.g. the number of total claims
#' params$geo_level: list("state", "county")

#' Main function for getting backfill corrected estimates
#' 
#' @param params
#' 
#' @import constants
#' @import preprocessing
#' @import beta_prior_estimation
#' @import model
#' 
#' @export
run_backfill <- function(params){
  # Get the input data
  df <- read_data(params$data_path)
  refd_col <- "time_value"
  lag_col <- "lag"
  testing_window <- params$testing_window
  ref_lag <- params$ref_lag
  min_refd <- test_date_list[1]
  max_refd <- test_date_list[length(test_date_list)]
  
  for (geo_level in params$geo_levels){
    # Get full list of interested locations
    geo_list <- unique(df$geo_value)
    # Build model for each location
    for (geo in geo_list) {
      subdf <- df %>% filter(geo_value == geo) %>% filter(lag < ref_lag)
      subdf <- fill_rows(subdf, refd_col, lag_col, min_refd, max_refd)
      for (value_type in value_types){
        if (value_type == "count") { # For counts data only
          combined_df <- fill_missing_updates(subdf, params$num_col, refd_col, lag_col)
          combined_df <- add_7davs_and_target(combined_df, "value_raw", refd_col, lag_col)
        } else if (value_type == "ratio"){
          combined_num_df <- fill_missing_updates(subdf, params$num_col, refd_col, lag_col)
          combined_num_df <- add_7davs_and_target(combined_num_df, "value_raw", refd_col, lag_col)
          
          combined_denom_df <- fill_missing_updates(subdf, params$denom_col, refd_col, lag_col)
          combined_denom_df <- add_7davs_and_target(combined_denom_df, "value_raw", refd_col, lag_col)
          
          combined_df <- merge(combined_num_df, combined_denom_df,
                               by=c(refd_col, "issue_date", lag_col, "target_date"), all.y=TRUE,
                               suffixes=c("_num", "_denom"))
        }
        combined_df <- add_params_for_dates(combined_df, refd_col, lag_col)
        test_date_list <- get_test_dates(combined_df, params$test_dates)
        for (test_date in test_date_list){
          geo_train_data = combined_df %>% 
            filter(issue_date < test_date) %>% 
            filter(target_date <= test_date) %>%
            filter(target_date > test_date - training_days) %>%
            drop_na()
          geo_test_data = combined_df %>% 
            filter(issue_date >= test_date) %>% 
            filter(issue_date < test_date+testing_window) %>%
            drop_na()
          if (dim(geo_test_data)[1] == 0) next
          if (dim(geo_train_data)[1] <= 200) next
          if (value_type == "ratio"){
            geo_prior_test_data = combined_df %>% 
              filter(issue_date > test_date-7) %>% 
              filter(issue_date <= test_date)
            
            updated_data <- ratio_adj(geo_train_data, geo_test_data, geo_prior_test_data)
            geo_train_data <- updated_data[[1]]
            geo_test_data <- updated_data[[2]]
          }
          max_raw = sqrt(max(geo_train_data$value_raw))
          for (test_lag in c(1:14, 21, 35, 51)){
            filtered_data <- data_filteration(test_lag, geo_train_data, geo_test_data)
            train_data <- filtered_data[[1]]
            test_data <- filtered_data[[2]]
            
            updated_data <- add_sqrtscale(train_data, test_data, max_raw, "value_raw")
            train_data <- updated_data[[1]]
            test_data <- updated_data[[2]]
            sqrtscale <- updated_data[[3]]
            
            covariates <- list(y7dav, wd, wd2, wm, slope, sqrtscale)
            params_list <- c(yitl, as.vector(unlist(covariates)))
            
            # Model training and testing
            prediction_results <- model_training_and_testing(
              train_data, test_data, taus, params_list, lp_solver, lambda, test_date)
            test_data <- prediction_results[[1]]
            coefs <- prediction_results[[2]]
            test_data <- evl(test_data, params$taus)
            
            export_test_result(test_data, coefs, params$export_dir, geo_level,
                               geo, test_lag)
          }# End for test lags
        }# End for test date list
      }# End for value types
    }# End for geo lsit
  }# End for geo level
  
  
}