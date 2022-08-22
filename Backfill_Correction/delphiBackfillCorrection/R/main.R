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
#' params$ref_lag: reference lag, after x days, the update is considered to be
#'     the response. 60 is a reasonable choice for CHNG outpatient data
#' params$data_path: link to the input data file
#' params$testing_window: the testing window used for saving the runtime. Could 
#'     set it to be 1 if time allows
#' params$test_dates: list of two elements, the first one is the start date and 
#'     the second one is the end date
#' params$training_days: set it to be 270 or larger if you have enough data
#' params$num_col: the column name for the counts of the numerator, e.g. the 
#'     number of COVID claims 
#' params$denom_col: the column name for the counts of the denominator, e.g. the
#'     number of total claims
#' params$geo_level: list("state", "county")
#' params$taus: ??
#' params$lambda: ??
#' params$export_dir: ??

#' Get backfill-corrected estimates for a single signal + geo combination
#' 
#' @param df dataframe of input data containing a single indicator + signal +
#'     level of geographic coverage.
#' @param value_type string describing signal type of "count" and "ratio".
#' @param geo_level string describing geo coverage of input data. "state" or
#'     "county". If "county" is selected, only data from the 200 most populous
#'     counties in the US (*not* the dataset) will be used.
#' @param params named list containing modeling and data settings. Must include
#'     the following elements: `ref_lag`, `testing_window`, `test_dates`,
#'     `training_days`, `num_col`, `taus`, `lambda`, and `export_dir`.
#' @param refd_col string containing name of reference date field within `df`.
#' @param lag_col string containing name of lag field within `df`.
#' 
#' @import constants
#' @import preprocessing
#' @import beta_prior_estimation
#' @import model
#' 
#' @export
run_backfill <- function(df, value_type, geo_level, params,
                         refd_col = "time_value", lag_col = "lag") {
  # Get full list of interested locations
  geo_list <- unique(df$geo_value)
  if (geo_level == "county") {
    # Keep only 200 most populous (within the US) counties
    geo_list <- filter_counties(geo_list)
  }
  
  # Build model for each location
  for (geo in geo_list) {
    subdf <- df %>% filter(geo_value == geo) %>% filter(lag < params$ref_lag)
    min_refd <- min(subdf[[refd_col]])
    max_refd <- max(subdf[[refd_col]])
    subdf <- fill_rows(subdf, refd_col, lag_col, min_refd, max_refd)
    
    # Handle different signals
    if (value_type == "count") { # For counts data only
      combined_df <- fill_missing_updates(subdf, params$num_col, refd_col, lag_col)
      combined_df <- add_7davs_and_target(combined_df, "value_raw", refd_col, lag_col)
      
    } else if (value_type == "ratio"){
      combined_num_df <- fill_missing_updates(subdf, params$num_col, refd_col, lag_col)
      combined_num_df <- add_7davs_and_target(combined_num_df, "value_raw", refd_col, lag_col)
      
      combined_denom_df <- fill_missing_updates(subdf, params$denom_col, refd_col, lag_col)
      combined_denom_df <- add_7davs_and_target(combined_denom_df, "value_raw", refd_col, lag_col)
      
      combined_df <- merge(
        combined_num_df, combined_denom_df,
        by=c(refd_col, "issue_date", lag_col, "target_date"), all.y=TRUE,
        suffixes=c("_num", "_denom")
      )
    }
    combined_df <- add_params_for_dates(combined_df, refd_col, lag_col)
    test_date_list <- get_test_dates(combined_df, params$test_dates)
    
    for (test_date in test_date_list){
      geo_train_data = combined_df %>% 
        filter(issue_date < test_date) %>% 
        filter(target_date <= test_date) %>%
        filter(target_date > test_date - params$training_days) %>%
        drop_na()
      geo_test_data = combined_df %>% 
        filter(issue_date >= test_date) %>% 
        filter(issue_date < test_date + params$testing_window) %>%
        drop_na()
      if (dim(geo_test_data)[1] == 0) next
      if (dim(geo_train_data)[1] <= 200) next
      
      if (value_type == "ratio"){
        geo_prior_test_data = combined_df %>% 
          filter(issue_date > test_date - 7) %>% 
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
          train_data, test_data, params$taus, params_list,
          lp_solver, params$lambda, test_date
        )
        test_data <- prediction_results[[1]]
        coefs <- prediction_results[[2]]
        test_data <- evl(test_data, params$taus)
        
        export_test_result(test_data, coefs, params$export_dir, geo_level,
                           geo, test_lag)
      }# End for test lags
    }# End for test date list
  }# End for geo list
}

#' Perform backfill correction on all desired signals and geo levels
#' 
#' @import tidyverse
#' @import utils
#' @import constants
#' @import preprocessing
#' @import beta_prior_estimation
#' @import model
#' @importFrom dplyr bind_rows
#' 
#' @export
main <- function(params, ...){
  # Create groups by indicator, signal, and geo type. Cover all params$geo_level
  # values (should be state and county)
  # Set associated value_type as well.
  groups <- product(INDICATORS_AND_SIGNALS, params$geo_level)
  
  # Loop over every indicator + signal + geo type combination.
  for (input_group in groups) {
    # Convert input_group into file names.
    daily_pattern <- create_daily_name(
      input_group$indicator, input_group$signal, input_group$geo_level
    )
    rollup_pattern <- create_rollup_name(
      input_group$indicator, input_group$signal, input_group$geo_level
    )
    
    # Make sure we're reading in both 4-week rollup and daily files.
    daily_input_files <- list.files(params$data_path, pattern = daily_pattern)
    rollup_input_files <- list.files(params$data_path, pattern = rollup_pattern)
    
    ## TODO: what filtering do we need to do on dates?
    
    # Read in all listed files and combine
    input_data <- lapply(
      c(daily_input_files, rollup_input_files),
      function(file) {
        input_data[[file]] <- read_data(file)
      }
    ) %>% bind_rows
    
    # Check data type and required columns
    value_type <- get_value_type(input_group$indicator, input_group$signal)
    validity_checks(input_data, value_type)
    
    # Perform backfill corrections and save result
    run_backfill(input_data, value_type, input_group$geo_level, params)
  }
}

