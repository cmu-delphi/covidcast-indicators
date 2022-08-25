#' Corrected estimates from a single local signal
#' 
#' @template df-template
#' @param export_dir path to save output
#' @template taus-template
#' @param test_date_list Date vector of dates to make predictions for
#' @param test_lags integer vector of number of days ago to predict for
#' @param value_cols character vector of numerator and/or denominator field names
#' @param training_days integer number of days to use for training
#' @param testing_window the testing window used for saving the runtime. Could
#'     set it to be 1 if time allows
#' @param ref_lag max lag to use for training
#' @template value_type-template
#' @param lambda the level of lasso penalty
#' @param lp_solver the lp solver used in Quantgen
#'
#' @importFrom dplyr %>% filter
#' @importFrom plyr rbind.fill
#' @importFrom tidyr drop_na
#' @importFrom rlang .data
#'
#' @export
run_backfill_local <- function(df, export_dir, taus = TAUS,
                         test_date_list, test_lags = TEST_LAGS,
                         value_cols, training_days = TRAINING_DAYS, testing_window = TESTING_WINDOW,
                         ref_lag = REF_LAG, value_type, lambda = LAMBDA, lp_solver = LP_SOLVER) {
  # Get all the locations that are considered
  geo_list <- unique(df[df$time_value %in% test_date_list, "geo_value"])
  # Build model for each location
  res_list = list()
  res_indx = 1
  coef_df_list = list()

  for (geo in geo_list) {
    subdf <- df %>% filter(.data$geo_value == geo) %>% filter(.data$lag < ref_lag)
    min_refd <- min(subdf$time_value)
    max_refd <- max(subdf$time_value)
    subdf <- fill_rows(subdf, "time_value", "lag", min_refd, max_refd)
    if (value_type == "count") { # For counts data only
      combined_df <- fill_missing_updates(subdf, value_cols[1], "time_value", "lag")
      combined_df <- add_7davs_and_target(combined_df, "value_raw", "time_value", "lag", ref_lag)
    } else if (value_type == "fraction"){
      combined_num_df <- fill_missing_updates(subdf, value_cols[1], "time_value", "lag")
      combined_num_df <- add_7davs_and_target(combined_num_df, "value_raw", "time_value", "lag", ref_lag)
          
      combined_denom_df <- fill_missing_updates(subdf, value_cols[2], "time_value", "lag")
      combined_denom_df <- add_7davs_and_target(combined_denom_df, "value_raw", "time_value", "lag", ref_lag)
          
      combined_df <- merge(combined_num_df, combined_denom_df,
                           by=c("time_value", "issue_date", "lag", "target_date"), all.y=TRUE,
                           suffixes=c("_num", "_denom"))
    }
    combined_df <- add_params_for_dates(combined_df, "time_value", "lag")
    
    for (test_date in test_date_list) {
      geo_train_data = combined_df %>% 
        filter(.data$issue_date < test_date) %>%
        filter(.data$target_date <= test_date) %>%
        filter(.data$target_date > test_date - training_days) %>%
        drop_na()
      geo_test_data = combined_df %>% 
        filter(.data$issue_date >= test_date) %>%
        filter(.data$issue_date < test_date+testing_window) %>%
        drop_na()
      if (nrow(geo_test_data) == 0) next
      if (nrow(geo_train_data) <= 200) next
      if (value_type == "fraction"){
        geo_prior_test_data = combined_df %>% 
          filter(.data$issue_date > test_date-7) %>%
          filter(.data$issue_date <= test_date)
            
        updated_data <- ratio_adj(geo_train_data, geo_test_data, geo_prior_test_data)
        geo_train_data <- updated_data[[1]]
        geo_test_data <- updated_data[[2]]
      }
      
      max_raw = sqrt(max(geo_train_data$value_raw))
      for (test_lag in test_lags){
        filtered_data <- data_filteration(test_lag, geo_train_data, geo_test_data)
        train_data <- filtered_data[[1]]
        test_data <- filtered_data[[2]]
            
        updated_data <- add_sqrtscale(train_data, test_data, max_raw, "value_raw")
        train_data <- updated_data[[1]]
        test_data <- updated_data[[2]]
        sqrtscale <- updated_data[[3]]
            
        covariates <- list(
          Y7DAV, paste0(WEEKDAYS_ABBR, "_ref"), paste0(WEEKDAYS_ABBR, "_issue"),
          WEEK_ISSUES, SLOPE, SQRTSCALE
        )
        params_list <- c(YITL, as.vector(unlist(covariates)))
            
        # Model training and testing
        prediction_results <- model_training_and_testing(
            train_data, test_data, taus, params_list, lp_solver,
            lambda, test_date, geo
        )
        test_data <- prediction_results[[1]]
        coefs <- prediction_results[[2]]
        test_data <- evl(test_data, taus)
        test_data$test_date <- test_date
        coefs$test_date <- test_date
        coefs$test_lag <- test_lag
        coefs$geo_value <- geo
        
        res_list[[res_indx]] = test_data
        coef_df_list[[res_indx]] = coefs
        res_indx = res_indx+1
        export_test_result(test_data, coefs, export_dir,
                           geo, test_lag)
      }# End for test lags
    }# End for test date list
    result_df = do.call(rbind, res_list)
    coefs_df = do.call(rbind.fill, coef_df_list)
    export_test_result(result_df, coefs_df, export_dir, geo)
  }# End for geo list
}

#' Main function to correct a single local signal
#'
#' @param data_path path to the input data files
#' @param export_dir path to save output
#' @param test_start_date Date to start making predictions on
#' @param test_end_date Date to stop making predictions on
#' @param training_days integer number of days to use for training
#' @param testing_window the testing window used for saving the runtime. Could
#'     set it to be 1 if time allows
#' @template value_type-template
#' @param num_col name of numerator column in the input dataframe
#' @param denom_col name of denominator column in the input dataframe
#' @param lambda the level of lasso penalty
#' @param ref_lag max lag to use for training
#' @param lp_solver the lp solver used in Quantgen
#'
#' @importFrom readr read_csv
#' 
#' @export
main_local <- function(data_path, export_dir, 
                 test_start_date, test_end_date, training_days = TRAINING_DAYS, testing_window = TESTING_WINDOW,
                 value_type, num_col, denom_col, 
                 lambda = LAMBDA, ref_lag = REF_LAG, lp_solver = LP_SOLVER){
  # Check input data
  df = read_csv(data_path)

  # Check data type and required columns
  result <- validity_checks(df, value_type, num_col, denom_col)
  df <- result[["df"]]
  value_cols <- result[["value_cols"]]
  
  # Get test date list according to the test start date
  if (is.null(test_start_date)){
    test_start_date = max(df$issue_date)
  } else {
    test_start_date = as.Date(test_start_date)
  }
  
  if (is.null(test_end_date)){
    test_end_date = max(df$issue_date)
  } else {
    test_end_date = as.Date(test_end_date)
  }

  test_date_list = seq(test_start_date, test_end_date, by="days")
  
  # Check available training days
  training_days_check(df$issue_date, training_days)
  
  run_backfill_local(df, export_dir, TAUS,
               test_date_list, TEST_LAGS,
               value_cols, training_days, testing_window,
               ref_lag, value_type, lambda, lp_solver)
}
