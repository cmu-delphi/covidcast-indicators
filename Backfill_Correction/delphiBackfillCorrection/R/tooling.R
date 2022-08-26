#' Corrected estimates from a single local signal
#' 
#' @template df-template
#' @template export_dir-template
#' @param test_date_list Date vector of dates to make predictions for
#' @param value_cols character vector of numerator and/or denominator field names
#' @template value_type-template
#' @template taus-template
#' @param test_lags integer vector of number of days ago to predict for
#' @template training_days-template
#' @template testing_window-template
#' @template ref_lag-template
#' @template lambda-template
#' @template lp_solver-template
#'
#' @importFrom dplyr %>% filter
#' @importFrom plyr rbind.fill
#' @importFrom tidyr drop_na
#' @importFrom rlang .data .env
#'
#' @export
run_backfill_local <- function(df, export_dir, test_date_list, value_cols, value_type,
                         taus = TAUS, test_lags = TEST_LAGS,
                         training_days = TRAINING_DAYS, testing_window = TESTING_WINDOW,
                         ref_lag = REF_LAG, lambda = LAMBDA, lp_solver = LP_SOLVER) {
  # Get all the locations that are considered
  geo_list <- unique(df[df$time_value %in% test_date_list, "geo_value"])
  # Build model for each location
  res_list = list()
  res_indx = 1
  coef_df_list = list()

  for (geo in geo_list) {
    subdf <- df %>% filter(.data$geo_value == .env$geo) %>% filter(.data$lag < .env$ref_lag)
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
        filter(.data$issue_date < .env$test_date) %>%
        filter(.data$target_date <= .env$test_date) %>%
        filter(.data$target_date > .env$test_date - .env$training_days) %>%
        drop_na()
      geo_test_data = combined_df %>% 
        filter(.data$issue_date >= .env$test_date) %>%
        filter(.data$issue_date < .env$test_date + .env$testing_window) %>%
        drop_na()
      if (nrow(geo_test_data) == 0) next
      if (nrow(geo_train_data) <= 200) next
      if (value_type == "ratio"){
        geo_prior_test_data = combined_df %>% 
          filter(.data$issue_date > .env$test_date - 7) %>%
          filter(.data$issue_date <= .env$test_date)
            
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
        test_data <- evaluate(test_data, taus)
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
#' @template input_dir-template
#' @template export_dir-template
#' @param test_start_date Date or string in the format "YYYY-MM-DD" to start
#'     making predictions on
#' @param test_end_date Date or string in the format "YYYY-MM-DD" to stop
#'     making predictions on
#' @template num_col-template
#' @template denom_col-template
#' @template value_type-template
#' @template training_days-template
#' @template testing_window-template
#' @template lambda-template
#' @template ref_lag-template
#' @template lp_solver-template
#'
#' @importFrom readr read_csv
#' 
#' @export
main_local <- function(input_dir, export_dir,
                 test_start_date, test_end_date,
                 num_col, denom_col,value_type = c("count", "ratio"),
                 training_days = TRAINING_DAYS, testing_window = TESTING_WINDOW,
                 lambda = LAMBDA, ref_lag = REF_LAG, lp_solver = LP_SOLVER){
  value_type <- match.arg(value_type)

  # Check input data
  df = read_csv(input_dir)

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
  
  run_backfill_local(df, export_dir,
               test_date_list, value_cols, value_type,
               TAUS, TEST_LAGS, training_days, testing_window,
               ref_lag, lambda, lp_solver)
}
