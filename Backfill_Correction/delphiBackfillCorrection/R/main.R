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
#'     `training_days`, `num_col`, `taus`, `lambda`, `export_dir`, `lp_solver`,
#'     and `data_path` (input dir).
#' @param refd_col string containing name of reference date field within `df`.
#' @param lag_col string containing name of lag field within `df`.
#' 
#' @importFrom dplyr %>% filter
#' @importFrom tidyr drop_na
#' 
#' @export
run_backfill <- function(df, value_type, geo_level, params,
                         refd_col = "time_value", lag_col = "lag",
                         signal_suffixes = c("")) {
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
    
    for (suffix in signal_suffixes) {
      # For each suffix listed in `signal_suffixes`, run training/testing
      # process again. Main use case is for quidel which has overall and
      # age-based signals.
      if (suffix != "") {
        num_col <- paste(params$num_col, suffix, sep = "_")
        denom_col <- paste(params$denom_col, suffix, sep = "_")
      } else {
        num_col <- params$num_col
        denom_col <- params$denom_col
      }
      
      # Handle different signal types
      if (value_type == "count") { # For counts data only
        combined_df <- fill_missing_updates(subdf, num_col, refd_col, lag_col)
        combined_df <- add_7davs_and_target(combined_df, "value_raw", refd_col, lag_col)
        
      } else if (value_type == "ratio"){
        combined_num_df <- fill_missing_updates(subdf, num_col, refd_col, lag_col)
        combined_num_df <- add_7davs_and_target(combined_num_df, "value_raw", refd_col, lag_col)
        
        combined_denom_df <- fill_missing_updates(subdf, denom_col, refd_col, lag_col)
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
        if (nrow(geo_test_data) == 0) next
        if (nrow(geo_train_data) <= 200) next
        
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
            params$lp_solver, params$lambda, test_date
          )
          test_data <- prediction_results[[1]]
          coefs <- prediction_results[[2]]
          test_data <- evl(test_data, params$taus)
          
          export_test_result(test_data, coefs, params$export_dir, geo_level, test_lag)
        }# End for test lags
      }# End for test date list
    }# End for signal suffixes
  }# End for geo list

  return(NULL)
}

#' Perform backfill correction on all desired signals and geo levels
#' 
#' @importFrom dplyr bind_rows
#' 
#' @export
main <- function(params, ...){
  # Load indicator x signal groups. Combine with params$geo_level to get all
  # possible geo x signal combinations.
  groups <- merge(indicators_and_signals, data.frame(geo_level = params$geo_level))
  
  # Loop over every indicator + signal + geo type combination.
  for (input_group in groups) {
    files_list <- get_files_list(
      input_group$indicator, input_group$signal, input_group$geo_level,
      params, input_group$sub_dir
    )
    
    if (length(files_list) == 0) {
      warning(str_interp("No files found for {input_group$indicator} {input_group$signal}, skipping"))
      next
    }
    
    # Read in all listed files and combine
    input_data <- lapply(
      files_list,
      function(file) {
        input_data[[file]] <- read_data(file)
      }
    ) %>% bind_rows
    
    if (nrow(input_data) == 0) {
      warning(str_interp("No data available for {input_group$indicator} {input_group$signal}, skipping"))
      next
    }
    
    # Check data type and required columns
    validity_checks(input_data, input_group$value_type)
    
    # Check available training days
    training_days_check(input_data$issue_date, params$training_days)
    
    # Perform backfill corrections and save result
    run_backfill(input_data, input_group$value_type, input_group$geo_level,
                 params, signal_suffixes = input_group$name_suffix
    )
  }
}
