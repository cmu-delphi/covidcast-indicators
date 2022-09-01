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
#' params$lp_solver: LP solver to use in quantile_lasso(); "gurobi" or "glpk"

#' Get backfill-corrected estimates for a single signal + geo combination
#' 
#' @param df dataframe of input data containing a single indicator + signal +
#'     level of geographic coverage.
#' @param value_type string describing signal type of "count" and "ratio".
#' @param params named list containing modeling and data settings. Must include
#'     the following elements: `ref_lag`, `testing_window`, `test_dates`,
#'     `training_days`, `num_col`, `taus`, `lambda`, `export_dir`, `lp_solver`,
#'     and `data_path` (input dir).
#' @param refd_col string containing name of reference date field within `df`.
#' @param lag_col string containing name of lag field within `df`.
#' 
#' @import constants
#' @import preprocessing
#' @import beta_prior_estimation
#' @import model
#' @importFrom dplyr select %>% group_by summarize across everything
#' 
#' @export
run_backfill <- function(df, value_type, params,
                         refd_col = "time_value", lag_col = "lag",
                         signal_suffixes = c("")) {
  # If county included, do county first since state processing modifies
  # `df` object.
  geo_levels <- params$geo_level
  if ("county" in ) {
    geo_levels <- c("county", setdiff(geo_levels, c("county")))
  }
  for (geo_level in geo_levels) {
    # Get full list of interested locations
    if (geo_level == "state") {
      # Drop county field and make new "geo_value" field from "state_id".
      # Aggregate counties up to state level
      df <- df %>%
        select(-geo_value, geo_value = state_id) %>%
        group_by(across(c("geo_value", refd_col, lag_col))) %>%
        # Summarized columns keep original names
        summarize(across(everything(), sum))
    }
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

        for (value_type in params$value_types) {
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
                params$lp_solver, params$lambda, test_date
              )
              test_data <- prediction_results[[1]]
              coefs <- prediction_results[[2]]
              test_data <- evl(test_data, params$taus)

              export_test_result(test_data, coefs, params$export_dir, geo_level,
                                 geo, test_lag)
            }# End for test lags
          }# End for test date list
        }# End for value types
      }# End for signal suffixes
    }# End for geo list
  }# End geo type
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
  # Loop over every indicator + signal combination.
  for (input_group in indicators_and_signals) {
    files_list <- get_files_list(
      input_group$indicator, input_group$signal, params, input_group$sub_dir
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
    for (value_type in params$value_types) {
      validity_checks(input_data, value_type)
    }
    
    # Check available training days
    training_days_check(input_data$issue_date, params$training_days)
    
    # Perform backfill corrections and save result
    run_backfill(input_data,
                 params, signal_suffixes = input_group$name_suffix
    )
  }
}
