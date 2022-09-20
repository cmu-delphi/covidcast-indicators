#' Get backfill-corrected estimates for a single signal + geo combination
#' 
#' @template df-template
#' @template params-template
#' @template refd_col-template
#' @template lag_col-template
#' @template signal_suffixes-template
#' @template indicator-template
#' @template signal-template
#' 
#' @importFrom dplyr %>% filter select group_by summarize across everything
#' @importFrom tidyr drop_na
#' @importFrom rlang .data .env
#' 
#' @export
run_backfill <- function(df, params, refd_col = "time_value",
                         lag_col = "lag", signal_suffixes = c(""),
                         indicator = "", signal = "") {
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
    
    geo_levels <- params$geo_levels
    if ("state" %in% geo_levels) {
      # If state included, do it last since state processing modifies the
      # `df` object.
      geo_levels <- c(setdiff(geo_levels, c("state")), "state")
    }
    
    for (geo_level in geo_levels) {
      # Get full list of interested locations
      if (geo_level == "state") {
        # Drop county field and make new "geo_value" field from "state_id".
        # Aggregate counties up to state level
        df <- df %>%
          dplyr::select(-.data$geo_value, geo_value = .data$state_id) %>%
          dplyr::group_by(across(c("geo_value", refd_col, lag_col))) %>%
          # Summarized columns keep original names
          dplyr::summarize(across(everything(), sum))
      }
      geo_list <- unique(df$geo_value)
      if (geo_level == "county") {
        # Keep only 200 most populous (within the US) counties
        geo_list <- filter_counties(geo_list)
      }
      
      model_path_prefix <- generate_model_filename_prefix(
        indicator, signal, geo_level, signal_suffix, lambda)

      test_data_list <- list()
      coef_list <- list()
      for (value_type in params$value_types) {
        test_data_list[[value_type]] <- list()
        coef_list[[value_type]] <- list()
      }
      # Build model for each location
      for (geo in geo_list) {
        subdf <- df %>% filter(.data$geo_value == .env$geo) %>% filter(.data$lag < params$ref_lag)
        min_refd <- min(subdf[[refd_col]])
        max_refd <- max(subdf[[refd_col]])
        subdf <- fill_rows(subdf, refd_col, lag_col, min_refd, max_refd)
        
        for (value_type in params$value_types) {
          # Handle different signal types
          if (value_type == "count") { # For counts data only
            combined_df <- fill_missing_updates(subdf, num_col, refd_col, lag_col)
            combined_df <- add_7davs_and_target(combined_df, "value_raw", refd_col, lag_col)
            
          } else if (value_type == "fraction") {
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
          combined_df <- combined_df %>% filter(.data$lag < params$ref_lag)

          test_date <- min(params$test_dates)
          geo_train_data <- combined_df %>%
            filter(.data$issue_date < .env$test_date) %>%
            filter(.data$target_date <= .env$test_date) %>%
            filter(.data$target_date > .env$test_date - params$training_days) %>%
            drop_na()
          geo_test_data <- combined_df %>%
            filter(.data$issue_date %in% params$test_dates) %>%
            drop_na()
          if (nrow(geo_test_data) == 0) next
          if (nrow(geo_train_data) <= 200) next

          if (value_type == "fraction") {
            # Use beta prior approach to adjust fractions
            geo_prior_test_data = combined_df %>%
              filter(.data$issue_date > .env$test_date - 7) %>%
              filter(.data$issue_date <= .env$test_date)
            updated_data <- frac_adj(geo_train_data, geo_test_data, 
                                     geo_prior_test_data, test_date-1, 
                                     model_save_dir, model_path_prefix,
                                     geo, value_type)
            geo_train_data <- updated_data[[1]]
            geo_test_data <- updated_data[[2]]
          }
          max_raw = sqrt(max(geo_train_data$value_raw))
          for (test_lag in c(1:14, 21, 35, 51)) {
            filtered_data <- data_filteration(test_lag, geo_train_data, geo_test_data)
            train_data <- filtered_data[[1]]
            test_data <- filtered_data[[2]]

            updated_data <- add_sqrtscale(train_data, test_data, max_raw, "value_raw")
            train_data <- updated_data[[1]]
            test_data <- updated_data[[2]]
            sqrtscale <- updated_data[[3]]

            covariates <- list(
              Y7DAV, paste0(WEEKDAYS_ABBR, "_issue"),
              paste0(WEEKDAYS_ABBR, "_ref"), WEEK_ISSUES, SLOPE, sqrtscale
            )
            params_list <- c(YITL, as.vector(unlist(covariates)))

            # Model training and testing
            prediction_results <- model_training_and_testing(
              train_data, test_data, params$taus, params_list, params$lp_solver, 
              params$lambda, model_save_dir = params$model_save_dir, 
              training_end_date = test_date - 1, test_lag = test_lag, 
              geo = geo, value_type = value_type,
              model_path_prefix=model_path_prefix,
              train_models = params$train_models,
              make_predictions = params$make_predictions
            )

            # Model objects are saved during training, so only need to export
            # output if making predictions/corrections
            if (params$make_predictions) {
              test_data <- prediction_results[[1]]
              coefs <- prediction_results[[2]]
              test_data <- evaluate(test_data, params$taus)
              
              idx <- length(test_data_list[[value_type]]) + 1
              test_data_list[[value_type]][[idx]] <- test_data
              coef_list[[value_type]][[idx]] <- coefs
            }
          }# End for test lags
        }# End for value types
      }# End for geo list
      if (params$make_predictions) {
        for (value_type in params$value_types) {
          test_combined <- do.call(plyr::rbind.fill, test_data_list[[value_type]]) 
          coef_combined <- do.call(plyr::rbind.fill, coef_list[[value_type]]) 
          export_test_result(test_combined, coef_combined, training_end_date,
                             value_type, params$export_dir, model_path_prefix)
        }
      }
    }# End for geo type
  }# End for signal suffixes
}

#' Perform backfill correction on all desired signals and geo levels
#' 
#' @template params-template
#'
#' @importFrom dplyr bind_rows
#' @importFrom parallel detectCores
#' 
#' @export
main <- function(params) {
  if (!params$train_models && !params$make_predictions) {
    message("both model training and prediction generation are turned off; exiting")
    return
  }

  ## Set default number of cores for mclapply to half of those available.
  if (params$parallel) {
    cores <- detectCores()

    if (is.na(cores)) {
      warning("Could not detect the number of CPU cores; parallel mode disabled")
      params$parallel <- FALSE
    } else {
      options(mc.cores = min(params$parallel_max_cores, floor(cores / 2)))
    }
  }

  # Loop over every indicator + signal combination.
  for (input_group in INDICATORS_AND_SIGNALS) {
    files_list <- get_files_list(
      input_group$indicator, input_group$signal, params, input_group$sub_dir
    )
    
    if (length(files_list) == 0) {
      warning(str_interp(
        "No files found for indicator {input_group$indicator} signal {input_group$signal}, skipping"
      ))
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
      warning(str_interp(
        "No data available for indicator {input_group$indicator} signal {input_group$signal}, skipping"
      ))
      next
    }
    
    # Check data type and required columns
    for (value_type in params$value_types) {
      result <- validity_checks(
        input_data, value_type,
        params$num_col, params$denom_col, input_group$name_suffix
      )
      input_data <- result[["df"]]
    }
    
    # Check available training days
    training_days_check(input_data$issue_date, params$training_days)
    
    # Perform backfill corrections and save result
    run_backfill(input_data, params,
      indicator = input_group$indicator, signal = input_group$signal,
      signal_suffixes = input_group$name_suffix)
  }
}
