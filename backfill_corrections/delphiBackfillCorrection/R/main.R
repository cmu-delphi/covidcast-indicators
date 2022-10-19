#' Get backfill-corrected estimates for a single signal + geo combination
#' 
#' @template df-template
#' @template params-template
#' @template refd_col-template
#' @template lag_col-template
#' @template issued_col-template
#' @template signal_suffixes-template
#' @template indicator-template
#' @template signal-template
#' @param training_end_date the most recent training date
#' 
#' @importFrom dplyr %>% filter select group_by summarize across everything group_split ungroup
#' @importFrom tidyr drop_na
#' @importFrom rlang .data .env
#' 
#' @export
run_backfill <- function(df, params, training_end_date,
                         refd_col = "time_value", lag_col = "lag", issued_col = "issue_date",
                         signal_suffixes = c(""), indicator = "", signal = "") {
  df <- filter(df, .data$lag < params$ref_lag + 30) # a rough filtration to save memory

  geo_levels <- params$geo_levels
  if ("state" %in% geo_levels) {
    # If state included, do it last since state processing modifies the
    # `df` object.
    geo_levels <- c(setdiff(geo_levels, c("state")), "state")
  }
  
  for (geo_level in geo_levels) {
    msg_ts(str_interp("geo level ${geo_level}"))
    # Get full list of interested locations
    if (geo_level == "state") {
      # Drop county field and make new "geo_value" field from "state_id".        
      # Aggregate counties up to state level
      agg_cols <- c("geo_value", issued_col, refd_col, lag_col)
      # Sum all non-agg columns. Summarized columns keep original names
      df <- df %>%
        select(-.data$geo_value, geo_value = .data$state_id) %>%
        group_by(across(agg_cols)) %>%
        summarize(across(everything(), sum)) %>%
        ungroup()
    }
    if (geo_level == "county") {
      # Keep only 200 most populous (within the US) counties
      top_200_geos <- get_populous_counties()
      df <- filter(df, .data$geo_value %in% top_200_geos)
    }
      
    test_data_list <- list()
    coef_list <- list()
    
    for (value_type in params$value_types) {
      for (signal_suffix in signal_suffixes) {
        key <- make_key(value_type, signal_suffix)
        test_data_list[[key]] <- list()
        coef_list[[key]] <- list()
      }
    }

    msg_ts("Splitting data into geo groups")
    group_dfs <- group_split(df, .data$geo_value)

    # Build model for each location
    for (subdf in group_dfs) {
      geo <- subdf$geo_value[1]
      msg_ts(str_interp("Processing ${geo} geo group"))

      min_refd <- min(subdf[[refd_col]])
      max_refd <- max(subdf[[refd_col]])
      subdf <- fill_rows(
        subdf, refd_col, lag_col, min_refd, max_refd, ref_lag = params$ref_lag
      )
      
      for (signal_suffix in signal_suffixes) {
        # For each suffix listed in `signal_suffixes`, run training/testing
        # process again. Main use case is for quidel which has overall and
        # age-based signals.
        if (signal_suffix != "") {
          msg_ts(str_interp("signal suffix ${signal_suffix}"))
          num_col <- paste(params$num_col, signal_suffix, sep = "_")
          denom_col <- paste(params$denom_col, signal_suffix, sep = "_")
        } else {
          num_col <- params$num_col
          denom_col <- params$denom_col
        }
        
        for (value_type in params$value_types) {
          msg_ts(str_interp("value type ${value_type}"))
          # Handle different signal types
          if (value_type == "count") { # For counts data only
            combined_df <- fill_missing_updates(subdf, num_col, refd_col, lag_col)
            combined_df <- add_7davs_and_target(
              combined_df, "value_raw", refd_col, lag_col, ref_lag = params$ref_lag
            )
            
          } else if (value_type == "fraction") {
            combined_num_df <- fill_missing_updates(subdf, num_col, refd_col, lag_col)
            combined_num_df <- add_7davs_and_target(
              combined_num_df, "value_raw", refd_col, lag_col, ref_lag = params$ref_lag
            )
            
            combined_denom_df <- fill_missing_updates(subdf, denom_col, refd_col, lag_col)
            combined_denom_df <- add_7davs_and_target(
              combined_denom_df, "value_raw", refd_col, lag_col, ref_lag = params$ref_lag
            )
            
            combined_df <- merge(
              combined_num_df, combined_denom_df,
              by=c(refd_col, issued_col, lag_col, "target_date"), all.y=TRUE,
              suffixes=c("_num", "_denom")
            )
          }
          combined_df <- add_params_for_dates(combined_df, refd_col, lag_col)
          combined_df <- combined_df %>% filter(.data$lag < params$ref_lag)

          geo_train_data <- combined_df %>%
            filter(.data$issue_date < training_end_date) %>%
            filter(.data$target_date <= training_end_date) %>%
            filter(.data$target_date > training_end_date - params$training_days) %>%
            drop_na()
          geo_test_data <- combined_df %>%
            filter(.data$issue_date %in% params$test_dates) %>%
            drop_na()

          if (nrow(geo_test_data) == 0) next
          if (nrow(geo_train_data) <= 200) next

          if (value_type == "fraction") {
            # Use beta prior approach to adjust fractions
            geo_prior_test_data = combined_df %>%
              filter(.data$issue_date > min(params$test_dates) - 7) %>%
              filter(.data$issue_date <= max(params$test_dates))
            updated_data <- frac_adj(geo_train_data, geo_test_data, geo_prior_test_data,
                                     indicator = indicator, signal = signal,
                                     geo_level = geo_level, signal_suffix = signal_suffix,
                                     lambda = params$lambda, value_type = value_type, geo = geo,
                                     training_end_date = training_end_date,
                                     model_save_dir = params$cache_dir,
                                     taus = params$taus,
                                     lp_solver = params$lp_solver,
                                     train_models = params$train_models,
                                     make_predictions = params$make_predictions)
            geo_train_data <- updated_data[[1]]
            geo_test_data <- updated_data[[2]]
          }
          max_raw = sqrt(max(geo_train_data$value_raw))
          for (test_lag in c(1:14, 21, 35, 51)) {
            msg_ts(str_interp("test lag ${test_lag}"))
            filtered_data <- data_filteration(test_lag, geo_train_data, 
                                              geo_test_data, params$lag_pad)
            train_data <- filtered_data[[1]]
            test_data <- filtered_data[[2]]

            if (nrow(train_data) == 0 || nrow(test_data) == 0) {
              msg_ts(str_interp(
                "Not enough data to either train or test for test_lag ${test_lag}, skipping"
              ))
              next
            }

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
            msg_ts("Training or loading models")
            prediction_results <- model_training_and_testing(
              train_data, test_data, taus = params$taus, covariates = params_list,
              lp_solver = params$lp_solver,
              lambda = params$lambda, test_lag = test_lag, geo = geo,
              value_type = value_type, model_save_dir = params$cache_dir,
              indicator = indicator, signal = signal, geo_level = geo_level,
              signal_suffix =signal_suffix, training_end_date = training_end_date,
              train_models = params$train_models,
              make_predictions = params$make_predictions
            )

            # Model objects are saved during training, so only need to export
            # output if making predictions/corrections
            if (params$make_predictions) {
              msg_ts("Generating predictions")
              test_data <- prediction_results[[1]]
              coefs <- prediction_results[[2]]
              test_data <- evaluate(test_data, params$taus)
              
              key <- make_key(value_type, signal_suffix)
              idx <- length(test_data_list[[key]]) + 1
              test_data_list[[key]][[idx]] <- test_data
              coef_list[[key]][[idx]] <- coefs
            }
          }# End for test lags
        }# End for value types
      }# End for signal suffixes
      
      if (params$make_predictions) {
        for (value_type in params$value_types) {
          for (signal_suffix in signal_suffixes) {
            key <- make_key(value_type, signal_suffix)
            test_combined <- bind_rows(test_data_list[[key]]) 
            coef_combined <- bind_rows(coef_list[[key]]) 
            export_test_result(test_combined, coef_combined, 
                               indicator, signal, 
                               geo_level, geo, signal_suffix, params$lambda,
                               training_end_date,
                               value_type, export_dir=params$export_dir)
          }
        }
      }
      
    }# End for geo list
  }# End for geo type
}

#' Perform backfill correction on all desired signals and geo levels
#' 
#' @template params-template
#'
#' @importFrom dplyr bind_rows mutate
#' @importFrom parallel detectCores
#' @importFrom rlang .data
#' 
#' @export
main <- function(params) {
  if (!params$train_models && !params$make_predictions) {
    msg_ts("both model training and prediction generation are turned off; exiting")
    return(NULL)
  }
  
  if (params$train_models) {
    msg_ts("Removing stored models")
    files_list <- list.files(params$cache_dir, pattern="*.model", full.names = TRUE)
    file.remove(files_list)
  }

  training_end_date <- as.Date(readLines(
    file.path(params$cache_dir, "training_end_date.txt")))
  msg_ts(str_interp("training_end_date is ${training_end_date}"))

  ## Set default number of cores for mclapply to half of those available.
  if (params$parallel) {
    cores <- detectCores()

    if (is.na(cores)) {
      warning("Could not detect the number of CPU cores; parallel mode disabled")
      params$parallel <- FALSE
    } else {
      options(mc.cores = min(params$parallel_max_cores, max(floor(cores / 2), 1L)))
    }
  }
  
  # Loop over every indicator + signal combination.
  for (group_i in seq_len(nrow(INDICATORS_AND_SIGNALS))) {
    input_group <- INDICATORS_AND_SIGNALS[group_i,]
    msg_ts(str_interp(
      "Processing indicator ${input_group$indicator} signal ${input_group$signal}"
    ))

    files_list <- get_files_list(
      input_group$indicator, input_group$signal, params, input_group$sub_dir
    )
    if (length(files_list) == 0) {
      warning(str_interp(
        "No files found for indicator ${input_group$indicator} signal ${input_group$signal}, skipping"
      ))
      next
    }
    
    msg_ts("Reading in and combining associated files")
    input_data <- lapply(
      files_list,
      function(file) {read_data(file)}
    ) %>%
      bind_rows()

    if (nrow(input_data) == 0) {
      warning(str_interp(
        "No data available for indicator ${input_group$indicator} signal ${input_group$signal}, skipping"
      ))
      next
    }

    # Check data type and required columns
    msg_ts("Validating input data")
    for (value_type in params$value_types) {
      msg_ts(str_interp("for ${value_type}"))
      result <- validity_checks(
        input_data, value_type,
        params$num_col, params$denom_col, input_group$name_suffix
      )
      input_data <- result[["df"]]
    }
    
    # Check available training days
    training_days_check(input_data$issue_date, params$training_days)
    
    # Perform backfill corrections and save result
    run_backfill(input_data, params, training_end_date,
      indicator = input_group$indicator, signal = input_group$signal,
      signal_suffixes = input_group$name_suffix)

    if (params$train_models) {
      # Save the training end date to a text file.
      writeLines(as.character(TODAY),
                 file.path(params$cache_dir, "training_end_date.txt"))
    }
  }
}
