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
#' 
#' @importFrom dplyr %>% filter group_by summarize across everything group_split ungroup
#' @importFrom tidyr drop_na
#' @importFrom purrr map map_dfr
#' @importFrom bettermc mclapply
#' 
#' @export
run_backfill <- function(df, params,
                         refd_col = "time_value", lag_col = "lag", issued_col = "issue_date",
                         signal_suffixes = c(""), indicator = "", signal = "") {
  geo_levels <- params$geo_levels
  if ("state" %in% geo_levels) {
    # If state included, do it last since state processing modifies the
    # `df` object.
    geo_levels <- c(setdiff(geo_levels, c("state")), "state")
  }
  
  for (geo_level in geo_levels) {
    msg_ts("geo level ", geo_level)
    # Get full list of interested locations
    if (geo_level == "state") {
      # Drop county field and make new "geo_value" field from "state_id".        
      # Aggregate counties up to state level
      agg_cols <- c("geo_value", issued_col, refd_col, lag_col)
      # Sum all non-agg columns. Summarized columns keep original names
      df$geo_value <- df$state_id
      df$state_id <- NULL
      df <- df %>%
        group_by(across(agg_cols)) %>%
        summarize(across(everything(), sum)) %>%
        ungroup()
    }
    if (geo_level == "county") {
      # Keep only 200 most populous (within the US) counties
      top_200_geos <- get_populous_counties()
      df <- filter(df, geo_value %in% top_200_geos)
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
    group_dfs <- group_split(df, geo_value)

    msg_ts("Beginning training and/or testing...")
    # Build model for each location
    apply_fn <- ifelse(params$parallel, mclapply, lapply)
    result <- apply_fn(group_dfs, function(subdf) {
      # Make a copy with the same structure.
      state_test_data_list <- test_data_list
      state_coef_list <- coef_list

      geo <- subdf$geo_value[1]
      
      msg_ts("Processing ", geo, " geo group")

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
          msg_ts("signal suffix ", signal_suffix)
          num_col <- paste(params$num_col, signal_suffix, sep = "_")
          denom_col <- paste(params$denom_col, signal_suffix, sep = "_")
        } else {
          num_col <- params$num_col
          denom_col <- params$denom_col
        }
        
        for (value_type in params$value_types) {
          msg_ts("value type ", value_type)
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
          combined_df <- filter(combined_df, lag < params$ref_lag)

          geo_train_data <- filter(combined_df,
              issue_date < params$training_end_date,
              target_date <= params$training_end_date,
              target_date > params$training_start_date,
            ) %>%
            drop_na()
          geo_test_data <- filter(combined_df,
              issue_date %in% as.character(params$test_dates)
            ) %>%
            drop_na()

          if (nrow(geo_test_data) == 0) {
            warning("No test data")
            next
          }
          if (nrow(geo_train_data) <= 200) {
            warning("Not enough training data")
            next
          }

          if (value_type == "fraction") {
            # Use beta prior approach to adjust fractions
            geo_prior_test_data = filter(combined_df,
                issue_date > min(params$test_dates) - 7,
                issue_date <= max(params$test_dates)
            )
            updated_data <- frac_adj(geo_train_data, geo_test_data, geo_prior_test_data,
                                     indicator = indicator, signal = signal,
                                     geo_level = geo_level, signal_suffix = signal_suffix,
                                     lambda = params$lambda, value_type = value_type, geo = geo,
                                     training_end_date = params$training_end_date,
                                     training_start_date = params$training_start_date,
                                     model_save_dir = params$cache_dir,
                                     taus = params$taus,
                                     lp_solver = params$lp_solver,
                                     train_models = params$train_models,
                                     make_predictions = params$make_predictions)
            geo_train_data <- updated_data[[1]]
            geo_test_data <- updated_data[[2]]
          }
          max_raw = sqrt(max(geo_train_data$value_raw))
          for (test_lag in params$test_lags) {
            msg_ts("test lag ", test_lag)
            filtered_data <- data_filteration(test_lag, geo_train_data, 
                                              geo_test_data, params$lag_pad)
            train_data <- filtered_data[[1]]
            test_data <- filtered_data[[2]]

            if (nrow(train_data) == 0 || nrow(test_data) == 0) {
              msg_ts("Not enough data to either train or test for test lag ",
                test_lag, ", skipping")
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
            prediction_results <- model_training_and_testing(
              train_data, test_data, taus = params$taus, covariates = params_list,
              lp_solver = params$lp_solver,
              lambda = params$lambda, test_lag = test_lag, geo = geo,
              value_type = value_type, model_save_dir = params$cache_dir,
              indicator = indicator, signal = signal, geo_level = geo_level,
              signal_suffix =signal_suffix,
              training_end_date = params$training_end_date,
              training_start_date = params$training_start_date,
              train_models = params$train_models,
              make_predictions = params$make_predictions
            )

            # Model objects are saved during training, so only need to export
            # output if making predictions/corrections
            if (params$make_predictions) {
              test_data <- prediction_results[[1]]
              coefs <- prediction_results[[2]]
              test_data <- evaluate(test_data, params$taus) %>%
                exponentiate_preds(params$taus)
              
              key <- make_key(value_type, signal_suffix)
              idx <- length(state_test_data_list[[key]]) + 1
              state_test_data_list[[key]][[idx]] <- test_data
              state_coef_list[[key]][[idx]] <- coefs
            }
          }# End for test lags
        }# End for value types
      }# End for signal suffixes

      return(list(coefs = state_coef_list, test_data = state_test_data_list))
    }) # End for geo list

    test_data_list <- map(result, ~.x$test_data)
    coef_list <- map(result, ~.x$coefs)
    
    if (params$make_predictions) {
      for (value_type in params$value_types) {
        for (signal_suffix in signal_suffixes) {
          key <- make_key(value_type, signal_suffix)
          test_combined <- map_dfr(test_data_list, ~.x[[key]])
          coef_combined <- map_dfr(coef_list, ~.x[[key]])
          export_test_result(test_combined, coef_combined, 
                             indicator=indicator, signal=signal,
                             signal_suffix=signal_suffix,
                             geo_level=geo_level, lambda=params$lambda,
                             training_end_date=params$training_end_date,
                             training_start_date=params$training_start_date,
                             value_type=value_type, export_dir=params$export_dir)
        }
      }
    }
  }# End for geo type
}

#' Perform backfill correction on all desired signals and geo levels
#' 
#' @template params-template
#' @template refd_col-template
#' @template lag_col-template
#' @template issued_col-template
#'
#' @importFrom dplyr bind_rows %>%
#' @importFrom parallel detectCores
#' @importFrom stringr str_interp
#' 
#' @export
main <- function(params,
  refd_col = "time_value", lag_col = "lag", issued_col = "issue_date") {
  if (!params$train_models && !params$make_predictions) {
    msg_ts("both model training and prediction generation are turned off; exiting")
    return(NULL)
  }

  indicators_subset <- INDICATORS_AND_SIGNALS
  if (params$indicators != "all") {
    indicators_subset <- filter(indicators_subset, indicator == params$indicators)
  }
  if (nrow(indicators_subset) == 0) {
    stop("no indicators to process")
  }
  
  if (params$train_models) {
    msg_ts("Removing stored models")
    model_name_pat <- "[.]model$"
    # Remove models for only currently selected indicator, if any.
    if (params$indicators != "all") {
      model_name_pat <- str_interp(".*${params$indicators}.*[.]model$")
    }
    files_list <- list.files(params$cache_dir, pattern=model_name_pat, full.names = TRUE)
    file.remove(files_list)
  }

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

  # Training start and end dates are the same for all indicators, so we can fetch
  # at the beginning.
  result <- get_training_date_range(params)
  params$training_start_date <- result$training_start_date
  params$training_end_date <- result$training_end_date

  msg_ts("training_start_date is ", params$training_start_date,
         ", training_end_date is ", params$training_end_date)

  # Loop over every indicator + signal combination.
  for (group_i in seq_len(nrow(indicators_subset))) {
    input_group <- indicators_subset[group_i,]
    msg_ts("Processing indicator ", input_group$indicator, " signal ", input_group$signal)

    files_list <- get_files_list(
      input_group$indicator, input_group$signal, params, input_group$sub_dir
    )
    if (length(files_list) == 0) {
      warning("No files found for indicator ", input_group$indicator,
              " signal ", input_group$signal, ", skipping")
      next
    }
    
    msg_ts("Reading in and combining associated files")
    input_data <- lapply(
      files_list, read_data # refd_col and issued_col read in as strings
    ) %>%
      bind_rows() %>%
      fips_to_geovalue() %>%
       # a rough filter to save memory
      filter(lag < params$ref_lag + 30)

    if (nrow(input_data) == 0) {
      warning("No data available for indicator ", input_group$indicator,
              " signal ", input_group$signal, ", skipping")
      next
    }

    # Check data type and required columns
    msg_ts("Validating input data")
    # Validate while date fields still stored as strings for speed.
    input_data <- validity_checks(
      input_data, params$value_types,
      params$num_col, params$denom_col, input_group$name_suffix,
      refd_col = refd_col, lag_col = lag_col, issued_col = issued_col
    )

    # Check available training days
    training_days_check(input_data[[issued_col]], params$training_days)
    
    # Perform backfill corrections and save result
    run_backfill(input_data, params,
      refd_col = refd_col, lag_col = lag_col, issued_col = issued_col,
      indicator = input_group$indicator, signal = input_group$signal,
      signal_suffixes = input_group$name_suffix)
  }
}
