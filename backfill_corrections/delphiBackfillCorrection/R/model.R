#' Filtration for training and testing data with different lags
#' 
#' @template test_lag-template
#' @param lag_pad lag padding for training
#' @param geo_train_data training data for a certain location
#' @param geo_test_data testing data for a certain location
#' 
#' @importFrom rlang .data .env
#'
#' @export
data_filteration <- function(test_lag, geo_train_data, geo_test_data, lag_pad) {
  if (test_lag <= 14){
    test_lag_pad=lag_pad
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
    filter(.data$lag >= .env$test_lag - .env$test_lag_pad ) %>%
    filter(.data$lag <= .env$test_lag + .env$test_lag_pad )
  test_data = geo_test_data %>%
    filter(.data$lag >= .env$test_lag - .env$test_lag_pad1 ) %>%
    filter(.data$lag <= .env$test_lag + .env$test_lag_pad2)

  return (list(train_data, test_data))
}

#' Add columns to indicate the scale of value at square root level
#' 
#' @template train_data-template
#' @param test_data Data Frame for testing
#' @param max_raw the maximum value in the training data at square root level
#' @template value_col-template
#' 
#' @export
add_sqrtscale<- function(train_data, test_data, max_raw, value_col) {
  if (!(value_col %in% colnames(train_data))){
    stop("value raw does not exist in training data!")
  }
  
  if (!(value_col %in% colnames(test_data))){
    stop("value raw does not exist in testing data!")
  }
  
  sqrtscale = c()
  sub_max_raw = sqrt(max(train_data[[value_col]])) / 2
  
  for (split in seq(0, 3)){
    if (sub_max_raw < (max_raw * (split+1) * 0.1)) break
    y0_col <- paste0("sqrty", as.character(split))
    train_data[y0_col] = 0
    test_data[y0_col] = 0
    qv_pre = max_raw * split * 0.2
    qv_next = max_raw * (split+1) * 0.2

    train_data[(train_data[[value_col]] <= (qv_next)^2)
               & (train_data[[value_col]] > (qv_pre)^2), 
               y0_col] = 1
    test_data[(test_data[[value_col]] <= (qv_next)^2)
              & (test_data[[value_col]] > (qv_pre)^2), 
              y0_col] = 1
    sqrtscale[split+1] = y0_col
  }
  return (list(train_data, test_data, sqrtscale))
}

#' Fetch model and use to generate predictions/perform corrections
#'
#' @template train_data-template
#' @param test_data Data frame for testing 
#' @template taus-template
#' @template covariates-template
#' @template lp_solver-template
#' @template lambda-template
#' @template geo_level-template
#' @template geo-template
#' @template indicator-template
#' @template signal-template
#' @template signal_suffix-template
#' @template value_type-template
#' @template test_lag-template
#' @template train_models-template
#' @template make_predictions-template
#' @param model_save_dir directory containing trained models
#' @param training_end_date Most recent training date
#'
#' @importFrom stats predict coef
#' @importFrom stringr str_interp
#'
#' @export
model_training_and_testing <- function(train_data, test_data, taus, covariates,
                                       lp_solver, lambda, test_lag,
                                       geo, value_type, model_save_dir, 
                                       indicator, signal, 
                                       geo_level, signal_suffix,
                                       training_end_date, 
                                       train_models = TRUE,
                                       make_predictions = TRUE) {
  success = 0
  coefs_result = list()
  coef_list = c("intercept", paste(covariates, '_coef', sep=''))
  for (tau in taus) {
    tryCatch(
      expr = {
        model_file_name <- generate_filename(indicator=indicator, signal=signal,
                                 geo_level=geo_level, signal_suffix=signal_suffix,
                                 lambda=lambda, training_end_date=training_end_date,
                                 geo=geo, value_type=value_type,
                                 test_lag=test_lag, tau=tau)
        model_path <- file.path(model_save_dir, model_file_name)
        obj <- get_model(model_path, train_data, covariates, tau,
                         lambda, lp_solver, train_models)

        if (make_predictions) {
          y_hat_all = as.numeric(predict(obj, newx = as.matrix(test_data[covariates])))
          test_data[[paste0("predicted_tau", as.character(tau))]] = y_hat_all

          coefs_result[[success+1]] = coef(obj)
        }

        success = success + 1
      },
      error=function(e) {msg_ts(str_interp("Training failed for ${model_path}"))}
    )
  }
  if (success < length(taus)) {return (NULL)}
  if (!make_predictions) {return (list())}
  
  coef_combined_result = data.frame(tau=taus, geo=geo, test_lag=test_lag)
  coef_combined_result[coef_list] = as.matrix(do.call(rbind, coefs_result))
  
  return (list(test_data, coef_combined_result))
}

#' Evaluation of the test results based on WIS score
#' The WIS score calculation is based on the weighted_interval_score function 
#' from the `evalcast` package from Delphi
#' 
#' @param test_data dataframe with a column containing the prediction results of
#'    each requested quantile. Each row represents an update with certain
#'    (reference_date, issue_date, location) combination.
#' @template taus-template
#' 
#' @importFrom evalcast weighted_interval_score
#' 
#' @export
evaluate <- function(test_data, taus) {
  n_row = nrow(test_data)
  taus_list = as.list(data.frame(matrix(replicate(n_row, taus), ncol=n_row)))
  
  # Calculate WIS
  predicted_all = as.matrix(test_data[c("predicted_tau0.01", "predicted_tau0.025",
                                        "predicted_tau0.1", "predicted_tau0.25",
                                        "predicted_tau0.5", "predicted_tau0.75",
                                        "predicted_tau0.9", "predicted_tau0.975",
                                        "predicted_tau0.99")])
  predicted_all_exp = exp(predicted_all)
  predicted_trans = as.list(data.frame(t(predicted_all - test_data$log_value_target)))
  test_data$wis = mapply(weighted_interval_score, taus_list, predicted_trans, 0)
  
  return (test_data)
}

#' Train model using quantile regression with Lasso penalty, or load from disk
#'
#' @param model_path path to read model from or to save model to
#' @template train_data-template
#' @template covariates-template
#' @param tau decimal quantile to be predicted. Values must be between 0 and 1.
#' @template lp_solver-template
#' @template lambda-template
#' @template train_models-template
#'
#' @importFrom quantgen quantile_lasso
#' @importFrom stringr str_interp
get_model <- function(model_path, train_data, covariates, tau,
            lambda, lp_solver, train_models) {
  if (train_models || !file.exists(model_path)) {
    if (!train_models && !file.exists(model_path)) {
      warning(str_interp("user requested use of cached model but file {model_path}"),
        " does not exist; training new model")
    }
    # Quantile regression
    obj <- quantile_lasso(as.matrix(train_data[covariates]),
                         train_data$log_value_target, tau = tau,
                         lambda = lambda, standardize = FALSE, lp_solver = lp_solver)

    # Save model to cache.
    create_dir_not_exist(dirname(model_path))
    save(obj, file=model_path)
  } else {
    # Load model from cache invisibly. Object has the same name as the original
    # model object, `obj`.
    msg_ts(str_interp("Loading from ${model_path}"))
    load(model_path)
  }

  return(obj)
}

#' Construct filename for model with given parameters
#'
#' @template indicator-template
#' @template signal-template
#' @template geo-template
#' @template signal_suffix-template
#' @template lambda-template
#' @template value_type-template
#' @template test_lag-template
#' @template geo_level-template
#' @template test_lag-template
#' @param dw string, indicate the day of a week
#' @param tau decimal quantile to be predicted. Values must be between 0 and 1.
#' @param beta_prior_mode bool, indicate whether it is for a beta prior model
#' @param model_mode bool, indicate whether the file name is for a model
#' @param training_end_date the most recent training date
#'
#' @return path to file containing model object
#'
#' @importFrom stringr str_interp
#' 
generate_filename <- function(indicator, signal, 
                              geo_level, signal_suffix, lambda,
                              training_end_date="", geo="", 
                              value_type = "", test_lag="", tau="", dw="",
                              beta_prior_mode = FALSE, model_mode = TRUE) {
  if (lambda != "") {
    lambda <- str_interp("lambda${lambda}")
  }
  if (test_lag != "") {
    test_lag <- str_interp("lag${test_lag}")
  }
  if (tau != "") {
    tau <- str_interp("tau${tau}")
  }
  if (beta_prior_mode) {
    beta_prior <- "beta_prior"
  } else {
    beta_prior <- ""
  }
  if (model_mode) {
    file_type <- ".model"
  } else {
    file_type <- ".csv.gz"
  }
  components <- c(as.character(training_end_date), beta_prior,
                  indicator, signal, signal_suffix,
                  geo_level, lambda, value_type,
                  geo, test_lag, dw, tau)
  
  filename = paste0(
    # Drop any empty strings.
    paste(components[components != ""], collapse="_"),
    file_type
  )
  return(filename)
}

#' Get date range of data to use for training models
#'
#' Calculate training end date, input data start date, and input
#' data end date based on user settings.
#'
#' Cases:
#'   1. We are training new models.
#'   2. We are not training new models and cached models exist.
#'   3. We are not training new models and cached models don't exist.
#'
#' Sometimes we want to allow the user to specify an end date in
#' params that overrides the automatically-generated end date. This is
#' only relevant when the user requests to train new models.
#'
#' @template params-template
get_training_date_range <- function(params) {
  if (params$train_models) {
    if (params_element_exists_and_valid(params, "training_end_date")) {
      # Use user-provided end date.
      training_end_date <- as.Date(params$training_end_date)
    } else {
      # Default end date is today.
      training_end_date <- TODAY
    }
  } else {
    # Get end date from cached model files.
    # Assumes filename format like `2022-06-28_changehc_covid_state_lambda0.1_count_ca_lag5_tau0.9.model`
    # where the leading date is the training end date for that model.
    model_files <- list.files(params$cache_dir, "202[0-9]-[0-9]{2}-[0-9]{2}*.model")
    if (length(model_files) == 0) {
      # We know we'll be retraining models today.
      training_end_date <- TODAY
    } else {
      # If only some models are in the cache, they will be used and those
      # missing will be regenerated as-of the training end date.
      training_end_date <- max(as.Date(substr(model_files, 1, 10)))
    }
  }

  training_start_date <- training_end_date - params$training_days

  msg_ts(paste0(
    str_interp("training_start_date is ${training_start_date}, "),
    str_interp("training_end_date is ${training_end_date}")
  ))

  return(list(
    "training_start_date"=training_start_date,
    "training_end_date"=training_end_date
  ))
}

train_models <- function(df_list, params, model_combos,
                         refd_col = "time_value", lag_col = "lag", issued_col = "issue_date",
                         signal_suffixes = c(""), indicator = "", signal = "") {
  make_predictions_or_train(df_list, params, mode = "train_models", model_combos = model_combos,
                            issued_col = issued_col, signal_suffixes = signal_suffixes,
                            indicator = indicator, signal = signal))
}

make_predictions <- function(df_list, params,
                         refd_col = "time_value", lag_col = "lag", issued_col = "issue_date",
                         signal_suffixes = c(""), indicator = "", signal = "") {
  make_predictions_or_train(df_list, params, mode = "make_predictions",
                            refd_col = refd_col, lag_col = lag_col,
                            issued_col = issued_col, signal_suffixes = signal_suffixes,
                            indicator = indicator, signal = signal)
}

make_predictions_or_train <- function(df_list, params, mode = c("make_predictions", "train_models"), model_combos = data.frame(),
                         refd_col = "time_value", lag_col = "lag", issued_col = "issue_date",
                         signal_suffixes = c(""), indicator = "", signal = "") {
  mode <- match.args(mode)

  for (geo_level in params$geo_levels) {
    msg_ts(str_interp("geo level ${geo_level}"))

    if (mode == "make_predictions") {
      test_data_list <- list()
      coef_list <- list()

      for (value_type in params$value_types) {
        for (signal_suffix in signal_suffixes) {
          key <- make_key(value_type, signal_suffix)
          test_data_list[[key]] <- list()
          coef_list[[key]] <- list()
        }
      }
    }

    # Process each location
    for (geo in names(df_list[[geo_level]])) {
      msg_ts(str_interp("Processing ${geo} geo group"))
      subdf <- df_list[[geo_level]][[geo]]

      for (signal_suffix in signal_suffixes) {
        # Run training/testing for each suffix listed in `signal_suffixes`.
        # Main use case is for quidel which has overall and age-based signals.
        result <- suffix_pad_col_names(signal_suffix, params)
        num_col <- result$num_col
        denom_col <- result$denom_col

        for (value_type in params$value_types) {
          if (mode == "train_models") {
            # Before we do the expensive 7dav calculations, check that some
            # models actually need this data.
            if (nrow(
              filter(model_combos[[geo_level]],
                .data$geo == .env$geo,
                .data$signal_suffix == .env$signal_suffix,
                .data$value_type == .env$value_type
              )) == 0) {
              next
            }
          }
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

          if (mode == "train_models") {
            # Make training data
            geo_data <- combined_df %>%
              filter(.data$issue_date < training_end_date) %>%
              filter(.data$target_date <= training_end_date) %>%
              filter(.data$target_date > training_start_date) %>%
              drop_na()

            if (nrow(geo_data) <= 200) next
          }
          if (mode == "make_predictions") {
            # Make testing data
            geo_data <- combined_df %>%
              filter(.data$issue_date %in% params$test_dates) %>%
              drop_na()

            if (nrow(geo_data) == 0) next
          }

          if (value_type == "fraction") {
            # Get empty df with same column names as `combined_df`
            geo_prior_test_data <- combined_df[FALSE,]
            if (mode == "train_models") {
              geo_prior_test_data = combined_df %>%
                filter(.data$issue_date > min(params$test_dates) - 7) %>%
                filter(.data$issue_date <= max(params$test_dates))
            }

            # Use beta prior approach to adjust fractions
            geo_data <- frac_adj(geo_train_data, geo_prior_test_data, params)
          }

          ## TODO: need to save to cache
          ## Also save:
          # sub_max_raw = sqrt(max(train_data[[value_col]])) / 2
          max_raw = sqrt(max(geo_train_data$value_raw))
          for (test_lag in params$test_lags) {
            msg_ts(str_interp("test lag ${test_lag}"))
            geo_data <- data_filteration(geo_data, params$lag_pad, test_lag)

            if (nrow(geo_data) == 0) {
              msg_ts(str_interp(
                "Not enough data to either train or test for test_lag ${test_lag}, skipping"
              ))
              next
            }

            updated_data <- add_sqrtscale(geo_data, max_raw, "value_raw")
            geo_data <- updated_data[[1]]
            sqrtscale <- updated_data[[2]]

            covariates <- list(
              Y7DAV, paste0(WEEKDAYS_ABBR, "_issue"),
              paste0(WEEKDAYS_ABBR, "_ref"), WEEK_ISSUES, SLOPE, sqrtscale
            )
            params_list <- c(YITL, as.vector(unlist(covariates)))

            # Model training and testing
            msg_ts("Training or loading models")
            prediction_results <- model_training_and_testing(
              geo_data, covariates = params_list, mode = mode
            )

            # Model objects are saved during training, so only need to export
            # output if making predictions/corrections
            if (mode == "make_predictions") {
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

      if (mode == "make_predictions") {
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

