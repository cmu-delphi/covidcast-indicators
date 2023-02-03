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
#' @template training_end_date-template
#' @template training_start_date-template
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
                                       training_start_date,
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
                                 training_start_date=training_start_date,
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
  
  test_data$geo_value = geo
  coef_combined_result = data.frame(tau=taus, geo_value=geo, test_lag=test_lag,
                                    training_end_date=training_end_date,
                                    training_start_date=training_start_date,
                                    lambda=lambda)
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
#' @template training_end_date-template
#' @template training_start_date-template
#'
#' @return path to file containing model object
#'
#' @importFrom stringr str_interp
#' 
generate_filename <- function(indicator, signal, 
                              geo_level, signal_suffix, lambda,
                              training_end_date, training_start_date, geo="",
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
  components <- c(format(training_end_date, "%Y%m%d"),
                  format(training_start_date, "%Y%m%d"), beta_prior,
                  indicator, signal, signal_suffix,
                  geo_level, lambda, value_type,
                  geo, test_lag, dw, tau)
  
  filename <- paste0(
    # Drop any empty strings.
    paste(components[components != ""], collapse="_"),
    file_type
  )
  return(filename)
}

#' Get date range of data to use for training models
#'
#' Calculate training start and end dates based on user settings.
#' `training_start_date` is the minimum allowed target date when selecting
#' training data to use. `training_end_date` is the maximum allowed target
#' date and maximum allowed issue date.
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
#'
#' @importFrom stringr str_interp
get_training_date_range <- function(params) {
  default_end_date <- TODAY - params$testing_window + 1

  if (params$train_models) {
    if (params_element_exists_and_valid(params, "training_end_date")) {
      # Use user-provided end date.
      training_end_date <- as.Date(params$training_end_date)
    } else {
      # Default end date is today.
      training_end_date <- default_end_date
    }
  } else {
    # Get end date from cached model files. Assumes filename format like
    # `20220628_20220529_changehc_covid_state_lambda0.1_count_ca_lag5_tau0.9.model`
    # where the leading date is the training end date for that model, and the
    # second date is the training start date.
    model_files <- list.files(params$cache_dir, "^20[0-9]{6}_20[0-9]{6}.*[.]model$")
    if (params$indicators != "all") {
      # If an single indicator is specified via the command-line
      # `--indicators` argument, the training end date from available model
      # files for only that indicator will be used. This means that model
      # training date ranges may not match across all indicators.
      model_files <- list.files(
        params$cache_dir,
        str_interp("^20[0-9]{6}_20[0-9]{6}_${params$indicators}.*[.]model$")
      )
    }
    if (length(model_files) == 0) {
      # We know we'll be retraining models today.
      training_end_date <- default_end_date
    } else {
      # If only some models are in the cache, they will be used and those
      # missing will be regenerated as-of the training end date.
      training_end_date <- max(as.Date(substr(model_files, 1, 8), "%Y%m%d"))
    }
  }

  # Calculate start date instead of reading from cached files. This assumes
  # that the user-provided `params$training_days` is more up-to-date. If
  # `params$training_days` has changed such that for a given training end
  # date, the calculated training start date differs from the start date
  # referenced in cached file names, then those cached files will not be used.
  training_start_date <- training_end_date - params$training_days

  return(list(
    "training_start_date"=training_start_date,
    "training_end_date"=training_end_date
  ))
}
