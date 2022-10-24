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
#' @importFrom tidyr crossing
#' @importFrom stats setNames
#' @importFrom dplyr bind_rows filter %>%
#' @importFrom rlang .data .env
run_backfill <- function(df, params,
                         refd_col = "time_value", lag_col = "lag", issued_col = "issue_date",
                         signal_suffixes = c(""), indicator = "", signal = "") {
  # Get all desired model names by combining input params. Need to get the
  # list of geos from test_data; everything else is static.
  model_combos <- list()
  for (geo_level in params$geo_levels) {
    model_combos[[geo_level]] <- as.data.frame(crossing(
      geo = unique(test_data[[geo_level]]$geo_value),
      signal_suffix = signal_suffixes,
      value_type = params$value_types,
      test_lag = params$test_lags,
      tau = params$taus
    )) %>%
      mutate(
        model_name = generate_filename(.env$indicator, .env$signal,
                                  .env$geo_level, .data$signal_suffix, params$lambda,
                                  training_end_date=.env$training_end_date, geo=.data$geo,
                                  value_type = .data$value_type, test_lag=.data$test_lag,
                                  tau=.data$tau, model_mode = TRUE),
        in_cache = file.exists(file.path(params$cache_dir, .data$model_name)))
      )
  }

  if (!params$train_models) {
    for (geo_level in params$geo_levels) {
      # Only keep missing models.
      model_combos <- filter(model_combos[[geo_level]], !.data$in_cache)
    }
  }

  if (nrow(bind_rows(model_combos)) > 0) {
    ## TODO: Get training data
    # Include filter in data fetch:
    # df <- filter(df_list[[geo_level]], .data$lag < params$ref_lag + 30) # a rough filtration to save memory
    train_data <- make_geo_level_dfs(train_data, params, issued_col, refd_col, lag_col)
    train_data <- split_by_geo_and_fill(train_data, params, refd_col, lag_col)
    train_models(train_data, params, model_combos,
                 issued_col = issued_col, signal_suffixes = signal_suffixes,
                 indicator = indicator, signal = signal)
  }

  if (params$make_predictions) {
    ## TODO: Get test data
    # Include filter in data fetch:
    # df <- filter(df_list[[geo_level]], .data$lag < params$ref_lag + 30) # a rough filtration to save memory
    test_data <- make_geo_level_dfs(test_data, params, issued_col, refd_col, lag_col)
    test_data <- split_by_geo_and_fill(test_data, params, refd_col, lag_col)
    make_predictions(test_data, params,
                     issued_col = issued_col, signal_suffixes = signal_suffixes,
                     indicator = indicator, signal = signal)
  }
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
    run_backfill(input_data, params,
      indicator = input_group$indicator, signal = input_group$signal,
      signal_suffixes = input_group$name_suffix)
  }
}
