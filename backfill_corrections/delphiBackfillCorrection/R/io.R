#' Read a parquet file into a dataframe
#' 
#' @template input_dir-template
#'
#' @importFrom arrow read_parquet
#'
#' @export
read_data <- function(input_dir) {
  df <- read_parquet(input_dir, as_data_frame = TRUE)
  return (df)
}

#' Export the result to customized directory
#'
#' @param test_data test data containing prediction results
#' @param coef_data data frame containing the estimated coefficients
#' @template indicator-template
#' @template signal-template
#' @template geo_level-template
#' @template geo-template
#' @template signal_suffix-template
#' @template lambda-template
#' @template value_type-template
#' @template export_dir-template
#' @template training_end_date-template
#' @template training_start_date-template
#'
#' @importFrom readr write_csv
#' @importFrom stringr str_interp str_split
export_test_result <- function(test_data, coef_data, indicator, signal, 
                               geo_level, geo, signal_suffix, lambda,
                               training_end_date, training_start_date,
                               value_type, export_dir) {
  base_name <- generate_filename(indicator=indicator, signal=signal,
                                 geo_level=geo_level, signal_suffix=signal_suffix,
                                 lambda=lambda, training_end_date=training_end_date,
                                 training_start_date=training_start_date,
                                 geo=geo, value_type=value_type, model_mode=FALSE)

  signal_info <- str_interp("indicator ${indicator} signal ${signal} geo ${geo} value_type ${value_type}")
  if (nrow(test_data) == 0) {
    warning(str_interp("No test data available for ${signal_info}"))
  } else {
    msg_ts(str_interp("Saving predictions to disk for ${signal_info} "))
    pred_output_file <- str_interp("prediction_${base_name}")
    write_csv(test_data, file.path(export_dir, pred_output_file))
  }
  
  if (nrow(coef_data) == 0) {
    warning(str_interp("No coef data available for ${signal_info}"))
  } else {
    msg_ts(str_interp("Saving coefficients to disk for ${signal_info}"))
  coef_output_file <- str_interp("coefs_${base_name}")
  write_csv(coef_data, file.path(export_dir, coef_output_file))
  }
}

#' List valid input files.
#'
#' @template indicator-template
#' @template signal-template
#' @template params-template
#' @param sub_dir string specifying the indicator-specific directory within
#'     the general input directory `params$input_dir`
get_files_list <- function(indicator, signal, params, sub_dir) {
  # Make sure we're reading in both 4-week rollup and daily files.
  if (!missing(sub_dir)) {
    input_dir <- file.path(params$input_dir, sub_dir)
  } else {
    input_dir <- params$input_dir
  }

  # Convert input_group into file names.
  daily_pattern <- create_name_pattern(indicator, signal, "daily")
  rollup_pattern <- create_name_pattern(indicator, signal, "rollup")
  
  # Filter files lists to only include those containing dates we need for training
  daily_input_files <- list.files(
      input_dir, pattern = daily_pattern, full.names = TRUE
    ) %>%
    subset_valid_files("daily", params)
  rollup_input_files <- list.files(
      input_dir, pattern = rollup_pattern, full.names = TRUE
    ) %>%
    subset_valid_files("rollup", params)
  
  return(c(daily_input_files, rollup_input_files))
}

#' Return file names only if they contain data to be used in training
#' 
#' Parse filenames to find included dates. Use different patterns if file
#' includes daily or rollup (multiple days) data.
#'
#' @param files_list character vector of input files of a given `file_type`
#' @template file_type-template
#' @template params-template
subset_valid_files <- function(files_list, file_type = c("daily", "rollup"), params) {
  file_type <- match.arg(file_type)
  date_format = "%Y%m%d"

  # Put min and max issue date for each file into vectors the same length as
  # `files_list`.
  switch(file_type,
         daily = {
           start_issue_dates <- as.Date(
             sub("^.*/.*_as_of_([0-9]{8})[.]parquet$", "\\1", files_list),
             format = date_format
           )
           end_issue_dates <- start_issue_dates
         },
         rollup = {
           rollup_pattern <- "^.*/.*_from_([0-9]{8})_to_([0-9]{8})[.]parquet$"
           start_issue_dates <- as.Date(
             sub(rollup_pattern, "\\1", files_list),
             format = date_format
           )
           end_issue_dates <- as.Date(
             sub(rollup_pattern, "\\2", files_list),
             format = date_format
           )
         }
  )
  
  # Find the earliest and latest issue dates needed for either training or testing.
  result <- get_issue_date_range(params)
  start_issue <- result$start_issue
  end_issue <- result$end_issue

  # Only keep files with data that falls at least somewhat between the desired
  # start and end issue dates.
  files_list <- files_list[
    !(( start_issue_dates < start_issue & end_issue_dates < start_issue ) |
        ( start_issue_dates > end_issue & end_issue_dates > end_issue ))]

  return(files_list)
}

#' Find the earliest and latest issue dates needed for either training or testing.
#'
#' With current logic, we always need to include data for model training (in
#' case cached models are not available for a "make_predictions"-only run and
#' we need to train new models).
#'
#' We generate test and train data by applying the following filters:
#'   - Test data is data where issue_date is in params$test_dates
#'     (as a continuous filter, min(params$test_dates) <= issue_date <= max(params$test_dates) )
#'   - Train data is data where issue_date < training_end_date; and
#'     training_start_date < target_date <= training_end_date
#'
#' Train data doesn't have an explicit lower bound on issue_date, but we can
#' derive one.
#'
#' Since target_date = reference_date + params$ref_lag and issue_date >=
#' reference_date, the requirement that training_start_date < target_date
#' also implies that issue date must be > training_start_date - params$ref_lag
#'
#' @template params-template
get_issue_date_range <- function(params) {
  result <- get_training_date_range(params)

  # Check that all training data is earlier than the earliest test date.
  #
  # It's inappropriate to make predictions of historical data based on a model
  # trained using future data. If we want to make predictions for an old test
  # date t0 (t0 < TODAY), we will always need to train a new model based on
  # data t < t0.
  assert(
    result$training_end_date < min(params$test_dates),
    "training end date must be earlier than the earliest test date to produce valid predictions"
  )

  ## TODO: right now, this gets both training and testing data regardless of
  #  which mode is selected
  start_issue <- result$training_start_date - params$ref_lag
  end_issue <- max(params$test_dates)

  return(list("start_issue" = start_issue, "end_issue" = end_issue))
}

#' Create pattern to match input files of a given type and signal
#' 
#' @template indicator-template
#' @template signal-template
#' @template file_type-template
#'
#' @importFrom stringr str_interp
create_name_pattern <- function(indicator, signal,
                                file_type = c("daily", "rollup")) {
  file_type <- match.arg(file_type)
  switch(file_type,
         daily = str_interp("${indicator}_${signal}_as_of_[0-9]{8}[.]parquet$"),
         rollup = str_interp("${indicator}_${signal}_from_[0-9]{8}_to_[0-9]{8}[.]parquet$")
  )
}
