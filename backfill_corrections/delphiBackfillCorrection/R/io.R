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
#' @param training_end_date the most recent training date
#'
#' @importFrom readr write_csv
#' @importFrom stringr str_interp str_split
export_test_result <- function(test_data, coef_data, indicator, signal, 
                               geo_level, geo, signal_suffix, lambda,
                               training_end_date,
                               value_type, export_dir) {
  base_name <- generate_filename(indicator=indicator, signal=signal,
                                 geo_level=geo_level, signal_suffix=signal_suffix,
                                 lambda=lambda, training_end_date=training_end_date,
                                 geo=geo, value_type=value_type, model_mode=FALSE)
  msg_ts("Saving predictions to disk")
  pred_output_file <- str_interp("prediction_${base_name}")
  write_csv(test_data, file.path(export_dir, pred_output_file))
  
  msg_ts("Saving coefficients to disk")
  coef_output_file <- str_interp("coefs_${base_name}")
  write_csv(coef_data, file.path(export_dir, coef_output_file))
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
  switch(file_type,
         daily = {
           start_dates <- as.Date(
             sub("^.*/.*_as_of_([0-9]{8}).parquet$", "\\1", files_list),
             format = date_format
           )
           end_dates <- start_dates
         },
         rollup = {
           rollup_pattern <- "^.*/.*_from_([0-9]{8})_to_([0-9]{8}).parquet$"
           start_dates <- as.Date(
             sub(rollup_pattern, "\\1", files_list),
             format = date_format
           )
           end_dates <- as.Date(
             sub(rollup_pattern, "\\2", files_list),
             format = date_format
           )
         }
  )
  
  ## TODO: start_date depends on if we're doing model training or just corrections.
  start_date <- TODAY - params$training_days - params$ref_lag
  end_date <- TODAY - 1
  
  # Only keep files with data that falls at least somewhat between the desired
  # start and end range dates.
  files_list <- files_list[
    !(( start_dates < start_date & end_dates < start_date ) | 
        ( start_dates > end_date & end_dates > end_date ))]
  
  return(files_list)
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
         daily = str_interp("${indicator}_${signal}_as_of_[0-9]{8}.parquet$"),
         rollup = str_interp("${indicator}_${signal}_from_[0-9]{8}_to_[0-9]{8}.parquet$")
  )
}
