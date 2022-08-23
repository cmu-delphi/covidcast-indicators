#' Return params file as an R list
#'
#' Reads a parameters file. If the file does not exist, the function will create a copy of
#' '"params.json.template" and read from that.
#'
#' @param path    path to the parameters file; if not present, will try to copy the file
#'                "params.json.template"
#' @param template_path    path to the template parameters file
#'
#' @return a named list of parameters values
#'
#' @importFrom dplyr if_else
#' @importFrom jsonlite read_json
#' @importFrom lubridate ymd_hms
#' @export
read_params <- function(path = "params.json", template_path = "params.json.template") {
  if (!file.exists(path)) file.copy(template_path, path)
  params <- read_json(path, simplifyVector = TRUE)
  
  params$num_filter <- if_else(params$debug, 2L, 100L)
  params$s_weight <- if_else(params$debug, 1.00, 0.01)
  params$s_mix_coef <- if_else(params$debug, 0.05, 0.05)
  
  params$start_time <- ymd_hms(
    sprintf("%s 00:00:00", params$start_date), tz = tz_to
  )
  params$end_time <- ymd_hms(
    sprintf("%s 23:59:59", params$end_date), tz = tz_to
  )
  
  params$parallel_max_cores <- if_else(
    is.null(params$parallel_max_cores),
    .Machine$integer.max,
    params$parallel_max_cores
  )
  
  return(params)
}

#' Create directory if not already existing
#'
#' @param path character vector giving the directory to create
#'
#' @export
create_dir_not_exist <- function(path)
{
  if (!dir.exists(path)) { dir.create(path) }
}

#' Read a parquet file into a dataframe
#' 
#' @param path path to the input data
#'
#' @importFrom arrow read_parquet
#' @importFrom dplyr select %>%
#'
#' @export
read_data <- function(path){
  df <- read_parquet(path, as_data_frame = TRUE) %>% 
    select(-`__index_level_0__`)
  return (df)
}


#‘ Export the result to customized directory

#' @param test_data test data with prediction result
#' @param coef_data data frame with the estimated coefficients
#' @param export_dir export directory
#' @param geo_level geographical level, can be county or state
#' @param geo the geogrpahical location
#' @param test_lag 
#' 
#' @export
export_test_result <- function(test_data, coef_data, export_dir, geo){
  pred_output_dir = paste("prediction", geo, sep="_")
  write.csv(test_data, paste(export_dir, pred_output_dir , ".csv", sep=""), row.names = FALSE)
  
  coef_output_dir = paste("coefs", geo, sep="_")
  write.csv(test_data, paste(export_dir, coef_output_dir , ".csv", sep=""), row.names = FALSE)
  
}


#' Check input data for validity
validity_checks <- function(df, value_type) {
  # Check data type and required columns
  if (value_type == "count"){
    if (num_col %in% colnames(df)) {value_cols=c(num_col)}
    else if (denom_col %in% colnames(df)) {value_cols=c(denom_col)}
    else {
      stop("No valid column name detected for the count values!")
    }
  } else if (value_type == "fraction"){
    value_cols = c(num_col, denom_col)
    if ( any(!value_cols %in% colnames(df)) ){
      stop("No valid column name detected for the fraction values!")
    }
  }
  
  # time_value must exists in the dataset
  if ( !"time_value" %in% colnames(df) ){stop("No column for the reference date")}
  
  # issue_date or lag should exist in the dataset
  if ( !"lag" %in% colnames(df) ){
    if ( "issue_date" %in% colnames(df) ){
      df$lag = as.integer(df$issue_date - df$time_value)
    }
    else {stop("No issue_date or lag exists!")}
  }
}

#' Check available training days
training_days_check <- function(issue_date, training_days) {
  valid_training_days = as.integer(max(issue_date) - min(issue_date))
  if (training_days > valid_training_days){
    warning(sprintf("Only %d days are available at most for training.", valid_training_days))
  }
}

#' List valid input files.
get_files_list(indicator, signal, geo_level, params) {
  # Convert input_group into file names.
  daily_pattern <- create_name_pattern(
    indicator, signal, geo_level, "daily"
  )
  rollup_pattern <- create_name_pattern(
    indicator, signal, geo_level, "rollup"
  )
  
  # Make sure we're reading in both 4-week rollup and daily files.
  daily_input_files <- list.files(params$data_path, pattern = daily_pattern)
  rollup_input_files <- list.files(params$data_path, pattern = rollup_pattern)
  
  # Filter files lists to only include those containing dates we need for training
  daily_input_files <- subset_valid_files(daily_input_files, "daily", params)
  rollup_input_files <- subset_valid_files(rollup_input_files, "rollup", params)
  
  return(c(daily_input_files, rollup_input_files))
}

#' Return file names only if they contain data to be used in training
#' 
#' Parse filenames to find included dates. Use different patterns if file
#' includes daily or rollup (multiple days) data.
subset_valid_files <- function(files_list, file_type = c("daily", "rollup"), params) {
  file_type <- match.arg(file_type)
  switch(file_type,
         daily = {
           ...
         },
         rollup = {
           ...
         }
  )
}

#' Create pattern to match input files of a given type, signal, and geo level
#' 
#' @importFrom stringr str_interp
create_name_pattern <- function(indicator, signal, geo_level,
                                file_type = c("daily", "rollup")) {
  file_type <- match.arg(file_type)
  switch(file_type,
         daily = str_interp("{indicator}_{signal}_as_of_[0-9]{8}.parquet"),
         rollup = str_interp("{indicator}_{signal}_from_[0-9]{8}_to_[0-9]{8}.parquet")
  )
}

