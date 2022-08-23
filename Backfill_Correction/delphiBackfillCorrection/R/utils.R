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
