#' Return params file as an R list
#'
#' Reads a parameters file. Copies global params to contingency params if not
#' already defined.
#'
#' @param path    path to the parameters file; if not present, will try to copy the file
#'                "params.json.template"
#' @param template_path    path to the template parameters file
#'
#' @return a named list of parameters values
#'
#' @export
read_contingency_params <- function(path = "params.json", template_path = "params.json.template") {
  params <- read_params(path, template_path)
  contingency_params <- params$contingency
  
  contingency_params$start_time <- ymd_hms(
    sprintf("%s 00:00:00", contingency_params$start_date), tz = tz_to
  )
  contingency_params$end_time <- ymd_hms(
    sprintf("%s 23:59:59", contingency_params$end_date), tz = tz_to
  )
  
  global_params <- c("archive_days", "backfill_days", "static_dir", "cache_dir", 
                     "archive_dir", "weights_in_dir", "input_dir", "debug", 
                     "parallel")
  for (param in global_params) {
    if ( is.null(contingency_params[[param]]) ) {
      contingency_params[[param]] <- params[[param]]
    }
  }
  
  contingency_params$num_filter <- if_else(contingency_params$debug, 2L, 100L)
  contingency_params$s_weight <- if_else(contingency_params$debug, 1.00, 0.01)
  contingency_params$s_mix_coef <- if_else(contingency_params$debug, 0.05, 0.05)
  
  return(contingency_params)
}


#' Update date and input files settings from params file
#' 
#' Use `end_date` and `aggregate_range` to calculate last full time period
#' (either month or week) to use for aggregations. Find all files in `input_dir`
#' within the calculated time period.
#'
#' @param params    Params object produced by read_params
#'
#' @return a named list of parameters values
#'
#' @importFrom lubridate ymd_hms as_date
#' 
#' @export
update_params <- function(params) {
  # Fill in end_time, if missing, with current time.
  if (is.null(params$end_time)) {
    params$end_time <- Sys.time()
  }
  
  if ( !is.null(params$start_date) ) {
    date_range <- list(params$start_time, params$end_time)
  } else {
    # If start_date is not provided, assume want to use preceding full time period.
    date_range <- get_range_prev_full_period(as_date(params$end_date), params$aggregate_range)
  }
  
  params$input <- get_filenames_in_range(date_range[[1]], date_range[[2]], params)
  if (length(params$input) == 0) {
    stop("no input files to read in")
  }
  
  params$start_time <- date_range[[1]]
  params$end_time <- date_range[[2]]
  params$start_date <- as_date(date_range[[1]])
  params$end_date <- as_date(date_range[[2]])

  return(params)
}

#' Get relevant input data file names from `input_dir`.
#'
#' @param start_date    Start of desired date range 
#' @param end_date    End of desired date range 
#' @param params    Params object produced by read_params
#'
#' @return Character vector of filenames
#' 
#' @importFrom lubridate as_date days
#' 
#' @export
get_filenames_in_range <- function(start_date, end_date, params) {
  start_date <- as_date(start_date) - days(params$backfill_days)
  end_date <- as_date(end_date)
  
  if ( is.null(params$input) | length(params$input) == 0 ) {
    date_pattern <- "^[0-9]{4}-[0-9]{2}-[0-9]{2}.*[.]csv$"
    youtube_pattern <- ".*YouTube[.]csv$"
    
    filenames <- list.files(path=params$input_dir)
    filenames <- filenames[grepl(date_pattern, filenames) & !grepl(youtube_pattern, filenames)]
  } else {
    filenames <- params$input
  }
    
  file_end_dates <- as_date(substr(filenames, 1, 10))
  file_start_dates <- as_date(substr(filenames, 12, 21))
  
  # Only keep files with data that falls at least somewhat between the desired
  # start and end range dates.
  filenames <- filenames[
    !(( file_start_dates < start_date & file_end_dates < start_date ) | 
        ( file_start_dates > end_date & file_end_dates > end_date ))]
  
  return(filenames)
}

#' Check user-set aggregations for basic validity and add a few necessary cols.
#'
#' @param aggregations Data frame with columns `name`, `var_weight`, `metric`,
#'   `group_by`, `compute_fn`, `post_fn`. Each row represents one aggregate
#'   to report. `name` is the aggregate's base column name; `var_weight` is the 
#'   column to use for its weights; `metric` is the column of `df` containing the
#'   response value. `group_by` is a list of variables used to perform the 
#'   aggregations over. `compute_fn` is the function that computes
#'   the aggregate response given many rows of data. `post_fn` is applied to the
#'   aggregate data after megacounty aggregation, and can perform any final
#'   calculations necessary.
#' 
#' @return a data frame of desired aggregations to calculate
#'
#' @export
verify_aggs <- function(aggregations) {
  aggregations <- unique(aggregations)
  aggregations$group_by <- as.list(aggregations$group_by)
  
  # Create unique row ID
  aggregations$id <- apply(aggregations[, c("name", "metric", "group_by")], 
                           MARGIN=1, FUN=paste, collapse="_")
  
  aggregations$var_weight <- "weight"
  aggregations$skip_mixing <- FALSE
  
  expected_names <- c("name", "var_weight", "metric", "group_by", "skip_mixing", 
                      "compute_fn", "post_fn", "id")
  if ( !all(expected_names %in% names(aggregations)) ) {
    stop(sprintf(
      "all expected columns %s must appear in the aggregations table", 
      paste(expected_names, collapse=", ")))
  }
  
  return(aggregations)
}

#' Write a message to the console
#'
#' @param text the body of the message to display
#'
#' @export
msg_plain <- function(text) {
  message(sprintf("%s --- %s", format(Sys.time()), text))
}
