#' Return params file as an R list
#'
#' Reads a parameters file. If the file does not exist, the function will create a copy of
#' '"params.json.template" and read from that.
#'
#' A params list should contain the following fields. If not included,
#' they will be filled with default values when possible.
#'
#' params$ref_lag: reference lag, after x days, the update is considered to be
#'     the response. 60 is a reasonable choice for CHNG outpatient data
#' params$data_path: link to the input data file
#' params$testing_window: the testing window used for saving the runtime. Could
#'     set it to be 1 if time allows
#' params$test_dates: list of two elements, the first one is the start date and
#'     the second one is the end date
#' params$training_days: set it to be 270 or larger if you have enough data
#' params$num_col: the column name for the counts of the numerator, e.g. the
#'     number of COVID claims
#' params$denom_col: the column name for the counts of the denominator, e.g. the
#'     number of total claims
#' params$geo_level: list("state", "county")
#' params$taus: ??
#' params$lambda: ??
#' params$export_dir: ??
#' params$lp_solver: LP solver to use in quantile_lasso(); "gurobi" or "glpk"
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

#' Subset list of counties to those included in the 200 most populous in the US
filter_counties <- function(geos) {
  top_200_geos <- get_populous_counties()
  return(intersect(geos, top_200_geos))
}

#' Subset list of counties to those included in the 200 most populous in the US
#' 
#' @importFrom dplyr select %>% arrange desc
get_populous_counties <- function() {
  return(
    covidcast::county_census %>%
      select(pop = POPESTIMATE2019, fips = FIPS) %>%
      # Drop megacounties (states)
      filter(!endsWith(fips, "000")) %>% 
      arrange(desc(pop)) %>%
      pull(fips) %>%
      head(n=200)
  )
}
