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
#' params$input_dir: link to the input data file
#' params$testing_window: the testing window used for saving the runtime. Could
#'     set it to be 1 if time allows
#' params$test_dates: list of two elements, the first one is the start date and
#'     the second one is the end date
#' params$training_days: set it to be 270 or larger if you have enough data
#' params$num_col: the column name for the counts of the numerator, e.g. the
#'     number of COVID claims
#' params$denom_col: the column name for the counts of the denominator, e.g. the
#'     number of total claims
#' params$geo_level: character vector of "state" and "county", by default
#' params$taus: vector of considered quantiles
#' params$lambda: the level of lasso penalty
#' params$export_dir: directory to save corrected data to
#' params$lp_solver: LP solver to use in quantile_lasso(); "gurobi" or "glpk"
#'
#' @param path path to the parameters file; if not present, will try to copy the file
#'     "params.json.template"
#' @param template_path path to the template parameters file
#' @template train_models-template
#' @template make_predictions-template
#'
#' @return a named list of parameters values
#'
#' @importFrom dplyr if_else
#' @importFrom jsonlite read_json
read_params <- function(path = "params.json", template_path = "params.json.template",
                        train_models = TRUE, make_predictions = TRUE) {
  if (!file.exists(path)) {file.copy(template_path, path)}
  params <- read_json(path, simplifyVector = TRUE)

  # Required parameters
  if (!("input_dir" %in% names(params)) || !dir.exists(params$input_dir)) {
    stop("input_dir must be set in `params` and exist")
  }
  params$train_models <- train_models
  params$make_predictions <- make_predictions
  
  ## Set default parameter values if not specified
  # Paths
  if (!("export_dir" %in% names(params))) {params$export_dir <- "./receiving"}
  if (!("cache_dir" %in% names(params))) {params$cache_dir <- "./cache"}

  # Parallel parameters
  if (!("parallel" %in% names(params))) {params$parallel <- FALSE}
  if (!("parallel_max_cores" %in% names(params))) {params$parallel_max_cores <- .Machine$integer.max}

  # Model parameters
  if (!("taus" %in% names(params))) {params$taus <- TAUS}
  if (!("lambda" %in% names(params))) {params$lambda <- LAMBDA}
  if (!("lp_solver" %in% names(params))) {params$lp_solver <- LP_SOLVER}

  # Data parameters
  if (!("num_col" %in% names(params))) {params$num_col <- "num"}
  if (!("denom_col" %in% names(params))) {params$denom_col <- "denom"}
  if (!("geo_level" %in% names(params))) {params$geo_level <- c("state", "county")}
  if (!("value_types" %in% names(params))) {params$lp_solver <- c("count", "fraction")}

  # Date parameters
  if (!("training_days" %in% names(params))) {params$training_days <- TRAINING_DAYS}
  if (!("ref_lag" %in% names(params))) {params$ref_lag <- REF_LAG}
  if (!("testing_window" %in% names(params))) {params$testing_window <- TESTING_WINDOW}
  if (!("test_dates" %in% names(params)) || length(params$test_dates) == 0) {
    start_date <- TODAY - params$testing_window
    end_date <- TODAY - 1
    params$test_dates <- seq(start_date, end_date, by="days")
  }
  
  return(params)
}

#' Create directory if not already existing
#'
#' @param path string specifying a directory to create
#'
#' @export
create_dir_not_exist <- function(path)
{
  if (!dir.exists(path)) { dir.create(path) }
}

#' Check input data for validity
#'
#' @template df-template
#' @template value_type-template
#' @template num_col-template
#' @template denom_col-template
#' @template signal_suffixes-template
#'
#' @return list of input dataframe augmented with lag column, if it
#'     didn't already exist, and character vector of one or two value
#'     column names, depending on requested `value_type`
validity_checks <- function(df, value_type, num_col, denom_col, signal_suffixes) {
  if (!missing(signal_suffixes)) {
    num_col <- paste(num_col, signal_suffixes, sep = "_")
    denom_col <- paste(num_col, signal_suffixes, sep = "_")
  }

  # Check data type and required columns
  if (value_type == "count") {
    if (all(num_col %in% colnames(df))) {value_cols=c(num_col)}
    else if (all(denom_col %in% colnames(df))) {value_cols=c(denom_col)}
    else {stop("No valid column name detected for the count values!")}
  } else if (value_type == "fraction") {
    value_cols = c(num_col, denom_col)
    if ( any(!(value_cols %in% colnames(df))) ) {
      stop("No valid column name detected for the fraction values!")
    }
  }
  
  # time_value must exist in the dataset
  if ( !"time_value" %in% colnames(df) ) {
    stop("No 'time_value' column detected for the reference date!")
  }
  
  # issue_date or lag should exist in the dataset
  if ( !"lag" %in% colnames(df) ) {
    if ( "issue_date" %in% colnames(df) ) {
      df$lag = as.integer(df$issue_date - df$time_value)
    }
    else {stop("No issue_date or lag exists!")}
  }

  return(list(df = df, value_cols = value_cols))
}

#' Check available training days
#'
#' @param issue_date contents of input data's `issue_date` column
#' @template training_days-template
training_days_check <- function(issue_date, training_days = TRAINING_DAYS) {
  valid_training_days = as.integer(max(issue_date) - min(issue_date))
  if (training_days > valid_training_days) {
    warning(sprintf("Only %d days are available at most for training.", valid_training_days))
  }
}

#' Subset list of counties to those included in the 200 most populous in the US
#'
#' @param geos character vector of county FIPS codes
filter_counties <- function(geos) {
  top_200_geos <- get_populous_counties()
  return(intersect(geos, top_200_geos))
}

#' Subset list of counties to those included in the 200 most populous in the US
#' 
#' @importFrom dplyr select %>% arrange desc pull
#' @importFrom rlang .data
#' @importFrom utils head
get_populous_counties <- function() {
  return(
    covidcast::county_census %>%
      select(pop = .data$POPESTIMATE2019, fips = .data$FIPS) %>%
      # Drop megacounties (states)
      filter(!endsWith(.data$fips, "000")) %>%
      arrange(desc(.data$pop)) %>%
      pull(.data$fips) %>%
      head(n=200)
  )
}
