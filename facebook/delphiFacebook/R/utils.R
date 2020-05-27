#' Return params file as an R list
#'
#' Reads a parameters file. If the file does not exist, the function will create a copy of
#' '"params.json.template" and read from that.
#'
#' @param path    path to the parameters file; if not present, will try to copy the file
#'                "params.json.template"
#'
#' @return a named list of parameters values
#'
#' @importFrom dplyr if_else
#' @importFrom jsonlite read_json
#' @importFrom lubridate ymd_hms
#' @export
read_params <- function(path = "params.json") {
  if (!file.exists(path)) file.copy("params.json.template", "params.json")
  params <- read_json(path, simplifyVector = TRUE)

  params$num_filter <- if_else(params$debug, 2L, 100L)
  params$s_weight <- if_else(params$debug, 1.00, 0.01)
  params$s_mix_coef <- if_else(params$debug, 0.05, 0.05)
  params$start_time <- ymd_hms(
    sprintf("%s 00:00:00", params$start_date), tz = "America/Los_Angeles"
  )
  params$end_time <- ymd_hms(
    sprintf("%s 23:59:59", params$end_date), tz = "America/Los_Angeles"
  )

  return(params)
}

#' Write a message to the console
#'
#' @param text the body of the message to display
#' @param df a data frame; the message will show the number of rows in the data frame
#'
#' @export
msg_df <- function(text, df) {
  message(sprintf("%s --- %s: %d rows", format(Sys.time()), text, nrow(df)))
}

#' Assert a logical value
#'
#' Will issue a \code{stop} command if the given statement is false.
#'
#' @param statement a logical value
#' @param msg a character string displayed as an additional message
#'
#' @export
assert <- function(statement, msg="")
{
  if (!statement)
  {
    stop(msg, call.=(msg==""))
  }
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

#' Return vector from the past n days, inclusive
#'
#' Returns dates as strings in the form "YYYYMMDD"
#'
#' @param date   a string containing a single date that can be parsed with `ymd`, such as
#'               "20201215"
#' @param ndays  how many days in the past to include
#'
#'
#' @importFrom lubridate ymd
#' @export
past_n_days <- function(date, ndays = 0L)
{
  return(format(ymd(date) - seq(0, ndays), format = "%Y%m%d"))
}

#' Adjust weights so no weight is not too much of the final estimate
#'
#' @param weights     a vector of sample weights
#' @param params      a named list containing an element named "num_filter"; the maximum
#'                    weight is assumed to be 1 / "num_filter" * 0.999.
#' @export
mix_weights <- function(weights, params)
{
  weights <- weights / sum(weights)
  max_weight <- max(weights)
  max_allowed_weight <- 1 / params$num_filter * 0.999

  mix_coef <- (max_weight - max_allowed_weight) / (max_weight  - 1 / length(weights))
  if (mix_coef < 0) { mix_coef <- 0 }
  if (mix_coef > 1) { mix_coef <- 1 }
  new_weights <- mix_coef / length(weights) + (1 - mix_coef) * weights

  return(new_weights)
}
