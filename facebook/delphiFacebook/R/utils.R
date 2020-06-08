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
  date_ymd <- ymd(date)
  date_ymd <- rep(date_ymd, each = (ndays + 1L))
  out <- format(date_ymd - seq(0, ndays), format = "%Y%m%d")
  out <- matrix(out, ncol = (ndays + 1L), byrow = TRUE)
  return(out)
}

#' Adjust weights so no weight is too much of the final estimate.
#'
#' For privacy and estimation quality, we do not want to allow one survey
#' response to have such a high weight that it becomes most of an estimate.
#'
#' So, for a specific time and location:
#'
#' 1. Normalize weights so they sum to 1.
#'
#' 2. Determine a "mixing coefficient" based on the maximum weight. The mixing
#' coefficient is chosen to make the maximum weight smaller than
#' `params$s_weight`, subject to the constraint that the mixing coefficient must
#' be larger than `params$s_mix_coef`.
#'
#' 3. Replace weights with a weighted average of the original weights and
#' uniform weights (meaning 1/N for every observation), weighted by the mixing
#' coefficient.
#'
#' @param weights a vector of sample weights
#' @param params a named list containing an element named "s_mix_coef" and
#'   another called "s_weight". The maximum desired weight is assumed to be
#'   params$s_weight; the minimum allowable mixing coefficient is
#'   params$s_mix_coef.
#' @export
mix_weights <- function(weights, params)
{
  N <- length(weights)

  ## Step 1: Normalize weights to sum to 1.
  weights <- weights / sum(weights)

  ## Step 2: Choose a mixing coefficient to bring down the maximum weight.
  max_weight <- max(weights)

  ## Choose the mix_coef to solve this problem:
  ##
  ## max_weight * (1 - mix_coef) + mix_coef / N <= params$s_weight
  ##
  ## TODO: Determine if the fudge factors are really necessary
  mix_coef <- (max_weight * N - 0.999 * N * params$s_weight + 1e-6) /
    (max_weight * N - 1 + 1e-6)

  ## Enforce minimum and maximum.
  if (mix_coef < params$s_mix_coef) { mix_coef <- params$s_mix_coef }
  if (mix_coef > 1) { mix_coef <- 1 }

  ## Step 3: Replace weights.
  new_weights <- mix_coef / N + (1 - mix_coef) * weights

  return(new_weights)
}
