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
#' @param s_mix_coef Minimum allowable mixing coefficient.
#' @param s_weight Maximum desired normalized mixing weight for any one observation.
#' @export
mix_weights <- function(weights, s_mix_coef, s_weight)
{
  N <- length(weights)
  
  ## Step 1: Normalize weights to sum to 1.
  weights <- weights / sum(weights)
  
  ## Step 2: Choose a mixing coefficient to bring down the maximum weight.
  max_weight <- max(weights)
  
  ## Choose the mix_coef to solve this problem:
  ##
  ## max_weight * (1 - mix_coef) + mix_coef / N <= s_weight
  ##
  ## TODO: Determine if the fudge factors are really necessary
  mix_coef <- if (max_weight <= s_weight) {
    0
  } else if (1/N > s_weight*0.999) {
    1
  } else {
    (max_weight * N - 0.999 * N * s_weight + 1e-6) /
      (max_weight * N - 1 + 1e-6)
  }
  precoef <- mix_coef
  
  ## Enforce minimum and maximum.
  if (mix_coef < s_mix_coef) { mix_coef <- s_mix_coef }
  if (mix_coef > 1) { mix_coef <- 1 }
  
  ## Step 3: Replace weights.
  new_weights <- mix_coef / N + (1 - mix_coef) * weights
  
  return(list(
    weights=new_weights,
    coef=mix_coef,
    precoef=precoef,
    maxp=max_weight,
    normalized_preweights=weights
  ))
}


#' Aggregates counties into megacounties that have low sample size values for a
#' given day.
#'
#' @param df_intr Input tibble that requires aggregation, with `geo_id`, `val`,
#'     `sample_size`, `effective_sample_size`, and `se` columns.
#' @param threshold Sample size value below which counties should be grouped
#'     into megacounties.
#' @param groupby_vars Character vector of column names to perform `group_by`
#'     over
#' @return Tibble of megacounties. Counties that are not grouped are not
#'     included in the output.
#' @importFrom dplyr group_by across all_of
megacounty <- function(
  df_intr, threshold, groupby_vars=c("day", "geo_id")
)
{
  df_megacounties <- df_intr[df_intr$sample_size < threshold |
                               df_intr$effective_sample_size < threshold, ]
  
  df_megacounties <- mutate(df_megacounties,
                            geo_id = make_megacounty_fips(.data$geo_id))
  
  df_megacounties <- group_by(df_megacounties, across(all_of(groupby_vars)))
  df_megacounties <- mutate(
    df_megacounties,
    county_weight = .data$effective_sample_size / sum(.data$effective_sample_size))
  
  df_megacounties <- summarize(
    df_megacounties,
    val = weighted.mean(.data$val, .data$effective_sample_size),
    se = sqrt(sum(.data$se^2 * .data$county_weight^2)),
    sample_size = sum(.data$sample_size),
    effective_sample_size = sum(.data$effective_sample_size)
  )
  
  df_megacounties <- mutate(df_megacounties, county_weight = NULL)
  df_megacounties <- ungroup(df_megacounties)
  
  return(df_megacounties)
}

#' Converts county FIPS code to megacounty code.
#'
#' We designate megacounties with a special FIPS ending in 000; for example, the
#' megacounty for state 26 would be 26000 and would comprise counties with FIPS
#' codes 26XXX.
#'
#' @param fips Geo-id
#' @return Megacounty
make_megacounty_fips <- function(fips) {
  paste0(substr(fips, 1, 2), "000")
}

#' `any_true` returns TRUE if at least one is TRUE
#' Returns FALSE if at least one is FALSE and none are TRUE
#' Returns NA if all are NA
#' 
#' @param ... One or more logical vectors of the same length.
#' @return A logical vector of the same length as the input vector(s).
#' @noRd
is_true <- function(x) x %in% TRUE
or <- function(a, b) ifelse(is.na(a) & is.na(b), NA, is_true(a) | is_true(b))
any_true <- function(...) Reduce(or, list(...), NA)

#' `all_true` returns TRUE if all are TRUE
#' Returns FALSE if at least one is FALSE and none are NA
#' Returns NA if at least one is NA
#' 
#' @param ... One or more logical vectors of the same length.
#' @return A logical vector of the same length as the input vector(s).
#' @noRd
and <- function(a, b) ifelse(is.na(a) | is.na(b), NA, a & b)
all_true <- function(...) Reduce(and, list(...), TRUE)