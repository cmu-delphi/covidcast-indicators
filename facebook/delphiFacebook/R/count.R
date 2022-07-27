#' Fetch data frame specifying all count indicators to report aggregates.
#'
#' @importFrom tibble tribble
get_hh_count_indicators <- function() {
  ind <- tribble(
    ~name, ~var_weight, ~metric, ~smooth_days, ~compute_fn, ~post_fn,
    "raw_cli", "weight_unif", "hh_p_cli", 0, compute_count_response, jeffreys_count,
    "raw_ili", "weight_unif", "hh_p_ili", 0, compute_count_response, jeffreys_count,
    "raw_wcli", "weight", "hh_p_cli", 0, compute_count_response, jeffreys_count,
    "raw_wili", "weight", "hh_p_ili", 0, compute_count_response, jeffreys_count,

    "smoothed_cli", "weight_unif", "hh_p_cli", 6, compute_count_response, jeffreys_count,
    "smoothed_ili", "weight_unif", "hh_p_ili", 6, compute_count_response, jeffreys_count,
    "smoothed_wcli", "weight", "hh_p_cli", 6, compute_count_response, jeffreys_count,
    "smoothed_wili", "weight", "hh_p_ili", 6, compute_count_response, jeffreys_count
  )

  return(ind)
}

#' Returns response estimates for a single geographic area.
#'
#' This function takes vectors as input and computes the count response values
#' (a point estimate named "val", a standard error named "se", and an effective
#' sample size named "effective_sample_size").
#'
#' @param response a vector of percentages (100 * cnt / total)
#' @param weight a vector of sample weights for inverse probability weighting;
#'   invariant up to a scaling factor
#' @param sample_size Unused.
#'
#' @importFrom stats weighted.mean
#' @export
compute_count_response <- function(response, weight, sample_size)
{
  assert(all( response >= 0 & response <= 100 ))
  assert(length(response) == length(weight))

  weight <- weight / sum(weight)
  val <- weighted.mean(response, weight)

  effective_sample_size <- length(weight) * mean(weight)^2 / mean(weight^2)

  se <- sqrt( sum( (weight * (response - val))^2 ) )

  return(list(
    val = val,
    se = se,
    effective_sample_size = effective_sample_size
  ))
}

#' Apply a Jeffreys correction to standard errors.
#'
#' See `jeffreys_se()` for reasoning and methods.
#'
#' @param df Data frame
#' @return Updated data frame.
#' @importFrom dplyr mutate
jeffreys_count <- function(df) {
  return(mutate(df,
                se = jeffreys_se(.data$se, .data$val,
                                 .data$effective_sample_size)))
}

#' Apply Jeffreys Prior to adjust standard error values.
#'
#' The Jeffreys approach for estimating binomial proportions assumes a Beta(1/2,
#' 1/2) prior on the proportion. If x is the number of successes, the posterior
#' mean is hence (x + 0.5) / (n + 1), which prevents the estimate from ever
#' being 0 or 1. This is desirable because the typical normal approximation SE
#' would be 0 in both cases, which is both misleading and prevents reasonable
#' resampling of the data for bootstrapping.
#'
#' We apply the Jeffreys approach only to the calculation of the standard error;
#' applying it to the estimate of proportion would introduce too much bias for
#' small proportions, like we typically see for symptoms within households.
#'
#' @param old_se          a numeric vector of previous standard errors
#' @param percent         a numeric vector of the the estimated point estimates
#' @param sample_size     a numeric vector of the sample sizes
#'
#' @export
jeffreys_se <- function(old_se, percent, sample_size)
{
  sqrt((50 - percent)^2 + sample_size^2 * old_se^2) / (1 + sample_size)
}
