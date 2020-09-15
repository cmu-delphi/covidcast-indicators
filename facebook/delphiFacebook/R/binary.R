#' Fetch binary indicators to report in aggregates
#'
#' @importFrom tibble tribble
get_binary_indicators <- function() {
  ind <- tribble(
    ~name, ~var_weight, ~metric, ~smooth_days, ~compute_fn, ~post_fn,
    "raw_hh_cmnty_cli", "weight_unif", "hh_community_yes", 0, compute_binary_response, jeffreys_binary,
    "raw_nohh_cmnty_cli", "weight_unif", "community_yes", 0, compute_binary_response, jeffreys_binary,
    "raw_whh_cmnty_cli", "weight", "hh_community_yes", 0, compute_binary_response, jeffreys_binary,
    "raw_wnohh_cmnty_cli", "weight", "community_yes", 0, compute_binary_response, jeffreys_binary,

    "smoothed_hh_cmnty_cli", "weight_unif", "hh_community_yes", 6, compute_binary_response, jeffreys_binary,
    "smoothed_nohh_cmnty_cli", "weight_unif", "community_yes", 6, compute_binary_response, jeffreys_binary,
    "smoothed_whh_cmnty_cli", "weight", "hh_community_yes", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wnohh_cmnty_cli", "weight", "community_yes", 6, compute_binary_response, jeffreys_binary,

    # mask wearing
    "wip_smoothed_wearing_mask", "weight_unif", "c_mask_often", 6, compute_binary_response, jeffreys_binary,
    "wip_smoothed_wwearing_mask", "weight", "c_mask_often", 6, compute_binary_response, jeffreys_binary,

    # mental health
    "wip_smoothed_worried_become_ill", "weight_unif", "mh_worried_ill", 6, compute_binary_response, jeffreys_binary,
    "wip_smoothed_wworried_become_ill", "weight", "mh_worried_ill", 6, compute_binary_response, jeffreys_binary,

    "wip_smoothed_anxious", "weight_unif", "mh_anxious", 6, compute_binary_response, jeffreys_binary,
    "wip_smoothed_depressed", "weight_unif", "mh_depressed", 6, compute_binary_response, jeffreys_binary,

    # travel outside state
    "wip_smoothed_travel_outside_state_5d", "weight_unif", "c_travel_state", 6, compute_binary_response, jeffreys_binary,
    "wip_smoothed_wtravel_outside_state_5d", "weight", "c_travel_state", 6, compute_binary_response, jeffreys_binary,

    # work outside home
    "wip_smoothed_work_outside_home_5d", "weight_unif", "c_work_outside_5d", 6, compute_binary_response, jeffreys_binary,
    "wip_smoothed_wwork_outside_home_5d", "weight", "c_work_outside_5d", 6, compute_binary_response, jeffreys_binary,

    # testing
    "wip_smoothed_tested_14d", "weight_unif", "t_tested_14d", 6, compute_binary_response, jeffreys_binary,
    "wip_smoothed_wtested_14d", "weight", "t_tested_14d", 6, compute_binary_response, jeffreys_binary,
    "wip_smoothed_tested_positive_14d", "weight_unif", "t_tested_positive_14d", 6, compute_binary_response, jeffreys_binary,
    "wip_smoothed_wtested_positive_14d", "weight", "t_tested_positive_14d", 6, compute_binary_response, jeffreys_binary,
    "wip_smoothed_wanted_test_14d", "weight_unif", "t_wanted_test_14d", 6, compute_binary_response, jeffreys_binary,
    "wip_smoothed_wwanted_test_14d", "weight", "t_wanted_test_14d", 6, compute_binary_response, jeffreys_binary
  )

  ind$skip_mixing <- TRUE

  return(ind)
}

#' Returns binary response estimates
#'
#' This function takes vectors as input and computes the binary response values
#' (a point estimate named "val", a standard error named "se", and a sample size
#' named "sample_size").
#'
#' @param response a vector of binary (0 or 1) responses
#' @param weight a vector of sample weights for inverse probability weighting;
#'   invariant up to a scaling factor
#' @param sample_size The sample size to use, which may be a non-integer (as
#'   responses from ZIPs that span geographical boundaries are weighted
#'   proportionately, and survey weights may also be applied)
#'
#' @importFrom stats weighted.mean
#' @export
compute_binary_response <- function(response, weight, sample_size)
{
  assert(all( (response == 0) | (response == 1) ))
  assert(length(response) == length(weight))

  response_prop <- weighted.mean(response, weight)

  val <- 100 * response_prop

  return(list(val = val,
              se = NA_real_,
              effective_sample_size = sample_size)) # TODO effective sample size
}

#' Apply a Jeffreys correction to estimates and their standard errors.
#'
#' @param df Data frame
#' @return Updated data frame.
#' @importFrom dplyr mutate
jeffreys_binary <- function(df) {
  return(mutate(df,
                val = jeffreys_percentage(.data$val, .data$sample_size),
                se = binary_se(.data$val, .data$sample_size)))
}

#' Adjust a percentage estimate to use the Jeffreys method.
#'
#' Takes a previously estimated percentage (calculated with num_yes / total *
#' 100) and replaces it with the Jeffreys version, where one pseudo-observation
#' with 50% yes is inserted.
#'
#' @param percentage Vector of percentages to adjust.
#' @param sample_size Vector of corresponding sample sizes.
#' @return Vector of adjusted percentages.
jeffreys_percentage <- function(percentage, sample_size) {
  return((percentage * sample_size + 50) / (sample_size + 1))
}

#' Calculate the standard error for a binary proportion (as a percentage)
#'
#' @param val Vector of estimated percentages
#' @param sample_size Vector of corresponding sample sizes
#' @return Vector of standard errors; NA when a sample size is 0.
binary_se <- function(val, sample_size) {
  return(ifelse(sample_size > 0,
                sqrt( (val * (100 - val) / sample_size) ),
                NA))
}
