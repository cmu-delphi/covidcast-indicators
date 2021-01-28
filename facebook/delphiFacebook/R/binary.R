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
    "smoothed_wearing_mask", "weight_unif", "c_mask_often", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wwearing_mask", "weight", "c_mask_often", 6, compute_binary_response, jeffreys_binary,
    "smoothed_others_masked", "weight_unif", "c_others_masked", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wothers_masked", "weight", "c_others_masked", 6, compute_binary_response, jeffreys_binary,

    # mental health
    "smoothed_worried_become_ill", "weight_unif", "mh_worried_ill", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wworried_become_ill", "weight", "mh_worried_ill", 6, compute_binary_response, jeffreys_binary,
    "smoothed_worried_finances", "weight_unif", "mh_worried_finances", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wworried_finances", "weight", "mh_worried_finances", 6, compute_binary_response, jeffreys_binary,

    "smoothed_anxious_5d", "weight_unif", "mh_anxious", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wanxious_5d", "weight", "mh_anxious", 6, compute_binary_response, jeffreys_binary,
    "smoothed_depressed_5d", "weight_unif", "mh_depressed", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wdepressed_5d", "weight", "mh_depressed", 6, compute_binary_response, jeffreys_binary,
    "smoothed_felt_isolated_5d", "weight_unif", "mh_isolated", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wfelt_isolated_5d", "weight", "mh_isolated", 6, compute_binary_response, jeffreys_binary,

    # travel outside state
    "smoothed_travel_outside_state_5d", "weight_unif", "c_travel_state", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wtravel_outside_state_5d", "weight", "c_travel_state", 6, compute_binary_response, jeffreys_binary,

    # work outside home
    # pre-wave-4
    "wip_smoothed_work_outside_home_5d", "weight_unif", "c_work_outside_5d", 6, compute_binary_response, jeffreys_binary,
    "wip_smoothed_wwork_outside_home_5d", "weight", "c_work_outside_5d", 6, compute_binary_response, jeffreys_binary,
    # wave 4+
    "smoothed_work_outside_home_1d", "weight_unif", "a_work_outside_home_1d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wwork_outside_home_1d", "weight", "a_work_outside_home_1d", 6, compute_binary_response, jeffreys_binary,

    # activities
    "smoothed_shop_1d", "weight_unif", "a_shop_1d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wshop_1d", "weight", "a_shop_1d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_restaurant_1d", "weight_unif", "a_restaurant_1d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wrestaurant_1d", "weight", "a_restaurant_1d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_spent_time_1d", "weight_unif", "a_spent_time_1d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wspent_time_1d", "weight", "a_spent_time_1d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_large_event_1d", "weight_unif", "a_large_event_1d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wlarge_event_1d", "weight", "a_large_event_1d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_public_transit_1d", "weight_unif", "a_public_transit_1d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wpublic_transit_1d", "weight", "a_public_transit_1d", 6, compute_binary_response, jeffreys_binary,

    # testing
    "smoothed_tested_14d", "weight_unif", "t_tested_14d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wtested_14d", "weight", "t_tested_14d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_tested_positive_14d", "weight_unif", "t_tested_positive_14d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wtested_positive_14d", "weight", "t_tested_positive_14d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wanted_test_14d", "weight_unif", "t_wanted_test_14d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wwanted_test_14d", "weight", "t_wanted_test_14d", 6, compute_binary_response, jeffreys_binary,

    # vaccines
    "smoothed_accept_covid_vaccine", "weight_unif", "v_accept_covid_vaccine", 6, compute_binary_response, jeffreys_binary,
    "smoothed_waccept_covid_vaccine", "weight", "v_accept_covid_vaccine", 6, compute_binary_response, jeffreys_binary,
    "smoothed_covid_vaccinated", "weight_unif", "v_covid_vaccinated", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wcovid_vaccinated", "weight", "v_covid_vaccinated", 6, compute_binary_response, jeffreys_binary,
    "smoothed_worried_vaccine_side_effects", "weight_unif", "v_worried_vaccine_side_effects", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wworried_vaccine_side_effects", "weight", "v_worried_vaccine_side_effects", 6, compute_binary_response, jeffreys_binary,

    # who would make more likely to accept vaccine
    "smoothed_vaccine_likely_friends", "weight_unif", "v_vaccine_likely_friends", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_likely_friends", "weight", "v_vaccine_likely_friends", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_likely_local_health", "weight_unif", "v_vaccine_likely_local_health", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_likely_local_health", "weight", "v_vaccine_likely_local_health", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_likely_who", "weight_unif", "v_vaccine_likely_who", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_likely_who", "weight", "v_vaccine_likely_who", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_likely_govt_health", "weight_unif", "v_vaccine_likely_govt_health", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_likely_govt_health", "weight", "v_vaccine_likely_govt_health", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_likely_politicians", "weight_unif", "v_vaccine_likely_politicians", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_likely_politicians", "weight", "v_vaccine_likely_politicians", 6, compute_binary_response, jeffreys_binary

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
