#' Make tables specifying aggregations to output
#'
#' Each row represents one aggregate to report. `name` is the aggregate's base
#' column name.`metric` is the column of `df` containing the response value.
#' `group_by` is a list of variables used to perform the aggregations over.
#' `compute_fn` is the function that computes the aggregate response given many
#' rows of data. `post_fn` is applied to the aggregate data and can perform any
#' final calculations necessary.
#'
#' Listing no groupby vars implicitly computes aggregations at the national
#' level only. Any multiple choice and multi-select questions used in the
#' aggregations should be recoded to be descriptive in
#' `contingency_variables::reformat_responses`.
#'
#' Compute functions must be one of the `compute_*` set (or another function
#' with similar format can be created). Post-processing functions should be one
#' of the `jeffreys_*` set or post_convert_count_to_pct or the identity `I`,
#' which does not modify the data.
#'
#' @return named list
#'
#' @importFrom tibble tribble
#' @importFrom dplyr filter
get_aggs <- function() {

  regions <- list(
    "nation",
    "state"
  )

  groups <- list(
    c(),                # Use no grouping variables.
    c("age", "gender"), # Use age and gender.
    c("agefull", "gender"),
    c("age65plus", "gender"),
    c("age", "gender", "race", "hispanic"),
    c("age", "gender", "raceethnicity"),
    c("age65plus", "gender", "raceethnicity"),
    c("age", "gender", "healthcareworker"),
    c("raceethnicity", "healthcareworker"),
    c("age", "gender", "eligible"),
    c("age", "gender", "eligiblepregsmokeobese"),
    c("age"),
    c("agefull"),
    c("age65plus"),
    c("gender"),
    c("race", "hispanic"),
    c("raceethnicity"),
    c("healthcareworker"),
    c("eligible"),
    c("eligiblepregsmokeobese"),
    c("comorbidheartdisease"),
    c("comorbidcancer"),
    c("comorbidkidneydisease"),
    c("comorbidlungdisease"),
    c("comorbiddiabetes"),
    c("comorbidimmuno"),
    c("comorbidobese"),
    c("pregnant"),
    c("smoker"),
    c("edulevel"),
    c("age", "edulevel"),
    c("gender", "edulevel"),
    c("occupation"),
    c("pregnant", "raceethnicity"),
    c("race", "hispanic", "pregnant")
  )

  indicators <- tribble(
    ~name, ~metric, ~compute_fn, ~post_fn,
    "pct_wearing_mask_5d", "b_wearing_mask_5d", compute_binary, jeffreys_binary,
    "pct_wearing_mask_7d", "b_wearing_mask_7d", compute_binary, jeffreys_binary,
    "pct_cli", "n_cli", compute_household_binary, jeffreys_count,
    "pct_ili", "n_ili", compute_household_binary, jeffreys_count,
    "pct_hh_cmnty_cli", "b_hh_cmnty_cli", compute_binary, jeffreys_binary,
    "pct_direct_contact", "b_direct_contact", compute_binary, jeffreys_binary,
    "pct_anosmia", "b_anosmia", compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_vaccinated", compute_binary, jeffreys_binary,
    "pct_received_2_vaccine_doses", "b_received_2_vaccine_doses", compute_binary, jeffreys_binary,
    "pct_accept_vaccine", "b_accept_vaccine", compute_binary, jeffreys_binary,
    "pct_hesitant_vaccine", "b_hesitant_vaccine", compute_binary, jeffreys_binary,
    "pct_vaccinated_or_accept", "b_vaccinated_or_accept", compute_binary, jeffreys_binary,
    "pct_accept_vaccine_defyes", "b_accept_vaccine_defyes", compute_binary, jeffreys_multinomial_factory(4),
    "pct_accept_vaccine_probyes", "b_accept_vaccine_probyes", compute_binary, jeffreys_multinomial_factory(4),
    "pct_accept_vaccine_probno", "b_accept_vaccine_probno", compute_binary, jeffreys_multinomial_factory(4),
    "pct_accept_vaccine_defno", "b_accept_vaccine_defno", compute_binary, jeffreys_multinomial_factory(4),
    "pct_vaccine_likely_friends", "b_vaccine_likely_friends", compute_binary, jeffreys_binary,
    "pct_vaccine_likely_local_health", "b_vaccine_likely_local_health", compute_binary, jeffreys_binary,
    "pct_vaccine_likely_who", "b_vaccine_likely_who", compute_binary, jeffreys_binary,
    "pct_vaccine_likely_govt_health", "b_vaccine_likely_govt_health", compute_binary, jeffreys_binary,
    "pct_vaccine_likely_politicians", "b_vaccine_likely_politicians", compute_binary, jeffreys_binary,
    "pct_vaccine_likely_doctors", "b_vaccine_likely_doctors", compute_binary, jeffreys_binary,
    "pct_worried_vaccine_sideeffects", "b_worried_vaccine_sideeffects", compute_binary, jeffreys_binary,
    "pct_hesitant_worried_vaccine_sideeffects", "b_hesitant_worried_vaccine_sideeffects", compute_binary, jeffreys_binary,
    "pct_hesitant_vaccine_likely_friends", "b_hesitant_vaccine_likely_friends", compute_binary, jeffreys_binary,
    "pct_hesitant_vaccine_likely_local_health", "b_hesitant_vaccine_likely_local_health", compute_binary, jeffreys_binary,
    "pct_hesitant_vaccine_likely_who", "b_hesitant_vaccine_likely_who", compute_binary, jeffreys_binary,
    "pct_hesitant_vaccine_likely_govt", "b_hesitant_vaccine_likely_govt", compute_binary, jeffreys_binary,
    "pct_hesitant_vaccine_likely_politicians", "b_hesitant_vaccine_likely_politicians", compute_binary, jeffreys_binary,
    "pct_hesitant_vaccine_likely_doctors", "b_hesitant_vaccine_likely_doctors", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_sideeffects", "b_hesitant_barrier_sideeffects", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_allergic", "b_hesitant_barrier_allergic", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_ineffective", "b_hesitant_barrier_ineffective", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_dontneed", "b_hesitant_barrier_dontneed", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_dislike_vaccines", "b_hesitant_barrier_dislike_vaccines", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_not_recommended", "b_hesitant_barrier_not_recommended", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_wait_safety", "b_hesitant_barrier_wait_safety", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_low_priority", "b_hesitant_barrier_low_priority", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_cost", "b_hesitant_barrier_cost", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_distrust_vaccines", "b_hesitant_barrier_distrust_vaccines", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_distrust_govt", "b_hesitant_barrier_distrust_govt", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_religious", "b_hesitant_barrier_religious", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_health_condition", "b_hesitant_barrier_health_condition", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_pregnant", "b_hesitant_barrier_pregnant", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_other", "b_hesitant_barrier_other", compute_binary, jeffreys_binary,
    "pct_hesitant_dontneed_reason_had_covid", "b_hesitant_dontneed_reason_had_covid", compute_binary, jeffreys_binary,
    "pct_hesitant_dontneed_reason_dont_spend_time", "b_hesitant_dontneed_reason_dont_spend_time", compute_binary, jeffreys_binary,
    "pct_hesitant_dontneed_reason_not_high_risk", "b_hesitant_dontneed_reason_not_high_risk", compute_binary, jeffreys_binary,
    "pct_hesitant_dontneed_reason_precautions", "b_hesitant_dontneed_reason_precautions", compute_binary, jeffreys_binary,
    "pct_hesitant_dontneed_reason_not_serious", "b_hesitant_dontneed_reason_not_serious", compute_binary, jeffreys_binary,
    "pct_hesitant_dontneed_reason_not_beneficial", "b_hesitant_dontneed_reason_not_beneficial", compute_binary, jeffreys_binary,
    "pct_hesitant_dontneed_reason_other", "b_hesitant_dontneed_reason_other", compute_binary, jeffreys_binary,
    "pct_barrier_sideeffects", "b_barrier_sideeffects", compute_binary, jeffreys_binary,
    "pct_barrier_allergic", "b_barrier_allergic", compute_binary, jeffreys_binary,
    "pct_barrier_ineffective", "b_barrier_ineffective", compute_binary, jeffreys_binary,
    "pct_barrier_dontneed", "b_barrier_dontneed", compute_binary, jeffreys_binary,
    "pct_barrier_dislike_vaccines", "b_barrier_dislike_vaccines", compute_binary, jeffreys_binary,
    "pct_barrier_not_recommended", "b_barrier_not_recommended", compute_binary, jeffreys_binary,
    "pct_barrier_wait_safety", "b_barrier_wait_safety", compute_binary, jeffreys_binary,
    "pct_barrier_low_priority", "b_barrier_low_priority", compute_binary, jeffreys_binary,
    "pct_barrier_cost", "b_barrier_cost", compute_binary, jeffreys_binary,
    "pct_barrier_distrust_vaccines", "b_barrier_distrust_vaccines", compute_binary, jeffreys_binary,
    "pct_barrier_distrust_govt", "b_barrier_distrust_govt", compute_binary, jeffreys_binary,
    "pct_barrier_religious", "b_barrier_religious", compute_binary, jeffreys_binary,
    "pct_barrier_health_condition", "b_barrier_health_condition", compute_binary, jeffreys_binary,
    "pct_barrier_pregnant", "b_barrier_pregnant", compute_binary, jeffreys_binary,
    "pct_barrier_other", "b_barrier_other", compute_binary, jeffreys_binary,
    "pct_dontneed_reason_had_covid", "b_dontneed_reason_had_covid", compute_binary, jeffreys_binary,
    "pct_dontneed_reason_dont_spend_time", "b_dontneed_reason_dont_spend_time", compute_binary, jeffreys_binary,
    "pct_dontneed_reason_not_high_risk", "b_dontneed_reason_not_high_risk", compute_binary, jeffreys_binary,
    "pct_dontneed_reason_precautions", "b_dontneed_reason_precautions", compute_binary, jeffreys_binary,
    "pct_dontneed_reason_not_serious", "b_dontneed_reason_not_serious", compute_binary, jeffreys_binary,
    "pct_dontneed_reason_not_beneficial", "b_dontneed_reason_not_beneficial", compute_binary, jeffreys_binary,
    "pct_dontneed_reason_other", "b_dontneed_reason_other", compute_binary, jeffreys_binary,
    "pct_defno_barrier_sideeffects", "b_defno_barrier_sideeffects", compute_binary, jeffreys_binary,
    "pct_defno_barrier_allergic", "b_defno_barrier_allergic", compute_binary, jeffreys_binary,
    "pct_defno_barrier_ineffective", "b_defno_barrier_ineffective", compute_binary, jeffreys_binary,
    "pct_defno_barrier_dontneed", "b_defno_barrier_dontneed", compute_binary, jeffreys_binary,
    "pct_defno_barrier_dislike_vaccines", "b_defno_barrier_dislike_vaccines", compute_binary, jeffreys_binary,
    "pct_defno_barrier_not_recommended", "b_defno_barrier_not_recommended", compute_binary, jeffreys_binary,
    "pct_defno_barrier_wait_safety", "b_defno_barrier_wait_safety", compute_binary, jeffreys_binary,
    "pct_defno_barrier_low_priority", "b_defno_barrier_low_priority", compute_binary, jeffreys_binary,
    "pct_defno_barrier_cost", "b_defno_barrier_cost", compute_binary, jeffreys_binary,
    "pct_defno_barrier_distrust_vaccines", "b_defno_barrier_distrust_vaccines", compute_binary, jeffreys_binary,
    "pct_defno_barrier_distrust_govt", "b_defno_barrier_distrust_govt", compute_binary, jeffreys_binary,
    "pct_defno_barrier_religious", "b_defno_barrier_religious", compute_binary, jeffreys_binary,
    "pct_defno_barrier_health_condition", "b_defno_barrier_health_condition", compute_binary, jeffreys_binary,
    "pct_defno_barrier_pregnant", "b_defno_barrier_pregnant", compute_binary, jeffreys_binary,
    "pct_defno_barrier_other", "b_defno_barrier_other", compute_binary, jeffreys_binary,
    "pct_defno_dontneed_reason_had_covid", "b_defno_dontneed_reason_had_covid", compute_binary, jeffreys_binary,
    "pct_defno_dontneed_reason_dont_spend_time", "b_defno_dontneed_reason_dont_spend_time", compute_binary, jeffreys_binary,
    "pct_defno_dontneed_reason_not_high_risk", "b_defno_dontneed_reason_not_high_risk", compute_binary, jeffreys_binary,
    "pct_defno_dontneed_reason_precautions", "b_defno_dontneed_reason_precautions", compute_binary, jeffreys_binary,
    "pct_defno_dontneed_reason_not_serious", "b_defno_dontneed_reason_not_serious", compute_binary, jeffreys_binary,
    "pct_defno_dontneed_reason_not_beneficial", "b_defno_dontneed_reason_not_beneficial", compute_binary, jeffreys_binary,
    "pct_defno_dontneed_reason_other", "b_defno_dontneed_reason_other", compute_binary, jeffreys_binary,
    "pct_informed_access", "b_informed_access", compute_binary, jeffreys_binary,
    "pct_appointment_have", "b_appointment_have", compute_binary, jeffreys_binary,
    "pct_appointment_tried", "b_appointment_tried", compute_binary, jeffreys_binary
  )

  aggs <- create_aggs_product(regions, groups, indicators)
  

  ### Include handful of original public tables not already covered by set above
  common_group <- c("agefull", "gender", "race", "hispanic")
  
  ## Cut 1: side effects generally and if hesitant about getting vaccine
  cut1_aggs <- create_aggs_product(
    regions,
    list(common_group),
    filter(indicators, name %in% c("pct_worried_vaccine_sideeffects", "pct_hesitant_worried_vaccine_sideeffects"))
  )
  
  ## Cut 2: trust various institutions if hesitant about getting vaccine
  cut2_aggs <- create_aggs_product(
    regions,
    list(common_group),
    filter(indicators, startsWith(name, "pct_hesitant_vaccine_likely_"))
  )
  
  ## Cut 3: trust various institutions
  cut3_aggs <- create_aggs_product(
    regions,
    list(common_group),
    filter(indicators, startsWith(name, "pct_vaccine_likely_"))
  )
  
  ## Cuts 4, 5, 6: vaccinated and accepting generally, or if senior, or in healthcare
  cut456_groups <- list(
    c("healthcareworker", "agefull", "gender", "race", "hispanic"),
    c("age65plus", "gender", "race", "hispanic"),
    c("agefull", "gender", "race", "hispanic")
  )
  
  cut456_aggs <- create_aggs_product(
    regions,
    cut456_groups,
    filter(indicators, name %in% c("pct_vaccinated", "pct_accept_vaccine"))
  )
  
  ## Cuts 4, 5, 6: marginal
  cut456_marginal_groups <- list(
    c("healthcareworker", "agefull"),
    c("healthcareworker", "gender"),
    c("healthcareworker", "race", "hispanic"),
    c("age65plus", "race", "hispanic")
  )
  
  cut456_marginal_aggs <- create_aggs_product(
    list("state"),
    cut456_marginal_groups,
    filter(indicators, name %in% c("pct_vaccinated", "pct_accept_vaccine"))
  )
  
  ### Combine full set and additional original tables.
  aggs <- rbind(aggs, cut1_aggs, cut2_aggs, cut3_aggs, cut456_aggs, cut456_marginal_aggs)
  
  weekly_aggs <- aggs
  monthly_aggs <- aggs
  
  return(list("week"=weekly_aggs, "month"=monthly_aggs))
}


#' Create aggs from all combinations of provided input sets
#'
#' @param regions list of strings indicating geo level
#' @param groups list of character vectors indicating grouping variables
#' @param indicators tibble with `name`, `metric`, `compute_fn`, and `post_fn`
#'   columns
#'
#' @return tibble of created aggs
#'
#' @importFrom tibble tribble
create_aggs_product <- function(regions, groups, indicators) {
  aggs <- tribble(
    ~name, ~metric, ~group_by, ~compute_fn, ~post_fn,
  )
  for (region in regions) {
    for (group in groups) {
      # Grouping variables should appear in the output in alphabetical order.
      group <- sort(group)
      for (row in 1:nrow(indicators)) {
        ind <- indicators[row, ]
        agg <- tribble(
          ~name, ~metric, ~group_by, ~compute_fn, ~post_fn,
          ind$name, ind$metric, na.omit(c(region, group)), ind$compute_fn[[1]], ind$post_fn[[1]]
        )
        aggs <- rbind(aggs, agg)
      }
    }
  }
  
  return(aggs)
}
