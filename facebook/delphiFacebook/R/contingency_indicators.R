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
    # behavior
    ## Mask wearing and distancing
    "pct_wearing_mask_5d", "c_mask_often", compute_binary, jeffreys_binary,
    "pct_wearing_mask_7d", "c_mask_often_7d", compute_binary, jeffreys_binary,
    "pct_others_masked_public", "c_others_masked_public", compute_binary_response, jeffreys_binary,
    "pct_others_distanced_public", "c_others_distanced_public", compute_binary_response, jeffreys_binary,
    
    "pct_cli", "hh_p_cli", compute_household_binary, jeffreys_count,
    "pct_ili", "hh_p_ili", compute_household_binary, jeffreys_count,
    "pct_hh_cmnty_cli", "hh_community_yes", compute_binary, jeffreys_binary,
    "pct_direct_contact", "c_direct_contact", compute_binary, jeffreys_binary,
    
    # symptoms
    "pct_symp_fever", "symp_fever", compute_binary, jeffreys_binary,
    "pct_symp_cough", "symp_cough", compute_binary, jeffreys_binary,
    "pct_symp_shortness_breath", "symp_shortness_breath", compute_binary, jeffreys_binary,
    "pct_symp_diff_breathing", "symp_diff_breathing", compute_binary, jeffreys_binary,
    "pct_symp_fatigue", "symp_fatigue", compute_binary, jeffreys_binary,
    "pct_symp_nasal_congestion", "symp_nasal_congestion", compute_binary, jeffreys_binary,
    "pct_symp_runny_nose", "symp_runny_nose", compute_binary, jeffreys_binary,
    "pct_symp_aches", "symp_aches", compute_binary, jeffreys_binary,
    "pct_symp_sore_throat", "symp_sore_throat", compute_binary, jeffreys_binary,
    "pct_symp_chest_pain", "symp_chest_pain", compute_binary, jeffreys_binary,
    "pct_symp_nausea", "symp_nausea", compute_binary, jeffreys_binary,
    "pct_symp_diarrhea", "symp_diarrhea", compute_binary, jeffreys_binary,
    "pct_symp_loss_smell_taste", "symp_loss_smell_taste", compute_binary, jeffreys_binary,
    "pct_symp_other", "symp_other", compute_binary, jeffreys_binary,
    "pct_symp_none", "symp_none", compute_binary, jeffreys_binary,
    "pct_symp_eye_pain", "symp_eye_pain", compute_binary, jeffreys_binary,
    "pct_symp_chills", "symp_chills", compute_binary, jeffreys_binary,
    "pct_symp_headache", "symp_headache", compute_binary, jeffreys_binary,
    "pct_symp_sleep_changes", "symp_sleep_changes", compute_binary, jeffreys_binary,
    "pct_symp_stuffy_nose", "symp_stuffy_nose", compute_binary, jeffreys_binary,
    
    # vaccines
    "pct_vaccinated", "v_covid_vaccinated", compute_binary, jeffreys_binary,
    "pct_received_2_vaccine_doses", "v_received_2_vaccine_doses", compute_binary, jeffreys_binary,
    "pct_accept_vaccine", "v_accept_covid_vaccine", compute_binary, jeffreys_binary,
    "pct_accept_vaccine_no_appointment", "v_accept_covid_vaccine_no_appointment", compute_binary, jeffreys_binary,
    "pct_appointment_or_accept_vaccine", "v_appointment_or_accept_covid_vaccine", compute_binary, jeffreys_binary,
    "pct_hesitant_vaccine", "v_hesitant_vaccine", compute_binary, jeffreys_binary,
    "pct_vaccinated_or_accept", "v_covid_vaccinated_or_accept", compute_binary, jeffreys_binary,
    "pct_vaccinated_appointment_or_accept", "v_covid_vaccinated_appointment_or_accept", compute_binary, jeffreys_binary,
    "pct_covid_vaccinated_friends", "v_covid_vaccinated_friends", compute_binary_response, jeffreys_binary,
    
    "pct_vaccinate_children", "v_vaccinate_children", compute_binary_response, jeffreys_binary,
    
    "pct_accept_vaccine_defyes", "v_accept_vaccine_defyes", compute_binary, jeffreys_multinomial_factory(4),
    "pct_accept_vaccine_probyes", "v_accept_vaccine_probyes", compute_binary, jeffreys_multinomial_factory(4),
    "pct_accept_vaccine_probno", "v_accept_vaccine_probno", compute_binary, jeffreys_multinomial_factory(4),
    "pct_accept_vaccine_defno", "v_accept_vaccine_defno", compute_binary, jeffreys_multinomial_factory(4),
    "pct_accept_vaccine_no_appointment_defyes", "v_accept_vaccine_no_appointment_defyes", compute_binary, jeffreys_multinomial_factory(4),
    "pct_accept_vaccine_no_appointment_probyes", "v_accept_vaccine_no_appointment_probyes", compute_binary, jeffreys_multinomial_factory(4),
    "pct_accept_vaccine_no_appointment_probno", "v_accept_vaccine_no_appointment_probno", compute_binary, jeffreys_multinomial_factory(4),
    "pct_accept_vaccine_no_appointment_defno", "v_accept_vaccine_no_appointment_defno", compute_binary, jeffreys_multinomial_factory(4),
    
    # vaccine influences
    "pct_vaccine_likely_friends", "v_vaccine_likely_friends", compute_binary, jeffreys_binary,
    "pct_vaccine_likely_local_health", "v_vaccine_likely_local_health", compute_binary, jeffreys_binary,
    "pct_vaccine_likely_who", "v_vaccine_likely_who", compute_binary, jeffreys_binary,
    "pct_vaccine_likely_govt_health", "v_vaccine_likely_govt_health", compute_binary, jeffreys_binary,
    "pct_vaccine_likely_politicians", "v_vaccine_likely_politicians", compute_binary, jeffreys_binary,
    "pct_vaccine_likely_doctors", "v_vaccine_likely_doctors", compute_binary, jeffreys_binary,
    "pct_trust_covid_info_doctors", "i_trust_covid_info_doctors", compute_binary, jeffreys_binary,
    "pct_trust_covid_info_experts", "i_trust_covid_info_experts", compute_binary, jeffreys_binary,
    "pct_trust_covid_info_cdc", "i_trust_covid_info_cdc", compute_binary, jeffreys_binary,
    "pct_trust_covid_info_govt_health", "i_trust_covid_info_govt_health", compute_binary, jeffreys_binary,
    "pct_trust_covid_info_politicians", "i_trust_covid_info_politicians", compute_binary, jeffreys_binary,
    "pct_trust_covid_info_journalists", "i_trust_covid_info_journalists", compute_binary, jeffreys_binary,
    "pct_trust_covid_info_friends", "i_trust_covid_info_friends", compute_binary, jeffreys_binary,
    "pct_trust_covid_info_religious", "i_trust_covid_info_religious", compute_binary, jeffreys_binary,
    
    "pct_worried_vaccine_sideeffects", "v_worried_vaccine_side_effects", compute_binary, jeffreys_binary,
    "pct_hesitant_worried_vaccine_sideeffects", "v_hesitant_worried_vaccine_sideeffects", compute_binary, jeffreys_binary,

    "pct_hesitant_vaccine_likely_friends", "v_hesitant_vaccine_likely_friends", compute_binary, jeffreys_binary,
    "pct_hesitant_vaccine_likely_local_health", "v_hesitant_vaccine_likely_local_health", compute_binary, jeffreys_binary,
    "pct_hesitant_vaccine_likely_who", "v_hesitant_vaccine_likely_who", compute_binary, jeffreys_binary,
    "pct_hesitant_vaccine_likely_govt", "v_hesitant_vaccine_likely_govt", compute_binary, jeffreys_binary,
    "pct_hesitant_vaccine_likely_politicians", "v_hesitant_vaccine_likely_politicians", compute_binary, jeffreys_binary,
    "pct_hesitant_vaccine_likely_doctors", "v_hesitant_vaccine_likely_doctors", compute_binary, jeffreys_binary,
    "pct_hesitant_trust_covid_info_doctors", "i_hesitant_trust_covid_info_doctors", compute_binary, jeffreys_binary,
    "pct_hesitant_trust_covid_info_experts", "i_hesitant_trust_covid_info_experts", compute_binary, jeffreys_binary,
    "pct_hesitant_trust_covid_info_cdc", "i_hesitant_trust_covid_info_cdc", compute_binary, jeffreys_binary,
    "pct_hesitant_trust_covid_info_govt_health", "i_hesitant_trust_covid_info_govt_health", compute_binary, jeffreys_binary,
    "pct_hesitant_trust_covid_info_politicians", "i_hesitant_trust_covid_info_politicians", compute_binary, jeffreys_binary,
    "pct_hesitant_trust_covid_info_journalists", "i_hesitant_trust_covid_info_journalists", compute_binary, jeffreys_binary,
    "pct_hesitant_trust_covid_info_friends", "i_hesitant_trust_covid_info_friends", compute_binary, jeffreys_binary,
    "pct_hesitant_trust_covid_info_religious", "i_hesitant_trust_covid_info_religious", compute_binary, jeffreys_binary,
    
    # vaccine worries
    "pct_hesitant_barrier_sideeffects", "v_hesitant_barrier_sideeffects", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_allergic", "v_hesitant_barrier_allergic", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_ineffective", "v_hesitant_barrier_ineffective", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_dontneed", "v_hesitant_barrier_dontneed", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_dislike_vaccines", "v_hesitant_barrier_dislike_vaccines", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_not_recommended", "v_hesitant_barrier_not_recommended", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_wait_safety", "v_hesitant_barrier_wait_safety", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_low_priority", "v_hesitant_barrier_low_priority", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_cost", "v_hesitant_barrier_cost", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_distrust_vaccines", "v_hesitant_barrier_distrust_vaccines", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_distrust_govt", "v_hesitant_barrier_distrust_govt", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_religious", "v_hesitant_barrier_religious", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_health_condition", "v_hesitant_barrier_health_condition", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_pregnant", "v_hesitant_barrier_pregnant", compute_binary, jeffreys_binary,
    "pct_hesitant_barrier_other", "v_hesitant_barrier_other", compute_binary, jeffreys_binary,
    
    "pct_hesitant_dontneed_reason_had_covid", "v_hesitant_dontneed_reason_had_covid", compute_binary, jeffreys_binary,
    "pct_hesitant_dontneed_reason_dont_spend_time", "v_hesitant_dontneed_reason_dont_spend_time", compute_binary, jeffreys_binary,
    "pct_hesitant_dontneed_reason_not_high_risk", "v_hesitant_dontneed_reason_not_high_risk", compute_binary, jeffreys_binary,
    "pct_hesitant_dontneed_reason_precautions", "v_hesitant_dontneed_reason_precautions", compute_binary, jeffreys_binary,
    "pct_hesitant_dontneed_reason_not_serious", "v_hesitant_dontneed_reason_not_serious", compute_binary, jeffreys_binary,
    "pct_hesitant_dontneed_reason_not_beneficial", "v_hesitant_dontneed_reason_not_beneficial", compute_binary, jeffreys_binary,
    "pct_hesitant_dontneed_reason_other", "v_hesitant_dontneed_reason_other", compute_binary, jeffreys_binary,
    
    "pct_barrier_sideeffects", "v_hesitancy_reason_sideeffects", compute_binary, jeffreys_binary,
    "pct_barrier_allergic", "v_hesitancy_reason_allergic", compute_binary, jeffreys_binary,
    "pct_barrier_ineffective", "v_hesitancy_reason_ineffective", compute_binary, jeffreys_binary,
    "pct_barrier_dontneed", "v_hesitancy_reason_unnecessary", compute_binary, jeffreys_binary,
    "pct_barrier_dislike_vaccines", "v_hesitancy_reason_dislike_vaccines", compute_binary, jeffreys_binary,
    "pct_barrier_not_recommended", "v_hesitancy_reason_not_recommended", compute_binary, jeffreys_binary,
    "pct_barrier_wait_safety", "v_hesitancy_reason_wait_safety", compute_binary, jeffreys_binary,
    "pct_barrier_low_priority", "v_hesitancy_reason_low_priority", compute_binary, jeffreys_binary,
    "pct_barrier_cost", "v_hesitancy_reason_cost", compute_binary, jeffreys_binary,
    "pct_barrier_distrust_vaccines", "v_hesitancy_reason_distrust_vaccines", compute_binary, jeffreys_binary,
    "pct_barrier_distrust_govt", "v_hesitancy_reason_distrust_gov", compute_binary, jeffreys_binary,
    "pct_barrier_religious", "v_hesitancy_reason_religious", compute_binary, jeffreys_binary,
    "pct_barrier_health_condition", "v_hesitancy_reason_health_condition", compute_binary, jeffreys_binary,
    "pct_barrier_pregnant", "v_hesitancy_reason_pregnant", compute_binary, jeffreys_binary,
    "pct_barrier_other", "v_hesitancy_reason_other", compute_binary, jeffreys_binary,
    
    # vaccine "don't need" reasons
    "pct_dontneed_reason_had_covid", "v_dontneed_reason_had_covid_5abc_6", compute_binary, jeffreys_binary,
    "pct_dontneed_reason_dont_spend_time", "v_dontneed_reason_dont_spend_time_5abc_6", compute_binary, jeffreys_binary,
    "pct_dontneed_reason_not_high_risk", "v_dontneed_reason_not_high_risk_5abc_6", compute_binary, jeffreys_binary,
    "pct_dontneed_reason_precautions", "v_dontneed_reason_precautions_5abc_6", compute_binary, jeffreys_binary,
    "pct_dontneed_reason_not_serious", "v_dontneed_reason_not_serious_5abc_6", compute_binary, jeffreys_binary,
    "pct_dontneed_reason_not_beneficial", "v_dontneed_reason_not_beneficial_5abc_6", compute_binary, jeffreys_binary,
    "pct_dontneed_reason_other", "v_dontneed_reason_other_5abc_6", compute_binary, jeffreys_binary,
    
    "pct_defno_barrier_sideeffects", "v_defno_barrier_sideeffects", compute_binary, jeffreys_binary,
    "pct_defno_barrier_allergic", "v_defno_barrier_allergic", compute_binary, jeffreys_binary,
    "pct_defno_barrier_ineffective", "v_defno_barrier_ineffective", compute_binary, jeffreys_binary,
    "pct_defno_barrier_dontneed", "v_defno_barrier_dontneed", compute_binary, jeffreys_binary,
    "pct_defno_barrier_dislike_vaccines", "v_defno_barrier_dislike_vaccines", compute_binary, jeffreys_binary,
    "pct_defno_barrier_not_recommended", "v_defno_barrier_not_recommended", compute_binary, jeffreys_binary,
    "pct_defno_barrier_wait_safety", "v_defno_barrier_wait_safety", compute_binary, jeffreys_binary,
    "pct_defno_barrier_low_priority", "v_defno_barrier_low_priority", compute_binary, jeffreys_binary,
    "pct_defno_barrier_cost", "v_defno_barrier_cost", compute_binary, jeffreys_binary,
    "pct_defno_barrier_distrust_vaccines", "v_defno_barrier_distrust_vaccines", compute_binary, jeffreys_binary,
    "pct_defno_barrier_distrust_govt", "v_defno_barrier_distrust_govt", compute_binary, jeffreys_binary,
    "pct_defno_barrier_religious", "v_defno_barrier_religious", compute_binary, jeffreys_binary,
    "pct_defno_barrier_health_condition", "v_defno_barrier_health_condition", compute_binary, jeffreys_binary,
    "pct_defno_barrier_pregnant", "v_defno_barrier_pregnant", compute_binary, jeffreys_binary,
    "pct_defno_barrier_other", "v_defno_barrier_other", compute_binary, jeffreys_binary,
    
    "pct_defno_dontneed_reason_had_covid", "v_defno_dontneed_reason_had_covid", compute_binary, jeffreys_binary,
    "pct_defno_dontneed_reason_dont_spend_time", "v_defno_dontneed_reason_dont_spend_time", compute_binary, jeffreys_binary,
    "pct_defno_dontneed_reason_not_high_risk", "v_defno_dontneed_reason_not_high_risk", compute_binary, jeffreys_binary,
    "pct_defno_dontneed_reason_precautions", "v_defno_dontneed_reason_precautions", compute_binary, jeffreys_binary,
    "pct_defno_dontneed_reason_not_serious", "v_defno_dontneed_reason_not_serious", compute_binary, jeffreys_binary,
    "pct_defno_dontneed_reason_not_beneficial", "v_defno_dontneed_reason_not_beneficial", compute_binary, jeffreys_binary,
    "pct_defno_dontneed_reason_other", "v_defno_dontneed_reason_other", compute_binary, jeffreys_binary,
    
    "pct_informed_access", "v_informed_access", compute_binary, jeffreys_binary,
    
    # appointments
    "pct_appointment_have", "v_appointment_have", compute_binary, jeffreys_binary,
    "pct_appointment_not_vaccinated", "v_appointment_not_vaccinated", compute_binary, jeffreys_binary,
    "pct_appointment_tried", "v_appointment_tried", compute_binary, jeffreys_binary,
    "pct_vaccine_tried", "v_vaccine_tried", compute_binary, jeffreys_binary,
    
    "pct_had_covid_ever", "t_had_covid_ever", compute_binary, jeffreys_binary,
    
    # vaccine barriers
    "pct_vaccine_barrier_eligible", "v_vaccine_barrier_eligible", compute_binary_response, jeffreys_binary,
    "pct_vaccine_barrier_no_appointments", "v_vaccine_barrier_no_appointments", compute_binary_response, jeffreys_binary,
    "pct_vaccine_barrier_appointment_time", "v_vaccine_barrier_appointment_time", compute_binary_response, jeffreys_binary,
    "pct_vaccine_barrier_technical_difficulties", "v_vaccine_barrier_technical_difficulties", compute_binary_response, jeffreys_binary,
    "pct_vaccine_barrier_document", "v_vaccine_barrier_document", compute_binary_response, jeffreys_binary,
    "pct_vaccine_barrier_technology_access", "v_vaccine_barrier_technology_access", compute_binary_response, jeffreys_binary,
    "pct_vaccine_barrier_travel", "v_vaccine_barrier_travel", compute_binary_response, jeffreys_binary,
    "pct_vaccine_barrier_language", "v_vaccine_barrier_language", compute_binary_response, jeffreys_binary,
    "pct_vaccine_barrier_childcare", "v_vaccine_barrier_childcare", compute_binary_response, jeffreys_binary,
    "pct_vaccine_barrier_time", "v_vaccine_barrier_time", compute_binary_response, jeffreys_binary,
    "pct_vaccine_barrier_type", "v_vaccine_barrier_type", compute_binary_response, jeffreys_binary,
    "pct_vaccine_barrier_none", "v_vaccine_barrier_none", compute_binary_response, jeffreys_binary,
    
    # beliefs
    "pct_belief_masking_effective", "b_belief_masking_effective", compute_binary_response, jeffreys_binary,
    "pct_belief_distancing_effective", "b_belief_distancing_effective", compute_binary_response, jeffreys_binary,
    "pct_belief_vaccinated_mask_unnecessary", "b_belief_vaccinated_mask_unnecessary", compute_binary_response, jeffreys_binary,
    "pct_belief_children_immune", "b_belief_children_immune", compute_binary_response, jeffreys_binary,
    "pct_belief_created_small_group", "b_belief_created_small_group", compute_binary_response, jeffreys_binary,
    "pct_belief_govt_exploitation", "b_belief_govt_exploitation", compute_binary_response, jeffreys_binary,
    
    # medical care beliefs and experiences
    "pct_race_treated_fairly_healthcare", "b_race_treated_fairly_healthcare", compute_binary_response, jeffreys_binary,
    "pct_delayed_care_cost", "b_delayed_care_cost", compute_binary_response, jeffreys_binary,
    
    "pct_worried_catch_covid", "mh_worried_catch_covid", compute_binary_response, jeffreys_binary,
    
    # topics want to learn about
    "pct_want_info_covid_treatment", "i_want_info_covid_treatment", compute_binary_response, jeffreys_binary,
    "pct_want_info_vaccine_access", "i_want_info_vaccine_access", compute_binary_response, jeffreys_binary,
    "pct_want_info_vaccine_types", "i_want_info_vaccine_types", compute_binary_response, jeffreys_binary,
    "pct_want_info_covid_variants", "i_want_info_covid_variants", compute_binary_response, jeffreys_binary,
    "pct_want_info_children_education", "i_want_info_children_education", compute_binary_response, jeffreys_binary,
    "pct_want_info_mental_health", "i_want_info_mental_health", compute_binary_response, jeffreys_binary,
    "pct_want_info_relationships", "i_want_info_relationships", compute_binary_response, jeffreys_binary,
    "pct_want_info_employment", "i_want_info_employment", compute_binary_response, jeffreys_binary,
    "pct_want_info_none", "i_want_info_none", compute_binary_response, jeffreys_binary,
    
    # news
    "pct_received_news_local_health", "i_received_news_local_health", compute_binary_response, jeffreys_binary,
    "pct_received_news_experts", "i_received_news_experts", compute_binary_response, jeffreys_binary,
    "pct_received_news_cdc", "i_received_news_cdc", compute_binary_response, jeffreys_binary,
    "pct_received_news_govt_health", "i_received_news_govt_health", compute_binary_response, jeffreys_binary,
    "pct_received_news_politicians", "i_received_news_politicians", compute_binary_response, jeffreys_binary,
    "pct_received_news_journalists", "i_received_news_journalists", compute_binary_response, jeffreys_binary,
    "pct_received_news_friends", "i_received_news_friends", compute_binary_response, jeffreys_binary,
    "pct_received_news_religious", "i_received_news_religious", compute_binary_response, jeffreys_binary,
    "pct_received_news_none", "i_received_news_none", compute_binary_response, jeffreys_binary,
    
    # testing
    "pct_tested_14d", "t_tested_14d", compute_binary_response, jeffreys_binary,
    "pct_tested_positive_14d", "t_tested_positive_14d", compute_binary_response, jeffreys_binary,
    "pct_wanted_test_14d", "t_wanted_test_14d", compute_binary_response, jeffreys_binary,

    # mental health
    "pct_worried_become_ill", "mh_worried_ill", compute_binary_response, jeffreys_binary,
    # pre-wave 10
    "pct_anxious_5d", "mh_anxious", compute_binary_response, jeffreys_binary,
    "pct_depressed_5d", "mh_depressed", compute_binary_response, jeffreys_binary,
    "pct_felt_isolated_5d", "mh_isolated", compute_binary_response, jeffreys_binary,
    # wave 10+
    "pct_anxious_7d", "mh_anxious_7d", compute_binary_response, jeffreys_binary,
    "pct_depressed_7d", "mh_depressed_7d", compute_binary_response, jeffreys_binary,
    "pct_felt_isolated_7d", "mh_isolated_7d", compute_binary_response, jeffreys_binary,
    
    # travel outside state
    # pre-wave 10
    "pct_travel_outside_state_5d", "c_travel_state", compute_binary_response, jeffreys_binary,
    # wave 10+
    "pct_travel_outside_state_7d", "c_travel_state_7d", compute_binary_response, jeffreys_binary
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
    filter(indicators, name %in% c("pct_vaccinated", "pct_accept_vaccine", "pct_appointment_or_accept_vaccine", "pct_accept_vaccine_no_appointment"))
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
    filter(indicators, name %in% c("pct_vaccinated", "pct_accept_vaccine", "pct_appointment_or_accept_vaccine", "pct_accept_vaccine_no_appointment"))
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
