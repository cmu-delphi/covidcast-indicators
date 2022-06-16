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
    c("race", "hispanic", "pregnant"),
    c("vaccinationstatus")
  )

  indicators <- tribble(
    ~name, ~metric, ~compute_fn, ~post_fn,
    # behavior
    ## Mask wearing and distancing
    "pct_wearing_mask_5d", "c_mask_often", compute_binary, jeffreys_binary,
    "pct_wearing_mask_5d_alt", "c_mask_some_often", compute_binary, jeffreys_binary,
    "pct_wearing_mask_7d", "c_mask_often_7d", compute_binary, jeffreys_binary,
    "pct_wearing_mask_7d_alt", "c_mask_some_often_7d", compute_binary, jeffreys_binary,

    "pct_others_masked", "c_others_masked", compute_binary, jeffreys_binary,
    "pct_others_masked_alt", "c_others_some_masked", compute_binary, jeffreys_binary,

    "pct_others_masked_public", "c_others_masked_public", compute_binary, jeffreys_binary,
    "pct_others_masked_public_alt", "c_others_masked_some_public", compute_binary, jeffreys_binary,

    "pct_others_distanced_public", "c_others_distanced_public", compute_binary, jeffreys_binary,
    "pct_others_distanced_public_alt", "c_others_distanced_some_public", compute_binary, jeffreys_binary,
    
    "pct_cli", "hh_p_cli", compute_household_binary, jeffreys_count,
    "pct_ili", "hh_p_ili", compute_household_binary, jeffreys_count,
    "pct_hh_cmnty_cli", "hh_community_yes", compute_binary, jeffreys_binary,
    "pct_nohh_cmnty_cli", "community_yes", compute_binary, jeffreys_binary,

    "pct_direct_contact", "c_direct_contact", compute_binary, jeffreys_binary,
    "pct_direct_contact_covid", "c_direct_contact_covid", compute_binary, jeffreys_binary,
    "pct_direct_contact_covid_hh", "c_direct_contact_covid_hh", compute_binary, jeffreys_binary,

    "pct_avoid_contact", "c_avoid_contact", compute_binary, jeffreys_binary,
    "pct_avoid_contact_7d", "c_avoid_contact_7d", compute_binary, jeffreys_binary,
    
    # symptoms
    "pct_taken_temp", "t_taken_temp", compute_binary, jeffreys_binary,

    "pct_cough_mucus", "symp_cough_mucus", compute_binary, jeffreys_binary,

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
    "pct_anosmia", "symp_loss_smell_taste", compute_binary, jeffreys_binary,
    "pct_symp_other", "symp_other", compute_binary, jeffreys_binary,
    "pct_symp_none", "symp_none", compute_binary, jeffreys_binary,
    "pct_symp_eye_pain", "symp_eye_pain", compute_binary, jeffreys_binary,
    "pct_symp_chills", "symp_chills", compute_binary, jeffreys_binary,
    "pct_symp_headache", "symp_headache", compute_binary, jeffreys_binary,
    "pct_symp_sleep_changes", "symp_sleep_changes", compute_binary, jeffreys_binary,
    "pct_symp_stuffy_nose", "symp_stuffy_nose", compute_binary, jeffreys_binary,
    
    # unusual symptoms
    "pct_symp_fever_unusual", "symp_fever_unusual", compute_binary, jeffreys_binary,
    "pct_symp_cough_unusual", "symp_cough_unusual", compute_binary, jeffreys_binary,
    "pct_symp_shortness_breath_unusual", "symp_shortness_breath_unusual", compute_binary, jeffreys_binary,
    "pct_symp_diff_breathing_unusual", "symp_diff_breathing_unusual", compute_binary, jeffreys_binary,
    "pct_symp_fatigue_unusual", "symp_fatigue_unusual", compute_binary, jeffreys_binary,
    "pct_symp_nasal_congestion_unusual", "symp_nasal_congestion_unusual", compute_binary, jeffreys_binary,
    "pct_symp_runny_nose_unusual", "symp_runny_nose_unusual", compute_binary, jeffreys_binary,
    "pct_symp_aches_unusual", "symp_aches_unusual", compute_binary, jeffreys_binary,
    "pct_symp_sore_throat_unusual", "symp_sore_throat_unusual", compute_binary, jeffreys_binary,
    "pct_symp_chest_pain_unusual", "symp_chest_pain_unusual", compute_binary, jeffreys_binary,
    "pct_symp_nausea_unusual", "symp_nausea_unusual", compute_binary, jeffreys_binary,
    "pct_symp_diarrhea_unusual", "symp_diarrhea_unusual", compute_binary, jeffreys_binary,
    "pct_anosmia_unusual", "symp_loss_smell_taste_unusual", compute_binary, jeffreys_binary,
    "pct_symp_eye_pain_unusual", "symp_eye_pain_unusual", compute_binary, jeffreys_binary,
    "pct_symp_chills_unusual", "symp_chills_unusual", compute_binary, jeffreys_binary,
    "pct_symp_headache_unusual", "symp_headache_unusual", compute_binary, jeffreys_binary,
    "pct_symp_sleep_changes_unusual", "symp_sleep_changes_unusual", compute_binary, jeffreys_binary,
    "pct_symp_stuffy_nose_unusual", "symp_stuffy_nose_unusual", compute_binary, jeffreys_binary,
    
    # vaccines
    "pct_vaccinated", "v_covid_vaccinated", compute_binary, jeffreys_binary,
    "pct_received_2_vaccine_doses", "v_received_2_vaccine_doses", compute_binary, jeffreys_binary,
    "pct_accept_vaccine", "v_accept_covid_vaccine", compute_binary, jeffreys_binary,
    "pct_accept_vaccine_no_appointment", "v_accept_covid_vaccine_no_appointment", compute_binary, jeffreys_binary,
    "pct_appointment_or_accept_vaccine", "v_appointment_or_accept_covid_vaccine", compute_binary, jeffreys_binary,
    "pct_hesitant_vaccine", "v_hesitant_vaccine", compute_binary, jeffreys_binary,
    "pct_overall_vaccine_hesitancy", "overall_vaccine_hesitancy", compute_binary, jeffreys_binary,
    "pct_vaccinated_or_accept", "v_covid_vaccinated_or_accept", compute_binary, jeffreys_binary,
    "pct_vaccinated_appointment_or_accept", "v_covid_vaccinated_appointment_or_accept", compute_binary, jeffreys_binary,

    "pct_covid_vaccinated_friends", "v_covid_vaccinated_friends", compute_binary, jeffreys_binary,
    "pct_covid_vaccinated_friends_alt", "v_covid_vaccinated_some_friends", compute_binary, jeffreys_binary,
    
    "pct_vaccinate_children", "v_vaccinate_children", compute_binary, jeffreys_binary,
    "pct_vaccinate_child_oldest", "v_vaccinate_child_oldest", compute_binary, jeffreys_binary,
    
    "pct_child_vaccine_already", "v_child_vaccine_already", compute_binary, jeffreys_multinomial_factory(5),
    "pct_child_vaccine_yes_def", "v_child_vaccine_yes_def", compute_binary, jeffreys_multinomial_factory(5),
    "pct_child_vaccine_yes_prob", "v_child_vaccine_yes_prob", compute_binary, jeffreys_multinomial_factory(5),
    "pct_child_vaccine_no_prob", "v_child_vaccine_no_prob", compute_binary, jeffreys_multinomial_factory(5),
    "pct_child_vaccine_no_def", "v_child_vaccine_no_def", compute_binary, jeffreys_multinomial_factory(5),

    "pct_accept_vaccine_defyes", "v_accept_vaccine_defyes", compute_binary, jeffreys_multinomial_factory(4),
    "pct_accept_vaccine_probyes", "v_accept_vaccine_probyes", compute_binary, jeffreys_multinomial_factory(4),
    "pct_accept_vaccine_probno", "v_accept_vaccine_probno", compute_binary, jeffreys_multinomial_factory(4),
    "pct_accept_vaccine_defno", "v_accept_vaccine_defno", compute_binary, jeffreys_multinomial_factory(4),
    "pct_accept_vaccine_no_appointment_defyes", "v_accept_vaccine_no_appointment_defyes", compute_binary, jeffreys_multinomial_factory(4),
    "pct_accept_vaccine_no_appointment_probyes", "v_accept_vaccine_no_appointment_probyes", compute_binary, jeffreys_multinomial_factory(4),
    "pct_accept_vaccine_no_appointment_probno", "v_accept_vaccine_no_appointment_probno", compute_binary, jeffreys_multinomial_factory(4),
    "pct_accept_vaccine_no_appointment_defno", "v_accept_vaccine_no_appointment_defno", compute_binary, jeffreys_multinomial_factory(4),
    
    "pct_initial_dose_one_of_one", "v_initial_dose_one_of_one", compute_binary, jeffreys_multinomial_factory(4),
    "pct_initial_dose_one_of_two", "v_initial_dose_one_of_two", compute_binary, jeffreys_multinomial_factory(4),
    "pct_initial_dose_two_of_two", "v_initial_dose_two_of_two", compute_binary, jeffreys_multinomial_factory(4),
    
    "pct_vaccinated_one_booster", "v_vaccinated_one_booster", compute_binary, jeffreys_multinomial_factory(4),
    "pct_vaccinated_two_or_more_boosters", "v_vaccinated_two_or_more_boosters", compute_binary, jeffreys_multinomial_factory(4),
    "pct_vaccinated_no_booster", "v_vaccinated_no_booster", compute_binary, jeffreys_multinomial_factory(4),
    "pct_vaccinated_at_least_one_booster", "v_vaccinated_at_least_one_booster", compute_binary, jeffreys_binary,
    
    "pct_vaccinated_booster_accept", "v_vaccinated_booster_accept", compute_binary, jeffreys_binary,
    "pct_vaccinated_booster_hesitant", "v_vaccinated_booster_hesitant", compute_binary, jeffreys_binary,
    
    "pct_vaccinated_booster_defyes", "v_vaccinated_booster_defyes", compute_binary, jeffreys_multinomial_factory(4),
    "pct_vaccinated_booster_probyes", "v_vaccinated_booster_probyes", compute_binary, jeffreys_multinomial_factory(4),
    "pct_vaccinated_booster_probno", "v_vaccinated_booster_probno", compute_binary, jeffreys_multinomial_factory(4),
    "pct_vaccinated_booster_defno", "v_vaccinated_booster_defno", compute_binary, jeffreys_multinomial_factory(4),

    "pct_vaccine_all_doses", "v_received_all_doses", compute_binary, jeffreys_binary,

    "pct_flu_shot_1y", "v_flu_vaccinated_1y", compute_binary, jeffreys_binary,
    "pct_flu_vaccine_june_2020", "v_flu_vaccinated_june_2020", compute_binary, jeffreys_binary,
    "pct_flu_vaccine_july_2020", "v_flu_vaccinated_july_2020", compute_binary, jeffreys_binary,
    "pct_flu_vaccinated_2021", "v_flu_vaccinated_2021", compute_binary, jeffreys_binary,
    
    
    # vaccine timing
    "pct_vaccine_timing_weeks", "v_vaccine_timing_weeks", compute_binary, jeffreys_multinomial_factory(7),
    "pct_vaccine_timing_onemonth", "v_vaccine_timing_onemonth", compute_binary, jeffreys_multinomial_factory(7),
    "pct_vaccine_timing_threemonths", "v_vaccine_timing_threemonths", compute_binary, jeffreys_multinomial_factory(7),
    "pct_vaccine_timing_sixmonths", "v_vaccine_timing_sixmonths", compute_binary, jeffreys_multinomial_factory(7),
    "pct_vaccine_timing_morethansix", "v_vaccine_timing_morethansix", compute_binary, jeffreys_multinomial_factory(7),
    "pct_vaccine_timing_dontknow", "v_vaccine_timing_dontknow", compute_binary, jeffreys_multinomial_factory(7),
    
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
    "pct_hesitant_barrier_dislike_vaccines_generally", "v_hesitant_barrier_dislike_vaccines_generally", compute_binary, jeffreys_binary,
    
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
    "pct_barrier_dislike_vaccines_generally", "v_hesitancy_reason_dislike_vaccines_generally", compute_binary, jeffreys_binary,
    
    # vaccine incomplete reasons
    "pct_vaccine_incomplete_sideeffect", "v_vaccine_incomplete_sideeffect", compute_binary, jeffreys_binary,
    "pct_vaccine_incomplete_allergic", "v_vaccine_incomplete_allergic", compute_binary, jeffreys_binary,
    "pct_vaccine_incomplete_wontwork", "v_vaccine_incomplete_wontwork", compute_binary, jeffreys_binary,
    "pct_vaccine_incomplete_dontbelieve", "v_vaccine_incomplete_dontbelieve", compute_binary, jeffreys_binary,
    "pct_vaccine_incomplete_dontlike", "v_vaccine_incomplete_dontlike", compute_binary, jeffreys_binary,
    "pct_vaccine_incomplete_not_recommended", "v_vaccine_incomplete_not_recommended", compute_binary, jeffreys_binary,
    "pct_vaccine_incomplete_wait", "v_vaccine_incomplete_wait", compute_binary, jeffreys_binary,
    "pct_vaccine_incomplete_otherpeople", "v_vaccine_incomplete_otherpeople", compute_binary, jeffreys_binary,
    "pct_vaccine_incomplete_cost", "v_vaccine_incomplete_cost", compute_binary, jeffreys_binary,
    "pct_vaccine_incomplete_distrust_vaccine", "v_vaccine_incomplete_distrust_vaccine", compute_binary, jeffreys_binary,
    "pct_vaccine_incomplete_distrust_gov", "v_vaccine_incomplete_distrust_gov", compute_binary, jeffreys_binary,
    "pct_vaccine_incomplete_health", "v_vaccine_incomplete_health", compute_binary, jeffreys_binary,
    "pct_vaccine_incomplete_other", "v_vaccine_incomplete_other", compute_binary, jeffreys_binary,
    "pct_vaccine_incomplete_pregnant", "v_vaccine_incomplete_pregnant", compute_binary, jeffreys_binary,
    "pct_vaccine_incomplete_religion", "v_vaccine_incomplete_religion", compute_binary, jeffreys_binary,

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
    "pct_defno_barrier_dislike_vaccines_generally", "v_defno_barrier_dislike_vaccines_generally", compute_binary, jeffreys_binary,
    
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
    "pct_vaccine_barrier_eligible", "v_vaccine_barrier_eligible", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_no_appointments", "v_vaccine_barrier_no_appointments", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_appointment_time", "v_vaccine_barrier_appointment_time", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_technical_difficulties", "v_vaccine_barrier_technical_difficulties", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_document", "v_vaccine_barrier_document", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_technology_access", "v_vaccine_barrier_technology_access", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_travel", "v_vaccine_barrier_travel", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_language", "v_vaccine_barrier_language", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_childcare", "v_vaccine_barrier_childcare", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_time", "v_vaccine_barrier_time", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_type", "v_vaccine_barrier_type", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_none", "v_vaccine_barrier_none", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_appointment_location", "v_vaccine_barrier_appointment_location", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_other", "v_vaccine_barrier_other", compute_binary, jeffreys_binary,
    
    # beliefs
    "pct_belief_masking_effective", "b_belief_masking_effective", compute_binary, jeffreys_binary,
    "pct_belief_distancing_effective", "b_belief_distancing_effective", compute_binary, jeffreys_binary,
    "pct_belief_vaccinated_mask_unnecessary", "b_belief_vaccinated_mask_unnecessary", compute_binary, jeffreys_binary,
    "pct_belief_children_immune", "b_belief_children_immune", compute_binary, jeffreys_binary,
    "pct_belief_created_small_group", "b_belief_created_small_group", compute_binary, jeffreys_binary,
    "pct_belief_govt_exploitation", "b_belief_govt_exploitation", compute_binary, jeffreys_binary,
    
    # medical care beliefs and experiences
    "pct_race_treated_fairly_healthcare", "b_race_treated_fairly_healthcare", compute_binary, jeffreys_binary,
    "pct_delayed_care_cost", "b_delayed_care_cost", compute_binary, jeffreys_binary,
    
    # topics want to learn about
    "pct_want_info_covid_treatment", "i_want_info_covid_treatment", compute_binary, jeffreys_binary,
    "pct_want_info_vaccine_access", "i_want_info_vaccine_access", compute_binary, jeffreys_binary,
    "pct_want_info_vaccine_types", "i_want_info_vaccine_types", compute_binary, jeffreys_binary,
    "pct_want_info_covid_variants", "i_want_info_covid_variants", compute_binary, jeffreys_binary,
    "pct_want_info_children_education", "i_want_info_children_education", compute_binary, jeffreys_binary,
    "pct_want_info_mental_health", "i_want_info_mental_health", compute_binary, jeffreys_binary,
    "pct_want_info_relationships", "i_want_info_relationships", compute_binary, jeffreys_binary,
    "pct_want_info_employment", "i_want_info_employment", compute_binary, jeffreys_binary,
    "pct_want_info_none", "i_want_info_none", compute_binary, jeffreys_binary,
    
    # news
    "pct_received_news_local_health", "i_received_news_local_health", compute_binary, jeffreys_binary,
    "pct_received_news_experts", "i_received_news_experts", compute_binary, jeffreys_binary,
    "pct_received_news_cdc", "i_received_news_cdc", compute_binary, jeffreys_binary,
    "pct_received_news_govt_health", "i_received_news_govt_health", compute_binary, jeffreys_binary,
    "pct_received_news_politicians", "i_received_news_politicians", compute_binary, jeffreys_binary,
    "pct_received_news_journalists", "i_received_news_journalists", compute_binary, jeffreys_binary,
    "pct_received_news_friends", "i_received_news_friends", compute_binary, jeffreys_binary,
    "pct_received_news_religious", "i_received_news_religious", compute_binary, jeffreys_binary,
    "pct_received_news_none", "i_received_news_none", compute_binary, jeffreys_binary,
    
    # testing
    "pct_tested_14d", "t_tested_14d", compute_binary, jeffreys_binary,
    "pct_tested_positive_14d", "t_tested_positive_14d", compute_binary, jeffreys_binary,
    "pct_wanted_test_14d", "t_wanted_test_14d", compute_binary, jeffreys_binary,
    "pct_ever_tested", "t_ever_tested", compute_binary, jeffreys_binary,

    "pct_unusual_symptom_tested", "t_unusual_symptom_tested", compute_binary, jeffreys_binary,
    "pct_unusual_symptom_tested_positive", "t_unusual_symptom_tested_positive", compute_binary, jeffreys_binary,
    "pct_unusual_symptom_hospital", "t_unusual_symptom_hospital", compute_binary, jeffreys_binary,

    "pct_unusual_symptom_medical_care_called_doctor", "unusual_symptom_medical_care_called_doctor", compute_binary, jeffreys_binary,
    "pct_unusual_symptom_medical_care_telemedicine", "unusual_symptom_medical_care_telemedicine", compute_binary, jeffreys_binary,
    "pct_unusual_symptom_medical_care_visited_doctor", "unusual_symptom_medical_care_visited_doctor", compute_binary, jeffreys_binary,
    "pct_unusual_symptom_medical_care_urgent_care", "unusual_symptom_medical_care_urgent_care", compute_binary, jeffreys_binary,
    "pct_unusual_symptom_medical_care_er", "unusual_symptom_medical_care_er", compute_binary, jeffreys_binary,
    "pct_unusual_symptom_medical_care_hospital", "unusual_symptom_medical_care_hospital", compute_binary, jeffreys_binary,
    "pct_unusual_symptom_medical_care_tried", "unusual_symptom_medical_care_tried", compute_binary, jeffreys_binary,

    "pct_reason_not_tested_tried", "t_reason_not_tested_tried", compute_binary, jeffreys_binary,
    "pct_reason_not_tested_appointment", "t_reason_not_tested_appointment", compute_binary, jeffreys_binary,
    "pct_reason_not_tested_location", "t_reason_not_tested_location", compute_binary, jeffreys_binary,
    "pct_reason_not_tested_cost", "t_reason_not_tested_cost", compute_binary, jeffreys_binary,
    "pct_reason_not_tested_time", "t_reason_not_tested_time", compute_binary, jeffreys_binary,
    "pct_reason_not_tested_travel", "t_reason_not_tested_travel", compute_binary, jeffreys_binary,
    "pct_reason_not_tested_stigma", "t_reason_not_tested_stigma", compute_binary, jeffreys_binary,
    "pct_reason_not_tested_none", "t_reason_not_tested_none", compute_binary, jeffreys_binary,
    
    ## testing reasons
    "pct_test_reason_sick", "t_tested_reason_sick", compute_binary, jeffreys_binary,
    "pct_test_reason_contact", "t_tested_reason_contact", compute_binary, jeffreys_binary,
    "pct_test_reason_medical", "t_tested_reason_medical", compute_binary, jeffreys_binary,
    "pct_test_reason_required", "t_tested_reason_employer", compute_binary, jeffreys_binary,
    "pct_test_reason_large_event", "t_tested_reason_large_event", compute_binary, jeffreys_binary,
    "pct_test_reason_crowd", "t_tested_reason_crowd", compute_binary, jeffreys_binary,
    "pct_test_reason_visit", "t_tested_reason_visit", compute_binary, jeffreys_binary,
    "pct_test_reason_none", "t_tested_reason_other", compute_binary, jeffreys_binary,
    "pct_test_reason_travel", "t_tested_reason_travel", compute_binary, jeffreys_binary,

    # mental health
    "pct_financial_threat", "mh_financial_threat", compute_binary, jeffreys_binary,

    "pct_worried_become_ill", "mh_worried_ill", compute_binary, jeffreys_binary,
    "pct_worried_catch_covid", "mh_worried_catch_covid", compute_binary, jeffreys_binary,

    "pct_finance", "mh_worried_finances", compute_binary, jeffreys_binary,
    "pct_finance_alt", "mh_very_worried_finances", compute_binary, jeffreys_binary,

    "pct_anxious_5d", "mh_anxious", compute_binary, jeffreys_binary,
    "pct_anxious_5d_alt", "mh_some_anxious", compute_binary, jeffreys_binary,
    "pct_anxious_7d", "mh_anxious_7d", compute_binary, jeffreys_binary,
    "pct_anxious_7d_alt", "mh_some_anxious_7d", compute_binary, jeffreys_binary,

    "pct_depressed_5d", "mh_depressed", compute_binary, jeffreys_binary,
    "pct_depressed_5d_alt", "mh_some_depressed", compute_binary, jeffreys_binary,
    "pct_depressed_7d", "mh_depressed_7d", compute_binary, jeffreys_binary,
    "pct_depressed_7d_alt", "mh_some_depressed_7d", compute_binary, jeffreys_binary,

    "pct_felt_isolated_5d", "mh_isolated", compute_binary, jeffreys_binary,
    "pct_isolated_5d_alt", "mh_some_isolated", compute_binary, jeffreys_binary,
    "pct_felt_isolated_7d", "mh_isolated_7d", compute_binary, jeffreys_binary,
    "pct_isolated_7d_alt", "mh_some_isolated_7d", compute_binary, jeffreys_binary,
    
    # travel outside state
    # pre-wave 10
    "pct_travel_outside_state_5d", "c_travel_state", compute_binary, jeffreys_binary,
    # wave 10+
    "pct_travel_outside_state_7d", "c_travel_state_7d", compute_binary, jeffreys_binary,
    
    # activities outside the home
    "pct_work_outside_home_5d", "c_work_outside_5d", compute_binary, jeffreys_binary,
    "pct_work_outside_home_1d", "a_work_outside_home_1d", compute_binary, jeffreys_binary,
    "pct_work_outside_home_4w", "a_work_outside_home_4w", compute_binary, jeffreys_binary,
    "pct_work_outside_home_indoors_1d", "a_work_outside_home_indoors_1d", compute_binary, jeffreys_binary,

    "pct_work_for_pay_outside_home_4w", "a_work_for_pay_outside_home_4w", compute_binary, jeffreys_binary,

    "pct_shop_1d", "a_shop_1d", compute_binary, jeffreys_binary,
    "pct_restaurant_1d", "a_restaurant_1d", compute_binary, jeffreys_binary,
    "pct_spent_time_1d", "a_spent_time_1d", compute_binary, jeffreys_binary,
    "pct_large_event_1d", "a_large_event_1d", compute_binary, jeffreys_binary,
    "pct_public_transit_1d", "a_public_transit_1d", compute_binary, jeffreys_binary,

    "pct_shop_indoors_1d", "a_shop_indoors_1d", compute_binary, jeffreys_binary,
    "pct_restaurant_indoors_1d", "a_restaurant_indoors_1d", compute_binary, jeffreys_binary,
    "pct_spent_time_indoors_1d", "a_spent_time_indoors_1d", compute_binary, jeffreys_binary,
    "pct_large_event_indoors_1d", "a_large_event_indoors_1d", compute_binary, jeffreys_binary,

    "pct_work_healthcare_5d", "a_work_healthcare_5d", compute_binary, jeffreys_binary,
    "pct_work_nursing_home_5d", "a_work_nursing_home_5d", compute_binary, jeffreys_binary,
    
    # masked activities outside the home
    "pct_mask_work_outside_home_1d", "a_mask_work_outside_home_1d", compute_binary, jeffreys_binary,
    "pct_mask_shop_1d", "a_mask_shop_1d", compute_binary, jeffreys_binary,
    "pct_mask_restaurant_1d", "a_mask_restaurant_1d", compute_binary, jeffreys_binary,
    "pct_mask_spent_time_1d", "a_mask_spent_time_1d", compute_binary, jeffreys_binary,
    "pct_mask_large_event_1d", "a_mask_large_event_1d", compute_binary, jeffreys_binary,
    "pct_mask_public_transit_1d", "a_mask_public_transit_1d", compute_binary, jeffreys_binary,
    
    "pct_mask_work_outside_home_indoors_1d", "a_mask_work_outside_home_indoors_1d", compute_binary, jeffreys_binary,
    "pct_mask_shop_indoors_1d", "a_mask_shop_indoors_1d", compute_binary, jeffreys_binary,
    "pct_mask_restaurant_indoors_1d", "a_mask_restaurant_indoors_1d", compute_binary, jeffreys_binary,
    "pct_mask_spent_time_indoors_1d", "a_mask_spent_time_indoors_1d", compute_binary, jeffreys_binary,
    "pct_mask_large_event_indoors_1d", "a_mask_large_event_indoors_1d", compute_binary, jeffreys_binary,

    # vaccine barriers for vaccinated
    "pct_vaccine_barrier_eligible_has", "v_vaccine_barrier_eligible_has", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_no_appointments_has", "v_vaccine_barrier_no_appointments_has", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_appointment_time_has", "v_vaccine_barrier_appointment_time_has", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_technical_difficulties_has", "v_vaccine_barrier_technical_difficulties_has", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_document_has", "v_vaccine_barrier_document_has", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_technology_access_has", "v_vaccine_barrier_technology_access_has", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_travel_has", "v_vaccine_barrier_travel_has", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_language_has", "v_vaccine_barrier_language_has", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_childcare_has", "v_vaccine_barrier_childcare_has", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_time_has", "v_vaccine_barrier_time_has", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_type_has", "v_vaccine_barrier_type_has", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_none_has", "v_vaccine_barrier_none_has", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_appointment_location_has", "v_vaccine_barrier_appointment_location_has", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_other_has", "v_vaccine_barrier_other_has", compute_binary, jeffreys_binary,
    
    # vaccine barriers for tried vaccinated
    "pct_vaccine_barrier_eligible_tried", "v_vaccine_barrier_eligible_tried", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_no_appointments_tried", "v_vaccine_barrier_no_appointments_tried", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_appointment_time_tried", "v_vaccine_barrier_appointment_time_tried", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_technical_difficulties_tried", "v_vaccine_barrier_technical_difficulties_tried", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_document_tried", "v_vaccine_barrier_document_tried", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_technology_access_tried", "v_vaccine_barrier_technology_access_tried", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_travel_tried", "v_vaccine_barrier_travel_tried", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_language_tried", "v_vaccine_barrier_language_tried", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_childcare_tried", "v_vaccine_barrier_childcare_tried", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_time_tried", "v_vaccine_barrier_time_tried", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_type_tried", "v_vaccine_barrier_type_tried", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_none_tried", "v_vaccine_barrier_none_tried", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_appointment_location_tried", "v_vaccine_barrier_appointment_location_tried", compute_binary, jeffreys_binary,
    "pct_vaccine_barrier_other_tried", "v_vaccine_barrier_other_tried", compute_binary, jeffreys_binary,

    # schooling
    "pct_inperson_school_fulltime", "s_inperson_school_fulltime", compute_binary, jeffreys_binary,
    "pct_inperson_school_parttime", "s_inperson_school_parttime", compute_binary, jeffreys_binary,

    "pct_remote_school_fulltime_oldest", "s_remote_school_fulltime_oldest", compute_binary, jeffreys_multinomial_factory(3),
    "pct_inperson_school_fulltime_oldest", "s_inperson_school_fulltime_oldest", compute_binary, jeffreys_multinomial_factory(3),
    "pct_inperson_school_parttime_oldest", "s_inperson_school_parttime_oldest", compute_binary, jeffreys_multinomial_factory(3),

    "pct_school_safety_measures_mask_students", "s_school_safety_measures_mask_students", compute_binary, jeffreys_binary,
    "pct_school_safety_measures_mask_teachers", "s_school_safety_measures_mask_teachers", compute_binary, jeffreys_binary,
    "pct_school_safety_measures_restricted_entry", "s_school_safety_measures_restricted_entry", compute_binary, jeffreys_binary,
    "pct_school_safety_measures_separators", "s_school_safety_measures_separators", compute_binary, jeffreys_binary,
    "pct_school_safety_measures_extracurricular", "s_school_safety_measures_extracurricular", compute_binary, jeffreys_binary,
    "pct_school_safety_measures_symptom_screen", "s_school_safety_measures_symptom_screen", compute_binary, jeffreys_binary,
    "pct_school_safety_measures_ventilation", "s_school_safety_measures_ventilation", compute_binary, jeffreys_binary,
    "pct_school_safety_measures_testing_staff", "s_school_safety_measures_testing_staff", compute_binary, jeffreys_binary,
    "pct_school_safety_measures_testing_students", "s_school_safety_measures_testing_students", compute_binary, jeffreys_binary,
    "pct_school_safety_measures_vaccine_staff", "s_school_safety_measures_vaccine_staff", compute_binary, jeffreys_binary,
    "pct_school_safety_measures_vaccine_students", "s_school_safety_measures_vaccine_students", compute_binary, jeffreys_binary,
    "pct_school_safety_measures_cafeteria", "s_school_safety_measures_cafeteria", compute_binary, jeffreys_binary,
    "pct_school_safety_measures_dont_know", "s_school_safety_measures_dont_know", compute_binary, jeffreys_binary,

    # Means. No post-processing required.
    "mean_days_symptoms", "symp_n_days", compute_numeric_mean, I,
    "mean_ppl_symptoms_household", "hh_number_sick", compute_numeric_mean, I,
    "mean_ppl_symptoms_community", "community_number_sick", compute_numeric_mean, I
  )

  aggs <- create_aggs_product(regions, groups, indicators)


  monthly_indicators <- tribble(
    ~name, ~metric, ~compute_fn, ~post_fn,
    "pct_child_school_public", "s_child_school_public", compute_binary, jeffreys_multinomial_factory(5),
    "pct_child_school_private", "s_child_school_private", compute_binary, jeffreys_multinomial_factory(5),
    "pct_child_school_homeschool", "s_child_school_homeschool", compute_binary, jeffreys_multinomial_factory(5),
    "pct_child_school_not", "s_child_school_not", compute_binary, jeffreys_multinomial_factory(5),
    "pct_child_school_other", "s_child_school_other", compute_binary, jeffreys_multinomial_factory(5)
  )
  monthly_aggs <- create_aggs_product(regions, groups, monthly_indicators)

  ### Include handful of original public tables not already covered by set above
  common_group <- c("agefull", "gender", "race", "hispanic")
  
  ## Cut 1: side effects generally and if hesitant about getting vaccine
  cut1_aggs <- create_aggs_product(
    regions,
    list(common_group),
    filter(indicators, .data$name %in% c("pct_worried_vaccine_sideeffects", "pct_hesitant_worried_vaccine_sideeffects"))
  )
  
  ## Cut 2: trust various institutions if hesitant about getting vaccine
  cut2_aggs <- create_aggs_product(
    regions,
    list(common_group),
    filter(indicators, startsWith(.data$name, "pct_hesitant_vaccine_likely_"))
  )
  
  ## Cut 3: trust various institutions
  cut3_aggs <- create_aggs_product(
    regions,
    list(common_group),
    filter(indicators, startsWith(.data$name, "pct_vaccine_likely_"))
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
    filter(indicators, .data$name %in% c("pct_vaccinated", "pct_accept_vaccine", "pct_appointment_or_accept_vaccine", "pct_accept_vaccine_no_appointment"))
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
    filter(indicators, .data$name %in% c("pct_vaccinated", "pct_accept_vaccine", "pct_appointment_or_accept_vaccine", "pct_accept_vaccine_no_appointment"))
  )
  
  ### Combine full set and additional original tables.
  aggs <- rbind(aggs, cut1_aggs, cut2_aggs, cut3_aggs, cut456_aggs, cut456_marginal_aggs)


  ### Demographic variables. Include in "overall" cuts only.
  demo_groups <- list(c())
  demo_indicators <- tribble(
    ~name, ~metric, ~compute_fn, ~post_fn,
    "pct_gender_male", "gender_male", compute_binary, jeffreys_multinomial_factory(3),
    "pct_gender_female", "gender_female", compute_binary, jeffreys_multinomial_factory(3),
    "pct_gender_nonbinary_other", "gender_nonbinary_other", compute_binary, jeffreys_multinomial_factory(3),

    "pct_age_18_24", "age_18_24", compute_binary, jeffreys_multinomial_factory(7),
    "pct_age_25_34", "age_25_34", compute_binary, jeffreys_multinomial_factory(7),
    "pct_age_35_44", "age_35_44", compute_binary, jeffreys_multinomial_factory(7),
    "pct_age_45_54", "age_45_54", compute_binary, jeffreys_multinomial_factory(7),
    "pct_age_55_64", "age_55_64", compute_binary, jeffreys_multinomial_factory(7),
    "pct_age_65_74", "age_65_74", compute_binary, jeffreys_multinomial_factory(7),
    "pct_age_75_older", "age_75_older", compute_binary, jeffreys_multinomial_factory(7),

    "pct_hispanic_latino", "hispanic", compute_binary, jeffreys_binary,

    "pct_race_american_indian_alaska_native", "race_american_indian_alaska_native", compute_binary, jeffreys_multinomial_factory(6),
    "pct_race_asian", "race_asian", compute_binary, jeffreys_multinomial_factory(6),
    "pct_race_black_african_american", "race_black_african_american", compute_binary, jeffreys_multinomial_factory(6),
    "pct_race_native_hawaiian_pacific_islander", "race_native_hawaiian_pacific_islander", compute_binary, jeffreys_multinomial_factory(6),
    "pct_race_white", "race_white", compute_binary, jeffreys_multinomial_factory(6),
    "pct_race_multiple_other", "race_multiple_other", compute_binary, jeffreys_multinomial_factory(6),

    "pct_education_less_than_highschool", "education_less_than_highschool", compute_binary, jeffreys_multinomial_factory(9),
    "pct_education_highschool_or_equivalent", "education_highschool_or_equivalent", compute_binary, jeffreys_multinomial_factory(9),
    "pct_education_some_college", "education_some_college", compute_binary, jeffreys_multinomial_factory(9),
    "pct_education_2yr_degree", "education_2yr_degree", compute_binary, jeffreys_multinomial_factory(9),
    "pct_education_4yr_degree", "education_4yr_degree", compute_binary, jeffreys_multinomial_factory(9),
    "pct_education_masters", "education_masters", compute_binary, jeffreys_multinomial_factory(9),
    "pct_education_professional_degree", "education_professional_degree", compute_binary, jeffreys_multinomial_factory(9),
    "pct_education_doctorate", "education_doctorate", compute_binary, jeffreys_multinomial_factory(9),

    "pct_language_home_english", "language_home_english", compute_binary, jeffreys_multinomial_factory(7),
    "pct_language_home_spanish", "language_home_spanish", compute_binary, jeffreys_multinomial_factory(7),
    "pct_language_home_chinese", "language_home_chinese", compute_binary, jeffreys_multinomial_factory(7),
    "pct_language_home_vietnamese", "language_home_vietnamese", compute_binary, jeffreys_multinomial_factory(7),
    "pct_language_home_french", "language_home_french", compute_binary, jeffreys_multinomial_factory(7),
    "pct_language_home_portuguese", "language_home_portuguese", compute_binary, jeffreys_multinomial_factory(7),
    "pct_language_home_other", "language_home_other", compute_binary, jeffreys_multinomial_factory(7),

    "pct_work_for_pay_4w", "a_work_for_pay_4w", compute_binary, jeffreys_binary,

    "pct_condition_asthma", "comorbid_asthma", compute_binary, jeffreys_binary,
    "pct_condition_lung", "comorbidlungdisease", compute_binary, jeffreys_binary,
    "pct_condition_cancer", "comorbidcancer", compute_binary, jeffreys_binary,
    "pct_condition_diabetes", "comorbiddiabetes", compute_binary, jeffreys_binary,
    "pct_condition_hbp", "comorbid_high_blood_pressure", compute_binary, jeffreys_binary,
    "pct_condition_kidney", "comorbidkidneydisease", compute_binary, jeffreys_binary,
    "pct_condition_immune", "comorbid_autoimmune", compute_binary, jeffreys_binary,
    "pct_condition_cvd", "comorbidheartdisease", compute_binary, jeffreys_binary,
    "pct_condition_obesity", "comorbidobese", compute_binary, jeffreys_binary,
    "pct_condition_none", "comorbid_none", compute_binary, jeffreys_binary,

    "pct_pregnant", "pregnant", compute_binary, jeffreys_binary,

    "pct_smoke", "smoker", compute_binary, jeffreys_binary,

    "pct_children_prek", "children_prek", compute_binary, jeffreys_binary,
    "pct_children_gr1_5", "children_gr1_5", compute_binary, jeffreys_binary,
    "pct_children_gr6_8", "children_gr6_8", compute_binary, jeffreys_binary,
    "pct_children_gr9_12", "children_gr9_12", compute_binary, jeffreys_binary,

    # Already in main grouping, including "overall". Just need to make sure to include in demo/parenting tables.
    # "pct_inperson_school_fulltime", "s_inperson_school_fulltime", compute_binary, jeffreys_binary,
    # "pct_inperson_school_parttime", "s_inperson_school_parttime", compute_binary, jeffreys_binary,
    # "pct_inperson_school_fulltime_oldest", "s_inperson_school_fulltime_oldest", compute_binary, jeffreys_binary,
    # "pct_remote_school_fulltime_oldest", "s_remote_school_fulltime_oldest", compute_binary, jeffreys_binary,
    # "pct_inperson_school_parttime_oldest", "s_inperson_school_parttime_oldest", compute_binary, jeffreys_binary,

    "pct_children_school_measure_mask_students", "children_school_measure_mask_students", compute_binary, jeffreys_binary,
    "pct_children_school_measure_mask_teachers", "children_school_measure_mask_teachers", compute_binary, jeffreys_binary,
    "pct_children_school_measure_same_teacher", "children_school_measure_same_teacher", compute_binary, jeffreys_binary,
    "pct_children_school_measure_same_students", "children_school_measure_same_students", compute_binary, jeffreys_binary,
    "pct_children_school_measure_outdoor", "children_school_measure_outdoor", compute_binary, jeffreys_binary,
    "pct_children_school_measure_entry", "children_school_measure_entry", compute_binary, jeffreys_binary,
    "pct_children_school_measure_class_size", "children_school_measure_class_size", compute_binary, jeffreys_binary,
    "pct_children_school_measure_cafeteria", "children_school_measure_cafeteria", compute_binary, jeffreys_binary,
    "pct_children_school_measure_playground", "children_school_measure_playground", compute_binary, jeffreys_binary,
    "pct_children_school_measure_desk_shield", "children_school_measure_desk_shield", compute_binary, jeffreys_binary,
    "pct_children_school_measure_desk_space", "children_school_measure_desk_space", compute_binary, jeffreys_binary,
    "pct_children_school_measure_extracurricular", "children_school_measure_extracurricular", compute_binary, jeffreys_binary,
    "pct_children_school_measure_supplies", "children_school_measure_supplies", compute_binary, jeffreys_binary,
    "pct_children_school_measure_screening", "children_school_measure_screening", compute_binary, jeffreys_binary,

    "pct_occ_4w_social", "occ_4w_social", compute_binary, jeffreys_multinomial_factory(16),
    "pct_occ_4w_education", "occ_4w_education", compute_binary, jeffreys_multinomial_factory(16),
    "pct_occ_4w_arts", "occ_4w_arts", compute_binary, jeffreys_multinomial_factory(16),
    "pct_occ_4w_health_prac", "occ_4w_health_prac", compute_binary, jeffreys_multinomial_factory(16),
    "pct_occ_4w_health_support", "occ_4w_health_support", compute_binary, jeffreys_multinomial_factory(16),
    "pct_occ_4w_protective", "occ_4w_protective", compute_binary, jeffreys_multinomial_factory(16),
    "pct_occ_4w_food", "occ_4w_food", compute_binary, jeffreys_multinomial_factory(16),
    "pct_occ_4w_building", "occ_4w_building", compute_binary, jeffreys_multinomial_factory(16),
    "pct_occ_4w_personal", "occ_4w_personal", compute_binary, jeffreys_multinomial_factory(16),
    "pct_occ_4w_sales", "occ_4w_sales", compute_binary, jeffreys_multinomial_factory(16),
    "pct_occ_4w_admin", "occ_4w_admin", compute_binary, jeffreys_multinomial_factory(16),
    "pct_occ_4w_construction", "occ_4w_construction", compute_binary, jeffreys_multinomial_factory(16),
    "pct_occ_4w_maintenance", "occ_4w_maintenance", compute_binary, jeffreys_multinomial_factory(16),
    "pct_occ_4w_production", "occ_4w_production", compute_binary, jeffreys_multinomial_factory(16),
    "pct_occ_4w_transportation", "occ_4w_transportation", compute_binary, jeffreys_multinomial_factory(16),
    "pct_occ_4w_other", "occ_4w_other", compute_binary, jeffreys_multinomial_factory(16),

    "pct_", "", compute_binary, jeffreys_binary,
  )
  demo_aggs <- create_aggs_product(
    regsions,
    demo_groups,
    demo_indicators
  )

  weekly_aggs <- rbind(aggs, demo_aggs)
  monthly_aggs <- rbind(aggs, monthly_aggs, demo_aggs)
  
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
#' @importFrom stats na.omit
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
