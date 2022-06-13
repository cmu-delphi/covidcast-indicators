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

    # mask wearing and distancing
    "smoothed_wearing_mask", "weight_unif", "c_mask_often", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wwearing_mask", "weight", "c_mask_often", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wearing_mask_7d", "weight_unif", "c_mask_often_7d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wwearing_mask_7d", "weight", "c_mask_often_7d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_others_masked", "weight_unif", "c_others_masked", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wothers_masked", "weight", "c_others_masked", 6, compute_binary_response, jeffreys_binary,
    "smoothed_others_masked_public", "weight_unif", "c_others_masked_public", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wothers_masked_public", "weight", "c_others_masked_public", 6, compute_binary_response, jeffreys_binary,
    "smoothed_others_distanced_public", "weight_unif", "c_others_distanced_public", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wothers_distanced_public", "weight", "c_others_distanced_public", 6, compute_binary_response, jeffreys_binary,

    # mental health
    "smoothed_worried_become_ill", "weight_unif", "mh_worried_ill", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wworried_become_ill", "weight", "mh_worried_ill", 6, compute_binary_response, jeffreys_binary,
    "smoothed_worried_finances", "weight_unif", "mh_worried_finances", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wworried_finances", "weight", "mh_worried_finances", 6, compute_binary_response, jeffreys_binary,
    # pre-wave 10
    "smoothed_anxious_5d", "weight_unif", "mh_anxious", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wanxious_5d", "weight", "mh_anxious", 6, compute_binary_response, jeffreys_binary,
    "smoothed_depressed_5d", "weight_unif", "mh_depressed", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wdepressed_5d", "weight", "mh_depressed", 6, compute_binary_response, jeffreys_binary,
    "smoothed_felt_isolated_5d", "weight_unif", "mh_isolated", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wfelt_isolated_5d", "weight", "mh_isolated", 6, compute_binary_response, jeffreys_binary,
    # wave 10+
    "smoothed_anxious_7d", "weight_unif", "mh_anxious_7d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wanxious_7d", "weight", "mh_anxious_7d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_depressed_7d", "weight_unif", "mh_depressed_7d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wdepressed_7d", "weight", "mh_depressed_7d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_felt_isolated_7d", "weight_unif", "mh_isolated_7d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wfelt_isolated_7d", "weight", "mh_isolated_7d", 6, compute_binary_response, jeffreys_binary,
    # wave 11
    "smoothed_worried_catch_covid", "weight_unif", "mh_worried_catch_covid", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wworried_catch_covid", "weight", "mh_worried_catch_covid", 6, compute_binary_response, jeffreys_binary,
    
    # travel outside state
    # pre-wave 10
    "smoothed_travel_outside_state_5d", "weight_unif", "c_travel_state", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wtravel_outside_state_5d", "weight", "c_travel_state", 6, compute_binary_response, jeffreys_binary,
    # wave 10+
    "smoothed_travel_outside_state_7d", "weight_unif", "c_travel_state_7d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wtravel_outside_state_7d", "weight", "c_travel_state_7d", 6, compute_binary_response, jeffreys_binary,
    
    # work outside home
    # pre-wave 4
    "smoothed_work_outside_home_5d", "weight_unif", "c_work_outside_5d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wwork_outside_home_5d", "weight", "c_work_outside_5d", 6, compute_binary_response, jeffreys_binary,
    # wave 4+, pre-wave 10
    "smoothed_work_outside_home_1d", "weight_unif", "a_work_outside_home_1d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wwork_outside_home_1d", "weight", "a_work_outside_home_1d", 6, compute_binary_response, jeffreys_binary,
    # wave 10+
    "smoothed_work_outside_home_indoors_1d", "weight_unif", "a_work_outside_home_indoors_1d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wwork_outside_home_indoors_1d", "weight", "a_work_outside_home_indoors_1d", 6, compute_binary_response, jeffreys_binary,

    # activities
    # pre-Wave 10
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
    # Wave 10+
    "smoothed_shop_indoors_1d", "weight_unif", "a_shop_indoors_1d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wshop_indoors_1d", "weight", "a_shop_indoors_1d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_restaurant_indoors_1d", "weight_unif", "a_restaurant_indoors_1d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wrestaurant_indoors_1d", "weight", "a_restaurant_indoors_1d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_spent_time_indoors_1d", "weight_unif", "a_spent_time_indoors_1d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wspent_time_indoors_1d", "weight", "a_spent_time_indoors_1d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_large_event_indoors_1d", "weight_unif", "a_large_event_indoors_1d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wlarge_event_indoors_1d", "weight", "a_large_event_indoors_1d", 6, compute_binary_response, jeffreys_binary,
    
    # testing
    "smoothed_tested_14d", "weight_unif", "t_tested_14d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wtested_14d", "weight", "t_tested_14d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_tested_positive_14d", "weight_unif", "t_tested_positive_14d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wtested_positive_14d", "weight", "t_tested_positive_14d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_screening_tested_positive_14d", "weight_unif", "t_screening_tested_positive_14d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wscreening_tested_positive_14d", "weight", "t_screening_tested_positive_14d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wanted_test_14d", "weight_unif", "t_wanted_test_14d", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wwanted_test_14d", "weight", "t_wanted_test_14d", 6, compute_binary_response, jeffreys_binary,
    
    "smoothed_had_covid_ever", "weight_unif", "t_had_covid_ever", 6, compute_binary_response, jeffreys_binary,
    "smoothed_whad_covid_ever", "weight", "t_had_covid_ever", 6, compute_binary_response, jeffreys_binary,

    # vaccines
    "smoothed_accept_covid_vaccine", "weight_unif", "v_accept_covid_vaccine", 6, compute_binary_response, jeffreys_binary,
    "smoothed_waccept_covid_vaccine", "weight", "v_accept_covid_vaccine", 6, compute_binary_response, jeffreys_binary,
    "smoothed_accept_covid_vaccine_no_appointment", "weight_unif", "v_accept_covid_vaccine_no_appointment", 6, compute_binary_response, jeffreys_binary,
    "smoothed_waccept_covid_vaccine_no_appointment", "weight", "v_accept_covid_vaccine_no_appointment", 6, compute_binary_response, jeffreys_binary,
    "smoothed_appointment_or_accept_covid_vaccine", "weight_unif", "v_appointment_or_accept_covid_vaccine", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wappointment_or_accept_covid_vaccine", "weight", "v_appointment_or_accept_covid_vaccine", 6, compute_binary_response, jeffreys_binary,
    "smoothed_covid_vaccinated_or_accept", "weight_unif", "v_covid_vaccinated_or_accept", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wcovid_vaccinated_or_accept", "weight", "v_covid_vaccinated_or_accept", 6, compute_binary_response, jeffreys_binary,
    "smoothed_covid_vaccinated_appointment_or_accept", "weight_unif", "v_covid_vaccinated_appointment_or_accept", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wcovid_vaccinated_appointment_or_accept", "weight", "v_covid_vaccinated_appointment_or_accept", 6, compute_binary_response, jeffreys_binary,
    "smoothed_appointment_not_vaccinated", "weight_unif", "v_appointment_not_vaccinated", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wappointment_not_vaccinated", "weight", "v_appointment_not_vaccinated", 6, compute_binary_response, jeffreys_binary,
    
    "smoothed_covid_vaccinated", "weight_unif", "v_covid_vaccinated", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wcovid_vaccinated", "weight", "v_covid_vaccinated", 6, compute_binary_response, jeffreys_binary,
    "smoothed_worried_vaccine_side_effects", "weight_unif", "v_worried_vaccine_side_effects", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wworried_vaccine_side_effects", "weight", "v_worried_vaccine_side_effects", 6, compute_binary_response, jeffreys_binary,
    "smoothed_received_2_vaccine_doses", "weight_unif", "v_received_2_vaccine_doses", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wreceived_2_vaccine_doses", "weight", "v_received_2_vaccine_doses", 6, compute_binary_response, jeffreys_binary,
    
    "smoothed_covid_vaccinated_friends", "weight_unif", "v_covid_vaccinated_friends", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wcovid_vaccinated_friends", "weight", "v_covid_vaccinated_friends", 6, compute_binary_response, jeffreys_binary,
    
    "smoothed_vaccinate_children", "weight_unif", "v_vaccinate_children", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccinate_children", "weight", "v_vaccinate_children", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccinate_child_oldest", "weight_unif", "v_vaccinate_child_oldest", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccinate_child_oldest", "weight", "v_vaccinate_child_oldest", 6, compute_binary_response, jeffreys_binary,

    "smoothed_wchild_vaccine_already", "weight", "v_child_vaccine_already", 6, compute_binary_response, jeffreys_multinomial_factory(5),
    "smoothed_wchild_vaccine_yes_def", "weight", "v_child_vaccine_yes_def", 6, compute_binary_response, jeffreys_multinomial_factory(5),
    "smoothed_wchild_vaccine_yes_prob", "weight", "v_child_vaccine_yes_prob", 6, compute_binary_response, jeffreys_multinomial_factory(5),
    "smoothed_wchild_vaccine_no_prob", "weight", "v_child_vaccine_no_prob", 6, compute_binary_response, jeffreys_multinomial_factory(5),
    "smoothed_wchild_vaccine_no_def", "weight", "v_child_vaccine_no_def", 6, compute_binary_response, jeffreys_multinomial_factory(5),

    "smoothed_try_vaccinate_1m", "weight_unif", "v_try_vaccinate_1m", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wtry_vaccinate_1m", "weight", "v_try_vaccinate_1m", 6, compute_binary_response, jeffreys_binary,
    
    "smoothed_winitial_dose_one_of_one", "weight", "v_initial_dose_one_of_one", 6, compute_binary_response, jeffreys_multinomial_factory(4),
    "smoothed_winitial_dose_one_of_two", "weight", "v_initial_dose_one_of_two", 6, compute_binary_response, jeffreys_multinomial_factory(4),
    "smoothed_winitial_dose_two_of_two", "weight", "v_initial_dose_two_of_two", 6, compute_binary_response, jeffreys_multinomial_factory(4),
    
    "smoothed_wvaccinated_one_booster", "weight", "v_vaccinated_one_booster", 6, compute_binary_response, jeffreys_multinomial_factory(4),
    "smoothed_wvaccinated_two_or_more_boosters", "weight", "v_vaccinated_two_or_more_boosters", 6, compute_binary_response, jeffreys_multinomial_factory(4),
    "smoothed_wvaccinated_no_booster", "weight", "v_vaccinated_no_booster", 6, compute_binary_response, jeffreys_multinomial_factory(4),
    "smoothed_wvaccinated_at_least_one_booster", "weight", "v_vaccinated_at_least_one_booster", 6, compute_binary_response, jeffreys_binary,
    
    "smoothed_wvaccinated_booster_accept", "weight", "v_vaccinated_booster_accept", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccinated_booster_hesitant", "weight", "v_vaccinated_booster_hesitant", 6, compute_binary_response, jeffreys_binary,
    
    "smoothed_wvaccinated_booster_defyes", "weight", "v_vaccinated_booster_defyes", 6, compute_binary_response, jeffreys_multinomial_factory(4),
    "smoothed_wvaccinated_booster_probyes", "weight", "v_vaccinated_booster_probyes", 6, compute_binary_response, jeffreys_multinomial_factory(4),
    "smoothed_wvaccinated_booster_probno", "weight", "v_vaccinated_booster_probno", 6, compute_binary_response, jeffreys_multinomial_factory(4),
    "smoothed_wvaccinated_booster_defno", "weight", "v_vaccinated_booster_defno", 6, compute_binary_response, jeffreys_multinomial_factory(4),
    
    "smoothed_wflu_vaccinated_2021", "weight", "v_flu_vaccinated_2021", 6, compute_binary_response, jeffreys_binary,

    
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
    "smoothed_wvaccine_likely_politicians", "weight", "v_vaccine_likely_politicians", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_likely_doctors", "weight_unif", "v_vaccine_likely_doctors", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_likely_doctors", "weight", "v_vaccine_likely_doctors", 6, compute_binary_response, jeffreys_binary,
    "smoothed_trust_covid_info_doctors", "weight_unif", "i_trust_covid_info_doctors", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wtrust_covid_info_doctors", "weight", "i_trust_covid_info_doctors", 6, compute_binary_response, jeffreys_binary,
    "smoothed_trust_covid_info_experts", "weight_unif", "i_trust_covid_info_experts", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wtrust_covid_info_experts", "weight", "i_trust_covid_info_experts", 6, compute_binary_response, jeffreys_binary,
    "smoothed_trust_covid_info_cdc", "weight_unif", "i_trust_covid_info_cdc", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wtrust_covid_info_cdc", "weight", "i_trust_covid_info_cdc", 6, compute_binary_response, jeffreys_binary,
    "smoothed_trust_covid_info_govt_health", "weight_unif", "i_trust_covid_info_govt_health", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wtrust_covid_info_govt_health", "weight", "i_trust_covid_info_govt_health", 6, compute_binary_response, jeffreys_binary,
    "smoothed_trust_covid_info_politicians", "weight_unif", "i_trust_covid_info_politicians", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wtrust_covid_info_politicians", "weight", "i_trust_covid_info_politicians", 6, compute_binary_response, jeffreys_binary,
    "smoothed_trust_covid_info_journalists", "weight_unif", "i_trust_covid_info_journalists", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wtrust_covid_info_journalists", "weight", "i_trust_covid_info_journalists", 6, compute_binary_response, jeffreys_binary,
    "smoothed_trust_covid_info_friends", "weight_unif", "i_trust_covid_info_friends", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wtrust_covid_info_friends", "weight", "i_trust_covid_info_friends", 6, compute_binary_response, jeffreys_binary,
    "smoothed_trust_covid_info_religious", "weight_unif", "i_trust_covid_info_religious", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wtrust_covid_info_religious", "weight", "i_trust_covid_info_religious", 6, compute_binary_response, jeffreys_binary,
    
    # vaccine hesitancy reasons
    "smoothed_hesitancy_reason_sideeffects", "weight_unif", "v_hesitancy_reason_sideeffects", 6, compute_binary_response, jeffreys_binary,
    "smoothed_whesitancy_reason_sideeffects", "weight", "v_hesitancy_reason_sideeffects", 6, compute_binary_response, jeffreys_binary,
    "smoothed_hesitancy_reason_allergic", "weight_unif", "v_hesitancy_reason_allergic", 6, compute_binary_response, jeffreys_binary,
    "smoothed_whesitancy_reason_allergic", "weight", "v_hesitancy_reason_allergic", 6, compute_binary_response, jeffreys_binary,
    "smoothed_hesitancy_reason_ineffective", "weight_unif", "v_hesitancy_reason_ineffective", 6, compute_binary_response, jeffreys_binary,
    "smoothed_whesitancy_reason_ineffective", "weight", "v_hesitancy_reason_ineffective", 6, compute_binary_response, jeffreys_binary,
    "smoothed_hesitancy_reason_unnecessary", "weight_unif", "v_hesitancy_reason_unnecessary", 6, compute_binary_response, jeffreys_binary,
    "smoothed_whesitancy_reason_unnecessary", "weight", "v_hesitancy_reason_unnecessary", 6, compute_binary_response, jeffreys_binary,
    "smoothed_hesitancy_reason_dislike_vaccines", "weight_unif", "v_hesitancy_reason_dislike_vaccines", 6, compute_binary_response, jeffreys_binary,
    "smoothed_whesitancy_reason_dislike_vaccines", "weight", "v_hesitancy_reason_dislike_vaccines", 6, compute_binary_response, jeffreys_binary,
    "smoothed_hesitancy_reason_not_recommended", "weight_unif", "v_hesitancy_reason_not_recommended", 6, compute_binary_response, jeffreys_binary,
    "smoothed_whesitancy_reason_not_recommended", "weight", "v_hesitancy_reason_not_recommended", 6, compute_binary_response, jeffreys_binary,
    "smoothed_hesitancy_reason_wait_safety", "weight_unif", "v_hesitancy_reason_wait_safety", 6, compute_binary_response, jeffreys_binary,
    "smoothed_whesitancy_reason_wait_safety", "weight", "v_hesitancy_reason_wait_safety", 6, compute_binary_response, jeffreys_binary,
    "smoothed_hesitancy_reason_low_priority", "weight_unif", "v_hesitancy_reason_low_priority", 6, compute_binary_response, jeffreys_binary,
    "smoothed_whesitancy_reason_low_priority", "weight", "v_hesitancy_reason_low_priority", 6, compute_binary_response, jeffreys_binary,
    "smoothed_hesitancy_reason_cost", "weight_unif", "v_hesitancy_reason_cost", 6, compute_binary_response, jeffreys_binary,
    "smoothed_whesitancy_reason_cost", "weight", "v_hesitancy_reason_cost", 6, compute_binary_response, jeffreys_binary,
    "smoothed_hesitancy_reason_distrust_vaccines", "weight_unif", "v_hesitancy_reason_distrust_vaccines", 6, compute_binary_response, jeffreys_binary,
    "smoothed_whesitancy_reason_distrust_vaccines", "weight", "v_hesitancy_reason_distrust_vaccines", 6, compute_binary_response, jeffreys_binary,
    "smoothed_hesitancy_reason_distrust_gov", "weight_unif", "v_hesitancy_reason_distrust_gov", 6, compute_binary_response, jeffreys_binary,
    "smoothed_whesitancy_reason_distrust_gov", "weight", "v_hesitancy_reason_distrust_gov", 6, compute_binary_response, jeffreys_binary,
    "smoothed_hesitancy_reason_health_condition", "weight_unif", "v_hesitancy_reason_health_condition", 6, compute_binary_response, jeffreys_binary,
    "smoothed_whesitancy_reason_health_condition", "weight", "v_hesitancy_reason_health_condition", 6, compute_binary_response, jeffreys_binary,
    "smoothed_hesitancy_reason_other", "weight_unif", "v_hesitancy_reason_other", 6, compute_binary_response, jeffreys_binary,
    "smoothed_whesitancy_reason_other", "weight", "v_hesitancy_reason_other", 6, compute_binary_response, jeffreys_binary,
    "smoothed_hesitancy_reason_pregnant", "weight_unif", "v_hesitancy_reason_pregnant", 6, compute_binary_response, jeffreys_binary,
    "smoothed_whesitancy_reason_pregnant", "weight", "v_hesitancy_reason_pregnant", 6, compute_binary_response, jeffreys_binary,
    "smoothed_hesitancy_reason_religious", "weight_unif", "v_hesitancy_reason_religious", 6, compute_binary_response, jeffreys_binary,
    "smoothed_whesitancy_reason_religious", "weight", "v_hesitancy_reason_religious", 6, compute_binary_response, jeffreys_binary,
    "smoothed_hesitancy_reason_dislike_vaccines_generally", "weight_unif", "v_hesitancy_reason_dislike_vaccines_generally", 6, compute_binary_response, jeffreys_binary,
    "smoothed_whesitancy_reason_dislike_vaccines_generally", "weight", "v_hesitancy_reason_dislike_vaccines_generally", 6, compute_binary_response, jeffreys_binary,

    # vaccine barriers
    "smoothed_vaccine_barrier_eligible", "weight_unif", "v_vaccine_barrier_eligible", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_eligible", "weight", "v_vaccine_barrier_eligible", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_no_appointments", "weight_unif", "v_vaccine_barrier_no_appointments", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_no_appointments", "weight", "v_vaccine_barrier_no_appointments", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_appointment_time", "weight_unif", "v_vaccine_barrier_appointment_time", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_appointment_time", "weight", "v_vaccine_barrier_appointment_time", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_technical_difficulties", "weight_unif", "v_vaccine_barrier_technical_difficulties", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_technical_difficulties", "weight", "v_vaccine_barrier_technical_difficulties", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_document", "weight_unif", "v_vaccine_barrier_document", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_document", "weight", "v_vaccine_barrier_document", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_technology_access", "weight_unif", "v_vaccine_barrier_technology_access", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_technology_access", "weight", "v_vaccine_barrier_technology_access", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_travel", "weight_unif", "v_vaccine_barrier_travel", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_travel", "weight", "v_vaccine_barrier_travel", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_language", "weight_unif", "v_vaccine_barrier_language", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_language", "weight", "v_vaccine_barrier_language", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_childcare", "weight_unif", "v_vaccine_barrier_childcare", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_childcare", "weight", "v_vaccine_barrier_childcare", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_time", "weight_unif", "v_vaccine_barrier_time", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_time", "weight", "v_vaccine_barrier_time", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_type", "weight_unif", "v_vaccine_barrier_type", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_type", "weight", "v_vaccine_barrier_type", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_none", "weight_unif", "v_vaccine_barrier_none", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_none", "weight", "v_vaccine_barrier_none", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_appointment_location", "weight_unif", "v_vaccine_barrier_appointment_location", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_appointment_location", "weight", "v_vaccine_barrier_appointment_location", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_other", "weight_unif", "v_vaccine_barrier_other", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_other", "weight", "v_vaccine_barrier_other", 6, compute_binary_response, jeffreys_binary,
    
    # vaccine barriers for vaccinated
    "smoothed_vaccine_barrier_eligible_has", "weight_unif", "v_vaccine_barrier_eligible_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_eligible_has", "weight", "v_vaccine_barrier_eligible_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_no_appointments_has", "weight_unif", "v_vaccine_barrier_no_appointments_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_no_appointments_has", "weight", "v_vaccine_barrier_no_appointments_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_appointment_time_has", "weight_unif", "v_vaccine_barrier_appointment_time_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_appointment_time_has", "weight", "v_vaccine_barrier_appointment_time_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_technical_difficulties_has", "weight_unif", "v_vaccine_barrier_technical_difficulties_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_technical_difficulties_has", "weight", "v_vaccine_barrier_technical_difficulties_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_document_has", "weight_unif", "v_vaccine_barrier_document_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_document_has", "weight", "v_vaccine_barrier_document_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_technology_access_has", "weight_unif", "v_vaccine_barrier_technology_access_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_technology_access_has", "weight", "v_vaccine_barrier_technology_access_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_travel_has", "weight_unif", "v_vaccine_barrier_travel_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_travel_has", "weight", "v_vaccine_barrier_travel_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_language_has", "weight_unif", "v_vaccine_barrier_language_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_language_has", "weight", "v_vaccine_barrier_language_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_childcare_has", "weight_unif", "v_vaccine_barrier_childcare_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_childcare_has", "weight", "v_vaccine_barrier_childcare_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_time_has", "weight_unif", "v_vaccine_barrier_time_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_time_has", "weight", "v_vaccine_barrier_time_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_type_has", "weight_unif", "v_vaccine_barrier_type_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_type_has", "weight", "v_vaccine_barrier_type_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_none_has", "weight_unif", "v_vaccine_barrier_none_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_none_has", "weight", "v_vaccine_barrier_none_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_appointment_location_has", "weight_unif", "v_vaccine_barrier_appointment_location_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_appointment_location_has", "weight", "v_vaccine_barrier_appointment_location_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_other_has", "weight_unif", "v_vaccine_barrier_other_has", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_other_has", "weight", "v_vaccine_barrier_other_has", 6, compute_binary_response, jeffreys_binary,
    
    # vaccine barriers for attempted vaccinated
    "smoothed_vaccine_barrier_eligible_tried", "weight_unif", "v_vaccine_barrier_eligible_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_eligible_tried", "weight", "v_vaccine_barrier_eligible_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_no_appointments_tried", "weight_unif", "v_vaccine_barrier_no_appointments_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_no_appointments_tried", "weight", "v_vaccine_barrier_no_appointments_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_appointment_time_tried", "weight_unif", "v_vaccine_barrier_appointment_time_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_appointment_time_tried", "weight", "v_vaccine_barrier_appointment_time_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_technical_difficulties_tried", "weight_unif", "v_vaccine_barrier_technical_difficulties_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_technical_difficulties_tried", "weight", "v_vaccine_barrier_technical_difficulties_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_document_tried", "weight_unif", "v_vaccine_barrier_document_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_document_tried", "weight", "v_vaccine_barrier_document_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_technology_access_tried", "weight_unif", "v_vaccine_barrier_technology_access_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_technology_access_tried", "weight", "v_vaccine_barrier_technology_access_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_travel_tried", "weight_unif", "v_vaccine_barrier_travel_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_travel_tried", "weight", "v_vaccine_barrier_travel_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_language_tried", "weight_unif", "v_vaccine_barrier_language_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_language_tried", "weight", "v_vaccine_barrier_language_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_childcare_tried", "weight_unif", "v_vaccine_barrier_childcare_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_childcare_tried", "weight", "v_vaccine_barrier_childcare_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_time_tried", "weight_unif", "v_vaccine_barrier_time_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_time_tried", "weight", "v_vaccine_barrier_time_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_type_tried", "weight_unif", "v_vaccine_barrier_type_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_type_tried", "weight", "v_vaccine_barrier_type_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_none_tried", "weight_unif", "v_vaccine_barrier_none_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_none_tried", "weight", "v_vaccine_barrier_none_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_appointment_location_tried", "weight_unif", "v_vaccine_barrier_appointment_location_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_appointment_location_tried", "weight", "v_vaccine_barrier_appointment_location_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_vaccine_barrier_other_tried", "weight_unif", "v_vaccine_barrier_other_tried", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wvaccine_barrier_other_tried", "weight", "v_vaccine_barrier_other_tried", 6, compute_binary_response, jeffreys_binary,
    
    
    # reasons for belief that vaccine is unnecessary
    "smoothed_dontneed_reason_had_covid", "weight_unif", "v_dontneed_reason_had_covid", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wdontneed_reason_had_covid", "weight", "v_dontneed_reason_had_covid", 6, compute_binary_response, jeffreys_binary,
    "smoothed_dontneed_reason_dont_spend_time", "weight_unif", "v_dontneed_reason_dont_spend_time", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wdontneed_reason_dont_spend_time", "weight", "v_dontneed_reason_dont_spend_time", 6, compute_binary_response, jeffreys_binary,
    "smoothed_dontneed_reason_not_high_risk", "weight_unif", "v_dontneed_reason_not_high_risk", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wdontneed_reason_not_high_risk", "weight", "v_dontneed_reason_not_high_risk", 6, compute_binary_response, jeffreys_binary,
    "smoothed_dontneed_reason_precautions", "weight_unif", "v_dontneed_reason_precautions", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wdontneed_reason_precautions", "weight", "v_dontneed_reason_precautions", 6, compute_binary_response, jeffreys_binary,
    "smoothed_dontneed_reason_not_serious", "weight_unif", "v_dontneed_reason_not_serious", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wdontneed_reason_not_serious", "weight", "v_dontneed_reason_not_serious", 6, compute_binary_response, jeffreys_binary,
    "smoothed_dontneed_reason_not_beneficial", "weight_unif", "v_dontneed_reason_not_beneficial", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wdontneed_reason_not_beneficial", "weight", "v_dontneed_reason_not_beneficial", 6, compute_binary_response, jeffreys_binary,
    "smoothed_dontneed_reason_other", "weight_unif", "v_dontneed_reason_other", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wdontneed_reason_other", "weight", "v_dontneed_reason_other", 6, compute_binary_response, jeffreys_binary,
    
    # schooling
    "smoothed_inperson_school_fulltime", "weight_unif", "s_inperson_school_fulltime", 6, compute_binary_response, jeffreys_binary,
    "smoothed_winperson_school_fulltime", "weight", "s_inperson_school_fulltime", 6, compute_binary_response, jeffreys_binary,
    "smoothed_inperson_school_parttime", "weight_unif", "s_inperson_school_parttime", 6, compute_binary_response, jeffreys_binary,
    "smoothed_winperson_school_parttime", "weight", "s_inperson_school_parttime", 6, compute_binary_response, jeffreys_binary,
    
    "smoothed_inperson_school_fulltime_oldest", "weight_unif", "s_inperson_school_fulltime_oldest", 6, compute_binary_response, jeffreys_multinomial_factory(3),
    "smoothed_winperson_school_fulltime_oldest", "weight", "s_inperson_school_fulltime_oldest", 6, compute_binary_response, jeffreys_multinomial_factory(3),
    "smoothed_inperson_school_parttime_oldest", "weight_unif", "s_inperson_school_parttime_oldest", 6, compute_binary_response, jeffreys_multinomial_factory(3),
    "smoothed_winperson_school_parttime_oldest", "weight", "s_inperson_school_parttime_oldest", 6, compute_binary_response, jeffreys_multinomial_factory(3),
    "smoothed_wremote_school_fulltime_oldest", "weight", "s_remote_school_fulltime_oldest", 6, compute_binary_response, jeffreys_multinomial_factory(3),

    "smoothed_wschool_safety_measures_mask_students", "weight", "s_school_safety_measures_mask_students", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wschool_safety_measures_mask_teachers", "weight", "s_school_safety_measures_mask_teachers", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wschool_safety_measures_restricted_entry", "weight", "s_school_safety_measures_restricted_entry", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wschool_safety_measures_separators", "weight", "s_school_safety_measures_separators", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wschool_safety_measures_extracurricular", "weight", "s_school_safety_measures_extracurricular", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wschool_safety_measures_symptom_screen", "weight", "s_school_safety_measures_symptom_screen", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wschool_safety_measures_ventilation", "weight", "s_school_safety_measures_ventilation", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wschool_safety_measures_testing_staff", "weight", "s_school_safety_measures_testing_staff", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wschool_safety_measures_testing_students", "weight", "s_school_safety_measures_testing_students", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wschool_safety_measures_vaccine_staff", "weight", "s_school_safety_measures_vaccine_staff", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wschool_safety_measures_vaccine_students", "weight", "s_school_safety_measures_vaccine_students", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wschool_safety_measures_cafeteria", "weight", "s_school_safety_measures_cafeteria", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wschool_safety_measures_dont_know", "weight", "s_school_safety_measures_dont_know", 6, compute_binary_response, jeffreys_binary,

    # beliefs
    "smoothed_belief_masking_effective", "weight_unif", "b_belief_masking_effective", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wbelief_masking_effective", "weight", "b_belief_masking_effective", 6, compute_binary_response, jeffreys_binary,
    "smoothed_belief_distancing_effective", "weight_unif", "b_belief_distancing_effective", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wbelief_distancing_effective", "weight", "b_belief_distancing_effective", 6, compute_binary_response, jeffreys_binary,
    
    "smoothed_belief_vaccinated_mask_unnecessary", "weight_unif", "b_belief_vaccinated_mask_unnecessary", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wbelief_vaccinated_mask_unnecessary", "weight", "b_belief_vaccinated_mask_unnecessary", 6, compute_binary_response, jeffreys_binary,
    "smoothed_belief_children_immune", "weight_unif", "b_belief_children_immune", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wbelief_children_immune", "weight", "b_belief_children_immune", 6, compute_binary_response, jeffreys_binary,
    "smoothed_belief_created_small_group", "weight_unif", "b_belief_created_small_group", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wbelief_created_small_group", "weight", "b_belief_created_small_group", 6, compute_binary_response, jeffreys_binary,
    "smoothed_belief_govt_exploitation", "weight_unif", "b_belief_govt_exploitation", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wbelief_govt_exploitation", "weight", "b_belief_govt_exploitation", 6, compute_binary_response, jeffreys_binary,
    
    # medical care beliefs and experiences
    "smoothed_race_treated_fairly_healthcare", "weight_unif", "b_race_treated_fairly_healthcare", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wrace_treated_fairly_healthcare", "weight", "b_race_treated_fairly_healthcare", 6, compute_binary_response, jeffreys_binary,
    "smoothed_delayed_care_cost", "weight_unif", "b_delayed_care_cost", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wdelayed_care_cost", "weight", "b_delayed_care_cost", 6, compute_binary_response, jeffreys_binary,
    
    # news
    "smoothed_received_news_local_health", "weight_unif", "i_received_news_local_health", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wreceived_news_local_health", "weight", "i_received_news_local_health", 6, compute_binary_response, jeffreys_binary,
    "smoothed_received_news_experts", "weight_unif", "i_received_news_experts", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wreceived_news_experts", "weight", "i_received_news_experts", 6, compute_binary_response, jeffreys_binary,
    "smoothed_received_news_cdc", "weight_unif", "i_received_news_cdc", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wreceived_news_cdc", "weight", "i_received_news_cdc", 6, compute_binary_response, jeffreys_binary,
    "smoothed_received_news_govt_health", "weight_unif", "i_received_news_govt_health", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wreceived_news_govt_health", "weight", "i_received_news_govt_health", 6, compute_binary_response, jeffreys_binary,
    "smoothed_received_news_politicians", "weight_unif", "i_received_news_politicians", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wreceived_news_politicians", "weight", "i_received_news_politicians", 6, compute_binary_response, jeffreys_binary,
    "smoothed_received_news_journalists", "weight_unif", "i_received_news_journalists", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wreceived_news_journalists", "weight", "i_received_news_journalists", 6, compute_binary_response, jeffreys_binary,
    "smoothed_received_news_friends", "weight_unif", "i_received_news_friends", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wreceived_news_friends", "weight", "i_received_news_friends", 6, compute_binary_response, jeffreys_binary,
    "smoothed_received_news_religious", "weight_unif", "i_received_news_religious", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wreceived_news_religious", "weight", "i_received_news_religious", 6, compute_binary_response, jeffreys_binary,
    "smoothed_received_news_none", "weight_unif", "i_received_news_none", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wreceived_news_none", "weight", "i_received_news_none", 6, compute_binary_response, jeffreys_binary,
    
    # topics want to learn about
    "smoothed_want_info_covid_treatment", "weight_unif", "i_want_info_covid_treatment", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wwant_info_covid_treatment", "weight", "i_want_info_covid_treatment", 6, compute_binary_response, jeffreys_binary,
    "smoothed_want_info_vaccine_access", "weight_unif", "i_want_info_vaccine_access", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wwant_info_vaccine_access", "weight", "i_want_info_vaccine_access", 6, compute_binary_response, jeffreys_binary,
    "smoothed_want_info_vaccine_types", "weight_unif", "i_want_info_vaccine_types", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wwant_info_vaccine_types", "weight", "i_want_info_vaccine_types", 6, compute_binary_response, jeffreys_binary,
    "smoothed_want_info_covid_variants", "weight_unif", "i_want_info_covid_variants", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wwant_info_covid_variants", "weight", "i_want_info_covid_variants", 6, compute_binary_response, jeffreys_binary,
    "smoothed_want_info_children_education", "weight_unif", "i_want_info_children_education", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wwant_info_children_education", "weight", "i_want_info_children_education", 6, compute_binary_response, jeffreys_binary,
    "smoothed_want_info_mental_health", "weight_unif", "i_want_info_mental_health", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wwant_info_mental_health", "weight", "i_want_info_mental_health", 6, compute_binary_response, jeffreys_binary,
    "smoothed_want_info_relationships", "weight_unif", "i_want_info_relationships", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wwant_info_relationships", "weight", "i_want_info_relationships", 6, compute_binary_response, jeffreys_binary,
    "smoothed_want_info_employment", "weight_unif", "i_want_info_employment", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wwant_info_employment", "weight", "i_want_info_employment", 6, compute_binary_response, jeffreys_binary,
    "smoothed_want_info_none", "weight_unif", "i_want_info_none", 6, compute_binary_response, jeffreys_binary,
    "smoothed_wwant_info_none", "weight", "i_want_info_none", 6, compute_binary_response, jeffreys_binary
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
  return( jeffreys_multinomial_factory(2)(df) )
}

#' Generate function that applies Jeffreys correction to multinomial estimates.
#'
#' @param k Number of groups.
#' 
#' @return Function to apply multinomial Jeffreys correction.
#' @importFrom dplyr mutate
jeffreys_multinomial_factory <- function(k) {
  # Apply a Jeffreys correction to multinomial estimates and their standard errors.
  #
  # Param df: Data frame
  # Returns: Updated data frame.
  jeffreys_multinomial <- function(df) {
    return(mutate(df,
                  val = jeffreys_percentage(.data$val, .data$sample_size, k),
                  se = binary_se(.data$val, .data$sample_size)))
  }
  
  return(jeffreys_multinomial)
}

#' Adjust a multinomial percentage estimate using the Jeffreys method.
#'
#' Takes a previously estimated percentage (calculated with num_group1 / total *
#' 100) and replaces it with the Jeffreys version, where one pseudo-observation
#' with 1/k mass in each group is inserted.
#'
#' @param percentage Vector of percentages to adjust.
#' @param sample_size Vector of corresponding sample sizes.
#' @param k Number of groups.
#' 
#' @return Vector of adjusted percentages.
jeffreys_percentage <- function(percentage, sample_size, k) {
  return((percentage * sample_size + 100/k) / (sample_size + 1))
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
