## Functions to code responses in the raw input data, called from
## `load_response_one`. Since these are called in `load_response_one`, which
## reads one specific Qualtrics file, their input data is always from only one
## wave of the survey -- they do not deal with inputs that have multiple waves
## mingled in one data frame.

#' Split multiselect options into codable form
#'
#' Multiselect options are coded by Qualtrics as a comma-separated string of
#' selected options, like "1,14", or the empty string if no options are
#' selected. Split these into vectors of selected options.
#'
#' @param column vector of selections, like c("1,4", "5", ...)
#' @return list of same length, each entry of which is a vector of selected
#'   options
split_options <- function(column) {
  return(strsplit(column, ",", fixed = TRUE))
}

#' Test if a specific selection is selected
#'
#' Checking whether a specific selection is selected in either "" (empty
#' string) or `NA` responses will produce `NA`s.
#'
#' @param vec A list whose entries are character vectors, such as c("14", "15").
#' @param selection one string, such as "14"
#' @return a logical vector; for each list entry, whether selection is contained
#'   in the character vector.
#'   
#' @importFrom parallel mclapply
is_selected <- function(vec, selection) {
  map_fn <- ifelse( is.null(getOption("mc.cores")) , lapply, mclapply)
  selections <- unlist(map_fn(
    vec,
    function(resp) {
      if (length(resp) == 0 || all(is.na(resp))) {
        # Qualtrics files code no selection as "" (empty string), which is
        # parsed by `read_csv` as `NA` (missing) by default. Since all our
        # selection items include "None of the above" or similar, treat both no
        # selection ("") or missing (NA) as missing, for generality.
        NA
      } else {
        selection %in% resp
      }
    }))

  return(selections)
}

#' Activities outside the home
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#' 
#' @return data frame augmented with `a_work_outside_home_1d`, `a_shop_1d`,
#'   `a_restaurant_1d`, `a_spent_time_1d`, `a_large_event_1d`,
#'   `a_public_transit_1d`
code_activities <- function(input_data, wave) {
  if ("C13" %in% names(input_data)) {
    # introduced in wave 4
    activities <- split_options(input_data$C13)

    input_data$a_work_outside_home_1d <- is_selected(activities, "1")
    input_data$a_shop_1d <- is_selected(activities, "2")
    input_data$a_restaurant_1d <- is_selected(activities, "3")
    input_data$a_spent_time_1d <- is_selected(activities, "4")
    input_data$a_large_event_1d <- is_selected(activities, "5")
    input_data$a_public_transit_1d <- is_selected(activities, "6")
  } else {
    input_data$a_work_outside_home_1d <- NA
    input_data$a_shop_1d <- NA
    input_data$a_restaurant_1d <- NA
    input_data$a_spent_time_1d <- NA
    input_data$a_large_event_1d <- NA
    input_data$a_public_transit_1d <- NA
  }

  if ("C13b" %in% names(input_data)) {
    # introduced in wave 10 as "indoors" activities version of C13
    activities <- split_options(input_data$C13b)

    input_data$a_work_outside_home_indoors_1d <- is_selected(activities, "1")
    input_data$a_shop_indoors_1d <- is_selected(activities, "2")
    input_data$a_restaurant_indoors_1d <- is_selected(activities, "3")
    input_data$a_spent_time_indoors_1d <- is_selected(activities, "4")
    input_data$a_large_event_indoors_1d <- is_selected(activities, "5")
    input_data$a_public_transit_1d <- is_selected(activities, "6")
  } else {
    input_data$a_work_outside_home_indoors_1d <- NA
    input_data$a_shop_indoors_1d <- NA
    input_data$a_restaurant_indoors_1d <- NA
    input_data$a_spent_time_indoors_1d <- NA
    input_data$a_large_event_indoors_1d <- NA
  }

  return(input_data)
}

#' Household symptom variables
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#' 
#' @return data frame augmented with `hh_fever`, `hh_sore_throat`, `hh_cough`,
#'   `hh_short_breath`, `hh_diff_breath`, `hh_number_sick`
code_symptoms <- function(input_data, wave) {
  input_data$hh_fever <- (input_data$A1_1 == 1L)
  input_data$hh_sore_throat <- (input_data$A1_2 == 1L)
  input_data$hh_cough <- (input_data$A1_3 == 1L)
  input_data$hh_short_breath <- (input_data$A1_4 == 1L)
  input_data$hh_diff_breath <- (input_data$A1_5 == 1L)
  suppressWarnings({ input_data$hh_number_sick <- as.integer(input_data$A2) })

  return(input_data)
}

#' Household total size
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#' 
#' @return data frame augmented with `hh_number_total`
code_hh_size <- function(input_data, wave) {
  if ("A5_1" %in% names(input_data)) {
    # This is Wave 4, where item A2b was replaced with 3 items asking about
    # separate ages. Many respondents leave blank the categories that do not
    # apply to their household, rather than entering 0, so if at least one of
    # the three items has a response, we impute 0 for the remaining items.
    suppressWarnings({
      age18 <- as.integer(input_data$A5_1)
      age1864 <- as.integer(input_data$A5_2)
      age65 <- as.integer(input_data$A5_3)
    })

    input_data$hh_number_total <- ifelse(
      is.na(age18) + is.na(age1864) + is.na(age65) < 3,
      (ifelse(is.na(age18), 0, age18) +
         ifelse(is.na(age1864), 0, age1864) +
         ifelse(is.na(age65), 0, age65)),
      NA_integer_
    )
  } else {
    # This is Wave <= 4, where item A2b measured household size
    suppressWarnings({
      input_data$hh_number_total <- as.integer(input_data$A2b)
    })
  }
  return(input_data)
}

#' Mental health variables
#'
#' Per our IRB, we only aggregate these variables for wave >= 4.
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#' 
#' @return data frame augmented with `mh_worried_ill`, `mh_anxious`,
#'   `mh_depressed`, `mh_isolated`, `mh_worried_finances`
code_mental_health <- function(input_data, wave) {
  input_data$mh_worried_ill <- NA
  input_data$mh_anxious <- NA
  input_data$mh_depressed <- NA
  input_data$mh_isolated <- NA
  input_data$mh_worried_finances <- NA
  input_data$mh_anxious_7d <- NA
  input_data$mh_depressed_7d <- NA
  input_data$mh_isolated_7d <- NA
  
  if (wave >= 4 && wave < 10) {
    input_data$mh_worried_ill <- input_data$C9 == 1 | input_data$C9 == 2
    input_data$mh_anxious <- input_data$C8_1 == 3 | input_data$C8_1 == 4
    input_data$mh_depressed <- input_data$C8_2 == 3 | input_data$C8_2 == 4
    input_data$mh_isolated <- input_data$C8_3 == 3 | input_data$C8_3 == 4
    input_data$mh_worried_finances <- input_data$C15 == 1 | input_data$C15 == 2
  } else if (wave == 10) {
    input_data$mh_worried_ill <- input_data$C9 == 1 | input_data$C9 == 2
    input_data$mh_anxious_7d <- input_data$C8a_1 == 3 | input_data$C8a_1 == 4
    input_data$mh_depressed_7d <- input_data$C8a_2 == 3 | input_data$C8a_2 == 4
    input_data$mh_isolated_7d <- input_data$C8a_3 == 3 | input_data$C8a_3 == 4
    input_data$mh_worried_finances <- input_data$C15 == 1 | input_data$C15 == 2
  } else if (wave >= 11) {
    input_data$mh_anxious_7d <- input_data$C18a == 3 | input_data$C18a == 4
    input_data$mh_depressed_7d <- input_data$C18b == 3 | input_data$C18b == 4
    input_data$mh_worried_finances <- input_data$C15 == 1 | input_data$C15 == 2
  }

  if ("G1" %in% names(input_data)) {
    # added in wave 11. Count "a great deal" (1) and "a moderate amount" (2) as
    # worried.
    input_data$mh_worried_catch_covid <- case_when(
      is.na(input_data$G1) ~ NA,
      input_data$G1 == 1 ~ TRUE,
      input_data$G1 == 2 ~ TRUE,
      TRUE ~ FALSE)
  } else {
    input_data$mh_worried_catch_covid <- NA
  }
  
  return(input_data)
}

#' Mask and contact variables
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#' 
#' @return data frame augmented with `c_travel_state`, `c_work_outside_5d`,
#'   `c_mask_often`, `c_others_masked`
code_mask_contact <- function(input_data, wave) {
  # private helper for both mask items, which are identically coded: 6 means the
  # respondent was not in public, 1 & 2 mean always/most, 3-5 mean some to none
  most_always <- function(item) {
    case_when(
      is.na(item) ~ NA,
      item == 6 ~ NA,
      item == 1 | item ==  2 ~ TRUE,
      TRUE ~ FALSE)
  }
  
  if ("C6" %in% names(input_data)) {
    input_data$c_travel_state <- input_data$C6 == 1
  } else {
    input_data$c_travel_state <- NA
  }
  
  if ("C6a" %in% names(input_data)) {
    input_data$c_travel_state_7d <- input_data$C6a == 1
  } else {
    input_data$c_travel_state_7d <- NA
  }
  
  if ("C14" %in% names(input_data)) {
    # added in wave 4. wearing mask most or all of the time; exclude respondents
    # who have not been in public
    input_data$c_mask_often <- most_always(input_data$C14)
  } else {
    input_data$c_mask_often <- NA
  }

  if ("C14a" %in% names(input_data)) {
    # added in wave 8. wearing mask most or all of the time (last 7 days);
    # exclude respondents who have not been in public
    input_data$c_mask_often_7d <- most_always(input_data$C14a)
  } else {
    input_data$c_mask_often_7d <- NA
  }

  if ("C16" %in% names(input_data)) {
    # added in wave 5. most/all *others* seen in public wearing masks; exclude
    # respondents who have not been in public.
    input_data$c_others_masked <- most_always(input_data$C16)
  } else {
    input_data$c_others_masked <- NA
  }
  
  if ("H2" %in% names(input_data)) {
    # added in wave 11, replaces C16. most/all *others* seen in public wearing
    # masks; exclude respondents who have not been in public. Coding is reversed.
    input_data$c_others_masked_public <- case_when(
      is.na(input_data$H2) ~ NA,
      input_data$H2 == 6 ~ NA,
      input_data$H2 == 4 | input_data$H2 == 5 ~ TRUE,
      TRUE ~ FALSE)
  } else {
    input_data$c_others_masked_public <- NA
  }
  
  if ("H1" %in% names(input_data)) {
    # added in wave 11. most/all *others* in public in the last 7 days; exclude
    # respondents who have not been in public.
    input_data$c_others_distanced_public <- case_when(
      is.na(input_data$H1) ~ NA,
      input_data$H1 == 6 ~ NA,
      input_data$H1 == 4 | input_data$H1 == 5 ~ TRUE,
      TRUE ~ FALSE)
  } else {
    input_data$c_others_distanced_public <- NA
  }

  if ("C3" %in% names(input_data)) {
    input_data$c_work_outside_5d <- input_data$C3 == 1
  } else {
    input_data$c_work_outside_5d <- NA
  }
  return(input_data)
}

#' Testing and test positivity variables
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#' 
#' @return data frame augmented with `t_tested_14d`, `t_tested_positive_14d`,
#'   `t_wanted_test_14d`
code_testing <- function(input_data, wave) {
  if ( all(c("B8", "B10") %in% names(input_data)) ) {
    # fraction tested in last 14 days. yes == 1 on B10; no == 2 on B8 *or* 3 on
    # B10 (which codes "no" as 3 for some reason)
    input_data$t_tested_14d <- case_when(
      input_data$B8 == 2 | input_data$B10 == 3 ~ 0,
      input_data$B10 == 1 ~ 1,
      TRUE ~ NA_real_
    )
  } else if ("B10" %in% names(input_data) && !("B8" %in% names(input_data))) {
      # fraction tested in last 14 days. yes == 1 on B10; no == 3 on B10 (which
      # codes "no" as 3 for some reason)
      input_data$t_tested_14d <- case_when(
        input_data$B10 == 3 ~ 0,
        input_data$B10 == 1 ~ 1,
        TRUE ~ NA_real_
      )
  } else {
    input_data$t_tested_14d <- NA_real_
  }
  
  if ("B12" %in% names(input_data)) {
    # fraction, of those not tested in past 14 days, who wanted to be tested but
    # were not
    input_data$t_wanted_test_14d <- input_data$B12 == 1
  } else {
    input_data$t_wanted_test_14d <- NA
  }
  
  # fraction, of those tested in past 14 days, who tested positive. yes == 1
  # on B10a/c, no == 2 on B10a/c; option 3 is "I don't know", which is excluded
  if ("B10a" %in% names(input_data)) {
    input_data$t_tested_positive_14d <- case_when(
      input_data$B10a == 1 ~ 1, # yes
      input_data$B10a == 2 ~ 0, # no
      input_data$B10a == 3 ~ NA_real_, # I don't know
      TRUE ~ NA_real_
    )
  } else if ("B10c" %in% names(input_data)) {
    input_data$t_tested_positive_14d <- case_when(
      input_data$B10c == 1 ~ 1, # yes
      input_data$B10c == 2 ~ 0, # no
      input_data$B10c == 3 ~ NA_real_, # I don't know
      TRUE ~ NA_real_
    )
  } else {
    input_data$t_tested_positive_14d <- NA_real_
  }

  if ( "B10b" %in% names(input_data) ) {
    testing_reasons <- split_options(input_data$B10b)
    
    input_data$t_tested_reason_sick <- is_selected(testing_reasons, "1")
    input_data$t_tested_reason_contact <- is_selected(testing_reasons, "2")
    input_data$t_tested_reason_medical <- is_selected(testing_reasons, "3")
    input_data$t_tested_reason_employer <- is_selected(testing_reasons, "4")
    input_data$t_tested_reason_large_event <- is_selected(testing_reasons, "5")
    input_data$t_tested_reason_crowd <- is_selected(testing_reasons, "6")
    input_data$t_tested_reason_visit_fam <- is_selected(testing_reasons, "7")
    input_data$t_tested_reason_other <- is_selected(testing_reasons, "8")
    input_data$t_tested_reason_travel <- is_selected(testing_reasons, "9")
    
    if (wave >= 11) {
      input_data$t_tested_reason_large_event <- NA
      input_data$t_tested_reason_crowd <- NA
    }
        
    input_data$t_tested_reason_screening <- case_when(
      input_data$t_tested_reason_sick == TRUE ~ 0,
      input_data$t_tested_reason_contact == TRUE ~ 0,
      input_data$t_tested_reason_crowd == TRUE ~ 0,
      
      input_data$t_tested_reason_medical == TRUE ~ 1,
      input_data$t_tested_reason_employer == TRUE ~ 1,
      input_data$t_tested_reason_large_event == TRUE ~ 1,
      input_data$t_tested_reason_visit_fam == TRUE ~ 1,
      input_data$t_tested_reason_travel == TRUE ~ 1,
      
      !is.na(input_data$B10b) ~ 0,
      TRUE ~ NA_real_
    )
    
    input_data$t_screening_tested_positive_14d <- case_when(
      input_data$t_tested_reason_screening == 1 ~ input_data$t_tested_positive_14d,
      TRUE ~ NA_real_
    )
  } else {
    input_data$t_screening_tested_positive_14d <- NA_real_
  }
  
  if ("B13" %in% names(input_data)) {
    input_data$t_had_covid_ever <- input_data$B13 == 1
  } else {
    input_data$t_had_covid_ever <- NA
  }
  
  return(input_data)
}

#' COVID vaccination variables
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#' 
#' @return data frame augmented with `v_covid_vaccinated` and
#'   `v_accept_covid_vaccine`
#'
#' @importFrom dplyr coalesce
code_vaccines <- function(input_data, wave) {
  if ("V1" %in% names(input_data)) {
    # coded as 1 = Yes, 2 = No, 3 = don't know. We assume that don't know = no,
    # because, well, you'd know.
    input_data$v_covid_vaccinated <- case_when(
      input_data$V1 == 1 ~ 1,
      input_data$V1 == 2 ~ 0,
      input_data$V1 == 3 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$v_covid_vaccinated <- NA_real_
  }

  if ("V2" %in% names(input_data)) {
    # coded as 1 = 1 dose/vaccination, 2 = 2 doses, 3 = don't know.
    input_data$v_received_2_vaccine_doses <- case_when(
      input_data$V2 == 1 ~ 0,
      input_data$V2 == 2 ~ 1,
      input_data$V2 == 3 ~ NA_real_,
      TRUE ~ NA_real_
    )
  } else {
    input_data$v_received_2_vaccine_doses <- NA_real_
  }

  input_data$v_accept_covid_vaccine <- NA
  input_data$v_appointment_or_accept_covid_vaccine <- NA_real_
  input_data$v_accept_covid_vaccine_no_appointment <- NA
  if ("V3" %in% names(input_data)) {
    input_data$v_accept_covid_vaccine <- (
      input_data$V3 == 1 | input_data$V3 == 2
    )
  } else if ( all(c("V3a", "V11a") %in% names(input_data)) ) {
    input_data$v_accept_covid_vaccine_no_appointment <- (
      input_data$V3a == 1 | input_data$V3a == 2
    )
    
    input_data$v_appointment_or_accept_covid_vaccine <- case_when(
      input_data$V11a == 1 ~ 1,
      input_data$V3a == 1 ~ 1,
      input_data$V3a == 2 ~ 1,
      input_data$V3a == 3 ~ 0,
      input_data$V3a == 4 ~ 0,
      TRUE ~ NA_real_
    )
  }

  input_data$v_covid_vaccinated_or_accept <- NA_real_
  input_data$v_covid_vaccinated_appointment_or_accept <- NA_real_
  if ( all(c("V1", "V3") %in% names(input_data)) ) {
    # "acceptance plus" means you either
    # - already have the vaccine (V1 == 1), or
    # - would get it if offered (V3 == 1 or 2)
    input_data$v_covid_vaccinated_or_accept <- case_when(
      input_data$V1 == 1 ~ 1,
      input_data$V3 == 1 ~ 1,
      input_data$V3 == 2 ~ 1,
      input_data$V3 == 3 ~ 0,
      input_data$V3 == 4 ~ 0,
      TRUE ~ NA_real_
    )
  } else if ( all(c("V1", "V3a", "V11a") %in% names(input_data)) ) {
    # Starting in Wave 11, only if a respondent does not have an appointment to
    # get a vaccine are they asked if they would get a vaccine (V3a).
    # "acceptance plus" means you either
    # - already have the vaccine (V1 == 1), or
    # - have an appointment to get it (V11a == 1), or
    # - would get it if offered (V3a == 1 or 2)
    input_data$v_covid_vaccinated_appointment_or_accept <- case_when(
      input_data$V1 == 1 ~ 1,
      input_data$V11a == 1 ~ 1,
      input_data$V3a == 1 ~ 1,
      input_data$V3a == 2 ~ 1,
      input_data$V3a == 3 ~ 0,
      input_data$V3a == 4 ~ 0,
      TRUE ~ NA_real_
    )
  }
  
  if ("V11a" %in% names(df)) {
    # Have an appointment to get vaccinated conditional on not being vaccinated.
    input_data$v_appointment_not_vaccinated <- df$V11a == 1
  } else {
    input_data$v_appointment_not_vaccinated <- NA  
  }

  if ("V4_1" %in% names(input_data)) {
    input_data$v_vaccine_likely_friends <- input_data$V4_1 == 1
    input_data$v_vaccine_likely_who <- input_data$V4_3 == 1
    input_data$v_vaccine_likely_govt_health <- input_data$V4_4 == 1
    input_data$v_vaccine_likely_politicians <- input_data$V4_5 == 1
    
    if (wave < 8) {
      input_data$v_vaccine_likely_local_health <- input_data$V4_2 == 1
      input_data$v_vaccine_likely_doctors <- NA_real_
    } else {
      input_data$v_vaccine_likely_local_health <- NA_real_
      input_data$v_vaccine_likely_doctors <- input_data$V4_2 == 1
    }
    
  } else {
    input_data$v_vaccine_likely_friends <- NA_real_
    input_data$v_vaccine_likely_local_health <- NA_real_
    input_data$v_vaccine_likely_who <- NA_real_
    input_data$v_vaccine_likely_govt_health <- NA_real_
    input_data$v_vaccine_likely_politicians <- NA_real_
    input_data$v_vaccine_likely_doctors <- NA_real_
  }
  
  # Close analogues to `v_vaccine_likely_*` as of Wave 11.
  if ( all(c("I6_1", "I6_2", "I6_3", "I6_4", "I6_5", "I6_6", "I6_7", "I6_8") %in% names(input_data)) ) {
    input_data$i_trust_covid_info_doctors <- input_data$I6_1 == 3
    input_data$i_trust_covid_info_experts <- input_data$I6_2 == 3
    input_data$i_trust_covid_info_cdc <- input_data$I6_3 == 3
    input_data$i_trust_covid_info_govt_health <- input_data$I6_4 == 3
    input_data$i_trust_covid_info_politicians <- input_data$I6_5 == 3
    input_data$i_trust_covid_info_journalists <- input_data$I6_6 == 3
    input_data$i_trust_covid_info_friends <- input_data$I6_7 == 3
    input_data$i_trust_covid_info_religious <- input_data$I6_8 == 3
  } else {
    input_data$i_trust_covid_info_doctors <- NA
    input_data$i_trust_covid_info_experts <- NA
    input_data$i_trust_covid_info_cdc <- NA
    input_data$i_trust_covid_info_govt_health <- NA
    input_data$i_trust_covid_info_politicians <- NA
    input_data$i_trust_covid_info_journalists <- NA
    input_data$i_trust_covid_info_friends <- NA
    input_data$i_trust_covid_info_religious <- NA
  }
  
  if ("V5a" %in% names(input_data) && "V5b" %in% names(input_data) && "V5c" %in% names(input_data)) {
    # introduced in Wave 8
    hesitancy_reasons <- coalesce(input_data$V5a, input_data$V5b, input_data$V5c)
    hesitancy_reasons <- split_options(hesitancy_reasons)

    input_data$v_hesitancy_reason_sideeffects <- is_selected(hesitancy_reasons, "1")
    input_data$v_hesitancy_reason_allergic <- is_selected(hesitancy_reasons, "2")
    input_data$v_hesitancy_reason_ineffective <- is_selected(hesitancy_reasons, "3")
    input_data$v_hesitancy_reason_unnecessary <- is_selected(hesitancy_reasons, "4")
    input_data$v_hesitancy_reason_dislike_vaccines <- is_selected(hesitancy_reasons, "5")
    input_data$v_hesitancy_reason_not_recommended <- is_selected(hesitancy_reasons, "6")
    input_data$v_hesitancy_reason_wait_safety <- is_selected(hesitancy_reasons, "7")
    input_data$v_hesitancy_reason_low_priority <- is_selected(hesitancy_reasons, "8")
    input_data$v_hesitancy_reason_cost <- is_selected(hesitancy_reasons, "9")
    input_data$v_hesitancy_reason_distrust_vaccines <- is_selected(hesitancy_reasons, "10")
    input_data$v_hesitancy_reason_distrust_gov <- is_selected(hesitancy_reasons, "11")
    input_data$v_hesitancy_reason_health_condition <- is_selected(hesitancy_reasons, "12")
    input_data$v_hesitancy_reason_other <- is_selected(hesitancy_reasons, "13")
    input_data$v_hesitancy_reason_pregnant <- is_selected(hesitancy_reasons, "14")
    input_data$v_hesitancy_reason_religious <- is_selected(hesitancy_reasons, "15")

    if (wave >= 11) {
      input_data$v_hesitancy_reason_allergic <- NA
      input_data$v_hesitancy_reason_not_recommended <- NA
      input_data$v_hesitancy_reason_distrust_vaccines <- NA
      input_data$v_hesitancy_reason_health_condition <- NA
      input_data$v_hesitancy_reason_pregnant <- NA
    }
    
  } else {
    input_data$v_hesitancy_reason_sideeffects <- NA_real_
    input_data$v_hesitancy_reason_allergic <- NA_real_
    input_data$v_hesitancy_reason_ineffective <- NA_real_
    input_data$v_hesitancy_reason_unnecessary <- NA_real_
    input_data$v_hesitancy_reason_dislike_vaccines <- NA_real_
    input_data$v_hesitancy_reason_not_recommended <- NA_real_
    input_data$v_hesitancy_reason_wait_safety <- NA_real_
    input_data$v_hesitancy_reason_low_priority <- NA_real_
    input_data$v_hesitancy_reason_cost <- NA_real_
    input_data$v_hesitancy_reason_distrust_vaccines <- NA_real_
    input_data$v_hesitancy_reason_distrust_gov <- NA_real_
    input_data$v_hesitancy_reason_health_condition <- NA_real_
    input_data$v_hesitancy_reason_other <- NA_real_
    input_data$v_hesitancy_reason_pregnant <- NA_real_
    input_data$v_hesitancy_reason_religious <- NA_real_
  }
  
  if ( "V6" %in% names(input_data) ) {
    # introduced in Wave 8
    dontneed_reasons <- split_options(input_data$V6)
    
    input_data$v_dontneed_reason_had_covid <- is_selected(dontneed_reasons, "1")
    input_data$v_dontneed_reason_dont_spend_time <- is_selected(dontneed_reasons, "2")
    input_data$v_dontneed_reason_not_high_risk <- is_selected(dontneed_reasons, "3")
    input_data$v_dontneed_reason_precautions <- is_selected(dontneed_reasons, "4")
    input_data$v_dontneed_reason_not_serious <- is_selected(dontneed_reasons, "5")
    input_data$v_dontneed_reason_not_beneficial <- is_selected(dontneed_reasons, "7")
    input_data$v_dontneed_reason_other <- is_selected(dontneed_reasons, "8")
    
  } else {
    input_data$v_dontneed_reason_had_covid <- NA
    input_data$v_dontneed_reason_dont_spend_time <- NA
    input_data$v_dontneed_reason_not_high_risk <- NA
    input_data$v_dontneed_reason_precautions <- NA
    input_data$v_dontneed_reason_not_serious <- NA
    input_data$v_dontneed_reason_not_beneficial <- NA
    input_data$v_dontneed_reason_other <- NA
  }

  if ("V9" %in% names(input_data)) {
    input_data$v_worried_vaccine_side_effects <- (
      input_data$V9 == 1 | input_data$V9 == 2
    )
  } else {
    input_data$v_worried_vaccine_side_effects <- NA_real_
  }

  if ( all(c("V15a", "V15b") %in% names(input_data)) ) {
    # introduced in Wave 11
    vaccine_barriers <- coalesce(input_data$V15a, input_data$V15b)
    vaccine_barriers <- ifelse(vaccine_barriers == "13", NA, vaccine_barriers)
    vaccine_barriers <- split_options(vaccine_barriers)
    
    input_data$v_vaccine_barrier_eligible <- is_selected(vaccine_barriers, "1")
    input_data$v_vaccine_barrier_no_appointments <- is_selected(vaccine_barriers, "2")
    input_data$v_vaccine_barrier_appointment_time <- is_selected(vaccine_barriers, "3")
    input_data$v_vaccine_barrier_technical_difficulties <- is_selected(vaccine_barriers, "4")
    input_data$v_vaccine_barrier_document <- is_selected(vaccine_barriers, "5")
    input_data$v_vaccine_barrier_technology_access <- is_selected(vaccine_barriers, "6")
    input_data$v_vaccine_barrier_travel <- is_selected(vaccine_barriers, "7")
    input_data$v_vaccine_barrier_language <- is_selected(vaccine_barriers, "8")
    input_data$v_vaccine_barrier_childcare <- is_selected(vaccine_barriers, "9")
    input_data$v_vaccine_barrier_time <- is_selected(vaccine_barriers, "10")
    input_data$v_vaccine_barrier_type <- is_selected(vaccine_barriers, "12")
    input_data$v_vaccine_barrier_none <- is_selected(vaccine_barriers, "11")
  } else {
    input_data$v_vaccine_barrier_eligible <- NA
    input_data$v_vaccine_barrier_no_appointments <- NA
    input_data$v_vaccine_barrier_appointment_time <- NA
    input_data$v_vaccine_barrier_technical_difficulties <- NA
    input_data$v_vaccine_barrier_document <- NA
    input_data$v_vaccine_barrier_technology_access <- NA
    input_data$v_vaccine_barrier_travel <- NA
    input_data$v_vaccine_barrier_language <- NA
    input_data$v_vaccine_barrier_childcare <- NA
    input_data$v_vaccine_barrier_time <- NA
    input_data$v_vaccine_barrier_type <- NA
    input_data$v_vaccine_barrier_none <- NA
  }
  
  if ( "E4" %in% names(input_data) ) {
    # introduced in Wave 11
    input_data$v_vaccinate_children <- case_when(
      input_data$E4 == 1 ~ 1,
      input_data$E4 == 2 ~ 1,
      input_data$E4 == 3 ~ 0,
      input_data$E4 == 4 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$v_vaccinate_children <- NA_real_
  }
  
  if ( "V16" %in% names(input_data) ) {
    # introduced in Wave 11
    input_data$v_try_vaccinate_1m <- case_when(
      input_data$V16 %in% c(1, 2) ~ 1,
      input_data$V16 %in% c(3, 4, 5, 6, 7) ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$v_try_vaccinate_1m <- NA_real_
  }
  
  if ("H3" %in% names(input_data)) {
    # added in wave 11. Coded as 1 = none, 2 = a few people, 3 = some people, 
    # 4 = most people, 6 = all of the people.
    input_data$v_covid_vaccinated_friends <- case_when(
      is.na(input_data$H3) ~ NA,
      input_data$H3 == 4 | input_data$H3 == 6 ~ TRUE,
      TRUE ~ FALSE)
  } else {
    input_data$v_covid_vaccinated_friends <- NA
  }
  
  return(input_data)
}

#' Schooling
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#' 
#' @return data frame augmented with `hh_number_total`
code_schooling <- function(input_data, wave) {
  if ("E2_1" %in% names(input_data)) {
    # Coded as 2 = "Yes", 3 = "No", 4 = "I don't know"
    input_data$s_inperson_school_fulltime <- case_when(
      input_data$E2_1 == 2 ~ 1,
      input_data$E2_1 == 3 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$s_inperson_school_fulltime <- NA_real_
  }
  
  if ("E2_2" %in% names(input_data)) {
    # Coded as 2 = "Yes", 3 = "No", 4 = "I don't know"
    input_data$s_inperson_school_parttime <- case_when(
      input_data$E2_2 == 2 ~ 1,
      input_data$E2_2 == 3 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$s_inperson_school_parttime <- NA_real_
  }
  return(input_data)
}

#' Beliefs
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#' 
#' @return augmented data frame
code_beliefs <- function(input_data, wave) {
  if ("G2" %in% names(input_data)) {
    # added in wave 11.
    input_data$b_belief_distancing_effective <- case_when(
      input_data$G2 == 1 | input_data$G2 == 2 ~ 1,
      input_data$G2 == 3 | input_data$G2 == 4 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$b_belief_distancing_effective <- NA_real_
  }
  
  if ("G3" %in% names(input_data)) {
    # added in wave 11.
    input_data$b_belief_masking_effective <- case_when(
      input_data$G3 == 1 | input_data$G3 == 2 ~ 1,
      input_data$G3 == 3 | input_data$G3 == 4 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$b_belief_masking_effective <- NA_real_
  }
  
  return(input_data)
}
