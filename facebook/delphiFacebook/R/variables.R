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
  if ( any(!is.na(column)) ) {
    return(strsplit(column, ",", fixed = TRUE))
  } else {
    return(rep(list(NA_character_), length(column)))
  }
}

#' Test if a specific selection is selected
#'
#' Checking whether a specific selection is selected in either "" (empty
#' string) or `NA` responses will produce `NA`s.
#'
#' @param vec A list whose entries are character vectors, such as c("14", "15").
#' @param selection one string, such as "14"
#' @param use_cpp boolean indicating whether to use the C++ verion or the R
#'   version of this function
#' 
#' @return a logical vector; for each list entry, whether selection is contained
#'   in the character vector.
#'   
#' @importFrom Rcpp evalCpp
#' @useDynLib delphiFacebook, .registration = TRUE
is_selected <- function(vec, selection, use_cpp=TRUE) {
  select_fn <- ifelse(use_cpp, is_selected_cpp, is_selected_r)
  return(select_fn(vec, selection))
}

#' Test if a specific selection is selected, R implementation
#'
#' Checking whether a specific selection is selected in either "" (empty
#' string) or `NA` responses will produce `NA`s. Looks only at unique values in
#' the input vector.
#'
#' @param vec A list whose entries are character vectors, such as c("14", "15").
#' @param selection one string, such as "14"
#' 
#' @return a logical vector; for each list entry, whether selection is contained
#'   in the character vector.
is_selected_r <- function(vec, selection) {
  vec_unique <- unique(vec)
  
  selections <- unlist(lapply(
    vec_unique,
    function(resp) {
      if (length(resp) == 0 || all(is.na(resp))) {
        # Qualtrics files code no selection as "" (empty string), which is
        # parsed by `read_csv` as `NA` (missing) by default. Since all our
        # selection items include "None of the above" or similar, treat both no
        # selection ("") or missing (NA) as missing, for generality.
        NA
      } else {
        any(resp == selection)
      }
    }))
  
  names(selections) <- vec_unique
  names(vec) <- vec
  
  return( as.logical(selections[names(vec)]) )
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

  if ("Q36" %in% names(input_data)) {
    # Included in waves 1, 2, 3. Coded as 1 = substantial threat,
    # 2 = moderate threat, 3 = not much of a threat, 4 = not a threat at all
    input_data$mh_financial_threat <- case_when(
      is.na(input_data$Q36) ~ NA,
      input_data$Q36 == 1 ~ TRUE,
      TRUE ~ FALSE)
  } else {
    input_data$mh_financial_threat <- NA
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
      item == 1 | item == 2 ~ TRUE,
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
    # Same indicator but include "sometimes" wear mask in public
    input_data$c_mask_some_often_7d <- case_when(
      is.na(input_data$C14a) ~ NA,
      input_data$C14a == 6 ~ NA,
      input_data$C14a == 1 | input_data$C14a == 2 | input_data$C14a == 3 ~ TRUE,
      TRUE ~ FALSE)

  } else {
    input_data$c_mask_often_7d <- NA
    input_data$c_mask_some_often_7d <- NA
  }

  if ("C16" %in% names(input_data)) {
    # added in wave 5. most/all *others* seen in public wearing masks; exclude
    # respondents who have not been in public.
    # Coded as 5 = no people in public are wearing masks, 4 = a few people are,
    # 3 = some people are, 2 = most people are, 1 = all people are, 6 = I have not been in public
    input_data$c_others_masked <- most_always(input_data$C16)
    # include others in public are masked "sometimes"
    input_data$c_others_some_masked <- case_when(
      is.na(input_data$C16) ~ NA,
      input_data$C16 == 6 ~ NA,
      input_data$C16 == 1 | input_data$C16 == 2 | input_data$C16 == 3 ~ TRUE,
      TRUE ~ FALSE)
  } else {
    input_data$c_others_masked <- NA
    input_data$c_others_some_masked <- NA
  }
  
  if ("H2" %in% names(input_data)) {
    # added in wave 11, replaces C16. most/all *others* seen in public wearing
    # masks; exclude respondents who have not been in public. Coding is reversed.
    # Coded as 1 = no people in public are wearing masks, 2 = a few people are,
    # 3 = some people are, 4 = most people are, 5 = all people are, 6 = I have not been in public
    input_data$c_others_masked_public <- case_when(
      is.na(input_data$H2) ~ NA,
      input_data$H2 == 6 ~ NA,
      input_data$H2 == 4 | input_data$H2 == 5 ~ TRUE,
      TRUE ~ FALSE)
    input_data$c_others_masked_some_public <- case_when(
      is.na(input_data$H2) ~ NA,
      input_data$H2 == 6 ~ NA,
      input_data$H2 == 4 | input_data$H2 == 5 | input_data$H2 == 3 ~ TRUE,
      TRUE ~ FALSE)
  } else {
    input_data$c_others_masked_public <- NA
    input_data$c_others_masked_some_public <- NA
  }
  
  if ("H1" %in% names(input_data)) {
    # added in wave 11. most/all *others* in public in the last 7 days; exclude
    # respondents who have not been in public.
    # Coded as 1 = no people in public are distancing, 2 = a few people are,
    # 3 = some people are, 4 = most people are, 5 = all people are, 6 = I have not been in public
    input_data$c_others_distanced_public <- case_when(
      is.na(input_data$H1) ~ NA,
      input_data$H1 == 6 ~ NA,
      input_data$H1 == 4 | input_data$H1 == 5 ~ TRUE,
      TRUE ~ FALSE)
    # include others in public are distanced "sometimes"
    input_data$c_others_distanced_some_public <- case_when(
      is.na(input_data$H1) ~ NA,
      input_data$H1 == 6 ~ NA,
      input_data$H1 == 4 | input_data$H1 == 5 | input_data$H1 == 3 ~ TRUE,
      TRUE ~ FALSE)
  } else {
    input_data$c_others_distanced_public <- NA
    input_data$c_others_distanced_some_public <- NA
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
    input_data$t_tested_reason_visit <- is_selected(testing_reasons, "7")
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
      input_data$t_tested_reason_visit == TRUE ~ 1,
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
    
    input_data$t_tested_reason_sick <- NA
    input_data$t_tested_reason_contact <- NA
    input_data$t_tested_reason_medical <- NA
    input_data$t_tested_reason_employer <- NA
    input_data$t_tested_reason_large_event <- NA
    input_data$t_tested_reason_crowd <- NA
    input_data$t_tested_reason_visit <- NA
    input_data$t_tested_reason_other <- NA
    input_data$t_tested_reason_travel <- NA
  }
  
  if ("B13" %in% names(input_data)) {
    input_data$t_had_covid_ever <- input_data$B13 == 1
  } else if ("B13a" %in% names(input_data)) {
    # B13a, replacing B13 as of Wave 12, removes "As far as you know" wording.
    input_data$t_had_covid_ever <- input_data$B13a == 1
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
  
  if ("V11a" %in% names(input_data)) {
    # Have an appointment to get vaccinated conditional on not being vaccinated.
    input_data$v_appointment_not_vaccinated <- input_data$V11a == 1
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
  
  if ("V5a" %in% names(input_data) && "V5b" %in% names(input_data) && "V5c" %in% names(input_data)) {
    # introduced in Wave 8
    hesitancy_reasons <- coalesce(input_data$V5a, input_data$V5b, input_data$V5c)
    hesitancy_reasons <- split_options(hesitancy_reasons)

    input_data$v_hesitancy_reason_sideeffects <- is_selected(hesitancy_reasons, "1")
    input_data$v_hesitancy_reason_allergic <- is_selected(hesitancy_reasons, "2") # removed as of Wave 11
    input_data$v_hesitancy_reason_ineffective <- is_selected(hesitancy_reasons, "3")
    input_data$v_hesitancy_reason_unnecessary <- is_selected(hesitancy_reasons, "4")
    input_data$v_hesitancy_reason_dislike_vaccines <- is_selected(hesitancy_reasons, "5") # removed as of Wave 12
    input_data$v_hesitancy_reason_not_recommended <- is_selected(hesitancy_reasons, "6") # removed as of Wave 11
    input_data$v_hesitancy_reason_wait_safety <- is_selected(hesitancy_reasons, "7")
    input_data$v_hesitancy_reason_low_priority <- is_selected(hesitancy_reasons, "8")
    input_data$v_hesitancy_reason_cost <- is_selected(hesitancy_reasons, "9")
    input_data$v_hesitancy_reason_distrust_vaccines <- is_selected(hesitancy_reasons, "10") # removed in Wave 11, reintroduced as of Wave 12
    input_data$v_hesitancy_reason_distrust_gov <- is_selected(hesitancy_reasons, "11")
    input_data$v_hesitancy_reason_health_condition <- is_selected(hesitancy_reasons, "12") # removed as of Wave 11
    input_data$v_hesitancy_reason_other <- is_selected(hesitancy_reasons, "13")
    input_data$v_hesitancy_reason_pregnant <- is_selected(hesitancy_reasons, "14") # removed as of Wave 11
    input_data$v_hesitancy_reason_religious <- is_selected(hesitancy_reasons, "15")
    input_data$v_hesitancy_reason_dislike_vaccines_generally <- is_selected(hesitancy_reasons, "16") # replacing choice 5 as of Wave 12

    # For waves before a given response choice existed, explicitly set the
    # derived field to missing since `is_selected` will return FALSE (meaning
    # "not selected") for them if the respondent selected at least once answer
    # choice.
    if (wave >= 11) {
      input_data$v_hesitancy_reason_allergic <- NA
      input_data$v_hesitancy_reason_not_recommended <- NA
      input_data$v_hesitancy_reason_health_condition <- NA
      input_data$v_hesitancy_reason_pregnant <- NA
    }
    if (wave == 11) {
      input_data$v_hesitancy_reason_distrust_vaccines <- NA
    }
    if (wave < 12) {
      input_data$v_hesitancy_reason_dislike_vaccines_generally <- NA
    }
    if (wave >= 12) {
      input_data$v_hesitancy_reason_dislike_vaccines <- NA
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
    input_data$v_hesitancy_reason_dislike_vaccines_generally <- NA_real_
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

  
  # Wave  V15a  V15b  V15c
  # 11    Yes   Yes   No
  # 12    No    Yes   Yes
  #
  # V15c replaces V15a as of Wave 12
  if ( all(c("V15a", "V15b") %in% names(input_data)) ) {
    # introduced in Wave 11
    vaccine_barriers <- coalesce(input_data$V15a, input_data$V15b)
    vaccine_barriers <- ifelse(vaccine_barriers == "13", NA_character_, vaccine_barriers)
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

    input_data$v_vaccine_barrier_appointment_location <- NA
    input_data$v_vaccine_barrier_other <- NA
  } else if ( all(c("V15c", "V15b") %in% names(input_data)) ) {
    # V15c introduced in Wave 12, replacing V15a with clarified wording.
    vaccine_barriers <- coalesce(input_data$V15c, input_data$V15b)
    vaccine_barriers <- ifelse(vaccine_barriers == "13", NA_character_, vaccine_barriers)
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
    input_data$v_vaccine_barrier_appointment_location <- is_selected(vaccine_barriers, "14")
    input_data$v_vaccine_barrier_other <- is_selected(vaccine_barriers, "15")
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

    input_data$v_vaccine_barrier_appointment_location <- NA
    input_data$v_vaccine_barrier_other <- NA
  }
  
  if ( "V15a" %in% names(input_data) ) {
    # introduced in Wave 11
    vaccine_barriers <- split_options(input_data$V15a)
    
    input_data$v_vaccine_barrier_eligible_has <- is_selected(vaccine_barriers, "1")
    input_data$v_vaccine_barrier_no_appointments_has <- is_selected(vaccine_barriers, "2")
    input_data$v_vaccine_barrier_appointment_time_has <- is_selected(vaccine_barriers, "3")
    input_data$v_vaccine_barrier_technical_difficulties_has <- is_selected(vaccine_barriers, "4")
    input_data$v_vaccine_barrier_document_has <- is_selected(vaccine_barriers, "5")
    input_data$v_vaccine_barrier_technology_access_has <- is_selected(vaccine_barriers, "6")
    input_data$v_vaccine_barrier_travel_has <- is_selected(vaccine_barriers, "7")
    input_data$v_vaccine_barrier_language_has <- is_selected(vaccine_barriers, "8")
    input_data$v_vaccine_barrier_childcare_has <- is_selected(vaccine_barriers, "9")
    input_data$v_vaccine_barrier_time_has <- is_selected(vaccine_barriers, "10")
    input_data$v_vaccine_barrier_type_has <- is_selected(vaccine_barriers, "12")
    input_data$v_vaccine_barrier_none_has <- is_selected(vaccine_barriers, "11")

    input_data$v_vaccine_barrier_appointment_location_has <- NA
    input_data$v_vaccine_barrier_other_has <- NA
  } else if ( "V15c" %in% names(input_data) ) {
    # V15c introduced in Wave 12, replacing V15a with clarified wording.
    vaccine_barriers <- split_options(input_data$V15c)
    
    input_data$v_vaccine_barrier_eligible_has <- is_selected(vaccine_barriers, "1")
    input_data$v_vaccine_barrier_no_appointments_has <- is_selected(vaccine_barriers, "2")
    input_data$v_vaccine_barrier_appointment_time_has <- is_selected(vaccine_barriers, "3")
    input_data$v_vaccine_barrier_technical_difficulties_has <- is_selected(vaccine_barriers, "4")
    input_data$v_vaccine_barrier_document_has <- is_selected(vaccine_barriers, "5")
    input_data$v_vaccine_barrier_technology_access_has <- is_selected(vaccine_barriers, "6")
    input_data$v_vaccine_barrier_travel_has <- is_selected(vaccine_barriers, "7")
    input_data$v_vaccine_barrier_language_has <- is_selected(vaccine_barriers, "8")
    input_data$v_vaccine_barrier_childcare_has <- is_selected(vaccine_barriers, "9")
    input_data$v_vaccine_barrier_time_has <- is_selected(vaccine_barriers, "10")
    input_data$v_vaccine_barrier_type_has <- is_selected(vaccine_barriers, "12")
    input_data$v_vaccine_barrier_none_has <- is_selected(vaccine_barriers, "11")
    input_data$v_vaccine_barrier_appointment_location_has <- is_selected(vaccine_barriers, "14")
    input_data$v_vaccine_barrier_other_has <- is_selected(vaccine_barriers, "15")
  } else {
    input_data$v_vaccine_barrier_eligible_has <- NA
    input_data$v_vaccine_barrier_no_appointments_has <- NA
    input_data$v_vaccine_barrier_appointment_time_has <- NA
    input_data$v_vaccine_barrier_technical_difficulties_has <- NA
    input_data$v_vaccine_barrier_document_has <- NA
    input_data$v_vaccine_barrier_technology_access_has <- NA
    input_data$v_vaccine_barrier_travel_has <- NA
    input_data$v_vaccine_barrier_language_has <- NA
    input_data$v_vaccine_barrier_childcare_has <- NA
    input_data$v_vaccine_barrier_time_has <- NA
    input_data$v_vaccine_barrier_type_has <- NA
    input_data$v_vaccine_barrier_none_has <- NA

    input_data$v_vaccine_barrier_appointment_location_has <- NA
    input_data$v_vaccine_barrier_other_has <- NA
  }
  
  if ( "V15b" %in% names(input_data) ) {
    # introduced in Wave 11
    vaccine_barriers <- ifelse(input_data$V15b == "13", NA_character_, input_data$V15b)
    vaccine_barriers <- split_options(vaccine_barriers)

    input_data$v_vaccine_barrier_eligible_tried <- is_selected(vaccine_barriers, "1")
    input_data$v_vaccine_barrier_no_appointments_tried <- is_selected(vaccine_barriers, "2")
    input_data$v_vaccine_barrier_appointment_time_tried <- is_selected(vaccine_barriers, "3")
    input_data$v_vaccine_barrier_technical_difficulties_tried <- is_selected(vaccine_barriers, "4")
    input_data$v_vaccine_barrier_document_tried <- is_selected(vaccine_barriers, "5")
    input_data$v_vaccine_barrier_technology_access_tried <- is_selected(vaccine_barriers, "6")
    input_data$v_vaccine_barrier_travel_tried <- is_selected(vaccine_barriers, "7")
    input_data$v_vaccine_barrier_language_tried <- is_selected(vaccine_barriers, "8")
    input_data$v_vaccine_barrier_childcare_tried <- is_selected(vaccine_barriers, "9")
    input_data$v_vaccine_barrier_time_tried <- is_selected(vaccine_barriers, "10")
    input_data$v_vaccine_barrier_type_tried <- is_selected(vaccine_barriers, "12")
    input_data$v_vaccine_barrier_none_tried <- is_selected(vaccine_barriers, "11")
    input_data$v_vaccine_barrier_appointment_location_tried <- is_selected(vaccine_barriers, "14")
    input_data$v_vaccine_barrier_other_tried <- is_selected(vaccine_barriers, "15")
    
    if (wave < 12) {
      # For waves before a given response choice existed, explicitly set the
      # derived field to missing since `is_selected` will return FALSE (meaning
      # "not selected") for them if the respondent selected at least once answer
      # choice.
      input_data$v_vaccine_barrier_appointment_location_tried <- NA
      input_data$v_vaccine_barrier_other_tried <- NA
    }
  } else {
    input_data$v_vaccine_barrier_eligible_tried <- NA
    input_data$v_vaccine_barrier_no_appointments_tried <- NA
    input_data$v_vaccine_barrier_appointment_time_tried <- NA
    input_data$v_vaccine_barrier_technical_difficulties_tried <- NA
    input_data$v_vaccine_barrier_document_tried <- NA
    input_data$v_vaccine_barrier_technology_access_tried <- NA
    input_data$v_vaccine_barrier_travel_tried <- NA
    input_data$v_vaccine_barrier_language_tried <- NA
    input_data$v_vaccine_barrier_childcare_tried <- NA
    input_data$v_vaccine_barrier_time_tried <- NA
    input_data$v_vaccine_barrier_type_tried <- NA
    input_data$v_vaccine_barrier_none_tried <- NA
    input_data$v_vaccine_barrier_appointment_location_tried <- NA
    input_data$v_vaccine_barrier_other_tried <- NA
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
  
  if ( "P3" %in% names(input_data) ) {
    # introduced in Wave 12, replacing E4
    # Yes definitely, Yes probably, Already vaccinated -> 1
    # No definitely not, No probably not -> 0
    input_data$v_vaccinate_child_oldest <- case_when(
      input_data$P3 == 1 ~ 1,
      input_data$P3 == 2 ~ 1,
      input_data$P3 == 3 ~ 0,
      input_data$P3 == 4 ~ 0,
      input_data$P3 == 5 ~ 1,
      TRUE ~ NA_real_
    )

    input_data$v_child_vaccine_already <- input_data$P3 == 5
    input_data$v_child_vaccine_yes_def <- input_data$P3 == 1
    input_data$v_child_vaccine_yes_prob <- input_data$P3 == 2
    input_data$v_child_vaccine_no_prob <- input_data$P3 == 3
    input_data$v_child_vaccine_no_def <- input_data$P3 == 4

  } else {
    input_data$v_vaccinate_child_oldest <- NA_real_
    input_data$v_child_vaccine_already <- NA
    input_data$v_child_vaccine_yes_def <- NA
    input_data$v_child_vaccine_yes_prob <- NA
    input_data$v_child_vaccine_no_prob <- NA
    input_data$v_child_vaccine_no_def <- NA
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
    # Add "some" people
    input_data$v_covid_vaccinated_some_friends <- case_when(
      is.na(input_data$H3) ~ NA,
      input_data$H3 == 4 | input_data$H3 == 6 | input_data$H3 == 3 ~ TRUE,
      TRUE ~ FALSE)
  } else {
    input_data$v_covid_vaccinated_friends <- NA
    input_data$v_covid_vaccinated_some_friends <- NA
  }
  
  if ("V2d" %in% names(input_data)) {
    input_data$v_initial_dose_one_of_one <- input_data$V2d == 1
    input_data$v_initial_dose_one_of_two <- input_data$V2d == 2
    input_data$v_initial_dose_two_of_two <- input_data$V2d == 3
  } else {
    input_data$v_initial_dose_one_of_one <- NA
    input_data$v_initial_dose_one_of_two <- NA
    input_data$v_initial_dose_two_of_two <- NA
  }
  
  if ("V2b" %in% names(input_data)) {
    input_data$v_vaccinated_one_booster <- input_data$V2b == 1
    input_data$v_vaccinated_two_or_more_boosters <- input_data$V2b == 2
    input_data$v_vaccinated_at_least_one_booster <- input_data$V2b == 1 | input_data$V2b == 2
    input_data$v_vaccinated_no_booster <- input_data$V2b == 3
  } else {
    input_data$v_vaccinated_one_booster <- NA
    input_data$v_vaccinated_two_or_more_boosters <- NA
    input_data$v_vaccinated_at_least_one_booster <- NA
    input_data$v_vaccinated_no_booster <- NA
  }
  
  if ("V2c" %in% names(input_data)) {
    input_data$v_vaccinated_booster_accept <- input_data$V2c == 1 | input_data$V2c == 2
    input_data$v_vaccinated_booster_hesitant <- input_data$V2c == 3 | input_data$V2c == 4
    input_data$v_vaccinated_booster_defyes <- input_data$V2c == 1
    input_data$v_vaccinated_booster_probyes <- input_data$V2c == 2
    input_data$v_vaccinated_booster_probno <- input_data$V2c == 3
    input_data$v_vaccinated_booster_defno   <- input_data$V2c == 4
  } else {
    input_data$v_vaccinated_booster_accept <- NA
    input_data$v_vaccinated_booster_hesitant <- NA
    input_data$v_vaccinated_booster_defyes <- NA
    input_data$v_vaccinated_booster_probyes <- NA
    input_data$v_vaccinated_booster_probno <- NA
    input_data$v_vaccinated_booster_defno   <- NA
  }
  
  if ("C17" %in% names(input_data)) {
    # Coded as 1 = "Yes", 4 = "No", 2 = "I don't know"
    input_data$v_flu_vaccinated_june_2020 <- input_data$C17 == 1
  } else {
    input_data$v_flu_vaccinated_june_2020 <- NA
  }

  if ("C17a" %in% names(input_data)) {
    # Coded as 1 = "Yes", 2 = "No", 3 = "I don't know"
    input_data$v_flu_vaccinated_july_2020 <- input_data$C17a == 1
  } else {
    input_data$v_flu_vaccinated_july_2020 <- NA
  }

  if ("C17b" %in% names(input_data)) {
    # Coded as 1 = "Yes", 2 = "No", 3 = "I don't know"
    input_data$v_flu_vaccinated_2021 <- input_data$C17b == 1
  } else {
    input_data$v_flu_vaccinated_2021 <- NA
  }
  
  return(input_data)
}

#' Misc children
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#'
#' @return augmented data frame
code_children <- function(input_data, wave) {
  if ("P2" %in% names(input_data)) {
    input_data$ch_has_child_under_18 <- input_data$P1 == 1
  } else {
    input_data$ch_has_child_under_18 <- NA
  }

  if ("P2" %in% names(input_data)) {
    input_data$ch_oldest_child_under_5 <- input_data$P2 == 1
    input_data$ch_oldest_child_5_to_11 <- input_data$P2 == 2
    input_data$ch_oldest_child_12_to_15 <- input_data$P2 == 3
    input_data$ch_oldest_child_16_to_17 <- input_data$P2 == 4
  } else {
    input_data$ch_oldest_child_under_5 <- NA
    input_data$ch_oldest_child_5_to_11 <- NA
    input_data$ch_oldest_child_12_to_15 <- NA
    input_data$ch_oldest_child_16_to_17 <- NA
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

  if ("P5" %in% names(input_data)) {
    # Coded as 1 = in person classes, 2 = online/remote/distance, 3 = both/mix,
    # 4 = not in school
    input_data$s_inperson_school_fulltime_oldest <- case_when(
      input_data$P5 == 1 ~ 1,
      input_data$P5 != 1 ~ 0,
      TRUE ~ NA_real_
    )
    input_data$s_inperson_school_parttime_oldest <- case_when(
      input_data$P5 == 3 ~ 1,
      input_data$P5 != 3 ~ 0,
      TRUE ~ NA_real_
    )
    input_data$s_remote_school_fulltime_oldest <- case_when(
      input_data$P5 == 2 ~ 1,
      input_data$P5 != 2 ~ 0,
      TRUE ~ NA_real_
    )
    
  } else {
    input_data$s_inperson_school_fulltime_oldest <- NA_real_
    input_data$s_inperson_school_parttime_oldest <- NA_real_
    input_data$s_remote_school_fulltime_oldest <- NA_real_
  }

  if ("P4" %in% names(input_data)) {
    input_data$s_child_school_public <- input_data$P4 == 1
    input_data$s_child_school_private <- input_data$P4 == 2
    input_data$s_child_school_homeschool <- input_data$P4 == 3
    input_data$s_child_school_not <- input_data$P4 == 4
    input_data$s_child_school_other <- input_data$P4 == 5
  } else {
    input_data$s_child_school_public <- NA
    input_data$s_child_school_private <- NA
    input_data$s_child_school_homeschool <- NA
    input_data$s_child_school_not <- NA
    input_data$s_child_school_other <- NA
  }


  if ("P6" %in% names(input_data)) {
    safety_measures <- split_options(input_data$P6)

    input_data$s_school_safety_measures_mask_students <- is_selected(safety_measures, "1")
    input_data$s_school_safety_measures_mask_teachers <- is_selected(safety_measures, "2")
    input_data$s_school_safety_measures_restricted_entry <- is_selected(safety_measures, "6")
    input_data$s_school_safety_measures_separators <- is_selected(safety_measures, "10")
    input_data$s_school_safety_measures_extracurricular <- is_selected(safety_measures, "12")
    input_data$s_school_safety_measures_symptom_screen <- is_selected(safety_measures, "15")
    input_data$s_school_safety_measures_ventilation <- is_selected(safety_measures, "17")
    input_data$s_school_safety_measures_testing_staff <- is_selected(safety_measures, "18")
    input_data$s_school_safety_measures_testing_students <- is_selected(safety_measures, "19")
    input_data$s_school_safety_measures_vaccine_staff <- is_selected(safety_measures, "20")
    input_data$s_school_safety_measures_vaccine_students <- is_selected(safety_measures, "21")
    input_data$s_school_safety_measures_cafeteria <- is_selected(safety_measures, "22")
    input_data$s_school_safety_measures_dont_know <- is_selected(safety_measures, "16")
  } else {
    input_data$s_school_safety_measures_mask_students <- NA
    input_data$s_school_safety_measures_mask_teachers <- NA
    input_data$s_school_safety_measures_restricted_entry <- NA
    input_data$s_school_safety_measures_separators <- NA
    input_data$s_school_safety_measures_extracurricular <- NA
    input_data$s_school_safety_measures_symptom_screen <- NA
    input_data$s_school_safety_measures_ventilation <- NA
    input_data$s_school_safety_measures_testing_staff <- NA
    input_data$s_school_safety_measures_testing_students <- NA
    input_data$s_school_safety_measures_vaccine_staff <- NA
    input_data$s_school_safety_measures_vaccine_students <- NA
    input_data$s_school_safety_measures_cafeteria <- NA
    input_data$s_school_safety_measures_dont_know <- NA
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
    # added in wave 11. Coded as 1 = Very effective, 2 = Moderately effective, 3
    # = Slightly effective, 4 = Not effective at all
    input_data$b_belief_distancing_effective <- case_when(
      input_data$G2 == 1 | input_data$G2 == 2 ~ 1,
      input_data$G2 == 3 | input_data$G2 == 4 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$b_belief_distancing_effective <- NA_real_
  }
  
  if ("G3" %in% names(input_data)) {
    # added in wave 11. Coded as 1 = Very effective, 2 = Moderately effective, 3
    # = Slightly effective, 4 = Not effective at all
    input_data$b_belief_masking_effective <- case_when(
      input_data$G3 == 1 | input_data$G3 == 2 ~ 1,
      input_data$G3 == 3 | input_data$G3 == 4 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$b_belief_masking_effective <- NA_real_
  }
  
  if ("I1" %in% names(input_data)) {
    # added in wave 11. Coded as 1 = Definitely false, 2 = Probably false, 3 = I
    # really have no idea, 4 = Probably true, 5 = Definitely true
    input_data$b_belief_vaccinated_mask_unnecessary <- case_when(
      input_data$I1 == 4 | input_data$I1 == 5 ~ 1,
      input_data$I1 == 1 | input_data$I1 == 2 | input_data$I1 == 3 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$b_belief_vaccinated_mask_unnecessary <- NA_real_
  }
  
  if ("I2" %in% names(input_data)) {
    # added in wave 11. Coded as 1 = Definitely false, 2 = Probably false, 3 = I
    # really have no idea, 4 = Probably true, 5 = Definitely true
    input_data$b_belief_children_immune <- case_when(
      input_data$I2 == 4 | input_data$I2 == 5 ~ 1,
      input_data$I2 == 1 | input_data$I2 == 2 | input_data$I2 == 3 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$b_belief_children_immune <- NA_real_
  }
  
  if ("I3" %in% names(input_data)) {
    # added in wave 11. Coded as 1 = Definitely false, 2 = Probably false, 3 = I
    # really have no idea, 4 = Probably true, 5 = Definitely true
    input_data$b_belief_created_small_group <- case_when(
      input_data$I3 == 4 | input_data$I3 == 5 ~ 1,
      input_data$I3 == 1 | input_data$I3 == 2 | input_data$I3 == 3 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$b_belief_created_small_group <- NA_real_
  }
  
  if ("I4" %in% names(input_data)) {
    # added in wave 11. Coded as 1 = Definitely false, 2 = Probably false, 3 = I
    # really have no idea, 4 = Probably true, 5 = Definitely true
    input_data$b_belief_govt_exploitation <- case_when(
      input_data$I4 == 4 | input_data$I4 == 5 ~ 1,
      input_data$I4 == 1 | input_data$I4 == 2 | input_data$I4 == 3 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$b_belief_govt_exploitation <- NA_real_
  }

  if ("K1" %in% names(input_data)) {
    # added in wave 11. Coded as 1 = Yes, 2 = No
    input_data$b_delayed_care_cost <- input_data$K1 == 1
  } else {
    input_data$b_delayed_care_cost <- NA_real_
  }
  
  if ("K2" %in% names(input_data)) {
    # added in wave 11. Coded as 1 = Strongly agree, 2 = Somewhat agree, 3 =
    # Somewhat disagree, 4 = Strongly disagree
    input_data$b_race_treated_fairly_healthcare <- case_when(
      input_data$K2 == 1 | input_data$K2 == 2 ~ 1,
      input_data$K2 == 3 | input_data$K2 == 4 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$b_race_treated_fairly_healthcare <- NA_real_
  }
    
  return(input_data)
}

#' COVID news and information variables
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#' 
#' @return augmented data frame
code_news_and_info <- function(input_data, wave) {
  if ("I5" %in% names(input_data)) {
    # introduced in wave 11
    news_sources <- split_options(input_data$I5)
    
    input_data$i_received_news_local_health <- is_selected(news_sources, "1")
    input_data$i_received_news_experts <- is_selected(news_sources, "2")
    input_data$i_received_news_cdc <- is_selected(news_sources, "3")
    input_data$i_received_news_govt_health <- is_selected(news_sources, "4")
    input_data$i_received_news_politicians <- is_selected(news_sources, "5")
    input_data$i_received_news_journalists <- is_selected(news_sources, "6")
    input_data$i_received_news_friends <- is_selected(news_sources, "7")
    input_data$i_received_news_religious <- is_selected(news_sources, "8")
    input_data$i_received_news_none <- is_selected(news_sources, "9")
  } else {
    input_data$i_received_news_local_health <- NA
    input_data$i_received_news_experts <- NA
    input_data$i_received_news_cdc <- NA
    input_data$i_received_news_govt_health <- NA
    input_data$i_received_news_politicians <- NA
    input_data$i_received_news_journalists <- NA
    input_data$i_received_news_friends <- NA
    input_data$i_received_news_religious <- NA
    input_data$i_received_news_none <- NA
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
  
  if ("I7" %in% names(input_data)) {
    # introduced in wave 11
    info_topic <- split_options(input_data$I7)
    
    input_data$i_want_info_covid_treatment <- is_selected(info_topic, "1")
    input_data$i_want_info_vaccine_access <- is_selected(info_topic, "2")
    input_data$i_want_info_vaccine_types <- is_selected(info_topic, "3")
    input_data$i_want_info_covid_variants <- is_selected(info_topic, "6")
    input_data$i_want_info_children_education <- is_selected(info_topic, "7")
    input_data$i_want_info_mental_health <- is_selected(info_topic, "8")
    input_data$i_want_info_relationships <- is_selected(info_topic, "9")
    input_data$i_want_info_employment <- is_selected(info_topic, "10")
    input_data$i_want_info_none <- is_selected(info_topic, "11")
  } else {
    input_data$i_want_info_covid_treatment <- NA
    input_data$i_want_info_vaccine_access <- NA
    input_data$i_want_info_vaccine_types <- NA
    input_data$i_want_info_covid_variants <- NA
    input_data$i_want_info_children_education <- NA
    input_data$i_want_info_mental_health <- NA
    input_data$i_want_info_relationships <- NA
    input_data$i_want_info_employment <- NA
    input_data$i_want_info_none <- NA
  }
  
  return(input_data)
}

#' Race/ethnicity
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#' 
#' @return augmented data frame
code_race_ethnicity <- function(input_data, wave) {
  # race
  if ("D7" %in% names(input_data)) {
    input_data$race <- case_when(
      input_data$D7 == 1 ~ "AmericanIndianAlaskaNative",
      input_data$D7 == 2 ~ "Asian",
      input_data$D7 == 3 ~ "BlackAfricanAmerican",
      input_data$D7 == 4 ~ "NativeHawaiianPacificIslander",
      input_data$D7 == 5 ~ "White",
      input_data$D7 == 6 ~ "MultipleOther",
      grepl(",", input_data$D7) ~ "MultipleOther", # Multiracial
      TRUE ~ NA_character_
    )
  } else {
    input_data$race <- NA_character_
  }
  
  # ethnicity
  if ("D6" %in% names(input_data)) {
    input_data$hispanic <- input_data$D6 == 1
  } else {
    input_data$hispanic <- NA
  }
  
  # Combo race-ethnicity
  if ( "hispanic" %in% names(input_data) &&
       "race" %in% names(input_data) ) {
    input_data$raceethnicity <- case_when(
      input_data$hispanic ~ "Hispanic",
      !input_data$hispanic & input_data$race == "AmericanIndianAlaskaNative" ~ "NonHispanicAmericanIndianAlaskaNative",
      !input_data$hispanic & input_data$race == "Asian" ~ "NonHispanicAsian",
      !input_data$hispanic & input_data$race == "BlackAfricanAmerican" ~ "NonHispanicBlackAfricanAmerican",
      !input_data$hispanic & input_data$race == "NativeHawaiianPacificIslander" ~ "NonHispanicNativeHawaiianPacificIslander",
      !input_data$hispanic & input_data$race == "White" ~ "NonHispanicWhite",
      !input_data$hispanic & input_data$race == "MultipleOther" ~ "NonHispanicMultipleOther",
      TRUE ~ NA_character_
    )
  } else {
    input_data$raceethnicity <- NA_character_
  }
  
  return(input_data)
}

#' Gender
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#' 
#' @return augmented data frame
code_gender <- function(input_data, wave) {
  if ("D1" %in% names(input_data)) {
    input_data$gender <- case_when(
      input_data$D1 == 1 ~ "Male",
      input_data$D1 == 2 ~ "Female",
      input_data$D1 == 3 ~ "Other",
      input_data$D1 == 4 ~ "Other",
      input_data$D1 == 5 ~ NA_character_,
      TRUE ~ NA_character_
    )
  } else {
    input_data$gender <- NA_character_
  }
  
  return(input_data)
}

#' Age-related fields
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#' 
#' @return augmented data frame
code_age <- function(input_data, wave) {
  if ("D2" %in% names(input_data)) {
    input_data$agefull <- case_when(
      input_data$D2 == 1 ~ "18-24",
      input_data$D2 == 2 ~ "25-34",
      input_data$D2 == 3 ~ "35-44",
      input_data$D2 == 4 ~ "45-54",
      input_data$D2 == 5 ~ "55-64",
      input_data$D2 == 6 ~ "65-74",
      input_data$D2 == 7 ~ "75plus",
      TRUE ~ NA_character_
    )
    
    # Condensed age categories
    input_data$age <- case_when(
      input_data$D2 == 1 ~ "18-24",
      input_data$D2 == 2 ~ "25-44",
      input_data$D2 == 3 ~ "25-44",
      input_data$D2 == 4 ~ "45-64",
      input_data$D2 == 5 ~ "45-64",
      input_data$D2 == 6 ~ "65plus",
      input_data$D2 == 7 ~ "65plus",
      TRUE ~ NA_character_
    )
    
    input_data$age65plus <- input_data$age == "65plus"
  } else {
    input_data$agefull <- NA_character_
    input_data$age <- NA_character_
    input_data$age65plus <- NA
  }
  
  return(input_data)
}

