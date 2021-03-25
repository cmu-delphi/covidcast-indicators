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
#' @return data frame augmented with `a_work_outside_home_1d`, `a_shop_1d`,
#'   `a_restaurant_1d`, `a_spent_time_1d`, `a_large_event_1d`,
#'   `a_public_transit_1d`
code_activities <- function(input_data) {
  wave <- unique(input_data$wave)
  assert(length(wave) == 1, "can only code one wave at a time")

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
#' @return data frame augmented with `hh_fever`, `hh_sore_throat`, `hh_cough`,
#'   `hh_short_breath`, `hh_diff_breath`, `hh_number_sick`
code_symptoms <- function(input_data) {
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
#' @return data frame augmented with `hh_number_total`
code_hh_size <- function(input_data) {
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
#' @return data frame augmented with `mh_worried_ill`, `mh_anxious`,
#'   `mh_depressed`, `mh_isolated`, `mh_worried_finances`
code_mental_health <- function(input_data) {
  wave <- unique(input_data$wave)
  assert(length(wave) == 1, "can only code one wave at a time")

  if (wave >= 4 && wave < 10) {
    input_data$mh_worried_ill <- input_data$C9 == 1 | input_data$C9 == 2
    input_data$mh_anxious <- input_data$C8_1 == 3 | input_data$C8_1 == 4
    input_data$mh_depressed <- input_data$C8_2 == 3 | input_data$C8_2 == 4
    input_data$mh_isolated <- input_data$C8_3 == 3 | input_data$C8_3 == 4
    input_data$mh_worried_finances <- input_data$C15 == 1 | input_data$C15 == 2
  } else {
    input_data$mh_worried_ill <- NA
    input_data$mh_anxious <- NA
    input_data$mh_depressed <- NA
    input_data$mh_isolated <- NA
    input_data$mh_worried_finances <- NA
  }
  
  if (wave >= 10) {
    input_data$mh_worried_ill <- input_data$C9 == 1 | input_data$C9 == 2
    input_data$mh_anxious_7d <- input_data$C8a_1 == 3 | input_data$C8a_1 == 4
    input_data$mh_depressed_7d <- input_data$C8a_2 == 3 | input_data$C8a_2 == 4
    input_data$mh_isolated_7d <- input_data$C8a_3 == 3 | input_data$C8a_3 == 4
    input_data$mh_worried_finances <- input_data$C15 == 1 | input_data$C15 == 2
  } else {
    input_data$mh_anxious_7d <- NA
    input_data$mh_depressed_7d <- NA
    input_data$mh_isolated_7d <- NA
  }
  return(input_data)
}

#' Mask and contact variables
#'
#' @param input_data input data frame of raw survey data
#' @return data frame augmented with `c_travel_state`, `c_work_outside_5d`,
#'   `c_mask_often`, `c_others_masked`
code_mask_contact <- function(input_data) {
  wave <- unique(input_data$wave)
  assert(length(wave) == 1, "can only code one wave at a time")

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
#' @return data frame augmented with `t_tested_14d`, `t_tested_positive_14d`,
#'   `t_wanted_test_14d`
code_testing <- function(input_data) {
  if ("B8" %in% names(input_data) && "B10" %in% names(input_data) &&
        "B12" %in% names(input_data)) {
    # fraction tested in last 14 days. yes == 1 on B10; no == 2 on B8 *or* 3 on
    # B10 (which codes "no" as 3 for some reason)
    input_data$t_tested_14d <- case_when(
      input_data$B8 == 2 | input_data$B10 == 3 ~ 0,
      input_data$B10 == 1 ~ 1,
      TRUE ~ NA_real_
    )

    # fraction, of those tested in past 14 days, who tested positive. yes == 1
    # on B10a, no == 2 on B10a; option 3 is "I don't know", which is excluded
    input_data$t_tested_positive_14d <- case_when(
      input_data$B10a == 1 ~ 1, # yes
      input_data$B10a == 2 ~ 0, # no
      input_data$B10a == 3 ~ NA_real_, # I don't know
      TRUE ~ NA_real_
    )

    # fraction, of those not tested in past 14 days, who wanted to be tested but
    # were not
    input_data$t_wanted_test_14d <- input_data$B12 == 1
  } else {
    input_data$t_tested_14d <- NA_real_
    input_data$t_tested_positive_14d <- NA_real_
    input_data$t_wanted_test_14d <- NA
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
    
    input_data$t_tested_reason_screening <- case_when(
      input_data$t_tested_reason_sick == TRUE ~ 0,
      input_data$t_tested_reason_contact == TRUE ~ 0,
      input_data$t_tested_reason_crowd == TRUE ~ 0,
      
      input_data$t_tested_reason_medical == TRUE ~ 1,
      input_data$t_tested_reason_employer == TRUE ~ 1,
      input_data$t_tested_reason_large_event == TRUE ~ 1,
      input_data$t_tested_reason_visit_fam == TRUE ~ 1,
      
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
  return(input_data)
}

#' COVID vaccination variables
#'
#' @param input_data input data frame of raw survey data
#' @return data frame augmented with `v_covid_vaccinated` and
#'   `v_accept_covid_vaccine`
#'
#' @importFrom dplyr coalesce
code_vaccines <- function(input_data) {
  wave <- unique(input_data$wave)
  assert(length(wave) == 1, "can only code one wave at a time")
  
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

  if ("V3" %in% names(input_data)) {
    input_data$v_accept_covid_vaccine <- (
      input_data$V3 == 1 | input_data$V3 == 2
    )
  } else {
    input_data$v_accept_covid_vaccine <- NA_real_
  }

  if ("V3" %in% names(input_data) && "V1" %in% names(input_data)) {
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
  } else {
    input_data$v_covid_vaccinated_or_accept <- NA_real_
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

  return(input_data)
}
