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
#' @param vec A list whose entries are character vectors, such as c("14", "15").
#' @param selection one string, such as "14"
#' @return a logical vector; for each list entry, whether selection is contained
#'   in the character vector.
#'   
#' @importFrom parallel mclapply
is_selected <- function(vec, selection) {
  selections <- unlist(mclapply(
    vec,
    function(resp) {
      if (length(resp) == 0 || all(is.na(resp))) {
        # All our selection items include "None of the above" or similar, so
        # treat no selection the same as missingness.
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
  return(input_data)
}

#' Household symptom variables
#'
#' @param input_data input data frame of raw survey data
#' @return data frame augmented with `hh_fever`, `hh_soar_throat`, `hh_cough`,
#'   `hh_short_breath`, `hh_diff_breath`, `hh_number_sick`
code_symptoms <- function(input_data) {
  input_data$hh_fever <- (input_data$A1_1 == 1L)
  input_data$hh_soar_throat <- (input_data$A1_2 == 1L)
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

  if (wave >= 4) {
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
  return(input_data)
}

#' Mask and contact variables
#'
#' @param input_data input data frame of raw survey data
#' @return data frame augmented with `c_travel_state`, `c_work_outside_5d`,
#'   `c_mask_often`, `c_others_masked`
code_mask_contact <- function(input_data) {
  # private helper for both mask items, which are identically coded: 6 means the
  # respondent was not in public, 1 & 2 mean always/most, 3-5 mean some to none
  most_always <- function(item) {
    case_when(
      is.na(item) ~ NA,
      item == 6 ~ NA,
      item == 1 | item ==  2 ~ TRUE,
      TRUE ~ FALSE)
  }

  input_data$c_travel_state <- input_data$C6 == 1

  if ("C14" %in% names(input_data)) {
    # added in wave 4. wearing mask most or all of the time; exclude respondents
    # who have not been in public
    input_data$c_mask_often <- most_always(input_data$C14)
  } else {
    input_data$c_mask_often <- NA
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
    input_data$t_tested_14d <- NA
    input_data$t_tested_positive_14d <- NA
    input_data$t_wanted_test_14d <- NA
  }
  return(input_data)
}

#' COVID vaccination variables
#'
#' @param input_data input data frame of raw survey data
#' @return data frame augmented with `v_covid_vaccinated` and
#'   `v_accept_covid_vaccine`
code_vaccines <- function(input_data) {
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
    input_data$v_vaccine_likely_local_health <- input_data$V4_2 == 1
    input_data$v_vaccine_likely_who <- input_data$V4_3 == 1
    input_data$v_vaccine_likely_govt_health <- input_data$V4_4 == 1
    input_data$v_vaccine_likely_politicians <- input_data$V4_5 == 1
  } else {
    input_data$v_vaccine_likely_friends <- NA_real_
    input_data$v_vaccine_likely_local_health <- NA_real_
    input_data$v_vaccine_likely_who <- NA_real_
    input_data$v_vaccine_likely_govt_health <- NA_real_
    input_data$v_vaccine_likely_politicians <- NA_real_
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
