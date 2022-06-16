## Functions to rename, reformat, and recode responses in the raw input data for
## use in contingency tables, called from `load_response_one`. Since these are
## called in `load_response_one`, which reads one specific Qualtrics file, their
## input data is always from only one wave of the survey -- they do not deal
## with inputs that have multiple waves mingled in one data frame.

#' Occupation
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#' 
#' @return augmented data frame
code_occupation <- function(input_data, wave) {
  if ("Q64" %in% names(input_data)) {
    input_data$healthcareworker <-
      input_data$Q64 == 5 |
      input_data$Q64 == 4
  } else {
    input_data$healthcareworker <- NA
  }
  
  if ("Q64" %in% names(input_data)) {
    input_data$occupation <- case_when(
      input_data$Q64 == 1 ~ "SocialService",
      input_data$Q64 == 2 ~ "Education",
      input_data$Q64 == 3 ~ "Arts",
      input_data$Q64 == 4 ~ "HealthcarePractitioner",
      input_data$Q64 == 5 ~ "HealthcareSupport",
      input_data$Q64 == 6 ~ "ProtectiveService",
      input_data$Q64 == 7 ~ "FoodService",
      input_data$Q64 == 8 ~ "BuildingMaintenance",
      input_data$Q64 == 9 ~ "PersonalCare",
      input_data$Q64 == 10 ~ "Sales",
      input_data$Q64 == 11 ~ "Office",
      input_data$Q64 == 12 ~ "Construction",
      input_data$Q64 == 13 ~ "Maintenance",
      input_data$Q64 == 14 ~ "Production",
      input_data$Q64 == 15 ~ "Transportation",
      input_data$Q64 == 16 ~ "Other",
      TRUE ~ NA_character_
    )

    input_data$occ_4w_social <- input_data$Q64 == 1
    input_data$occ_4w_education <- input_data$Q64 == 2
    input_data$occ_4w_arts <- input_data$Q64 == 3
    input_data$occ_4w_health_prac <- input_data$Q64 == 4
    input_data$occ_4w_health_support <- input_data$Q64 == 5
    input_data$occ_4w_protective <- input_data$Q64 == 6
    input_data$occ_4w_food <- input_data$Q64 == 7
    input_data$occ_4w_building <- input_data$Q64 == 8
    input_data$occ_4w_personal <- input_data$Q64 == 9
    input_data$occ_4w_sales <- input_data$Q64 == 10
    input_data$occ_4w_admin <- input_data$Q64 == 11
    input_data$occ_4w_construction <- input_data$Q64 == 12
    input_data$occ_4w_maintenance <- input_data$Q64 == 13
    input_data$occ_4w_production <- input_data$Q64 == 14
    input_data$occ_4w_transportation <- input_data$Q64 == 15
    input_data$occ_4w_other <- input_data$Q64 == 16
  } else {
    input_data$occupation <- NA_character_

    input_data$occ_4w_social <- NA
    input_data$occ_4w_education <- NA
    input_data$occ_4w_arts <- NA
    input_data$occ_4w_health_prac <- NA
    input_data$occ_4w_health_support <- NA
    input_data$occ_4w_protective <- NA
    input_data$occ_4w_food <- NA
    input_data$occ_4w_building <- NA
    input_data$occ_4w_personal <- NA
    input_data$occ_4w_sales <- NA
    input_data$occ_4w_admin <- NA
    input_data$occ_4w_construction <- NA
    input_data$occ_4w_maintenance <- NA
    input_data$occ_4w_production <- NA
    input_data$occ_4w_transportation <- NA
    input_data$occ_4w_other <- NA
  }
  
  return(input_data)
}

#' Education
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#' 
#' @return augmented data frame
code_education <- function(input_data, wave) {
  if ("D8" %in% names(input_data)) {
    input_data$edulevelfull <- case_when(
      input_data$D8 == 1 ~ "LessThanHighSchool",
      input_data$D8 == 2 ~ "HighSchool",
      input_data$D8 == 3 ~ "SomeCollege",
      input_data$D8 == 4 ~ "TwoYearDegree",
      input_data$D8 == 5 ~ "FourYearDegree",
      input_data$D8 == 8 ~ "MastersDegree",
      input_data$D8 == 6 ~ "ProfessionalDegree",
      input_data$D8 == 7 ~ "Doctorate",
      TRUE ~ NA_character_
    )
    
    input_data$edulevel <- case_when(
      input_data$D8 == 1 ~ "LessThanHighSchool",
      input_data$D8 == 2 ~ "HighSchool",
      input_data$D8 == 3 ~ "SomeCollege",
      input_data$D8 == 4 ~ "SomeCollege",
      input_data$D8 == 5 ~ "FourYearDegree",
      input_data$D8 == 8 ~ "PostGraduate",
      input_data$D8 == 6 ~ "PostGraduate",
      input_data$D8 == 7 ~ "PostGraduate",
      TRUE ~ NA_character_
    )
  } else {
    input_data$edulevelfull <- NA_character_
    input_data$edulevel <- NA_character_
  }
  
  return(input_data)
}

#' Health conditions and behaviors
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#' 
#' @return augmented data frame
code_health <- function(input_data, wave) {
  # pregnant. Ignore any responses if male (D1 == 1)
  if (all(c("D1b", "D1") %in% names(input_data))) {
    input_data$pregnant <- case_when(
      input_data$D1b == 1 ~ TRUE,
      input_data$D1b == 2 ~ FALSE,
      input_data$D1b == 4 ~ NA,
      input_data$D1 == 1 ~ NA,
      TRUE ~ NA
    )
  } else {
    input_data$pregnant <- NA
  }
  
  # smoker
  if ("D11" %in% names(input_data)) {
    input_data$smoker <- input_data$D11 == 1
  } else {
    input_data$smoker <- NA
  }
  
  # vaccine eligibility + priority vaccine-qualifying health conditions
  if ("C1" %in% names(input_data)) {
    comorbidities <- split_options(input_data$C1)
    
    input_data$comorbidheartdisease <- is_selected(comorbidities, "3")
    input_data$comorbid_high_blood_pressure <- is_selected(comorbidities, "5")
    input_data$comorbid_asthma <- is_selected(comorbidities, "5")
    input_data$comorbidcancer <- is_selected(comorbidities, "2")
    input_data$comorbidkidneydisease <- is_selected(comorbidities, "7")
    input_data$comorbidlungdisease <- is_selected(comorbidities, "6")
    input_data$comorbiddiabetes <-
      is_selected(comorbidities, "1") |
      is_selected(comorbidities, "12") |
      is_selected(comorbidities, "10")
    input_data$comorbidimmuno <- is_selected(comorbidities, "11")
    input_data$comorbid_autoimmune <- is_selected(comorbidities, "8")
    input_data$comorbidobese <- is_selected(comorbidities, "13")
    input_data$comorbid_none <- is_selected(comorbidities, "9")

    # Combo vaccine-eligibility
    input_data$eligible <- 
      input_data$comorbidheartdisease |
      input_data$comorbidcancer |
      input_data$comorbidkidneydisease |
      input_data$comorbidlungdisease |
      input_data$comorbiddiabetes |
      input_data$comorbidimmuno
    
  } else {
    input_data$comorbidheartdisease <- NA
    input_data$comorbidcancer <- NA
    input_data$comorbidkidneydisease <- NA
    input_data$comorbidlungdisease <- NA
    input_data$comorbiddiabetes <- NA
    input_data$comorbidimmuno <- NA
    input_data$comorbidobese <- NA
    input_data$eligible <- NA
    input_data$comorbid_high_blood_pressure <- NA
    input_data$comorbid_asthma <- NA
    input_data$comorbid_none <- NA
    input_data$comorbid_autoimmune <- NA
  }
  
  # Combo vaccine-eligibility updated to include smoking, pregnant, and obesity
  # status (added in Wave 8)
  if (all(c("eligible", "D1", "pregnant", "smoker", "comorbidobese") %in% names(input_data))) {
    # Fill in pregnant status with male gender as FALSE
    input_data$eligiblepregsmokeobese <-
      input_data$eligible |
      ifelse(input_data$D1 %in% 1, FALSE, input_data$pregnant) |
      input_data$smoker |
      input_data$comorbidobese
  } else {
    input_data$eligiblepregsmokeobese <- NA
  }
  
  return(input_data)
}

#' Vaccinated summary variable for making table cuts
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#' 
#' @return augmented data frame
code_vaccinated_breakdown <- function(input_data, wave) {
  # grouping variable - vaccination status
  if (all(c("V1", "V3a", "V11a") %in% names(input_data))) {
    input_data$vaccinationstatus <- case_when(
      input_data$V1 == 1 ~ "Vaccinated",
      input_data$V3a == 1 ~ "Accept/Appointment",
      input_data$V3a == 2 ~ "Accept/Appointment",
      input_data$V11a == 1 ~ "Accept/Appointment",
      input_data$V3a == 3 ~ "Hesitant",
      input_data$V3a == 4 ~ "Hesitant",
      TRUE ~ NA_character_
    )
  } else if (all(c("V1", "V3") %in% names(input_data))) {
    input_data$vaccinationstatus <- case_when(
      input_data$V1 == 1 ~ "Vaccinated",
      input_data$V3 == 1 ~ "Accept/Appointment",
      input_data$V3 == 2 ~ "Accept/Appointment",
      input_data$V3 == 3 ~ "Hesitant",
      input_data$V3 == 4 ~ "Hesitant",
      TRUE ~ NA_character_
    )
  } else {
    input_data$vaccinationstatus <- NA
  }

  return(input_data)
}

#' COVID vaccination variables with modifications for contingency tables
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#' 
#' @return augmented data frame
code_addl_vaccines <- function(input_data, wave) {
  ## Items V3 and V4 display logic was changed mid-wave 6 to be shown only to
  ## respondents indicated that they had not been vaccinated. For the purposes
  ## of the contingency tables, we will ignore responses to V3 and V4 from
  ## before the change.
  if ("v_accept_covid_vaccine" %in% names(input_data)) {	
    input_data$v_accept_covid_vaccine[input_data$start_dt < wave6_mod_date] <- NA
  }

  if ("V2a" %in% names(input_data)) {
    # coded as 1 = Yes, received all recommended doses, 2 = Plan to receive all recommended doses,
    # 3 = Don't plan to receive all recommended doses.
    input_data$v_received_all_doses <- case_when(
      input_data$V2a == 1 ~ 1,
      input_data$V2a == 2 ~ 0,
      input_data$V2a == 3 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$v_received_all_doses <- NA_real_
  }

  # hesitant_vaccine
  # Percentage who would definitely or probably NOT choose to get vaccinated
  input_data$v_hesitant_vaccine <- NA
  if ("v_accept_covid_vaccine" %in% names(input_data)) {	
    input_data$v_hesitant_vaccine <- !input_data$v_accept_covid_vaccine
  }
  # post-Wave 11. No longer includes respondents with vaccine appointments
  if ("V3a" %in% names(input_data)) {
    input_data$v_hesitant_vaccine <- coalesce(
      input_data$v_hesitant_vaccine, input_data$V3a == 3 | input_data$V3a == 4
    )
  }

  input_data$overall_vaccine_hesitancy <- coalesce(
    !input_data$v_covid_vaccinated_or_accept,
    !input_data$v_covid_vaccinated_appointment_or_accept
  )

  # accept_vaccine_defyes
  # accept_vaccine_probyes
  # accept_vaccine_probno
  # accept_vaccine_defno
  input_data$v_accept_vaccine_defyes <- NA
  input_data$v_accept_vaccine_probyes <- NA
  input_data$v_accept_vaccine_probno <- NA
  input_data$v_accept_vaccine_defno <- NA
  # post-Wave 11. No longer include respondents with vaccine appointments.
  input_data$v_accept_vaccine_no_appointment_defyes <- NA
  input_data$v_accept_vaccine_no_appointment_probyes <- NA
  input_data$v_accept_vaccine_no_appointment_probno <- NA
  input_data$v_accept_vaccine_no_appointment_defno <- NA
  
  if ("V3" %in% names(input_data)) {
    input_data$V3[input_data$start_dt < wave6_mod_date] <- NA
    
    input_data$v_accept_vaccine_defyes <- input_data$V3 == 1
    input_data$v_accept_vaccine_probyes <- input_data$V3 == 2
    input_data$v_accept_vaccine_probno <- input_data$V3 == 3
    input_data$v_accept_vaccine_defno <- input_data$V3 == 4
  }
  if ("V3a" %in% names(input_data)) {	
    input_data$v_accept_vaccine_no_appointment_defyes <- input_data$V3a == 1
    input_data$v_accept_vaccine_no_appointment_probyes <- input_data$V3a == 2
    input_data$v_accept_vaccine_no_appointment_probno <- input_data$V3a == 3
    input_data$v_accept_vaccine_no_appointment_defno <- input_data$V3a == 4
  }
  
  if ( "V16" %in% names(input_data) ) {
    # introduced in Wave 11
    input_data$v_vaccine_timing_weeks <- input_data$V16 == 1
    input_data$v_vaccine_timing_onemonth <- input_data$V16 == 2
    input_data$v_vaccine_timing_threemonths <- input_data$V16 == 3
    input_data$v_vaccine_timing_sixmonths <- input_data$V16 == 4
    input_data$v_vaccine_timing_morethansix <- input_data$V16 == 5
    input_data$v_vaccine_timing_dontknow <- input_data$V16 == 6
  } else {
    input_data$v_vaccine_timing_weeks <- NA
    input_data$v_vaccine_timing_onemonth <- NA
    input_data$v_vaccine_timing_threemonths <- NA
    input_data$v_vaccine_timing_sixmonths <- NA
    input_data$v_vaccine_timing_morethansix <- NA
    input_data$v_vaccine_timing_dontknow <- NA
  }
  
  if ("B3" %in% names(input_data)) {
    input_data$t_taken_temp <- input_data$B3 == 1
  } else {
    input_data$t_taken_temp <- NA
  }

  if ("B8" %in% names(input_data)) {
    input_data$t_ever_tested <- input_data$B8 == 1
  } else {
    input_data$t_ever_tested <- NA
  }
  
  if ("B5" %in% names(input_data)) {
    # Coded as 1 = "tested and COVID+", 2 = "tested and COVID-",
    # 3 = "tested, but no result yet", 4 = "tried to get tested but couldn't",
    # 5 = "didn't try to get tested"
    # Regardless of test result
    input_data$t_unusual_symptom_tested <- case_when(
      input_data$B5 %in% c(1, 2, 3) ~ 1,
      input_data$B5 %in% c(4, 5) ~ 0,
      TRUE ~ NA_real_
    )
    # Tested with positive result
    input_data$t_unusual_symptom_tested_positive <- input_data$B5 == 1
  } else {
    input_data$t_unusual_symptom_tested <- NA
    input_data$t_unusual_symptom_tested_positive <- NA
  }

  if ("B6" %in% names(input_data)) {
    input_data$t_unusual_symptom_hospital <- input_data$B6 == 1
  } else {
    input_data$t_unusual_symptom_hospital <- NA
  }

  if ("B7" %in% names(input_data)) {
    # Coded as 8 = no medical care sought, 1-6 = various types of medical care sought,
    # 7 = care sought but not received
    unusual_symptoms_care <- split_options(input_data$B7)

    input_data$unusual_symptom_medical_care_called_doctor <- is_selected(unusual_symptoms_care, "1")
    input_data$unusual_symptom_medical_care_telemedicine <- is_selected(unusual_symptoms_care, "2")
    input_data$unusual_symptom_medical_care_visited_doctor <- is_selected(unusual_symptoms_care, "3")
    input_data$unusual_symptom_medical_care_urgent_care <- is_selected(unusual_symptoms_care, "4")
    input_data$unusual_symptom_medical_care_er <- is_selected(unusual_symptoms_care, "5")
    input_data$unusual_symptom_medical_care_hospital <- is_selected(unusual_symptoms_care, "6")
    input_data$unusual_symptom_medical_care_tried <- is_selected(unusual_symptoms_care, "7")
  } else {
    input_data$unusual_symptom_medical_care_called_doctor <- NA
    input_data$unusual_symptom_medical_care_telemedicine <- NA
    input_data$unusual_symptom_medical_care_visited_doctor <- NA
    input_data$unusual_symptom_medical_care_urgent_care <- NA
    input_data$unusual_symptom_medical_care_er <- NA
    input_data$unusual_symptom_medical_care_hospital <- NA
    input_data$unusual_symptom_medical_care_tried <- NA
  }

  if ( "B12a" %in% names(input_data) ) {
    not_tested_reasons <- split_options(input_data$B12a)

    input_data$t_reason_not_tested_tried <- is_selected(not_tested_reasons, "1")
    input_data$t_reason_not_tested_appointment <- is_selected(not_tested_reasons, "2")
    input_data$t_reason_not_tested_location <- is_selected(not_tested_reasons, "3")
    input_data$t_reason_not_tested_cost <- is_selected(not_tested_reasons, "4")
    input_data$t_reason_not_tested_time <- is_selected(not_tested_reasons, "5")
    input_data$t_reason_not_tested_travel <- is_selected(not_tested_reasons, "6")
    input_data$t_reason_not_tested_stigma <- is_selected(not_tested_reasons, "7")
    input_data$t_reason_not_tested_none <- is_selected(not_tested_reasons, "8")
  } else {
    input_data$t_reason_not_tested_tried <- NA
    input_data$t_reason_not_tested_appointment <- NA
    input_data$t_reason_not_tested_location <- NA
    input_data$t_reason_not_tested_cost <- NA
    input_data$t_reason_not_tested_time <- NA
    input_data$t_reason_not_tested_travel <- NA
    input_data$t_reason_not_tested_stigma <- NA
    input_data$t_reason_not_tested_none <- NA
  }

if ("V5d" %in% names(input_data)) {
    # introduced in Wave 8, removed in Wave 11
    vaccine_incomplete_reasons <- split_options(input_data$V5d)

    input_data$v_vaccine_incomplete_sideeffect <- is_selected(vaccine_incomplete_reasons, "1")
    input_data$v_vaccine_incomplete_allergic <- is_selected(vaccine_incomplete_reasons, "2")
    input_data$v_vaccine_incomplete_wontwork <- is_selected(vaccine_incomplete_reasons, "3")
    input_data$v_vaccine_incomplete_dontbelieve <- is_selected(vaccine_incomplete_reasons, "4")
    input_data$v_vaccine_incomplete_dontlike <- is_selected(vaccine_incomplete_reasons, "5")
    input_data$v_vaccine_incomplete_not_recommended <- is_selected(vaccine_incomplete_reasons, "6")
    input_data$v_vaccine_incomplete_wait <- is_selected(vaccine_incomplete_reasons, "7")
    input_data$v_vaccine_incomplete_otherpeople <- is_selected(vaccine_incomplete_reasons, "8")
    input_data$v_vaccine_incomplete_cost <- is_selected(vaccine_incomplete_reasons, "9")
    input_data$v_vaccine_incomplete_distrust_vaccine <- is_selected(vaccine_incomplete_reasons, "10")
    input_data$v_vaccine_incomplete_distrust_gov <- is_selected(vaccine_incomplete_reasons, "11")
    input_data$v_vaccine_incomplete_health <- is_selected(vaccine_incomplete_reasons, "12")
    input_data$v_vaccine_incomplete_other <- is_selected(vaccine_incomplete_reasons, "13")
    input_data$v_vaccine_incomplete_pregnant <- is_selected(vaccine_incomplete_reasons, "14")
    input_data$v_vaccine_incomplete_religion <- is_selected(vaccine_incomplete_reasons, "15")
  } else {
    input_data$v_vaccine_incomplete_sideeffect <- NA_real_
    input_data$v_vaccine_incomplete_allergic <- NA_real_
    input_data$v_vaccine_incomplete_wontwork <- NA_real_
    input_data$v_vaccine_incomplete_dontbelieve <- NA_real_
    input_data$v_vaccine_incomplete_dontlike <- NA_real_
    input_data$v_vaccine_incomplete_not_recommended <- NA_real_
    input_data$v_vaccine_incomplete_wait <- NA_real_
    input_data$v_vaccine_incomplete_otherpeople <- NA_real_
    input_data$v_vaccine_incomplete_cost <- NA_real_
    input_data$v_vaccine_incomplete_distrust_vaccine <- NA_real_
    input_data$v_vaccine_incomplete_distrust_gov <- NA_real_
    input_data$v_vaccine_incomplete_health <- NA_real_
    input_data$v_vaccine_incomplete_other <- NA_real_
    input_data$v_vaccine_incomplete_pregnant <- NA_real_
    input_data$v_vaccine_incomplete_religion <- NA_real_
  }

if ("C2" %in% names(input_data)) {
    # Coded as 1 = "Yes", 2 = "No"
    input_data$v_flu_vaccinated_1y <- input_data$C2 == 1
  } else {
    input_data$v_flu_vaccinated_1y <- NA
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

  return(input_data)
}

#' Trust in various individuals and organizations for recommendations and info
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#' 
#' @return augmented data frame
code_trust <- function(input_data, wave) {
  # Drop values in vaccine_likely_<source> prior to item change date in Wave 6
  # Percentage more likely to get vaccinated if recommended by <source>
  if ("v_vaccine_likely_friends" %in% names(input_data)) {	
    input_data$v_vaccine_likely_friends[input_data$start_dt < wave6_mod_date] <- NA
  }
  if ("v_vaccine_likely_local_health" %in% names(input_data)) {	
    input_data$v_vaccine_likely_local_health[input_data$start_dt < wave6_mod_date] <- NA
  }
  if ("v_vaccine_likely_who" %in% names(input_data)) {	
    input_data$v_vaccine_likely_who[input_data$start_dt < wave6_mod_date] <- NA
  }
  if ("v_vaccine_likely_govt_health" %in% names(input_data)) {	
    input_data$v_vaccine_likely_govt_health[input_data$start_dt < wave6_mod_date] <- NA
  }
  if ("v_vaccine_likely_politicians" %in% names(input_data)) {	
    input_data$v_vaccine_likely_politicians[input_data$start_dt < wave6_mod_date] <- NA
  }

  # hesitant_vaccine_likely_<source> (discontinued as of Wave 11)
  # Percentage more likely to get vaccinated if recommended by <source> among
  # those who are hesitant
  if (all(c("v_hesitant_vaccine", "v_vaccine_likely_friends") %in% names(input_data))) {
    input_data$v_hesitant_vaccine_likely_friends <- case_when(
      input_data$v_hesitant_vaccine == 1 & input_data$v_vaccine_likely_friends == 1 ~ 1,
      input_data$v_hesitant_vaccine == 1 & input_data$v_vaccine_likely_friends == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$v_hesitant_vaccine_likely_friends <- NA_real_
  }
  
  if (all(c("v_hesitant_vaccine", "v_vaccine_likely_local_health") %in% names(input_data))) {
    input_data$v_hesitant_vaccine_likely_local_health <- case_when(
      input_data$v_hesitant_vaccine == 1 & input_data$v_vaccine_likely_local_health == 1 ~ 1,
      input_data$v_hesitant_vaccine == 1 & input_data$v_vaccine_likely_local_health == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$v_hesitant_vaccine_likely_local_health <- NA_real_
  }
  
  if (all(c("v_hesitant_vaccine", "v_vaccine_likely_who") %in% names(input_data))) {
    input_data$v_hesitant_vaccine_likely_who <- case_when(
      input_data$v_hesitant_vaccine == 1 & input_data$v_vaccine_likely_who == 1 ~ 1,
      input_data$v_hesitant_vaccine == 1 & input_data$v_vaccine_likely_who == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$v_hesitant_vaccine_likely_who <- NA_real_
  }
  
  if (all(c("v_hesitant_vaccine", "v_vaccine_likely_govt_health") %in% names(input_data))) {
    input_data$v_hesitant_vaccine_likely_govt <- case_when(
      input_data$v_hesitant_vaccine == 1 & input_data$v_vaccine_likely_govt_health == 1 ~ 1,
      input_data$v_hesitant_vaccine == 1 & input_data$v_vaccine_likely_govt_health == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$v_hesitant_vaccine_likely_govt <- NA_real_
  }
  
  if (all(c("v_hesitant_vaccine", "v_vaccine_likely_politicians") %in% names(input_data))) {
    input_data$v_hesitant_vaccine_likely_politicians <- case_when(
      input_data$v_hesitant_vaccine == 1 & input_data$v_vaccine_likely_politicians == 1 ~ 1,
      input_data$v_hesitant_vaccine == 1 & input_data$v_vaccine_likely_politicians == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$v_hesitant_vaccine_likely_politicians <- NA_real_
  }
  
  if (all(c("v_hesitant_vaccine", "v_vaccine_likely_doctors") %in% names(input_data))) {
    input_data$v_hesitant_vaccine_likely_doctors <- case_when(
      input_data$v_hesitant_vaccine == 1 & input_data$v_vaccine_likely_doctors == 1 ~ 1,
      input_data$v_hesitant_vaccine == 1 & input_data$v_vaccine_likely_doctors == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$v_hesitant_vaccine_likely_doctors <- NA_real_
  }
  
  # Replacing set of hesitant_vaccine_likely_<source> indicators as of Wave 11
  if (all(c("v_hesitant_vaccine", "i_trust_covid_info_doctors") %in% names(input_data))) {
    input_data$i_hesitant_trust_covid_info_doctors <- case_when(
      input_data$v_hesitant_vaccine == 1 & input_data$i_trust_covid_info_doctors == 1 ~ 1,
      input_data$v_hesitant_vaccine == 1 & input_data$i_trust_covid_info_doctors == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$i_hesitant_trust_covid_info_doctors <- NA_real_
  }
  
  if (all(c("v_hesitant_vaccine", "i_trust_covid_info_experts") %in% names(input_data))) {
    input_data$i_hesitant_trust_covid_info_experts <- case_when(
      input_data$v_hesitant_vaccine == 1 & input_data$i_trust_covid_info_experts == 1 ~ 1,
      input_data$v_hesitant_vaccine == 1 & input_data$i_trust_covid_info_experts == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$i_hesitant_trust_covid_info_experts <- NA_real_
  }
  
  if (all(c("v_hesitant_vaccine", "i_trust_covid_info_cdc") %in% names(input_data))) {
    input_data$i_hesitant_trust_covid_info_cdc <- case_when(
      input_data$v_hesitant_vaccine == 1 & input_data$i_trust_covid_info_cdc == 1 ~ 1,
      input_data$v_hesitant_vaccine == 1 & input_data$i_trust_covid_info_cdc == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$i_hesitant_trust_covid_info_cdc <- NA_real_
  }
  
  if (all(c("v_hesitant_vaccine", "i_trust_covid_info_govt_health") %in% names(input_data))) {
    input_data$i_hesitant_trust_covid_info_govt_health <- case_when(
      input_data$v_hesitant_vaccine == 1 & input_data$i_trust_covid_info_govt_health == 1 ~ 1,
      input_data$v_hesitant_vaccine == 1 & input_data$i_trust_covid_info_govt_health == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$i_hesitant_trust_covid_info_govt_health <- NA_real_
  }
  
  if (all(c("v_hesitant_vaccine", "i_trust_covid_info_politicians") %in% names(input_data))) {
    input_data$i_hesitant_trust_covid_info_politicians <- case_when(
      input_data$v_hesitant_vaccine == 1 & input_data$i_trust_covid_info_politicians == 1 ~ 1,
      input_data$v_hesitant_vaccine == 1 & input_data$i_trust_covid_info_politicians == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$i_hesitant_trust_covid_info_politicians <- NA_real_
  }
  
  if (all(c("v_hesitant_vaccine", "i_trust_covid_info_journalists") %in% names(input_data))) {
    input_data$i_hesitant_trust_covid_info_journalists <- case_when(
      input_data$v_hesitant_vaccine == 1 & input_data$i_trust_covid_info_journalists == 1 ~ 1,
      input_data$v_hesitant_vaccine == 1 & input_data$i_trust_covid_info_journalists == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$i_hesitant_trust_covid_info_journalists <- NA_real_
  }
  
  if (all(c("v_hesitant_vaccine", "i_trust_covid_info_friends") %in% names(input_data))) {
    input_data$i_hesitant_trust_covid_info_friends <- case_when(
      input_data$v_hesitant_vaccine == 1 & input_data$i_trust_covid_info_friends == 1 ~ 1,
      input_data$v_hesitant_vaccine == 1 & input_data$i_trust_covid_info_friends == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$i_hesitant_trust_covid_info_friends <- NA_real_
  }
  
  if (all(c("v_hesitant_vaccine", "i_trust_covid_info_religious") %in% names(input_data))) {
    input_data$i_hesitant_trust_covid_info_religious <- case_when(
      input_data$v_hesitant_vaccine == 1 & input_data$i_trust_covid_info_religious == 1 ~ 1,
      input_data$v_hesitant_vaccine == 1 & input_data$i_trust_covid_info_religious == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$i_hesitant_trust_covid_info_religious <- NA_real_
  }
  
  return(input_data)
}

#' Vaccination barriers
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#' 
#' @return augmented data frame
code_vaccine_barriers <- function(input_data, wave) {
  # hesitant_worried_vaccine_sideeffects
  # Percentage very or moderately concerned about side effects among those who
  # are hesitant
  if (all(c("v_hesitant_vaccine", "v_worried_vaccine_side_effects") %in% names(input_data))) {
    input_data$v_hesitant_worried_vaccine_sideeffects <- case_when(
      input_data$v_hesitant_vaccine == 1 & input_data$v_worried_vaccine_side_effects == 1 ~ 1,
      input_data$v_hesitant_vaccine == 1 & input_data$v_worried_vaccine_side_effects == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$v_hesitant_worried_vaccine_sideeffects <- NA_real_
  }

  # barrier_<reason>
  # Percentage of all respondents to V5a, V5b, and V5c who have X barrier to
  # choosing to get a COVID-19 vaccine
  # (# of respondents who selected X in any of V5a, V5b, V5c) / (# of
  # respondents who selected at least one option in any of V5a, V5b, V5c)
  # Created as v_hesitancy_reason_<source> in variables.R

  # hesitant_barrier_<reason>
  # Percentage of all hesitant respondents to V5 who have X barrier to choosing
  # to get a COVID-19 vaccine
  # (# of respondents who selected X in any of V5b, Vc) / (# of respondents who
  # selected at least one option in any of V5b, V5c)
  if (all(c("V5b", "V5c") %in% names(input_data))) {
    hesitancy_reasons <- coalesce(input_data$V5b, input_data$V5c)
    hesitancy_reasons <- split_options(hesitancy_reasons)
    
    input_data$v_hesitant_barrier_sideeffects <- is_selected(hesitancy_reasons, "1")
    input_data$v_hesitant_barrier_allergic <- is_selected(hesitancy_reasons, "2")
    input_data$v_hesitant_barrier_ineffective <- is_selected(hesitancy_reasons, "3")
    input_data$v_hesitant_barrier_dontneed <- is_selected(hesitancy_reasons, "4")
    input_data$v_hesitant_barrier_dislike_vaccines <- is_selected(hesitancy_reasons, "5")
    input_data$v_hesitant_barrier_not_recommended <- is_selected(hesitancy_reasons, "6")
    input_data$v_hesitant_barrier_wait_safety <- is_selected(hesitancy_reasons, "7")
    input_data$v_hesitant_barrier_low_priority <- is_selected(hesitancy_reasons, "8")
    input_data$v_hesitant_barrier_cost <- is_selected(hesitancy_reasons, "9")
    input_data$v_hesitant_barrier_distrust_vaccines <- is_selected(hesitancy_reasons, "10")
    input_data$v_hesitant_barrier_distrust_govt <- is_selected(hesitancy_reasons, "11")
    input_data$v_hesitant_barrier_health_condition <- is_selected(hesitancy_reasons, "12")
    input_data$v_hesitant_barrier_other <- is_selected(hesitancy_reasons, "13")
    input_data$v_hesitant_barrier_pregnant <- is_selected(hesitancy_reasons, "14")
    input_data$v_hesitant_barrier_religious <- is_selected(hesitancy_reasons, "15")
    input_data$v_hesitant_barrier_dislike_vaccines_generally <- is_selected(hesitancy_reasons, "16") # replacing choice 5 as of Wave 12
    
    # For waves before a given response choice existed, explicitly set the
    # derived field to missing since `is_selected` will return FALSE (meaning
    # "not selected") for them if the respondent selected at least once answer
    # choice.
    if (wave >= 11) {
      input_data$v_hesitant_barrier_allergic <- NA
      input_data$v_hesitant_barrier_not_recommended <- NA
      input_data$v_hesitant_barrier_health_condition <- NA
      input_data$v_hesitant_barrier_pregnant <- NA
    }
    if (wave == 11) {
      input_data$v_hesitant_barrier_distrust_vaccines <- NA
    }
    if (wave < 12) {
      input_data$v_hesitant_barrier_dislike_vaccines_generally <- NA
    }
    if (wave >= 12) {
      input_data$v_hesitant_barrier_dislike_vaccines <- NA
    }
  } else {
    input_data$v_hesitant_barrier_sideeffects <- NA
    input_data$v_hesitant_barrier_allergic <- NA
    input_data$v_hesitant_barrier_ineffective <- NA
    input_data$v_hesitant_barrier_dontneed <- NA
    input_data$v_hesitant_barrier_dislike_vaccines <- NA
    input_data$v_hesitant_barrier_not_recommended <- NA
    input_data$v_hesitant_barrier_wait_safety <- NA
    input_data$v_hesitant_barrier_low_priority <- NA
    input_data$v_hesitant_barrier_cost <- NA
    input_data$v_hesitant_barrier_distrust_vaccines <- NA
    input_data$v_hesitant_barrier_distrust_govt <- NA
    input_data$v_hesitant_barrier_health_condition <- NA
    input_data$v_hesitant_barrier_other <- NA
    input_data$v_hesitant_barrier_pregnant <- NA
    input_data$v_hesitant_barrier_religious <- NA
    input_data$v_hesitant_barrier_dislike_vaccines_generally <- NA
  }

  # defno_barrier_<reason>
  # Percentage of respondents who would definitely not choose to get vaccinated
  # AND who have X barrier to choosing to get a COVID-19 vaccine
  # (# of respondents who selected X in V5c) / (# of respondents who selected
  # at least one option in V5c)
  if ("V5c" %in% names(input_data)) {
    defno_reasons <- split_options(input_data$V5c)

    input_data$v_defno_barrier_sideeffects <- is_selected(defno_reasons, "1")
    input_data$v_defno_barrier_allergic <- is_selected(defno_reasons, "2")
    input_data$v_defno_barrier_ineffective <- is_selected(defno_reasons, "3")
    input_data$v_defno_barrier_dontneed <- is_selected(defno_reasons, "4")
    input_data$v_defno_barrier_dislike_vaccines <- is_selected(defno_reasons, "5")
    input_data$v_defno_barrier_not_recommended <- is_selected(defno_reasons, "6")
    input_data$v_defno_barrier_wait_safety <- is_selected(defno_reasons, "7")
    input_data$v_defno_barrier_low_priority <- is_selected(defno_reasons, "8")
    input_data$v_defno_barrier_cost <- is_selected(defno_reasons, "9")
    input_data$v_defno_barrier_distrust_vaccines <- is_selected(defno_reasons, "10")
    input_data$v_defno_barrier_distrust_govt <- is_selected(defno_reasons, "11")
    input_data$v_defno_barrier_health_condition <- is_selected(defno_reasons, "12")
    input_data$v_defno_barrier_other <- is_selected(defno_reasons, "13")
    input_data$v_defno_barrier_pregnant <- is_selected(defno_reasons, "14")
    input_data$v_defno_barrier_religious <- is_selected(defno_reasons, "15")
    input_data$v_defno_barrier_dislike_vaccines_generally <- is_selected(defno_reasons, "16") # replacing choice 5 as of Wave 12
    
    # For waves before a given response choice existed, explicitly set the
    # derived field to missing since `is_selected` will return FALSE (meaning
    # "not selected") for them if the respondent selected at least once answer
    # choice.
    if (wave >= 11) {
      input_data$v_defno_barrier_allergic <- NA
      input_data$v_defno_barrier_not_recommended <- NA
      input_data$v_defno_barrier_health_condition <- NA
      input_data$v_defno_barrier_pregnant <- NA
    }
    if (wave == 11) {
      input_data$v_defno_barrier_distrust_vaccines <- NA
    }
    if (wave < 12) {
      input_data$v_defno_barrier_dislike_vaccines_generally <- NA
    }
    if (wave >= 12) {
      input_data$v_defno_barrier_dislike_vaccines <- NA
    }
  } else {
    input_data$v_defno_barrier_sideeffects <- NA
    input_data$v_defno_barrier_allergic <- NA
    input_data$v_defno_barrier_ineffective <- NA
    input_data$v_defno_barrier_dontneed <- NA
    input_data$v_defno_barrier_dislike_vaccines <- NA
    input_data$v_defno_barrier_not_recommended <- NA
    input_data$v_defno_barrier_wait_safety <- NA
    input_data$v_defno_barrier_low_priority <- NA
    input_data$v_defno_barrier_cost <- NA
    input_data$v_defno_barrier_distrust_vaccines <- NA
    input_data$v_defno_barrier_distrust_govt <- NA
    input_data$v_defno_barrier_health_condition <- NA
    input_data$v_defno_barrier_other <- NA
    input_data$v_defno_barrier_pregnant <- NA
    input_data$v_defno_barrier_religious <- NA
    input_data$v_defno_barrier_dislike_vaccines_generally <- NA
  }

# dontneed_reason_<reason>
  # Percentage of all respondents to (V5a, V5b, OR V5c) AND V6 who don’t
  # believe they need a COVID-19 vaccine for X reason
  # (# of respondents who selected “I don't believe I need a COVID-19 vaccine.”
  # in any of V5a, V5b, V5c) AND selected X in V6./ (# of respondents who
  # selected at least one option in any of V5a, V5b, V5c AND selected at least
  # one option in V6)
  if ("v_hesitancy_reason_unnecessary" %in% names(input_data)) {
    # v_hesitancy_reason_unnecessary is those who answered that they don't need the vaccine
    # to any of questions V5a, V5b, or V6c. It is created originally as
    # v_hesitancy_reason_unnecessary in variables.R.
    dontneed <- input_data$v_hesitancy_reason_unnecessary == 1

    if ("v_dontneed_reason_had_covid" %in% names(input_data)) {
      input_data$v_dontneed_reason_had_covid_5abc_6 <-
        all_true(dontneed, input_data$v_dontneed_reason_had_covid)
    } else {
      input_data$v_dontneed_reason_had_covid_5abc_6 <- NA
    }

    if ("v_dontneed_reason_dont_spend_time" %in% names(input_data)) {
      input_data$v_dontneed_reason_dont_spend_time_5abc_6 <-
        all_true(dontneed, input_data$v_dontneed_reason_dont_spend_time)
    } else {
      input_data$v_dontneed_reason_dont_spend_time_5abc_6 <- NA
    }

    if ("v_dontneed_reason_not_high_risk" %in% names(input_data)) {
      input_data$v_dontneed_reason_not_high_risk_5abc_6 <-
        all_true(dontneed, input_data$v_dontneed_reason_not_high_risk)
    } else {
      input_data$v_dontneed_reason_not_high_risk_5abc_6 <- NA
    }

    if ("v_dontneed_reason_precautions" %in% names(input_data)) {
      input_data$v_dontneed_reason_precautions_5abc_6 <-
        all_true(dontneed, input_data$v_dontneed_reason_precautions)
    } else {
      input_data$v_dontneed_reason_precautions_5abc_6 <- NA
    }

    if ("v_dontneed_reason_not_serious" %in% names(input_data)) {
      input_data$v_dontneed_reason_not_serious_5abc_6 <-
        all_true(dontneed, input_data$v_dontneed_reason_not_serious)
    } else {
      input_data$v_dontneed_reason_not_serious_5abc_6 <- NA
    }

    if ("v_dontneed_reason_not_beneficial" %in% names(input_data)) {
      input_data$v_dontneed_reason_not_beneficial_5abc_6 <-
        all_true(dontneed, input_data$v_dontneed_reason_not_beneficial)
    } else {
      input_data$v_dontneed_reason_not_beneficial_5abc_6 <- NA
    }

    if ("v_dontneed_reason_other" %in% names(input_data)) {
      input_data$v_dontneed_reason_other_5abc_6 <-
        all_true(dontneed, input_data$v_dontneed_reason_other)
    } else {
      input_data$v_dontneed_reason_other_5abc_6 <- NA
    }
  } else {
    input_data$v_dontneed_reason_had_covid_5abc_6 <- NA
    input_data$v_dontneed_reason_dont_spend_time_5abc_6 <- NA
    input_data$v_dontneed_reason_not_high_risk_5abc_6 <- NA
    input_data$v_dontneed_reason_precautions_5abc_6 <- NA
    input_data$v_dontneed_reason_not_serious_5abc_6 <- NA
    input_data$v_dontneed_reason_not_beneficial_5abc_6 <- NA
    input_data$v_dontneed_reason_other_5abc_6 <- NA
  }

  # hesitant_dontneed_reason_<reason>
  # Percentage of all hesitant respondents to V5 AND V6 who don’t believe
  # they need a COVID-19 vaccine for X reason
  # (# of respondents who selected “I don't believe I need a COVID-19 vaccine.”
  # in any of V5b, V5c) AND selected X in V6./ (# of respondents who selected
  # at least one option in any of V5b, V5c AND selected at least one option in
  # V6)
  if ("v_hesitant_barrier_dontneed" %in% names(input_data)) {
    dontneed <- input_data$v_hesitant_barrier_dontneed == 1
    
    if ("v_dontneed_reason_had_covid" %in% names(input_data)) {
      input_data$v_hesitant_dontneed_reason_had_covid <-
        all_true(dontneed, input_data$v_dontneed_reason_had_covid)
    } else {
      input_data$v_hesitant_dontneed_reason_had_covid <- NA
    }
    
    if ("v_dontneed_reason_dont_spend_time" %in% names(input_data)) {
      input_data$v_hesitant_dontneed_reason_dont_spend_time <-
        all_true(dontneed, input_data$v_dontneed_reason_dont_spend_time)
    } else {
      input_data$v_hesitant_dontneed_reason_dont_spend_time <- NA
    }
    
    if ("v_dontneed_reason_not_high_risk" %in% names(input_data)) {
      input_data$v_hesitant_dontneed_reason_not_high_risk <-
        all_true(dontneed, input_data$v_dontneed_reason_not_high_risk)
    } else {
      input_data$v_hesitant_dontneed_reason_not_high_risk <- NA
    }
    
    if ("v_dontneed_reason_precautions" %in% names(input_data)) {
      input_data$v_hesitant_dontneed_reason_precautions <-
        all_true(dontneed, input_data$v_dontneed_reason_precautions)
    } else {
      input_data$v_hesitant_dontneed_reason_precautions <- NA
    }
    
    if ("v_dontneed_reason_not_serious" %in% names(input_data)) {
      input_data$v_hesitant_dontneed_reason_not_serious <-
        all_true(dontneed, input_data$v_dontneed_reason_not_serious)
    } else {
      input_data$v_hesitant_dontneed_reason_not_serious <- NA
    }
    
    if ("v_dontneed_reason_not_beneficial" %in% names(input_data)) {
      input_data$v_hesitant_dontneed_reason_not_beneficial <-
        all_true(dontneed, input_data$v_dontneed_reason_not_beneficial)
    } else {
      input_data$v_hesitant_dontneed_reason_not_beneficial <- NA
    }
    
    if ("v_dontneed_reason_other" %in% names(input_data)) {
      input_data$v_hesitant_dontneed_reason_other <-
        all_true(dontneed, input_data$v_dontneed_reason_other)
    } else {
      input_data$v_hesitant_dontneed_reason_other <- NA
    }
  } else {
    input_data$v_hesitant_dontneed_reason_had_covid <- NA
    input_data$v_hesitant_dontneed_reason_dont_spend_time <- NA
    input_data$v_hesitant_dontneed_reason_not_high_risk <- NA
    input_data$v_hesitant_dontneed_reason_precautions <- NA
    input_data$v_hesitant_dontneed_reason_not_serious <- NA
    input_data$v_hesitant_dontneed_reason_not_beneficial <- NA
    input_data$v_hesitant_dontneed_reason_other <- NA
  }
  
  # defno_dontneed_reason_<reason>
  # Percentage of respondents who would definitely not choose to get vaccinated
  # AND who don’t believe they need a COVID-19 vaccine for X
  # reason
  # (# of respondents who selected “I don't believe I need a COVID-19 vaccine.”
  # in V5c) AND selected X in V6./ (# of respondents who selected at least one
  # option in V5c AND selected at least one option in V6)
  if ("v_defno_barrier_dontneed" %in% names(input_data)) {
    dontneed <- input_data$v_defno_barrier_dontneed == 1
    
    if ("v_dontneed_reason_had_covid" %in% names(input_data)) {
      input_data$v_defno_dontneed_reason_had_covid <-
        all_true(dontneed, input_data$v_dontneed_reason_had_covid)
    } else {
      input_data$v_defno_dontneed_reason_had_covid <- NA
    }
    
    if ("v_dontneed_reason_dont_spend_time" %in% names(input_data)) {
      input_data$v_defno_dontneed_reason_dont_spend_time <-
        all_true(dontneed, input_data$v_dontneed_reason_dont_spend_time)
    } else {
      input_data$v_defno_dontneed_reason_dont_spend_time <- NA
    }
    
    if ("v_dontneed_reason_not_high_risk" %in% names(input_data)) {
      input_data$v_defno_dontneed_reason_not_high_risk <-
        all_true(dontneed, input_data$v_dontneed_reason_not_high_risk)
    } else {
      input_data$v_defno_dontneed_reason_not_high_risk <- NA
    }
    
    if ("v_dontneed_reason_precautions" %in% names(input_data)) {
      input_data$v_defno_dontneed_reason_precautions <-
        all_true(dontneed, input_data$v_dontneed_reason_precautions)
    } else {
      input_data$v_defno_dontneed_reason_precautions <- NA
    }
    
    if ("v_dontneed_reason_not_serious" %in% names(input_data)) {
      input_data$v_defno_dontneed_reason_not_serious <-
        all_true(dontneed, input_data$v_dontneed_reason_not_serious)
    } else {
      input_data$v_defno_dontneed_reason_not_serious <- NA
    }
    
    if ("v_dontneed_reason_not_beneficial" %in% names(input_data)) {
      input_data$v_defno_dontneed_reason_not_beneficial <-
        all_true(dontneed, input_data$v_dontneed_reason_not_beneficial)
    } else {
      input_data$v_defno_dontneed_reason_not_beneficial <- NA
    }
    
    if ("v_dontneed_reason_other" %in% names(input_data)) {
      input_data$v_defno_dontneed_reason_other <-
        all_true(dontneed, input_data$v_dontneed_reason_other)
    } else {
      input_data$v_defno_dontneed_reason_other <- NA
    }
  } else {
    input_data$v_defno_dontneed_reason_had_covid <- NA
    input_data$v_defno_dontneed_reason_dont_spend_time <- NA
    input_data$v_defno_dontneed_reason_not_high_risk <- NA
    input_data$v_defno_dontneed_reason_precautions <- NA
    input_data$v_defno_dontneed_reason_not_serious <- NA
    input_data$v_defno_dontneed_reason_not_beneficial <- NA
    input_data$v_defno_dontneed_reason_other <- NA
  }
  
  return(input_data)
}

#' Attempts to get vaccinated
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#' 
#' @return augmented data frame
code_attempt_vaccine <- function(input_data, wave) {
  # informed_access
  # Percentage of respondents who are very or moderately informed about how to
  # get a vaccination
  # # very or moderately / # responses
  if ("V13" %in% names(input_data)) {
    input_data$v_informed_access <- case_when(
      input_data$V13 %in% c(1, 2) ~ 1,
      input_data$V13 %in% c(3, 4) ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$v_informed_access <- NA_real_
  }
  
  # appointment_have (discontinued as of Wave 11)
  # Percentage of people who have an appointment to get a COVID-19 vaccine
  # conditional on being accepting
  if ("V11" %in% names(input_data)) {
    input_data$v_appointment_have <- input_data$V11 == 1
  } else {
    input_data$v_appointment_have <- NA
  }
  
  # appointment_tried (discontinued as of Wave 11)
  # Percentage of people without an appointment who have tried to get one
  # conditional on being accepting
  if ("V12" %in% names(input_data)) {
    input_data$v_appointment_tried <- input_data$V12 == 1
  } else {
    input_data$v_appointment_tried <- NA
  }
  
  # vaccine_tried
  # Percentage of people without an appointment who have tried to get a vaccine
  # conditional on being accepting
  if ("V12a" %in% names(input_data)) {
    # 1 = "yes", 2 = "no", no "I don't know" option
    input_data$v_vaccine_tried <- case_when(
      input_data$v_accept_covid_vaccine_no_appointment == 1 ~ input_data$V12a == 1,
      TRUE ~ NA
    )
  } else {
    input_data$v_vaccine_tried <- NA
  }
  
  return(input_data)
}

#' COVID symptoms with modifications for contingency tables
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#' 
#' @return augmented data frame
code_addl_symptoms <- function(input_data, wave) {
  if ("B2b" %in% names(input_data)) {
    # How many days have you had one or more new or unusual symptom?
    # Free response
    suppressWarnings({ B2b_int <- as.integer(input_data$B2b) })
    input_data$symp_n_days <- case_when(
      B2b_int < 0 ~ NA_integer_,
      B2b_int > 1000 ~ NA_integer_,
      is.na(B2b_int) ~ NA_integer_,
      TRUE ~ B2b_int
    )
  } else {
    input_data$symp_n_days <- NA_integer_
  }

  if ("A4" %in% names(input_data)) {
    # How many additional people in your community do you know who are sick with fever + another symptom?
    # Free response
    suppressWarnings({ A4_int <- as.integer(input_data$A4) })
    input_data$community_number_sick <- case_when(
      A4_int < 0 ~ NA_integer_,
      A4_int > 100 ~ NA_integer_,
      is.na(A4_int) ~ NA_integer_,
      TRUE ~ A4_int
    )
  } else {
    input_data$community_number_sick <- NA_integer_
  }

  # anosmia
  # Percentage of respondents experiencing anosmia
  # loss of taste or smell / # any B2 response
  if ("B2" %in% names(input_data)) {
    symptoms <- split_options(input_data$B2)
    
    input_data$symp_fever <- is_selected(symptoms, "1")
    input_data$symp_cough <- is_selected(symptoms, "2")
    input_data$symp_shortness_breath <- is_selected(symptoms, "3")
    input_data$symp_diff_breathing <- is_selected(symptoms, "4")
    input_data$symp_fatigue <- is_selected(symptoms, "5")
    input_data$symp_nasal_congestion <- is_selected(symptoms, "6")
    input_data$symp_runny_nose <- is_selected(symptoms, "7")
    input_data$symp_aches <- is_selected(symptoms, "8")
    input_data$symp_sore_throat <- is_selected(symptoms, "9")
    input_data$symp_chest_pain <- is_selected(symptoms, "10")
    input_data$symp_nausea <- is_selected(symptoms, "11")
    input_data$symp_diarrhea <- is_selected(symptoms, "12")
    input_data$symp_loss_smell_taste <- is_selected(symptoms, "13")
    input_data$symp_other <- is_selected(symptoms, "14")
    input_data$symp_none <- is_selected(symptoms, "15")
    input_data$symp_eye_pain <- is_selected(symptoms, "16")
    input_data$symp_chills <- is_selected(symptoms, "17")
    input_data$symp_headache <- is_selected(symptoms, "18")
    input_data$symp_sleep_changes <- is_selected(symptoms, "19")
    input_data$symp_stuffy_nose <- is_selected(symptoms, "20")
  } else {
    input_data$symp_fever <- NA
    input_data$symp_cough <- NA
    input_data$symp_shortness_breath <- NA
    input_data$symp_diff_breathing <- NA
    input_data$symp_fatigue <- NA
    input_data$symp_nasal_congestion <- NA
    input_data$symp_runny_nose <- NA
    input_data$symp_aches <- NA
    input_data$symp_sore_throat <- NA
    input_data$symp_chest_pain <- NA
    input_data$symp_nausea <- NA
    input_data$symp_diarrhea <- NA
    input_data$symp_loss_smell_taste <- NA
    input_data$symp_other <- NA
    input_data$symp_none <- NA
    input_data$symp_eye_pain <- NA
    input_data$symp_chills <- NA
    input_data$symp_headache <- NA
    input_data$symp_sleep_changes <- NA
    input_data$symp_stuffy_nose <- NA
  }
  
  calc_unusual_given_symptom <- function(symptom, unusual_symptom) {
    case_when(
      symptom & unusual_symptom ~ TRUE,
      symptom & !unusual_symptom ~ FALSE,
      TRUE ~ NA
    )
  }
  
  if ("B2c" %in% names(input_data)) {
    symptoms <- split_options(input_data$B2c)
    
    input_data$symp_fever_unusual <- calc_unusual_given_symptom(
      input_data$symp_fever, is_selected(symptoms, "1")
    )
    input_data$symp_cough_unusual <- calc_unusual_given_symptom(
      input_data$symp_cough, is_selected(symptoms, "2")
    )
    input_data$symp_shortness_breath_unusual <- calc_unusual_given_symptom(
      input_data$symp_shortness_breath, is_selected(symptoms, "3")
    )
    input_data$symp_diff_breathing_unusual <- calc_unusual_given_symptom(
      input_data$symp_diff_breathing, is_selected(symptoms, "4")
    )
    input_data$symp_fatigue_unusual <- calc_unusual_given_symptom(
      input_data$symp_fatigue, is_selected(symptoms, "5")
    )
    input_data$symp_nasal_congestion_unusual <- calc_unusual_given_symptom(
      input_data$symp_nasal_congestion, is_selected(symptoms, "6")
    )
    input_data$symp_runny_nose_unusual <- calc_unusual_given_symptom(
      input_data$symp_runny_nose, is_selected(symptoms, "7")
    )
    input_data$symp_aches_unusual <- calc_unusual_given_symptom(
      input_data$symp_aches, is_selected(symptoms, "8")
    )
    input_data$symp_sore_throat_unusual <- calc_unusual_given_symptom(
      input_data$symp_sore_throat, is_selected(symptoms, "9")
    )
    input_data$symp_chest_pain_unusual <- calc_unusual_given_symptom(
      input_data$symp_chest_pain, is_selected(symptoms, "10")
    )
    input_data$symp_nausea_unusual <- calc_unusual_given_symptom(
      input_data$symp_nausea, is_selected(symptoms, "11")
    )
    input_data$symp_diarrhea_unusual <- calc_unusual_given_symptom(
      input_data$symp_diarrhea, is_selected(symptoms, "12")
    )
    input_data$symp_loss_smell_taste_unusual <- calc_unusual_given_symptom(
      input_data$symp_loss_smell_taste, is_selected(symptoms, "13")
    )
    input_data$symp_eye_pain_unusual <- calc_unusual_given_symptom(
      input_data$symp_eye_pain, is_selected(symptoms, "16")
    )
    input_data$symp_chills_unusual <- calc_unusual_given_symptom(
      input_data$symp_chills, is_selected(symptoms, "17")
    )
    input_data$symp_headache_unusual <- calc_unusual_given_symptom(
      input_data$symp_headache, is_selected(symptoms, "18")
    )
    input_data$symp_sleep_changes_unusual <- calc_unusual_given_symptom(
      input_data$symp_sleep_changes, is_selected(symptoms, "19")
    )
    input_data$symp_stuffy_nose_unusual <- calc_unusual_given_symptom(
      input_data$symp_stuffy_nose, is_selected(symptoms, "20")
    )
  } else {
    input_data$symp_fever_unusual <- NA
    input_data$symp_cough_unusual <- NA
    input_data$symp_shortness_breath_unusual <- NA
    input_data$symp_diff_breathing_unusual <- NA
    input_data$symp_fatigue_unusual <- NA
    input_data$symp_nasal_congestion_unusual <- NA
    input_data$symp_runny_nose_unusual <- NA
    input_data$symp_aches_unusual <- NA
    input_data$symp_sore_throat_unusual <- NA
    input_data$symp_chest_pain_unusual <- NA
    input_data$symp_nausea_unusual <- NA
    input_data$symp_diarrhea_unusual <- NA
    input_data$symp_loss_smell_taste_unusual <- NA
    input_data$symp_eye_pain_unusual <- NA
    input_data$symp_chills_unusual <- NA
    input_data$symp_headache_unusual <- NA
    input_data$symp_sleep_changes_unusual <- NA
    input_data$symp_stuffy_nose_unusual <- NA
  }

  if ("B4" %in% names(input_data)) {
    # Cough with mucus given have cough in last 1 day
    input_data$symp_cough_mucus <- input_data$B4 == 1
  } else {
    input_data$symp_cough_mucus <- NA
  }
  
  return(input_data)
}

#' Behaviors
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#' 
#' @return augmented data frame
code_behaviors <- function(input_data, wave) {
  # direct_contact (discontinued as of Wave 11)
  # Percentage of respondents that have reported having had direct contact 
  # (longer than 5 minutes) with people not staying with them.
  # "respondent = someone who answered any of the four contact types
  # (responses to at least one contact type > 0) / # (responses to at least one
  # contact type)"
  if (all(c("C10_1_1", "C10_2_1",
            "C10_3_1", "C10_4_1") %in% names(input_data))) {
    input_data$c_direct_contact <- any_true(
      input_data$C10_1_1 > 0,
      input_data$C10_2_1 > 0,
      input_data$C10_3_1 > 0,
      input_data$C10_4_1 > 0
    )
  } else {
    input_data$c_direct_contact <- NA
  }

  if ("C11" %in% names(input_data)) {
    # Had "direct contact" with someone COVID-positive in the last 24 hours
    # Coded as 1 = Yes, 2 = No
    input_data$c_direct_contact_covid <- case_when(
      input_data$C11 == 1 ~ 1,
      input_data$C11 == 2 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$c_direct_contact_covid <- NA_real_
  }

  if (all(c("C11", "C12") %in% names(input_data))) {
    # C12: was the person in C11 a member of your household
    # Coded as 1 = Yes, 2 = No
    input_data$c_direct_contact_covid_hh <- case_when(
      input_data$C11 == 1 & input_data$C12 == 1 ~ 1,
      input_data$C11 == 2 | input_data$C12 == 2 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$c_direct_contact_covid_hh <- NA_real_
  }

  # avoid_contact
  # Percentage of respondents that have reported having intentionally avoided
  # contact with other people all or most of the time
  # 1 = all of the time, 2 = most of the time, 3 = some of the time,
  # 4 = none of the time
  if ("C7" %in% names(input_data)) {
    input_data$c_avoid_contact <- case_when(
      input_data$C7 %in% c(1, 2) ~ 1,
      input_data$C7 %in% c(3, 4) ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$c_avoid_contact <- NA_real_
  }

  # avoid_contact_7d
  # Percentage of respondents that have reported having intentionally avoided
  # contact with other people all or most of the time in the last 7 days
  # 1 = all of the time, 2 = most of the time, 3 = some of the time,
  # 4 = a little of the time, 5 = none of the time
  if ("C7a" %in% names(input_data)) {
    input_data$c_avoid_contact_7d <- case_when(
      input_data$C7a %in% c(1, 2) ~ 1,
      input_data$C7a %in% c(3, 4, 5) ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$c_avoid_contact_7d <- NA_real_
  }
  
  return(input_data)
}

#' Additional mental health indicators
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#'
#' @return augmented data frame
code_addl_mental_health <- function(input_data, wave) {
  input_data$mh_some_anxious <- NA
  input_data$mh_some_anxious_7d <- NA
  input_data$mh_some_depressed <- NA
  input_data$mh_some_depressed_7d <- NA
  input_data$mh_some_isolated <- NA
  input_data$mh_some_isolated_7d <- NA
  input_data$mh_very_worried_finances <- NA

  if (wave >= 4 && wave < 10) {
    # All coded as 1 = none of the time, 2 = some of the time, 3 = most of the time, 4 = all of the time
    input_data$mh_some_anxious <- input_data$C8_1 == 3 | input_data$C8_1 == 4 | input_data$C8_1 == 2
    input_data$mh_some_depressed <- input_data$C8_2 == 3 | input_data$C8_2 == 4 | input_data$C8_2 == 2
    input_data$mh_some_isolated <- input_data$C8_3 == 3 | input_data$C8_3 == 4 | input_data$C8_3 == 2
    # Coded as 1 = very worried, 2 = somewhat worried, 3 = not too worried, 4 = not worried at all
    input_data$mh_very_worried_finances <- input_data$C15 == 1
  } else if (wave == 10) {
    # All coded as 1 = none of the time, 2 = some of the time, 3 = most of the time, 4 = all of the time
    input_data$mh_some_anxious_7d <- input_data$C8a_1 == 3 | input_data$C8a_1 == 4 | input_data$C8a_1 == 2
    input_data$mh_some_depressed_7d <- input_data$C8a_2 == 3 | input_data$C8a_2 == 4 | input_data$C8a_2 == 2
    input_data$mh_some_isolated_7d <- input_data$C8a_3 == 3 | input_data$C8a_3 == 4 | input_data$C8a_3 == 2
    # Coded as 1 = very worried, 2 = somewhat worried, 3 = not too worried, 4 = not worried at all
    input_data$mh_very_worried_finances <- input_data$C15 == 1
  } else if (wave >= 11) {
    # All coded as 1 = none of the time, 2 = some of the time, 3 = most of the time, 4 = all of the time
    input_data$mh_some_anxious_7d <- input_data$C18a == 3 | input_data$C18a == 4 | input_data$C18a == 2
    input_data$mh_some_depressed_7d <- input_data$C18b == 3 | input_data$C18b == 4 | input_data$C18b == 2
    # Coded as 1 = very worried, 2 = somewhat worried, 3 = not too worried, 4 = not worried at all
    input_data$mh_very_worried_finances <- input_data$C15 == 1
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

#' Activities
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#' 
#' @return augmented data frame
code_addl_activities <- function(input_data, wave) {
  # Work outside the home
  if (all(c("D9", "D10") %in% names(input_data))) {
    # D9: in the past 4 weeks, did you work
    # Coded as 1 = Yes, 2 = No
    # D10: if answered yes to D9, was your work in the last 4w outisde your home
    # Coded as 1 = Yes, 2 = No
    input_data$a_work_outside_home_4w <- case_when(
      input_data$D9 == 1 & input_data$D10 == 1 ~ 1,
      input_data$D9 == 2 | input_data$D10 == 2 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    input_data$a_work_outside_home_4w <- NA_real_
  }

  if ("D10" %in% names(input_data)) {
    # D10: if answered yes to D9, was your work in the last 4w outisde your home
    # Coded as 1 = Yes, 2 = No
    input_data$a_work_for_pay_outside_home_4w <- input_data$D10 == 1
  } else {
    input_data$a_work_for_pay_outside_home_4w <- NA_real_
  }

  if ("D9" %in% names(input_data)) {
    # D9: in the past 4 weeks, did you work
    # Coded as 1 = Yes, 2 = No
    input_data$a_work_for_pay_4w <- input_data$D9 == 1
  } else {
    input_data$a_work_for_pay_4w <- NA_real_
  }


  calc_masking_given_activity <- function(activity, masked_during_activity) {
    case_when(
      activity & masked_during_activity ~ TRUE,
      activity & !masked_during_activity ~ FALSE,
      TRUE ~ NA
    )
  }
  
  if ("C13a" %in% names(input_data)) {
    # introduced in wave 4
    activities <- split_options(input_data$C13a)
    
    input_data$a_mask_work_outside_home_1d <- calc_masking_given_activity(
      input_data$a_work_outside_home_1d, is_selected(activities, "1")
    )
    input_data$a_mask_shop_1d <- calc_masking_given_activity(
      input_data$a_shop_1d, is_selected(activities, "2")
    )
    input_data$a_mask_restaurant_1d <- calc_masking_given_activity(
      input_data$a_restaurant_1d, is_selected(activities, "3")
    )
    input_data$a_mask_spent_time_1d <- calc_masking_given_activity(
      input_data$a_spent_time_1d, is_selected(activities, "4")
    )
    input_data$a_mask_large_event_1d <- calc_masking_given_activity(
      input_data$a_large_event_1d, is_selected(activities, "5")
    )
    input_data$a_mask_public_transit_1d <- calc_masking_given_activity(
      input_data$a_public_transit_1d, is_selected(activities, "6")
    )
  } else {
    input_data$a_mask_work_outside_home_1d <- NA
    input_data$a_mask_shop_1d <- NA
    input_data$a_mask_restaurant_1d <- NA
    input_data$a_mask_spent_time_1d <- NA
    input_data$a_mask_large_event_1d <- NA
    input_data$a_mask_public_transit_1d <- NA
  }
  
  if ("C13c" %in% names(input_data)) {
    # introduced in wave 10 as "indoors" activities version of C13a
    activities <- split_options(input_data$C13c)
    
    input_data$a_mask_work_outside_home_indoors_1d <- calc_masking_given_activity(
      input_data$a_work_outside_home_indoors_1d, is_selected(activities, "1")
    )
    input_data$a_mask_shop_indoors_1d <- calc_masking_given_activity(
      input_data$a_shop_indoors_1d, is_selected(activities, "2")
    )
    input_data$a_mask_restaurant_indoors_1d <- calc_masking_given_activity(
      input_data$a_restaurant_indoors_1d, is_selected(activities, "3")
    )
    input_data$a_mask_spent_time_indoors_1d <- calc_masking_given_activity(
      input_data$a_spent_time_indoors_1d, is_selected(activities, "4")
    )
    input_data$a_mask_large_event_indoors_1d <- calc_masking_given_activity(
      input_data$a_large_event_indoors_1d, is_selected(activities, "5")
    )
    input_data$a_mask_public_transit_1d <- calc_masking_given_activity(
      input_data$a_public_transit_1d, is_selected(activities, "6")
    )
  } else {
    input_data$a_mask_work_outside_home_indoors_1d <- NA
    input_data$a_mask_shop_indoors_1d <- NA
    input_data$a_mask_restaurant_indoors_1d <- NA
    input_data$a_mask_spent_time_indoors_1d <- NA
    input_data$a_mask_large_event_indoors_1d <- NA
    input_data$a_mask_public_transit_1d <- NA
  }
  
  if ("C4" %in% names(input_data)) {
    # Worked/volunteered in healthcare (hospital, medical office, etc) in last 5 days
    # Coded as 1 = Yes, 2 = No
    input_data$a_work_healthcare_5d <- input_data$C4 == 1
  } else {
    input_data$a_work_healthcare_5d <- NA
  }

  if ("C5" %in% names(input_data)) {
    # Worked/visited nursing home, etc, in last 5 days
    # Coded as 1 = Yes, 2 = No
    input_data$a_work_nursing_home_5d <- input_data$C5 == 1
  } else {
    input_data$a_work_nursing_home_5d <- NA
  }

  return(input_data)
}

#' Demographics
#'
#' @param input_data input data frame of raw survey data
#' @param wave integer indicating survey version
#'
#' @return augmented data frame
code_addl_demographic <- function(input_data, wave) {
  if ("D1" %in% names(input_data)) {
    # Coded as 1 = male, 2 = female, 3 = non-binary, 4 = self-describe, 5 = no answer
    input_data$gender_male <- input_data$D1 == 1
    input_data$gender_female <- input_data$D1 == 2
    input_data$gender_nonbinary_other <- case_when(
      input_data$D1 %in% c(3, 4) ~ 1,
      input_data$D1 %in% c(1, 2, 5) ~ 0,
      TRUE ~ NA_real_
    )
    input_data$gender_unknown <- input_data$D1 == 5
  } else {
    input_data$gender_male <- NA_real_
    input_data$gender_female <- NA_real_
    input_data$gender_nonbinary_other <- NA_real_
    input_data$gender_unknown <- NA_real_
  }

  if ("D2" %in% names(input_data)) {
    # Coded as 1 = 18-24, 2 = 25-34, 3 = 35-44, 4 = 45-54,
    # 5 = 55-64, 6 = 65-74, 7 = 75+
    input_data$age_18_24 <- input_data$D2 == 1
    input_data$age_25_34 <- input_data$D2 == 2
    input_data$age_35_44 <- input_data$D2 == 3
    input_data$age_45_54 <- input_data$D2 == 4
    input_data$age_55_64 <- input_data$D2 == 5
    input_data$age_65_74 <- input_data$D2 == 6
    input_data$age_75_older <- input_data$D2 == 7
  } else {
    input_data$age_18_24 <- NA
    input_data$age_25_34 <- NA
    input_data$age_35_44 <- NA
    input_data$age_45_54 <- NA
    input_data$age_55_64 <- NA
    input_data$age_65_74 <- NA
    input_data$age_75_older <- NA
  }

  # race
  if ("D7" %in% names(input_data)) {
    input_data$race_american_indian_alaska_native <- input_data$D7 == 1
    input_data$race_asian <- input_data$D7 == 2
    input_data$race_black_african_american <- input_data$D7 == 3
    input_data$race_native_hawaiian_pacific_islander <- input_data$D7 == 4
    input_data$race_white <- input_data$D7 == 5
    input_data$race_multiple_other <- (input_data$D7 == 6 | grepl(",", input_data$D7))
  } else {
    input_data$race_american_indian_alaska_native <- NA
    input_data$race_asian <- NA
    input_data$race_black_african_american <- NA
    input_data$race_native_hawaiian_pacific_islander <- NA
    input_data$race_white <- NA
    input_data$race_multiple_other <- NA
  }

  if ("D8" %in% names(input_data)) {
    input_data$education_less_than_highschool <- input_data$D8 == 1
    input_data$education_highschool_or_equivalent <- input_data$D8 == 2
    input_data$education_some_college <- input_data$D8 == 3
    input_data$education_2yr_degree <- input_data$D8 == 4
    input_data$education_4yr_degree <- input_data$D8 == 5
    input_data$education_masters <- input_data$D8 == 8
    input_data$education_professional_degree <- input_data$D8 == 6
    input_data$education_doctorate <- input_data$D8 == 7
  } else {
    input_data$education_less_than_highschool <- NA
    input_data$education_highschool_or_equivalent <- NA
    input_data$education_some_college <- NA
    input_data$education_2yr_degree <- NA
    input_data$education_4yr_degree <- NA
    input_data$education_masters <- NA
    input_data$education_professional_degree <- NA
    input_data$education_doctorate <- NA
  }

  if ("D12" %in% names(input_data)) {
    input_data$language_home_english <- input_data$D12 == 1
    input_data$language_home_spanish <- input_data$D12 == 2
    input_data$language_home_chinese <- input_data$D12 == 3
    input_data$language_home_vietnamese <- input_data$D12 == 4
    input_data$language_home_french <- input_data$D12 == 5
    input_data$language_home_portugese <- input_data$D12 == 6
    input_data$language_home_other <- input_data$D12 == 7
  } else {
    input_data$language_home_english <- NA
    input_data$language_home_spanish <- NA
    input_data$language_home_chinese <- NA
    input_data$language_home_vietnamese <- NA
    input_data$language_home_french <- NA
    input_data$language_home_portugese <- NA
    input_data$language_home_other <- NA
  }

  # Children by age
  if (all(c("E1_1", "E1_2", "E1_3", "E1_4") %in% names(input_data))) {
    # All subquestions coded as 1 = Yes, 2 = No, 5 = don't know
    input_data$children_prek <- input_data$E1_1 == 1
    input_data$children_gr1_5 <- input_data$E1_2 == 1
    input_data$children_gr6_8 <- input_data$E1_3 == 1
    input_data$children_gr9_12 <- input_data$E1_4 == 1
  } else {
    input_data$children_prek <- NA
    input_data$children_gr1_5 <- NA
    input_data$children_gr6_8 <- NA
    input_data$children_gr9_12 <- NA
  }

  if ("E3" %in% names(input_data)) {
    school_measures <- split_options(input_data$E3)

    input_data$children_school_measure_mask_students <- is_selected(school_measures, "1")
    input_data$children_school_measure_mask_teachers <- is_selected(school_measures, "2")
    input_data$children_school_measure_same_teacher <- is_selected(school_measures, "3")
    input_data$children_school_measure_same_students <- is_selected(school_measures, "4")
    input_data$children_school_measure_outdoor <- is_selected(school_measures, "5")
    input_data$children_school_measure_entry <- is_selected(school_measures, "6")
    input_data$children_school_measure_class_size <- is_selected(school_measures, "7")
    input_data$children_school_measure_cafeteria <- is_selected(school_measures, "8")
    input_data$children_school_measure_playground <- is_selected(school_measures, "9")
    input_data$children_school_measure_desk_shield <- is_selected(school_measures, "10")
    input_data$children_school_measure_desk_space <- is_selected(school_measures, "11")
    input_data$children_school_measure_extracurricular <- is_selected(school_measures, "12")
    input_data$children_school_measure_supplies <- is_selected(school_measures, "14")
    input_data$children_school_measure_screening <- is_selected(school_measures, "15")
  } else {
    input_data$children_school_measure_mask_students <- NA
    input_data$children_school_measure_mask_teachers <- NA
    input_data$children_school_measure_same_teacher <- NA
    input_data$children_school_measure_same_students <- NA
    input_data$children_school_measure_outdoor <- NA
    input_data$children_school_measure_entry <- NA
    input_data$children_school_measure_class_size <- NA
    input_data$children_school_measure_cafeteria <- NA
    input_data$children_school_measure_playground <- NA
    input_data$children_school_measure_desk_shield <- NA
    input_data$children_school_measure_desk_space <- NA
    input_data$children_school_measure_extracurricular <- NA
    input_data$children_school_measure_supplies <- NA
    input_data$children_school_measure_screening <- NA
  }

  if ("P2" %in% names(input_data)) {
    input_data$child_age <- case_when(
      input_data$P2 == 1 ~ "less than 5 years",
      input_data$P2 == 2 ~ "5-11 years",
      input_data$P2 == 3 ~ "12-15 years",
      input_data$P2 == 4 ~ "16-17 years",
      TRUE ~ NA_character_
    )
  } else {
    input_data$child_age <- NA_character_
  }

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
    input_data$ppl_in_household_children <- ifelse(
      is.na(age18) + is.na(age1864) + is.na(age65) < 3,
      ifelse(is.na(age18), 0, age18),
      NA_integer_
    )
    input_data$ppl_in_household_adults <- ifelse(
      is.na(age18) + is.na(age1864) + is.na(age65) < 3,
      ifelse(is.na(age1864), 0, age1864),
      NA_integer_
    )
    input_data$ppl_in_household_older <- ifelse(
      is.na(age18) + is.na(age1864) + is.na(age65) < 3,
      ifelse(is.na(age65), 0, age65),
      NA_integer_
    )
  } else {
    input_data$ppl_in_household_children <- NA_integer_
    input_data$ppl_in_household_adults <- NA_integer_
    input_data$ppl_in_household_older <- NA_integer_
  }

  if ("D3" %in% names(input_data)) {
    # How many children younger than 18 currently stay in your household
    # Free response
    suppressWarnings({ D3_int <- as.integer(input_data$D3) })
    input_data$children_in_household <- case_when(
      D3_int < 0 ~ NA_integer_,
      D3_int > 20 ~ NA_integer_,
      is.na(D3_int) ~ NA_integer_,
      TRUE ~ D3_int
    )
  } else {
    input_data$children_in_household <- NA_integer_
  }

  if ("D4" %in% names(input_data)) {
    # How many adults 18-65 currently stay in your household
    # Free response
    suppressWarnings({ D4_int <- as.integer(input_data$D4) })
    input_data$adults_in_household <- case_when(
      D4_int < 0 ~ NA_integer_,
      D4_int > 20 ~ NA_integer_,
      is.na(D4_int) ~ NA_integer_,
      TRUE ~ D4_int
    )
  } else {
    input_data$adults_in_household <- NA_integer_
  }

  if ("D5" %in% names(input_data)) {
    # How many adults 65 or older currently stay in your household
    # Free response
    suppressWarnings({ D5_int <- as.integer(input_data$D5) })
    input_data$older_in_household <- case_when(
      D5_int < 0 ~ NA_integer_,
      D5_int > 20 ~ NA_integer_,
      is.na(D5_int) ~ NA_integer_,
      TRUE ~ D5_int
    )
  } else {
    input_data$older_in_household <- NA_integer_
  }

  return(input_data)
}
