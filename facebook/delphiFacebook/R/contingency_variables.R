## Functions handling renaming, reformatting, or recoding response columns.

#' Rename question codes to informative descriptions.
#'
#' Column names beginning with "b_" are binary (T/F/NA); with "t_" are user-
#' entered text; with "n_" are user-entered numeric; with "mc_" are multiple
#' choice (where only a single response can be selected); and with "ms_" are
#' so-called multi-select, where multiple responses can be selected.
#'
#' Only binary responses with a third "I don't know" option are mapped from
#' response codes to interpretable values. Multiple choice, multi-select, and
#' pure binary (yes/no) questions use the original numeric response codes.
#'
#' @param input_data    Data frame of individual response data
#'
#' @return Data frame with descriptive column names
#'
#' @importFrom dplyr rename
#'
#' @export
make_human_readable <- function(input_data) {
  input_data <- remap_responses(input_data)
  input_data <- create_derivative_columns(input_data)

  return(input_data)
}

#' Remap binary columns, race, and others to make more interpretable.
#'
#' @param df Data frame of individual response data.
#'
#' @return data frame of individual response data with newly mapped columns
remap_responses <- function(df) {
  msg_plain("Mapping response codes to descriptive values...")
  # Map responses with multiple races selected into a single category.
  if ("D7" %in% names(df)) {
    df[grepl(",", df$D7), "D7"] <- "multiracial"
  }

  # Map "I don't know" to NA in otherwise binary columns.
  df <- remap_response(df, "B11", c("1"=1, "2"=0, "3"=NA)) %>%
    remap_response("C17", c("1"=1, "4"=0, "2"=NA)) %>%

    remap_response("E1_1", c("1"=1, "2"=0, "5"=NA)) %>%
    remap_response("E1_2", c("1"=1, "2"=0, "5"=NA)) %>%
    remap_response("E1_3", c("1"=1, "2"=0, "5"=NA)) %>%
    remap_response("E1_4", c("1"=1, "2"=0, "5"=NA)) %>%

    remap_response("E2_1", c("2"=1, "3"=0, "4"=NA)) %>%
    remap_response("E2_2", c("2"=1, "3"=0, "4"=NA))

  ## Specifies human-readable values that response codes correspond to for each
  ## question. `default` is the value that all non-specified response codes map
  ## to. Please avoid including commas or other punctuation in replacement
  ## strings for ease of down-stream usage.
  map_old_new_responses <- list(
    D2=list(
      "map"=c(
        "1"="18-24",
        "2"="25-34",
        "3"="35-44",
        "4"="45-54",
        "5"="55-64",
        "6"="65-74",
        "7"="75plus"),
      "default"=NULL,
      "type"="mc"
    ),
    D7=list(
      "map"=c(
        "1"="American Indian or Alaska Native",
        "2"="Asian",
        "3"="Black or African American",
        "4"="Native Hawaiian or Pacific Islander",
        "5"="White",
        "6"="Other",
        "multiracial"="Multiracial"),
      "default"=NULL,
      "type"="mc"
    ),
    V3=list(
      "map"=c(
        "1"="def vaccinate",
        "2"="prob vaccinate",
        "3"="prob not vaccinate",
        "4"="def not vaccinate"),
      "default"=NULL,
      "type"="mc"
    ),
    D1=list(
      "map"=c(
        "1"="Male",
        "2"="Female",
        "3"="Other",
        "4"="Other",
        "5"=NA),
      "default"=NULL,
      "type"="mc"
    ),
    D8=list(
      "map"=c(
        "1"="Less than high school",
        "2"="High school graduate or equivalent",
        "3"="Some college",
        "4"="2 year degree",
        "5"="4 year degree",
        "8"="Master's degree",
        "6"="Professional degree",
        "7"="Doctorate"),
      "default"=NULL,
      "type"="mc"
    ),
    C1=list(
      "map"=c(
        "1"="Diabetes", # Waves 1-3; later separated into types 1 and 2
        "2"="Cancer",
        "3"="Heart disease",
        "4"="High blood pressure",
        "5"="Asthma",
        "6"="Chronic lung disease",
        "7"="Kidney disease",
        "8"="Autoimmune disorder",
        "9"="None listed",
        "10"="Type 2 diabetes",
        "11"="Compromised immune system",
        "12"="Type 1 diabetes",
        "13"="Obesity"),
      "default"=NULL,
      "type"="ms"
    ),
    Q64=list(
      "map"=c(
        "1"="Community and social",
        "2"="Education",
        "3"="Arts and media",
        "4"="Healthcare practitioner",
        "5"="Healthcare support",
        "6"="Protective",
        "7"="Food",
        "8"="Building upkeep",
        "9"="Personal care",
        "10"="Sales",
        "11"="Administrative",
        "12"="Construction and extraction",
        "13"="Maintenance and repair",
        "14"="Production",
        "15"="Transportation and delivery",
        "16"="Other"),
      "default"=NULL,
      "type"="mc"
    )
  )

  for (col_var in names(map_old_new_responses)) {
    df <- remap_response(df, col_var,
                         map_old_new_responses[[col_var]][["map"]],
                         map_old_new_responses[[col_var]][["default"]],
                         map_old_new_responses[[col_var]][["type"]]
    )
  }

  msg_plain("Finished remapping response codes")
  return(df)
}


#' Create new columns, based on existing ones, for use in aggregates.
#'
#' @param df Data frame of individual response data.
#'
#' @return data frame of individual response data with newly derived columns
#' 
#' @importFrom lubridate ymd
create_derivative_columns <- function(df) {
  wave6_mod_date <- ymd("2021-01-06", tz=tz_to)
    
  ###---
  # Grouping variables.
  ###---
  
  # age
  # agefull
  # age65plus
  if ("D2" %in% names(df)) {
    df$agefull <- df$D2

    df$age <- case_when(
      df$agefull == "18-24"  ~ "18-24",
      df$agefull == "25-34"  ~ "25-44",
      df$agefull == "35-44"  ~ "25-44",
      df$agefull == "45-54"  ~ "45-64",
      df$agefull == "55-64"  ~ "45-64",
      df$agefull == "65-74"  ~ "65plus",
      df$agefull == "75plus" ~ "65plus",
      TRUE ~ NA_character_
    )

    df$age65plus <- df$age == "65plus"
  } else {
    df$agefull <- NA_character_
    df$age <- NA_character_
    df$age65plus <- NA
  }

  # gender
  if ("D1" %in% names(df)) {
    df$gender <- df$D1
  }

  # race
  if ("D7" %in% names(df)) {
    df$race <- case_when(
        df$D7 == "American Indian or Alaska Native" ~ "AmericanIndianAlaskaNative",
        df$D7 == "Asian" ~ "Asian",
        df$D7 == "Black or African American" ~ "BlackAfricanAmerican",
        df$D7 == "Native Hawaiian or Pacific Islander" ~ "NativeHawaiianPacificIslander",
        df$D7 == "White" ~ "White",
        df$D7 == "Other" ~ "MultipleOther",
        df$D7 == "Multiracial" ~ "MultipleOther",
        TRUE ~ NA_character_
    )
  } else {
    df$race <- NA_character_
  }

  # hispanic
  if ("D6" %in% names(df)) {
    df$hispanic <- df$D6 == 1
  } else {
    df$hispanic <- NA
  }

  # raceethnicity
  if ( "hispanic" %in% names(df) &
       "race" %in% names(df) ) {
    df$raceethnicity <- case_when(
      df$hispanic ~ "Hispanic",
      !df$hispanic & df$race == "AmericanIndianAlaskaNative" ~ "NonHispanicAmericanIndianAlaskaNative",
      !df$hispanic & df$race == "Asian" ~ "NonHispanicAsian",
      !df$hispanic & df$race == "BlackAfricanAmerican" ~ "NonHispanicBlackAfricanAmerican",
      !df$hispanic & df$race == "NativeHawaiianPacificIslander" ~ "NonHispanicNativeHawaiianPacificIslander",
      !df$hispanic & df$race == "White" ~ "NonHispanicWhite",
      !df$hispanic & df$race == "Other" ~ "NonHispanicMultipleOther",
      TRUE ~ NA_character_
    )
  } else {
    df$raceethnicity <- NA_character_
  }

  # healthcareworker
  if ("Q64" %in% names(df)) {
    df$healthcareworker <-
      df$Q64 == "Healthcare support" |
      df$Q64 == "Healthcare practitioner"
  } else {
    df$healthcareworker <- NA
  }

  # pregnant
  if (all(c("D1b", "gender") %in% names(df))) {
    df$pregnant <- case_when(
      df$D1b == 1 ~ TRUE,
      df$D1b == 2 ~ FALSE,
      df$D1b == 4 ~ NA,
      df$gender == "Male" ~ NA,
      TRUE ~ NA
    )
  } else {
    df$pregnant <- NA
  }

  # smoker
  if ("D11" %in% names(df)) {
    df$smoker <- as.numeric(df$D11 == 1)
  } else {
    df$smoker <- NA
  }

  # eligible
  # comorbidheartdisease
  # comorbidcancer
  # comorbidkidneydisease
  # comorbidlungdisease
  # comorbiddiabetes
  # comorbidimmuno
  # comorbidobese
  if ("C1" %in% names(df)) {
    comorbidities <- split_options(df$C1)

    df$comorbidheartdisease <- is_selected(comorbidities, "Heart disease")
    df$comorbidcancer <- is_selected(comorbidities, "Cancer")
    df$comorbidkidneydisease <- is_selected(comorbidities, "Kidney disease")
    df$comorbidlungdisease <- is_selected(comorbidities, "Chronic lung disease")
    df$comorbiddiabetes <-
      is_selected(comorbidities, "Diabetes") |
      is_selected(comorbidities, "Type 1 diabetes") |
      is_selected(comorbidities, "Type 2 diabetes")
    df$comorbidimmuno <- is_selected(comorbidities, "Compromised immune system")
    df$comorbidobese <- is_selected(comorbidities, "Obesity")
    df$eligible <- 
      df$comorbidheartdisease |
      df$comorbidcancer |
      df$comorbidkidneydisease |
      df$comorbidlungdisease |
      df$comorbiddiabetes |
      df$comorbidimmuno
      
  } else {
    df$comorbidheartdisease <- NA
    df$comorbidcancer <- NA
    df$comorbidkidneydisease <- NA
    df$comorbidlungdisease <- NA
    df$comorbiddiabetes <- NA
    df$comorbidimmuno <- NA
    df$comorbidobese <- NA
    df$eligible <- NA
  }

  # eligiblepregsmokeobese
  if (all(c("eligible", "gender", "pregnant", "smoker", "comorbidobese") %in% names(df))) {
    df$eligiblepregsmokeobese <-
      df$eligible |
      ifelse(df$gender %in% "Male", FALSE, df$pregnant) |
      df$smoker |
      df$comorbidobese
  } else {
    df$eligiblepregsmokeobese <- NA
  }
  
  # edulevelfull
  if ("D8" %in% names(df)) {
    df$edulevelfull <- case_when(
      df$D8 == "Less than high school" ~ "LessThanHighSchool",
      df$D8 == "High school graduate or equivalent" ~ "HighSchool",
      df$D8 == "Some college" ~ "SomeCollege",
      df$D8 == "2 year degree" ~ "TwoYearDegree",
      df$D8 == "4 year degree" ~ "FourYearDegree",
      df$D8 == "Master's degree" ~ "MastersDegree",
      df$D8 == "Professional degree" ~ "ProfessionalDegree",
      df$D8 == "Doctorate" ~ "Doctorate",
      TRUE ~ NA_character_
    )
  } else {
    df$edulevelfull <- NA_character_
  }

  # edulevel
  if ("D8" %in% names(df)) {
    df$edulevel <- case_when(
      df$D8 == "Less than high school" ~ "LessThanHighSchool",
      df$D8 == "High school graduate or equivalent" ~ "HighSchool",
      df$D8 == "Some college" ~ "SomeCollege",
      df$D8 == "2 year degree" ~ "SomeCollege",
      df$D8 == "4 year degree" ~ "FourYearDegree",
      df$D8 == "Master's degree" ~ "PostGraduate",
      df$D8 == "Professional degree" ~ "PostGraduate",
      df$D8 == "Doctorate" ~ "PostGraduate",
      TRUE ~ NA_character_
    )
  } else {
    df$edulevel <- NA_character_
  }

  # occupation
  if ("Q64" %in% names(df)) {
    df$occupation <- case_when(
      df$Q64 == "Community and social" ~ "SocialService",
      df$Q64 == "Education" ~ "Education",
      df$Q64 == "Arts and media" ~ "Arts",
      df$Q64 == "Healthcare practitioner" ~ "HealthcarePractitioner",
      df$Q64 == "Healthcare support" ~ "HealthcareSupport",
      df$Q64 == "Protective" ~ "ProtectiveService",
      df$Q64 == "Food" ~ "FoodService",
      df$Q64 == "Building upkeep" ~ "BuildingMaintenance",
      df$Q64 == "Personal care" ~ "PersonalCare",
      df$Q64 == "Sales" ~ "Sales",
      df$Q64 == "Administrative" ~ "Office",
      df$Q64 == "Construction and extraction" ~ "Construction",
      df$Q64 == "Maintenance and repair" ~ "Maintenance",
      df$Q64 == "Production" ~ "Production",
      df$Q64 == "Transportation and delivery" ~ "Transportation",
      df$Q64 == "Other" ~ "Other",
      TRUE ~ NA_character_
    )
  } else {
    df$occupation <- NA_character_
  }

  ###---
  # Indicator variables
  ###---

  # wearing_mask
  # Percentage of people who wore a mask most or all of the time while in
  # public in the past 5/7 days
  # # most of the time OR all of the time / # respondents
  if ("c_mask_often" %in% names(df)) {
    df$b_wearing_mask_5d <- as.numeric(df$c_mask_often == 1)
  } else {
    df$b_wearing_mask_5d <- NA_real_
  }

  if ("c_mask_often_7d" %in% names(df)) {
    df$b_wearing_mask_7d <- as.numeric(df$c_mask_often_7d == 1)
  } else {
    df$b_wearing_mask_7d <- NA_real_
  }

  # cli
  # Percentage with COVID-like illness
  # # fever, along with cough, or shortness of breath, or difficulty breathing
  # / # any B2 response
  # defined elsewhere and copied here with the required name
  if ("hh_p_cli" %in% names(df)) {
    df$n_cli <- df$hh_p_cli
  } else {
    df$n_cli <- NA_real_
  }

  # ili
  # Percentage with influenza-like illness
  # # fever, along with cough or sore throat / # any B2 response
  # defined elsewhere and copied here with the required name
  if ("hh_p_ili" %in% names(df)) {
    df$n_ili <- df$hh_p_ili
  } else {
    df$n_ili <- NA_real_
  }

  # hh_cmnty_cli
  # Percentage reporting illness in their local community including their
  # household
  # As in API
  # made elsewhere and renamed in `rename_responses`

  # direct_contact (discontinued as of Wave 11)
  # Percentage of respondents that have reported having had direct contact 
  # (longer than 1 minute) with people not staying with them.
  # "respondent = someone who answered any of the four contact types
  # (responses to at least one contact type > 0) / # (responses to at least one
  # contact type)"
  if (all(c("C10_1_1", "C10_2_1",
            "C10_3_1", "C10_4_1") %in% names(df))) {
    df$b_direct_contact <- as.numeric(any_true(
      df$C10_1_1 > 0,
      df$C10_2_1 > 0,
      df$C10_3_1 > 0,
      df$C10_4_1 > 0
    ))
  } else {
    df$b_direct_contact <- NA_real_
  }

  # anosmia
  # Percentage of respondents experiencing anosmia
  # loss of taste or smell / # any B2 response
  if ("B2" %in% names(df)) {
    symptoms <- split_options(df$B2)
    df$b_anosmia <- as.numeric(is_selected(symptoms, "13"))
  } else {
    df$b_anosmia <- NA_real_
  }

  # vaccinated
  # Percentage vaccinated
  # # yes / # V1 responses
  # made elsewhere and renamed in `rename_responses`

  # received_2_vaccine_doses
  # Percentage receiving two doses
  # # 2 doses/ # V2 responses
  # made elsewhere and renamed in `rename_responses`

  # accept_vaccine (discontinued as of Wave 11)
  # Percentage who would definitely or probably choose to get vaccinated
  # # (yes, definitely OR yes, probably) / #V3 responses
  # made elsewhere and renamed in `rename_responses`
  
  ## Items V3 and V4 display logic was changed mid-wave 6 to be shown only to
  ## respondents indicated that they had not been vaccinated. For the purposes
  ## of the contingency tables, we will ignore responses to V3 and V4 from
  ## before the change.
  if ("v_accept_covid_vaccine" %in% names(df)) {	
    df$v_accept_covid_vaccine[df$start_dt < wave6_mod_date] <- NA
  }
  
  # accept_vaccine_no_appointment (replacing accept_vaccine as of Wave 11 to not include people with vaccine appointments)
    
  # appointment_or_accept_vaccine (replacing accept_vaccine as of Wave 11)

  # hesitant_vaccine (changing meaning as of Wave 11 to not include people with vaccine appointments)
  # Percentage who would definitely or probably NOT choose to get vaccinated
  # # (no, definitely not OR no, probably not) / #V3 responses
  df$b_hesitant_vaccine <- NA_real_
  if ("v_accept_covid_vaccine" %in% names(df)) {	
    df$b_hesitant_vaccine <- as.numeric(!df$v_accept_covid_vaccine)
  }
  if ("V3a" %in% names(df)) {
    df$b_hesitant_vaccine <- coalesce(
      df$b_hesitant_vaccine,
      as.numeric(df$V3a == 3 | df$V3a == 4)
    )
  }
  
  # vaccinated_or_accept (discontinued as of Wave 11)
  # Percentage who have either already received a COVID vaccine or would 
  # definitely or probably choose to get vaccinated, if a vaccine were offered 
  # to them today.
  # # (yes to V1) OR ((yes, definitely OR yes, probably) to V3) / 
  # # (respondents to V3 OR (yes to V1))
  # made elsewhere and renamed in `rename_responses`
  
  # vaccinated_appointment_or_accept (replacing vaccinated_or_accept as of Wave 11 to include people with vaccine appointments)

  ## All replaced by accept_vaccine_no_appointment_* as of Wave 11 to not include people with vaccine apointments
  # accept_vaccine_defyes
  # accept_vaccine_probyes
  # accept_vaccine_probno
  # accept_vaccine_defno
  # # (option chosen) / # V3 responses
  df$b_accept_vaccine_defyes <- NA_real_
  df$b_accept_vaccine_probyes <- NA_real_
  df$b_accept_vaccine_probno <- NA_real_
  df$b_accept_vaccine_defno <- NA_real_
  
  df$b_accept_vaccine_no_appointment_defyes <- NA
  df$b_accept_vaccine_no_appointment_probyes <- NA
  df$b_accept_vaccine_no_appointment_probno <- NA
  df$b_accept_vaccine_no_appointment_defno <- NA
  
  if ("V3" %in% names(df)) {
    df$V3[df$start_dt < wave6_mod_date] <- NA
    
    df$b_accept_vaccine_defyes <- as.numeric(df$V3 == "def vaccinate")
    df$b_accept_vaccine_probyes <- as.numeric(df$V3 == "prob vaccinate")
    df$b_accept_vaccine_probno <- as.numeric(df$V3 == "prob not vaccinate")
    df$b_accept_vaccine_defno <- as.numeric(df$V3 == "def not vaccinate")
  }
  if ("V3a" %in% names(df)) {	
    df$b_accept_vaccine_no_appointment_defyes <- as.numeric(df$V3a == 1)
    df$b_accept_vaccine_no_appointment_probyes <- as.numeric(df$V3a == 2)
    df$b_accept_vaccine_no_appointment_probno <- as.numeric(df$V3a == 3)
    df$b_accept_vaccine_no_appointment_defno <- as.numeric(df$V3a == 4)
  }

  # vaccine_likely_friends (discontinued as of Wave 11)
  # Percentage more likely to get vaccinated if recommended by friends & family
  # # more likely / #V4 responses
  # made elsewhere and renamed in `rename_responses`
  if ("v_vaccine_likely_friends" %in% names(df)) {	
    df$v_vaccine_likely_friends[df$start_dt < wave6_mod_date] <- NA
  }

  # vaccine_likely_local_health (discontinued as of Wave 11)
  # Percentage more likely to get vaccinated if recommended by local healthcare
  # workers
  # # more likely / #V4 responses
  # made elsewhere and renamed in `rename_responses`
  if ("v_vaccine_likely_local_health" %in% names(df)) {	
    df$v_vaccine_likely_local_health[df$start_dt < wave6_mod_date] <- NA
  }

  # vaccine_likely_who (discontinued as of Wave 11)
  # Percentage more likely to get vaccinated if recommended by WHO
  # # more likely / # V4 responses
  # made elsewhere and renamed in `rename_responses`
  if ("v_vaccine_likely_who" %in% names(df)) {	
    df$v_vaccine_likely_who[df$start_dt < wave6_mod_date] <- NA
  }
  
  # vaccine_likely_govt_health (discontinued as of Wave 11)
  # Percentage more likely to get vaccinated if recommended by government 
  # health officials
  # # more likely / # V4 responses
  # made elsewhere and renamed in `rename_responses`
  if ("v_vaccine_likely_govt_health" %in% names(df)) {	
    df$v_vaccine_likely_govt_health[df$start_dt < wave6_mod_date] <- NA
  }
  
  # vaccine_likely_politicians (discontinued as of Wave 11)
  # Percentage more likely to get vaccinated if recommended by politicians
  # # more likely / # V4 responses
  # made elsewhere and renamed in `rename_responses`
  if ("v_vaccine_likely_politicians" %in% names(df)) {	
    df$v_vaccine_likely_politicians[df$start_dt < wave6_mod_date] <- NA
  }
  
  # vaccine_likely_doctors (discontinued as of Wave 11)
  # Percentage more likely to get vaccinated if recommended by doctors and 
  # other health professionals
  # # more likely / # V4 responses
  # made elsewhere and renamed in `rename_responses`

  # worried_vaccine_sideeffects
  # Percentage very or moderately concerned about side effects
  # #(very concerned OR moderately concerned) / # V9 responses
  # made elsehwere and renamed in `rename_responses`

  # hesitant_worried_vaccine_sideeffects
  # Percentage very or moderately concerned about side effects among those who
  # are hesitant
  # # ((very concerned OR moderately concerned) AND (no, probably not OR no
  # definitely not)/# (V3 hesitant AND V9) responses
  if (all(c("b_hesitant_vaccine", "v_worried_vaccine_side_effects") %in% names(df))) {
    df$b_hesitant_worried_vaccine_sideeffects <- case_when(
      df$b_hesitant_vaccine == 1 & df$v_worried_vaccine_side_effects == 1 ~ 1,
      df$b_hesitant_vaccine == 1 & df$v_worried_vaccine_side_effects == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    df$b_hesitant_worried_vaccine_sideeffects <- NA_real_
  }

  # hesitant_vaccine_likely_friends (discontinued as of Wave 11)
  # Percentage more likely to get vaccinated if recommended by friends & family
  # among those who are hesitant
  # # more likely / # V4 responses who are also hesitant
  if (all(c("b_hesitant_vaccine", "v_vaccine_likely_friends") %in% names(df))) {
    df$b_hesitant_vaccine_likely_friends <- case_when(
      df$b_hesitant_vaccine == 1 & df$v_vaccine_likely_friends == 1 ~ 1,
      df$b_hesitant_vaccine == 1 & df$v_vaccine_likely_friends == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    df$b_hesitant_vaccine_likely_friends <- NA_real_
  }

  # hesitant_vaccine_likely_local_health (discontinued as of Wave 11)
  # Percentage more likely to get vaccinated if recommended by local healthcare
  # workers among those who are hesitant
  # # more likely / # V4 responses who are also hesitant
  if (all(c("b_hesitant_vaccine", "v_vaccine_likely_local_health") %in% names(df))) {
    df$b_hesitant_vaccine_likely_local_health <- case_when(
      df$b_hesitant_vaccine == 1 & df$v_vaccine_likely_local_health == 1 ~ 1,
      df$b_hesitant_vaccine == 1 & df$v_vaccine_likely_local_health == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    df$b_hesitant_vaccine_likely_local_health <- NA_real_
  }

  # hesitant_vaccine_likely_who (discontinued as of Wave 11)
  # Percentage more likely to get vaccinated if recommended by WHO among those
  # who are hesitant
  # # more likely / # V4 responses who are also hesitant
  if (all(c("b_hesitant_vaccine", "v_vaccine_likely_who") %in% names(df))) {
    df$b_hesitant_vaccine_likely_who <- case_when(
      df$b_hesitant_vaccine == 1 & df$v_vaccine_likely_who == 1 ~ 1,
      df$b_hesitant_vaccine == 1 & df$v_vaccine_likely_who == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    df$b_hesitant_vaccine_likely_who <- NA_real_
  }

  # hesitant_vaccine_likely_govt (discontinued as of Wave 11)
  # Percentage more likely to get vaccinated if recommended by government
  # health officials among those who are hesitant
  # # more likely / # V4 responses who are also hesitant
  if (all(c("b_hesitant_vaccine", "v_vaccine_likely_govt_health") %in% names(df))) {
    df$b_hesitant_vaccine_likely_govt <- case_when(
      df$b_hesitant_vaccine == 1 & df$v_vaccine_likely_govt_health == 1 ~ 1,
      df$b_hesitant_vaccine == 1 & df$v_vaccine_likely_govt_health == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    df$b_hesitant_vaccine_likely_govt <- NA_real_
  }

  # hesitant_vaccine_likely_politicians (discontinued as of Wave 11)
  # Percentage more likely to get vaccinated if recommended by politicians
  # among those who are hesitant
  # # more likely / # V4 responses who are also hesitant
  if (all(c("b_hesitant_vaccine", "v_vaccine_likely_politicians") %in% names(df))) {
    df$b_hesitant_vaccine_likely_politicians <- case_when(
      df$b_hesitant_vaccine == 1 & df$v_vaccine_likely_politicians == 1 ~ 1,
      df$b_hesitant_vaccine == 1 & df$v_vaccine_likely_politicians == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    df$b_hesitant_vaccine_likely_politicians <- NA_real_
  }

  # hesitant_vaccine_likely_doctors (discontinued as of Wave 11)
  if (all(c("b_hesitant_vaccine", "v_vaccine_likely_doctors") %in% names(df))) {
    df$b_hesitant_vaccine_likely_doctors <- case_when(
      df$b_hesitant_vaccine == 1 & df$v_vaccine_likely_doctors == 1 ~ 1,
      df$b_hesitant_vaccine == 1 & df$v_vaccine_likely_doctors == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    df$b_hesitant_vaccine_likely_doctors <- NA_real_
  }
  
  # Replacing set of hesitant_vaccine_likely_* indicators as of Wave 11
  if (all(c("b_hesitant_vaccine", "i_trust_covid_info_doctors") %in% names(df))) {
    df$b_hesitant_trust_covid_info_doctors <- case_when(
      df$b_hesitant_vaccine == 1 & df$i_trust_covid_info_doctors == 1 ~ 1,
      df$b_hesitant_vaccine == 1 & df$i_trust_covid_info_doctors == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    df$b_hesitant_trust_covid_info_doctors <- NA_real_
  }
  
  if (all(c("b_hesitant_vaccine", "i_trust_covid_info_experts") %in% names(df))) {
    df$b_hesitant_trust_covid_info_experts <- case_when(
      df$b_hesitant_vaccine == 1 & df$i_trust_covid_info_experts == 1 ~ 1,
      df$b_hesitant_vaccine == 1 & df$i_trust_covid_info_experts == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    df$b_hesitant_trust_covid_info_experts <- NA_real_
  }
  
  if (all(c("b_hesitant_vaccine", "i_trust_covid_info_cdc") %in% names(df))) {
    df$b_hesitant_trust_covid_info_cdc <- case_when(
      df$b_hesitant_vaccine == 1 & df$i_trust_covid_info_cdc == 1 ~ 1,
      df$b_hesitant_vaccine == 1 & df$i_trust_covid_info_cdc == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    df$b_hesitant_trust_covid_info_cdc <- NA_real_
  }
  
  if (all(c("b_hesitant_vaccine", "i_trust_covid_info_govt_health") %in% names(df))) {
    df$b_hesitant_trust_covid_info_govt_health <- case_when(
      df$b_hesitant_vaccine == 1 & df$i_trust_covid_info_govt_health == 1 ~ 1,
      df$b_hesitant_vaccine == 1 & df$i_trust_covid_info_govt_health == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    df$b_hesitant_trust_covid_info_govt_health <- NA_real_
  }
  
  if (all(c("b_hesitant_vaccine", "i_trust_covid_info_politicians") %in% names(df))) {
    df$b_hesitant_trust_covid_info_politicians <- case_when(
      df$b_hesitant_vaccine == 1 & df$i_trust_covid_info_politicians == 1 ~ 1,
      df$b_hesitant_vaccine == 1 & df$i_trust_covid_info_politicians == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    df$b_hesitant_trust_covid_info_politicians <- NA_real_
  }
  
  if (all(c("b_hesitant_vaccine", "i_trust_covid_info_journalists") %in% names(df))) {
    df$b_hesitant_trust_covid_info_journalists <- case_when(
      df$b_hesitant_vaccine == 1 & df$i_trust_covid_info_journalists == 1 ~ 1,
      df$b_hesitant_vaccine == 1 & df$i_trust_covid_info_journalists == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    df$b_hesitant_trust_covid_info_journalists <- NA_real_
  }
  
  if (all(c("b_hesitant_vaccine", "i_trust_covid_info_friends") %in% names(df))) {
    df$b_hesitant_trust_covid_info_friends <- case_when(
      df$b_hesitant_vaccine == 1 & df$i_trust_covid_info_friends == 1 ~ 1,
      df$b_hesitant_vaccine == 1 & df$i_trust_covid_info_friends == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    df$b_hesitant_trust_covid_info_friends <- NA_real_
  }
  
  if (all(c("b_hesitant_vaccine", "i_trust_covid_info_religious") %in% names(df))) {
    df$b_hesitant_trust_covid_info_religious <- case_when(
      df$b_hesitant_vaccine == 1 & df$i_trust_covid_info_religious == 1 ~ 1,
      df$b_hesitant_vaccine == 1 & df$i_trust_covid_info_religious == 0 ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    df$b_hesitant_trust_covid_info_religious <- NA_real_
  }
  

  # hesitant_barrier_sideeffects
  # hesitant_barrier_allergic
  # hesitant_barrier_ineffective
  # hesitant_barrier_dontneed
  # hesitant_barrier_dislike_vaccines
  # hesitant_barrier_not_recommended
  # hesitant_barrier_wait_safety
  # hesitant_barrier_low_priority
  # hesitant_barrier_cost
  # hesitant_barrier_distrust_vaccines
  # hesitant_barrier_religious
  # hesitant_barrier_health_condition
  # hesitant_barrier_pregnant
  # hesitant_barrier_other
  # Percentage of all hesitant respondents to V5 who have X barrier to choosing
  # to get a COVID-19 vaccine
  # (# of respondents who selected X in any of V5b, Vc) / (# of respondents who
  # selected at least one option in any of V5b, V5c)
  if (all(c("V5b", "V5c") %in% names(df))) {
    hesitancy_reasons <- coalesce(df$V5b, df$V5c)
    hesitancy_reasons <- split_options(hesitancy_reasons)
    
    df$b_hesitant_barrier_sideeffects <- as.numeric(is_selected(hesitancy_reasons, "1"))
    df$b_hesitant_barrier_allergic <- as.numeric(is_selected(hesitancy_reasons, "2"))
    df$b_hesitant_barrier_ineffective <- as.numeric(is_selected(hesitancy_reasons, "3"))
    df$b_hesitant_barrier_dontneed <- as.numeric(is_selected(hesitancy_reasons, "4"))
    df$b_hesitant_barrier_dislike_vaccines <- as.numeric(is_selected(hesitancy_reasons, "5"))
    df$b_hesitant_barrier_not_recommended <- as.numeric(is_selected(hesitancy_reasons, "6"))
    df$b_hesitant_barrier_wait_safety <- as.numeric(is_selected(hesitancy_reasons, "7"))
    df$b_hesitant_barrier_low_priority <- as.numeric(is_selected(hesitancy_reasons, "8"))
    df$b_hesitant_barrier_cost <- as.numeric(is_selected(hesitancy_reasons, "9"))
    df$b_hesitant_barrier_distrust_vaccines <- as.numeric(is_selected(hesitancy_reasons, "10"))
    df$b_hesitant_barrier_distrust_govt <- as.numeric(is_selected(hesitancy_reasons, "11"))
    df$b_hesitant_barrier_health_condition <- as.numeric(is_selected(hesitancy_reasons, "12"))
    df$b_hesitant_barrier_other <- as.numeric(is_selected(hesitancy_reasons, "13"))
    df$b_hesitant_barrier_pregnant <- as.numeric(is_selected(hesitancy_reasons, "14"))
    df$b_hesitant_barrier_religious <- as.numeric(is_selected(hesitancy_reasons, "15"))
    
    # These response choices were removed starting in Wave 11. They are explicitly set to missing
    # for waves 11 and later since `is_selected` will return FALSE (meaning "not selected") for
    # them if the respondent selected at least once answer choice.
    df$b_hesitant_barrier_allergic[df$wave >= 11] <- NA
    df$b_hesitant_barrier_not_recommended[df$wave >= 11] <- NA
    df$b_hesitant_barrier_distrust_vaccines[df$wave >= 11] <- NA
    df$b_hesitant_barrier_health_condition[df$wave >= 11] <- NA
    df$b_hesitant_barrier_pregnant[df$wave >= 11] <- NA
    
  } else {
    df$b_hesitant_barrier_sideeffects <- NA_real_
    df$b_hesitant_barrier_allergic <- NA_real_
    df$b_hesitant_barrier_ineffective <- NA_real_
    df$b_hesitant_barrier_dontneed <- NA_real_
    df$b_hesitant_barrier_dislike_vaccines <- NA_real_
    df$b_hesitant_barrier_not_recommended <- NA_real_
    df$b_hesitant_barrier_wait_safety <- NA_real_
    df$b_hesitant_barrier_low_priority <- NA_real_
    df$b_hesitant_barrier_cost <- NA_real_
    df$b_hesitant_barrier_distrust_vaccines <- NA_real_
    df$b_hesitant_barrier_distrust_govt <- NA_real_
    df$b_hesitant_barrier_health_condition <- NA_real_
    df$b_hesitant_barrier_other <- NA_real_
    df$b_hesitant_barrier_pregnant <- NA_real_
    df$b_hesitant_barrier_religious <- NA_real_
  }

  # hesitant_dontneed_reason_had_covid
  # hesitant_dontneed_reason_dont_spend_time
  # hesitant_dontneed_reason_not_high_risk
  # hesitant_dontneed_reason_precautions
  # hesitant_dontneed_reason_not_serious
  # hesitant_dontneed_reason_not_beneficial
  # hesitant_dontneed_reason_other
  # Percentage of all hesitant respondents to V5 AND V6 who don’t believe
  # they need a COVID-19 vaccine for X reason
  # (# of respondents who selected “I don't believe I need a COVID-19 vaccine.”
  # in any of V5b, V5c) AND selected X in V6./ (# of respondents who selected
  # at least one option in any of V5b, V5c AND selected at least one option in
  # V6)
  if ("b_hesitant_barrier_dontneed" %in% names(df)) {
    dontneed <- df$b_hesitant_barrier_dontneed == 1

    if ("v_dontneed_reason_had_covid" %in% names(df)) {
      df$b_hesitant_dontneed_reason_had_covid <- as.numeric(
        all_true(dontneed, df$v_dontneed_reason_had_covid)
      )
    } else {
      df$b_hesitant_dontneed_reason_had_covid <- NA_real_
    }
    
    if ("v_dontneed_reason_dont_spend_time" %in% names(df)) {
      df$b_hesitant_dontneed_reason_dont_spend_time <- as.numeric(
        all_true(dontneed, df$v_dontneed_reason_dont_spend_time)
      )
    } else {
      df$b_hesitant_dontneed_reason_dont_spend_time <- NA_real_
    }
    
    if ("v_dontneed_reason_not_high_risk" %in% names(df)) {
      df$b_hesitant_dontneed_reason_not_high_risk <- as.numeric(
        all_true(dontneed, df$v_dontneed_reason_not_high_risk)
      )
    } else {
      df$b_hesitant_dontneed_reason_not_high_risk <- NA_real_
    }
    
    if ("v_dontneed_reason_precautions" %in% names(df)) {
      df$b_hesitant_dontneed_reason_precautions <- as.numeric(
        all_true(dontneed, df$v_dontneed_reason_precautions)
      )
    } else {
      df$b_hesitant_dontneed_reason_precautions <- NA_real_
    }
    
    if ("v_dontneed_reason_not_serious" %in% names(df)) {
      df$b_hesitant_dontneed_reason_not_serious <- as.numeric(
        all_true(dontneed, df$v_dontneed_reason_not_serious)
      )
    } else {
      df$b_hesitant_dontneed_reason_not_serious <- NA_real_
    }
    
    if ("v_dontneed_reason_not_beneficial" %in% names(df)) {
      df$b_hesitant_dontneed_reason_not_beneficial <- as.numeric(
        all_true(dontneed, df$v_dontneed_reason_not_beneficial)
      )
    } else {
      df$b_hesitant_dontneed_reason_not_beneficial <- NA_real_
    }
    
    if ("v_dontneed_reason_other" %in% names(df)) {
      df$b_hesitant_dontneed_reason_other <- as.numeric(
        all_true(dontneed, df$v_dontneed_reason_other)
      )
    } else {
      df$b_hesitant_dontneed_reason_other <- NA_real_
    }
  } else {
    df$b_hesitant_dontneed_reason_had_covid <- NA_real_ 
    df$b_hesitant_dontneed_reason_dont_spend_time <- NA_real_
    df$b_hesitant_dontneed_reason_not_high_risk <- NA_real_
    df$b_hesitant_dontneed_reason_precautions <- NA_real_
    df$b_hesitant_dontneed_reason_not_serious <- NA_real_
    df$b_hesitant_dontneed_reason_not_beneficial <- NA_real_
    df$b_hesitant_dontneed_reason_other <- NA_real_
  }

  # barrier_sideeffects
  # barrier_allergic
  # barrier_ineffective
  # barrier_dontneed
  # barrier_dislike_vaccines
  # barrier_not_recommended
  # barrier_wait_safety
  # barrier_low_priority
  # barrier_cost
  # barrier_distrust_vaccines
  # barrier_distrust_govt
  # barrier_religious
  # barrier_health_condition
  # barrier_pregnant
  # barrier_other
  # Percentage of all respondents to V5a, V5b, and V5c who have X barrier to
  # choosing to get a COVID-19 vaccine
  # (# of respondents who selected X in any of V5a, V5b, V5c) / (# of
  # respondents who selected at least one option in any of V5a, V5b, V5c)
  # made elsewhere and renamed in `rename_responses`.
  
  # defno_barrier_sideeffects
  # defno_barrier_allergic
  # defno_barrier_ineffective
  # defno_barrier_dontneed
  # defno_barrier_dislike_vaccines
  # defno_barrier_not_recommended
  # defno_barrier_wait_safety
  # defno_barrier_low_priority
  # defno_barrier_cost
  # defno_barrier_distrust_vaccines
  # defno_barrier_distrust_govt
  # defno_barrier_religious
  # defno_barrier_health_condition
  # defno_barrier_pregnant
  # defno_barrier_other
  # Percentage of respondents who would definitely not choose to get vaccinated
  # AND who have X barrier to choosing to get a COVID-19 vaccine
  # (# of respondents who selected X in V5c) / (# of respondents who selected
  # at least one option in V5c)
  if ("V5c" %in% names(df)) {
    defno_reasons <- split_options(df$V5c)
    
    df$b_defno_barrier_sideeffects <- as.numeric(is_selected(defno_reasons, "1"))
    df$b_defno_barrier_allergic <- as.numeric(is_selected(defno_reasons, "2"))
    df$b_defno_barrier_ineffective <- as.numeric(is_selected(defno_reasons, "3"))
    df$b_defno_barrier_dontneed <- as.numeric(is_selected(defno_reasons, "4"))
    df$b_defno_barrier_dislike_vaccines <- as.numeric(is_selected(defno_reasons, "5"))
    df$b_defno_barrier_not_recommended <- as.numeric(is_selected(defno_reasons, "6"))
    df$b_defno_barrier_wait_safety <- as.numeric(is_selected(defno_reasons, "7"))
    df$b_defno_barrier_low_priority <- as.numeric(is_selected(defno_reasons, "8"))
    df$b_defno_barrier_cost <- as.numeric(is_selected(defno_reasons, "9"))
    df$b_defno_barrier_distrust_vaccines <- as.numeric(is_selected(defno_reasons, "10"))
    df$b_defno_barrier_distrust_govt <- as.numeric(is_selected(defno_reasons, "11"))
    df$b_defno_barrier_health_condition <- as.numeric(is_selected(defno_reasons, "12"))
    df$b_defno_barrier_other <- as.numeric(is_selected(defno_reasons, "13"))
    df$b_defno_barrier_pregnant <- as.numeric(is_selected(defno_reasons, "14"))
    df$b_defno_barrier_religious <- as.numeric(is_selected(defno_reasons, "15"))
    
    # These response choices were removed starting in Wave 11. They are explicitly set to missing
    # for waves 11 and later since `is_selected` will return FALSE (meaning "not selected") for
    # them if the respondent selected at least once answer choice.
    df$b_defno_barrier_allergic[df$wave >= 11] <- NA
    df$b_defno_barrier_not_recommended[df$wave >= 11] <- NA
    df$b_defno_barrier_distrust_vaccines[df$wave >= 11] <- NA
    df$b_defno_barrier_health_condition[df$wave >= 11] <- NA
    df$b_defno_barrier_pregnant[df$wave >= 11] <- NA
    
  } else {
    df$b_defno_barrier_sideeffects <- NA_real_
    df$b_defno_barrier_allergic <- NA_real_
    df$b_defno_barrier_ineffective <- NA_real_
    df$b_defno_barrier_dontneed <- NA_real_
    df$b_defno_barrier_dislike_vaccines <- NA_real_
    df$b_defno_barrier_not_recommended <- NA_real_
    df$b_defno_barrier_wait_safety <- NA_real_
    df$b_defno_barrier_low_priority <- NA_real_
    df$b_defno_barrier_cost <- NA_real_
    df$b_defno_barrier_distrust_vaccines <- NA_real_
    df$b_defno_barrier_distrust_govt <- NA_real_
    df$b_defno_barrier_health_condition <- NA_real_
    df$b_defno_barrier_other <- NA_real_
    df$b_defno_barrier_pregnant <- NA_real_
    df$b_defno_barrier_religious <- NA_real_
  }

  # defno_dontneed_reason_had_covid
  # defno_dontneed_reason_dont_spend_time
  # defno_dontneed_reason_not_high_risk
  # defno_dontneed_reason_precautions
  # defno_dontneed_reason_not_serious
  # defno_dontneed_reason_not_beneficial
  # defno_dontneed_reason_other
  # Percentage of respondents who would definitely not choose to get vaccinated
  # AND who don’t believe they need a COVID-19 vaccine for X
  # reason
  # (# of respondents who selected “I don't believe I need a COVID-19 vaccine.”
  # in V5c) AND selected X in V6./ (# of respondents who selected at least one
  # option in V5c AND selected at least one option in V6)
  if ("b_defno_barrier_dontneed" %in% names(df)) {
    dontneed <- df$b_defno_barrier_dontneed == 1
    
    if ("v_dontneed_reason_had_covid" %in% names(df)) {
      df$b_defno_dontneed_reason_had_covid <- as.numeric(
        all_true(dontneed, df$v_dontneed_reason_had_covid)
      )
    } else {
      df$b_defno_dontneed_reason_had_covid <- NA_real_
    }
    
    if ("v_dontneed_reason_dont_spend_time" %in% names(df)) {
      df$b_defno_dontneed_reason_dont_spend_time <- as.numeric(
        all_true(dontneed, df$v_dontneed_reason_dont_spend_time)
      )
    } else {
      df$b_defno_dontneed_reason_dont_spend_time <- NA_real_
    }
    
    if ("v_dontneed_reason_not_high_risk" %in% names(df)) {
      df$b_defno_dontneed_reason_not_high_risk <- as.numeric(
        all_true(dontneed, df$v_dontneed_reason_not_high_risk)
      )
    } else {
      df$b_defno_dontneed_reason_not_high_risk <- NA_real_
    }
    
    if ("v_dontneed_reason_precautions" %in% names(df)) {
      df$b_defno_dontneed_reason_precautions <- as.numeric(
        all_true(dontneed, df$v_dontneed_reason_precautions)
      )
    } else {
      df$b_defno_dontneed_reason_precautions <- NA_real_
    }
    
    if ("v_dontneed_reason_not_serious" %in% names(df)) {
      df$b_defno_dontneed_reason_not_serious <- as.numeric(
        all_true(dontneed, df$v_dontneed_reason_not_serious)
      )
    } else {
      df$b_defno_dontneed_reason_not_serious <- NA_real_
    }
    
    if ("v_dontneed_reason_not_beneficial" %in% names(df)) {
      df$b_defno_dontneed_reason_not_beneficial <- as.numeric(
        all_true(dontneed, df$v_dontneed_reason_not_beneficial)
      )
    } else {
      df$b_defno_dontneed_reason_not_beneficial <- NA_real_
    }
    
    if ("v_dontneed_reason_other" %in% names(df)) {
      df$b_defno_dontneed_reason_other <- as.numeric(
        all_true(dontneed, df$v_dontneed_reason_other)
      )
    } else {
      df$b_defno_dontneed_reason_other <- NA_real_
    }
  } else {
    df$b_defno_dontneed_reason_had_covid <- NA_real_ 
    df$b_defno_dontneed_reason_dont_spend_time <- NA_real_
    df$b_defno_dontneed_reason_not_high_risk <- NA_real_
    df$b_defno_dontneed_reason_precautions <- NA_real_
    df$b_defno_dontneed_reason_not_serious <- NA_real_
    df$b_defno_dontneed_reason_not_beneficial <- NA_real_
    df$b_defno_dontneed_reason_other <- NA_real_
  }

  # informed_access
  # Percentage of respondents who are very or moderately informed about how to
  # get a vaccination
  # # very or moderately / # responses
  if ("V13" %in% names(df)) {
    df$b_informed_access <- case_when(
      df$V13 %in% c(1, 2) ~ 1,
      df$V13 %in% c(3, 4) ~ 0,
      TRUE ~ NA_real_
    )
  } else {
    df$b_informed_access <- NA_real_
  }

  # appointment_have (discontinued as of Wave 11)
  # Percentage of people who have an appointment to get a COVID-19 vaccine
  # conditional on being accepting
  # # yes / # respondents to V11
  df$b_appointment_have <- NA_real_
  if ("V11" %in% names(df)) {
    df$b_appointment_have <- as.numeric(df$V11 == 1)
  }
  # Replaced by v_appointment_not_vaccinated (defined in API pipeline)

  # appointment_tried (discontinued as of Wave 11)
  # Percentage of people without an appointment who have tried to get one
  # conditional on being accepting
  # # yes / # respondents to V12
  if ("V12" %in% names(df)) {
    df$b_appointment_tried <- as.numeric(df$V12 == 1)
  } else {
    df$b_appointment_tried <- NA_real_
  }
  
  # vaccine_tried
  # Percentage of people without an appointment who have tried to get a vaccine
  # conditional on being accepting
  # # yes / # respondents to V12a
  if ("V12a" %in% names(df)) {
    # 1 = "yes", 2 = "no", no "I don't know" option
    df$b_vaccine_tried <- case_when(
      df$v_accept_covid_vaccine_no_appointment == 1 ~ as.numeric(df$V12a == 1),
      TRUE ~ NA_real_
    )
  } else {
    df$b_vaccine_tried <- NA_real_
  }
  
  # dontneed_reason_had_covid
  # dontneed_reason_dont_spend_time
  # dontneed_reason_not_high_risk
  # dontneed_reason_precautions
  # dontneed_reason_not_serious
  # dontneed_reason_not_beneficial
  # dontneed_reason_other
  # Percentage of all respondents to (V5a, V5b, OR V5c) AND V6 who don’t
  # believe they need a COVID-19 vaccine for X reason
  # (# of respondents who selected “I don't believe I need a COVID-19 vaccine.”
  # in any of V5a, V5b, V5c) AND selected X in V6./ (# of respondents who
  # selected at least one option in any of V5a, V5b, V5c AND selected at least
  # one option in V6)
  #
  # WARNING: This section MUST come after all other variables, since it
  # modifies the `b_dontneed_reason` variables which are used elsewhere in
  # this function.
  if ("v_hesitancy_reason_unnecessary" %in% names(df)) {
    # v_hesitancy_reason_unnecessary is those who answered that they don't need the vaccine
    # to any of questions V5a, V5b, or V6c. It is created originally as
    # v_hesitancy_reason_unnecessary in variables.R.
    dontneed <- df$v_hesitancy_reason_unnecessary == 1
    
    if ("v_dontneed_reason_had_covid" %in% names(df)) {
      df$b_dontneed_reason_had_covid <- as.numeric(
        all_true(dontneed, df$v_dontneed_reason_had_covid)
      )
    } else {
      df$b_dontneed_reason_had_covid <- NA_real_
    }
    
    if ("v_dontneed_reason_dont_spend_time" %in% names(df)) {
      df$b_dontneed_reason_dont_spend_time <- as.numeric(
        all_true(dontneed, df$v_dontneed_reason_dont_spend_time)
      )
    } else {
      df$b_dontneed_reason_dont_spend_time <- NA_real_
    }
    
    if ("v_dontneed_reason_not_high_risk" %in% names(df)) {
      df$b_dontneed_reason_not_high_risk <- as.numeric(
        all_true(dontneed, df$v_dontneed_reason_not_high_risk)
      )
    } else {
      df$b_dontneed_reason_not_high_risk <- NA_real_
    }
    
    if ("v_dontneed_reason_precautions" %in% names(df)) {
      df$b_dontneed_reason_precautions <- as.numeric(
        all_true(dontneed, df$v_dontneed_reason_precautions)
      )
    } else {
      df$b_dontneed_reason_precautions <- NA_real_
    }
    
    if ("v_dontneed_reason_not_serious" %in% names(df)) {
      df$b_dontneed_reason_not_serious <- as.numeric(
        all_true(dontneed, df$v_dontneed_reason_not_serious)
      )
    } else {
      df$b_dontneed_reason_not_serious <- NA_real_
    }
    
    if ("v_dontneed_reason_not_beneficial" %in% names(df)) {
      df$b_dontneed_reason_not_beneficial <- as.numeric(
        all_true(dontneed, df$v_dontneed_reason_not_beneficial)
      )
    } else {
      df$b_dontneed_reason_not_beneficial <- NA_real_
    }
    
    if ("v_dontneed_reason_other" %in% names(df)) {
      df$b_dontneed_reason_other <- as.numeric(
        all_true(dontneed, df$v_dontneed_reason_other)
      )
    } else {
      df$b_dontneed_reason_other <- NA_real_
    }
  } else {
    df$b_dontneed_reason_had_covid <- NA_real_
    df$b_dontneed_reason_dont_spend_time <- NA_real_
    df$b_dontneed_reason_not_high_risk <- NA_real_
    df$b_dontneed_reason_precautions <- NA_real_
    df$b_dontneed_reason_not_serious <- NA_real_
    df$b_dontneed_reason_not_beneficial <- NA_real_
    df$b_dontneed_reason_other <- NA_real_
  }

  return(df)
}


#' Convert numeric response codes in a single survey item to values specified in
#' map. Returns as-is for numeric columns.
#'
#' Maps for recoding are set manually in `remap_responses`.
#'
#' @param df Data frame of individual response data.
#' @param col_var Name of response var to recode
#' @param map_old_new Named vector of new values we want to use; names are the
#'     original response codes
#' @param default Default to use if value is not explicitly remapped in
#'     `map_old_new`; often `NA`, `NA_character_`, etc. See `recode`
#'     [documentation](https://rdrr.io/cran/dplyr/man/recode.html) for more info
#' @param response_type Str indicating if response is binary, multiple choice, or
#'     multi-select.
#'
#' @importFrom dplyr recode
#' @importFrom parallel mcmapply
#'
#' @return list of data frame of individual response data with newly mapped column
remap_response <- function(df, col_var, map_old_new, default=NULL, response_type="b") {
  msg_plain(paste0("Mapping codes for ", col_var))
  if (  is.null(df[[col_var]]) | (response_type == "b" & FALSE %in% df[[col_var]]) | inherits(df[[col_var]], "logical") ) {
    # Column is missing/not in this wave or already in boolean format
    return(df)
  }

  if (response_type %in% c("b", "mc")) {
    df[[col_var]] <- recode(df[[col_var]], !!!map_old_new, .default=default)
  } else if (response_type == "ms") {
    split_col <- split_options(df[[col_var]])

    map_fn <- if (is.null(getOption("mc.cores"))) { mapply } else { mcmapply }
    df[[col_var]] <- map_fn(split_col, FUN=function(row) {
      if ( length(row) == 1 && all(is.na(row)) ) {
        NA
      } else {
        paste(recode(row, !!!map_old_new, .default=default), collapse=",")
      }
    })
  }

  return(df)
}


#' Wrapper for `remap_response` that returns `aggregations` also
#'
#' Assumes binary response variable and is coded with 1 = TRUE (agree), 2 = FALSE,
#' 3 = "I don't know"
#'
#' @param df Data frame of individual response data.
#' @param aggregations Data frame with columns `name`, `var_weight`, `metric`,
#'   `group_by`, `compute_fn`, `post_fn`. Each row represents one aggregate
#'   to report. `name` is the aggregate's base column name; `var_weight` is the
#'   column to use for its weights; `metric` is the column of `df` containing the
#'   response value. `group_by` is a list of variables used to perform the
#'   aggregations over. `compute_fn` is the function that computes
#'   the aggregate response given many rows of data. `post_fn` is applied to the
#'   aggregate data after megacounty aggregation, and can perform any final
#'   calculations necessary.
#' @param col_var Name of response var
#'
#' @return list of data frame of individual response data and user-set data
#' frame of desired aggregations
#'
#' @export
code_binary <- function(df, aggregations, col_var) {
  df <- remap_response(df, col_var, c("1"=1, "2"=0, "3"=NA))
  return(list(df, aggregations))
}

#' Convert a single multi-select response column to a set of boolean columns.
#'
#' Update aggregations table to use new set of columns where `col_var` had
#' previously been used as the metric to aggregate. Does not change columns
#' referenced in `groupby`
#'
#' @param df Data frame of individual response data.
#' @param aggregations Data frame with columns `name`, `var_weight`, `metric`,
#'   `group_by`, `compute_fn`, `post_fn`. Each row represents one aggregate
#'   to report. `name` is the aggregate's base column name; `var_weight` is the
#'   column to use for its weights; `metric` is the column of `df` containing the
#'   response value. `group_by` is a list of variables used to perform the
#'   aggregations over. `compute_fn` is the function that computes
#'   the aggregate response given many rows of data. `post_fn` is applied to the
#'   aggregate data after megacounty aggregation, and can perform any final
#'   calculations necessary.
#' @param col_var Name of response var
#'
#' @return list of data frame of individual response data and user-set data
#' frame of desired aggregations
#'
#' @importFrom stats na.omit
#' @importFrom tibble add_row
#' @importFrom stringi stri_replace_all
#'
#' @export
code_multiselect <- function(df, aggregations, col_var) {
  # Get unique response codes. Sort alphabetically.
  response_codes <- sort( na.omit(
    unique(do.call(c, strsplit(unique(df[[col_var]]), ",")))))

  # Turn each response code into a new binary col
  new_binary_cols <- as.character(lapply(
    response_codes,
    function(code) {
      paste(col_var,
            stri_replace_all(code, "_", fixed=" "),
            sep="_")
      }
    ))

  #### TODO: eval(parse()) here is not the best approach, but I can't find another
  # way to get col_var (a string) to be used as a var that references a column
  # rather than as an actual string. This approach causes a shallow copy to be
  # made (warning is raised).
  df[!is.na(df[[col_var]]), c(new_binary_cols) :=
       lapply(response_codes, function(code) {
         as.numeric( grepl(sprintf("^%s$", code), eval(parse(text=col_var))) |
                       grepl(sprintf("^%s,", code), eval(parse(text=col_var))) |
                       grepl(sprintf(",%s$", code), eval(parse(text=col_var))) |
                       grepl(sprintf(",%s,", code), eval(parse(text=col_var))) )
       })]

  # Update aggregations table
  old_rows <- aggregations[aggregations$metric == col_var, ]
  for (row_ind in seq_along(old_rows$id)) {
    old_row <- old_rows[row_ind, ]

    for (col_ind in seq_along(new_binary_cols)) {
      new_row <- old_row
      response_code <- response_codes[col_ind]

      new_row$name <- paste(old_row$name,
                            stri_replace_all(response_code, "_", fixed=" "),
                            sep="_")
      new_row$id <- paste(old_row$id, response_code, sep="_")
      new_row$metric <- new_binary_cols[col_ind]
      aggregations <- add_row(aggregations, new_row)
    }
  }

  return(list(df, aggregations[aggregations$metric != col_var, ]))
}

#' Convert a single free response column to numeric.
#'
#' @param df Data frame of individual response data.
#' @param aggregations Data frame with columns `name`, `var_weight`, `metric`,
#'   `group_by`, `compute_fn`, `post_fn`. Each row represents one aggregate
#'   to report. `name` is the aggregate's base column name; `var_weight` is the
#'   column to use for its weights; `metric` is the column of `df` containing the
#'   response value. `group_by` is a list of variables used to perform the
#'   aggregations over. `compute_fn` is the function that computes
#'   the aggregate response given many rows of data. `post_fn` is applied to the
#'   aggregate data after megacounty aggregation, and can perform any final
#'   calculations necessary.
#' @param col_var Name of response var
#'
#' @return list of data frame of individual response data and user-set data
#' frame of desired aggregations
#'
#' @export
code_numeric_freeresponse <- function(df, aggregations, col_var) {
  df[[col_var]] <- as.numeric(df[[col_var]])
  return(list(df, aggregations))
}
