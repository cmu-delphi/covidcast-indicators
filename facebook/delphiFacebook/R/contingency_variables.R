## Functions handling renaming, reformatting, or recoding response columns.

#' Rename question codes to informative descriptions
#'
#' Column names beginning with "b_" are binary (T/F/NA); with "t_" are user-
#' entered text; with "n_" are user-entered numeric; with "mc_" are multiple
#' choice (where only a single response can be selected); and with "ms_" are
#' so-called multi-select, where multiple responses can be selected.
#' 
#' Only binary columns are mapped from response codes to real values. Multiple
#' choice and multi-select questions use the original numeric response codes.
#'
#' @param input_data    Data frame of individual response data
#' 
#' @return Data frame with descriptive column names
#' 
#' @importFrom dplyr rename
#' 
#' @export
make_human_readable <- function(input_data) {
  # Named list of question numbers and str replacement names
  # These columns are not available for aggregation:
  #   "t_zipcode" = "A3", -> Please use `zip5` instead
  #   "t_symptoms_other" = "B2_14_TEXT",
  #   "t_unusual_symptoms_other" = "B2c_14_TEXT",
  #   "t_gender_other" = "D1_4_TEXT",
  map_old_new_names <- c(
    ## free response
    # Either number ("n"; can be averaged although may need processing) or text ("t")
    "n_hh_num_sick" = "hh_number_sick", # A2
    "n_hh_num_children" = "A5_1",
    "n_hh_num_adults" = "A5_2",
    "n_hh_num_seniors" = "A5_3",
    "n_cmnty_num_sick" = "A4",
    "n_days_unusual_symptoms" = "B2b",
    "n_contact_num_work" = "C10_1_1",
    "n_contact_num_shopping" = "C10_2_1",
    "n_contact_num_social" = "C10_3_1",
    "n_contact_num_other" = "C10_4_1",
    "n_hh_num_total" = "hh_number_total", # A2b from Waves <4 and summed A5 from Wave 4
    "n_highest_temp_f" = "Q40",
    "n_hh_num_children_old" = "D3", # Wave 1, etc versions of A5
    "n_hh_num_adults_not_self" = "D4",
    "n_hh_num_seniors_not_self" = "D5",
    
    ## binary response (b)
    # False (no) is mapped to 2 and True (yes/agreement) is mapped to 1
    "b_consent" = "S1",
    "b_hh_fever" = "hh_fever", # A1_1
    "b_hh_sore_throat" = "hh_soar_throat", # A1_2
    "b_hh_cough" = "hh_cough", # A1_3
    "b_hh_shortness_of_breath" = "hh_short_breath", # A1_4
    "b_hh_difficulty_breathing" = "hh_diff_breath", # A1_5
    "b_tested_ever" = "B8",
    "b_tested_14d" = "t_tested_14d", # B10; "No" coded as 3, but dealt with in conversion to "t_tested_14d"
    "b_wanted_test_14d" = "t_wanted_test_14d", # B12
    "b_state_travel" = "C6", # c_travel_state
    "b_contact_tested_pos" = "C11",
    "b_contact_tested_pos_hh" = "C12",
    "b_hispanic" = "D6",
    "b_worked_4w" = "D9",
    "b_worked_outside_home_4w" = "D10",
    "b_took_temp" = "B3",
    "b_flu_shot_12m" = "C2",
    "b_worked_outside_home_5d" = "c_work_outside_5d", # C3
    "b_worked_healthcare_5d" = "C4",
    "b_worked_nursing_home_5d" = "C5",
    "b_anxious" = "mh_anxious", # Binary version of C8_1
    "b_depressed" = "mh_depressed", # Binary version of C8_2
    "b_isolated" = "mh_isolated", # Binary version of C8_3
    "b_worried_family_ill" = "mh_worried_ill", # Binary version of C9   
    "b_public_mask_often" = "c_mask_often", # Binary version of C14
    "b_tested_pos_14d" = "t_tested_positive_14d", # B10a; binary with an "I don't know" (3) option
    "b_tested_pos_ever" = "B11", # binary with an "I don't know" (3) option
    "b_have_cli" = "is_cli", # Based on symptoms in A1
    "b_have_ili" = "is_ili", # Based on symptoms in A1
    "b_cmnty_have_cli" = "community_yes",
    "b_hh_or_cmnty_have_cli" = "hh_community_yes",
    "b_flu_shot_jun2020" = "C17", # binary with "I don't know" option
    "b_children_grade_pre-k" = "E1_1", # binary with "I don't know" option
    "b_children_grade_1-5" = "E1_2", # binary with "I don't know" option
    "b_children_grade_6-8" = "E1_3", # binary with "I don't know" option
    "b_children_grade_9-12" = "E1_4", # binary with "I don't know" option
    "b_children_fulltime_school" = "E2_1", # binary with "I don't know" option
    "b_children_parttime_school" = "E2_2", # binary with "I don't know" option
    
    ## multiple choice (mc)
    # Can only select one of n > 2 choices
    "mc_state" = "A3b",
    "mc_mask_often" = "C14",
    "mc_anxiety" = "C8_1",
    "mc_depression" = "C8_2",
    "mc_isolation" = "C8_3",
    "mc_worried_family_ill" = "C9",
    "mc_financial_worry" = "C15",
    "mc_gender" = "D1",
    "mc_age" = "D2",
    "mc_race" = "D7",
    "mc_education" = "D8",
    "mc_occupational_group" = "Q64",
    "mc_job_type_cmnty_social" = "Q65",
    "mc_job_type_education" = "Q66",
    "mc_job_type_arts_media" = "Q67",
    "mc_job_type_healthcare" = "Q68",
    "mc_job_type_healthcare_support" = "Q69",
    "mc_job_type_protective" = "Q70",
    "mc_job_type_food" = "Q71",
    "mc_job_type_maintenance" = "Q72",
    "mc_job_type_personal_care" = "Q73",
    "mc_job_type_sales" = "Q74",
    "mc_job_type_office_admin" = "Q75",
    "mc_job_type_construction" = "Q76",
    "mc_job_type_repair" = "Q77",
    "mc_job_type_production" = "Q78",
    "mc_job_type_transport" = "Q79",
    "mc_occupational_group_other" = "Q80",
    "mc_cough_mucus" = "B4",
    "mc_tested_current_illness" = "B5",
    "mc_hospital" = "B6",
    "mc_social_avoidance" = "C7",
    "mc_financial_threat" = "Q36",
    "mc_cmnty_mask_prevalence" = "C16",
    "mc_pregnant" = "D1b", # Somewhat of a binary response (yes, no, prefer not to answer, and not applicable)
    
    ## multiselect (ms)
    # Can select more than one choice; saved as comma-separated list of choice codes
    "ms_symptoms" = "B2",
    "ms_unusual_symptoms" = "B2c",
    "ms_medical_care" = "B7",
    "ms_reasons_tested_14d" = "B10b",
    "ms_reasons_not_tested_14d" = "B12a",
    "ms_trips_outside_home" = "C13",
    "ms_mask_outside_home" = "C13a",
    "ms_school_safety_measures" = "E3",
    "ms_comorbidities" = "C1",
    
    ## other (created in previous data-cleaning steps)
    "n_num_symptoms" = "cnt_symptoms", # Based on symptoms in A1
    "n_hh_prop_cli" = "hh_p_cli", # Based on symptoms in A1, and hh sick and total counts
    "n_hh_prop_ili" = "hh_p_ili" # Based on symptoms in A1, and hh sick and total counts
  )
  
  map_old_new_names <- map_old_new_names[!(names(map_old_new_names) %in% names(input_data))]
  
  input_data <- rename(input_data, map_old_new_names[map_old_new_names %in% names(input_data)])
  input_data$t_zipcode <- input_data$zip5 # Keep existing parsed zipcode column
  
  # Map responses with multiple race options selected into a single category.
  input_data[grepl(",", input_data$mc_race), "mc_race"] <- "multiracial"
  
  # Map "I don't know" to NA in otherwise binary columns.
  input_data <- code_binary_with_idk(input_data, "b_tested_pos_ever")
  input_data <- code_binary_with_idk(input_data, "b_flu_shot_jun2020", 1, 4, 2)
  
  input_data <- code_binary_with_idk(input_data, "b_children_grade_pre-k", 1, 2, 5)
  input_data <- code_binary_with_idk(input_data, "b_children_grade_1-5", 1, 2, 5)
  input_data <- code_binary_with_idk(input_data, "b_children_grade_6-8", 1, 2, 5)
  input_data <- code_binary_with_idk(input_data, "b_children_grade_9-12", 1, 2, 5)
  
  input_data <- code_binary_with_idk(input_data, "b_children_fulltime_school", 2, 3, 4)
  input_data <- code_binary_with_idk(input_data, "b_children_parttime_school", 2, 3, 4)
  
  return(input_data)
}

#' Convert a single binary response + "I don't know" column to boolean
#' 
#' "I don't know" is set to NA (missing) and excluded.
#' 
#' @param df Data frame of individual response data.
#' @param col_var Name of response var
#' @param yes_val Response code corresponding to answering "yes"
#' @param no_val Response code corresponding to answering "yes"
#' @param idk_val Response code corresponding to answering "I don't know"
#'
#' @return list of data frame of individual response data with newly mapped column
#'
#' @export
code_binary_with_idk <- function(df, col_var, yes_val=1, no_val=2, idk_val=3) {
  if (FALSE %in% df[[col_var]]) {
    # Already in boolean format.
    return(df)
  }
  
  # Map values following existing approach in variables.R::code_testing().
  if (col_var %in% names(df)) {
    # Convert to binary, excluding "I don't know".
    df[[col_var]] <- case_when(
      df[[col_var]] == yes_val ~ 1, # yes
      df[[col_var]] == no_val ~ 0, # no
      df[[col_var]] == idk_val ~ NA_real_, # I don't know
      TRUE ~ NA_real_
    )
  }
  
  return(df)
}

#' Convert a single binary response column to boolean
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
  if (FALSE %in% df[[col_var]]) {
    # Already in boolean format.
    return(list(df, aggregations))
  }
  
  df[[col_var]] <- (df[[col_var]] == 1L)
  return(list(df, aggregations))
}

#' Convert a single multi-select response column to a set of boolean columns
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
code_multiselect <- function(df, aggregations, col_var) {
  # Get unique response codes
  response_codes <- sort(na.omit(
    unique(do.call(c, strsplit(unique(df[[col_var]]), ",")))))
  
  # Turn each response code into a new binary col
  new_binary_cols <- as.character(lapply(
    response_codes, 
    function(code) { paste(col_var, code, sep="_") }))
  #### TODO: eval(parse()) here is not the best approach, but I can't find another 
  # way to get col_var (a string) to be used as a var that references a column
  # rather than as an actual string. This approach causes a shallow copy to be 
  # made (warning is raised).
  df[!is.na(df[[col_var]]), c(new_binary_cols) := 
       lapply(response_codes, function(code) { 
         ( grepl(sprintf("^%s$", code), eval(parse(text=col_var))) | 
             grepl(sprintf("^%s,", code), eval(parse(text=col_var))) | 
             grepl(sprintf(",%s$", code), eval(parse(text=col_var))) | 
             grepl(sprintf(",%s,", code), eval(parse(text=col_var))) ) 
       })]
  
  # Update aggregations table
  old_rows <- aggregations[aggregations$metric == col_var, ]
  for (row_ind in seq_along(old_rows$name)) {
    old_row <- old_rows[row_ind, ]
    
    for (col_ind in seq_along(new_binary_cols)) {
      new_row <- old_row
      new_row$name <- paste(old_row$name, col_ind, sep="_")
      new_row$metric <- new_binary_cols[col_ind]
      aggregations <- add_row(aggregations, new_row)
    }
  }
  
  return(list(df, aggregations[aggregations$metric != col_var, ]))
}

#' Convert a single free response column to numeric
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
