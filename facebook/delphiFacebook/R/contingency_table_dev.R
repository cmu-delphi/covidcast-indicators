#### TODO
# - Let user specify function keyword instead of actual function?
# - set up to be able to aggregate multiple time periods in series?
# - compute function for mean vs n vs frequency
# - Do type-checking to make sure desired aggregate function is compatible with metric specified

# # Get data
# path_to_raw_data = "/mnt/sshftps/surveys/raw/"
# 
# wave1 = "2020-08-29.2020-08-22.2020-08-29.Survey_of_COVID-Like_Illness_-_TODEPLOY_2020-04-06.csv"
# wave2 = "2020-11-06.2020-10-30.2020-11-06.Survey_of_COVID-Like_Illness_-_TODEPLOY_......_-_US_Expansion.csv"
# wave3 = "2020-11-06.2020-10-30.2020-11-06.Survey_of_COVID-Like_Illness_-_TODEPLOY-_US_Expansion_-_With_Translations.csv"
# wave4 = "2020-11-06.2020-10-30.2020-11-06.Survey_of_COVID-Like_Illness_-_Wave_4.csv"
# 
# 
# # All surveys have 2 non-response rows at the top. First is detail of question.
# # Second is json(?) field access info -- not needed.
# wave1 = read.csv(file.path(path_to_raw_data, wave1), header = TRUE)
# # Shows 1 (uncompleted) response; 59 fields
# wave2 = read.csv(file.path(path_to_raw_data, wave2), header = TRUE)
# # Shows 230k responses; 83 fields. Most updated form of survey.
# wave3 = read.csv(file.path(path_to_raw_data, wave3), header = TRUE)
# # Shows 230k responses; 83 fields. Most updated form of survey.
# wave4 = read.csv(file.path(path_to_raw_data, wave4), header = TRUE)


# Start with example:
#     group_by(epiweek, state, age, race) %>% summarize(mean(tested_positive), mean(cli), n())
library(lubridate)
library(parallel)
library(dplyr)
library(data.table)
library(tibble)
library(jsonlite)
library(readr)


#' Return params file as an R list
#'
#' Reads a parameters file. If the file does not exist, the function will create a copy of
#' '"params.json.template" and read from that.
#'
#' @param path    path to the parameters file; if not present, will try to copy the file
#'                "params.json.template"
#'
#' @return a named list of parameters values
#'
#' @importFrom dplyr if_else
#' @importFrom jsonlite read_json
#' @importFrom lubridate ymd_hms
#' @export
read_params <- function(path = "params.json") {
  if (!file.exists(path)) file.copy("params.json.template", "params.json")
  params <- read_json(path, simplifyVector = TRUE)

  params$num_filter <- if_else(params$debug, 2L, 100L)
  params$s_weight <- if_else(params$debug, 1.00, 0.01)
  params$s_mix_coef <- if_else(params$debug, 0.05, 0.05)

  if (params$end_date == "current") {
    date_range = get_range_prev_full_period(Sys.Date(), params$aggregate_range)
  } else {
    end_date <- ymd_hms(
      sprintf("%s 23:59:59", params$end_date), tz = "America/Los_Angeles"
    )
    date_range = get_range_prev_full_period(end_date, params$aggregate_range)
  }

  params$start_time <- date_range[[1]]
  params$end_time <- date_range[[2]]

  params$start_date <- as.Date(date_range[[1]])
  params$end_date <- as.Date(date_range[[2]])

  return(params)
}


#' Run the contingency table production pipeline
#'
#' See the README.md file in the source directory for more information about how to run
#' this function.
#'
#' @param params    Params object produced by read_params
#'
#' @return none
#' @importFrom parallel detectCores
#' @export
run_contingency_tables <- function(params)
{
  cw_list <- produce_crosswalk_list(params$static_dir)
  archive <- load_archive(params)
  msg_df("archive data loaded", archive$input_data)

  #### TODO: if end_date == "current", use regex to choose which files to read in from input_dir
  input_data <- load_responses_all(params)
  input_data <- filter_responses(input_data, params)
  msg_df("response input data", input_data)

  input_data <- merge_responses(input_data, archive)
  data_agg <- create_data_for_aggregatation(input_data)

  data_agg <- filter_data_for_aggregatation(data_agg, params, lead_days = 12)
  data_agg <- join_weights(data_agg, params, weights = "step1")
  msg_df("response data to aggregate", data_agg)

  ## Set default number of cores for mclapply to the total available number,
  ## because we are greedy and this will typically run on a server.
  if (params$parallel) {
    cores <- detectCores()

    if (is.na(cores)) {
      warning("Could not detect the number of CPU cores; parallel mode disabled")
      params$parallel <- FALSE
    } else {
      options(mc.cores = cores)
    }
  }

  if ( "cids" %in% params$output )
  {
    write_cid(data_agg, "part_a", params)
  }
  if ( "archive" %in% params$output )
  {
    update_archive(input_data, archive, params)
  }

  data_agg <- set_human_readable_colnames(data_agg)

  # if (params$aggregate_range == "weekly") {
  #   data_agg$period_start_date <- start_of_week(data_agg$day)
  # } else if (params$aggregate_range == "monthly") {
  #   data_agg$period_start_date <- start_of_month(data_agg$day)
  # }

  aggregations <- set_aggs(params)

  if (nrow(aggregations) > 0) {
    aggregate_aggs(data_agg, aggregations, cw_list, params)
  }

}


#### TODO: process binary vars on use (i.e. if aggregations$metric is a binary var)
#### TODO: map response codes to sensical values?
#### TODO: How to get this to work with existing column renaming/processing?
#### TODO: how to know if var to calculate has already been turned into number, or still represents the responses code?
#' Rename question codes to informative descriptions
#'
#'
#' @param params    Params object produced by read_params
#'
#' @return Data frame with descriptive column names
#'
#' @params input_data Data frame of individual response data
#' @importFrom dplyr rename
#' @export
set_human_readable_colnames <- function(input_data) {
  # Named list of question numbers and str replacement names
  map_old_new_names <- c(
    ## free response
    # Either number ("n"; can be averaged although may need processing) or text ("t")
    "n_hh_num_sick" = "hh_number_sick", # A2
    "n_hh_num_children" = "A5_1",
    "n_hh_num_adults" = "A5_2",
    "n_hh_num_seniors" = "A5_3",
    # "t_zipcode" = "A3", # zip5
    "n_cmnty_num_sick" = "A4",
    "t_symptoms_other" = "B2_14_TEXT",
    "t_unusual_symptoms_other" = "B2c_14_TEXT",
    "t_gender_other" = "D1_4_TEXT",
    "n_days_unusual_symptoms" = "B2b",
    "n_contact_num_work" = "C10_1_1",
    "n_contact_num_shopping" = "C10_2_1",
    "n_contact_num_social" = "C10_3_1",
    "n_contact_num_other" = "C10_4_1",
    "n_hh_num_total" = "hh_number_total", # A2b from Waves <4 and summed A5 from Wave 4
    "n_highest_temp_f" = "Q40",
    "n_hh_num_children" = "D3", # Wave 1, etc versions of A5
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
    "b_tested_14d" = "t_tested_14d", # B10; "No" coded as 3, but dealth with in conversion to "t_tested_14d"
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
    "mc_flu_shot_jun2020" = "C17",
    "mc_children_grade" = "E1",
    "mc_children_school" = "E2",
    "mc_pregnant" = "D1b",
    
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

  map_old_new_names = map_old_new_names[!(names(map_old_new_names) %in% names(input_data))]

  input_data <- rename(input_data, map_old_new_names[map_old_new_names %in% names(input_data)])
  input_data$t_zipcode = input_data$zip5 # Keep old zipcode column
  
  if ("b_tested_pos_ever" %in% names(input_data)) {
    # Convert to binary, excluding "I don't know". yes == 1
    # no == 2; "I don't know" == 3
    input_data$b_tested_pos_ever <- case_when(
      input_data$b_tested_pos_ever == 1 ~ 1, # yes
      input_data$b_tested_pos_ever == 2 ~ 0, # no
      input_data$b_tested_pos_ever == 3 ~ NA_real_, # I don't know
      TRUE ~ NA_real_
    )
  }
  
  return(input_data)
}



#' Sets user-specified aggregations.
#'
#' User should add additional desired aggregations here following existing format.
#'
#' @param params Named list of configuration parameters.
#'
#' @import data.table
#'
#' @export
set_aggs <- function(params) {
  aggs <- tribble(
    ~name, ~var_weight, ~metric, ~group_by, ~skip_mixing, ~compute_fn, ~post_fn,
    "tested_reasons_freq", "weight", "ms_reasons_tested_14d", c("mc_age", "b_tested_14d"), FALSE, compute_binary_response, I,
    "tested_pos_freq_given_tested", "weight", "b_tested_pos_14d", c("national", "mc_age", "b_tested_14d"), FALSE, compute_binary_response, I,
    "hh_num_mean", "weight", "n_hh_num_total", c("state"), FALSE, compute_count_response, I,
    
    "mean_tested_positive_by_demos", "weight", "b_tested_pos_14d", c("state", "mc_age", "mc_race"), FALSE, mean, I,
    "mean_cli", "weight", "cli", c("state", "mc_age", "mc_race"), FALSE, mean, I,
    "comorbidities_by_demos", "weight", "ms_comorbidities", c("county", "mc_race", "mc_gender"), FALSE, n, I,
    
    "reasons_tested_freq", "weight", "ms_reasons_tested_14d", c("county"), FALSE, mean, I,
    "reasons_not_tested_freq_by_race", "weight", "ms_reasons_not_tested_14d", c("mc_race", "b_hispanic"), FALSE, mean, I,
    "reasons_not_tested_freq_by_age", "weight", "ms_reasons_not_tested_14d", c("mc_age"), FALSE, mean, I,
    "reasons_not_tested_freq_by_job", "weight", "ms_reasons_not_tested_14d", c("mc_occupational_group"), FALSE, mean, I,
    "seek_medical_care_freq", "weight", "ms_medical_care", c("county"), FALSE, mean, I,
    "unusual_symptom_freq", "weight", "ms_unusual_symptoms", c("b_tested_pos_14d"), FALSE, mean, I,
  )

  return(aggs)
}



#' Returns response estimates for a single data grouping.
#'
#' This function takes vectors as input and computes the count response values
#' (a point estimate named "val" and an effective
#' sample size named "effective_sample_size").
#'
#' @param response a vector of percentages (100 * cnt / total)
#' @param weight a vector of sample weights for inverse probability weighting;
#'   invariant up to a scaling factor
#' @param sample_size Unused.
#'
#' @importFrom stats weighted.mean
#' @export
compute_count_response <- function(response, weight, sample_size)
{
  #### TODO: Why does this need to be for a percent response?
  assert(all( response >= 0 & response <= 100 ))
  assert(length(response) == length(weight))

  weight <- weight / sum(weight)
  val <- weighted.mean(response, weight)

  effective_sample_size <- length(weight) * mean(weight)^2 / mean(weight^2)

  return(list(
    val = val,
    sample_size = sample_size,
    effective_sample_size = effective_sample_size
  ))
}


#' Returns binary response estimates
#'
#' This function takes vectors as input and computes the binary response values
#' (a point estimate named "val" and a sample size
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
             sample_size = sample_size,
             effective_sample_size = sample_size)) # TODO effective sample size
}


#' Produce aggregates for all desired aggregations.
#'
#' Writes the outputs directly to CSVs in the directory specified by `params`.
#' Produces output for all available days between `params$start_date -
#' params$backfill_days` and `params$end_date`, inclusive. (We re-output days
#' before `start_date` in case there was backfill for those days.)
#'
#' Warning: The maximum value of `smooth_days` needs to be accounted for in
#' `run.R` in the `lead_days` argument to `filter_data_for_aggregation`, so
#' the correct amount of archived data is included, plus the expected backfill
#' length.
#'
#' @param df Data frame of individual response data.
#' @param aggregations Data frame with columns `name`, `var_weight`, `metric`,
#'   `compute_fn`, `post_fn`. Each row represents one aggregate
#'   to report. `name` is the aggregate's base file name; `var_weight` is the column
#'   to use for its weights; `metric` is the column of `df` containing the
#'   response value. `compute_fn` is the function that computes
#'   the aggregate response given many rows of data. `post_fn` is applied to the
#'   aggregate data after megacounty aggregation, and can perform any final
#'   calculations necessary.
#' @param cw_list Named list of geographic crosswalks, each of which maps a zip5
#'   to a geographic level such as county or state. Aggregates will be produced
#'   for each geographic level.
#' @param params Named list of configuration parameters.
#'
#' @import data.table
#' @importFrom dplyr filter mutate_at vars bind_rows
#'
#' @export
aggregate_aggs <- function(df, aggregations, cw_list, params) {
  ## For the day range lookups we do on df, use a data.table key. This puts the
  ## table in sorted order so data.table can use a binary search to find
  ## matching dates, rather than a linear scan, and is important for very large
  ## input files.
  df <- as.data.table(df)
  setkey(df, day)

  # Keep only obs in desired date range.
  df <- df[start_dt >= params$start_time & start_dt <= params$end_time]

  # Add implied geo_level to each group_by. Order alphabetically
  aggregations$geo_level = NA
  for (agg_ind in seq_along(aggregations$group_by)) {
    geo_level = intersect(aggregations$group_by[agg_ind], names(cw_list))
    if (length(geo_level) > 1) {
      stop('more than one geo type provided for a single aggregation')
    } else if (length(geo_level) == 0) {
      geo_level = "national"
    }

    aggregations$group_by[agg_ind][[1]] = sort(unique(append(aggregations$group_by[agg_ind][[1]], geo_level)))
    aggregations$geo_level[agg_ind] = geo_level
  }

  agg_groups = unique(aggregations$group_by)
  
  # Convert any columns being aggregated to the appropriate format.
  #### TODO: want to convert/check groupby cols?
  # check_col_types = unique(c(do.call(c, agg_groups), aggregations$metric))
  check_col_types = unique(aggregations$metric)
  
  for (col_var in check_col_types) {
    if (startsWith(col_var, "b_")) { # Binary
      output <- convert_qcodes_to_bool(df, aggregations, col_var)
      df <- output[[1]]
      aggregations <- output[[2]]
      
    } else if (startsWith(col_var, "ms_")) { # Multiselect
      output <- convert_multiselect_to_binary_cols(df, aggregations, col_var)
      df <- output[[1]]
      aggregations <- output[[2]]
      
    } else if (startsWith(col_var, "n_")) { # Numeric free response
      output <- convert_freeresponse_to_num(df, aggregations, col_var)
      df <- output[[1]]
      aggregations <- output[[2]]
    }
  }

  # For each unique combination of groupby_vars, run aggregation process once
  # and calculate all desired aggregations on the grouping. Save to individual
  # files
  #### TODO: want to save all results for a given grouping to the same file. Will need to rename cols and put groupby vars into file name.
  for (agg_group in agg_groups) {
    these_aggs = aggregations[mapply(aggregations$group_by,
                                     FUN=function(x) {setequal(x, agg_group)
                                       }), ]

    geo_level = these_aggs$geo_level[1]
    geo_crosswalk = cw_list[[geo_level]]

    dfs_out <- summarize_aggs(df, geo_crosswalk, these_aggs, geo_level, params)

    browser()
    # Save each aggregation to separate file
    for (agg_ind in seq_along(these_aggs$name)) {
      aggregation = these_aggs$name[agg_ind]
      groupby_vars = these_aggs$group_by[agg_ind]

      private_df = apply_privacy_censoring(dfs_out[[aggregation]], params)
      write_data_api(private_df, params, geo_level, aggregation, groupby_vars)
    }
  }
}


convert_qcodes_to_bool <- function(df, aggregations, col_var) {
  if (is.null(df[[col_var]])) {
    # Column not defined.
    return(list(df,  aggregations[aggregations$name != col_var, ]))
  }
  
  if (FALSE %in% df[[col_var]] | TRUE %in% df[[col_var]]) {
    return(df)
  }
  
  df[[col_var]] <- (df[[col_var]] == 1L)
  return(list(df, aggregations))
}



convert_multiselect_to_binary_cols <- function(df, aggregations, col_var) {
  if (is.null(df[[col_var]])) {
    # Column not defined.
    return(list(df,  aggregations[aggregations$name != col_var, ]))
  }
  
  # Get unique response codes
  response_codes <- sort(na.omit(unique(do.call(c, strsplit(unique(df[[col_var]]), ",")))))
  
  browser()
  # Turn each response code into a new binary col
  new_binary_cols = as.character(lapply(response_codes, function(code) { paste(col_var, code, sep="_") }))
  #### TODO: eval(parse()) here is not the best approach, but I can't find another 
  # way to get col_var (a string) to be used as a var rather than a string
  df[!is.na(df[[col_var]]), c(new_binary_cols) := 
       lapply(response_codes, function(code) { 
         ( grepl(sprintf("^%s$", code), eval(parse(text=col_var))) | 
             grepl(sprintf("^%s,", code), eval(parse(text=col_var))) | 
             grepl(sprintf(",%s$", code), eval(parse(text=col_var))) | 
             grepl(sprintf(",%s,", code), eval(parse(text=col_var))) ) 
         })]

  # Update aggregations table
  old_row = aggregations[aggregations$name == col_var, ]
  for (col_ind in seq_along(new_binary_cols)) {
    old_row$name = paste(col_var, col_ind, sep="_")
    old_row$metric = new_binary_cols[col_ind]
    add_row(aggregations, old_row)
  }
  
  return(list(df, aggregations[aggregations$name != col_var, ]))
}


convert_freeresponse_to_num <- function(df, aggregations, col_var) {
  if (is.null(df[[col_var]])) {
    # Column not defined.
    return(list(df,  aggregations[aggregations$name != col_var, ]))
  }
  
  df[[col_var]] <- as.numeric(df[[col_var]])
  return(list(df, aggregations))
}



#' Censor aggregates to ensure privacy.
#'
#' Currently done in simple, static way: Rows with sample size less than 100 are
#' removed; no noise is added and sample size setting is not changeable via
#' the params file.
#'
#' @param df a data frame of summarized response data
#' @param params a named list with entries "s_weight", "s_mix_coef",
#'   "num_filter"
#'
#' @export
apply_privacy_censoring <- function(df, params) {
  return(df["sample_size" >= 100])
}



#' Calculate aggregates across all days for all indicators.
#'
#' The organization may seem a bit contorted, but this is designed for speed.
#' The primary bottleneck is repeatedly filtering the data frame to find data
#' for the day and geographic area of interest. To save time, we do this once
#' and then calculate all indicators for that day-area combination, rather than
#' separately filtering every time we want to calculate a new indicator. We also
#' rely upon data.table's keys and indices to allow us to do the filtering in
#' O(log n) time, which is important when the data frame contains millions of
#' rows.
#'
#' @param df a data frame of survey responses
#' @param crosswalk_data An aggregation, such as zip => county or zip => state,
#'   as a data frame with a "zip5" column to join against.
#' @param aggregations Data frame of desired aggregations.
#' @param geo_level the aggregation level, such as county or state, being used
#' @param params a named list with entries "s_weight", "s_mix_coef",
#'   "num_filter"
#'
#' @importFrom dplyr inner_join bind_rows
#' @importFrom parallel mclapply
#' @export
summarize_aggs <- function(df, crosswalk_data, aggregations, geo_level, params) {
  ## dplyr complains about joining a data.table, saying it is likely to be
  ## inefficient; profiling shows the cost to be negligible, so shut it up
  # Geo group column is always named "geo_id"
  df <- suppressWarnings(inner_join(df, crosswalk_data, by = "zip5"))

  ## We do batches of just one set of groupby vars at a time, since we have
  ## to select rows based on this.
  assert( length(unique(aggregations$group_by)) == 1 )

  groupby_vars <- aggregations$group_by[[1]]
  groupby_vars[groupby_vars == geo_level] = "geo_id"

  unique_group_combos = unique(df[, ..groupby_vars])
  unique_group_combos = unique_group_combos[complete.cases(unique_group_combos)]

  if (nrow(unique_group_combos) == 0) {
    return(list())
  }


  ## Set an index on the groupby var columns so that the groupby step can be
  ## dramatically faster; data.table stores the sort order of the column and
  ## uses a binary search to find matching values, rather than a linear scan.
  setindexv(df, groupby_vars)

  calculate_group <- function(ii) {
    target_group <- unique_group_combos[ii]
    # Use data.table's index to make this filter efficient
    out <- summarize_aggregations_group(
      df[as.list(target_group), on=names(target_group)],
      aggregations,
      target_group,
      geo_level,
      params)

    return(out)
  }


  if (params$parallel) {
    dfs <- mclapply(seq_along(transpose(unique_group_combos)), calculate_group)
  } else {
    dfs <- lapply(seq_along(transpose(unique_group_combos)), calculate_group)
  }

  ## Now we have a list, with one entry per groupby level, each containing a
  ## list of one data frame per aggregation. Rearrange it.
  dfs_out <- list()
  for (aggregation in aggregations$name) {
    dfs_out[[aggregation]] <- bind_rows(lapply(dfs, function(groupby_levels) { groupby_levels[[aggregation]] }))
  }

  return(dfs_out)
}


#' Produce estimates for all indicators in a specific target group.
#' @param group_df Data frame containing all data needed to estimate one group.
#'   Estimates for `target_group` will be based on all of this data.
#' @param aggregations Aggregations to report. See `aggregate_aggs()`.
#' @param target_group A `data.table` with one row specifying the grouping
#'   variable values used to select this group.
#' @param params Named list of configuration options.
#' @importFrom dplyr mutate filter
#' @importFrom tibble add_column
#' @importFrom rlang .data
summarize_aggregations_group <- function(group_df, aggregations, target_group, geo_level, params) {
  ## Prepare outputs.
  dfs_out <- list()
  for (index in seq_along(aggregations$name)) {
    aggregation = aggregations$name[index]

    dfs_out[[aggregation]] = target_group %>%
      as.list %>%
      as_tibble %>%
      add_column(val=NA_real_) %>%
      add_column(se=NA_real_) %>%
      add_column(sample_size=NA_real_) %>%
      add_column(effective_sample_size=NA_real_)
  }

  for (row in seq_len(nrow(aggregations))) {
    aggregation <- aggregations$name[row]
    metric <- aggregations$metric[row]
    var_weight <- aggregations$var_weight[row]
    compute_fn <- aggregations$compute_fn[[row]]

    agg_df <- group_df[!is.na(group_df[[var_weight]]) & !is.na(group_df[[metric]]), ]

    if (nrow(agg_df) > 0)
    {
      s_mix_coef <- params$s_mix_coef
      mixing <- mix_weights(agg_df[[var_weight]] * agg_df$weight_in_location,
                            s_mix_coef, params$s_weight)

      sample_size <- sum(agg_df$weight_in_location)

      ## TODO Fix this. Old pipeline for community responses did not apply
      ## mixing. To reproduce it, we ignore the mixed weights. Once a better
      ## mixing/weighting scheme is chosen, all signals should use it.
      new_row <- compute_fn(
        response = agg_df[[metric]],
        weight = if (aggregations$skip_mixing[row]) { mixing$normalized_preweights } else { mixing$weights },
        sample_size = sample_size)

      dfs_out[[aggregation]]$val <- new_row$val
      dfs_out[[aggregation]]$se <- new_row$se
      dfs_out[[aggregation]]$sample_size <- sample_size
      dfs_out[[aggregation]]$effective_sample_size <- new_row$effective_sample_size
    }
  }

  for (row in seq_len(nrow(aggregations))) {
    aggregation <- aggregations$name[row]
    post_fn <- aggregations$post_fn[[row]]

    dfs_out[[aggregation]] <- dfs_out[[aggregation]][
      rowSums(is.na(dfs_out[[aggregation]][, c("val", "sample_size", names(target_group))])) == 0,
    ]

    if (geo_level == "county") {
      df_megacounties <- megacounty(dfs_out[[aggregation]], params$num_filter)
      dfs_out[[aggregation]] <- bind_rows(dfs_out[[aggregation]], df_megacounties)
    }

    dfs_out[[aggregation]] <- filter(dfs_out[[aggregation]],
                                   .data$sample_size >= params$num_filter,
                                   .data$effective_sample_size >= params$num_filter)

    ## *After* gluing together megacounties, apply the post-function
    dfs_out[[aggregation]] <- post_fn(dfs_out[[aggregation]])
  }

  return(dfs_out)
}


#' Write csv file for sharing with researchers
#'
#' @param data           a data frame to save; must contain the columns "geo_id", "val",
#'                       "se", "sample_size", and grouping variables. The first four are saved in the
#'                       output; day is used for spliting the data into files.
#' @param params         a named list, containing the value "export_dir" indicating the
#'                       directory where the csv should be saved
#' @param geo_name       name of the geographic level; used for naming the output file
#' @param signal_name    name of the signal; used for naming the output file
#'
#' @importFrom readr write_csv
#' @importFrom dplyr arrange
#' @importFrom rlang .data
#' @export
write_data_api <- function(data, params, geo_level, signal_name, groupby_vars)
{
  if (!is.null(data)) {
    data <- arrange(data, groupby_vars)
  } else {
    data <- data.frame()
  }

  file_out <- file.path(
    params$export_dir, sprintf("%s_%s_%s.csv", format(params$start_date, "%Y%m%d"),
                               geo_level, signal_name)
  )

  create_dir_not_exist(params$export_dir)

  msg_df(sprintf(
    "saving contingency table data to %-35s",
    sprintf("%s_%s_%s", format(params$start_date, "%Y%m%d"), geo_level, signal_name)
  ), data)
  write_csv(data, file_out)
}


start_of_month <- function(end_date) {
  return(floor_date(end_date, "month") - months(1))
}

end_of_prev_full_month <- function(end_date) {
  if (ceiling_date(end_date, "month") == end_date) {
    return(end_date)
  }

  return(floor_date(end_date, "month") - days(1))
}


get_range_prev_full_month <- function(end_date = Sys.Date()) {
  eom = end_of_prev_full_month(end_date)

  if (eom == end_date) {
    som = start_of_month(end_date + months(1))
  } else {
    som = start_of_month(end_date)
  }

  return(list(som, eom))
}



start_of_week <- function(end_date) {
  return(floor_date(end_date, "week") - weeks(1))
}

end_of_prev_full_week <- function(end_date) {
  if (ceiling_date(end_date, "week") == end_date) {
    return(end_date)
  }

  return(floor_date(end_date, "week") - days(1))
}


#### TODO: should be epiweeks eventually. Already exists a package to calculate?
get_range_prev_full_week <- function(end_date = Sys.Date()) {
  eow = end_of_prev_full_week(end_date)

  if (eow == end_date) {
    sow = start_of_week(end_date + weeks(1))
  } else {
    sow = start_of_week(end_date)
  }

  return(list(sow, eow))
}

get_range_prev_full_period <- function(end_date = Sys.Date(), weekly_or_monthly_flag) {
  if (weekly_or_monthly_flag == "monthly") {
    # Get start and end of previous full month.
    date_period_range = get_range_prev_full_month(end_date)
  } else if (weekly_or_monthly_flag == "weekly") {
    # Get start and end of previous full epiweek.
    date_period_range = get_range_prev_full_week(end_date)
  }

  date_period_range[[1]] =  ymd_hms(
    sprintf("%s 00:00:00", date_period_range[[1]]), tz = "America/Los_Angeles"
  )
  date_period_range[[2]] =  ymd_hms(
    sprintf("%s 23:59:59", date_period_range[[2]]), tz = "America/Los_Angeles"
  )

  return(date_period_range)
}
