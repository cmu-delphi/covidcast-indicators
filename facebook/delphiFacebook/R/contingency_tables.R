#### TODO
# - set up to be able to aggregate multiple time periods in series? wrapper function that modifies params.json more likely
# - map response codes to descriptive values? Would need mapping for every individual question
# - when params$end_date is specified exactly (not "current"), should we just use that date
# range instead of finding the most recent full week/month?


#' Update date and input files settings from params file
#' 
#' Use `end_date` and `aggregate_range` to calculate last full time period
#' (either month or week) to use for aggregations. Find all files in `input_dir`
#' within the calculated time period.
#'
#' @param params    Params object produced by read_params
#'
#' @return a named list of parameters values
#'
#' @importFrom lubridate ymd_hms
#' 
#' @export
update_params <- function(params) {
  if (params$end_date == "current") {
    date_range <- get_range_prev_full_period(Sys.Date(), params$aggregate_range)
    params$input <- get_filenames_in_range(date_range, params)
  } else {
    end_date <- ymd_hms(
      sprintf("%s 23:59:59", params$end_date), tz = "America/Los_Angeles"
    )
    date_range <- get_range_prev_full_period(end_date, params$aggregate_range)
  }
  
  if (length(params$input) == 0) {
    stop("no input files to read in")
  }
  
  params$start_time <- date_range[[1]]
  params$end_time <- date_range[[2]]
  
  params$start_date <- as.Date(date_range[[1]])
  params$end_date <- as.Date(date_range[[2]])
  
  return(params)
}


#' Get relevant input data file names from `input_dir`.
#'
#' @param date_range    List of two dates specifying start and end of desired
#' date range 
#' @param params    Params object produced by read_params
#'
#' @return Character vector of filenames
#' 
#' @export
get_filenames_in_range <- function(date_range, params) {
  start_date <- as.Date(date_range[[1]]) - params$archive_days
  end_date <- as.Date(date_range[[2]])
  date_pattern <- "^[0-9]{4}-[0-9]{2}-[0-9]{2}.*[.]csv$"
  youtube_pattern <- ".*YouTube[.]csv$"
  
  filenames <- list.files(path=params$input_dir)
  filenames <- filenames[grepl(date_pattern, filenames) & !grepl(youtube_pattern, filenames)]
  
  file_end_dates <- as.Date(substr(filenames, 1, 10))
  file_start_dates <- file_end_dates
  
  # Only keep files with data that falls at least somewhat between the desired
  # start and end range dates.
  filenames <- filenames[
    !(( file_start_dates < start_date & file_end_dates < start_date ) | 
        ( file_start_dates > end_date & file_end_dates > end_date ))]
  
  return(filenames)
}


#' Run the contingency table production pipeline
#'
#' See the README.md file in the source directory for more information about how to run
#' this function.
#'
#' @param params    Params object produced by read_params
#' @param aggregations Data frame with columns `name`, `var_weight`, `metric`,
#'   `group_by`, `compute_fn`, `post_fn`. Each row represents one aggregate
#'   to report. `name` is the aggregate's base column name; `var_weight` is the 
#'   column to use for its weights; `metric` is the column of `df` containing the
#'   response value. `group_by` is a list of variables used to perform the 
#'   aggregations over. `compute_fn` is the function that computes
#'   the aggregate response given many rows of data. `post_fn` is applied to the
#'   aggregate data after megacounty aggregation, and can perform any final
#'   calculations necessary.
#'
#' @return none
#' 
#' @importFrom parallel detectCores
#' 
#' @export
run_contingency_tables <- function(params, aggregations)
{
  params <- update_params(params)
  
  cw_list <- produce_crosswalk_list(params$static_dir)
  archive <- load_archive(params)
  msg_df("archive data loaded", archive$input_data)
  
  input_data <- load_responses_all(params)
  input_data <- filter_responses(input_data, params)
  msg_df("response input data", input_data)
  
  input_data <- merge_responses(input_data, archive)
  data_agg <- create_data_for_aggregatation(input_data)
  
  data_agg <- filter_data_for_aggregatation(data_agg, params, lead_days = 12)
  data_agg <- join_weights(data_agg, params, weights = "full")
  browser()
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
  
  data_agg <- make_human_readable(data_agg)
  aggregations <- get_aggs(params, aggregations)
  
  if (nrow(aggregations) > 0) {
    aggregate_aggs(data_agg, aggregations, cw_list, params)
  }
  
}


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
  #   "t_zipcode" = "A3", (Please use `zip5` instead)
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
  
  map_old_new_names <- map_old_new_names[!(names(map_old_new_names) %in% names(input_data))]
  
  input_data <- rename(input_data, map_old_new_names[map_old_new_names %in% names(input_data)])
  input_data$t_zipcode <- input_data$zip5 # Keep existing parsed zipcode column
  
  # Map responses with multiple race options selected into a single category.
  input_data[grepl(",", input_data$mc_race), "mc_race"] <- "multiracial"
  
  # This column has "yes", "no", and "I don't know". Handle following existing
  # approach in variables.R::code_testing().
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


#' Checks user-set aggregations for basic validity
#'
#' @param params Named list of configuration parameters.
#' @param aggs Data frame with columns `name`, `var_weight`, `metric`,
#'   `group_by`, `compute_fn`, `post_fn`. Each row represents one aggregate
#'   to report. `name` is the aggregate's base column name; `var_weight` is the 
#'   column to use for its weights; `metric` is the column of `df` containing the
#'   response value. `group_by` is a list of variables used to perform the 
#'   aggregations over. `compute_fn` is the function that computes
#'   the aggregate response given many rows of data. `post_fn` is applied to the
#'   aggregate data after megacounty aggregation, and can perform any final
#'   calculations necessary.
#' 
#' @return a data frame of desired aggregations to calculate
#'
#' @export
get_aggs <- function(params, aggs) {
  aggregations <- unique(aggs)
  
  if ( length(unique(aggregations$name)) < nrow(aggregations) ) {
    stop("all aggregation names must be unique")
  }
  
  expected_names <- c("name", "var_weight", "metric", "group_by", "skip_mixing", 
                     "compute_fn", "post_fn")
  if ( !all(expected_names %in% names(aggs)) ) {
    stop(sprintf(
      "all expected columns %s must appear in aggs", 
      paste(expected_names, collapse=", ")))
  }
  
  return(aggregations)
}



#' Wrapper for `compute_count_response` that adds sample_size
#'
#' @param response a vector of percentages (100 * cnt / total)
#' @param weight a vector of sample weights for inverse probability weighting;
#'   invariant up to a scaling factor
#' @param sample_size Unused.
#' 
#' @return a vector of mean values
#'
#' @export
compute_mean <- function(response, weight, sample_size)
{
  response_mean <- compute_count_response(response, weight, sample_size)
  response_mean$sample_size <- sample_size
  
  return(response_mean)
}


#' Wrapper for `compute_binary_response` that adds sample_size
#'
#' @param response a vector of binary (0 or 1) responses
#' @param weight a vector of sample weights for inverse probability weighting;
#'   invariant up to a scaling factor
#' @param sample_size The sample size to use, which may be a non-integer (as
#'   responses from ZIPs that span geographical boundaries are weighted
#'   proportionately, and survey weights may also be applied)
#'   
#' @return a vector of percentages
#'
#' @export
compute_pct <- function(response, weight, sample_size)
{
  response_pct <- compute_binary_response(response, weight, sample_size)
  response_pct$sample_size <- sample_size
  
  return(response_pct)
}



#' Returns multiple choice response estimates
#'
#' This function takes vectors as input and computes the response values
#' (a point estimate named "val" and a sample size
#' named "sample_size").
#'
#' @param response a vector of multiple choice responses
#' @param weight a vector of sample weights for inverse probability weighting;
#'   invariant up to a scaling factor
#' @param sample_size The sample size to use, which may be a non-integer (as
#'   responses from ZIPs that span geographical boundaries are weighted
#'   proportionately, and survey weights may also be applied)
#'
#' @return a vector of counts
#'
#' @export
compute_count <- function(response, weight, sample_size)
{
  assert(all( response >= 0 ))
  assert(length(response) == length(weight))
  
  return(list(val = sample_size,
              sample_size = sample_size,
              se = NA_real_,
              effective_sample_size = sample_size)) # TODO effective sample size
}


#' Produce aggregates for all desired aggregations.
#'
#' Writes the outputs directly to CSVs in the directory specified by `params`.
#' Produces output using all available data between `params$start_date` and 
#' `params$end_date`, inclusive.
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
#' @param cw_list Named list of geographic crosswalks, each of which maps a zip5
#'   to a geographic level such as county or state. Aggregates will be produced
#'   for each geographic level.
#' @param params Named list of configuration parameters.
#'
#' @return none
#'
#' @import data.table
#' @importFrom dplyr full_join
#' @importFrom purrr reduce
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
  
  output <- post_process_aggs(df, aggregations, cw_list)
  df <- output[[1]]
  aggregations <- output[[2]]
  
  agg_groups <- unique(aggregations[c("group_by", "geo_level")])
  
  # For each unique combination of groupby_vars and geo level, run aggregation process once
  # and calculate all desired aggregations on the grouping. Rename columns. Save
  # to individual files
  for (group_ind in seq_along(agg_groups$group_by)) {
    
    agg_group <- agg_groups$group_by[group_ind][[1]]
    geo_level <- agg_groups$geo_level[group_ind]
    geo_crosswalk <- cw_list[[geo_level]]
    
    # Subset aggregations to keep only those grouping by the current agg_group
    # and with the current geo_level. `setequal` ignores differences in 
    # ordering and only looks at unique elements.
    these_aggs <- aggregations[mapply(aggregations$group_by,
                                     FUN=function(x) {setequal(x, agg_group)
                                     }) & aggregations$geo_level == geo_level, ]
    
    dfs_out <- summarize_aggs(df, geo_crosswalk, these_aggs, geo_level, params)
    
    # If want to additionally keep "se" and "effective_sample_size", add here.
    keep_vars <- c("val", "sample_size")
    
    for (agg_metric in names(dfs_out)) {
      map_old_new_names <- keep_vars
      names(map_old_new_names) <- paste(keep_vars, agg_metric, sep="_")
      
      dfs_out[[agg_metric]] <- rename(
        dfs_out[[agg_metric]][, c(agg_group, keep_vars)], map_old_new_names)
    }
    
    df_out <- dfs_out %>% reduce(full_join, by=agg_group, suff=c("", ""))
    write_contingency_tables(df_out, params, geo_level, agg_group)
  }
}


#' Post-process aggregations and data to make more generic
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
#' @param cw_list Named list of geographic crosswalks, each of which maps a zip5
#'   to a geographic level such as county or state. Aggregates will be produced
#'   for each geographic level.
#'   
#' @return list of data frame of individual response data and user-set data
#' frame of desired aggregations
#'
#' @export
post_process_aggs <- function(df, aggregations, cw_list) {
  aggregations$geo_level <- NA
  for (agg_ind in seq_along(aggregations$group_by)) {
    # Add implied geo_level to each group_by. Order alphabetically. Replace 
    # geo_level with generic "geo_id" var
    geo_level <- intersect(aggregations$group_by[agg_ind][[1]], names(cw_list))
    if (length(geo_level) > 1) {
      stop('more than one geo type provided for a single aggregation')
    } else if (length(geo_level) == 0) {
      geo_level <- "nation"
      aggregations$group_by[agg_ind][[1]] <- 
        sort(append(aggregations$group_by[agg_ind][[1]], "geo_id"))
    } else {
      aggregations$group_by[agg_ind][[1]][
        aggregations$group_by[agg_ind][[1]] == geo_level] <- "geo_id"
      aggregations$group_by[agg_ind][[1]] <- 
        sort(unique(aggregations$group_by[agg_ind][[1]]))
    }
    
    aggregations$geo_level[agg_ind] <- geo_level
    
    if (startsWith(aggregations$metric[agg_ind], "mc_")) {
      # Multiple choice metrics should also be included in the groupby vars
      if ( !(aggregations$metric[agg_ind] %in% 
             aggregations$group_by[agg_ind][[1]]) ) {
        aggregations$group_by[agg_ind][[1]] <- 
          c(aggregations$group_by[agg_ind][[1]], aggregations$metric[agg_ind])
      }
    }
  }
  
  # Convert most columns being used in aggregations to the appropriate format.
  # Multiple choice and multi-select used for grouping are left as-is.
  agg_groups <- unique(aggregations$group_by)
  group_cols_to_convert <- unique(do.call(c, agg_groups))
  group_cols_to_convert <- group_cols_to_convert[startsWith(group_cols_to_convert, "b_")]
  
  metric_cols_to_convert <- unique(aggregations$metric)
  
  for (col_var in c(group_cols_to_convert, metric_cols_to_convert)) {
    if ( is.null(df[[col_var]]) ) {
      # Column not defined.
      aggregations <- aggregations[aggregations$metric != col_var, ]
      next
    }
    
    if (startsWith(col_var, "b_")) { # Binary
      output <- convert_binary_qcodes_to_bool(df, aggregations, col_var)
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
  
  return(list(df, aggregations))
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
convert_binary_qcodes_to_bool <- function(df, aggregations, col_var) {
  if (FALSE %in% df[[col_var]] || TRUE %in% df[[col_var]]) {
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
convert_multiselect_to_binary_cols <- function(df, aggregations, col_var) {
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
convert_freeresponse_to_num <- function(df, aggregations, col_var) {
  df[[col_var]] <- as.numeric(df[[col_var]])
  return(list(df, aggregations))
}


#' Performs calculations across all groupby levels for all aggregations.
#'
#' The organization may seem a bit contorted, but this is designed for speed.
#' The primary bottleneck is repeatedly filtering the data frame to find data
#' for the grouping variables of interest. To save time, we do this once
#' and then calculate all indicators for that groupby combination, rather than
#' separately filtering every time we want to calculate a new table. We also
#' rely upon data.table's keys and indices to allow us to do the filtering in
#' O(log n) time, which is important when the data frame contains millions of
#' rows.
#'
#' @param df a data frame of survey responses
#' @param crosswalk_data An aggregation, such as zip => county or zip => state,
#'   as a data frame with a "zip5" column to join against.
#' @param aggregations Data frame with columns `name`, `var_weight`, `metric`,
#'   `group_by`, `compute_fn`, `post_fn`. Each row represents one aggregate
#'   to report. `name` is the aggregate's base column name; `var_weight` is the 
#'   column to use for its weights; `metric` is the column of `df` containing the
#'   response value. `group_by` is a list of variables used to perform the 
#'   aggregations over. `compute_fn` is the function that computes
#'   the aggregate response given many rows of data. `post_fn` is applied to the
#'   aggregate data after megacounty aggregation, and can perform any final
#'   calculations necessary.
#' @param geo_level the aggregation level, such as county or state, being used
#' @param params a named list with entries "s_weight", "s_mix_coef",
#'   "num_filter"
#'
#' @importFrom dplyr inner_join bind_rows
#' @importFrom parallel mclapply
#' 
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
  
  if (all(groupby_vars %in% names(df))) {
    unique_group_combos <- unique(df[, ..groupby_vars])
    unique_group_combos <- unique_group_combos[complete.cases(unique_group_combos)]
  } else {
    msg_plain(
      sprintf(
        "not all of groupby columns %s available in data; skipping this aggregation", 
        paste(groupby_vars, collapse=", ")
      ))
  }
  
  if (!exists("unique_group_combos") || nrow(unique_group_combos) == 0) {
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
    dfs <- mclapply(seq_along(unique_group_combos[[1]]), calculate_group)
  } else {
    dfs <- lapply(seq_along(unique_group_combos[[1]]), calculate_group)
  }
  
  ## Now we have a list, with one entry per groupby level, each containing a
  ## list of one data frame per aggregation. Rearrange it.
  dfs_out <- list()
  for (aggregation in aggregations$name) {
    dfs_out[[aggregation]] <- bind_rows( lapply(dfs, function(groupby_levels) { 
      groupby_levels[[aggregation]] 
    }))
  }
  
  ## Do post-processing.
  for (row in seq_len(nrow(aggregations))) {
    aggregation <- aggregations$name[row]
    groupby_vars <- aggregations$group_by[[row]]
    post_fn <- aggregations$post_fn[[row]]
    
    dfs_out[[aggregation]] <- dfs_out[[aggregation]][
      rowSums(is.na(dfs_out[[aggregation]][, c("val", "sample_size", groupby_vars)])) == 0,
    ]
    
    if (geo_level == "county") {
      df_megacounties <- megacounty(dfs_out[[aggregation]], params$num_filter, groupby_vars)
      dfs_out[[aggregation]] <- bind_rows(dfs_out[[aggregation]], df_megacounties)
    }
    
    dfs_out[[aggregation]] <- apply_privacy_censoring(dfs_out[[aggregation]], params)
    
    ## *After* gluing together megacounties, apply the post-function
    dfs_out[[aggregation]] <- post_fn(dfs_out[[aggregation]])
  }
  
  return(dfs_out)
}



#' Censor aggregates to ensure privacy.
#'
#' Currently done in simple, static way: Rows with sample size less than 100 are
#' removed; no noise is added.
#'
#' @param df a data frame of summarized response data
#' @param params a named list with entries "s_weight", "s_mix_coef",
#'   "num_filter"
#'
#' @importFrom dplyr filter
#' @importFrom rlang .data
#'
#' @export
apply_privacy_censoring <- function(df, params) {
  return(filter(df,
         .data$sample_size >= params$num_filter,
         .data$effective_sample_size >= params$num_filter))
}



#' Produce estimates for all indicators in a specific target group.
#' 
#' @param group_df Data frame containing all data needed to estimate one group.
#'   Estimates for `target_group` will be based on all of this data.
#' @param aggregations Aggregations to report. See `aggregate_aggs()`.
#' @param target_group A `data.table` with one row specifying the grouping
#'   variable values used to select this group.
#' @param params Named list of configuration options.
#' 
#' @importFrom tibble add_column
#' 
#' @export
summarize_aggregations_group <- function(group_df, aggregations, target_group, geo_level, params) {
  ## Prepare outputs.
  dfs_out <- list()
  for (index in seq_along(aggregations$name)) {
    aggregation <- aggregations$name[index]
    
    dfs_out[[aggregation]] <- target_group %>%
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
  
  return(dfs_out)
}


#' Write csv file for sharing with researchers
#' 
#' CSV name includes date specifying start of time period aggregated, geo level,
#' and grouping variables.
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
#' @importFrom dplyr arrange_at
#' 
#' @export
write_contingency_tables <- function(data, params, geo_level, groupby_vars)
{
  if (!is.null(data) && nrow(data) != 0) {
    data <- arrange_at(data, groupby_vars)
  } else {
    msg_plain(sprintf(
      "no aggregations produced for grouping variables %s (%s); CSV will not be saved", 
      paste(groupby_vars, collapse=", "), geo_level
    ))
    return()
  }
  
  file_out <- file.path(
    params$export_dir, sprintf("%s_%s_%s.csv", format(params$start_date, "%Y%m%d"),
                               geo_level, paste(groupby_vars, collapse="_"))
  )
  
  create_dir_not_exist(params$export_dir)
  
  msg_df(sprintf(
    "saving contingency table data to %-35s",
    sprintf("%s_%s_%s", format(params$start_date, "%Y%m%d"),
            geo_level, paste(groupby_vars, collapse="_"))
  ), data)
  write_csv(data, file_out)
}



#' Get the date of the first day of the previous month
#'
#' @param date Date that output will be calculated relative to
#' 
#' @return Date
#' 
#' @importFrom lubridate floor_date
#' 
#' @export
start_of_prev_full_month <- function(date) {
  return(floor_date(date, "month") - months(1))
}

#' Get the date of the last day of the previous month
#'
#' @param date Date that output will be calculated relative to
#' 
#' @return Date
#' 
#' @importFrom lubridate ceiling_date days
#' 
#' @export
end_of_prev_full_month <- function(date) {
  if (ceiling_date(date, "month") == date) {
    return(date)
  }
  
  return(floor_date(date, "month") - days(1))
}


#' Get the date range specifying the previous month
#'
#' @param date Date that output will be calculated relative to
#' 
#' @return list of two Dates
#' 
#' @export
get_range_prev_full_month <- function(date = Sys.Date()) {
  eom <- end_of_prev_full_month(date)
  
  if (eom == date) {
    som <- start_of_prev_full_month(date + months(1))
  } else {
    som <- start_of_prev_full_month(date)
  }
  
  return(list(som, eom))
}


#' Get the date of the first day of the previous week
#'
#' @param date Date that output will be calculated relative to
#' 
#' @return Date
#' 
#' @importFrom lubridate floor_date weeks
#' 
#' @export
start_of_prev_full_week <- function(date) {
  return(floor_date(date, "week") - weeks(1))
}


#' Get the date of the last day of the previous week
#'
#' @param date Date that output will be calculated relative to
#' 
#' @return Date
#' 
#' @importFrom lubridate ceiling_date days
#' 
#' @export
end_of_prev_full_week <- function(date) {
  if (ceiling_date(date, "week") == date) {
    return(date)
  }
  
  return(floor_date(date, "week") - days(1))
}


#### TODO: should be epiweeks eventually. Already exists a package to calculate?
#' Get the date range specifying the previous week
#'
#' @param date Date that output will be calculated relative to
#' 
#' @return list of two Dates
#' 
#' @importFrom lubridate weeks
#' 
#' @export
get_range_prev_full_week <- function(date = Sys.Date()) {
  eow <- end_of_prev_full_week(date)
  
  if (eow == date) {
    sow <- start_of_prev_full_week(date + weeks(1))
  } else {
    sow <- start_of_prev_full_week(date)
  }
  
  return(list(sow, eow))
}


#' Get the date range specifying the previous full time period
#'
#' @param date Date that output will be calculated relative to
#' @param weekly_or_monthly_flag string "weekly" or "monthly" indicating desired
#' time period to aggregate over
#' 
#' @return list of two Dates
#' 
#' @importFrom lubridate ymd_hms
#' 
#' @export
get_range_prev_full_period <- function(date = Sys.Date(), weekly_or_monthly_flag) {
  if (weekly_or_monthly_flag == "month") {
    # Get start and end of previous full month.
    date_period_range = get_range_prev_full_month(date)
  } else if (weekly_or_monthly_flag == "epiweek") {
    # Get start and end of previous full epiweek.
    date_period_range = get_range_prev_full_week(date)
  }
  
  date_period_range[[1]] =  ymd_hms(
    sprintf("%s 00:00:00", date_period_range[[1]]), tz = "America/Los_Angeles"
  )
  date_period_range[[2]] =  ymd_hms(
    sprintf("%s 23:59:59", date_period_range[[2]]), tz = "America/Los_Angeles"
  )
  
  return(date_period_range)
}


#' Write a time-stamped message to the console
#'
#' @param text the body of the message to display
#'
#' @export
msg_plain <- function(text) {
  message(sprintf("%s --- %s", format(Sys.time()), text))
}
