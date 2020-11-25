# Get data
path_to_raw_data = "/mnt/sshftps/surveys/raw/"

wave1 = "2020-08-29.2020-08-22.2020-08-29.Survey_of_COVID-Like_Illness_-_TODEPLOY_2020-04-06.csv"
wave2 = "2020-11-06.2020-10-30.2020-11-06.Survey_of_COVID-Like_Illness_-_TODEPLOY_......_-_US_Expansion.csv"
wave3 = "2020-11-06.2020-10-30.2020-11-06.Survey_of_COVID-Like_Illness_-_TODEPLOY-_US_Expansion_-_With_Translations.csv"
wave4 = "2020-11-06.2020-10-30.2020-11-06.Survey_of_COVID-Like_Illness_-_Wave_4.csv"


# All surveys have 2 non-response rows at the top. First is detail of question.
# Second is json(?) field access info -- not needed.
wave1 = read.csv(file.path(path_to_raw_data, wave1), header = TRUE)
# Shows 1 (uncompleted) response; 59 fields
wave2 = read.csv(file.path(path_to_raw_data, wave2), header = TRUE)
# Shows 230k responses; 83 fields. Most updated form of survey.
wave3 = read.csv(file.path(path_to_raw_data, wave3), header = TRUE)
# Shows 230k responses; 83 fields. Most updated form of survey.
wave4 = read.csv(file.path(path_to_raw_data, wave4), header = TRUE)


summarize_indicators_day(wave_data, tibble(), "target_day", "state", params)

# Start with example:
#     group_by(epiweek, state, age, race) %>% summarize(mean(tested_positive), mean(cli), n())
library(lubridate)


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
  cw_list <- produce_crosswalk_list(params$static_dir) # Get mapping of zip code to each geo region at different geo levels
  archive <- load_archive(params) # Load archive of already-seen CIDs; archive also
  # saves the last params$archive_days of data for backfill and smoothing (e.g. 7dav) purposes
  msg_df("archive data loaded", archive$input_data)

  # load all input csv files and filter according to selection criteria
  input_data <- load_responses_all(params) # Load all files listed in params$input from params$input_dir
  input_data <- filter_responses(input_data, params) # Keep only first instance of each CID. Keep responses after params$end_date.
  # Individual and aggregate procedures take care of filtering before params$start_date
  msg_df("response input data", input_data)

  input_data <- merge_responses(input_data, archive) # combine newly loaded data with
  # archived data from last params$archive_days. If newly completed response was started
  # before a previously-completed response with the same CID, keep the one started first.

  # create data that will be aggregated for covidcast
  data_agg <- create_data_for_aggregatation(input_data) # Create new columns for reporting.
  # Includes # of sick people, symptom counts, flags for ili and cli, and prob of
  # cli and ili, flags for knowing someone in community and/or household who are sick

  data_agg <- filter_data_for_aggregatation(data_agg, params, lead_days = 12) # Keep
  # only good zips, and data after params$start_date with adjustment for lead_days.
  # Keep only rows with aggregate columns of interest not missing/unreasonable
  data_agg <- join_weights(data_agg, params, weights = "step1") # Add weights to data,
  # step 1 for aggregations
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

  # write files for each specific output
  if ( "cids" %in% params$output )
  {
    write_cid(data_agg, "part_a", params)
  }
  if ( "archive" %in% params$output )
  {
    update_archive(input_data, archive, params)
  }

  data_agg <- set_human_readable_colnames(data_agg)

  if (params$aggregate_range == "weekly") {
    data_agg$period_start_date <- start_of_week(data_agg$day)
  } else if (params$aggregate_range == "monthly") {
    data_agg$period_start_date <- start_of_month(data_agg$day)
  }

  aggregations <- get_aggs_from_params(params)

  if (nrow(aggregations) > 0) {
    aggregate_aggs(data_agg, aggregations, cw_list, params)
  }

}


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
                             "consent" = "S1",
                             "hh_fever" = "A1_1",
                             "hh_sore_throat" = "A1_2",
                             "hh_cough" = "A1_3",
                             "hh_shortness_of_breath" = "A1_4",
                             "hh_difficulty_breathing" = "A1_5",
                             "hh_num_sick" = "A2",
                             "hh_num" = "A2b",
                             "hh_num_children" = "A5_1",
                             "hh_num_adults" = "A5_2",
                             "hh_num_seniors" = "A5_3",
                             "zipcode" = "A3",
                             "state" = "A3b",
                             "cmnty_num_sick" = "A4",
                             "symptoms" = "B2",
                             "symptoms_other" = "B2_14_TEXT",
                             "unusual_symptoms" = "B2c",
                             "unusual_symptoms_other" = "B2c_14_TEXT",
                             "days_unusual_symptoms" = "B2b",
                             "took_temp" = "B3",
                             "highest_temp_f" = "Q40",
                             "cough_mucus" = "B4",
                             "tested_current_illness" = "B5",
                             "hospital" = "B6",
                             "medical_care" = "B7",
                             "tested_ever" = "B8",
                             "tested_14d" = "B10",
                             "tested_pos_14d" = "B10a",
                             "reasons_tested_14d" = "B10b",
                             "wanted_test_14d" = "B12",
                             "reasons_not_tested_14d" = "B12a",
                             "tested_pos_ever" = "B11",
                             "comorbidities" = "C1",
                             "flu_shot_12m" = "C2",
                             "worked_outside_home_5d" = "C3",
                             "worked_healthcare_5d" = "C4",
                             "worked_nursing_home_5d" = "C5",
                             "social_avoidance" = "C7",
                             "trips_outside_home" = "C13",
                             "mask_outside_home" = "C13a",
                             "contact_num_work" = "C10_1_1",
                             "contact_num_shopping" = "C10_2_1",
                             "contact_num_social" = "C10_3_1",
                             "contact_num_other" = "C10_4_1",
                             "mask_public" = "C14",
                             "financial_worry" = "C15",
                             "cmnty_mask_prevalence" = "C16",
                             "flu_shot_jun2020" = "C17",
                             "state_travel" = "C6",
                             "contact_tested_pos" = "C11",
                             "contact_tested_pos_hh" = "C12",
                             "anxiety" = "C8_1",
                             "depression" = "C8_2",
                             "isolation" = "C8_3",
                             "worried_family_ill" = "C9",
                             "gender" = "D1",
                             "gender_other" = "D1_4_TEXT",
                             "pregnant" = "D1b",
                             "age" = "D2",
                             "hh_num_children" = "D3", # Wave 1, etc versions of A5
                             "hh_num_adults_not_self" = "D4",
                             "hh_num_seniors_not_self" = "D5",
                             "hispanic" = "D6",
                             "race" = "D7",
                             "education" = "D8",
                             "worked_4w" = "D9",
                             "worked_outside_home_4w" = "D10",
                             "children_grade" = "E1",
                             "children_school" = "E2",
                             "school_safety_measures" = "E3",
                             "financial_threat" = "Q36",
                             "occupational_group" = "Q64",
                             "job_type_cmnty_social" = "Q65",
                             "job_type_education" = "Q66",
                             "job_type_arts_media" = "Q67",
                             "job_type_healthcare" = "Q68",
                             "job_type_healthcare_support" = "Q69",
                             "job_type_protective" = "Q70",
                             "job_type_food" = "Q71",
                             "job_type_maintenance" = "Q72",
                             "job_type_personal_care" = "Q73",
                             "job_type_sales" = "Q74",
                             "job_type_office_admin" = "Q75",
                             "job_type_construction" = "Q76",
                             "job_type_repair" = "Q77",
                             "job_type_production" = "Q78",
                             "job_type_transport" = "Q79",
                             "occupational_group_other" = "Q80"
                            )

  input_data <- rename(input_data, map_old_new_names[map_old_new_names %in% names(input_data)])
  return(input_data)
}



#' Parses user-specified aggregations.
#'
#' Turns aggregations settings from params file into tibble. Maps keywords to 
#' appropriate functions.
#'
#' @param params Named list of configuration parameters.
#' 
#' @import data.table
#' 
#' @export
get_aggs_from_params <- function(params) {
  # Aggregation settings in params.json are saved as a data.frame with columns
  # group_by, summary_var, and summary_funcs.
  aggregations = params$aggregations
  
  # Always want to calculate sample size, whether or not user requested it
  
  return(aggregations)
}



#' Produce aggregates for all indicators.
#'
#' Writes the outputs directly to CSVs in the directory specified by `params`.
#' Produces output for all available days between `params$start_date -
#' params$backfill_days` and `params$end_date`, inclusive. (We re-output days
#' before `start_date` in case there was backfill for those days.)
#'
#' Warning: The maximum value of `smooth_days` needs to be accounted for in
#' `run.R` in the `lead_days` argument to `filter_data_for_aggregatation`, so
#' the correct amount of archived data is included, plus the expected backfill
#' length.
#'
#' @param df Data frame of individual response data.
#' @param indicators Data frame with columns `name`, `var_weight`, `metric`,
#'   `smooth_days`, `compute_fn`, `post_fn`. Each row represents one indicator
#'   to report. `name` is the indicator's API name; `var_weight` is the column
#'   to use for its weights; `metric` is the column of `df` containing the
#'   response value. `smooth_days` determines how many days to aggregate to
#'   produce one day of responses. `compute_fn` is the function that computes
#'   the aggregate response given many rows of data. `post_fn` is applied to the
#'   aggregate data after megacounty aggregation, and can perform any final
#'   calculations necessary.
#' @param cw_list Named list of geographic crosswalks, each of which maps a zip5
#'   to a geographic level such as county or state. Aggregates will be produced
#'   for each geographic level.
#' @param params Named list of configuration parameters.
#' @import data.table
#' @importFrom dplyr filter mutate_at vars
#' 
#' @export
aggregate_aggs <- function(df, aggregations, cw_list, params) {
  ## The data frame will include more days than just [start_date, end_date], so
  ## select just the unique days contained in that interval.
  days <- unique(df$day)
  #### TODO: filter down to range of days in agg period. here?
  days <- days[days >= as.Date(params$start_date) - params$backfill_days &
                 days <= as.Date(params$end_date)]

  ## For the day range lookups we do on df, use a data.table key. This puts the
  ## table in sorted order so data.table can use a binary search to find
  ## matching dates, rather than a linear scan, and is important for very large
  ## input files.
  df <- as.data.table(df)
  setkey(df, day)

  agg_groups = unique(aggregations$groupby)

  for (agg_group in agg_groups) {
    these_inds = aggregations[aggregations$groupby == agg_group]

    geo_level = intersect(agg_group, names(cw_list))
    if (length(geo_level) > 1) {
      stop('more than one geo type provided for a single aggregation')
    } else if (length(geo_level) == 0) {
      geo_level = "national"
    }
    geo_crosswalk = cw_list[[geo_level]]

    dfs_out <- summarize_aggs(df, geo_crosswalk, these_inds, params)

    for (aggregation in names(dfs_out)) {
      private_df = apply_privacy_censoring(dfs_out[[aggregation]], params)
      #### TODO: see what this write func does; probably amend
      write_data_api(private_df, params, geo_level, aggregation)
    }
  }
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
  return(df[sample_size >= 100])
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
#' @param indicators Data frame of indicators.
#' @param geo_level the aggregation level, such as county or state, being used
#' @param days a vector of Dates for which we should generate response estimates
#' @param params a named list with entries "s_weight", "s_mix_coef",
#'   "num_filter"
#'
#' @importFrom dplyr inner_join bind_rows
#' @importFrom parallel mclapply
#' @export
summarize_aggs <- function(df, crosswalk_data, aggregations, params) {
  ## dplyr complains about joining a data.table, saying it is likely to be
  ## inefficient; profiling shows the cost to be negligible, so shut it up
  # Geo group column is always named "geo_id"
  df <- suppressWarnings(inner_join(df, crosswalk_data, by = "zip5"))

  ## We do batches of just one set of groupby vars at a time, since we have
  ## to select rows based on this.
  assert( length(unique(aggregations$groupby)) == 1 )

  groupby_vars <- aggregations$groupby[1]

  # #### Short form (less optimized? uses built-in data.table grouping functionality)
  # # Always runs count, since we need it for privacy filtering.
  # df_grouped = df[, list(count = .N, summary_vars), keyby = groupby_vars]
  # df_grouped = df_grouped[complete.cases(df_grouped)]


  #### Long form (runs in parallel. faster?)

  ## Set an index on the groupby var columns so that the groupby step can be
  ## dramatically faster; data.table stores the sort order of the column and
  ## uses a binary search to find matching values, rather than a linear scan.
  setindexv(df, groupby_vars)
  unique_group_combos = unique(dt[, ..groupby_vars])

  calculate_group <- function(ii) {
    target_group <- unique_group_combos[ii]
    # Use data.table's index to make this filter efficient
    out <- summarize_aggregations_group(df[as.list(target_group)], aggregations,
                                        target_group, params)

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

  ### TODO: bind_rows again so all unique groups are in same df

  return(dfs_out)
}


#' Produce estimates for all indicators in a specific target group.
#' @param group_df Data frame containing all data needed to estimate one group.
#'   Estimates for `target_group` will be based on all of this data.
#' @param indicators Indicators to report. See `aggregate_indicators()`.
#' @param target_group A `data.table` with one row specifying the grouping
#'   variable values used to select this group.
#' @param params Named list of configuration options.
#' @importFrom dplyr mutate filter
#' @importFrom rlang .data
summarize_aggregations_group <- function(group_df, aggregations, target_group, params) {
  ## Prepare outputs.
  dfs_out <- list()
  geo_ids <- unique(group_df$geo_id)
  for (indicator in indicators$name) {
    dfs_out[[indicator]] <- tibble(
      geo_id = geo_ids,
      day = target_day,
      val = NA_real_,
      se = NA_real_,
      sample_size = NA_real_,
      effective_sample_size = NA_real_
    )
  }

  for (ii in seq_along(geo_ids))
  {
    target_geo <- geo_ids[ii]

    sub_df <- group_df[geo_id == target_geo]

    for (row in seq_len(nrow(indicators))) {
      indicator <- indicators$name[row]
      metric <- indicators$metric[row]
      var_weight <- indicators$var_weight[row]
      compute_fn <- indicators$compute_fn[[row]]

      ind_df <- sub_df[!is.na(sub_df[[var_weight]]) & !is.na(sub_df[[metric]]), ]

      if (nrow(ind_df) > 0)
      {
        s_mix_coef <- params$s_mix_coef
        mixing <- mix_weights(ind_df[[var_weight]] * ind_df$weight_in_location,
                              s_mix_coef, params$s_weight)

        sample_size <- sum(ind_df$weight_in_location)

        ## TODO Fix this. Old pipeline for community responses did not apply
        ## mixing. To reproduce it, we ignore the mixed weights. Once a better
        ## mixing/weighting scheme is chosen, all signals should use it.
        new_row <- compute_fn(
          response = ind_df[[metric]],
          weight = if (indicators$skip_mixing[row]) { mixing$normalized_preweights } else { mixing$weights },
          sample_size = sample_size)

        dfs_out[[indicator]]$val[ii] <- new_row$val
        dfs_out[[indicator]]$se[ii] <- new_row$se
        dfs_out[[indicator]]$sample_size[ii] <- sample_size
        dfs_out[[indicator]]$effective_sample_size[ii] <- new_row$effective_sample_size
      }
    }
  }

  for (row in seq_len(nrow(indicators))) {
    indicator <- indicators$name[row]
    post_fn <- indicators$post_fn[[row]]

    dfs_out[[indicator]] <- dfs_out[[indicator]][
      rowSums(is.na(dfs_out[[indicator]][, c("val", "sample_size", "geo_id", "day")])) == 0,
    ]

    if (geo_level == "county") {
      df_megacounties <- megacounty(dfs_out[[indicator]], params$num_filter)
      dfs_out[[indicator]] <- bind_rows(dfs_out[[indicator]], df_megacounties)
    }

    dfs_out[[indicator]] <- filter(dfs_out[[indicator]],
                                   .data$sample_size >= params$num_filter,
                                   .data$effective_sample_size >= params$num_filter)

    ## *After* gluing together megacounties, apply the Jeffreys correction to
    ## the standard errors.
    dfs_out[[indicator]] <- post_fn(dfs_out[[indicator]])
  }

  return(dfs_out)
}
