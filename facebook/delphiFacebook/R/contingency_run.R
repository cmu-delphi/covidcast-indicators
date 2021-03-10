#' Make tables specifying aggregations to output
#'
#' Each row represents one aggregate to report. `name` is the aggregate's base
#' column name.`metric` is the column of `df` containing the response value.
#' `group_by` is a list of variables used to perform the aggregations over.
#' `compute_fn` is the function that computes the aggregate response given many
#' rows of data. `post_fn` is applied to the aggregate data and can perform any
#' final calculations necessary.
#'
#' Listing no groupby vars implicitly computes aggregations at the national
#' level only. Any multiple choice and multi-select questions used in the
#' aggregations should be recoded to be descriptive in
#' `contingency_variables::reformat_responses`.
#'
#' Compute functions must be one of the `compute_*` set (or another function
#' with similar format can be created). Post-processing functions should be one
#' of the `jeffreys_*` set or post_convert_count_to_pct or the identity `I`,
#' which does not modify the data.
#'
#' @return named list
#' 
#' @importFrom tibble tribble
set_aggs <- function() {
  weekly_aggs <- tribble(
    ~name, ~metric, ~group_by, ~compute_fn, ~post_fn,
  )

  monthly_aggs <- tribble(
    ~name, ~metric, ~group_by, ~compute_fn, ~post_fn,
    #### Cut 1: side effects if hesitant about getting vaccine and generally
    # National
    "pct_concerned_sideeffects", "b_concerned_sideeffects", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_hesitant_sideeffects", "b_hesitant_sideeffects", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,

    # State
    "pct_concerned_sideeffects", "b_concerned_sideeffects", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_sideeffects", "b_hesitant_sideeffects", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,

    # State marginal
    "pct_concerned_sideeffects", "b_concerned_sideeffects", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_concerned_sideeffects", "b_concerned_sideeffects", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_concerned_sideeffects", "b_concerned_sideeffects", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_sideeffects", "b_hesitant_sideeffects", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_sideeffects", "b_hesitant_sideeffects", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_sideeffects", "b_hesitant_sideeffects", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,



    #### Cut 2: trust various institutions if hesitant about getting vaccine
    # National
    "pct_hesitant_trust_fam", "b_hesitant_trust_fam", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_healthcare", "b_hesitant_trust_healthcare", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_who", "b_hesitant_trust_who", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_govt", "b_hesitant_trust_govt", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_politicians", "b_hesitant_trust_politicians", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,

    # State
    "pct_hesitant_trust_fam", "b_hesitant_trust_fam", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_healthcare", "b_hesitant_trust_healthcare", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_who", "b_hesitant_trust_who", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_govt", "b_hesitant_trust_govt", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_politicians", "b_hesitant_trust_politicians", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,

    # State marginal
    "pct_hesitant_trust_fam", "b_hesitant_trust_fam", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_fam", "b_hesitant_trust_fam", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_fam", "b_hesitant_trust_fam", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_healthcare", "b_hesitant_trust_healthcare", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_healthcare", "b_hesitant_trust_healthcare", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_healthcare", "b_hesitant_trust_healthcare", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_who", "b_hesitant_trust_who", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_who", "b_hesitant_trust_who", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_who", "b_hesitant_trust_who", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_govt", "b_hesitant_trust_govt", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_govt", "b_hesitant_trust_govt", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_govt", "b_hesitant_trust_govt", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_politicians", "b_hesitant_trust_politicians", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_politicians", "b_hesitant_trust_politicians", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_politicians", "b_hesitant_trust_politicians", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,



    #### Cut 3: trust various institutions
    # National
    "pct_trust_fam", "b_vaccine_likely_friends", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_trust_healthcare", "b_vaccine_likely_local_health", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_trust_who", "b_vaccine_likely_who", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_trust_govt", "b_vaccine_likely_govt_health", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_trust_politicians", "b_vaccine_likely_politicians", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,

    # State
    "pct_trust_fam", "b_vaccine_likely_friends", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_trust_healthcare", "b_vaccine_likely_local_health", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_trust_who", "b_vaccine_likely_who", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_trust_govt", "b_vaccine_likely_govt_health", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_trust_politicians", "b_vaccine_likely_politicians", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,

    # State marginal
    "pct_trust_fam", "b_vaccine_likely_friends", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_trust_fam", "b_vaccine_likely_friends", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_trust_fam", "b_vaccine_likely_friends", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_trust_healthcare", "b_vaccine_likely_local_health", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_trust_healthcare", "b_vaccine_likely_local_health", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_trust_healthcare", "b_vaccine_likely_local_health", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_trust_who", "b_vaccine_likely_who", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_trust_who", "b_vaccine_likely_who", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_trust_who", "b_vaccine_likely_who", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_trust_govt", "b_vaccine_likely_govt_health", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_trust_govt", "b_vaccine_likely_govt_health", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_trust_govt", "b_vaccine_likely_govt_health", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_trust_politicians", "b_vaccine_likely_politicians", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_trust_politicians", "b_vaccine_likely_politicians", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_trust_politicians", "b_vaccine_likely_politicians", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,



    #### Cuts 4, 5, 6: vaccinated and accepting if senior, in healthcare, or generally
    # National
    "pct_vaccinated", "b_had_cov_vaccine", c("b_work_in_healthcare", "mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("b_65_or_older", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_work_in_healthcare", "mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_65_or_older", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,

    # State
    "pct_vaccinated", "b_had_cov_vaccine", c("b_work_in_healthcare", "mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("b_65_or_older", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_work_in_healthcare", "mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_65_or_older", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,

    # State marginal
    "pct_vaccinated", "b_had_cov_vaccine", c("b_work_in_healthcare", "mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("b_work_in_healthcare","mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("b_work_in_healthcare", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("b_65_or_older", "mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("b_65_or_older", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_work_in_healthcare", "mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_work_in_healthcare","mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_work_in_healthcare", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_65_or_older", "mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_65_or_older", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,

  )

  return(list("week"=weekly_aggs, "month"=monthly_aggs))
}


#' Run the contingency table production pipeline
#'
#' @param params    Params object produced by read_params
#'
#' @return none
#' 
#' @importFrom parallel detectCores
#'
#' @export
run_contingency_tables <- function(params) {
  aggs <- set_aggs()
  
  ## Set default number of cores for mclapply to the total available number,
  ## because we are greedy and this will typically run on a server.
  if (params$parallel) {
    cores <- detectCores()
    
    if (is.na(cores)) {
      warning("Could not detect the number of CPU cores; parallel mode disabled")
      params$parallel <- FALSE
    } else {
      options(mc.cores = cores)
      msg_plain(paste0("Running on ", cores, " cores"))
    }
  }
  
  if (params$aggregate_range == "week") {
    run_contingency_tables_many_periods(params, aggs$week)
  } else if (params$aggregate_range == "month") {
    run_contingency_tables_many_periods(params, aggs$month)
  } else if (params$aggregate_range == "both") {
    params$aggregate_range <- "week"
    run_contingency_tables_many_periods(params, aggs$week)

    params$aggregate_range <- "month"
    run_contingency_tables_many_periods(params, aggs$month)
  }
}


#' Wrapper that runs `run_contingency_tables_one_period` over several time ranges
#'
#' Allows pipeline to create a series of CSVs for a range of dates.
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
#' @importFrom lubridate ymd days
run_contingency_tables_many_periods <- function(params, aggregations)
{
  if (!is.null(params$n_periods)) {
    msg_plain(paste0("Producing CSVs for ", params$n_periods, " time periods"))

    if (params$aggregate_range == "week") {
      period_step <- days(7)
    } else if (params$aggregate_range == "month") {
      period_step <- months(1)
    } else if (is.null(params$aggregate_range)) {
      stop("aggregate_range setting must be provided in params")
    }
    
    params$end_date <- ifelse(
      is.null(params$end_date), as.character(Sys.Date()), params$end_date
    )
    # Make list of dates to aggregate over.
    end_dates <- as.character(sort(
      ymd(params$end_date) - period_step * seq(0, params$n_periods - 1)
    ))

    for (end_date in end_dates) {
      period_params <- params
      
      # Update start/end date and time.
      period_params$end_date <- end_date
      if ( !(end_date == end_dates[1]) ) {
        period_params$start_date <- NULL
      }
      
      period_params$start_time <- ymd_hms(
        sprintf("%s 00:00:00", period_params$start_date), tz = tz_to
      )
      period_params$end_time <- ymd_hms(
        sprintf("%s 23:59:59", period_params$end_date), tz = tz_to
      )
      
      run_contingency_tables_one_period(period_params, aggregations)
    }
  } else {
    ## Produce CSVs for a single time period
    run_contingency_tables_one_period(params, aggregations)
  }


}

#' Run the contingency table production pipeline for a single time period
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
run_contingency_tables_one_period <- function(params, aggregations)
{
  params <- update_params(params)
  aggregations <- verify_aggs(aggregations)
  
  msg_plain(paste0("Producing aggregates for ", params$start_date, " through ", params$end_date))

  if (nrow(aggregations) > 0) {
    cw_list <- produce_crosswalk_list(params$static_dir)
    archive <- load_archive(params)
    msg_df("archive data loaded", archive$input_data)

    input_data <- load_responses_all(params)
    input_data <- filter_responses(input_data, params)
    msg_df("response input data", input_data)

    input_data <- merge_responses(input_data, archive)
    data_agg <- create_data_for_aggregation(input_data)
    data_agg <- filter_data_for_aggregation(data_agg, params,
                                              lead_days = params$backfill_days)

    if (nrow(data_agg) == 0) {
      msg_plain(sprintf("no data available in the desired date range %s- to %s",
                        params$start_date, params$end_date))
      return()
    }

    data_agg <- join_weights(data_agg, params, weights = "full")
    msg_df("response data to aggregate", data_agg)

    data_agg <- make_human_readable(data_agg)

    produce_aggregates(data_agg, aggregations, cw_list, params)
  }

}
