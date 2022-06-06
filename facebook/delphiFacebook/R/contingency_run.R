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
  if (!is.null(params$debug) && params$debug) {
    debug_msg <- "!!!debug is on and the standard privacy threshold for sample size is disabled!!!"
    msg_plain(debug_msg)
    warning(debug_msg)
  }
  
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
  
  aggs <- get_aggs()
  
  if ( length(params[["aggregate_range"]]) != 1 || !(params$aggregate_range %in% c("week", "month")) ) {
    stop(paste0("aggregate_range setting must be provided in params and be one",
    " of 'week' or 'month'"))
  }
  
  run_contingency_tables_many_periods(params, aggs[[params$aggregate_range]])
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
    
    params$end_date <- ifelse(
      is.null(params$end_date), as.character(Sys.Date()), params$end_date
    )
    
    # Make list of dates to aggregate over.
    if (params$aggregate_range == "month") {
      period_step <- months(1)
    } else {
      period_step <- days(7)
    }
    
    end_dates <- as.character(sort(
      ymd(params$end_date) - period_step * seq(0, params$n_periods - 1)
    ))

    for (end_date in end_dates) {
      period_params <- params
      
      # Update start/end date and time.
      period_params$end_date <- end_date
      if ( end_date != end_dates[1] ) {
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

    input_data <- load_responses_all(params, contingency_run = TRUE)
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

    data_agg <- add_weights(data_agg, params, weights = "full")$df
    msg_df("response data to aggregate", data_agg)

    produce_aggregates(data_agg, aggregations, cw_list, params)
  }

}
