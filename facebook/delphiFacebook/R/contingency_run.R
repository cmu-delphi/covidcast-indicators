#' Wrapper that runs `run_contingency_tables_one_period` over several time periods
#'
#' Allows pipeline to be create a series of CSVs for a range of dates.
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
#' @importFrom lubridate ymd days
#' 
#' @export
run_contingency_tables <- function(params, aggregations)
{
  if (!is.null(params$end_date) & !is.null(params$n_periods)) {
    ## Produce historical CSVs
    
    if (params$aggregate_range == "week") {
      period_step <- days(7)
    } else if (params$aggregate_range == "week") {
      period_step <- months(1)
    } else if (is.null(params$aggregate_range)) {
      stop("setting aggregate_range must be provided in params")
    }
    
    # Make list of dates to aggregate over.
    end_dates <- sort( ymd(params$end_date) - period_step * seq(0, params$n_periods) )
    
    for (end_date in end_dates) {
      period_params <- params
      period_params$end_date <- end_date
      run_contingency_tables_one_period(period_params, aggregations)
    }
  } else {
    ## Produce CSVs for a single time period
    run_contingency_tables_one_period(params, aggregations)
  }
  

}

#' Run the contingency table production pipeline for a single time period
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
run_contingency_tables_one_period <- function(params, aggregations)
{
  params <- update_params(params)
  aggregations <- verify_aggs(aggregations)
  
  cw_list <- produce_crosswalk_list(params$static_dir)
  archive <- load_archive(params)
  msg_df("archive data loaded", archive$input_data)
  
  input_data <- load_responses_all(params)
  input_data <- filter_responses(input_data, params)
  input_data <- bodge_v4_translation(input_data)
  msg_df("response input data", input_data)
  
  input_data <- merge_responses(input_data, archive)
  data_agg <- create_data_for_aggregatation(input_data)
  data_agg <- filter_data_for_aggregatation(data_agg, params, 
                                            lead_days = params$backfill_days)
  
  if (nrow(data_agg) == 0) {
    msg_plain(sprintf("no data available in the desired date range %s- to %s", 
            params$start_date, params$end_date))
    return()
  }
  
  data_agg <- join_weights(data_agg, params, weights = "full")
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
  
  if (nrow(aggregations) > 0) {
    aggregate_aggs(data_agg, aggregations, cw_list, params)
  }
  
}
