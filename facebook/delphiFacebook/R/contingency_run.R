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
  aggregations <- verify_aggs(aggregations)
  
  cw_list <- produce_crosswalk_list(params$static_dir)
  archive <- load_archive(params)
  msg_df("archive data loaded", archive$input_data)
  
  input_data <- load_responses_all(params)
  input_data <- filter_responses(input_data, params)
  msg_df("response input data", input_data)
  
  input_data <- merge_responses(input_data, archive)
  data_agg <- create_data_for_aggregatation(input_data)
  
  data_agg <- filter_data_for_aggregatation(data_agg, params, lead_days = 12)
  
  if (nrow(data_agg) == 0) {
    stop("no data available in the desired date range")
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
