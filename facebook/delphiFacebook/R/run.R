#' Run the entire pipeline
#'
#' See the README.md file in the source directory for more information about how to run
#' this function.
#'
#' @param params    Params object produced by read_params
#'
#' @return none
#' @importFrom parallel detectCores
#' @export
run_facebook <- function(params)
{
  assert(params$start_date <= params$end_date,
         "start date must precede end date")

  cw_list <- produce_crosswalk_list(params$static_dir)
  archive <- load_archive(params)
  msg_df("archive data loaded", archive$input_data)

  # load all input csv files and filter according to selection criteria
  input_data <- load_responses_all(params)
  input_data <- filter_responses(input_data, params)
  msg_df("response input data", input_data)

  input_data <- merge_responses(input_data, archive)


  # Create "part a" data
  data_agg <- create_data_for_aggregation(input_data)
  data_agg <- filter_data_for_aggregation(data_agg, params, lead_days = 12)
  data_agg <- create_complete_responses(data_agg, cw_list$county, params)
  weight_result <- join_weights(data_agg, params, weights = "weekly part a")
  data_agg <- weight_result$df
  latest_weight_date_step1 <- weight_result$weight_date
  msg_df("part a data", data_agg)


  params$latest_weight_date <- ifelse(
    is.na(latest_weight_date_step1), as.Date(params$end_date), latest_weight_date_step1
  )

  # Create "partial" data (old "full" data)
  data_full <- create_complete_responses(input_data, cw_list$county, params)
  data_full <- filter_complete_responses(data_full, params)
  data_full <- join_weights(data_full, params, weights = "weekly partial")$df
  msg_df("partial data", data_full)


  # Create "full" data (AKA "module complete" data)
  data_module_complete <- create_complete_responses(input_data, cw_list$county, params, "module complete")
  data_module_complete <- filter_module_complete_responses(data_module_complete, params)
  data_module_complete_a <- data_module_complete[["a"]]
  data_module_complete_b <- data_module_complete[["b"]]
  data_module_complete <- bind_rows(data_module_complete_a, data_module_complete_b)
  data_module_complete <- join_weights(data_module_complete, params, weights = "weekly full")$df
  msg_df("full data", data_module_complete)

  
  ## Set default number of cores for mclapply to the total available number,
  ## because we are greedy and this will typically run on a server.
  if (params$parallel) {
    cores <- detectCores()

    if (is.na(cores)) {
      warning("Could not detect the number of CPU cores; parallel mode disabled")
      params$parallel <- FALSE
    } else {
      options(mc.cores = min(params$parallel_max_cores, cores))
    }
  }

  # write files for each specific output
  if ( "individual" %in% params$output )
  {
    write_individual(data_agg, params, "weekly_part_a")
    write_individual(data_full, params, "weekly_partial")
    write_individual(data_module_complete, params, "weekly_full")
  }

}
