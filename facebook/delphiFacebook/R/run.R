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

  # create data that will be aggregated for covidcast
  data_agg <- create_data_for_aggregation(input_data)
  data_agg <- filter_data_for_aggregation(data_agg, params, lead_days = 12)
  weight_result <- add_weights(data_agg, params, weights = "step1")
  data_agg <- weight_result$df
  latest_weight_date_step1 <- weight_result$weight_date
  msg_df("response data to aggregate", data_agg)

  params$latest_weight_date <- ifelse(
    is.na(latest_weight_date_step1), as.Date(params$end_date), latest_weight_date_step1
  )

  # create "complete" data (microdata) that will be shared with research partners
  data_full <- create_complete_responses(input_data, cw_list$county, params)
  data_full <- filter_complete_responses(data_full, params)
  data_full <- add_weights(data_full, params, weights = "full", add_weekly_weights = TRUE)$df
  msg_df("full data to share with research partners", data_full)

  # create module-complete data used to create CID lists separately for each module
  data_module_complete <- filter_module_complete_responses(input_data, params)
  data_module_complete_a <- data_module_complete[["a"]]
  data_module_complete_b <- data_module_complete[["b"]]
  
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
  if ( "cids" %in% params$output )
  {
    write_cid(data_full, "full", params)
    write_cid(data_agg, "part_a", params)
    
    write_cid_experimental_wrapper(data_full, "full", params, "")
    write_cid_experimental_wrapper(data_agg, "part_a", params, "")
    write_cid_experimental_wrapper(data_module_complete_a, "module_complete", params, "modul_a_")
    write_cid_experimental_wrapper(data_module_complete_b, "module_complete", params, "modul_b_")
  }
  if ( "archive" %in% params$output )
  {
    update_archive(input_data, archive, params)
  }
  if ( "individual" %in% params$output )
  {
    write_individual(data_full, params)
  }
  if ( "covidalert" %in% params$output )
  {
    count_indicators <- get_hh_count_indicators()
  } else {
    count_indicators <- tibble()
  }
  if ( "community" %in% params$output )
  {
    binary_indicators <- get_binary_indicators()
  } else {
    binary_indicators <- tibble()
  }

  indicators <- bind_rows(count_indicators, binary_indicators)

  if (nrow(indicators) > 0) {
    aggregate_indicators(data_agg, indicators, cw_list, params)
  }

}
