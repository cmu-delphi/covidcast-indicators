#' Write csv file for sharing with researchers.
#'
#' CSV name includes date specifying start of time period aggregated, geo level,
#' and grouping variables.
#'
#' @param data           a data frame to save; must contain the columns in
#'                       `groupby_vars`.
#' @param params         a named list, containing the values:
#'                       "export_dir" - directory where the csv should be saved
#'                       "static_dir" - directory where the state lookup file is
#'                       "aggregate_range" - "month", "week", etc.
#'                       "start_date" - start date of the aggregate range
#'                       "end_date" - end date of the aggregate range
#' @param geo_type       name of the geographic level; used for naming the output file
#' @param groupby_vars   character vector of column names used for grouping to
#'                       calculate aggregations; used for naming the output file
#'
#' @importFrom readr write_csv
#' @importFrom dplyr arrange across
#' @importFrom stringi stri_trim
#'
#' @export
write_contingency_tables <- function(data, params, geo_type, groupby_vars)
{
  if (!is.null(data) && nrow(data) != 0) {
    
    # Reorder the group-by columns and sort the dataset by them.
    groupby_vars <- c("geo_id", sort(setdiff(groupby_vars, "geo_id")))
    data <- data %>%
      select(all_of(groupby_vars), everything()) %>%
      arrange(across(all_of(groupby_vars)))

    # Format reported columns.
    format_number <- function(x) {
      stri_trim(formatC(as.numeric(x), digits=7, format="f", drop0trailing=TRUE))
    }
    data <- mutate_at(data, vars(-groupby_vars), format_number)
    
    # Add standard geographic and metadata variables to the data.
    data <- add_geo_vars(data, params, geo_type)
    data <- add_metadata_vars(data, params, geo_type, groupby_vars)
    
    create_dir_not_exist(params$export_dir)
    
    file_name <- get_file_name(params, geo_type, groupby_vars)
    msg_df(sprintf("saving contingency table data to %-35s", file_name), data)
    write_csv(data, file.path(params$export_dir, file_name))

  } else {
    msg_plain(sprintf(
      "no aggregations produced for grouping variables %s (%s); CSV will not be saved",
      paste(groupby_vars, collapse=", "), geo_type
    ))
  }
}

#' Add geographic variables to a dataset, e.g. state and state FIPS codes.
#' 
#' @param data A data frame, containing the variables in groupby_vars.
#' @param params A parameters object with the `static_dir` resources folder.
#' @param geo_type "nation", "state".
#' 
#' @importFrom dplyr bind_cols left_join select
#' @importFrom readr read_csv cols
#' @noRd
add_geo_vars <- function(data, params, geo_type) {
  
  overall <- "Overall"
  
  first <- data.frame(
    country = "United States",
    ISO_3 = "USA",
    GID_0 = "USA"
  )
  
  if (geo_type == "nation") {
    
    rest <- data.frame(
      region = overall,
      GID_1 = NA_character_,
      state = overall,
      state_fips = NA_character_,
      county = overall,
      county_fips = NA_character_
    )
    
  } else if (geo_type == "state") {
    
    states <- read_csv(
      file.path(params$static_dir, "state_list.csv"),
      col_types = cols(.default = "c")
    )
    
    rest <- data.frame(
      region = toupper(data$geo_id),
      state = toupper(data$geo_id),
      county = overall,
      county_fips = NA_character_
    )
    
    rest <- left_join(rest, states, by = "state") %>%
      select(region, GID_1, state, state_fips, county, county_fips)
  }
  
  geo_vars <- bind_cols(first, rest)
  
  # Insert the geographic variables in place of the "geo_id" variable.
  index <- which(names(data) == "geo_id")
  before <- if (index > 1) data[1:(index-1)] else NULL
  after <- data[(index+1):ncol(data)]
  result <- bind_cols(before, geo_vars, after)
  
  return(result)
}

#' Add metadata variables to a dataset, e.g. start and end dates.
#' 
#' @param data A data frame, containing the variables in `groupby_vars.`
#' @param params A parameters object containing start & end date, period, etc.
#' @param geo_type "nation", "state", "county".
#' @param groupby_vars A list of variables `data` is aggregated by.
#' 
#' @importFrom dplyr bind_cols
#' @noRd
add_metadata_vars <- function(data, params, geo_type, groupby_vars) {
  
  aggregation_type <- setdiff(groupby_vars, "geo_id")
  if (length(aggregation_type) == 0) aggregation_type <- "overall"
  
  # Add metadata about this period and level of aggregation.
  metadata <- data.frame(
    survey_geo = "us",
    period_start = format(params$start_date, "%Y%m%d"),
    period_end = format(params$end_date, "%Y%m%d"),
    period_val = get_period_val(params$aggregate_range, params$start_date),
    period_type = get_period_type(params$aggregate_range),
    geo_type = geo_type,
    aggregation_type = paste(aggregation_type, collapse = "_")
  )
  data <- bind_cols(metadata, data)
  data$issue_date <- format(Sys.Date(), "%Y%m%d")
  
  return(data)
}

#' Get the file name for the given parameters, geography, and set of group-by variables.
#' @noRd
get_file_name <- function(params, geo_type, groupby_vars) {
  
  aggregation_type <- setdiff(groupby_vars, "geo_id")
  if (length(aggregation_type) == 0) aggregation_type <- "overall"
  
  file_name <- paste(
    format(params$start_date, "%Y%m%d"),
    format(params$end_date, "%Y%m%d"),
    get_period_type(params$aggregate_range),
    geo_type,
    paste(aggregation_type, collapse = "_"),
    sep = "_"
  )
  file_name <- paste0(file_name, ".csv")
  return(file_name)
}

#' Get the period type for the given range, i.e. "weekly" or "monthly".
#' @noRd
get_period_type <- function(range) {
  switch(
    range,
    "month" = "monthly",
    "week" = "weekly",
    ""
  )
}

#' Get the period value (e.g. epiweek number) for the range and start date.
#' @importFrom lubridate epiweek
#' @noRd
get_period_val <- function(range, period_start) {
  switch(
    range,
    "week" = epiweek(period_start),
    "month" = as.integer(format(period_start, "%m")),
    NA_integer_
  )
}
