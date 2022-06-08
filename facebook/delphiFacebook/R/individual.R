#' Save individual response data
#'
#' The "token" column is removed from the input dataset and only rows with
#' corresponding weights are written.
#'
#' @param data_full_w data frame of response values
#' @param params a named list containing an element named "individual_dir"
#'     indicating where the output file will be written, and values "start_time"
#'     and "end_time" used in naming the output file
#'
#' @importFrom dplyr select rename
#' @importFrom readr read_csv cols col_character
#' @importFrom rlang .data
#' @export
write_individual <- function(data_full_w, params)
{
  # Has columns "cid", "date", and "test_group"
  experimental_groups <- read_csv("cmu_cid_qp_user_group_mapping_20220408.csv", col_types = cols(.default = col_character())) %>% distinct()
  data_full_w <- data_full_w %>% left_join(experimental_groups, by=c("token"="cid", "Date"="date"))

  data_to_write <- select(data_full_w, -.data$token)
  data_to_write <- rename(data_to_write, fips = .data$geo_id)
  data_to_write <- dplyr::filter(data_to_write, !is.na(.data$weight))

  ## Date is a column of YYYY-MM-DD strings
  dates <- as.Date(unique(data_to_write$Date))

  if (!is.null(params$produce_individual_raceeth) && params$produce_individual_raceeth) {
    create_dir_not_exist(params$individual_raceeth_dir)
    for (ii in seq_along(dates)) {
      write_individual_day(data_to_write, params, dates[ii], params$individual_raceeth_dir, raceeth_version = "_raceeth")
    }
    
    data_to_write <- data_to_write %>% select(-raceethnicity)
  }
  
  assert(!( "raceethnicity" %in% names(data_to_write) ),
         "race/ethnicity information should not be included in standard microdata output")
 
  create_dir_not_exist(params$individual_dir) 
  for (ii in seq_along(dates)) {
    write_individual_day(data_to_write, params, dates[ii], params$individual_dir)
  }
}


#' Save individual response data for a single date
#'
#' @param data_to_write data frame of response values
#' @param params a named list containing values "start_time"
#'     and "end_time" used in naming the output file
#' @param date selected day to save data for
#' @param out_dir location where the output file will be written
#'
#' @importFrom readr write_csv
#' @importFrom dplyr select
#' @importFrom rlang .data
write_individual_day <- function(data_to_write, params, date, out_dir, raceeth_version = "") {
  fname_out <- sprintf(
    "cvid_responses_%s_experimental_groups%s.csv",
    format(date, "%Y_%m_%d"),
    raceeth_version
  )
  
  day_data <- dplyr::filter(data_to_write, .data$Date == date)
  day_data <- select(day_data, -.data$Date)
  
  msg_df(sprintf("saving individual data for %s",
                 format(date, "%Y%m%d")),
         day_data)
  
  write_csv(day_data,
            file.path(out_dir, fname_out))
}
