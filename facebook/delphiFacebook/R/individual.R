#' Save individual response data
#'
#' The "token" column is removed from the input dataset and only rows with
#' corresponding weights are written.
#'
#' @param data_full_w data frame of response values
#' @param params a named list containing an element named "individual_dir"
#'     indicating where the output file will be written, and values "start_time"
#'     and "end_time" used in the naming the output file
#'
#' @importFrom readr write_csv
#' @importFrom dplyr select rename
#' @importFrom rlang .data
#' @export
write_individual <- function(data_full_w, params)
{
  tz_to <- "America/Los_Angeles"
  create_dir_not_exist(params$individual_dir)

  data_to_write <- select(data_full_w, -.data$token)
  data_to_write <- rename(data_to_write, fips = .data$geo_id)
  data_to_write <- dplyr::filter(data_to_write, !is.na(.data$weight))

  ## Date is a column of YYYY-MM-DD strings
  dates <- as.Date(unique(data_to_write$Date))

  for (ii in seq_along(dates)) {
    date <- dates[ii]

    fname_out <- sprintf(
      "cvid_responses_%s_recordedby_%s.csv",
      format(date, "%Y_%m_%d"),
      format(as.Date(params$end_date), "%Y_%m_%d")
    )

    day_data <- dplyr::filter(data_to_write, .data$Date == date)
    day_data <- select(day_data, -.data$Date)

    msg_df(sprintf("saving individual data for %s",
                   format(date, "%Y%m%d")),
           day_data)

    write_csv(day_data,
              file.path(params$individual_dir, fname_out))

  }
}
