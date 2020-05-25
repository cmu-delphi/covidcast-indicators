#' Save individual response data
#'
#' The "token" column is removed from the input dataset, which is otherwise saved as-is.
#'
#' @param data_full_w    data frame of response values
#' @param params         a named list containing an element named "individual_dir"
#'                       indicating where the output file will be written, and values
#'                       "start_time" and "end_time" used in the naming the output file
#'
#' @importFrom readr write_csv
#' @importFrom dplyr select
#' @importFrom rlang .data
#' @export
write_individual <- function(data_full_w, params)
{
  tz_to <- "America/Los_Angeles"

  fname_out <- sprintf(
    "cvid_responses_%s_-_%s_for_weights_%s.csv",
    format(params$start_time, "%H_%M_%Y_%m_%d", tz = tz_to),
    format(params$end_time, "%H_%M_%Y_%m_%d", tz = tz_to),
    format(params$start_time, "%Y_%m_%d", tz = tz_to),
    ".csv"
  )

  create_dir_not_exist(params$individual_dir)

  msg_df("saving individual data", data_full_w)
  write_csv(select(data_full_w, -.data$token), file.path(params$individual_dir, fname_out))
}
