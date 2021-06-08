#' Write csv file for injestion into the Delphi API
#'
#' @param data           a data frame to save; must contain the columns "geo_id", "val",
#'                       "se", "sample_size", and "day". The first four are saved in the
#'                       output; day is used for spliting the data into files.
#' @param params         a named list, containing the value "export_dir" indicating the
#'                       directory where the csv should be saved
#' @param geo_name       name of the geographic level; used for naming the output file
#' @param signal_name    name of the signal; used for naming the output file
#'
#' @importFrom readr write_csv
#' @importFrom dplyr arrange
#' @importFrom rlang .data
#' @export
write_data_api <- function(data, params, geo_name, signal_name)
{
  data <- arrange(data, .data$day, .data$geo_id)

  ## Workaround for annoying R misfeature: a for loop over a Date vector yield
  ## numeric entries, rather than dates. Index directly to get the dates.
  unique_dates <- unique(data$day)

  for (ii in seq_along(unique_dates))
  {
    tunit <- unique_dates[ii]

    df <- data[data$day == tunit, c("geo_id", "val", "se", "sample_size", "effective_sample_size")]
    df <- mutate_at(df, vars(-geo_id), function(x) {
      formatC(x, digits=7, format="f", drop0trailing=TRUE)
    })
    file_out <- file.path(
      params$export_dir, sprintf("%s_%s_%s.csv", format(tunit, "%Y%m%d"),
                                 geo_name, signal_name)
    )

    create_dir_not_exist(params$export_dir)

    msg_df(sprintf(
      "saving data for API to %-35s",
      sprintf("%s_%s_%s", format(tunit, "%Y%m%d"), geo_name, signal_name)
    ), df)
    write_csv(df, file_out)
  }
}
