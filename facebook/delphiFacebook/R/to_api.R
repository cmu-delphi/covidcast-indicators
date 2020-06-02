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
  for (tunit in unique(data$day))
  {
    df <- data[data$day == tunit, c("geo_id", "val", "se", "sample_size", "effective_sample_size")]
    df$val <- round(df$val, 7L)
    df$se <- round(df$se, 7L)
    df$sample_size <- round(df$sample_size, 7L)
    file_out <- file.path(
      params$export_dir, sprintf("%s_%s_%s.csv", tunit, geo_name, signal_name)
    )

    create_dir_not_exist(params$export_dir)

    msg_df(sprintf(
      "saving data for API to %-35s",
      sprintf("%s_%s_%s", tunit, geo_name, signal_name)
    ), df)
    write_csv(df, file_out)
  }
}
