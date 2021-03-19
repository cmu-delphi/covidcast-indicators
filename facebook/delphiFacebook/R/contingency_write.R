#' Write csv file for sharing with researchers.
#'
#' CSV name includes date specifying start of time period aggregated, geo level,
#' and grouping variables.
#'
#' @param data           a data frame to save; must contain the columns "geo_id", "val",
#'                       "se", "sample_size", and grouping variables. The first four are saved in the
#'                       output; day is used for spliting the data into files.
#' @param params         a named list, containing the value "export_dir" indicating the
#'                       directory where the csv should be saved
#' @param geo_level      name of the geographic level; used for naming the output file
#' @param groupby_vars   character vector of column names used for grouping to
#'                       calculate aggregations; used for naming the output file
#'
#' @importFrom readr write_csv
#' @importFrom dplyr arrange across
#' @importFrom stringi stri_trim
#'
#' @export
write_contingency_tables <- function(data, params, geo_level, groupby_vars)
{
  if (!is.null(data) && nrow(data) != 0) {
    data <- arrange(data, across(all_of(groupby_vars)))

    # Format reported columns.
    data <- mutate_at(data, vars(-c(groupby_vars)),
                      function(x) {
                        stri_trim(
                          formatC(as.numeric(x), digits=7, format="f", drop0trailing=TRUE)
                        )
                      })

    # Reduce verbosity of grouping vars for output purposes
    groupby_vars <- gsub("_", "", sub(
      ".+?_", "", groupby_vars[groupby_vars != "geo_id"]))
    filename <- sprintf("%s_%s.csv", format(params$start_date, "%Y%m%d"),
                        paste(c(geo_level, groupby_vars), collapse="_"))
    file_out <- file.path(params$export_dir, filename)

    create_dir_not_exist(params$export_dir)

    msg_df(sprintf("saving contingency table data to %-35s", filename), data)
    write_csv(data, file_out)

  } else {
    msg_plain(sprintf(
      "no aggregations produced for grouping variables %s (%s); CSV will not be saved",
      paste(groupby_vars, collapse=", "), geo_level
    ))
  }
}
