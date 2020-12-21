#' Write csv file for sharing with researchers
#' 
#' CSV name includes date specifying start of time period aggregated, geo level,
#' and grouping variables.
#'
#' @param data           a data frame to save; must contain the columns "geo_id", "val",
#'                       "se", "sample_size", and grouping variables. The first four are saved in the
#'                       output; day is used for spliting the data into files.
#' @param params         a named list, containing the value "export_dir" indicating the
#'                       directory where the csv should be saved
#' @param geo_name       name of the geographic level; used for naming the output file
#' @param signal_name    name of the signal; used for naming the output file
#'
#' @importFrom readr write_csv
#' @importFrom dplyr arrange across
#' 
#' @export
write_contingency_tables <- function(data, params, geo_level, groupby_vars)
{
  if (!is.null(data) && nrow(data) != 0) {
    data <- arrange(data, across(groupby_vars))
  } else {
    msg_plain(sprintf(
      "no aggregations produced for grouping variables %s (%s); CSV will not be saved", 
      paste(groupby_vars, collapse=", "), geo_level
    ))
    return()
  }
  
  # Format reported columns.
  data <- mutate_at(data, vars(-c(groupby_vars)), 
                    function(x) formatC(x,digits=7,format="f",drop0trailing=TRUE))
  file_out <- file.path(
    params$export_dir, sprintf("%s_%s_%s.csv", format(params$start_date, "%Y%m%d"),
                               geo_level, paste(groupby_vars, collapse="_"))
  )
  
  create_dir_not_exist(params$export_dir)
  
  msg_df(sprintf(
    "saving contingency table data to %-35s",
    sprintf("%s_%s_%s", format(params$start_date, "%Y%m%d"),
            geo_level, paste(groupby_vars, collapse="_"))
  ), data)
  write_csv(data, file_out)
}
