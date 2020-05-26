#' Load data from cached archive
#'
#' @param params     a named list, read from read_params. Must contain and element named
#'                   archive_dir (containing the file "data_agg.Rds") and a logical value
#'                   named debug.
#'
#' @importFrom readr read_rds
#' @importFrom dplyr semi_join
#' @export
load_archive <- function(params)
{
  if (file.exists(a_path <- file.path(params$archive_dir, "data_agg.Rds")) & !params$debug)
  {
    archive <- read_rds(a_path)
  } else {
    archive <- list(data_agg = NULL, seen_tokens = NULL)
    return(archive)
  }

  return(archive)
}

#' Save data to the archive
#'
#' @param df         a data frame with columns "token", "start_dr" and "ResponseID"
#' @param archive    a data frame previously loaded by \code{load_archive}
#' @param params     a named list, read from read_params. Must contain and element named
#'                   archive_dir (containing the file "data_agg.Rds").
#'
#' @importFrom dplyr select bind_rows arrange
#' @importFrom readr write_rds
#' @importFrom rlang .data
#' @export
update_archive <- function(df, archive, params)
{
  seen_tokens <- df[, c("token", "start_dt", "ResponseId")]
  seen_tokens <- bind_rows(archive$seen_tokens, seen_tokens)
  seen_tokens <- arrange(seen_tokens, .data$start_dt)
  seen_tokens <- seen_tokens[!duplicated(seen_tokens$token), ]

  create_dir_not_exist(params$archive_dir)

  write_rds(
    x = list(data_agg = df, seen_tokens = seen_tokens),
    file.path(params$archive_dir, "data_agg.Rds"),
    compress = "bz2"
  )
}
