#' Load data from cached archive
#'
#' The archive is necessary for several reasons:
#'
#' 1. We want to include only the first use of any unique token, and exclude
#' later survey responses that also use that token. This requires a complete
#' history of tokens used. We store this in an RDS instead of having to read the
#' complete CSVs of all historical data.
#'
#' 2. Some of our aggregates are based on averages over 7-day windows. We need
#' at least 7 days of data available to correctly report them.
#'
#' The archiving implicitly assumes a time ordering to the use of this package
#' -- that is, that we calculate indicators using successively newer survey data
#' files, so the archive is updated with newer and newer data. The pipeline does
#' not break if run out of order, but point 2 above may not be met.
#'
#' @param params a named list, read from read_params. Must contain and element
#'   named archive_dir (containing the file "data_agg.Rds") and a logical value
#'   named debug.
#'
#' @importFrom readr read_rds
#' @export
load_archive <- function(params)
{
  if (file.exists(a_path <- file.path(params$archive_dir, "data_agg.Rds")) && !params$debug)
  {
    archive <- read_rds(a_path)
  } else {
    archive <- list(input_data = NULL, seen_tokens = NULL)
  }

  return(archive)
}

#' Save new data to the archive
#'
#' The archive contains:
#'
#' - all previously seen tokens
#'
#' - all prior rows of input data (before filtering for aggregation or sharing),
#'   except for rows with duplicate tokens.
#'
#' @param df a data frame with columns "token", "start_dr" and "ResponseID"
#' @param archive a data frame previously loaded by \code{load_archive}
#' @param params a named list, read from read_params. Must contain an element
#'   named archive_dir (containing the file "data_agg.Rds").
#'
#' @importFrom dplyr bind_rows arrange
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
    x = list(input_data = bind_rows(archive$input_data, df),
             seen_tokens = seen_tokens),
    file.path(params$archive_dir, "data_agg.Rds"),
    compress = "gz" # gz is fast to compress and decompress
  )
}
