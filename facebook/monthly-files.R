#!/usr/bin/env Rscript

## Produce monthly rollup files.
##
## Usage:
##
## Rscript monthly-files.R YYYY NN path/to/individual/files/
##
## where YYYY is the four-digit year and NN is the two-digit month number, such
## as 06 for June.
##
## Writes the uncompressed aggregated CSV to STDOUT, so redirect STDOUT to your
## desired location and compress as desired.

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(purrr)
})

#' Fetch all survey data in a chosen directory.
#'
#' There can be multiple data files for a single day of survey responses, for
#' example if the data is reissued when late-arriving surveys are recorded.
#' Each file contains *all* data recorded for that date, so only the most
#' recently updated file for each date is needed.
#'
#' This function extracts the date from each file, determines which files contain
#' reissued data, and produces a single data frame representing the most recent
#' data available for each day. It can read gzip-compressed CSV files, such as
#' those on the SFTP site, using `readr::read_csv`.
#'
#' This function handles column types correctly for surveys up to Wave 4.
#'
#' @param directory Directory in which to look for survey CSV files, relative to
#'     the current working directory.
#' @param pattern Regular expression indicating which files in that directory to
#'     open. By default, selects all `.csv.gz` files, such as those downloaded
#'     from the SFTP site.
#' @return A single data frame containing all survey responses. Note that this
#'     data frame may have millions of rows and use gigabytes of memory, if this
#'     function is run on *all* survey responses.
get_survey_df <- function(directory, pattern = "*.csv.gz$") {
  files <- list.files(directory, pattern = pattern)

  if (length(files) == 0) {
    stop("No matching data files.")
  }

  files <- map_dfr(files, get_file_properties)

  latest_files <- files %>%
    group_by(date) %>%
    filter(recorded == max(recorded)) %>%
    ungroup() %>%
    pull(filename)

  big_df <- map_dfr(
    latest_files,
    function(f) {
      # stop readr from thinking commas = thousand separators,
      # and from inferring column types incorrectly
      read_csv(file.path(directory, f), locale = locale(grouping_mark = ""),
               col_types = cols(
                 .default = col_character()))
    }
  )
  return(big_df)
}

## Helper function to extract dates from each file's filename.
get_file_properties <- function(filename) {
  short <- strsplit(filename, ".", fixed = TRUE)[[1]][1]
  parts <- strsplit(short, "_", fixed = TRUE)[[1]]

  filedate <- as.Date(paste(parts[3:5], collapse = "-"))
  recordeddate <- as.Date(paste(parts[7:9], collapse = "-"))

  return(data.frame(filename = filename,
                    date = filedate,
                    recorded = recordeddate))
}

args <- commandArgs(TRUE)

if (length(args) < 3) {
  stop("Usage: Rscript monthly-files.R YYYY NN path/to/individual/files/")
}

year <- args[1]
month <- sprintf("%02d", args[2]) # 0-padded 2-digit month
path <- args[3]

pattern <- paste0("^cvid_responses_", year, "_", month, "_.*.csv.gz$")

df <- get_survey_df(path, pattern)

cat(format_csv(df))
