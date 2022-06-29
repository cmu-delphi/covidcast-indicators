#!/usr/bin/env Rscript

## Modify monthly microdata. Add state column. Rename `wave` field to `version`.
##
## Usage:
##
## Rscript microdata_add_state_col__rename_wave.R path/to/individual/files/ path/to/output/dir/ [/path/to/static/dir/]
##
## Writes the processed files to the specified directory under the original file name.

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(rlang)
  library(stringi)
  library(delphiFacebook)
})

amend_microdata <- function(input_dir, output_dir, static_dir, pattern = ".*[.]csv[.]gz$") {
  # Create mapping of county FIPS codes to state postal codes.
  state_county_map <- read_csv(
    file.path(static_dir, "02_20_uszips.csv"),
    col_types = cols(.default = "c")
  ) %>%
    mutate(
      fips = stri_pad(.data$fips, 5, pad="0")
    ) %>%
    select(fips, state = .data$state_id) %>%
    distinct()

  # Read in each monthly file from the microdata directory.
  for (fname in list.files(input_dir, pattern = pattern)) {
    # Read in file and rename `wave` field.
    # stop readr from thinking commas = thousand separators,
    # and from inferring column types incorrectly
    message("reading data in")
    data <- read_csv(file.path(input_dir, fname), locale = locale(grouping_mark = ""),
             col_types = cols(
               .default = col_character())) %>%
      rename(version = .data$wave)

    # Add state column based on county FIPS code.
    data <- left_join(data, state_county_map, by="fips")

    assert(is.na(data$fips) == is.na(data$state))

    # Drop any territories.
    data <- filter(data, !(state %in% c("AS", "GU", "PR", "VI", "MP")))

    # Save file under original name but in output directory.
    message("writing data for ", fname)
    write_csv(data, file.path(output_dir, fname))
  }
}

args <- commandArgs(TRUE)

if (!(length(args) %in% c(2, 3))) {
  stop("Usage: Rscript microdata_add_state_col__rename_wave.R path/to/individual/files/ path/to/output/dir/ [/path/to/static/dir/]")
}

input_dir <- args[1]
output_dir <- args[2]

if (length(args) == 3) {
  static_dir <- args[3]
} else {
  static_dir <- "static"
}

# Specifies monthly microdata rollup naming scheme like "YYYY-MM.csv.gz" and the
# race-ethnicity version "YYYY-MM-race-ethnicity.csv.gz"
pattern <- "^202[0-9]-[0-9]{2}(-race-ethnicity)?[.]csv[.]gz$"

amend_microdata(input_dir, output_dir, static_dir, pattern = pattern)
