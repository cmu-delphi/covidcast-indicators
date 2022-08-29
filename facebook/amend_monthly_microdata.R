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
  library(covidcast)
  library(delphiFacebook)
})

amend_microdata <- function(input_dir, output_dir, static_dir, pattern = ".*[.]csv[.]gz$") {
  # Create mapping of county FIPS codes to state postal codes.
  zips <- read_csv(
    file.path(static_dir, "02_20_uszips.csv"),
    col_types = cols(.default = "c", population = "i")
  ) %>%
    mutate(
      fips = stri_pad(.data$fips, 5, pad="0"),
      zip = stri_pad(.data$zip, 5, pad="0")
    )
  invalid_zips <- zips %>%
    filter(population <= 100) %>%
    pull(zip)
  territory_zips <- zips %>%
    filter(state_id %in% c("AS", "GU", "PR", "VI", "MP")) %>%
    pull(zip)

  # Read in each monthly file from the microdata directory.
  for (fname in list.files(input_dir, pattern = pattern)) {
    # Read in file.
    # stop readr from thinking commas = thousand separators,
    # and from inferring column types incorrectly
    message("reading data in")
    data <- read_csv(file.path(input_dir, fname), locale = locale(grouping_mark = ""),
             col_types = cols(
               .default = col_character())) %>%
      # Rename `wave` field.
      rename(version = .data$wave) %>%
      create_zip5()

    # Add state column based on county FIPS code.
    data <- mutate(data, state = state_fips_to_name(substr(fips, 1, 2)) %>% name_to_abbr())

    assert(
      all(is.na(data$fips) == is.na(data$state)),
      "fips and state fields are not missing in the same places"
    )

    # Drop any territories.
    data <- filter(data,
      !(.data$state %in% c("AS", "GU", "PR", "VI", "MP")),
      # If fips not available and state didn't get filled in.
      !(.data$zip5 %in% territory_zips)
    )

    # what zip5 values have a large enough population (>100) to include in micro
    # output. Those with too small of a population are blanked to NA
    data <- blank_zips(data, invalid_zips, fname)

    # Save file under original name but in output directory.
    message("writing data for ", fname)
    write_csv(data, file.path(output_dir, fname))
  }
}

create_zip5 <- function(data) {
  data$zip5 <- data$A3

  # clean the ZIP data
  data$zip5 <- stri_replace_all(data$zip5, "", regex = " *")
  data$zip5 <- stri_replace(data$zip5, "", regex ="-.*")

  # some people enter 9-digit ZIPs, which could make them easily identifiable in
  # the individual output files. rather than truncating to 5 digits -- which may
  # turn nonsense entered by some respondents into a valid ZIP5 -- we simply
  # replace these ZIPs with NA.
  data$zip5 <- ifelse(nchar(data$zip5) > 5, NA_character_,
                            data$zip5)

  return(data)
}

blank_zips <- function(data, invalid_zips, fname) {
  change_zip <- (data$zip5 %in% invalid_zips)
  # Population-based blanking of zip codes was implemented in late May 2020. For
  # later files, we shouldn't be blanking any new obs.
  if (sum(change_zip) > 0) {
    warning("trying to remove obs with invalid zip via population")
    print(fname)
    print(head(data[change_zip,] %>% select(zip5, fips, state)))
  }
  data$A3[change_zip] <- NA

  data <- select(data, -zip5)

  return(data)
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

