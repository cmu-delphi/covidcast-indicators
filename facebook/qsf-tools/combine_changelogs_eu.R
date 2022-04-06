#!/usr/bin/env Rscript

## Combine an EU and a non-EU changelog (UMD only), adding in a column indicating
## whether a given field was included in just the EU version, just the non-EU
## version, or in both.
##
## Usage:
##
## Rscript combine_codebooks_eu.R.R path/to/eu/codebook path/to/noneu/codebook path/to/combined/codebook

suppressPackageStartupMessages({
  library(tidyverse)
})


combine_codebooks <- function(path_to_codebook_eu,
                              path_to_codebook_noneu) {
  
  codebook_eu <- read_csv(path_to_codebook_eu, col_types = cols(
    .default = col_character(),
    new_wave = col_double(),
    old_wave = col_double()
  )) %>%
  mutate(
    eu_version = "EU"
  )
  
  codebook_noneu <- read_csv(path_to_codebook_noneu, col_types = cols(
    .default = col_character(),
    new_wave = col_double(),
    old_wave = col_double()
  )) %>%
    mutate(
      eu_version = "Non-EU"
    )

  # Using rbind here to raise an error if columns differ between the existing
  # codebook and the new wave data.
  codebook_with_duplicates <- rbind(codebook_eu, codebook_noneu)
  
  count_duplicated_rows <- codebook_with_duplicates %>% group_by(across(c(-eu_version))) %>% summarize(count = n())
  codebook <- codebook_with_duplicates %>% left_join(count_duplicated_rows)
  codebook$eu_version[codebook$count == 2] <- "Both"

  # Sort so that items with missing type (non-Qualtrics fields) are at the top.
  # Drop duplicates.
  codebook <- codebook %>%
    arrange(variable_name, eu_version) %>% 
    select(-count) %>% 
    distinct()

  return(codebook)
}

combine_codebook_main <- function(path_to_codebook_eu, path_to_codebook_noneu,path_to_combined) {
  codebook <- combine_codebooks(path_to_codebook_eu, path_to_codebook_noneu)
  write_excel_csv(codebook, path_to_combined, quote="needed")
}


args <- commandArgs(TRUE)

if (length(args) != 3) {
  stop("Usage: Rscript combine_changelogs_eu.R path/to/eu/codebook path/to/noneu/codebook path/to/combined/codebook")
}

path_to_codebook_eu <- args[1]
path_to_codebook_noneu <- args[2]
path_to_combined <- args[3]

invisible(combine_codebook_main(path_to_codebook_eu, path_to_codebook_noneu, path_to_combined))