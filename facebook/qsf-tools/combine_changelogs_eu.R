#!/usr/bin/env Rscript

## Combine an EU and a non-EU changelog (UMD only), adding in a column indicating
## whether a given field was included in just the EU version, just the non-EU
## version, or in both.
##
## Usage:
##
## Rscript combine_changelogs_eu.R path/to/eu/changelog path/to/noneu/changelog path/to/combined/changelog

suppressPackageStartupMessages({
  library(tidyverse)
})


combine_changelogs <- function(path_to_changelog_eu,
                              path_to_changelog_noneu) {
  
  changelog_eu <- read_csv(path_to_changelog_eu, col_types = cols(
    .default = col_character(),
    new_version = col_double(),
    old_version = col_double()
  )) %>%
  mutate(
    eu_noneu = "EU"
  )
  
  changelog_noneu <- read_csv(path_to_changelog_noneu, col_types = cols(
    .default = col_character(),
    new_version = col_double(),
    old_version = col_double()
  )) %>%
    mutate(
      eu_noneu = "Non-EU"
    )

  # Using rbind here to raise an error if columns differ between the existing
  # changelog and the new wave data.
  changelog_with_duplicates <- rbind(changelog_eu, changelog_noneu)
  
  count_duplicated_rows <- changelog_with_duplicates %>% group_by(across(c(-eu_noneu))) %>% summarize(count = n())
  changelog <- changelog_with_duplicates %>% left_join(count_duplicated_rows)
  changelog$eu_noneu[changelog$count == 2] <- "Both"

  # Sort so that items with missing type (non-Qualtrics fields) are at the top.
  # Drop duplicates.
  changelog <- changelog %>%
    arrange(variable_name, eu_noneu) %>% 
    select(-count) %>% 
    distinct()

  return(changelog)
}

combine_changelog_main <- function(path_to_changelog_eu, path_to_changelog_noneu,path_to_combined) {
  changelog <- combine_changelogs(path_to_changelog_eu, path_to_changelog_noneu)
  write_excel_csv(changelog, path_to_combined, quote="needed")
}


args <- commandArgs(TRUE)

if (length(args) != 3) {
  stop("Usage: Rscript combine_changelogs_eu.R path/to/eu/changelog path/to/noneu/changelog path/to/combined/changelog")
}

path_to_changelog_eu <- args[1]
path_to_changelog_noneu <- args[2]
path_to_combined <- args[3]

invisible(combine_changelog_main(path_to_changelog_eu, path_to_changelog_noneu, path_to_combined))