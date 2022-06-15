#!/usr/bin/env Rscript

## Combine an EU and a non-EU codebook (UMD only), adding in a column indicating
## whether a given field was included in just the EU version, just the non-EU
## version, or in both.
##
## Usage:
##
## Rscript append-old-changelog-umd.R path/to/output/changelog path/to/old/changelog


suppressPackageStartupMessages({
  library(tidyverse)
})

add_rationales_from_old_changelog <- function(path_to_changelog, path_to_old_changelog) {
  # If path_to_old_changelog is provided, prefer it over existing notes column.

    changelog <- read_csv(path_to_changelog, col_types = cols(
        .default = col_character(),
        new_version = col_double(),
        old_version = col_double()
    )) 
    old_changelog <- read_csv(path_to_old_changelog, col_types = cols(
        .default = col_character(),
        new_version = col_double(),
        old_version = col_double()
    )) %>%
        select(new_version, old_version, variable_name, change_type,eu_version, notes)
    changelog <- changelog %>%
        select(-notes) %>%
        left_join(old_changelog, by=c("new_version", "old_version", "variable_name", "change_type","eu_version"))


    write_excel_csv(changelog, path_to_changelog, quote="needed")
}

args <- commandArgs(TRUE)

if (!(length(args) %in% c(2))) {
  stop("Usage: Rscript append-old-changelog-umd.R path/to/output/changelog path/to/old/changelog")
}

path_to_changelog <- args[1]
path_to_old_changelog <- args[2]
add_rationales_from_old_changelog(path_to_changelog, path_to_old_changelog)