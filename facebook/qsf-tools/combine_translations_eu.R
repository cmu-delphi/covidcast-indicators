#!/usr/bin/env Rscript

## Combine a set of EU and non-EU translation files (UMD only), adding in a
## column indicating whether a given field was included in just the EU version,
## just the non-EU version, or in both.
##
## Usage:
##
## Rscript combine_translations_eu.R path/to/eu/translations/dir path/to/noneu/translations/dir path/to/combined/translations/dir

suppressPackageStartupMessages({
  library(tidyverse)
  source("qsf-utils.R")
})


combine_translation_pair <- function(eu_translation,
                                     noneu_translation) {
  translation <- bind_rows(eu_translation, noneu_translation) %>%
    mutate(eu_noneu = case_when(
      startsWith(PhraseID, "intro1_eu") ~ "EU",
      startsWith(PhraseID, "intro2_eu") ~ "EU",
      startsWith(PhraseID, "intro1_noneu") ~ "nonEU",
      startsWith(PhraseID, "intro2_noneu") ~ "nonEU",
      TRUE ~ "Both"
    ))
  return(translation)
}

combine_translations <- function(path_to_eu_translations,
                                 path_to_noneu_translations,
                                 path_to_combined) {
  eu_name_pattern <- "_eu_"
  if (!grepl(eu_name_pattern, path_to_eu_translations)) {
    stop(path_to_eu_translations, "does not specify that it is for the EU")
  }
  noneu_name_pattern <- "_noneu_"
  if (!grepl(noneu_name_pattern, path_to_noneu_translations)) {
    stop(path_to_noneu_translations, "does not specify that it is for the non-EU")
  }
  
  eu_files <- list.files(path_to_eu_translations, pattern = "*.csv$", full.names = TRUE)
  eu_translations <- list()
  for (filename in eu_files) {
    eu_translations[[as.character(get_wave_from_csv(filename))]] <- read_csv(filename, show_col_types = FALSE) %>% 
      filter(startsWith(PhraseID, "intro"))
  }
  
  noneu_files <- list.files(path_to_noneu_translations, pattern = "*.csv$", full.names = TRUE)
  noneu_translations <- list()
  for (filename in noneu_files) {
    noneu_translations[[as.character(get_wave_from_csv(filename))]] <- read_csv(filename, show_col_types = FALSE) %>% 
      # Drop response options for the country + region question, they take up way too much space.
      filter(
        !startsWith(PhraseID, "A2_3_Answer"),
        !startsWith(PhraseID, "A2_2_Answer"),
        !startsWith(PhraseID, "NA_")
      )
  }
  
  if (!identical(sort(names(eu_translations)), sort(names(noneu_translations)))) {
    stop("not all waves are available for both EU and non-EU")
  }
  
  dir.create(path_to_combined, showWarnings = FALSE)
  for (wave in names(eu_translations)) {
    combined <- combine_translation_pair(
      eu_translations[[wave]],
      noneu_translations[[wave]]
    )

    write_excel_csv(
      combined,
      file.path(
        path_to_combined,
        sprintf("umd_ctis_combined_v%02g_translations.csv", as.numeric(wave))
      ),
      quote = "needed")
  }
}


args <- commandArgs(TRUE)

if (length(args) != 3) {
  stop("Usage: Rscript combine_translations_eu.R path/to/eu/translations/dir path/to/noneu/translations/dir path/to/combined/translations/dir")
}

path_to_eu_translations <- args[1]
path_to_noneu_translations <- args[2]
path_to_combined <- args[3]

invisible(combine_translations(path_to_eu_translations, path_to_noneu_translations, path_to_combined))


