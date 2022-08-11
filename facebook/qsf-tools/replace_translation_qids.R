#!/usr/bin/env Rscript

## In translation CSVs, replace the QID in the name column with the human-readable
## item name (e.g. A1). Export modified translation CSVs in the same format as the
## original.
##
## Usage:
##
## Rscript replace_translation_qids.R path/to/translation/directory/or/single/translation/CSV path/to/codebook

suppressPackageStartupMessages({
  library(tidyverse)
  library(purrr)
  library(stringr)
  source("qsf-utils.R")
})


replace_qid_wrapper <- function(path_to_translations, path_to_codebook) {
  if (dir.exists(path_to_translations)) {
    # Process all CSVs in directory
    csvs <- list.files(path_to_translations, pattern = "*.csv$", full.names = TRUE)
    for (csv in csvs) {
      replace_qids(csv, path_to_codebook)
    }
  } else if (file.exists(path_to_translations)) {
    replace_qids(path_to_translations, path_to_codebook)
  } else {
    stop(path_to_translations, " is not a valid file or directory")
  }
}

replace_qids <- function(path_to_translation_file, path_to_codebook) {
  wave <- get_wave_from_csv(path_to_translation_file)
  # Load codebook
  codebook <- read_csv(path_to_codebook, col_types = cols(
    .default = col_character(),
    version = col_double()
  )) %>%
    filter(!is.na(qid), version == wave)

  # Load translation file
  translation <- read_csv(path_to_translation_file, show_col_types = FALSE) %>% 
    # Drop survey ID line
    filter(!startsWith(PhraseID, "SV_"))  

  # Use codebook to make a mapping of QID -> item name.
  var_qid_pairs <- codebook %>% mutate(variable = coalesce(originating_item_name, variable)) %>% distinct(qid, variable)
  qid_item_map <- var_qid_pairs %>% pull(variable)
  names(qid_item_map) <- var_qid_pairs %>% pull(qid)
  
  # Use QID-name mapping to replace QID in first column.
  ii_qid <- startsWith(translation$PhraseID, "QID")
  translation[ii_qid,] <- translation[ii_qid,] %>% mutate(
    PhraseID = str_replace(PhraseID, "(^QID[0-9]*)_", function(match) {
      paste0(qid_item_map[str_sub(match, 1, -2)], "_")
    })
  ) 
  
  # Save processed file back to CSV under the same name.
  write_excel_csv(translation, path_to_translation_file, quote = "needed")
}

args <- commandArgs(TRUE)

if (!(length(args) %in% c(2))) {
  stop("Usage: Rscript replace_translation_qids.R path/to/translation/directory/or/single/translation/CSV path/to/codebook")
}

path_to_translations <- args[1]
path_to_codebook <- args[2]

invisible(replace_qid_wrapper(path_to_translations, path_to_codebook))
