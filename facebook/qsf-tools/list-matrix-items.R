#!/usr/bin/env Rscript

## Print a list of survey questions that we handle as matrices.
##
## Usage:
##
## Rscript list-matrix-items.R [UMD/CMU] path/to/qsf

suppressPackageStartupMessages({
  library(jsonlite)
  library(tidyverse)
  source("qsf-utils.R")
})


print_matrix_items <- function(path_to_qsf, survey_version=c("CMU", "UMD")) {
  survey_version <- match.arg(survey_version)
  q <- read_json(path_to_qsf)
  wave <- get_wave(path_to_qsf)
  
  displayed_questions <- subset_qsf_to_displayed(q)
  
  # Get survey item names
  item_names <- displayed_questions %>% 
    map_chr(~ .x$Payload$DataExportTag) %>%
    patch_item_names(survey_version, wave)
  
  # Get survey item formats
  qtype <- get_question_formats(displayed_questions, item_names, survey_version)
  
  qdf <- tibble(variable = item_names,
                type = qtype)
  
  matrix_items <- qdf %>% filter(type == "Matrix") %>% pull(variable)
  message("Wave ", wave, " has ", length(matrix_items), " matrix items: ", paste(matrix_items, collapse=", "))
  
  return(NULL)
}

args <- commandArgs(TRUE)

if (length(args) != 2) {
  stop("Usage: Rscript list-matrix-items.R [UMD/CMU] path/to/qsf")
}

survey_version <- args[1]
path_to_qsf <- args[2]

invisible(print_matrix_items(path_to_qsf, survey_version))
