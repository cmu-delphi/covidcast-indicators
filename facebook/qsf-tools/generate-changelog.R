#!/usr/bin/env Rscript

## Combine the codebook and annotated diffs into a single file showing and
## rationalizing changes between waves.
##
## Usage:
##
## Rscript generate-changelog.R [UMD/CMU] path/to/codebook path/to/annotated/diff

suppressPackageStartupMessages({
  library(tidyverse)
  library(purrr)
  library(stringr)
  source("qsf-utils.R")
})


generate_changelog <- function(path_to_codebook,
                               path_to_diff,
                               path_to_changelog,
                               survey_version,
                               rename_map_file="item_rename_map.csv") {
  # Get the codebook. Contains details about each question (text, answer
  # choices, display logic) by wave.
  codebook <- read_csv(path_to_codebook, col_types = cols(
    .default = col_character(),
    wave = col_double()
  )) %>%
    rename(question_text = question, matrix_subquestion_text = matrix_subquestion) %>%
    select(
      -replaces, -description, -question_type,
      -response_option_randomization, -respondent_group
    )
  
  # Get the diffs + rationale. Contains info about which items changed between
  # waves, plus a description of what changed and why.
  annotated_diff <- read_csv(path_to_diff, col_types = cols(
    .default = col_character(),
    new_wave = col_double(),
    old_wave = col_double()
  )) %>%
    rename(variable_name = item) %>%
    select(-contains("qid"))
  
  # The diff only lists base name for matrix questions that changed. For
  # example, `variable_name` is "Z1" if any matrix subquestion ("Z1_1", "Z1_2",
  # etc) changed. Which subquestions changed is noted in another column,
  # `impacted_subquestions`.
  #
  # Since the codebook lists matrix subquestions separately, we need to split up
  # the `impacted_subquestions` such that each subquestion is its own
  # observation. This will allow us to join the codebook onto the diff.
  nonmatrix_changes <- annotated_diff %>%
    filter(is.na(impacted_subquestions)) %>%
    select(-impacted_subquestions)
  # Separately process any obs with non-missing `impacted_subquestions.`
  matrix_changes <- annotated_diff %>%
    filter(!is.na(impacted_subquestions)) %>%
    # If multiple matrix subquestions changed, list each separately.
    rowwise() %>%
    mutate(new = list(	
      tibble(
        new_wave = new_wave,
        old_wave = old_wave,
        change_type = change_type,
        variable_name = str_split(impacted_subquestions, ", ") %>% unlist(),   
        notes = notes
      )
    )) %>% 
    select(new) %>% 
    unnest(new)
  
  # Combine matrix and non-matrix subquestions. Use rbind to warn if our columns
  # differ.
  annotated_diff <- rbind(nonmatrix_changes, matrix_changes) %>%
    arrange(new_wave, old_wave)
  
  # Rename items as necessary
  path_to_rename_map <- localize_static_filepath(rename_map_file, survey_version)
  annotated_diff <- annotated_diff %>%
    rowwise() %>% 
    mutate(
      variable_name = patch_item_names(variable_name, path_to_rename_map, new_wave)
    )
  
  changelog <- annotated_diff %>%
    # Add info about new version of question
    left_join(
      codebook %>% rename_with(function(column_names) {
        paste("new", column_names, sep = "_")
      }),
      by=c("new_wave" = "new_wave", "variable_name" = "new_variable")
    ) %>%
    # Add info about previous version of question
    left_join(
      codebook %>% rename_with(function(column_names) {
        paste("old", column_names, sep = "_")
      }),
      by=c("old_wave" = "old_wave", "variable_name" = "old_variable")
    ) %>%
    select(
      new_wave,
      old_wave,
      variable_name,
      change_type,
      new_question_text,
      new_matrix_subquestion_text,
      new_response_options,
      new_display_logic,
      old_question_text,
      old_matrix_subquestion_text,
      old_response_options,
      old_display_logic,
      notes
    )

  write_excel_csv(changelog, path_to_changelog, quote="needed")
}


args <- commandArgs(TRUE)

if (length(args) != 4) {
  stop("Usage: Rscript generate-changelog.R [UMD/CMU] path/to/codebook path/to/annotated/diff path/to/changelog")
}

survey_version <- args[1]
path_to_codebook <- args[2]
path_to_diff <- args[3]
path_to_changelog <- args[4]

invisible(generate_changelog(path_to_codebook, path_to_diff, path_to_changelog, survey_version))
