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
  
  # If variable_name from the annotated_diff is not also listed as a variable in
  # the codebook, try adding an underscore to the end and looking for a variable
  # in the codebook that starts with that string. The matrix_subquestion_text
  # field of the match should be populated, although we want to ignore it and
  # fill with NA instead.
  vars_not_in_codebook <- setdiff(
    annotated_diff %>% distinct(variable_name) %>% pull(),
    codebook %>% distinct(variable) %>% pull()
  )
  # Add an underscore to the unmatched variable names to create a regex pattern
  matrix_prefixes <- paste0(vars_not_in_codebook, "_")
  names(matrix_prefixes) <- vars_not_in_codebook
  
  # First matrix item match by wave and matrix base question.
  map_matrix_prefix_to_first_match <- codebook %>% 
    mutate(
      join_variable = case_when(
        # Create the basename for matrix items.
        grepl("_", variable) ~ strsplit(variable, "_") %>% purrr::map_chr(~ .x[1]) %>% paste0("_"),
        TRUE ~ variable
      )
    ) %>%
    filter(join_variable %in% matrix_prefixes) %>%
    group_by(wave, join_variable) %>%
    slice_head() %>%
    select(wave, variable, join_variable)
  
  # Add the regex patterns onto the diff.
  annotated_diff <- annotated_diff %>%
    mutate(
      join_variable = case_when(
        variable_name %in% vars_not_in_codebook ~ matrix_prefixes[variable_name],
        TRUE ~ variable_name
      )
    ) %>%
    left_join(
      map_matrix_prefix_to_first_match %>% rename_with(function(column_names) {
        paste("new", column_names, sep = "_")
      }),
      by=c("new_wave" = "new_wave", "join_variable"="new_join_variable")
    ) %>%
    left_join(
      map_matrix_prefix_to_first_match %>% rename_with(function(column_names) {
        paste("old", column_names, sep = "_")
      }),
      by=c("old_wave" = "old_wave", "join_variable"="old_join_variable")
    ) %>%
    rename(
      join_variable_new_wave = new_variable,
      join_variable_old_wave = old_variable
    ) %>% 
    mutate(
      join_variable_new_wave = coalesce(join_variable_new_wave, variable_name),
      join_variable_old_wave = coalesce(join_variable_old_wave, variable_name)
    ) %>%
    select(-join_variable)

  # Create changelog by joining codebook onto annotated diff.
  changelog <- annotated_diff %>%
    # Add info about new version of question
    left_join(
      codebook %>% rename_with(function(column_names) {
        paste("new", column_names, sep = "_")
      }),
      by=c("new_wave" = "new_wave", "join_variable_new_wave" = "new_variable")
    ) %>%
    # Add info about previous version of question
    left_join(
      codebook %>% rename_with(function(column_names) {
        paste("old", column_names, sep = "_")
      }),
      by=c("old_wave" = "old_wave", "join_variable_old_wave" = "old_variable")
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
    ) %>%
    # If item is a matrix question where something other than the matrix
    # subquestions changed between waves, drop matrix_subquestion_text fields,
    # which are relevant for only a single subquestion.
    mutate(
      new_matrix_subquestion_text = case_when(
        variable_name %in% vars_not_in_codebook ~ NA_character_,
        TRUE ~ new_matrix_subquestion_text
      ),
      old_matrix_subquestion_text = case_when(
        variable_name %in% vars_not_in_codebook ~ NA_character_,
        TRUE ~ old_matrix_subquestion_text
      )
    )
  
  write_excel_csv(changelog, path_to_changelog, quote="needed")
}


# args <- commandArgs(TRUE)
# 
# if (length(args) != 4) {
#   stop("Usage: Rscript generate-changelog.R [UMD/CMU] path/to/codebook path/to/annotated/diff path/to/changelog")
# }
# 
# survey_version <- args[1]
# path_to_codebook <- args[2]
# path_to_diff <- args[3]
# path_to_changelog <- args[4]
path_to_codebook <- "codebook.csv"
path_to_diff <- "diff_10-11.csv"
path_to_changelog <- "changelog_test_10v11.csv"
survey_version <- "CMU"

invisible(generate_changelog(path_to_codebook, path_to_diff, path_to_changelog, survey_version))
