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
                               path_to_old_changelog,
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
  
  if (!("notes" %in% names(annotated_diff))) {
    if (is.null(path_to_old_changelog)) {
      stop("rationales must be provided either in the diff or via an old version of the changelog")
    }
    annotated_diff$notes <- NA_character_
  }
  
  # The diff only lists base name for matrix questions that changed. For
  # example, `variable_name` is "Z1" if any matrix subquestion ("Z1_1", "Z1_2",
  # etc) changed. The subquestions that changed is noted in another column,
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
  
  # If path_to_old_changelog is provided, prefer it over existing notes column.
  if (!is.null(path_to_old_changelog)) {
    old_changelog <- read_csv(path_to_old_changelog, col_types = cols(
      .default = col_character(),
      new_wave = col_double(),
      old_wave = col_double()
    )) %>% 
      select(new_wave, old_wave, variable_name, change_type, notes)
    changelog <- changelog %>%
      select(-notes) %>%
      left_join(old_changelog, by=c("new_wave", "old_wave", "variable_name", "change_type"))
  }
  
  if (any(is.na(changelog$notes))) {
    vars_missing_rationales <- changelog %>%
      filter(is.na(notes) | notes == "") %>%
      pull(variable_name)
    warning(
      "variables ", paste(vars_missing_rationales, collapse = ", "),
      " are missing rationales in the `notes` column"
    )
  }
  
  write_excel_csv(changelog, path_to_changelog, quote="needed")
}


args <- commandArgs(TRUE)

if (!(length(args) %in% c(4, 5))) {
  stop("Usage: Rscript generate-changelog.R [UMD/CMU] path/to/codebook path/to/annotated/diff path/to/output/changelog [path/to/old/changelog]")
}

survey_version <- args[1]
path_to_codebook <- args[2]
path_to_diff <- args[3]
path_to_changelog <- args[4]

if (length(args) == 5) {
  path_to_old_changelog <- args[5]
} else {
  path_to_old_changelog <- NULL
}

invisible(generate_changelog(path_to_codebook, path_to_diff, path_to_changelog, path_to_old_changelog, survey_version))
