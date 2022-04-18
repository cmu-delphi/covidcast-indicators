#!/usr/bin/env Rscript

## Combine the codebook and one or more diffs into a single file showing and
## rationalizing changes between waves. The diffs can be annotated, containing a
## `notes` column with rationales for the changes, or the rationales from a
## previous changelog version can be used.
##
## Usage:
##
## Rscript generate-changelog.R UMD|CMU path/to/codebook path/to/diff/or/diff/directory path/to/output/changelog [path/to/old/changelog]"

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
  codebook <- get_codebook(path_to_codebook)
  
  # Get the diffs + rationale. Contains info about which items changed between
  # waves, plus a description of what changed and why.
  qsf_diff <- get_diff(path_to_diff)
  
  if (!("notes" %in% names(qsf_diff))) {
    if (is.null(path_to_old_changelog)) {
      stop("rationales must be provided either in the diff or via an old version of the changelog")
    }
    qsf_diff$notes <- NA_character_
  }
  
  qsf_diff <- expand_out_matrix_subquestions(qsf_diff)
  
  # Rename items as necessary
  path_to_rename_map <- localize_static_filepath(rename_map_file, survey_version)
  qsf_diff <- qsf_diff %>%
    rowwise() %>% 
    mutate(
      variable_name = patch_item_names(variable_name, path_to_rename_map, new_wave)
    )
  
  result <- prepare_matrix_base_questions_for_join(qsf_diff, codebook)
  qsf_diff <- result$diff
  vars_not_in_codebook <- result$vars_not_in_codebook

  changelog <- make_changelog_from_codebook_and_diff(qsf_diff, codebook, vars_not_in_codebook)
  changelog <- add_rationales_from_old_changelog(changelog, path_to_old_changelog)
  check_missing_rationales(changelog)
  
  write_excel_csv(changelog, path_to_changelog, quote="needed")
}

# Read codebook from path. Drop fields we don't use in the changelog.
get_codebook <- function(path_to_codebook) {
  codebook <- read_csv(path_to_codebook, col_types = cols(
    .default = col_character(),
    wave = col_double()
  )) %>%
    rename(question_text = question, matrix_subquestion_text = matrix_subquestion) %>%
    select(
      -replaces, -description, -question_type,
      -response_option_randomization, -respondent_group
    )
  
  return(codebook)
}
  
# Try to load `path_to_diff`. Check if it is a single CSV or a directory
# containing a set of CSVs.
get_diff <- function(path_to_diff) {
  if (dir.exists(path_to_diff)) {
    # Load all CSVs from a directory
    csvs <- list.files(path_to_diff, pattern = "*.csv$", full.names = TRUE)
    qsf_diff <- list()
    for (csv in csvs) {
      qsf_diff[[csv]] <- read_csv(csv, col_types = cols(
        .default = col_character(),
        new_wave = col_double(),
        old_wave = col_double()
      ))
    }
    qsf_diff <- purrr::reduce(qsf_diff, rbind) %>%
      rename(variable_name = item) %>%
      select(-contains("qid"))
  } else if (file.exists(path_to_diff)) {
    # Load a single file
    qsf_diff <- read_csv(path_to_diff, col_types = cols(
      .default = col_character(),
      new_wave = col_double(),
      old_wave = col_double()
    )) %>%
      rename(variable_name = item) %>%
      select(-contains("qid"))
  } else {
    stop(path_to_diff, " is not a valid file or directory")
  }
  
  return(qsf_diff)
}

# Turn any item listed as an `impacted_subquestion` into its own observation.
# Other fields are set to be the same as the base question (e.g. the base
# question is E1 for matrix subquestion E1_1).
expand_out_matrix_subquestions <- function(qsf_diff) {
  # The diff only lists base name for matrix questions that changed. For
  # example, `variable_name` is "Z1" if any matrix subquestion ("Z1_1", "Z1_2",
  # etc) changed. The modified subquestions are listed in column
  # `impacted_subquestions`.
  #
  # Since the codebook lists matrix subquestions separately, we need to split up
  # the `impacted_subquestions` such that each subquestion is its own
  # observation. This will allow us to join the codebook onto the diff.
  nonmatrix_changes <- qsf_diff %>%
    filter(is.na(impacted_subquestions)) %>%
    select(-impacted_subquestions)
  # Separately process any obs with non-missing `impacted_subquestions.`
  matrix_changes <- qsf_diff %>%
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
  qsf_diff <- rbind(nonmatrix_changes, matrix_changes) %>%
    arrange(new_wave, old_wave)

  return(qsf_diff)
}

# Matrix base questions (e.g. the base question is E1 for matrix subquestion
# E1_1) exist in diffs but not in the codebook. To be able to join them between
# the two dfs, create a variable name mapping specifically for use in the join
# operation.
#
# A matrix base question is mapped to the first associated subquestion instance
# for a particular wave. The first subquestion is used for convenience and
# reproducibility; subquestion-specific fields are set to `NA`.
prepare_matrix_base_questions_for_join <- function(qsf_diff, codebook) {
  # If variable_name from the qsf_diff is not also listed as a variable in
  # the codebook, try adding an underscore to the end and looking for a variable
  # in the codebook that starts with that string. The matrix_subquestion_text
  # field of the match should be populated, although we want to ignore it and
  # fill with NA instead.
  vars_not_in_codebook <- setdiff(
    qsf_diff %>% distinct(variable_name) %>% pull(),
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
  qsf_diff <- qsf_diff %>%
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
  
  return(list("diff" = qsf_diff, "vars_not_in_codebook" = vars_not_in_codebook))
}

# Join codebook onto diff and modify columns to make the changelog.
make_changelog_from_codebook_and_diff <- function(qsf_diff, codebook, vars_not_in_codebook) {
  # Create changelog by joining codebook onto annotated diff.
  changelog <- qsf_diff %>%
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
  
  return(changelog)
}

# Add old rationales, if available, to new changelog
add_rationales_from_old_changelog <- function(changelog, path_to_old_changelog) {
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
  
  return(changelog)
}

check_missing_rationales <- function(changelog) {
  if (any(is.na(changelog$notes))) {
    vars_missing_rationales <- changelog %>%
      filter(is.na(notes) | notes == "") %>%
      pull(variable_name)
    waves <- changelog %>%
      filter(is.na(notes) | notes == "") %>%
      pull(new_wave)
    change_types <- changelog %>%
      filter(is.na(notes) | notes == "") %>%
      pull(change_type)
    warning(
      "variables ", paste0(vars_missing_rationales, " (new_wave ", waves, ", ", change_types, ")", collapse = ", "),
      " are missing rationales"
    )
  }
  
  return(NULL)
}

args <- commandArgs(TRUE)

if (!(length(args) %in% c(4, 5))) {
  stop("Usage: Rscript generate-changelog.R UMD|CMU path/to/codebook path/to/diff/directory path/to/output/changelog [path/to/old/changelog]")
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
