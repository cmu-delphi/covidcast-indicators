#!/usr/bin/env Rscript

## Combine the codebook and one or more diffs into a single file showing and
## rationalizing changes between waves. The diffs can be annotated, containing a
## `notes` column with rationales for the changes, or the rationales from a
## previous changelog version can be used.
##
## Usage:
##
## Rscript generate-changelog-with-codebook.R UMD|CMU path/to/codebook path/to/output/changelog [path/to/old/changelog]

suppressPackageStartupMessages({
  library(tidyverse)
})

# "old" = new
WAVE_COMPARE_MAP <- list(
  "UMD" = c(
    "1" = 2,
    "2" = 3,
    "3" = 4,
    "4" = 5,
    "5" = 6,
    "6" = 7,
    "7" = 8,
    "8" = 9,
    "9" = 10,
    "10" = 11,
    "11" = 12,
    "12" = 13
  ),
  "CMU" = c(
    "1" = 2,
    "2" = 3,
    "3" = 4,
    "4" = 5,
    "5" = 6,
    "6" = 7,
    "7" = 8,
    "8" = 10,
    "10" = 11,
    "11" = 12,
    "12" = 13
  )
)

DIFF_COLS <- c(
  "question",
  "matrix_subquestion",
  "response_options",
  "display_logic",
  "response_option_randomization",
  "respondent_group"
)

CHANGE_TYPE_MAP <- c(
  added = "Item added",
  removed = "Item removed",
  question = "Question wording changed",
  display_logic = "Display logic changed",
  response_options = "Answer choices changed",
  matrix_subquestion = "Matrix subquestions changed",
  response_option_randomization = "Answer choice order changed",
  respondent_group = "Respondent group changed"
)


generate_changelog <- function(path_to_codebook,
                               path_to_changelog,
                               path_to_old_changelog,
                               survey_version) {
  # Get the codebook. Contains details about each question (text, answer
  # choices, display logic) by wave.
  codebook <- codebook <- read_csv(path_to_codebook, col_types = cols(
    .default = col_character(),
    version = col_double()
  ))
  
  local_compare_map <- WAVE_COMPARE_MAP[[survey_version]]
  # Add new-old wave mapping columns. Drop unused rows.
  codebook <- codebook %>%
    mutate(
      old_version = version,
      new_version = local_compare_map[as.character(version)]
    ) %>%
    # If new_version is missing, the survey wavey of that obs doesn't have an
    # new wave to compare against.
    filter(
      !is.na(new_version)
    )
  
  codebook <- full_join(
    # with old columns
    codebook %>%
      rename_with(function(cols) {
        map_chr(cols, ~ rename_col(.x, "old"))
      }) %>%
      select(-replaces),
    # with new columns
    codebook %>%
      rename_with(function(cols) {
        map_chr(cols, ~ rename_col(.x, "new"))
      }) %>%
      select(-description, -question_type, -replaces, -old_version, -new_version),
    by = c("new_version" = "version", "variable" = "variable")
  ) %>%
    select(-version) %>%
    rename(variable_name = variable)
  
  ## Find differences.
  result <- list()
  
  # Drop obs where both old and new info is missing -- these are metavariables
  # that we report in the microdata, like "weight" and "StartDate"
  codebook <- codebook %>%
    filter(
      !(is.na(old_question) & 
          is.na(old_display_logic) & 
          is.na(old_response_option_randomization) & 
          is.na(old_respondent_group) & 
          is.na(new_question) & 
          is.na(new_display_logic) & 
          is.na(new_response_option_randomization) & 
          is.na(new_respondent_group))
    )
  
  # Any item with missing "old_*" fields has been added.
  result[["added"]] <- codebook %>%
    filter(
      is.na(old_question),
      is.na(old_display_logic),
      is.na(old_response_option_randomization),
      is.na(old_respondent_group),
      !is.na(new_question),
      !is.na(new_display_logic),
      !is.na(new_response_option_randomization),
      !is.na(new_respondent_group)
    ) %>%
    mutate(
      change_type = CHANGE_TYPE_MAP["added"],
      old_version = map_chr(
        new_version,
        ~ ifelse(
          length(names(local_compare_map[local_compare_map == .x])) == 0,
          NA_character_,
          names(local_compare_map[local_compare_map == .x])
        )
      ) %>% as.integer()
    ) %>% 
    filter(!is.na(old_version))
  codebook <- codebook %>%
    filter(
      !(is.na(old_question) & 
          is.na(old_display_logic) & 
          is.na(old_response_option_randomization) & 
          is.na(old_respondent_group) & 
          !is.na(new_question) & 
          !is.na(new_display_logic) & 
          !is.na(new_response_option_randomization) & 
          !is.na(new_respondent_group))
    )

  # Any item with missing "new_*" fields has been removed.
  result[["removed"]] <- codebook %>%
    filter(
      !is.na(old_question),
      !is.na(old_display_logic),
      !is.na(old_response_option_randomization),
      !is.na(old_respondent_group),
      is.na(new_question),
      is.na(new_display_logic),
      is.na(new_response_option_randomization),
      is.na(new_respondent_group)
    ) %>%
    mutate(
      change_type = CHANGE_TYPE_MAP["removed"]
    )
  codebook <- codebook %>%
    filter(
      !(!is.na(old_question) & 
          !is.na(old_display_logic) & 
          !is.na(old_response_option_randomization) & 
          !is.na(old_respondent_group) & 
          is.na(new_question) & 
          is.na(new_display_logic) & 
          is.na(new_response_option_randomization) & 
          is.na(new_respondent_group))
    )
  
  # Do all other comparisons
  for (col in DIFF_COLS) {
    new_col <- paste("new", col, sep="_")
    old_col <- paste("old", col, sep="_")
    items_not_identical <- find_col_differences(codebook, new_col, old_col)
    
    changed <- codebook %>%
      filter(items_not_identical) %>%
      mutate(change_type = CHANGE_TYPE_MAP[col])
    if (col == "question") {
      # Drop obs if the change is due to trivial formatting, e.g. nbsp
      changed <- changed %>%
        mutate(
          new_question_wo_formatting = str_replace_all(new_question, "&nbsp;", " "),
          old_question_wo_formatting = str_replace_all(old_question, "&nbsp;", " ")
        ) %>% 
        filter(new_question_wo_formatting != old_question_wo_formatting) %>% 
        select(-new_question_wo_formatting, -old_question_wo_formatting)
    }
    result[[col]] <- changed
  }
  
  changelog <- bind_rows(result)
  
  ## Don't report all matrix subquestions when the change is shared between all
  ## of them, just report the base item.
  
  # Group by variable_base_name and change_type, as long as change is not "Matrix subquestion changed" and variable_base_name is not NA.
  # Keep only one obs for each group.
  # Set var name in kept obs to variable_base_name for generality and to be able to join rationales on.
  
  ## Join old rationales on.
  # TODO: The first time this happens using this new script, need to manually map
  # some rationales for "Matrix subquestions changed", since previously this tag
  # would include added and removed subquestions.
  if (is.null(path_to_old_changelog)) {
    warning("rationales will be empty unless an old version of the changelog is provided")
    changelog$notes <- NA_character_
  } else {
    old_changelog <- read_csv(path_to_old_changelog, col_types = cols(
      .default = col_character(),
      new_version = col_double(),
      old_version = col_double()
    )) %>% 
      select(new_version, old_version, variable_name, change_type, notes)
    changelog <- changelog %>%
      left_join(old_changelog, by=c("new_version", "old_version", "variable_name", "change_type"))
  }
  
  write_excel_csv(
    changelog  %>% 
      rename(
        new_question_text = new_question,
        old_question_text = old_question,
        new_matrix_subquestion_text = new_matrix_subquestion,
        old_matrix_subquestion_text = old_matrix_subquestion
      ) %>% 
      select(
        new_version,
        old_version,
        variable_name,
        description,
        change_type,
        new_question_text,
        new_matrix_subquestion_text,
        new_response_options,
        new_display_logic,
        new_response_option_randomization,
        new_respondent_group,
        old_question_text,
        old_matrix_subquestion_text,
        old_response_options,
        old_display_logic,
        old_response_option_randomization,
        old_respondent_group,
        notes
      ) %>%
      arrange(new_version, old_version),
    path_to_changelog, quote="needed"
  )
}

rename_col <- function(col, prefix) {
  if (col %in% DIFF_COLS) {
    paste(prefix, col, sep = "_") 
  } else {
    col
  }
}

find_col_differences <- function(codebook, new_col, old_col) {
  codebook[[old_col]] != codebook[[new_col]]
}

args <- commandArgs(TRUE)

if (!(length(args) %in% c(3, 4))) {
  stop("Usage: Rscript generate-changelog-with-codebook.R UMD|CMU path/to/codebook path/to/output/changelog [path/to/old/changelog]")
}

survey_version <- args[1]
path_to_codebook <- args[2]
path_to_changelog <- args[3]

if (length(args) == 4) {
  path_to_old_changelog <- args[4]
} else {
  path_to_old_changelog <- NULL
}

invisible(generate_changelog(path_to_codebook, path_to_changelog, path_to_old_changelog, survey_version))

