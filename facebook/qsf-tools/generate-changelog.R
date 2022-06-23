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
  matrix_subquestion = "Matrix subquestion text changed",
  response_option_randomization = "Answer choice order changed",
  respondent_group = "Respondent group changed"
)


generate_changelog <- function(path_to_codebook,
                               path_to_changelog,
                               path_to_old_changelog,
                               survey_version) {
  # Get the codebook. Contains details about each question (text, answer
  # choices, display logic) by wave.
  codebook_raw <- read_csv(path_to_codebook, col_types = cols(
    .default = col_character(),
    version = col_double()
  ))
  
  local_compare_map <- WAVE_COMPARE_MAP[[survey_version]]
  # Add new-old wave mapping columns. Drop unused rows.
  codebook <- codebook_raw %>%
    mutate(
      old_version = version,
      new_version = local_compare_map[as.character(version)]
    )
  
  codebook <- full_join(
    # with old columns
    codebook %>%
      rename_with(function(cols) {
        map_chr(cols, ~ rename_col(.x, "old"))
      }) %>%
      select(-replaces) %>% 
      mutate(x_exists = TRUE),
    # with new columns
    codebook %>%
      rename_with(function(cols) {
        map_chr(cols, ~ rename_col(.x, "new"))
      }) %>%
      select(-replaces, -old_version, -new_version) %>% 
      mutate(y_exists = TRUE),
    by = c("new_version" = "version", "variable" = "variable")
  ) %>%
    mutate(
      description = coalesce(description.x, description.y),
      question_type = coalesce(question_type.x, question_type.y),
    ) %>% 
    select(-version, -description.x, -description.y, -question_type.x, -question_type.y) %>%
    rename(variable_name = variable)
  
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
  
  # Fill in version where missing
  codebook$new_version <- coalesce(codebook$new_version, map_dbl(codebook$old_version, ~ local_compare_map[as.character(.x)]))
  codebook$old_version <- coalesce(codebook$old_version, map_dbl(codebook$new_version, ~ get_old_version(.x, local_compare_map) %>% as.double()))
  
  # Drop obs where version is not in names or values of the wave mapping (i.e. 12.5)
  codebook <- codebook %>%
    filter(
      new_version %in% c(local_compare_map, names(local_compare_map)),
      old_version %in% c(local_compare_map, names(local_compare_map))
    )
  
  ## Find differences.
  result <- list()
  
  # Any item where x (old fields) does not exist but y does has been "added"
  added_items <- codebook %>%
    filter(
      is.na(x_exists) & y_exists
    )
  codebook <- anti_join(codebook, added_items)
  
  # Process added items
  added_items <- added_items %>%
    mutate(
      change_type = CHANGE_TYPE_MAP["added"]
    ) %>% 
    select(-x_exists, -y_exists)
  
  combos <- added_items %>%
    filter(question_type == "Matrix" | !is.na(new_matrix_base_name) | !is.na(new_matrix_subquestion)) %>%
    distinct(old_version, new_matrix_base_name)
  
  for (i in seq_len(nrow(combos))) {
    wave = combos[i,] %>% pull(old_version)
    base_name = combos[i,] %>% pull(new_matrix_base_name)
    tmp <- added_items %>%
      filter(
        old_version == wave, new_matrix_base_name == base_name
      )
    added_items <- anti_join(added_items, tmp)
    if (nrow(filter(codebook_raw, version == wave, matrix_base_name == base_name)) == 0) {
      # Dedup subqs so only report base question once
      tmp <- tmp %>%
        group_by(old_matrix_base_name, new_matrix_base_name, new_version, old_version) %>%
        mutate(
          variable_name = new_matrix_base_name,
          old_matrix_subquestion = NA,
          new_matrix_subquestion = NA,
          old_response_options = case_when(
            length(unique(old_response_options)) == 1 ~ old_response_options,
            TRUE ~ NA
          ),
          new_response_options = case_when(
            length(unique(new_response_options)) == 1 ~ new_response_options,
            TRUE ~ NA
          )
        ) %>%
        slice_head() %>%
        ungroup()
    } else {
      tmp <- mutate(tmp, change_type = "Matrix subquestion added to existing item")
    }
    added_items <- rbind(added_items, tmp)
  }

  result[["added"]] <- added_items
  
  # Any item where x (old fields) exists but y does not has been "removed"
  removed_items <- codebook %>%
    filter(
      x_exists & is.na(y_exists)
    )
  codebook <- anti_join(codebook, removed_items) %>% 
    select(-x_exists, -y_exists)
  
  # Process removed items.
  removed_items <- removed_items %>%
    mutate(
      change_type = CHANGE_TYPE_MAP["removed"]
    ) %>% 
    select(-x_exists, -y_exists)
  
  combos <- removed_items %>%
    filter(question_type == "Matrix" | !is.na(old_matrix_base_name) | !is.na(old_matrix_subquestion)) %>%
    distinct(new_version, old_matrix_base_name)
  
  for (i in seq_len(nrow(combos))) {
    wave = combos[i,] %>% pull(new_version)
    base_name = combos[i,] %>% pull(old_matrix_base_name)
    tmp <- removed_items %>%
      filter(
        new_version == wave, old_matrix_base_name == base_name
      )
    removed_items <- anti_join(removed_items, tmp)
    if (nrow(filter(codebook_raw, version == wave, matrix_base_name == base_name)) == 0) {
      # Dedup subqs so only report base question once
      tmp <- tmp %>%
        group_by(old_matrix_base_name, new_matrix_base_name, new_version, old_version) %>%
        mutate(
          variable_name = old_matrix_base_name,
          old_matrix_subquestion = NA,
          new_matrix_subquestion = NA,
          old_response_options = case_when(
            length(unique(old_response_options)) == 1 ~ old_response_options,
            TRUE ~ NA
          ),
          new_response_options = case_when(
            length(unique(new_response_options)) == 1 ~ new_response_options,
            TRUE ~ NA
          )
        ) %>%
        slice_head() %>%
        ungroup()
    } else {
      tmp <- mutate(tmp, change_type = "Matrix subquestion removed from existing item")
    }
    removed_items <- rbind(removed_items, tmp)
  }
  
  result[["removed"]] <- removed_items
  
  
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
  # Group by matrix_base_name, change_type, and wave, as long as the change_type is relevant and matrix_base_name is not NA.
  # Keep only one obs for each group.
  # Set var name in kept obs to matrix_base_name for generality and to be able to join rationales on.
  combos <- changelog %>%
    filter((question_type == "Matrix" | !is.na(old_matrix_base_name) | !is.na(old_matrix_subquestion)) & 
             change_type %in% c(
               "Question wording changed",
               "Display logic changed",
               "Answer choices changed", ## TODO: needs special logic, because Matrix subquestions can actually have different answer choices. Not needed for UMD
               "Answer choice order changed", ## TODO: needs special logic, because Matrix subquestions can actually have different answer choices. Not needed for UMD
               "Respondent group changed"
             )
    ) %>%
    distinct(new_version, old_version, new_matrix_base_name, old_matrix_base_name, change_type)
  
  SPECIAL_HANDLING <- list(
    "Answer choices changed" = list("new_response_options", "old_response_options"),
    "Answer choices order changed" = list("new_response_option_randomization", "old_response_option_randomization")
  )
  for (i in seq_len(nrow(combos))) {
    new_v <- combos[i,] %>% pull(new_version)
    old_v <- combos[i,] %>% pull(old_version)
    new_base <- combos[i,] %>% pull(new_matrix_base_name)
    old_base <- combos[i,] %>% pull(old_matrix_base_name)
    change <- combos[i,] %>% pull(change_type)
    
    tmp <- changelog %>%
      filter(
        new_version == new_v,
        old_version == old_v,
        new_matrix_base_name == new_base,
        old_matrix_base_name == old_base,
        change_type == change
      )
    changelog <- anti_join(changelog, tmp)
    
    combine_flag <- FALSE
    if (change %in% names(SPECIAL_HANDLING)) {
      # See if the changed column is the same for all obs. Check if all matrix
      # subquestions are listed.
      new_col <- SPECIAL_HANDLING[[change]][[1]]
      old_col <- SPECIAL_HANDLING[[change]][[2]]
      if (
        length(unique(tmp[[new_col]])) == 1 &&
        length(unique(tmp[[old_col]])) == 1 &&
        (
          nrow(tmp) == codebook_raw %>% filter(version == old_v, matrix_base_name == old_base) %>% nrow() ||
          nrow(tmp) == codebook_raw %>% filter(version == new_v, matrix_base_name == new_base) %>% nrow()
        )
      ) {
        combine_flag <- TRUE   
      }
    } else {
      combine_flag <- TRUE  
    }
    
    if (combine_flag) {
      tmp <- tmp %>% 
        slice_head() %>% 
        mutate(
          variable_name = case_when(
            old_matrix_base_name != new_matrix_base_name ~ paste(old_matrix_base_name, new_matrix_base_name, sep="/"),
            TRUE ~ old_matrix_base_name
          ),
          old_matrix_subquestion = NA,
          new_matrix_subquestion = NA
        )
    }
    
    changelog <- rbind(changelog, tmp)
  }
  
  ## Join old rationales on.
  # TODO: The first time this happens using this new script, need to manually map
  # some rationales for "Matrix subquestions changed", since previously this tag
  # would include added and removed subquestions.
  if (is.null(path_to_old_changelog)) {
    warning("rationales will be empty; an old version of the changelog was not provided")
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
        new_matrix_base_name,
        new_question_text,
        new_matrix_subquestion_text,
        new_response_options,
        new_display_logic,
        new_response_option_randomization,
        new_respondent_group,
        old_matrix_base_name,
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
  if (col %in% c(DIFF_COLS, "matrix_base_name")) {
    paste(prefix, col, sep = "_") 
  } else {
    col
  }
}

find_col_differences <- function(codebook, new_col, old_col) {
  codebook[[old_col]] != codebook[[new_col]]
}

get_old_version <- function(new_version, compare_map) {
  ifelse(new_version %in% compare_map, compare_map[compare_map == new_version] %>% names(), NA_character_)
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

