#!/usr/bin/env Rscript

## Add information to the survey codebook from one .qsf translation file or all
## .qsf files contained in the indicated directory.
##
## Usage:
##
## Rscript generate-codebook.R [UMD/CMU] path/to/qsf/file/or/dir path/to/codebook

suppressPackageStartupMessages({
  library(tidyverse)
  library(jsonlite)
  library(rjson)
  library(stringr)
  library(gsubfn)
  source("qsf-utils.R")
})

process_qsf <- function(path_to_qsf,
                        survey_version,
                        shortname_map_file="item_shortquestion_map.csv",
                        replacement_map_file="item_replacement_map.csv",
                        rename_map_file="item_rename_map.csv", 
                        drop_columns_file="item_drop.csv") {
  # update paths based on survey version
  path_to_shortname_map <- localize_static_filepath(shortname_map_file, survey_version)
  path_to_replacement_map <- localize_static_filepath(replacement_map_file, survey_version)
  path_to_rename_map <- localize_static_filepath(rename_map_file, survey_version)
  path_to_drop_columns <- localize_static_filepath(drop_columns_file, survey_version)
  
  q <- read_json(path_to_qsf)
  wave <- get_wave(path_to_qsf)
  
  displayed_questions <- subset_qsf_to_displayed(q)
  
  # get Qualtrics auto-assigned question IDs
  qids <- displayed_questions %>% 
    map_chr(~ .x$Payload$QuestionID)
  
  # get item names
  item_names <- displayed_questions %>% 
    map_chr(~ .x$Payload$DataExportTag) %>%
    patch_item_names(path_to_rename_map, wave)
  
  if (survey_version == "UMD") {
    item_names[item_names == "D2_30" & qids == "QID294"] <- "D2_30_cheer"
    item_names[item_names == "D2_30" & qids == "QID293"] <- "D2_30_calm"
    item_names[item_names == "B13" & qids == "QID253"] <- "B13_likert"
    item_names[item_names == "B13" & qids == "QID255"] <- "B13_profile"
    item_names[item_names == "B14" & qids == "QID254"] <- "B14_likert"
    item_names[item_names == "B14" & qids == "QID259"] <- "B14_profile"
    item_names[item_names == "B12a" & qids == "QID250"] <- "B12a_likert"
    item_names[item_names == "B12a" & qids == "QID258"] <- "B12a_profile"
    item_names[item_names == "B12b" & qids == "QID251"] <- "B12b_likert"
    item_names[item_names == "B12b" & qids == "QID257"] <- "B12b_profile"
  }

  # get question text:
  questions <- displayed_questions %>% 
    map_chr(~ .x$Payload$QuestionText)
  
  # get question types
  qtype <- get_question_formats(displayed_questions, item_names, survey_version)

  # get the choices (vertical components); subquestions for matrix items and answer choices for non-matrix items.
  choices <- displayed_questions %>% 
    map(~ .x$Payload$Choices) %>% 
    map(~ map(.x, "Display"))
  
  # derive from choices where users can fill in "Other" response with free text
  other_text_option <- displayed_questions %>% 
    map(~ .x$Payload$Choices) %>% 
    map(~ map(.x, "TextEntry")) %>%
    map(~ map(.x, ~ identical(.x, "true")))
  ii_other_text_option <- other_text_option %>%
    map(~ any(.x)) %>% 
    unlist() %>%
    suppressWarnings()
  
  text_elem <- other_text_option[ii_other_text_option] %>%
    map(unlist) %>%
    map(which) %>%
    map(names) %>%
    unlist()
  other_text_items <- paste(item_names[ii_other_text_option], text_elem, "TEXT", sep="_") %>%
    setNames(item_names[ii_other_text_option])
  
  # some questions port the choices from other questions
  ii_carryforward <- displayed_questions %>%
    map(~ .x$Payload$DynamicChoices$Locator) %>%
    map(~ !is.null(.x)) %>% 
    unlist() %>% 
    which()
  carryforward_choices_qid <- displayed_questions[ii_carryforward] %>%
    map_chr(~ .x$Payload$DynamicChoices$Locator) %>% 
    str_split(., "/") %>% 
    map(~ .x[3]) %>% unlist()
  # create recode map for reference and later use
  recode_map <- displayed_questions %>% 	
    map(~ .x$Payload$RecodeValues)
  
  choices[ii_carryforward] <- lapply(	
    seq_along(carryforward_choices_qid),	
    function(ind) {	
      # Get choices that are new for this question	
      old_choices <- choices[qids == qids[ii_carryforward[ind]]] %>% unlist(recursive = FALSE)	
      carryforward_choices <- choices[qids == carryforward_choices_qid[ind]] %>% unlist(recursive = FALSE)
      
      # By default, carried forward choices are coded during the carry as
      # "x<original code>". They are then recoded as pure numeric codes using
      # the `RecodeValues` field. Some carried forward choices do not have
      # `RecodeValues` defined and so in that case we don't want to prepend the
      # codes with "x".
      recode_values <- recode_map[qids == qids[ii_carryforward[ind]]] %>% unlist(recursive = FALSE)	
      
      if (!is.null(recode_values)) {
        names(carryforward_choices) <- paste0("x", names(carryforward_choices))  
      }
      # Combine new choices and carried-forward choices
      c(old_choices, carryforward_choices)	
    }	
  )	
  
  # get the "answers". These are answer choices for matrix items, and missing for non-matrix items.
  matrix_answers <- displayed_questions %>%	
    map(~ .x$Payload$Answers) %>%	
    map(~ map(.x, "Display"))
  # Get index to reattempt any containing only NULLs with a different format.
  ii_nulls <- matrix_answers %>%
    map(function(.x) {
      if (length(.x) == 0) {
        return(FALSE)
      } else {
        all(map_lgl(.x, ~ is.null(.x)))
      }
    }) %>% unlist() %>% which()
  
  # Swap matrix answer choices into `choices` and matrix subquestion text into another variable	
  ii_matrix <- which(qtype == "Matrix")	
  matrix_subquestions <- rep(list(list()), length(choices))	
  matrix_subquestions[ii_matrix] <- choices[ii_matrix]	
  choices[ii_matrix] <- matrix_answers[ii_matrix]

  # Recode response options if overriding Qualtrics auto-assigned coding.	
  ii_recode <- recode_map %>%	
    map(~ !is.null(.x)) %>% 	
    unlist() %>% 	
    which()	
  choices[ii_recode] <- map2(.x=choices[ii_recode], .y=recode_map[ii_recode],	
                             ~ setNames(.x, .y[names(.x)])	
  )	
  
  # Reattempt matrix choices containing only NULLs with a different format _after_ recoding.
  choices[ii_nulls] <- displayed_questions[ii_nulls] %>%	
    map(~ .x$Payload$Answers) %>%	
    map(~ map(.x, ~ map(.x, "Display")))
  
  # Get matrix subquestion field names as reported in microdata. NULL if not	
  # defined (not a Matrix question); FALSE if not set; otherwise a list	
  matrix_subquestion_field_names <- displayed_questions %>%	
    map(~ .x$Payload$ChoiceDataExportTags)
  # When subquestion field names are not set, generate incrementing names
  ii_unset_matrix_subq_names <- (matrix_subquestion_field_names %>%	
    map(~ !inherits(.x, "list")) %>% 	
    unlist() & qtype == "Matrix") %>% 	
    which()
  matrix_subquestion_field_names[ii_unset_matrix_subq_names] <- lapply(ii_unset_matrix_subq_names, function(ind){
    paste(
      item_names[ind],	
      1:length(matrix_subquestions[ind] %>% unlist()),	
      sep = "_"	
    ) %>% list()
  })

  if (survey_version == "CMU") {
    # Bodge E1_* names for Wave 11
    if (wave == 11) {
      matrix_subquestion_field_names[item_names == "E1"] <- list(c("E1_1", "E1_2", "E1_3", "E1_4"))
    }
  }
  
  # deduce if randomizing or reversing order of responses
  raw_random_type <- displayed_questions %>% 
    map(~ .x$Payload$Randomization$Type) %>% 
    map(~ ifelse(is.null(.x), NA, .x))
  raw_random_all <- displayed_questions %>% 
    map(~ .x$Payload$Randomization$Advanced$RandomizeAll) %>% 
    map(~ ifelse(is.null(.x), NA, .x))
  raw_scale_reversal <- displayed_questions %>% 
    map(~ .x$Payload$Randomization$ConsistentScaleReversal) %>% 
    map(~ ifelse(is.null(.x), NA, .x))
  response_option_randomization <- case_when(
      raw_random_type == "All" ~ "randomized",
      raw_random_type == "Advanced" & map(raw_random_all, length) > 0 ~ "randomized",
      raw_random_type == "ScaleReversal" ~ "scale reversal",
      raw_scale_reversal == TRUE ~ "scale reversal",
      TRUE ~ "none"
    )

  # format display logic
  # Not all questions have display logic; if NULL, shown to all respondents within section.
  # Also check "InPageDisplayLogic". If not null, combine with normal "DisplayLogic".
  inpage_ii <- displayed_questions %>% 
    map(~ .x$Payload$InPageDisplayLogic) %>% map_lgl(~ !is.null(.x)) %>% which()
  inpage_logic <- displayed_questions %>% 
    map(~ .x$Payload$InPageDisplayLogic)
  
  display_logic <- displayed_questions %>% 
    map(~ .x$Payload$DisplayLogic)
  if (display_logic[inpage_ii] %>% map_lgl(~ !is.null(.x)) %>% any()) {
    stop("At least one question has both 'DisplayLogic' and 'InPageDisplayLogic'.",
         " 'DisplayLogic' would be overwritten.")
  }
  display_logic[inpage_ii] <- inpage_logic[inpage_ii]
  display_logic_raw <- display_logic
  
  display_logic <- display_logic %>% 
    map(~ .x$`0`) %>% 
    map(~ paste(
      map(.x, "Conjuction"),
      map(.x, "LeftOperand"),
      "Is",
      map(.x, "Operator"),
      map(.x, "RightOperand")
    )) %>% 
    # Remove empty logic
    map(~ gsub("  Is  ", "", .x)) %>% 
    map(~ gsub("NULL NULL Is NULL NULL", "", .x)) %>%
    map(~ gsub(" ?NULL ?", "", .x)) %>%
    # Remove QID flag
    map(~ gsub("q://", "", .x)) %>%
    # Recode choice numbers
    map(~ gsubfn("(QID[0-9]+)(/SelectableChoice/)([0-9]+)", function(qid, selectable_text, option_code) {
      curr_map <- recode_map[qids == qid][[1]]
      
      if ( !is.null(curr_map) ) {
        option_code <- ifelse(option_code %in% names(curr_map), curr_map[[which(names(curr_map) == option_code)]], option_code)
      }
      
      paste(c(qid, selectable_text, option_code), collapse="")
    }, .x)) %>% 
    # Replace QID with question number (A2, etc)
    map(~ gsubfn("(QID[0-9]+)", function(qid) {item_names[qids == qid]}, .x)) %>% 
    # Collapse logic into a single string.
    map(~ paste(.x, collapse=" "))
  
  # Handle questions that use a fixed condition ("If False", "If True")
  ii_boolean_displaylogic <- (display_logic_raw %>% 
                                map(~ .x$`0`) %>% 
                                map(~ map(.x, "LogicType") %>% unlist()) == "BooleanValue") %>% 
    which()
  
  display_logic[ii_boolean_displaylogic] <- display_logic_raw[ii_boolean_displaylogic] %>% 
    map(~ .x$`0`) %>% 
    map(~ paste(
      map(.x, "Value")
    )) %>%
    map(~ gsub(" ?NULL ?", "", .x)) %>% 
    # Collapse logic into a single string.
    map(~ paste(.x, collapse=""))
    
  logic_type <- display_logic_raw %>% 
    map(~ .x$`0`$Type)
  
  display_logic <- paste(logic_type, display_logic) %>%
    map(~ gsub(" ?NULL ?", "", .x)) %>% 
    map(~ gsub(" $", "", .x)) %>%
    unlist()
  
  # Hard-code display logic for UMD V15a.
  if (survey_version == "UMD" && wave == 12) {
    display_logic[which(item_names == "V15a")] <- "If V1/SelectableChoice/1 Is NotSelected"
  }
  
  # format all qsf content lists into a single tibble
  qdf <- tibble(variable = item_names,
                question = questions,
                question_type = qtype,
                response_options = choices,
                matrix_subquestions = matrix_subquestions,	
                display_logic = display_logic,	
                response_option_randomization = response_option_randomization,	
                matrix_subquestion_field_names = matrix_subquestion_field_names)
  if (file.exists(path_to_drop_columns)){	
    drop_cols <- read_csv(path_to_drop_columns, trim_ws = FALSE,
                          col_types = cols(item = col_character()
                          ))
    qdf <- filter(qdf, !(variable %in% drop_cols$item))
  } else {
    warning("path_to_drop_columns ", path_to_drop_columns, " not found")
  }
  # Add on module randomization
  block_id_item_map <- map_qids_to_module(q)
  block_id_item_map <- block_id_item_map %>%
    left_join(data.frame(qid=qids, item=item_names), by=c("Questions"="qid"))
  qdf <- qdf %>% left_join(block_id_item_map, by=c(variable="item")) %>%
    rename(respondent_group = BlockName)
  
  # If a question's display logic depends on a question from a randomized
  # module, consider it randomized too
  module_assignment_from_display_logic <- map(qdf$display_logic, ~ sapply(seq_along(block_id_item_map$item), function(i) {
    if (length(grep(paste0(block_id_item_map$item[i], "/"), .x)) != 0) {
      block_id_item_map$BlockName[i]
    } else {
      NA_character_
    }
  })) %>%
    map(unique) %>%
    map(~ if (length(.x) > 1) { .x[!is.na(.x)] } else {NA}) %>% 
    unlist()
  qdf <- qdf %>% mutate(respondent_group=coalesce(respondent_group, module_assignment_from_display_logic)) %>%
    replace_na(list(respondent_group = "all"))  
  

  if (length(qdf$variable) != length(unique(qdf$variable))) {
    duplicate_items <- qdf$variable[duplicated(qdf$variable)]
    stop(
      "Item names should be unique, but ",
      paste(duplicate_items, collapse=", "),
      " each appears multiple times"
    )
  }
  
  # Remove blank questions (rare).
  qdf <- qdf %>% filter(question != "Click to write the question text")
  
  # Add short description mapped to variable name
  if (file.exists(path_to_shortname_map)) {
  description_map <- read_csv(path_to_shortname_map,
                           col_types = cols(item = col_character(),
                                            description = col_character()
                           )) %>%
    remove_rownames() %>%
    column_to_rownames(var="item")
  } else {
    error("path_to_shortname_map ", path_to_shortname_map, " not found")
  }
  qdf <- qdf %>% 
    mutate(description = description_map[variable, "description"])
  
  # set blank display logic to "none"
  qdf$display_logic <- ifelse(qdf$display_logic == "", "none", qdf$display_logic)
  
  # separate matrix subquestions into separate fields (to match exported data)	
  nonmatrix_items <- qdf %>%	
    filter(question_type != "Matrix") %>%	
    mutate(matrix_base_name = NA_character_) %>% 
    select(-matrix_subquestion_field_names)
  
  has_response_by_subq <- qdf %>%	
    filter(question_type == "Matrix") %>%
    pull(response_options) %>%
    map_lgl(~ all(map_lgl(.x, ~ inherits(.x, "list"))) &&
              !identical(.x, list()))
  
  matrix_items <- qdf %>%	
    filter(question_type == "Matrix") %>%
    filter(!has_response_by_subq) %>%
    rowwise() %>% 	
    mutate(new = list(	
      tibble(matrix_base_name = variable,
             variable = unlist(matrix_subquestion_field_names),
             question = question,	
             matrix_subquestion = unlist(matrix_subquestions),	
             question_type = question_type,	
             response_option_randomization = ifelse(	
               response_option_randomization == "randomized", "none", response_option_randomization),	
             description = description,	
             response_options = list(response_options),
             display_logic = display_logic,
             respondent_group = respondent_group)
      )) %>% 
    select(new) %>% 
    unnest(new)
    
  
  matrix_items_resp_by_subq <- qdf %>%	
    filter(question_type == "Matrix") %>%
    filter(has_response_by_subq) %>%
    rowwise() %>% 	
    mutate(new = list(	
      tibble(matrix_base_name = variable,
             variable = unlist(matrix_subquestion_field_names),	
             question = question,	
             matrix_subquestion = unlist(matrix_subquestions),	
             question_type = question_type,	
             response_option_randomization = ifelse(	
               response_option_randomization == "randomized", "none", response_option_randomization),	
             description = description,	
             response_options = map(response_options, ~ list(.x)),
             display_logic = display_logic,
             respondent_group = respondent_group)
    )) %>% 
    select(new) %>% 
    unnest(new)
  
  matrix_items <- rbind(matrix_items, matrix_items_resp_by_subq) %>%
    select(variable, matrix_base_name, everything())
  
  # Custom matrix formatting
  if (survey_version == "CMU") {
    # A5 and C10 are special cases b/c they are matrices of text entry questions:
    # also C10 needs an extra _1.
    matrix_items <- matrix_items %>% 
      mutate(variable = if_else(str_starts(variable, "C10"), paste0(variable, "_1"), variable),
             question_type = if_else(str_starts(variable, "A5|C10"), "Text", question_type),
             response_options = if_else(str_starts(variable, "A5|C10"), list(list()), response_options))
  } else if (survey_version == "UMD") {
    # pass
  }
  
  qdf <- bind_rows(nonmatrix_items, matrix_items)
  
  # indicate which new items have replaced old items.
  replaces_map <- read_csv(path_to_replacement_map,
                           col_types = cols(new_item = col_character(),
                                            old_item = col_character()
                           )) %>%
    remove_rownames() %>%
    column_to_rownames(var="new_item")
  qdf <- qdf %>%
    mutate(replaces = ifelse(variable %in% rownames(replaces_map), 
                             replaces_map[variable, "old_item"], 
                             "none"),
           wave = wave
    ) %>% 
    select(wave,
           variable,
           matrix_base_name,
           replaces,
           description,
           question,
           matrix_subquestion,
           response_options,
           question_type,
           display_logic,
           response_option_randomization,
           respondent_group)
  
  # Format choices as json string
  qdf$response_options <- map(qdf$response_options, function(x) {
    if (is_empty(x)) { NA }
    else { toJSON(x) }
  }) %>%
    unlist()
  
  # add free text response options
  other_text_items <- qdf %>%
    filter(variable %in% names(other_text_items)) %>%
    # "Other text" items should borrow most fields from base question.
    mutate(variable = other_text_items[variable],
           question_type = "Text",
           description = paste0(description, " other text")
    )
  qdf <- rbind(qdf, other_text_items)
  qdf$response_options[qdf$question_type == "Text"] <- NA
  
  # Drop occasional start and end square brackets from matrix response options.
  qdf <- qdf %>%
    mutate(
      response_options = map_chr(response_options, ~ remove_brackets(.x))
    )
  
  # Quality checks
  stopifnot(length(qdf$variable) == length(unique(qdf$variable)))
  
  if (any(is.na(qdf$description))) {
    nonlabelled_items <- qdf$variable[is.na(qdf$description)]
    stop(sprintf("items %s do not have a description provided",
                 paste(nonlabelled_items, collapse=", "))
    )
  }
  
  return(qdf %>% rename(version = wave))
}

remove_brackets <- function(response_set) {
  if ( !is.na(response_set) && startsWith(response_set, "[") && endsWith(response_set, "]") ) {
    str_sub(response_set, 2, -2)
  } else {
    response_set
  }
}

#' Append the parsed and formatted info from the QSF to the existing codebook
#'
#' @param qdf dataframe containing parsed QSF data
#' @param path_to_codebook
#'
#' @return dataframe of existing codebook augmented with new wave QSF data
add_qdf_to_codebook <- function(qdf,
                                path_to_codebook,
                                survey_version,
                                static_fields_file="static_microdata_fields.csv") {
  path_to_static_fields <- localize_static_filepath(static_fields_file, survey_version)
  qdf_wave <- unique(qdf$version)
  
  if (!file.exists(path_to_codebook)) {
    # Create an empty df with the right column names and order
    codebook <- qdf[FALSE, ]
  } else {
    codebook <- read_csv(path_to_codebook, col_types = cols(
      .default = col_character(),
      version = col_double(),
      variable = col_character(),
      replaces = col_character(),
      description = col_character(),
      question = col_character(),
      matrix_subquestion = col_character(),
      question_type = col_character(),
      display_logic = col_character(),
      response_option_randomization = col_character()
    ))
  }
  
  if (qdf_wave %in% codebook$version) {
    warning(sprintf("wave %s already added to codebook. removing existing rows and replacing with newer data", qdf_wave))
    codebook <- codebook %>% filter(version != qdf_wave)
  }
  
  if (survey_version == "UMD") {
    if ("replaces" %in% names(codebook)) {
      codebook <- codebook %>% select(-replaces)
    }
    if ("replaces" %in% names(qdf)) {
      qdf <- qdf %>% select(-replaces)
    }
  }
  
  # Using rbind here to raise an error if columns differ between the existing
  # codebook and the new wave data.
  # Sort so that items with missing question_type (non-Qualtrics fields) are at the top.
  codebook <- rbind(codebook, qdf) %>%
    add_static_fields(qdf_wave, survey_version, path_to_static_fields) %>% 
    arrange(!is.na(.data$question_type), variable, version)
  
  ii_replacing_DNE <- which( !(codebook$replaces %in% codebook$variable) & !is.na(codebook$replaces) & codebook$replaces != "none")
  if ( length(ii_replacing_DNE) > 0 ) {
    replacing_variables <- unique( codebook$variable[ii_replacing_DNE] )
    warning(sprintf("the items that %s report replacing do not exist in the codebook",
                 paste(replacing_variables, collapse=", "))
    )
  }
  return(codebook)
}

#' Add non-Qualtrics data fields to codebook, filling in missing fields with NA
#'
#' @param codebook
#' @param wave integer survey wave number
#' @param path_to_static_fields
#'
#' @return codebook dataframe augmented with non-Qualtrics fields included in microdata
add_static_fields <- function(codebook,
                              wave,
                              survey_version,
                              path_to_static_fields="static_microdata_fields.csv") {
  static_fields <- get_static_fields(wave, path_to_static_fields)
  
  codebook <- bind_rows(codebook, static_fields)
  
  if (survey_version == "CMU") {
    codebook <- filter(
      codebook,
      !(variable == "module" & version < 11), # module field is only available for wave >= 11
      !(variable %in% c("wave", "UserLanguage", "fips") & version < 4), # wave, UserLangauge, and fips fields are only available for wave >= 4
      !(variable == "w12_treatment" & version != 12.5) # experimental arm field is only available for wave == 12.5
    )
  } else if (survey_version == "UMD") {
    codebook <- filter(
      codebook,
      !(variable == "module" & version < 11), # module field is only available for wave >= 11
      !(variable == "w12_treatment" & version != 12.5) # experimental arm field is only available for wave == 12.5
    )
  }
  return(codebook)
}

#' Load dataframe of non-Qualtrics data fields
#'
#' @param wave integer survey wave number
#' @param path_to_static_fields
#'
#' @return dataframe of non-Qualtrics fields included in microdata
get_static_fields <- function(wave,
                              path_to_static_fields="static_microdata_fields.csv") {
  static_fields <- read_csv(path_to_static_fields,
                            col_types = cols(variable = col_character(),
                                             replaces = col_character(),
                                             description = col_character(),
                                             question = col_character(),
                                             matrix_subquestion = col_character(),
                                             question_type = col_character(),
                                             response_option_randomization = col_character()
                            )) %>%
    mutate(version = wave) %>%
    select(version, everything())
  
  return(static_fields)
}

#' Run the tool, saving the updated codebook to disk.
#'
#' @param path_to_qsf
#' @param path_to_codebook
#'
#' @return none
add_qsf_to_codebook <- function(path_to_qsf, path_to_codebook,
                                survey_version=c("CMU", "UMD")) {
  survey_version <- match.arg(survey_version)
  qdf <- process_qsf(path_to_qsf, survey_version)
  codebook <- add_qdf_to_codebook(qdf, path_to_codebook, survey_version)
  write_excel_csv(codebook, path_to_codebook, quote="needed")
}



args <- commandArgs(TRUE)

if (length(args) != 3) {
  stop("Usage: Rscript generate-codebook.R [UMD/CMU] path/to/qsf/file/or/dir path/to/codebook")
}

survey_version <- args[1]
path_to_qsf <- args[2]
path_to_codebook <- args[3]

if (dir.exists(path_to_qsf)) {
  # Iteratively process .qsf files contained in the directory
  qsfs <- list.files(path_to_qsf, pattern = "*.qsf$", full.names = TRUE)
  
  if (length(qsfs) == 0) {
    stop(path_to_qsf, " does not contain any .qsf files")
  }
  
  options(warn = 1)
  for (path_to_one_qsf in qsfs) {
    invisible(add_qsf_to_codebook(path_to_one_qsf, path_to_codebook, survey_version))
  }
} else if (file.exists(path_to_qsf)) {
  invisible(add_qsf_to_codebook(path_to_qsf, path_to_codebook, survey_version)) 
} else {
  stop(path_to_qsf, " is not a directory or a file, and cannot be processed")
}
