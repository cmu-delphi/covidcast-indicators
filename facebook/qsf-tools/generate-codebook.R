#!/usr/bin/env Rscript

## Add information from one .qsf translation file at a time to the survey codebook
##
## Usage:
##
## Rscript generate-codebook.R [UMD/CMU] path/to/qsf path/to/codebook

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
  choices[ii_carryforward] <- lapply(	
    seq_along(carryforward_choices_qid),	
    function(ind) {	
      # Get choices that are new for this question	
      old_choices <- choices[qids == qids[ii_carryforward[ind]]] %>% unlist(recursive = FALSE)	
      carryforward_choices <- choices[qids == carryforward_choices_qid[ind]] %>% unlist(recursive = FALSE)	
      # Combine new choices and carried-forward choices	
      c(old_choices, carryforward_choices)	
    }	
  )	
  
  # get the "answers". These are answer choices for matrix items, and missing for non-matrix items.	
  matrix_answers <- displayed_questions %>%	
    map(~ .x$Payload$Answers) %>%	
    map(~ map(.x, "Display"))	
  
  # Swap matrix answer choices into `choices` and matrix subquestion text into another variable	
  ii_matrix <- which(qtype == "Matrix")	
  matrix_subquestions <- rep(list(list()), length(choices))	
  matrix_subquestions[ii_matrix] <- choices[ii_matrix]	
  choices[ii_matrix] <- matrix_answers[ii_matrix]	
  
  # Recode response options if overriding Qualtrics auto-assigned coding.	
  recode_map <- displayed_questions %>% 	
    map(~ .x$Payload$RecodeValues)	
  ii_recode <- recode_map %>%	
    map(~ !is.null(.x)) %>% 	
    unlist() %>% 	
    which()	
  choices[ii_recode] <- map2(.x=choices[ii_recode], .y=recode_map[ii_recode],	
                             ~ setNames(.x, .y[names(.x)])	
  )	
  
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
  display_logic <- displayed_questions %>% 
    map(~ .x$Payload$DisplayLogic) %>% 
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
        option_code <- curr_map[names(curr_map) == option_code]
      }
      
      paste(c(qid, selectable_text, option_code), collapse="")
    }, .x)) %>% 
    # Replace QID with question number (A2, etc)
    map(~ gsubfn("(QID[0-9]+)", function(qid) {item_names[qids == qid]}, .x)) %>% 
    # Collapse logic into a single string.
    map(~ paste(.x, collapse=" "))
    
  logic_type <- displayed_questions %>% 
    map(~ .x$Payload$DisplayLogic) %>% 
    map(~ .x$`0`$Type)
  
  display_logic <- paste(logic_type, display_logic) %>%
    map(~ gsub(" ?NULL ?", "", .x)) %>% 
    map(~ gsub(" $", "", .x)) %>%
    unlist()
  
  # format all qsf content lists into a single tibble
  qdf <- tibble(variable = item_names,
                question = questions,
                type = qtype,
                choices = choices,
                matrix_subquestions = matrix_subquestions,	
                display_logic = display_logic,	
                response_option_randomization = response_option_randomization,	
                matrix_subquestion_field_names = matrix_subquestion_field_names)	
  
  # Add on module randomization
  block_id_item_map <- map_qids_to_module(q)
  block_id_item_map <- block_id_item_map %>%
    left_join(data.frame(qid=qids, item=item_names), by=c("Questions"="qid"))
  qdf <- qdf %>% left_join(block_id_item_map, by=c(variable="item")) %>%
    rename(group_of_respondents_item_was_shown_to = BlockName)
  
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
  qdf <- qdf %>% mutate(group_of_respondents_item_was_shown_to=coalesce(group_of_respondents_item_was_shown_to, module_assignment_from_display_logic)) %>%
    replace_na(list(group_of_respondents_item_was_shown_to = "all"))  
  

  stopifnot(length(qdf$variable) == length(unique(qdf$variable)))
  
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
    filter(type != "Matrix") %>%	
    select(-matrix_subquestion_field_names)
  matrix_items <- qdf %>%	
    filter(type == "Matrix") %>% 	
    rowwise() %>% 	
    mutate(new = list(	
      tibble(variable = unlist(matrix_subquestion_field_names),	
             question = question,	
             matrix_subquestion = unlist(matrix_subquestions),	
             type = type,	
             response_option_randomization = ifelse(	
               response_option_randomization == "randomized", "none", response_option_randomization),	
             description = description,	
             choices = list(choices),
             display_logic = display_logic,
             group_of_respondents_item_was_shown_to = group_of_respondents_item_was_shown_to)
      )) %>% 
    select(new) %>% 
    unnest(new)
  
  # Custom matrix formatting
  if (survey_version == "CMU") {
    # A5 and C10 are special cases b/c they are matrices of text entry questions:
    # also C10 needs an extra _1.
    matrix_items <- matrix_items %>% 
      mutate(variable = if_else(str_starts(variable, "C10"), paste0(variable, "_1"), variable),
             type = if_else(str_starts(variable, "A5|C10"), "Text", type),
             choices = if_else(str_starts(variable, "A5|C10"), list(list()), choices))
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
                             NA_character_),
           wave = wave
    ) %>% 
    select(wave,
           variable,
           replaces,
           description,
           question,
           matrix_subquestion,
           choices,
           type,
           display_logic,
           response_option_randomization,
           group_of_respondents_item_was_shown_to)
  
  # Format choices as json string
  qdf$choices <- map(qdf$choices, function(x) {
    if (is_empty(x)) { NA }
    else { toJSON(x) }
  }) %>%
    unlist()
  
  # add free text response options
  other_text_items <- qdf %>%
    filter(variable %in% names(other_text_items)) %>%
    mutate(variable = other_text_items[variable],
           type = "Text",
           response_option_randomization = NA,
           description = paste0(description, " other text")
    )
  qdf <- rbind(qdf, other_text_items)
  qdf$choices[qdf$type == "Text"] <- NA
  
  if (file.exists(path_to_drop_columns)){	
    drop_cols <- read_csv(path_to_drop_columns, trim_ws = FALSE,
                          col_types = cols(item = col_character()
                          ))
    qdf <- filter(qdf, !(variable %in% drop_cols$item))
  } else {
    warning("path_to_drop_columns ", path_to_drop_columns, " not found")
  }
  
  # Quality checks
  stopifnot(length(qdf$variable) == length(unique(qdf$variable)))
  
  if (any(is.na(qdf$description))) {
    nonlabelled_items <- qdf$variable[is.na(qdf$description)]
    stop(sprintf("items %s do not have a description provided",
                 paste(nonlabelled_items, collapse=", "))
    )
  }
  
  return(qdf)
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
  qdf_wave <- unique(qdf$wave)
  
  if (!file.exists(path_to_codebook)) {
    # Create an empty df with the right column names and order
    codebook <- qdf[FALSE, ]
  } else {
    codebook <- read_csv(path_to_codebook, col_types = cols(
      .default = col_character(),
      wave = col_double(),
      variable = col_character(),
      replaces = col_character(),
      description = col_character(),
      question = col_character(),
      matrix_subquestion = col_character(),
      type = col_character(),
      display_logic = col_character(),
      response_option_randomization = col_character()
    ))
  }
  
  if (qdf_wave %in% codebook$wave) {
    warning(sprintf("wave %s already added to codebook. removing existing rows and replacing with newer data", qdf_wave))
    codebook <- codebook %>% filter(wave != qdf_wave)
  }
  
  # Using rbind here to raise an error if columns differ between the existing
  # codebook and the new wave data.
  # Sort so that items with missing type (non-Qualtrics fields) are at the top.
  codebook <- rbind(codebook, qdf) %>%
    add_static_fields(qdf_wave, survey_version, path_to_static_fields) %>% 
    arrange(!is.na(.data$type), variable, wave)
  
  ii_replacing_DNE <- which( !(codebook$replaces %in% codebook$variable) & !is.na(codebook$replaces) )
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
      !(variable == "module" & wave < 11), # module field is only available for wave >= 11
      !(variable %in% c("wave", "UserLanguage", "fips") & wave < 4), # wave, UserLangauge, and fips fields are only available for wave >= 4
      !(variable == "w12_treatment" & wave != 12.5) # experimental arm field is only available for wave == 12.5
    )
  } else if (survey_version == "UMD") {
    codebook <- filter(
      codebook,
      !(variable == "module" & wave < 11), # module field is only available for wave >= 11
      !(variable %in% c("wave", "UserLanguage", "fips") & wave < 4), # wave, UserLangauge, and fips fields are only available for wave >= 4
      !(variable == "w12_treatment" & wave != 12.5) # experimental arm field is only available for wave == 12.5
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
                                             type = col_character(),
                                             response_option_randomization = col_character()
                            )) %>%
    mutate(wave = wave) %>% 
    select(wave, everything())
  
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
  stop("Usage: Rscript generate-codebook.R [UMD/CMU] path/to/qsf path/to/codebook")
}

survey_version <- args[1]
path_to_qsf <- args[2]
path_to_codebook <- args[3]

invisible(add_qsf_to_codebook(path_to_qsf, path_to_codebook, survey_version))
