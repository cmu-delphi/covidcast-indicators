#!/usr/bin/env Rscript

## Add information from one .qsf translation file at a time to the survey codebook
##
## Usage:
##
## Rscript generate-codebook.R path/to/qsf path/to/codebook

suppressPackageStartupMessages({
  library(tidyverse)
  library(jsonlite)
  library(rjson)
  library(stringr)
  library(gsubfn)
  source("qsf-utils.R")
})


process_qsf <- function(path_to_qsf,
                        path_to_shortname_map="./static/item_shortquestion_map.csv",
                        path_to_replacement_map="./static/item_replacement_map.csv") {
  q <- read_json(path_to_qsf)
  
  # get the survey elements with flow logic (should be one per block randomization branch)
  ii_flow <- q$SurveyElements %>%
    map_chr("Element") %>%
    {. == "FL"} %>%
    which()
  ii_block_randomizer <- q$SurveyElements[ii_flow] %>%
    map(~ .x$Payload$Flow) %>%
    map(~ map(.x,~ .x$Type == "BlockRandomizer")) %>%
    unlist() %>% 
    which()
  random_block_ids <- q$SurveyElements[ii_flow] %>%
    map(~ .x$Payload$Flow) %>%
    map(~ .x[ii_block_randomizer]) %>% 
    map(~ map(.x,~ .x$Flow)) %>% 
    map(~ map(.x,~ map(.x,~ .x$ID))) %>%
    unlist()
  
  block_id_item_map <- get_block_item_map(q)
  block_id_item_map <- block_id_item_map %>% filter(BlockID %in% random_block_ids) %>%
    select(-BlockID)
  
  # get the survey elements that are questions:
  ii_questions <- q$SurveyElements %>% 
    map_chr("Element") %>%
    {. == "SQ"} %>% 
    which()
  
  # get the questions that were shown to respondents
  shown_items <- get_shown_items(q)
  ii_shown <- q$SurveyElements[ii_questions] %>% 
    map_chr(~ .x$Payload$QuestionID) %>%
    {. %in% shown_items} %>% 
    which()
  
  # subset qsf to valid elements
  displayed_questions <- q$SurveyElements[ii_questions][ii_shown]
  
  # Qualtrics auto-assigned question IDs
  qids <- displayed_questions %>% 
    map_chr(~ .x$Payload$QuestionID)
  
  # the items are how we will match these to the survey data:
  items <- displayed_questions %>% 
    map_chr(~ .x$Payload$DataExportTag)
  
  # B13 was originally named incorrectly. Rename manually as needed
  items[items == "B13 "] <- "B13"
  
  # get the text of the question:
  questions <- displayed_questions %>% 
    map_chr(~ .x$Payload$QuestionText) %>% 
    str_remove_all("<[^<>]+>") %>% 
    str_replace_all("&nbsp;", " ")
  
  # get the type of question:
  type_map <- c(MC = "Multiple choice", TE = "Text", Matrix = "Matrix")
  qtype <- displayed_questions %>%
    map_chr(~ .x$Payload$QuestionType) %>% 
    {type_map[.]}
  
  ii_multiselect <- displayed_questions %>%
    map_chr(~ .x$Payload$Selector) %>%
    {. == "MAVR"} %>% 
    which()
  qtype[ii_multiselect] <- "Multiselect"
  qtype[items == "A5"] <- "Matrix" # this will be treated like C10
  
  # get the choices (for MC and Matrix):
  choices <- displayed_questions %>% 
    map(~ .x$Payload$Choices) %>% 
    map(~ map(.x, "Display"))
  recode_map <- displayed_questions %>% 
    map(~ .x$Payload$RecodeValues)
  
  # Recode response options if overriding Qualtrics auto-assigned coding.
  ii_recode <- recode_map %>%
    map(~ !is.null(.x)) %>% 
    unlist() %>% 
    which()
  choices[ii_recode] <- map2(.x=choices[ii_recode], .y=recode_map[ii_recode],
                             ~ setNames(.x, .y[names(.x)])
  )
  
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
  other_text_items <- paste(items[ii_other_text_option], text_elem, "TEXT", sep="_") %>%
    setNames(items[ii_other_text_option])
  
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
  choices[ii_carryforward] <- choices[qids %in% carryforward_choices_qid]
  
  # get the answers (for Matrix):
  answers <- displayed_questions %>% 
    map(~ .x$Payload$Answers) %>% 
    map(~ map(.x, "Display"))
  
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
    map(~ gsubfn("(QID[0-9]+)", function(qid) {items[qids == qid]}, .x)) %>% 
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
  qdf <- tibble(variable = items,
                question = questions,
                type = qtype,
                choices = choices,
                answers = answers,
                display_logic = display_logic,
                response_option_randomization = response_option_randomization)
  
  # Add on module randomization
  block_id_item_map <- block_id_item_map %>%
    left_join(data.frame(qid=qids, item=items), by=c("Questions"="qid"))
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
  description_map <- read_csv(path_to_shortname_map,
                           col_types = cols(item = col_character(),
                                            description = col_character()
                           )) %>%
    remove_rownames() %>%
    column_to_rownames(var="item")
  qdf <- qdf %>% 
    mutate(description = description_map[variable, "description"])
  
  # set blank display logic to "none"
  qdf$display_logic <- ifelse(qdf$display_logic == "", "none", qdf$display_logic)
  
  # matrix to separate items (to match exported data)
  nonmatrix_items <- qdf %>% filter(type != "Matrix")
  matrix_items <- qdf %>%
    filter(type == "Matrix") %>% 
    rowwise() %>% 
    mutate(new = list(
      tibble(variable = paste(variable, 1:length(choices), sep = "_"),
             question = question,
             matrix_subquestion = unlist(choices),
             type = type,
             response_option_randomization = ifelse(
               response_option_randomization == "randomized", "none", response_option_randomization),
             description = description,
             choices = list(answers),
             answers = list(list()),
             display_logic = display_logic,
             group_of_respondents_item_was_shown_to = group_of_respondents_item_was_shown_to)
      )) %>% 
    select(new) %>% 
    unnest(new)
  
  # A5 and C10 are special cases b/c of they are matrix of text entry questions:
  # also C10 needs an extra _1.
  matrix_items <- matrix_items %>% 
    mutate(variable = if_else(str_starts(variable, "C10"), paste0(variable, "_1"), variable),
           type = if_else(str_starts(variable, "A5|C10"), "Text", type),
           choices = if_else(str_starts(variable, "A5|C10"), list(list()), choices))
  
  qdf <- bind_rows(nonmatrix_items, matrix_items)
  
  # indicate which items have replaced old items.
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
           wave = get_wave(path_to_qsf)
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
                                path_to_static_fields="./static/static_microdata_fields.csv") {
  qdf_wave <- unique(qdf$wave)
  
  if (!file.exists(path_to_codebook)) {
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
    add_static_fields(qdf_wave, path_to_static_fields) %>% 
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
                              path_to_static_fields="./static/static_microdata_fields.csv") {
  static_fields <- get_static_fields(wave, path_to_static_fields)
  
  codebook <- bind_rows(codebook, static_fields) %>% 
    filter(!(variable == "module" & wave < 11), # module field is only available for wave >= 11
           !(variable %in% c("wave", "UserLanguage", "fips") & wave < 4), # wave, UserLangauge, and fips fields are only available for wave >= 4
           !(variable == "w12_treatment" & wave == 12.5), # experimental arm field is only available for wave == 12.5
           variable != "Random_Number"
    )

  return(codebook)
}

#' Load dataframe of non-Qualtrics data fields
#'
#' @param wave integer survey wave number
#' @param path_to_static_fields
#'
#' @return dataframe of non-Qualtrics fields included in microdata
get_static_fields <- function(wave,
                              path_to_static_fields="./static/static_microdata_fields.csv") {
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
add_qsf_to_codebook <- function(path_to_qsf, path_to_codebook) {
  qdf <- process_qsf(path_to_qsf)
  codebook <- add_qdf_to_codebook(qdf, path_to_codebook)
  write_excel_csv(codebook, path_to_codebook, quote="needed")
}



args <- commandArgs(TRUE)

if (length(args) != 2) {
  stop("Usage: Rscript generate-codebook.R path/to/qsf path/to/codebook")
}

path_to_qsf <- args[1]
path_to_codebook <- args[2]

invisible(add_qsf_to_codebook(path_to_qsf, path_to_codebook))
