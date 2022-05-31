#!/usr/bin/env Rscript

## Diff two .qsf files to find new and changed survey items
##
## Usage:
##
## Rscript qsf-differ.R UMD|CMU path/to/old/qsf path/to/new/qsf [path/to/desired/output-dir]

options(warn = 1)

suppressPackageStartupMessages({
  library(jsonlite)
  library(stringr)
  library(dplyr)
  library(purrr)
  library(readr)
  source("qsf-utils.R")
})

#' Load and diff chosen qsf files.
#'
#' @param old_qsf_path path to older Qualtrics survey file in .qsf format
#' @param new_qsf_path path to newer Qualtrics survey file in .qsf format
#' @param output_dir path to desired output directory. It doesn't already exist, the
#'     directory will be created
#' @param survey_version "CMU" or "UMD"
diff_qsf_files <- function(old_qsf_path, new_qsf_path, output_dir,
                           survey_version=c("CMU", "UMD")) {
  survey_version <- match.arg(survey_version)
  
  old_qsf <- get_qsf_file(old_qsf_path, survey_version)
  new_qsf <- get_qsf_file(new_qsf_path, survey_version)
  
  old_wave <- get_wave(old_qsf_path)
  new_wave <- get_wave(new_qsf_path)
  
  out <- diff_surveys(old_qsf, new_qsf) %>%
    mutate(
      old_wave = old_wave, new_wave = new_wave
    ) %>% 
    select(new_wave, old_wave, everything())

  if (!dir.exists(output_dir)) { dir.create(output_dir) }
  write_csv(
    out,
    file.path(output_dir, paste0("diff_", old_wave, "-", new_wave, ".csv", collapse=""))
  )
  
  return(NULL)
}

#' Fetch and format a single .qsf file, keeping block and question info
#'
#' @param path path to Qualtrics survey file in .qsf format
#' @param keep_items character vector of survey item fields to keep.
#'   Setting to c("all") keeps all fields.
#'
#' @return A named list
get_qsf_file <- function(path, survey_version,
                         keep_items = c("QuestionID", "DataExportTag",
                                        "QuestionText", "QuestionType",
                                        "Choices", "Answers", "DisplayLogic",
                                        "InPageDisplayLogic")
) {
  wave <- get_wave(path)
  # Read file as json.
  qsf <- read_json(path)
  ## Block
  shown_items <- get_shown_items(qsf)
  
  ## Questions
  questions <- Filter(function(elem) { elem[["Element"]] == "SQ" }, qsf$SurveyElements)
  
  qids <- questions %>% 
    map_chr(~ .x$Payload$QuestionID)
  
  qid_item_map <- list()
  questions_out <- list()
  for (question_raw in questions) {
    question_raw <- question_raw$Payload
    
    # Skip items not shown to respondents.
    if ( !(question_raw$QuestionID %in% shown_items) ) {
      next
    }
    
    question <- question_raw[names(question_raw) %in% c("QuestionID", keep_items)]
    
    recode_values <- question_raw$RecodeValues # If doesn't exist, will be NULL
    carryforward_choices <- question_raw$DynamicChoices$Locator # If doesn't exist, will be NULL
    
    if (!is.null(carryforward_choices)) {
      # Get choices that are programmed specifically for this question	
      old_choices <- question$Choices
      
      # Get carried-forward choices
      carryforward_choices_qid <- carryforward_choices %>% 
        str_split(., "/") %>% 
        map(~ .x[3]) %>% unlist()
      carryforward_question <- questions[qids == carryforward_choices_qid][[1]]$Payload
      carryforward_choices <- carryforward_question$Choices
      
      # By default, carried forward choices are coded during the carry as
      # "x<original code>". They are then recoded as pure numeric codes using
      # the `RecodeValues` field. Some carried forward choices do not have
      # `RecodeValues` defined and so in that case we don't want to prepend the
      # codes with "x".
      #
      # This also applies to matrix questions, although they have missing
      # `RecodeValues`. `RecodeValues` are applied to the answer choices;
      # subquestions are recoded with `ChoiceDataExportTags`.
      if (!is.null(recode_values) | question$QuestionType == "Matrix") {
        names(carryforward_choices) <- paste0("x", names(carryforward_choices))  
      }
      # Combine new choices and carried-forward choices
      question$Choices <- c(old_choices, carryforward_choices)
    }
    
    if ("QuestionType" %in% names(question)) {
      if (question$QuestionType == "Matrix") {
        # Rearrange Answers/Choices elements to be consistent between matrix and
        # other items.
        question$Subquestions <- question$Choices
        question$Choices <- question$Answers
        question$Answers <- NULL
        
        # Recode subquestion names to match exported data.
        # FALSE if not set, otherwise a list
        matrix_subquestion_field_names <- question_raw$ChoiceDataExportTags
        if (!inherits(matrix_subquestion_field_names, "list")) {
          # When subquestion field names are not set, generate incrementing names
          names(question$Subquestions) <- paste(
            question$DataExportTag,	
            1:length(question$Subquestions),	
            sep = "_"	
          )
        } else {
          names(question$Subquestions) <- matrix_subquestion_field_names[names(question$Subquestions)] %>% unlist()
        }
      }
    }
    
    # DisplayLogic "Description" summarizes display logic, including the text of
    # the conditioning question. We don't want to detect changes in conditioning
    # question text if the QID stayed the same, so keep only fields not named
    # "Description".
    if ("DisplayLogic" %in% names(question)) {
      display_logic <- unlist(question$DisplayLogic)
      question$DisplayLogic <- sort(display_logic[!str_detect(names(display_logic), "Description")])
    }
    if ("InPageDisplayLogic" %in% names(question)) {
      display_logic <- unlist(question$InPageDisplayLogic)
      question$DisplayLogic <- sort(display_logic[!str_detect(names(display_logic), "Description")])
    }
    
    
    # Deduplicate some UMD items.
    if (survey_version == "UMD") {
      question$DataExportTag <- case_when(
        question$DataExportTag == "D2_30" & question$QuestionID == "QID294" ~ "D2_30_cheer",
        question$DataExportTag == "D2_30" & question$QuestionID == "QID293" ~ "D2_30_calm",
        question$DataExportTag == "B13" & question$QuestionID == "QID253" ~ "B13_likert",
        question$DataExportTag == "B13" & question$QuestionID == "QID255" ~ "B13_profile",
        question$DataExportTag == "B14" & question$QuestionID == "QID254" ~ "B14_likert",
        question$DataExportTag == "B14" & question$QuestionID == "QID259" ~ "B14_profile",
        question$DataExportTag == "B12a" & question$QuestionID == "QID250" ~ "B12a_likert",
        question$DataExportTag == "B12a" & question$QuestionID == "QID258" ~ "B12a_profile",
        question$DataExportTag == "B12b" & question$QuestionID == "QID251" ~ "B12b_likert",
        question$DataExportTag == "B12b" & question$QuestionID == "QID257" ~ "B12b_profile",
        TRUE ~ question$DataExportTag
      )
    } else if (survey_version == "CMU") {
      if (wave == 10) {
        question$DataExportTag <- case_when(
          question$DataExportTag == "C6" ~ "C6a",
          question$DataExportTag == "C8" ~ "C8a",
          TRUE ~ question$DataExportTag
        )
      } else if (wave == 11) {
        if (question$DataExportTag == "E1") {
          names(question$Subquestions) <- c("E1_1", "E1_2", "E1_3", "E1_4")
        }
      }
    }
      
    questions_out <- safe_insert_question(questions_out, question)
    qid_item_map[[question$QuestionID]] <- question$DataExportTag
  }
  
  qid_item_map <- unlist(qid_item_map)
  shown_items <- qid_item_map[shown_items]
  
  return(list(questions=questions_out, shown_items=shown_items))
}

#' Insert new question data into list without overwriting item of the same name
#'
#' @param question_list named list storing a collection of questions
#' @param question named list storing data for a single trimmed question from `get_qsf_file`
#'
#' @return The modified questions_list object
safe_insert_question <- function(questions_list, question) {
  if ( !is.null(questions_list[[question$DataExportTag]]) ) {
    already_seen_qid <- questions_list[[question$DataExportTag]]$QuestionID
    new_qid <- question$QuestionID
    
    stop(paste0("Multiple copies of item ", question$DataExportTag, " exist, ", already_seen_qid, " and ", new_qid))
  }
  
  questions_list[[question$DataExportTag]] <- question
  return(questions_list)
}

#' Compare the two surveys.
#'
#' @param old_qsf named list of trimmed output from `get_qsf_file` for older survey
#' @param new_qsf named list of trimmed output from `get_qsf_file` for newer survey
diff_surveys <- function(old_qsf, new_qsf) {
  ## Diff blocks
  old_shown_items <- old_qsf$shown_items
  new_shown_items <- new_qsf$shown_items
  old_questions <- old_qsf$questions
  new_questions <- new_qsf$questions
  
  added_qs <- setdiff(new_shown_items, old_shown_items)
  added <- rep(NA, length(added_qs))
  names(added) <- added_qs
  
  removed_qs <- setdiff(old_shown_items, new_shown_items)
  removed <- rep(NA, length(removed_qs))
  names(removed) <- removed_qs
  
  added_df <- create_diff_df(added, "Added", NULL, new_questions)
  removed_df <- create_diff_df(removed, "Removed", old_questions, NULL)
  
  ## For questions that appear in both surveys, check for changes in wording,
  ## display logic, and answer options.
  shared <- intersect(old_shown_items, new_shown_items)
  
  text_df <- diff_question(shared, "QuestionText", old_questions, new_questions)
  logic_df <- diff_question(shared, "DisplayLogic", old_questions, new_questions)
  choice_df <- diff_question(shared, "Choices", old_questions, new_questions)
  subq_df <- diff_question(shared, "Subquestions", old_questions, new_questions)
  
  out <- bind_rows(
    added_df, removed_df, text_df, logic_df, choice_df, subq_df
  )
  return(out)
}

#' Compare a single question field in the two surveys.
#'
#' @param names character vector of Qualtrics question IDs
#' @param change_type character; type of change to look for and name of question
#'   element to compare between survey versions
#' @param old_qsf named list of trimmed output from `get_qsf_file` for older
#'   survey
#' @param new_qsf named list of trimmed output from `get_qsf_file` for newer
#'   survey
diff_question <- function(names, change_type=c("Choices", "QuestionText",
                                               "DisplayLogic", "Subquestions"),
                          old_qsf, new_qsf) {
  change_type <- match.arg(change_type)
  
  changed <- list()
  for (question in names) {
    if ( !identical(old_qsf[[question]][[change_type]], new_qsf[[question]][[change_type]]) ) {
      changed_subquestions <- c()
      if (change_type == "Subquestions") {
        subquestion_codes <- unique(
          c(
            names(old_qsf[[question]][[change_type]]),
            names(new_qsf[[question]][[change_type]])
          )
        )

        for (code in subquestion_codes) {
          if ( !identical(old_qsf[[question]][[change_type]][[code]], new_qsf[[question]][[change_type]][[code]]) ) {
            changed_subquestions <- append(changed_subquestions, code)
          }
        }
        changed_subquestions <- paste(changed_subquestions, collapse=", ")
      }
      
      if (length(changed_subquestions) == 0 || identical(changed_subquestions, "")) {
        changed_subquestions <- NA
      }
      changed[[question]] <- changed_subquestions 
    }
  }
  out <- create_diff_df(unlist(changed), change_type, old_qsf, new_qsf)
  
  return(out)
}

#' Print results with custom message for each possible change type.
#'
#' @param questions character vector of Qualtrics question IDs for items that
#'   changed between survey versions
#' @param change_type character; type of change to look for
#' @param old_qsf named list of trimmed output from `get_qsf_file` for older
#'   survey
#' @param new_qsf named list of trimmed output from `get_qsf_file` for newer
#'   survey
create_diff_df <- function(questions, change_type=c("Added", "Removed",
                                                    "Choices", "QuestionText",
                                                    "DisplayLogic", "Subquestions"),
                           old_qsf, new_qsf) {
  out <- data.frame()
  
  if ( length(questions) > 0 ) {
    change_type <- match.arg(change_type)
    
    change_descriptions <- list(
      Added = "Item added",
      Removed = "Item removed",
      QuestionText = "Question wording changed",
      DisplayLogic = "Display logic changed",
      Choices = "Answer choices changed",
      Subquestions = "Matrix subquestions changed"
    )    

    if (!is.null(old_qsf)) {
      old_qids <- sapply(names(questions), function(question) { old_qsf[[question]]$QuestionID })
    } else {
      old_qids <- NA
    }
    if (!is.null(new_qsf)) {
      new_qids <- sapply(names(questions), function(question) { new_qsf[[question]]$QuestionID })
    } else {
      new_qids <- NA
    }
    
    out <- data.frame(
      change_type=change_descriptions[[change_type]],
      item=names(questions),
      old_qid=old_qids,
      new_qid=new_qids,
      impacted_subquestions=questions
    ) %>%
    arrange(item)
  }
  
  return(out)
}



args <- commandArgs(TRUE)

if (!(length(args) %in% c(3, 4))) {
  stop("Usage: Rscript qsf-differ.R UMD|CMU path/to/old/qsf path/to/new/qsf [path/to/desired/output-dir]")
}

survey_version <- args[1]
old_qsf <- args[2]
new_qsf <- args[3]

if (length(args) == 4) {
  output_dir <- args[4]
} else {
  output_dir <- "."
}


invisible(diff_qsf_files(old_qsf, new_qsf, output_dir, survey_version))
