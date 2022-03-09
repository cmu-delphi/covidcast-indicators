#!/usr/bin/env Rscript

## Diff two .qsf files to find new and changed survey items
##
## Usage:
##
## Rscript qsf-differ.R path/to/old/qsf path/to/new/qsf
##
## Writes the lists of new and changed items to STDOUT, so redirect STDOUT to
## your desired location.

options(warn = 1)

suppressPackageStartupMessages({
  library(jsonlite)
  library(stringr)
  library(dplyr)
  library(readr)
  source("qsf-utils.R")
})

#' Load and diff chosen qsf files.
#'
#' @param old_qsf_path path to older Qualtrics survey file in .qsf format
#' @param new_qsf_path path to newer Qualtrics survey file in .qsf format
diff_qsf_files <- function(old_qsf_path, new_qsf_path) {
  old_qsf <- get_qsf_file(old_qsf_path)
  new_qsf <- get_qsf_file(new_qsf_path)
  
  old_wave <- get_wave(old_qsf_path)
  new_wave <- get_wave(new_qsf_path)
  
  out <- diff_surveys(old_qsf, new_qsf) %>%
    mutate(
      old_wave = old_wave, new_wave = new_wave
    ) %>% 
    select(new_wave, old_wave, everything())
  write_csv(
    out,
    paste0("diff_", old_wave, "-", new_wave, ".csv", collapse="")
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
get_qsf_file <- function(path,
                         keep_items = c("QuestionID", "DataExportTag",
                                        "QuestionText", "QuestionType",
                                        "Choices", "Answers", "DisplayLogic")
) {
  # Read file as json.
  qsf <- read_json(path)
  
  ## Block
  shown_items <- get_shown_items(qsf)
  
  ## Questions
  questions <- Filter(function(elem) { elem[["Element"]] == "SQ" }, qsf$SurveyElements)
  
  qid_item_map <- list()
  questions_out <- list()
  for (question in questions) {
    question <- question$Payload
    
    # Skip items not shown to respondents.
    if ( !(question$QuestionID %in% shown_items) ) {
      next
    }
    
    if (!identical(keep_items, c("all"))) {
      question <- question[names(question) %in% c("QuestionID", keep_items)]
    }
    
    # Rearrange Answers/Choices elements to be consistent between matrix and
    # other items.
    if ("QuestionType" %in% names(question)) {
      if (question$QuestionType == "Matrix") {
        question$Subquestions <- question$Choices
        question$Choices <- question$Answers
        question$Answers <- NULL
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
  
  added <- setdiff(new_shown_items, old_shown_items)
  removed <- setdiff(old_shown_items, new_shown_items)
  
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
  
  changed <- c()
  for (question in names) {
    if ( !identical(old_qsf[[question]][[change_type]], new_qsf[[question]][[change_type]]) ) {
      changed <- append(changed, question)
    }
  }
  out <- create_diff_df(changed, change_type, old_qsf, new_qsf)
  
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
    questions <- sort(questions)

    if (!is.null(old_qsf, new_qsf {
      old_qids <- sapply(questions, function(question) { old_qsf, new_qsfquestion]]$QuestionID })
    } else {
      old_qids <- NA
    }
    if (!is.null(new_qsf)) {
      new_qids <- sapply(questions, function(question) { new_qsf[[question]]$QuestionID })
    } else {
      new_qids <- NA
    }
    
    out <- data.frame(
      change_type=change_descriptions[[change_type]],
      item=questions,
      old_qid=old_qids,
      new_qid=new_qids
    )
  }
  
  return(out)
}



args <- commandArgs(TRUE)

if (length(args) != 2) {
  stop("Usage: Rscript qsf-differ.R path/to/old/qsf path/to/new/qsf")
}

old_qsf <- args[1]
new_qsf <- args[2]

invisible(diff_qsf_files(old_qsf, new_qsf))
