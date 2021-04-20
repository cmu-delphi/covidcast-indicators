#!/usr/bin/env Rscript

## Diff two .qsf translation files to find new and changed survey items
##
## Usage:
##
## Rscript translation-differ.R path/to/old/qsf path/to/new/qsf
##
## Writes the lists of new and changed items to STDOUT, so redirect STDOUT to
## your desired location and compress as desired.


#### Background on .qsf files
## Keep only useful fields (item number and question text). We are only
## interested in "SQ" (survey questions) and "BL" (block name + contained
## questions -- ordered?).
# SurveyElement types are BL (block), FL (flow), SO (survey options), SCO
# (scoring), PROJ (?), notes, STAT (statistics), QC (question count), SQ
# (survey questions), and RS (response set). Detailed info:
# https://gist.github.com/ctesta01/d4255959dace01431fb90618d1e8c241.


# Payload (list) elements:
# [1] "QuestionText"         "QuestionType"         "Selector"             "Configuration"        "QuestionDescription"  "Choices"              "ChoiceOrder"         
# [8] "Validation"           "AnalyzeChoices"       "Language"             "QuestionText_Unsafe"  "DataExportTag"        "QuestionID"           "DataVisibility"      
# [15] "NextChoiceId"         "NextAnswerId"         "DefaultChoices"       "SubSelector"          "DisplayLogic"         "GradingData"          "Answers"             
# [22] "AnswerOrder"          "ChoiceDataExportTags" "Randomization"        "RecodeValues"         "DynamicChoices"       "DynamicChoicesData"   "SearchSource"        
# [29] "QuestionJS"  



suppressPackageStartupMessages({
  library(dplyr)
  library(jsonlite)
})

#' Diff chosen translation files.
#'
#' @param old_qsf_path path to older Qualtrics translation file in .qsf format
#' @param new_qsf_path path to newer Qualtrics translation file in .qsf format
#'
#' @return A named list containing new and changed item names
diff_qsf_files <- function(old_qsf_path, new_qsf_path) {
  old_qsf <- get_qsf_file(old_qsf_path)
  new_qsf <- get_qsf_file(new_qsf_path)
  
  diff_questions(old_qsf, new_qsf)
  
  return(NULL)
}

## Fetch and format a single translation file.
get_qsf_file <- function(path) {
  # Read file as json.
  qsf <- read_json(path, simplifyVector = TRUE)
  
  questions <- filter(qsf$SurveyElements, Element == "SQ")
  keep_items <- c("QuestionID", "DataExportTag", "QuestionText", "QuestionType", "Choices", "Answers", "DisplayLogic")
  
  questions_out <- list()
  for (question in questions$Payload) {
    question <- question[names(question) %in% keep_items]
    
    subquestion <- names(question$Choices)
    question$Choices <- unlist(question$Choices)
    names(question$Choices) <- subquestion
    
    response_codes <- names(question$Answers)
    question$Answers <- unlist(question$Answers)
    names(question$Answers) <- response_codes
    
    if (question$QuestionType == "Matrix") {
      question$Subquestions <- question$Choices
      question$Choices <- question$Answers
      question$Answers <- NULL
    } else {
      question$Subquestions <- "NULL"
    }
    
    question$DisplayLogic <- ifelse(is.null(question$DisplayLogic$`0`$`0`$Description), "NULL", question$DisplayLogic$`0`$`0`$Description)
    
    questions_out[[question$QuestionID]] <- question
  }
  
  return(questions_out)
}

## Compare old vs new questions.
diff_questions <- function(old_qsf, new_qsf) {
  old_names <- names(old_qsf)
  new_names <- names(new_qsf)
  
  added <- setdiff(new_names, old_names)
  print_questions(added, "added", new_qsf)
  
  removed <- setdiff(old_names, new_names)
  print_questions(removed, "removed", old_qsf)
  
  ## For questions that appear in both surveys, check for changes in wording,
  ## display logic, and answer options.
  shared <- intersect(old_names, new_names)
  
  # Wording
  diff_question(shared, "wording", "QuestionText", old_qsf, new_qsf)
  
  # Display logic
  diff_question(shared, "display logic", "DisplayLogic", old_qsf, new_qsf)
  
  # Answer choices
  diff_question(shared, "answers", "Choices", old_qsf, new_qsf)
  
  # Matrix sub-questions
  diff_question(shared, "subquestions", "Subquestions", old_qsf, new_qsf)
  
  return(NULL)
}

## Compare a single question field.
diff_question <- function(names, change_type=c("answers", "wording", "display logic", "subquestions"), comparator, old_qsf, new_qsf) {
  change_type <- match.arg(change_type)
  
  changed <- c()
  for (question in names) {
    if ( !identical(old_qsf[[question]][[comparator]], new_qsf[[question]][[comparator]]) ) {
      changed <- append(changed, question)
    }
  }
  print_questions(changed, change_type, new_qsf)
  
  return(NULL)
}

## Print results with custom message.
print_questions <- function(questions, change_type=c("added", "removed", "answers", "wording", "display logic", "subquestions"), reference_qsf) {
  if ( length(questions) > 0 ) {
    change_type <- match.arg(change_type)
    qids <- sort(questions)
    item <- sapply(qids, function(question) {reference_qsf[[question]]$DataExportTag})
    item_text <- sapply(qids, function(question) {reference_qsf[[question]]$QuestionText})
    
    if (change_type == "added") {
      cat(paste0("Added: item ", item, " (", qids, ") asking '", item_text, "'.\n"))
    } else if (change_type == "removed") {
      cat(paste0("Removed: item ", item, " (", qids, ") asking '", item_text, "'.\n"))
    } else if (change_type == "wording") {
      cat(paste0("Wording changed: item ", item, " (", qids, ") asking '", item_text, "'.\n"))
    } else if (change_type == "display logic") {
      cat(paste0("Display logic changed: item ", item, " (", qids, ") asking '", item_text, "'.\n"))
    } else if (change_type == "answers") {
      cat(paste0("Answer choices changed: item ", item, " (", qids, ") asking '", item_text, "'.\n"))
    } else if (change_type == "subquestions") {
      cat(paste0("Subquestions changed: matrix item ", item, " (", qids, ") asking '", item_text, "'.\n"))
    }
  }
  return(NULL)
}



args <- commandArgs(TRUE)

if (length(args) != 2) {
  stop("Usage: Rscript translation-differ.R path/to/old/qsf path/to/new/qsf")
}

old_qsf <- args[1]
new_qsf <- args[2]

invisible(diff_qsf_files(old_qsf, new_qsf))
