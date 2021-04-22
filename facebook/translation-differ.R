#!/usr/bin/env Rscript

## Diff two .qsf translation files to find new and changed survey items
##
## Usage:
##
## Rscript translation-differ.R path/to/old/qsf path/to/new/qsf
##
## Writes the lists of new and changed items to STDOUT, so redirect STDOUT to
## your desired location.


#### Background on .qsf files

## A .qsf file is a json containing two elements, SurveyEntry with survey
## metadata (start date, ID, name, etc) and SurveyElements with a numbered list
## of survey components. For the purpose of finding differences in survey
## contents, we are only interested in examining the SurveyElements item.

## SurveyElement types are BL (block), FL (flow), SO (survey options), SCO
## (scoring), PROJ (?), notes, STAT (statistics), QC (question count), SQ
## (survey questions), and RS (response set). Detailed info:
## https://gist.github.com/ctesta01/d4255959dace01431fb90618d1e8c241

## We are only interested in survey questions ("SQ"). Within each SQ item,
## details are stored in the Payload element, which can contain the following
## fields:

# [1] "QuestionText"         "QuestionType"         "Selector"             "Configuration"        "QuestionDescription"  "Choices"              "ChoiceOrder"         
# [8] "Validation"           "AnalyzeChoices"       "Language"             "QuestionText_Unsafe"  "DataExportTag"        "QuestionID"           "DataVisibility"      
# [15] "NextChoiceId"         "NextAnswerId"         "DefaultChoices"       "SubSelector"          "DisplayLogic"         "GradingData"          "Answers"             
# [22] "AnswerOrder"          "ChoiceDataExportTags" "Randomization"        "RecodeValues"         "DynamicChoices"       "DynamicChoicesData"   "SearchSource"        
# [29] "QuestionJS"  

## The meaning of "Answers" and "Choices" differs for matrix vs non-matrix
## items. "Choices" list the vertical components -- subquestions for matrix
## items and answer choices for non-matrix items. "Answers" list the answer
## choices for matrix items and are missing for non-matrix items.


suppressPackageStartupMessages({
  library(dplyr)
  library(jsonlite)
})

#' Diff chosen translation files.
#'
#' @param old_qsf_path path to older Qualtrics translation file in .qsf format
#' @param new_qsf_path path to newer Qualtrics translation file in .qsf format
diff_qsf_files <- function(old_qsf_path, new_qsf_path) {
  old_qsf <- get_qsf_file(old_qsf_path)
  new_qsf <- get_qsf_file(new_qsf_path)
  
  diff_questions(old_qsf, new_qsf)
  
  return(NULL)
}

#' Fetch and format a single .qsf translation file.
#'
#' @param path path to Qualtrics translation file in .qsf format
#'
#' @return A named list
get_qsf_file <- function(path) {
  # Read file as json.
  qsf <- read_json(path, simplifyVector = TRUE)
  
  questions <- filter(qsf$SurveyElements, Element == "SQ")
  keep_items <- c("QuestionID", "DataExportTag", "QuestionText",
                  "QuestionType", "Choices", "Answers", "DisplayLogic")
  
  questions_out <- list()
  for (question in questions$Payload) {
    question <- question[names(question) %in% keep_items]
    
    # These "questions" are not shown to respondents and thus don't need to be
    # accounted for in documentation or code. Skip.
    if (question$QuestionText == "Click to write the question text") {
      warning(paste0("Item ", question$DataExportTag, " (", question$QuestionID,
                     ") has question text '", question$QuestionText,
                     "' and appears to be a remnant of the survey creation process; please remove."
      ))
      next
    }
    
    # For matrix questions, "Choices" are the subquestion text and code. For all
    # other questions, "Choices" are the answer choices and corresponding
    # response code.
    subquestion <- names(question$Choices)
    question$Choices <- unlist(question$Choices)
    names(question$Choices) <- subquestion
    
    # For matrix questions, "Answers" are the answer choices and corresponding
    # response code. For all other questions, this is not provided.
    response_codes <- names(question$Answers)
    question$Answers <- unlist(question$Answers)
    names(question$Answers) <- response_codes
    
    # Rearrange items to be consistent between matrix and other items.
    if (question$QuestionType == "Matrix") {
      question$Subquestions <- question$Choices
      question$Choices <- question$Answers
      question$Answers <- NULL
    } else {
      question$Subquestions <- "NULL"
    }
    
    # DisplayLogic "Description" summarizes display logic (e.g. if item is shown
    # conditional on another question), including the conditioning question
    # text, comparator, and comparison value.
    question$DisplayLogic <- ifelse(
      is.null(question$DisplayLogic$`0`$`0`$Description),
      "NULL",
      question$DisplayLogic$`0`$`0`$Description
      )
    
    questions_out <- safe_insert_question(questions_out, question)
  }
  
  return(questions_out)
}

#' Insert new question data into list without overwriting item of the same name
#'
#' @param question_list named list storing a collection of questions
#' @param question named list storing data for a single trimmed question from `get_qsf_file`
#'
#' @return A named list
safe_insert_question <- function(questions_list, question) {
  if ( is.null(questions_list[[question$DataExportTag]]) ) {
    questions_list[[question$DataExportTag]] <- question
  } else {
    old_qid <- questions_list[[question$DataExportTag]]$QuestionID
    new_qid <- question$QuestionID
    
    warning(paste0("Multiple copies of item ", question$DataExportTag, " exist; using the newer of ", old_qid, " and ", new_qid))
    
    old_qid <- qid_to_int(questions_list[[question$DataExportTag]]$QuestionID)
    new_qid <- qid_to_int(question$QuestionID)
    
    if (old_qid < new_qid) {
      questions_list[[question$DataExportTag]] <- question
    }
  }
  return(questions_list)
}

#' Convert QID to integer
#'
#' @param qid_str
#'
#' @return An integer
qid_to_int <- function(qid_str) {
  qid_int <- as.integer(sub("QID", "", qid_str))
  qid_int <- ifelse(is.na(qid_int), 0, qid_int)
  return(qid_int)
}

#' Compare old vs new questions in the two surveys.
#'
#' @param old_qsf trimmed output from `get_qsf_file` for older survey
#' @param new_qsf trimmed output from `get_qsf_file` for newer survey
diff_questions <- function(old_qsf, new_qsf) {
  old_names <- names(old_qsf)
  new_names <- names(new_qsf)
  browser()
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

#' Compare a single question field in the two surveys.
#'
#' @param names character vector of Qualtrics question IDs
#' @param change_type character; type of change to look for
#' @param comparator character; name of question element to compare between
#'   survey versions
#' @param old_qsf trimmed output from `get_qsf_file` for older survey
#' @param new_qsf trimmed output from `get_qsf_file` for newer survey
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

#' Print results with custom message for each possible change type.
#'
#' @param questions character vector of Qualtrics question IDs for items that
#'   changed between survey versions
#' @param change_type character; type of change to look for
#' @param reference_qsf trimmed output from `get_qsf_file` for survey that
#'   contains descriptive info about a particular type of change. For "removed"
#'   questions, should be older survey, else newer survey.
print_questions <- function(questions, change_type=c("added", "removed", "answers", "wording", "display logic", "subquestions"), reference_qsf) {
  if ( length(questions) > 0 ) {
    change_type <- match.arg(change_type)
    
    item <- sort(questions)
    qids <- sapply(item, function(question) {reference_qsf[[question]]$QuestionID})
    item_text <- sapply(item, function(question) {reference_qsf[[question]]$QuestionText})
    
    if (change_type == "added") {
      cat("\n ")
      cat(paste0("Added: item ", item, " (", qids, ")", "\n"))
    } else if (change_type == "removed") {
      cat("\n ")
      cat(paste0("Removed: item ", item, " (", qids, ")", "\n"))
    } else if (change_type == "wording") {
      cat("\n ")
      cat(paste0("Wording changed: item ", item, " (", qids, ")", "\n"))
    } else if (change_type == "display logic") {
      cat("\n ")
      cat(paste0("Display logic changed: item ", item, " (", qids, ")", "\n"))
    } else if (change_type == "answers") {
      cat("\n ")
      cat(paste0("Answer choices changed: item ", item, " (", qids, ")", "\n"))
    } else if (change_type == "subquestions") {
      cat("\n ")
      cat(paste0("Subquestions changed: matrix item ", item, " (", qids, ")", "\n"))
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

options(nwarnings = 10000)

invisible(diff_qsf_files(old_qsf, new_qsf))
cat("\nWarning messages:\n ")
cat(paste0(names(warnings()), "\n"))
