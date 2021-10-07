#' Get list of QIDs of items that were shown to respondents
#'
#' Any item not in the "Trash" block of the survey is considered shown to
#' respondents.
#'
#' @param qsf list containing QSF data
#'
#' @return list of Qualtrics Question IDs (QIDs) of items shown to respondents
get_shown_items <- function(qsf) {
  block_out <- Filter(function(elem) { elem[["Element"]] == "BL" }, qsf$SurveyElements)[[1]]$Payload
  
  shown_items <- list()
  for (block in block_out) {
    if (block$Type == "Trash") {
      next
    }
    
    shown_items[[block$Description]] <- sapply(
      block$BlockElements, function(elem) {
        if (elem$Type == "Question") { elem$QuestionID }
      })
  }
  shown_items <- unlist(shown_items)
  
  return(shown_items)
}


get_block_item_map <- function(qsf) {
  block_out <- Filter(function(elem) { elem[["Element"]] == "BL" }, qsf$SurveyElements)[[1]]$Payload
  
  items <- list()
  for (block in block_out) {
    if (block$Type == "Trash") {
      next
    }
    
    items[[block$ID]] <- tibble(BlockID = block$ID, BlockName = block$Description, Questions = sapply(
      block$BlockElements, function(elem) {
        if (elem$Type == "Question") { elem$QuestionID }
      }) %>% unlist())
  }
  
  return(bind_rows(items))
}


#' Get wave number from qsf filename
#' 
#' Wave number as provided in the qsf name should be an integer or a float with
#' one decimal place.
#'
#' @param path_to_qsf
#'
#' @return (mostly) integer wave number
get_wave <- function(path_to_qsf) {
  qsf_name_pattern <- "(.*Wave_)([0-9]*([.][0-9])?)([.]qsf)$"
  if (!grepl(qsf_name_pattern, path_to_qsf)) {
    stop("qsf filename should be of the format 'Survey_of_COVID-Like_Illness_-_Wave_XX.qsf'")
  }
  
  wave <- as.numeric(
    sub(qsf_name_pattern, "\\2", path_to_qsf)
  ) 
  
  return(wave)
}
