#' Get list of QIDs of items that were shown to respondents
#'
#' Any item not in the "Trash" block of the survey is considered shown to
#' respondents.
#'
#' @param qsf list containing QSF data
#'
#' @return list of Qualtrics Question IDs (QIDs) of items shown to respondents
get_shown_items <- function(qsf) {
  all_blocks <- Filter(function(elem) { elem[["Element"]] == "BL" }, qsf$SurveyElements)[[1]]$Payload
  
  shown_items <- list()
  for (block in all_blocks) {
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
  all_blocks <- Filter(function(elem) { elem[["Element"]] == "BL" }, qsf$SurveyElements)[[1]]$Payload
  
  items <- list()
  for (block in all_blocks) {
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
    stop("qsf filename should be of the format '<survey prefix>Wave_XX.qsf' where 'XX' is an integer or float")
  }
  
  wave <- as.numeric(
    sub(qsf_name_pattern, "\\2", path_to_qsf)
  ) 
  
  return(wave)
}

#' Create mapping of QIDs to module name
#'
#' @param qsf contents of QSF file in JSON format
#'
#' @return dataframe with `BlockName` (module name) and `Questions` (QIDs) columns
map_qids_to_module <- function(qsf) {
  # get the survey elements with flow logic (should be one per block randomization branch)
  ii_flow <- qsf$SurveyElements %>%
    map_chr("Element") %>%
    {. == "FL"} %>%
    which()
  ii_block_randomizer <- qsf$SurveyElements[ii_flow] %>%
    map(~ .x$Payload$Flow) %>%
    map(~ map(.x,~ .x$Type == "BlockRandomizer")) %>%
    unlist() %>% 
    which()
  random_block_ids <- qsf$SurveyElements[ii_flow] %>%
    map(~ .x$Payload$Flow) %>%
    map(~ .x[ii_block_randomizer]) %>% 
    map(~ map(.x,~ .x$Flow)) %>% 
    map(~ map(.x,~ map(.x,~ .x$ID))) %>%
    unlist()
  
  block_id_item_map <- get_block_item_map(qsf)
  block_id_item_map <- block_id_item_map %>% filter(BlockID %in% random_block_ids) %>%
    select(-BlockID)
  
  return(block_id_item_map)
}

#' Get only questions that were shown to respondents, using definition in `get_shown_items`
#'
#' @param qsf contents of QSF file in JSON format
#'
#' @return QSF subsetted to only displayed questions
subset_qsf_to_displayed <- function(qsf) {
  # get the survey elements that are questions:
  ii_questions <- qsf$SurveyElements %>% 
    map_chr("Element") %>%
    {. == "SQ"} %>% 
    which()
  
  # get the questions that were shown to respondents
  shown_items <- get_shown_items(qsf)
  ii_shown <- qsf$SurveyElements[ii_questions] %>% 
    map_chr(~ .x$Payload$QuestionID) %>%
    {. %in% shown_items} %>% 
    which()
  
  # subset qsf to valid elements
  displayed_questions <- qsf$SurveyElements[ii_questions][ii_shown]
  
  return(displayed_questions)
}

#' Replace erroneous question names
#'
#' @param item_names character vector of survey question names
#' @param wave integer or float survey version
#'
#' @return character vector of repaired survey question names
patch_item_names <- function(item_names, path_to_rename_map, wave) {
  if (file.exists(path_to_rename_map)){
    rename_map <- read_csv(path_to_rename_map, trim_ws = FALSE,
                           col_types = cols(old_item = col_character(),
                                            new_item = col_character(),
                                            in_wave = col_number() # integer or float
                           ))	%>%
      filter(is.na(in_wave) | in_wave == wave)
    replacement_names <- rename_map$new_item
    names(replacement_names) <- rename_map$old_item
    
    ii_to_replace <- item_names %in% names(replacement_names) %>% which()
    item_names[ii_to_replace] <- replacement_names[item_names[ii_to_replace]]
  }
  
  return(item_names)
}

#' Fetch and customize question format types.
#'
#' @param qsf contents of QSF file in JSON format
#' @param item_names character vector of survey question names
#' @param survey_version either "UMD" or "CMU"
#'
#' @return character vector of repaired survey question names
get_question_formats <- function(qsf, item_names, survey_version){
  type_map <- c(MC = "Multiple choice", TE = "Text", Matrix = "Matrix")
  
  qtype <- qsf %>%
    map_chr(~ .x$Payload$QuestionType) %>% 
    {type_map[.]}
  
  ii_multiselect <- qsf %>%
    map_chr(~ .x$Payload$Selector) %>%
    {. == "MAVR"} %>% 
    which()
  qtype[ii_multiselect] <- "Multiselect"
  
  if (survey_version == "CMU") {
    qtype[item_names == "A5"] <- "Matrix" # this will be treated like C10
  } else if (survey_version == "UMD") {
    # pass
  }
  
  return(qtype)
}

#' Construct filepath for UMD or CMU survey version
#'
#' @param filename name of file that can be found in the `static` dir
#' @param survey_version either "UMD" or "CMU"
#'
#' @return character vector of repaired survey question names
localize_static_filepath <- function(filename, survey_version){
  file.path(".", "static", survey_version, filename)
}
