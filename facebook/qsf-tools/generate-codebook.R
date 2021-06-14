#!/usr/bin/env Rscript

## Add information from one .qsf translation file at a time to the survey codebook
##
## Usage:
##
## Rscript generate-codebook.R path/to/qsf path/to/codebook

suppressPackageStartupMessages({
  library(tidyverse)
  library(jsonlite)
  library(stringr)
  source("qsf-utils.R")
})


process_qsf <- function(path_to_qsf) {
  q <- read_json(path_to_qsf)
  
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
  randomization <- case_when(
      raw_random_type == "All" ~ "randomized",
      raw_random_type == "Advanced" & map(raw_random_all, length) > 0 ~ "randomized",
      raw_random_type == "ScaleReversal" ~ "scale reversal",
      raw_scale_reversal == TRUE ~ "scale reversal",
      TRUE ~ NA_character_
    )
  
  qdf <- tibble(item = items,
                question = questions,
                type = qtype,
                choices = choices,
                answers = answers,
                randomization = randomization)

  stopifnot(length(qdf$item) == length(unique(qdf$item)))
  
  # Remove blank questions (rare).
  qdf <- qdf %>% filter(question != "Click to write the question text")
  
  # Add short question name mapped to item name
  short_name_map <- c(
    S1 = "consent",
    A1 = "hh symptoms",
    A2 = "hh num sick",
    A2b = "hh num total",
    A5 = "hh num",
    A3 = "zip code",
    A3b = "state",
    A4 = "community num sick",
    B2 = "symptoms",
    B2_14_TEXT = "symptoms other text",
    B2b = "unusual symptoms num days",
    B2c = "unusual symptoms",
    B2c_14_TEXT = "unusual symptoms other text",
    B3 = "taken temp 1d",
    B4 = "expectorate",
    B5 = "tested unusual symptoms",
    B6 = "hospital unusual symptoms 1d",
    B7 = "unusual symptoms medical care",
    B8 = "tested ever",
    B10 = "tested 14d",
    B10a = "positive test 14d",
    B10b = "reason tested 14d",
    B12 = "wanted test 14d",
    B12a = "reason not tested 14d",
    B11 = "positive test ever",
    C1 = "comorbidities",
    C2 = "flu shot 1y",
    C3 = "work outside home 5d",
    C4 = "work healthcare 5d",
    C5 = "nursing home 5d",
    C6 = "travel 5d",
    C7 = "avoid contact",
    C8 = "mental health 5d",
    C9 = "worried ill",
    C10 = "direct contact num 1d",
    C11 = "direct contact COVID 1d",
    C12 = "hh direct contact COVID 1d",
    C13 = "activities 1d",
    C13a = "activities mask 1d",
    C14 = "mask public 5d",
    C15 = "worried finances 1m",
    D1 = "gender",
    D1_4_TEXT = "gender other text",
    D1b = "pregnant",
    D2 = "age",
    D3 = "num children",
    D4 = "num adults",
    D5 = "num elderly",
    D6 = "hispanic",
    D7 = "race",
    D8 = "education",
    D9 = "work 1m",
    Q36 = "financial threat",
    Q40 = "fever temp",
    Q64 = "occupation",
    Q65 = "occupation social",
    Q66 = "occupation education",
    Q67 = "occupation arts",
    Q68 = "occupation healthcare practice",
    Q69 = "occupation healthcare support",
    Q70 = "occupation protective",
    Q71 = "occupation food",
    Q72 = "occupation building maintenance",
    Q73 = "occupation personal care",
    Q74 = "occupation sales",
    Q75 = "occupation office",
    Q76 = "occupation construction",
    Q77 = "occupation repair",
    Q78 = "occupation production",
    Q79 = "occupation transportation",
    Q80 = "occupation other",
    D10 = "work outside home 1m",
    C16 = "public others masked 7d",
    C17 = "flu vaccine since June 2020",
    E1 = "children",
    E2 = "children school",
    E3 = "children school measures",
    V1 = "vaccinated",
    V2 = "vaccine doses",
    V3 = "vaccine accepting",
    V4 = "trust vaccine recommendation source",
    V4a = "trust vaccine recommendation source",
    V9 = "worried side effects",
    C14a = "mask public 7d",
    C17a = "flu vaccine since July 1 2020",
    V2a = "get all vaccine doses",
    V5a = "vaccine hesitancy reasons prob yes",
    V5b = "vaccine hesitancy reasons prob no",
    V5c = "vaccine hesitancy reasons def no",
    V5d = "vaccine hesitancy reasons not all doses",
    V6 = "vaccine unnecessary reasons",
    D11 = "smoke",
    C6a = "travel 7d",
    C8a = "mental health 7d",
    C13b = "indoor activities 1d",
    C13c = "indoor activities mask 1d",
    V11 = "have vaccine appointment",
    V12 = "tried get vaccine appointment",
    V13 = "informed vaccine access",
    V14 = "when access vaccine",
    B10c = "positive test 14d",
    B13 = "COVID ever",
    C18a = "anxious 7d",
    C18b = "depressed 7d",
    C7a = "avoid contact 7d",
    D12 = "language",
    E4 = "vaccinate children",
    G1 = "worry catch COVID",
    G2 = "believe distancing",
    G3 = "believe masking",
    H1 = "public others distanced 7d",
    H2 = "public others masked 7d",
    H3 = "friends fam vaccinated",
    I1 = "believe stop masking",
    I2 = "believe children immune",
    I3 = "belive small group",
    I4 = "believe govt control",
    I5 = "news sources 7d",
    I6 = "trust COVID source",
    I7 = "want COVID info",
    K1 = "delay medical care 1y",
    K2 = "believe racial disrimination",
    V11a = "have vaccine appointment",
    V12a = "tried get vaccine",
    V15a = "vaccine access barriers vaccinated",
    V15b = "vaccine access barriers unvaccinated",
    V16 = "when try vaccinated",
    V3a = "vaccine accepting"
  )
  qdf <- qdf %>% 
    mutate(short_question = short_name_map[item])
  
  # matrix to separate items (to match exported data)
  # Question text and short-name have subquestion text appended
  nonmatrix_items <- qdf %>% filter(type != "Matrix")
  matrix_items <- qdf %>%
    filter(type == "Matrix") %>% 
    rowwise() %>% 
    mutate(new = list(
      tibble(item = paste(item, 1:length(choices), sep = "_"),
             question = question,
             matrix_subquestion = unlist(choices),
             type = type,
             randomization = ifelse(randomization == "randomized", NA_character_, randomization),
             short_question = short_question,
             choices = list(answers),
             answers = list(list()))
      )) %>% 
    select(new) %>% 
    unnest(new)
  
  # A5 and C10 are special cases b/c of they are matrix of text entry questions:
  # also C10 needs an extra _1.
  matrix_items <- matrix_items %>% 
    mutate(item = if_else(str_starts(item, "C10"), paste0(item, "_1"), item),
           type = if_else(str_starts(item, "A5|C10"), "Text", type),
           choices = if_else(str_starts(item, "A5|C10"), list(list()), choices))

  qdf <- bind_rows(nonmatrix_items, matrix_items) %>% 
    select(-answers, -choices)
  
  # indicate which items have replaced old items. Format: new = old
  replaces_map <- c(
    B8 = "B5",
    B9 = "B5",
    B10 = "B5",
    B11 = "B5",
    B12 = "B5",
    B7 = "B6",
    C13 = "C3",
    D9 = "C3",
    Q64 = "C4",
    Q68 = "C4",
    Q69 = "C4",
    Q64 = "C5",
    Q68 = "C5",
    Q69 = "C5",
    C13 = "C7",
    C13a = "C7",
    C14 = "C7",
    C6a = "C6",
    C8a_1 = "C8_1",
    C8a_2 = "C8_2",
    C8a_3 = "C8_3",
    C15a = "C8a_1",
    C15b = "C8a_2",
    C15 = "Q36",
    C13b = "C13",
    C13c = "C13a",
    C14a = "C14",
    C17a = "C17",
    V2a = "V2",
    V3a = "V3",
    V4a_1 = "V4_1",
    V4a_2 = "V4_2",
    V4a_3 = "V4_3",
    V4a_4 = "V4_4",
    V4a_5 = "V4_5",
    V11a = "V11",
    V12a = "V12",
    C7a = "C7"
  )
  qdf <- qdf %>%
    mutate(replaces = ifelse(
      item %in% names(replaces_map), 
      replaces_map[item], 
      NA_character_),
      wave = get_wave(path_to_qsf)
    ) %>% 
    relocate(replaces, short_question, .after = item) %>%
    relocate(matrix_subquestion, .after = question) %>% 
    relocate(wave, everything())
  
  # Quality checks
  stopifnot(length(qdf$item) == length(unique(qdf$item)))
  
  if (any(is.na(qdf$short_question))) {
    nonlabelled_items <- qdf$item[is.na(qdf$short_question)]
    stop(sprintf("items %s do not have a short name assigned",
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
add_qdf_to_codebook <- function(qdf, path_to_codebook) {
  if (!file.exists(path_to_codebook)) {
    return(qdf)
  }
  
  codebook <- read_csv(path_to_codebook, col_types = cols(
    wave = col_integer(),
    item = col_character(),
    replaces = col_character(),
    short_question = col_character(),
    question = col_character(),
    matrix_subquestion = col_character(),
    type = col_character(),
    randomization = col_character()
  ))
  
  qdf_wave <- unique(qdf$wave)
  if (qdf_wave %in% codebook$wave) {
    warning(sprintf("wave %s already added to codebook. removing existing rows and replacing with newer data", qdf_wave))
    codebook <- codebook %>% filter(wave != qdf_wave)
  }
  
  # Using rbind here to raise an error if columns differ between the existing
  # codebook and the new wave data.
  codebook <- rbind(codebook, qdf) %>% arrange(item, wave)
  
  ii_replacing_DNE <- which( !(codebook$replaces %in% codebook$item) )
  if ( length(ii_replacing_DNE) > 0 ) {
    replacing_items <- unique( codebook$item[ii_replacing_DNE] )
    warning(sprintf("the items that %s report replacing do not exist in the codebook",
                 paste(replacing_items, collapse=", "))
    )
  }
  return(codebook)
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
  write_csv(codebook, path_to_codebook)
}



args <- commandArgs(TRUE)

if (length(args) != 2) {
  stop("Usage: Rscript generate-codebook.R path/to/qsf path/to/codebook")
}

path_to_qsf <- args[1]
path_to_codebook <- args[2]

invisible(add_qsf_to_codebook(path_to_qsf, path_to_codebook))
