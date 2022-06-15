#' Load all response datasets in a local directory
#'
#' Note that if some columns are not present in all files -- for example, if
#' survey questions changed and so newer data files have different columns --
#' the resulting data frame will contain all columns, with NAs in rows where
#' that column was not present.
#'
#' @param params a named listed containing a value named "input", a vector of
#'   paths to load by the function, and "input_dir", the directory where the
#'   input files are found
#' @param contingency_run boolean indicating if currently running contingency
#'   code
#' @return A data frame of all loaded data files concatenated into one data
#'   frame
#'
#' @importFrom dplyr bind_rows
#' @importFrom parallel mclapply
#' @export
load_responses_all <- function(params, contingency_run = FALSE) {
  msg_plain(paste0("Loading ", length(params$input), " CSVs"))

  map_fn <- if (params$parallel) { mclapply } else { lapply }
  input_data <- map_fn(seq_along(params$input), function(i) {
    load_response_one(params$input[i], params, contingency_run)
  })
  
  msg_plain(paste0("Finished loading CSVs"))
  
  which_errors <- unlist(lapply(input_data, inherits, "try-error"))
  if (any( which_errors )) {
    errored_filenames <- paste(params$input[which_errors], collapse=", ")
    stop(
      "ingestion and field creation failed for at least one of input data file(s) ",
      errored_filenames,
      " with error(s)\n",
      unique(input_data[which_errors])
    )
  }
  
  input_data <- bind_rows(input_data)
  msg_plain(paste0("Finished combining CSVs"))
  return(input_data)
}

#' Load a single set of responses
#'
#' @param input_filename  filename of the input CSV file
#' @param params          a named list containing a value named "input_dir", the directory
#'                        where the input file are found
#' @param contingency_run boolean indicating if currently running contingency
#'   code
#'
#' @importFrom stringi stri_split stri_extract stri_replace_all stri_replace
#' @importFrom readr read_lines cols locale col_character col_integer
#' @importFrom dplyr arrange desc case_when mutate if_else
#' @importFrom lubridate force_tz with_tz
#' @importFrom rlang .data
#' @export
load_response_one <- function(input_filename, params, contingency_run) {
  msg_plain(paste0("Reading ", input_filename))
  # read the input data; need to deal with column names manually because of header
  full_path <- file.path(params$input_dir, input_filename)

  # Qualtrics provides a row of JSON-encoded metadata entries. Extract the
  # timezone entry.
  meta_data <- read_csv(full_path, skip = 2L, n_max = 1L, col_names = FALSE)$X1
  tz_from <- stri_extract(meta_data, regex = "[a-zA-Z_]+/[a-zA-Z_]+")

  col_names <- stri_split(read_lines(full_path, n_max = 1L), fixed = ",")[[1]]
  col_names <- stri_replace_all(col_names, "", fixed = "\"")
  
  ## The CSVs have some columns with column-separated fields showing which of
  ## multiple options a user selected; readr would interpret these as thousand
  ## separators by default, so we tell it that no thousands separators are used.
  ## Sometimes readr gets confused and records the "Other" columns (*_TEXT) as
  ## logicals or multiple-choice columns as numeric, so we tell it that those
  ## are always character data.
  input_data <- read_csv(full_path, skip = 3L, col_names = col_names,
                         col_types = cols(
                           A2 = col_character(),
                           A3 = col_character(),
                           B2 = col_character(),
                           B2_14_TEXT = col_character(),
                           B2c = col_character(),
                           B2c_14_TEXT = col_character(),
                           B7 = col_character(),
                           B10b = col_character(),
                           B12a = col_character(),
                           C1 = col_character(),
                           C13 = col_character(),
                           C13a = col_character(),
                           C13b = col_character(),
                           C13c = col_character(),
                           D1_4_TEXT = col_character(),
                           D1b = col_integer(),
                           D7 = col_character(),
                           D11 = col_integer(),
                           E3 = col_character(),
                           Q_TerminateFlag = col_character(),
                           V1 = col_integer(),
                           V2 = col_integer(),
                           V2a = col_integer(),
                           V3 = col_integer(),
                           V4_1 = col_integer(),
                           V4_2 = col_integer(),
                           V4_3 = col_integer(),
                           V4_4 = col_integer(),
                           V4_5 = col_integer(),
                           V4a_1 = col_integer(),
                           V4a_2 = col_integer(),
                           V4a_3 = col_integer(),
                           V4a_4 = col_integer(),
                           V4a_5 = col_integer(),
                           V5a = col_character(),
                           V5b = col_character(),
                           V5c = col_character(),
                           V5d = col_character(),
                           V6 = col_character(),
                           V9 = col_integer(),
                           V11 = col_integer(),
                           V12 = col_integer(),
                           V13 = col_integer(),
                           V14_1 = col_character(),
                           V14_2 = col_character(),
                           V15a = col_character(),
                           V15b = col_character(),
                           Q65 = col_integer(),
                           Q66 = col_integer(),
                           Q67 = col_integer(),
                           Q68 = col_integer(),
                           Q69 = col_integer(),
                           Q70 = col_integer(),
                           Q71 = col_integer(),
                           Q72 = col_integer(),
                           Q73 = col_integer(),
                           Q74 = col_integer(),
                           Q75 = col_integer(),
                           Q76 = col_integer(),
                           Q77 = col_integer(),
                           Q78 = col_integer(),
                           Q79 = col_integer(),
                           Q80 = col_integer(),
                           I5 = col_character(),
                           I7 = col_character(),
                           V1alt = col_character(),
                           V15c = col_character(),
                           P6 = col_character(),
                           E2_1 = col_integer(),
                           E2_2 = col_integer()
                         ),
                         locale = locale(grouping_mark = ""))
  if (nrow(input_data) == 0) {
    return(tibble())
  }
  
  input_data <- arrange(input_data, desc(.data$StartDate))
  if (!("SurveyID" %in% names(input_data))) {
    # The first wave files didn't actually record their survey id
    input_data$SurveyID <- "SV_8zYl1sFN6fAFIxv"
  }

  # Occasionally we get single responses with no SurveyID, which prevents us
  # from knowing their wave. Discard.
  input_data <- filter(input_data, !is.na(.data$SurveyID))

  # Convert A2 to integer, keeping only responses that are integers or have a
  # single value-less decimal place ("xx.0")
  input_data <- mutate(input_data,
                       A2 = if_else(grepl("^[0-9]+[.]?0?$", .data$A2),
                                    as.integer(.data$A2),
                                    NA_integer_))

  input_data$wave <- surveyID_to_wave(input_data$SurveyID)
  input_data$zip5 <- input_data$A3
  
  wave <- unique(input_data$wave)
  assert(length(wave) == 1, "can only code one wave at a time")
  
  input_data <- module_assignment(input_data, wave)
  input_data <- experimental_arm_assignment(input_data, wave)
  
  input_data <- bodge_v4_translation(input_data, wave)
  input_data <- bodge_C6_C8(input_data, wave)
  input_data <- bodge_B13(input_data, wave)
  input_data <- bodge_E1(input_data, wave)
  input_data <- bodge_V2a(input_data, wave)

  input_data <- code_symptoms(input_data, wave)
  input_data <- code_hh_size(input_data, wave)
  input_data <- code_mental_health(input_data, wave)
  input_data <- code_mask_contact(input_data, wave)
  input_data <- code_testing(input_data, wave)
  input_data <- code_activities(input_data, wave)
  input_data <- code_vaccines(input_data, wave)
  input_data <- code_schooling(input_data, wave)
  input_data <- code_children(input_data, wave)
  input_data <- code_beliefs(input_data, wave)
  input_data <- code_news_and_info(input_data, wave)
  input_data <- code_gender(input_data, wave)
  input_data <- code_age(input_data, wave)
  
  if (!is.null(params$produce_individual_raceeth) && params$produce_individual_raceeth) {
    input_data <- code_race_ethnicity(input_data, wave)
  }

  # create testing variables

  # When a token begins with a hyphen, Qualtrics CSVs contain a lone single
  # quote in front, for some mysterious reason. Strip these from the token,
  # since they are erroneous and won't match with CIDs in weights files.
  input_data$token <- stri_replace(input_data$token, "-", regex = "^'-")

  # clean date time data, forcing to be in the "America/Los_Angeles" timezone
  input_data$start_dt <- force_tz(input_data$StartDate, tz_from)
  input_data$start_dt <- with_tz(input_data$start_dt, tz_to)
  input_data$date <- format(input_data$start_dt, "%Y-%m-%d", tz = tz_to)
  input_data$end_dt <- force_tz(input_data$EndDate, tz_from)
  input_data$end_dt <- with_tz(input_data$end_dt, tz_to)

  # clean the ZIP data
  input_data$zip5 <- stri_replace_all(input_data$zip5, "", regex = " *")
  input_data$zip5 <- stri_replace(input_data$zip5, "", regex ="-.*")

  # some people enter 9-digit ZIPs, which could make them easily identifiable in
  # the individual output files. rather than truncating to 5 digits -- which may
  # turn nonsense entered by some respondents into a valid ZIP5 -- we simply
  # replace these ZIPs with NA.
  input_data$zip5 <- ifelse(nchar(input_data$zip5) > 5, NA_character_,
                            input_data$zip5)
  
  if (contingency_run) {
    ## Create additional fields for aggregations.
    # Demographic grouping variables
    input_data <- code_race_ethnicity(input_data, wave)
    input_data <- code_occupation(input_data, wave)
    input_data <- code_education(input_data, wave)
    input_data <- code_vaccinated_breakdown(input_data, wave)
    
    # Indicators
    input_data <- code_addl_vaccines(input_data, wave)
    input_data <- code_attempt_vaccine(input_data, wave)
    input_data <- code_addl_symptoms(input_data, wave)
    input_data <- code_health(input_data, wave)
    input_data <- code_trust(input_data, wave)
    input_data <- code_vaccine_barriers(input_data, wave)
    input_data <- code_behaviors(input_data, wave)
    input_data <- code_addl_activities(input_data, wave)
  }

  return(input_data)
}

#' Filter responses for privacy and validity
#'
#' @param input_data data frame containing response data
#' @param params named list containing values "static_dir", "start_time", and
#'   "end_time"
#'
#' @importFrom dplyr anti_join filter
#' @importFrom rlang .data
#' @export
filter_responses <- function(input_data, params) {
  msg_plain(paste0("Filtering data..."))
  input_data <- arrange(input_data, .data$StartDate)
  
  ## Remove invalid, duplicated, and out-of-range observations.
  # Take only the first instance of each token.
  # Take the right dates. We don't filter the start date because the aggregate
  # and individual data pipelines handle that themselves (aggregate in
  # particular needs data well before start_date)
  input_data <- filter(input_data, 
                       .data$token != "", 
                       !duplicated(.data$token), 
                       .data$S1 == 1, 
                       .data$DistributionChannel != "preview",
                       as.Date(.data$date) <= params$end_date
  )
  
  return(input_data)
}

#' Merge new data with archived data
#'
#' See the README for details on how the archive works and why it is necessary
#' to use the archive to check submitted tokens.
#'
#' @param input_data data frame containing the new response data
#' @param archive archive data read by `load_archive()`
#'
#' @return single data frame containing the merged data to be used for analysis
#' @importFrom dplyr bind_rows
#' @export
merge_responses <- function(input_data, archive) {
  msg_plain(paste0("Merging new and archived data..."))
  # First, merge the new data with the archived data, taking the first start
  # date for any given token. This allows for backfill. Note that the order
  # matters: since arrange() uses order(), which is a stable sort, ties will
  # result in the input data being used in preference over the archive data.
  # This means that if we run the pipeline, then change the input CSV, running
  # again will used the changed data instead of the archived data.
  data <- bind_rows(input_data, archive$input_data)
  msg_plain(paste0("Sorting by start date"))
  data <- arrange(data, .data$StartDate)

  msg_plain(paste0("Removing duplicated tokens"))
  data <- data[!duplicated(data$token), ]

  # Next, filter out responses with tokens that were seen before in responses
  # started before even the responses in `data`. These are responses submitted
  # recently with tokens that were initially used long ago, before the data
  # contained in `archive$input_data`.
  msg_plain(paste0("Join on seen tokens from archive"))
  if (!is.null(archive$seen_tokens)) {
    data <- left_join(data, archive$seen_tokens,
                      by = "token", suffix = c("", ".seen"))
    data <- data[is.na(data$start_dt.seen) | data$start_dt <= data$start_dt.seen, ]
  }

  msg_plain(paste0("Finished merging new and archived data"))
  return(data)
}

#' Create variables needed for aggregation
#'
#' @param input_data   the input data frame of (filtered) responses
#'
#' @export
create_data_for_aggregation <- function(input_data)
{
  msg_plain(paste0("Creating data for aggregations..."))
  df <- input_data
  df$weight_unif <- 1.0
  df$day <- as.Date(df$date)

  msg_plain(paste0("Creating variables for CLI and ILI signals"))
  # create variables for cli and ili signals
  hh_cols <- c("hh_fever", "hh_sore_throat", "hh_cough", "hh_short_breath", "hh_diff_breath")
  df$cnt_symptoms <- apply(df[,hh_cols], 1, sum, na.rm = TRUE)
  df$hh_number_sick[df$cnt_symptoms <= 0] <- 0
  df$is_cli <- df$hh_fever & (
    df$hh_cough | df$hh_short_breath | df$hh_diff_breath
  )
  df$is_cli[is.na(df$is_cli)] <- FALSE
  df$is_ili <- df$hh_fever & (df$hh_sore_throat | df$hh_cough)
  df$is_ili[is.na(df$is_ili)] <- FALSE
  df$hh_p_cli <- 100 * df$is_cli * df$hh_number_sick / df$hh_number_total
  df$hh_p_ili <- 100 * df$is_ili * df$hh_number_sick / df$hh_number_total

  ### Create variables for community survey.
  ## Question A4: how many people you know in the local community (not your
  ## household) with CLI
  msg_plain(paste0("Creating variables for community signals"))
  df$community_yes <- as.numeric(as.numeric(df$A4) > 0)

  ## Whether you know someone in your local community *or* household who is
  ## sick.
  df$hh_community_yes <- as.numeric(as.numeric(df$A4) + df$hh_number_sick > 0)

  msg_plain(paste0("Finished creating data for aggregations..."))
  return(df)
}

#' Filter data that is appropriate for aggregation
#'
#' @param df data frame of responses
#' @param params list containing `static_dir`, indicating where to find ZIP data
#'   files, and `start_date`, indicating the first day for which estimates
#'   should be produced
#' @param lead_days Integer specifying how many days of data *before*
#'   `start_date` should be included in the data frame for aggregation. For
#'   example, if we expect up to four days of survey backfill and seven days of
#'   smoothing, we'd want to include at least 11 days of data before
#'   `start_date`, so estimates on `start_date` are based on the correct data.
#'
#' @importFrom dplyr filter
#' @export
filter_data_for_aggregation <- function(df, params, lead_days = 12L)
{
  msg_plain(paste0("Filtering data for aggregations..."))
  # Exclude responses with bad zips
  known_zips <- produce_zip_metadata(params$static_dir)
  df <- filter(df, 
               .data$zip5 %in% known_zips$zip5,
               !is.na(.data$hh_number_sick) & !is.na(.data$hh_number_total),
               dplyr::between(.data$hh_number_sick, 0L, 30L),
               dplyr::between(.data$hh_number_total, 1L, 30L),
               .data$hh_number_sick <= .data$hh_number_total,
               .data$day >= (as.Date(params$start_date) - lead_days),
               .data$wave != 12.5 # Ignore experimental Wave 12 data
  )

  msg_plain(paste0("Finished filtering data for aggregations"))
  return(df)
}

#' Fix translation error in Wave 6.
#'
#' In Wave 6's first deployment, some of the translations swapped the order of
#' responses in V4, so these responses can't be interpreted correctly. Rather
#' than recoding them, we simply delete non-English translations of V4.
#'
#' The updated deployment replaces V4 with V4a with correct translations. We
#' delete non-English V4 responses, then use V4a in place of V4 when present.
#' @param input_data data frame of responses, before subsetting to select
#'   variables
#' @param wave integer indicating survey version
#' 
#' @return corrected data frame, where V4 is the authoritative column
#' @importFrom dplyr case_when
bodge_v4_translation <- function(input_data, wave) {
  if (!("V4_1" %in% names(input_data)) &&
        !("V4a_1" %in% names(input_data))) {
    # Data unaffected; skip.
    return(input_data)
  }

  affected <- c("V4_1", "V4_2", "V4_3", "V4_4", "V4_5")
  corrected <- c("V4a_1", "V4a_2", "V4a_3", "V4a_4", "V4a_5")

  if (any(affected %in% names(input_data))) {
    # This wave is affected by the problem. Step 1: For any non-English results,
    # null out V4 responses. There are NAs because of filtering earlier in the
    # pipeline that incorrectly handles NA, so also remove these.
    non_english <- is.na(input_data$UserLanguage) | input_data$UserLanguage != "EN"
    for (col in affected) {
      input_data[non_english, col] <- NA
    }
  } else {
    # This wave does not have V4, only V4a. We will move V4a's responses into V4
    # below, so users do not need to know about our goof. Ensure the columns
    # exist so the later code can move data into them.
    for (col in affected) {
      input_data[[col]] <- NA
    }
  }

  # Step 2: If this data does not have V4a, stop.
  if (!("V4a_1" %in% names(input_data))) {
    return(input_data)
  }

  # Step 3: Wherever there are values in the new columns, move them to the old
  # columns.
  for (ii in seq_along(affected)) {
    bad <- affected[ii]
    good <- corrected[ii]

    input_data[[bad]] <- ifelse(
      !is.na(input_data[[good]]),
      input_data[[good]],
      input_data[[bad]]
    )
  }

  return(input_data)
}

#' Fix column names in Wave 10.
#'
#' In Wave 10's deployment, the meaning of items C6 and C8 changed (from "In the
#' past 5 days, have you traveled outside of your state?" and "In the past 5
#' days, how often have you... felt depressed?", etc, to "In the past 7
#' days..."), but the names were not changed. The names are changed in later
#' waves.
#'
#' We rename C6 and C8_\* to C6a and C8a_\*, respectively, to match the existing
#' naming scheme.
#' @param input_data data frame of responses, before subsetting to select
#'   variables
#' @param wave integer indicating survey version
#'   
#' @return corrected data frame
#' @importFrom dplyr rename
bodge_C6_C8 <- function(input_data, wave) {
  if ( wave != 10 ) {
    # Data unaffected; skip.
    return(input_data)
  }
  
  input_data <- rename(input_data,
                       C6a = .data$C6,
                       C8a_1 = .data$C8_1,
                       C8a_2 = .data$C8_2,
                       C8a_3 = .data$C8_3
  )

  return(input_data)
}

#' Fix B13 name in Wave 11.
#' 
#' @param input_data data frame of responses, before subsetting to select
#'   variables
#' @param wave integer indicating survey version
#'   
#' @return corrected data frame
#' @importFrom dplyr rename
bodge_B13 <- function(input_data, wave) {
  if ( "B13 " %in% names(input_data) ) {
    input_data <- rename(input_data, B13 = "B13 ")
  }
  return(input_data)
}

#' Fix column names in Wave 13.
#'
#' In Wave 13, a new item ("How many initial doses or shots did you receive of a
#' COVID-19 vaccine?") was added under the name V2a. However, a different item
#' ("Did you receive (or do you plan to receive) all recommended doses?") was
#' asked under the name V2a in Waves 8-10. To differentiate, use the name V2d
#' for the newer item.
#'
#' @param input_data data frame of responses, before subsetting to select
#'   variables
#' @param wave integer indicating survey version
#'
#' @return corrected data frame
#' @importFrom dplyr rename
bodge_V2a <- function(input_data, wave) {
  if ( wave != 13 ) {
    # Data unaffected; skip.
    return(input_data)
  }

  if ( "V2a" %in% names(input_data) ) {
    input_data <- rename(input_data,
                         V2d = .data$V2a
    )
  }

  return(input_data)
}


#' Fix E1_* names in Wave 11 data after ~June 16, 2021.
#' 
#' Items E1_1 through E1_4 are part of a matrix. Qualtrics, for unknown reasons,
#' switched to naming these E1_4 through E1_7 in June. Convert back to the
#' intended names.
#'
#' @param input_data data frame of responses, before subsetting to select
#'   variables
#' @param wave integer indicating survey version
#'   
#' @return corrected data frame
#' @importFrom dplyr rename
bodge_E1 <- function(input_data, wave) {
  E14_present <- all(c("E1_1", "E1_2", "E1_3", "E1_4") %in% names(input_data))
  E47_present <- all(c("E1_4", "E1_5", "E1_6", "E1_7") %in% names(input_data))
  assert(!(E14_present && E47_present), "fields E1_1-E1_4 should not be present at the same time as fields E1_4-E1_7")
  
  if ( E47_present ) {
    input_data <- rename(input_data,
                         E1_1 = "E1_4",
                         E1_2 = "E1_5",
                         E1_3 = "E1_6",
                         E1_4 = "E1_7"
    )
  }
  return(input_data)
}

#' Process module assignment column.
#' 
#' Rename `module` and recode to A/B/`NA`. Note: module assignment column name
#' may change with survey version.
#' 
#' @param input_data data frame of responses, before subsetting to select
#'   variables
#' @param wave integer indicating survey version
#' 
#' @return data frame with new `module` column
#' @importFrom dplyr case_when
module_assignment <- function(input_data, wave) {
  if ( "FL_23_DO" %in% names(input_data) ) {
    input_data$module <- case_when(
      input_data$FL_23_DO == "ModuleA" ~ "A",
      input_data$FL_23_DO == "ModuleB" ~ "B",
      TRUE ~ NA_character_
    )
  } else {
    input_data$module <- NA_character_
  }
  
  return(input_data)
}

#' Label arms of experimental Wave 12.
#' 
#' @param input_data data frame of responses, before subsetting to select
#'   variables
#' @param wave integer indicating survey version
#' 
#' @return data frame with new `module` column
#' @importFrom dplyr case_when
experimental_arm_assignment <- function(input_data, wave) {
  if (wave == 12.5) {
    assert( "random_number_exp" %in% names(input_data) )
    input_data$w12_treatment <- case_when(
      input_data$random_number_exp >= 0.6666 ~ 1, # demographics placed after symptom items
      input_data$random_number_exp >= 0.3333 ~ 2, # demographics placed after vaccine items
      input_data$random_number_exp < 0.3333 ~ 3, # alternative wording to V1
      TRUE ~ NA_real_
    )
  }
  
  return(input_data)
}

#' Create dataset for sharing with research partners
#'
#' Different survey waves may have different sets of questions. Here we report
#' all questions across any wave, along with a wave identifier so analysts know
#' which wave a user took.
#'
#' @param input_data data frame of responses
#' @param county_crosswalk crosswalk mapping ZIP5 to counties
#' @param params list containing `produce_individual_raceeth`, indicating
#'   whether or not to issue microdata with race-ethnicity field
#' @importFrom stringi stri_trim stri_replace_all
#' @importFrom dplyr left_join group_by filter ungroup select rename
#'
#' @export
create_complete_responses <- function(input_data, county_crosswalk, params)
{
  cols_to_report <- c(
    "start_dt", "end_dt", "date",
    "A1_1", "A1_2", "A1_3", "A1_4", "A1_5", "A2",
    "A2b", "A5_1", "A5_2", "A5_3", # A5 added in Wave 4
    "A3", "A3b", "A4",
    "B2", "B2_14_TEXT", "B2b", "B2c", "B2c_14_TEXT", "B3", "B4", "B5", "B6", "B7",
    "B8", "B10", "B10a", "B10b", "B12", "B12a", "B11", # added in Wave 4
    "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8_1", "C8_2", "C8_3", "C9", "C10_1_1", "C10_2_1",
    "C10_3_1", "C10_4_1", "C11", "C12",
    "C13", "C13a", "C14", "C15", # C13, C13a, C14, C15 added in Wave 4
    "D1", "D1_4_TEXT", "D1b", "D2", "D3", "D4", "D5",
    "D8", "D9", # D6-9 added in Wave 4; D6 & D7 withheld pending privacy procedures
    "Q36", "Q40",
    "Q64", "Q65", "Q66", "Q67", "Q68", "Q69", "Q70", "Q71", "Q72", "Q73", "Q74", "Q75",
    "Q76", "Q77", "Q78", "Q79", "Q80", # Q64-Q90 added in Wave 4
    "D10", # added in Wave 4
    "C16", "C17", "E1_1", "E1_2", "E1_3", "E1_4", "E2_1", "E2_2", "E3", # added in Wave 5
    "V1", "V2", "V3", "V4_1", "V4_2", "V4_3", "V4_4", "V4_5", # added in Wave 6
    "V9", # added in Wave 7,
    "C14a", "C17a", "V2a", "V5a", "V5b", "V5c", "V5d", "V6", "D11", # added in Wave 8
    "C6a", "C8a_1", "C8a_2", "C8a_3", "C13b", "C13c", "V11", "V12", "V13", "V14_1", "V14_2", # added in Wave 10
    "B10c", "B13", "C18a", "C18b", "C7a", "D12", "E4",
    "G1", "G2", "G3", "H1", "H2", "H3", "I1", "I2", "I3", "I4", "I5",
    "I6_1", "I6_2", "I6_3", "I6_4", "I6_5", "I6_6", "I6_7", "I6_8",
    "I7", "K1", "K2", "V11a", "V12a", "V15a", "V15b", "V16", "V3a", # added in Wave 11
    "V1alt", "B13a", "V15c", "P1", "P2", "P3", "P4", "P5", "P6", # added in experimental Wave 12
    "C17b", "V17_month", "V17_year", "V2b", "V2c", "V2d", # added in Wave 13
    
    "raceethnicity", "token", "wave", "w12_treatment", "module", "UserLanguage",
    "zip5" # temporarily; we'll filter by this column later and then drop it before writing
  )

  # Remove "raceethnicity" from cols_to_report if not producing race-ethnicity
  # microdata so we don't get an error that the field doesn't exist.
  if (is.null(params$produce_individual_raceeth) || !params$produce_individual_raceeth) {
    cols_to_report <- cols_to_report[cols_to_report != "raceethnicity"]
  }
  
  # Not all cols are present in all waves; if our data does not include some
  # questions, don't report them.
  if (any(!(cols_to_report %in% names(input_data)))) {
    warning("Some columns not present in individual response data; skipping them: ",
            paste0(cols_to_report[!(cols_to_report %in% names(input_data))],
                   collapse = ", "))
    cols_to_report <- cols_to_report[cols_to_report %in% names(input_data)]
  }

  data_full <- input_data[, cols_to_report]
  data_full <- rename(data_full,
                      StartDatetime = .data$start_dt,
                      EndDatetime = .data$end_dt,
                      Date = .data$date)

  # Join with counties. First, take the *primary* county for each crosswalk
  # entry. Otherwise the output will have more than one row per response.
  cc <- group_by(county_crosswalk, .data$zip5)
  cc <- filter(cc, .data$weight_in_location == max(.data$weight_in_location))
  cc <- ungroup(cc)
  cc <- select(cc, -.data$weight_in_location)

  data_full <- left_join(data_full, cc, by = "zip5")

  data_full$StartDatetime <- format(data_full$StartDatetime)
  data_full$EndDatetime <- format(data_full$EndDatetime)

  # convert numeric input from respondents to numeric
  for (var in c(
    "A2", "A2b", "B2b", "Q40", "C10_1_1", "C10_2_1", "C10_3_1", "C10_4_1", "D3", "D4", "D5"
  )) {
    if (var %in% names(data_full)) {
      data_full[[var]] <- as.numeric(data_full[[var]])
    }
  }

  vars <- sapply(data_full, class)
  for (var in names(vars)[vars == "character"])
  {
    data_full[[var]] <- stri_trim(stri_replace_all(data_full[[var]], " ", regex = "  *"))
    data_full[[var]][data_full[[var]] == ""] <- NA_character_
  }

  return(data_full)
}

#' Map Qualtrics survey IDs to wave number.
#'
#' Wave numbers are documented on our survey documentation site with full coding
#' details, so we can include wave number in the individual output files so
#' users know which wave the user completed.
#'
#' @param surveyID Qualtrics Survey ID
#' @return Wave number (integer), or NA if the survey ID is unknown
surveyID_to_wave <- Vectorize(function(surveyID) {
  waves <- list("SV_8zYl1sFN6fAFIxv" = 1,
                "SV_cT2ri3tFp2dhJGZ" = 2,
                "SV_8bKZvWZcGbvzsz3" = 3,
                "SV_eVXdPlGVNw04el7" = 4,
                "SV_2hErnivitm0th8F" = 5,
                "SV_8HCnaK1BJPsI3BP" = 6,
                "SV_ddjHkcYrrLWgM2V" = 7,
                "SV_ewAVaX7Wz3l0UqG" = 8,
                "SV_6PADB8DyF9SIyXk" = 10,
                "SV_4VEaeffqQtDo33M" = 11,
                "SV_3TL0r243mLkDzCK" = 12.5, # experimental version of Wave 12
                "SV_eDISRi5wQcNU70G" = 12, # finalized version of Wave 12
                "SV_2iv3tPKlYKqnalM" = 13
  )

  if ( any(names(waves) == surveyID) ) {
      return(waves[[surveyID]])
  }

  return(NA_real_)
})

#' Filter responses with sufficient data to share.
#'
#' Inclusion criteria:
#'
#' * answered age consent
#' * CID/token IS NOT missing
#' * distribution source (ie previews) IS NOT irregular
#' * start date IS IN range, pacific time
#' * Date is in [`params$start_date - params$backfill_days`, `end_date`],
#' inclusive.
#' * answered minimum of 2 additional questions, where to "answer" a numeric
#' open-ended question (A2, A2b, B2b, Q40, C10_1_1, C10_2_1, C10_3_1, C10_4_1,
#' D3, D4, D5) means to provide any number (floats okay) and to "answer" a radio
#' button question is to provide a selection.
#'
#' Most of these criteria are handled by `filter_responses()` above; this
#' function need only handle the last criterion.
#'
#' @param data_full data frame of responses
#' @param params named list of configuration options from `read_params()`,
#'   containing `start_date`, `backfill_days`, and `end_date`
#'
#' @importFrom dplyr filter
#' @importFrom rlang .data
#' @export
filter_complete_responses <- function(data_full, params)
{
  data_full <- filter(data_full,
                      .data$Date >= as.Date(params$start_date) - params$backfill_days,
                      .data$Date <= as.Date(params$end_date))

  # what zip5 values have a large enough population (>100) to include in micro
  # output. Those with too small of a population are blanked to NA
  zip_metadata <- produce_zip_metadata(params$static_dir)[, c("zip5", "keep_in_agg")]
  zipitude <- left_join(data_full, zip_metadata, by = "zip5")
  change_zip <- !is.na(zipitude$keep_in_agg) & !zipitude$keep_in_agg
  data_full$A3[change_zip] <- NA

  data_full <- select(data_full, -.data$zip5)

  # 9 includes StartDatetime, EndDatetime, Date, token, wave, geo_id,
  # UserLanguage + two questions (ignore raceethnicity, module,
  # w12_assignment, and weekly weights fields which may or may not exist, depending on params and
  # survey version)
  ignore_cols <- c("raceethnicity", "w12_assignment", "module", "weight_wf", "weight_wp")
  valid_row_filter <- rowSums( !is.na(data_full[, !(names(data_full) %in% ignore_cols)]) ) >= 9
  data_full <- data_full[valid_row_filter, ]

  return(data_full)
}

#' Filter responses to those that are "module-complete". Splits by module assignment
#'
#' Inclusion criteria:
#'
#' * answered age consent
#' * CID/token IS NOT missing
#' * distribution source (ie previews) IS NOT irregular
#' * start date IS IN range, pacific time
#' * Date is in [`params$start_date - params$backfill_days`, `end_date`],
#' inclusive.
#' * answered minimum of 2 additional questions, where to "answer" a numeric
#' open-ended question (A2, A2b, B2b, Q40, C10_1_1, C10_2_1, C10_3_1, C10_4_1,
#' D3, D4, D5) means to provide any number (floats okay) and to "answer" a radio
#' button question is to provide a selection.
#' * reached the end of the survey (i.e. sees the "Thank you" message)
#' * answered age and gender questions
#'
#' Most of these criteria are handled by `filter_responses()` and
#' `filter_complete_responses()` above; this function need only handle the last
#' two criteria.
#'
#' @param data_full data frame of responses
#' @param params named list of configuration options from `read_params()`,
#'   containing `start_date`, `backfill_days`, and `end_date`
#'
#' @importFrom dplyr filter
#' @importFrom rlang .data
#' @export
filter_module_complete_responses <- function(data_full, params)
{
  date_col <- if ("day" %in% names(data_full)) { "day" } else { "Date" }
  data_full <- rename(data_full, Date = .data$date) %>% 
    filter_complete_responses(params) %>% 
    filter(!is.na(.data$age),
           !is.na(.data$gender),
           .data$Finished == 1) %>% 
    select(date_col, .data$token, .data$module)
  
  data_a <- filter(data_full, .data$module == "A")
  data_b <- filter(data_full, .data$module == "B")
  
  return(list(a = data_a, b = data_b))
}

