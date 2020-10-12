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
#' @return A data frame of all loaded data files concatenated into one data
#'   frame
#'
#' @importFrom dplyr bind_rows
#' @export
load_responses_all <- function(params) {
  input_data <- vector("list", length(params$input))
  for (i in seq_along(input_data))
  {
    input_data[[i]] <- load_response_one(params$input[i], params)
  }
  input_data <- bind_rows(input_data)
  return(input_data)
}

#' Load a single set of responses
#'
#' @param input_filename  filename of the input CSV file
#' @param params          a named list containing a value named "input_dir", the directory
#'                        where the input file are found
#'
#' @importFrom stringi stri_split stri_extract stri_replace_all stri_replace
#' @importFrom readr read_lines cols locale col_character
#' @importFrom dplyr arrange desc case_when
#' @importFrom lubridate force_tz with_tz
#' @importFrom rlang .data
#' @export
load_response_one <- function(input_filename, params) {
  # read the input data; need to deal with column names manually because of header
  full_path <- file.path(params$input_dir, input_filename)
  meta_data <- read_lines(full_path, skip = 2L, n_max = 1L)
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
                           B2_14_TEXT = col_character(),
                           D1_4_TEXT = col_character(),
                           A3 = col_character(),
                           B2 = col_character(),
                           C1 = col_character()),
                         locale = locale(grouping_mark = ""))
  if (nrow(input_data) == 0) {
    return(tibble())
  }
  input_data <- arrange(input_data, desc(.data$StartDate))

  # create symptom variables
  input_data$hh_fever <- (input_data$A1_1 == 1L)
  input_data$hh_soar_throat <- (input_data$A1_2 == 1L)
  input_data$hh_cough <- (input_data$A1_3 == 1L)
  input_data$hh_short_breath <- (input_data$A1_4 == 1L)
  input_data$hh_diff_breath <- (input_data$A1_5 == 1L)
  suppressWarnings({ input_data$hh_number_sick <- as.integer(input_data$A2) })

  if ("A5_1" %in% names(input_data)) {
    # This is Wave 4, where item A2b was replaced with 3 items asking about
    # separate ages. Many respondents leave blank the categories that do not
    # apply to their household, rather than entering 0, so if at least one of
    # the three items has a response, we impute 0 for the remaining items.
    suppressWarnings({
      age18 <- as.integer(input_data$A5_1)
      age1864 <- as.integer(input_data$A5_2)
      age65 <- as.integer(input_data$A5_3)
    })

    input_data$hh_number_total <- ifelse(
      is.na(age18) + is.na(age1864) + is.na(age65) < 3,
      (ifelse(is.na(age18), 0, age18) +
         ifelse(is.na(age1864), 0, age1864) +
         ifelse(is.na(age65), 0, age65)),
      NA_integer_
    )
  } else {
    # This is Wave <= 4, where item A2b measured household size
    suppressWarnings({
      input_data$hh_number_total <- as.integer(input_data$A2b)
    })
  }
  input_data$zip5 <- input_data$A3

  # create mental health variables
  input_data$mh_worried_ill <- input_data$C9 == 1 | input_data$C9 == 2
  input_data$mh_anxious <- input_data$C8_1 == 3 | input_data$C8_1 == 4
  input_data$mh_depressed <- input_data$C8_2 == 3 | input_data$C8_2 == 4
  if ("C8_3" %in% names(input_data)) {
    input_data$mh_isolated <- input_data$C8_3 == 3 | input_data$C8_3 == 4
  } else {
    input_data$mh_isolated <- NA
  }

  # mask and contact variables
  input_data$c_travel_state <- input_data$C6 == 1
  if ("C14" %in% names(input_data)) {
    # wearing mask most or all of the time; exclude respondents who have not
    # been in public
    input_data$c_mask_often <- input_data$C14 == 1 | input_data$C14 == 2
    input_data$c_mask_often <- ifelse(input_data$C14 == 6, NA, input_data$c_mask_often)
  } else {
    input_data$c_mask_often <- NA
  }

  if ("C3" %in% names(input_data)) {
    input_data$c_work_outside_5d <- input_data$C3 == 1
  } else {
    input_data$c_work_outside_5d <- NA
  }

  # create testing variables
  if ("B8" %in% names(input_data) && "B10" %in% names(input_data) &&
        "B12" %in% names(input_data)) {
    # fraction tested in last 14 days. yes == 1 on B10; no == 2 on B8 *or* 3 on
    # B10 (which codes "no" as 3 for some reason)
    input_data$t_tested_14d <- case_when(
      input_data$B8 == 2 | input_data$B10 == 3 ~ 0,
      input_data$B10 == 1 ~ 1,
      TRUE ~ NA_real_
    )

    # fraction, of those tested in past 14 days, who tested positive. yes == 1
    # on B10a, no == 2 on B10a; option 3 is "I don't know", which is excluded
    input_data$t_tested_positive_14d <- case_when(
      input_data$B10a == 1 ~ 1, # yes
      input_data$B10a == 2 ~ 0, # no
      input_data$B10a == 3 ~ NA_real_, # I don't know
      TRUE ~ NA_real_
    )

    # fraction, of those not tested in past 14 days, who wanted to be tested but
    # were not
    input_data$t_wanted_test_14d <- input_data$B12 == 1
  } else {
    input_data$t_tested_14d <- NA
    input_data$t_tested_positive_14d <- NA
    input_data$t_wanted_test_14d <- NA
  }

  # When a token begins with a hyphen, Qualtrics CSVs contain a lone single
  # quote in front, for some mysterious reason. Strip these from the token,
  # since they are erroneous and won't match with CIDs in weights files.
  input_data$token <- stri_replace(input_data$token, "-", regex = "^'-")

  # clean date time data, forcing to be in the "America/Los_Angeles" timezone
  tz_to <- "America/Los_Angeles"
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
  input_data$zip5 <- ifelse(nchar(input_data$zip5) > 5, NA_character_, input_data$zip5)

  return(input_data)
}

#' Filter responses for privacy and validity
#'
#' @param input_data data frame containing response data
#' @param params named list containing values "static_dir", "start_time", and
#'   "end_time"
#'
#' @importFrom dplyr anti_join
#' @importFrom rlang .data
#' @export
filter_responses <- function(input_data, params) {
  # take only the first instance of each token
  input_data <- arrange(input_data, .data$StartDate)
  input_data <- input_data[input_data$token != "",]
  input_data <- input_data[!duplicated(input_data$token),]

  input_data <- input_data[input_data$S1 == 1, ]
  input_data <- input_data[input_data$DistributionChannel != "preview", ]

  # take the right dates. We don't filter the start date because the aggregate
  # and individual data pipelines handle that themselves (aggregate in
  # particular needs data well before start_date)
  input_data <- input_data[as.Date(input_data$date) <= params$end_date, ]

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
  # First, merge the new data with the archived data, taking the first start
  # date for any given token. This allows for backfill. Note that the order
  # matters: since arrange() uses order(), which is a stable sort, ties will
  # result in the input data being used in preference over the archive data.
  # This means that if we run the pipeline, then change the input CSV, running
  # again will used the changed data instead of the archived data.
  data <- bind_rows(input_data, archive$input_data)
  data <- arrange(data, .data$StartDate)

  data <- data[!duplicated(data$token), ]

  # Next, filter out responses with tokens that were seen before in responses
  # started before even the responses in `data`. These are responses submitted
  # recently with tokens that were initially used long ago, before the data
  # contained in `archive$input_data`.
  if (!is.null(archive$seen_tokens)) {
    data <- left_join(data, archive$seen_tokens,
                      by = "token", suffix = c("", ".seen"))
    data <- data[is.na(data$start_dt.seen) | data$start_dt <= data$start_dt.seen, ]
  }

  return(data)
}

#' Create variables needed for aggregation
#'
#' @param input_data   the input data frame of (filtered) responses
#'
#' @export
create_data_for_aggregatation <- function(input_data)
{
  df <- input_data
  df$weight_unif <- 1.0
  df$day <- as.Date(df$date)

  # create variables for cli and ili signals
  hh_cols <- c("hh_fever", "hh_soar_throat", "hh_cough", "hh_short_breath", "hh_diff_breath")
  df$cnt_symptoms <- apply(df[,hh_cols], 1, sum, na.rm = TRUE)
  df$hh_number_sick[df$cnt_symptoms <= 0] <- 0
  df$is_cli <- df$hh_fever & (
    df$hh_cough | df$hh_short_breath | df$hh_diff_breath
  )
  df$is_cli[is.na(df$is_cli)] <- FALSE
  df$is_ili <- df$hh_fever & (df$hh_soar_throat | df$hh_cough)
  df$is_ili[is.na(df$is_ili)] <- FALSE
  df$hh_p_cli <- 100 * df$is_cli * df$hh_number_sick / df$hh_number_total
  df$hh_p_ili <- 100 * df$is_ili * df$hh_number_sick / df$hh_number_total

  ### Create variables for community survey.
  ## Question A4: how many people you know in the local community (not your
  ## household) with CLI
  df$community_yes <- as.numeric(as.numeric(df$A4) > 0)

  ## Whether you know someone in your local community *or* household who is
  ## sick.
  df$hh_community_yes <- as.numeric(as.numeric(df$A4) + df$hh_number_sick > 0)

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
#' @export
filter_data_for_aggregatation <- function(df, params, lead_days = 12L)
{
  # Exclude responses with bad zips
  known_zips <- produce_zip_metadata(params$static_dir)
  df <- df[df$zip5 %in% known_zips$zip5,]

  df <- df[!is.na(df$hh_number_sick) & !is.na(df$hh_number_total), ]
  df <- df[dplyr::between(df$hh_number_sick, 0L, 30L), ]
  df <- df[dplyr::between(df$hh_number_total, 1L, 30L), ]
  df <- df[df$hh_number_sick <= df$hh_number_total, ]

  df <- df[df$day >= (as.Date(params$start_date) - lead_days), ]

  return(df)
}

#' Create dataset for sharing with research partners
#'
#' Different survey waves may have different sets of questions. Here we report
#' all questions across any wave, along with a wave identifier so analysts know
#' which wave a user took.
#'
#' @param input_data data frame of responses
#' @param county_crosswalk crosswalk mapping ZIP5 to counties
#' @importFrom stringi stri_trim stri_replace_all
#' @importFrom dplyr left_join group_by filter ungroup select rename
#'
#' @export
create_complete_responses <- function(input_data, county_crosswalk)
{
  input_data$wave <- surveyID_to_wave(input_data$SurveyID)

  cols_to_report <- c(
    "start_dt", "end_dt", "date",
    "A1_1", "A1_2", "A1_3", "A1_4", "A1_5", "A2",
    "A2b", "A5_1", "A5_2", "A5_3", # A5 added in Wave 4
    "A3", "A3b", "A4",
    "B2", "B2_14_TEXT", "B2b", "B2c", "B2c_14_TEXT", "B3", "B4", "B5", "B6", "B7",
    "B8", "B10", "B10a", "B10b", "B12", "B12a", "B11", # added in Wave 4
    "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8_1", "C8_2", "C8_3", "C9", "C10_1_1", "C10_2_1",
    "C10_3_1", "C10_4_1", "C11", "C12",
    "C13", "C13a", "C14", # C13, C13a, C14 added in Wave 4
    "D1", "D1_4_TEXT", "D1b", "D2", "D3", "D4", "D5",
    "D8", "D9", # D6-9 added in Wave 4; D6 & D7 withheld pending privacy procedures
    "Q36", "Q40",
    "Q64", "Q65", "Q66", "Q67", "Q68", "Q69", "Q70", "Q71", "Q72", "Q73", "Q74", "Q75",
    "Q76", "Q77", "Q78", "Q79", "Q80", # Q64-Q90 added in Wave 4
    "D10", # added in Wave 4
    "token", "wave", "UserLanguage",
    "zip5" # temporarily; we'll filter by this column later and then drop it before writing
  )

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
                "SV_eVXdPlGVNw04el7" = 4)

  if (surveyID %in% names(waves)) {
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
#' * answered minimum of 2 additional questions, where to "answer" a numeric
#' open-ended question (A2, A2b, B2b, Q40, C10_1_1, C10_2_1, C10_3_1, C10_4_1,
#' D3, D4, D5) means to provide any number (floats okay) and to "answer" a radio
#' button question is to provide a selection.
#' * Date is in [`params$start_date - params$backfill_days`, `end_date`],
#' inclusive.
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

  data_full <- select(data_full, -zip5)

  # 9 includes StartDatetime, EndDatetime, Date, token, wave, geo_id,
  # UserLanguage + two questions
  data_full <- data_full[rowSums(!is.na(data_full)) >= 9, ]

  return(data_full)
}
