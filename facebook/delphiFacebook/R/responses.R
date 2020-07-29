#' Load all response datasets in a local directory
#'
#' @param params    a named listed containing a value named "input", a vector of paths to
#'                  load by the function, and "input_dir", the directory where the input
#'                  files are found
#'
#' @importFrom dplyr bind_rows
#' @export
load_responses_all <- function(params)
{
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
#' @importFrom readr read_lines cols locale
#' @importFrom dplyr arrange desc
#' @importFrom lubridate force_tz with_tz
#' @importFrom rlang .data
#' @export
load_response_one <- function(input_filename, params)
{
  # read the input data; need to deal with column names manually because of header
  full_path <- file.path(params$input_dir, input_filename)
  meta_data <- read_lines(full_path, skip = 2L, n_max = 1L)
  tz_from <- stri_extract(meta_data, regex = "[a-zA-Z_]+/[a-zA-Z_]+")
  col_names <- stri_split(read_lines(full_path, n_max = 1L), fixed = ",")[[1]]
  col_names <- stri_replace_all(col_names, "", fixed = "\"")

  ## The CSVs have some columns with column-separated fields showing which of
  ## multiple options a user selected; readr would interpret these as thousand
  ## separators by default, so we tell it that no thousands separators are used.
  input_data <- read_csv(full_path, skip = 3L, col_names = col_names, col_types = cols(),
                         locale = locale(grouping_mark = ""))
  input_data <- arrange(input_data, desc(.data$StartDate))

  # create new variables
  input_data$hh_fever <- (input_data$A1_1 == 1L)
  input_data$hh_soar_throat <- (input_data$A1_2 == 1L)
  input_data$hh_cough <- (input_data$A1_3 == 1L)
  input_data$hh_short_breath <- (input_data$A1_4 == 1L)
  input_data$hh_diff_breath <- (input_data$A1_5 == 1L)
  suppressWarnings({ input_data$hh_number_sick <- as.integer(input_data$A2) })
  suppressWarnings({ input_data$hh_number_total <- as.integer(input_data$A2b) })
  input_data$zip5 <- input_data$A3

  input_data$token <- stri_replace(input_data$token, "-", fixed = "^'-")

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
#' @param input_data             data frame containing response data
#' @param seen_tokens_archive    data frame giving previously seen tokens
#' @param params                 named list containing values "static_dir", "start_time",
#'                               and "end_time"
#'
#' @importFrom dplyr anti_join
#' @importFrom rlang .data
#' @export
filter_responses <- function(input_data, seen_tokens_archive, params)
{
  # only include tokens we have not already seen
  if (!is.null(seen_tokens_archive))
  {
    input_data <- anti_join(input_data, seen_tokens_archive, by = "token")
  }

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
  # what zip5 values have a large enough population (>100) to include in aggregates
  allowed_zips <- produce_allowed_zip5(params$static_dir)
  df <- df[df$zip5 %in% allowed_zips,]

  df <- df[!is.na(df$hh_number_sick) & !is.na(df$hh_number_total), ]
  df <- df[dplyr::between(df$hh_number_sick, 0L, 30L), ]
  df <- df[dplyr::between(df$hh_number_total, 1L, 30L), ]
  df <- df[df$hh_number_sick <= df$hh_number_total, ]

  df <- df[df$day >= (as.Date(params$start_date) - lead_days), ]

  return(df)
}

#' Create dataset for sharing with research partners
#'
#' @param input_data    data frame of responses
#' @importFrom stringi stri_trim stri_replace_all
#'
#' @export
create_complete_responses <- function(input_data)
{
  data_full <- select(input_data,
    StartDatetime = "start_dt", EndDatetime = "end_dt", Date = "date",
    "A1_1", "A1_2", "A1_3", "A1_4", "A1_5", "A2", "A2b", "A3", "A3b", "A4",
    "B2", "B2_14_TEXT", "B2b", "B3", "B4", "B5", "B6",
    "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8_1", "C8_2", "C9", "C10_1_1", "C10_2_1",
    "C10_3_1", "C10_4_1", "C11", "C12",
    "D1", "D1_4_TEXT", "D1b", "D2", "D3", "D4", "D5",
    "Q36", "Q40",
    "token", wave = "SurveyID", "UserLanguage"
  )

  data_full$wave <- surveyID_to_wave(data_full$wave)

  data_full$StartDatetime <- format(data_full$StartDatetime)
  data_full$EndDatetime <- format(data_full$EndDatetime)

  for (var in c(
    "A2", "A2b", "B2b", "Q40", "C10_1_1", "C10_2_1", "C10_3_1", "C10_4_1", "D3", "D4", "D5"
  ))
  {
    data_full[[var]] <- as.numeric(data_full[[var]])
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
                "SV_8bKZvWZcGbvzsz3" = 3)

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
  # 6 includes StartDatetime, EndDatetime, Date, token, + two questions
  data_full <- data_full[rowSums(!is.na(data_full)) >= 6, ]

  data_full <- filter(data_full,
                      .data$Date >= as.Date(params$start_date) - params$backfill_days,
                      .data$Date <= as.Date(params$end_date))

  return(data_full)
}
