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
#' @importFrom readr read_lines cols
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
  input_data <- read_csv(full_path, skip = 3L, col_names = col_names, col_types = cols())
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

  return(input_data)
}

#' Filter responses for privacy and validity
#'
#' @param input_data             data frame containing response data
#' @param seen_tokens_archive    data frame giving previously seen tokens
#' @param params                 named list containing values "static_dir", "start_time",
#'                               and "end_time"
#'
#' @importFrom dplyr anti_join between
#' @importFrom rlang .data
#' @export
filter_responses <- function(input_data, seen_tokens_archive, params)
{
  # what zip5 values have a large enough population (>100) to include
  allowed_zips <- produce_allowed_zip5(params$static_dir)

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
  input_data <- input_data[between(input_data$start_dt, params$start_time, params$end_time),]
  input_data <- input_data[input_data$zip5 %in% allowed_zips,]

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
  df$day <- stri_replace_all(df$date, "", fixed = "-")

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
#' @param df  data frame of responses
#'
#' @export
filter_data_for_aggregatation <- function(df)
{
  df <- df[!is.na(df$hh_number_sick) & !is.na(df$hh_number_total), ]
  df <- df[between(df$hh_number_sick, 0L, 30L), ]
  df <- df[between(df$hh_number_total, 1L, 30L), ]
  df <- df[df$hh_number_sick <= df$hh_number_total, ]

  return(df)
}

#' Create dataset for sharing with research parteners
#'
#' @param input_data    data frame of responses
#'
#' @export
create_complete_responses <- function(input_data)
{
  data_full <- select(input_data,
    StartDatetime = "start_dt", EndDatetime = "start_dt", Date = "date",
    "A1_1", "A1_2", "A1_3", "A1_4", "A1_5", "A2", "A2b", "A3", "A3b", "A4",
    "B2", "B2_14_TEXT", "B2b", "B3", "B4", "B5", "B6",
    "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8_1", "C8_2", "C9", "C10_1_1", "C10_2_1",
    "C10_3_1", "C10_4_1", "C11", "C12",
    "D1", "D1_4_TEXT", "D1b", "D2", "D3", "D4", "D5",
    "Q36", "Q40",
    "token"
  )

  for (var in c(
    "A2", "A2b", "B2b", "Q40", "C10_1_1", "C10_2_1", "C10_3_1", "C10_4_1", "D3", "D4", "D5"
  ))
  {
    data_full[[var]] <- as.numeric(data_full[[var]])
  }

  vars <- sapply(data_full, class)
  for (var in names(vars)[vars == "character"])
  {
    data_full[[var]] <- stri_replace_all(data_full[[var]], " ", regex = "  *")
    data_full[[var]] <- stri_replace_all(data_full[[var]], "", regex = "  *")
    data_full[[var]][data_full[[var]] == ""] <- NA_character_
  }

  return(data_full)
}

#' Filter responses with sufficent responses to share
#'
#' @param data_full    data frame of responses
#'
#' @export
filter_complete_responses <- function(data_full)
{
  data_full <- data_full[rowSums(!is.na(data_full)) >= 6, ]

  return(data_full)
}
