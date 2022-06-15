#' Write a file containing the new tokens seen in the dataset
#'
#' Write only tokens that appeared between `start_date` and `end_date`.
#'
#' @param data a data frame containing a column named "token"
#' @param type_name character value used for naming the output file
#' @param params a named list; must contain entries "start_time", "end_time",
#'   and "weights_out_dir" or "experimental_weights_out_dir". These are used in
#'   constructing the path where the output data will be stored.
#' @param module_type character value used to indicate module filtering, if any
#' @param experimental_cids boolean flag indicating if CIDs to save should use
#'   the, as of now, experimental format
#'
#' @importFrom readr write_csv
#' @export
write_cid <- function(data, type_name, params, module_type="", experimental_cids=FALSE)
{
  if (experimental_cids) {
    weights_out_dir <- params$experimental_weights_out_dir
  } else {
    weights_out_dir <- params$weights_out_dir
  }
  create_dir_not_exist(weights_out_dir)

  fname <- generate_cid_list_filename(type_name, params, module_type)
  
  # aggregate data contains a `day` column that is a Date object. individual
  # data contains a `Date` column for the same purpose, which is *not* a `Date`
  # object but a formatted string. This sure is elegant!
  date_col <- if ("day" %in% names(data)) { "day" } else { "Date" }

  token_data <- data[data[[date_col]] >= as.Date(params$start_date) &
                       data[[date_col]] <= as.Date(params$end_date), ]
  
  msg <- ifelse(is.na(module_type) || module_type == "" || length(module_type) == 0,
                sprintf("writing weights data for %s", type_name),
                sprintf("writing weights data for %s, %s", type_name, module_type))
  msg_df(msg, token_data)
  write_csv(select(token_data, "token"), file.path(weights_out_dir, fname),
            col_names = FALSE)
}

#' Write a file containing newly-seen tokens, using new CID list names
#'
#' @param data a data frame containing a column named "token"
#' @param type_name character value used for naming the output file
#' @param params a named list; must contain entries "start_time", "end_time",
#'   and "experimental_weights_out_dir". These are used in constructing the path
#'   where the output data will be stored.
#' @param module_type character value used to indicate module filtering, if any
#'
#' @export
write_cid_experimental_wrapper <- function(data, type_name, params, module_type)
{
  map_type <- c(full = "partial",
                part_a = "part_a",
                module_complete = "full")
  type_name <- map_type[type_name]
  
  write_cid(data, type_name, params, module_type, experimental_cids=TRUE)
}

#' Create filename to output list of tokens.
#'
#' @param type_name character value used for naming the output file
#' @param params a named list; must contain entries "start_time" and "end_time".
#'   These are used in constructing the file name.
#' @param module_type character value used to indicate module filtering, if any
generate_cid_list_filename <- function(type_name, params, module_type) {
  sprintf(
    "cvid_cids_%s_response_%s%s_-_%s.csv",
    type_name,
    module_type,
    format(params$start_time, "%H_%M_%Y_%m_%d", tz = tz_to),
    format(params$end_time, "%H_%M_%Y_%m_%d", tz = tz_to)
  )
}

#' Add weights to a dataset of responses
#'
#' There are two types of weights: step 1 weights and full weights. Step 1
#' weights are used for producing our aggregations; full weights are used
#' exclusively for producing individual response files.
#'
#' @param data    a data frame containing a column called "token"
#' @param params  a named list containing value "weights_in_dir", indicating where the
#'                weights files are stored, and "weekly_weights_in_dir", indicating
#'                where the weekly weights files are stored, if `add_weekly_weights` is TRUE
#' @param weights Which weights to use -- step1 or full?
#' @param add_weekly_weights boolean indicating whether to add weekly partial and full
#'                weights in addition to daily weights
#'
#' @export
add_weights <- function(data, params, weights = c("step1", "full"), add_weekly_weights = FALSE)
{
  weights <- match.arg(weights)

  if (weights == "step1") {
    pattern <- "step_1_weights.csv$"
  } else if (weights == "full") {
    pattern <- "finish_full_survey_weights.csv$"
  }

  weight_result <- load_and_join_weights(data, params$weights_in_dir, pattern)
  data <- weight_result$df
  latest_weight_date <- weight_result$weight_date

  if (add_weekly_weights) {
    # Since each weight column is joined on as `weight`, we need to rename
    # each new weight column before performing the next join to avoid
    # overwriting any weights
    data <- rename(data, daily_weight = weight)
    data <- load_and_join_weights(
        data, params$weekly_weights_in_dir, pattern = "partial_weekly_weights.csv$"
      )$df %>%
      rename(weight_wp = weight)
    data <- load_and_join_weights(
        data, params$weekly_weights_in_dir, pattern = "full_weekly_weights.csv$"
      )$df %>%
      rename(weight_wf = weight, weight = daily_weight)
  }

  return( list(df = data, weight_date = latest_weight_date) )
}

#' Add a single type of weights to a dataset of responses as field `weight`
#'
#' @param data    a data frame containing a column called "token"
#' @param weights_dir directory to look for the weights in
#' @param pattern regular expression matching desired weights files
#'
#' @importFrom dplyr bind_rows left_join
#' @importFrom data.table fread
#' @importFrom stringi stri_extract_first
#' @importFrom utils tail
load_and_join_weights <- function(data, weights_dir, pattern) {
  weights_files <- dir(weights_dir, pattern = pattern, full.names = TRUE)
  weights_files <- sort(weights_files)

  latest_weight <- tail(weights_files, n = 1)
  latest_weight_date <- as.Date(
    stri_extract_first(basename(latest_weight), regex = "^[0-9]{4}-[0-9]{2}-[0-9]{2}")
  )

  col_types <- c("character", "double")
  col_names <- c("cid", "weight")
  agg_weights <- bind_rows(lapply(
    weights_files,
    fread,
    colClasses = col_types,
    col.names = col_names
    )
  )
  agg_weights <- agg_weights[!duplicated(cid),]
  data <- left_join(data, agg_weights, by = c("token" = "cid"))
  
  return( list(df = data, weight_date = latest_weight_date) )
}
