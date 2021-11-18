#' Write a file containing the new tokens seen in the dataset
#'
#' Write only tokens that appeared between `start_date` and `end_date`.
#'
#' @param data a data frame containing a column named "token"
#' @param type_name character value used for naming the output file
#' @param params a named list; must contain entries "start_time", "end_time",
#'   and "weights_out_dir". These are used in constructing the path where the
#'   output data will be stored.
#'
#' @importFrom readr write_csv
#' @export
write_cid <- function(data, type_name, params)
{
  fname <- sprintf(
    "cvid_cids_%s_response_%s_-_%s.csv",
    type_name,
    format(params$start_time, "%H_%M_%Y_%m_%d", tz = tz_to),
    format(params$end_time, "%H_%M_%Y_%m_%d", tz = tz_to)
  )

  create_dir_not_exist(params$weights_out_dir)

  # aggregate data contains a `day` column that is a Date object. individual
  # data contains a `Date` column for the same purpose, which is *not* a `Date`
  # object but a formatted string. This sure is elegant!
  date_col <- if ("day" %in% names(data)) { "day" } else { "Date" }

  token_data <- data[data[[date_col]] >= as.Date(params$start_date) &
                       data[[date_col]] <= as.Date(params$end_date), ]

  msg_df(sprintf("writing weights data for %s", type_name), token_data)
  write_csv(select(token_data, "token"), file.path(params$weights_out_dir, fname),
            col_names = FALSE)
}

#' Add weights to a dataset of responses
#'
#' There are two types of weights: step 1 weights and full weights. Step 1
#' weights are used for producing our aggregations; full weights are used
#' exclusively for producing individual response files.
#'
#' @param data    a data frame containing a column called "token"
#' @param params  a named list containing a value "weights_in_dir" indicating where the
#'                weights files are stored
#' @param weights Which weights to use -- step1 or full?
#'
#' @importFrom dplyr bind_rows left_join
#' @importFrom data.table fread
#' @importFrom stringi stri_extract_first
#' 
#' @export
join_weights <- function(data, params, weights = c("step1", "full"))
{
  weights <- match.arg(weights)

  if (weights == "step1") {
    pattern <- "step_1_weights.csv$"
  } else if (weights == "full") {
    pattern <- "finish_full_survey_weights.csv$"
  }

  weights_files <- dir(params$weights_in_dir, pattern = pattern, full.names = TRUE)
  weights_files <- sort(weights_files)

  latest_weight <- tail(weights_files, n = 1)
  latest_weight_date <- as.Date(
    stri_extract_first(latest_weight, regex = "^[0-9]{4}-[0-9]{2}-[0-9]{2}")
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
