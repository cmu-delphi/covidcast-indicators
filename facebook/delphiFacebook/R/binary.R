#' Write binary response variable for export to the API
#'
#' @param df          a data frame of survey responses
#' @param cw_list     a named list containing geometry crosswalk files from zip5 values
#' @param var_yes     name of the variable containing the number of yes responses
#' @param var_no      name of the variable containing the number of no responses
#' @param params      a named list with entries "start_time", and "end_time"
#' @param metric      name of the metric; used in the output file
#'
#' @export
write_binary_variable <- function(df, cw_list, var_yes, var_no, params, metric)
{
  for (i in seq_along(cw_list))
  {
    df_out <- summarize_binary(df, cw_list[[i]], var_yes, var_no, "weight_unif", params)
    write_data_api(df_out, params, names(cw_list)[i], sprintf("raw_%s", metric))

    df_out <- summarize_binary(df, cw_list[[i]], var_yes, var_no, "weight", params)
    write_data_api(df_out, params, names(cw_list)[i], sprintf("raw_w%s", metric))

    df_out <- summarize_binary(df, cw_list[[i]], var_yes, var_no, "weight_unif", params, 7L)
    write_data_api(df_out, params, names(cw_list)[i], sprintf("smoothed_%s", metric))

    df_out <- summarize_binary(df, cw_list[[i]], var_yes, var_no, "weight", params, 7L)
    write_data_api(df_out, params, names(cw_list)[i], sprintf("smoothed_w%s", metric))
  }
}

#' Summarize binary variables at a geographic level
#'
#' @param df               a data frame of survey responses
#' @param crosswalk_data   a named list containing geometry crosswalk files from zip5 values
#' @param var_yes          name of the variable containing the number of "yes" responses
#' @param var_no           name of the variable containing the number of "no" responses
#' @param var_weight       name of the variable containing the survey weights
#' @param params           a named list with entries "start_time", and "end_time"
#' @param smooth_days      integer; how many days in the past to smooth ?
#'
#' @importFrom dplyr inner_join group_by ungroup summarize n
#' @importFrom stats weighted.mean
#' @importFrom rlang .data
#' @export
summarize_binary <- function(
  df, crosswalk_data, var_yes, var_no, var_weight, params, smooth_days = 0L
)
{
  df <- inner_join(df, crosswalk_data, by = "zip5")
  names(df)[names(df) == var_yes] <- "yes"
  names(df)[names(df) == var_no] <- "no"
  names(df)[names(df) == var_weight] <- "w"

  df <- df[!is.na(df$yes) & !is.na(df$no),]
  df$yes <- df$yes * df$weight_in_location
  df$no <- df$no * df$weight_in_location

  if (smooth_days > 0) { df <- sum_n_days(df, smooth_days, params) }

  df <- group_by(df, .data$day, .data$geo_id)
  df <- ungroup(summarize(df,
    val = weighted.mean(.data$yes, .data$w) * n(),
    sample_size = weighted.mean(.data$yes + .data$no, .data$w) * n())
  )
  df$val <- 100 * (df$val + 0.5) / (df$sample_size + 1)
  df$se <- sqrt( df$val * (100 - df$val) ) / sqrt( df$sample_size )

  df <- df[rowSums(is.na(df[, c("val", "sample_size", "geo_id", "day")])) == 0,]
  df <- df[df$sample_size > params$num_filter, ]

  return(df)
}

#' Smooth data by summing responses n days in the past
#'
#' @param df               a data frame of survey responses
#' @param smooth_days      integer; how many days in the past to smooth ?
#' @param params           a named list with entries "start_time", and "end_time"
#'
#' @importFrom rlang .data
#' @importFrom tidyr complete
#' @importFrom dplyr group_by arrange mutate ungroup
#' @importFrom zoo rollapplyr
#' @export
sum_n_days <- function(df, smooth_days, params)
{
  day_set <- format(
    seq(
      as.Date(params$start_time, tz = "America/Los_Angeles"),
      as.Date(params$end_time, tz = "America/Los_Angeles"),
      by = '1 day'
    ),
    "%Y%m%d",
    tz = "America/Los_Angeles"
  )

  roll_sum <- function(val) rollapplyr(val, smooth_days, sum, partial = TRUE, na.rm = TRUE)

  df_complete <- complete(df, day = day_set, .data$geo_id, fill = list(yes = 0, no = 0))
  df_complete <- group_by(df_complete, .data$geo_id)
  df_complete <- arrange(df_complete, .data$day)
  df_complete <- mutate(
    df_complete, .data$day, yes = roll_sum(.data$yes), no = roll_sum(.data$no)
  )
  df_complete <- ungroup(df_complete)
  df_complete <- df_complete[!is.na(df_complete$yes) & !is.na(df_complete$no),]

  df_complete
}
