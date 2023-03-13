## Data Preprocessing
## 
## The raw input data should have 4/5 basic columns:
## time_value: reference date
## issue_date: issue date/date of reporting
## geo_value: location
## lag: the number of days between issue date and the reference date
## counts: the number of counts used for estimation


#' Re-index, fill na, make sure all reference date have enough rows for updates
#' @template df-template
#' @template refd_col-template
#' @template lag_col-template
#' @param min_refd the earliest reference date considered in the data
#' @param max_refd the latest reference date considered in the data
#' @template ref_lag-template
#' 
#' @return df_new Data Frame with filled rows for missing lags
#'
#' @importFrom tidyr crossing
#' @importFrom stats setNames
#'
#' @export
fill_rows <- function(df, refd_col, lag_col, min_refd, max_refd, ref_lag) {
  # Full list of lags
  # +30 to have values for calculating 7-day averages
  lags <- min(df[[lag_col]]): (ref_lag + 30)
  # Full list reference dates
  refds <- as.character(seq(as.Date(min_refd), as.Date(max_refd), by="day"))
  row_inds_df <- setNames(
    as.data.frame(crossing(refds, lags)),
    c(refd_col, lag_col)
  )
  df_new = merge(x=df, y=row_inds_df,
                 by=c(refd_col, lag_col),  all.y=TRUE)
  return (df_new)
}

#' Get pivot table, filling NANs. If there is no update on issue date D but 
#' previous reports exist for issue date D_p < D, all the dates between
#' [D_p, D] are filled with with the reported value on date D_p. If there is 
#' no update for any previous issue date, fill in with 0.
#' @template df-template
#' @template value_col-template
#' @template refd_col-template
#' @template lag_col-template
#' 
#' @importFrom tidyr fill pivot_wider pivot_longer
#' @importFrom dplyr %>% everything select
#' 
#' @export
fill_missing_updates <- function(df, value_col, refd_col, lag_col) {
  pivot_df <- df[order(df[[lag_col]], decreasing=FALSE), ] %>%
    pivot_wider(id_cols=lag_col, names_from=refd_col, values_from=value_col)
  
  if (any(diff(pivot_df[[lag_col]]) != 1)) {
    stop("Risk exists in forward filling")
  }
  pivot_df <- fill(pivot_df, everything(), .direction="down")
  
  # Fill NAs with 0s
  pivot_df[is.na(pivot_df)] <- 0
  
  backfill_df <- pivot_longer(pivot_df,
    -lag_col, values_to="value_raw", names_to=refd_col
  )
  
  return (as.data.frame(backfill_df))
}

#' Calculate 7 day moving average for each issue date
#' The 7dav for date D reported on issue date D_i is the average from D-7 to D-1
#' @param pivot_df Data Frame where the columns are issue dates and the rows are 
#'    reference dates
#' @template refd_col-template
#' 
#' @importFrom zoo rollmeanr
#' 
#' @export
get_7dav <- function(pivot_df, refd_col) {
  for (col in colnames(pivot_df)) {
    if (col == refd_col) next
    pivot_df[, col] <- rollmeanr(pivot_df[, col], 7, align="right", fill=NA)
  }
  backfill_df <- pivot_longer(pivot_df,
    -refd_col, values_to="value_raw", names_to="issue_date"
  )
  return (as.data.frame(backfill_df))
}

#' Used for data shifting in terms of reference date
#' 
#' @template df-template
#' @param n_day number of days to be shifted
#' @template refd_col-template
#' 
#' @export
add_shift <- function(df, n_day, refd_col) {
  df[, refd_col] <- as.character(as.Date(df[[refd_col]]) + n_day)
  return (df)
}

#' Add one hot encoding for day of a week info in terms of reference
#' and issue date
#' 
#' @template df-template
#' @param wd vector of days of a week
#' @template time_col-template
#' @param suffix suffix added to indicate which kind of date is used
#' 
#' @export
add_dayofweek <- function(df, time_col, suffix, wd = WEEKDAYS_ABBR) {
  dayofweek <- as.numeric(format(as.Date(df[[time_col]]), format="%u"))
  for (i in seq_along(wd)) {
    df[, paste0(wd[i], suffix)] <- as.numeric(dayofweek == i)
  }
  if (suffix == "_ref") {
    df[, paste0("Sun", suffix)] <- as.numeric(dayofweek == 7)
  }
  return (df)
}

#' Get week of a month info according to a date
#'
#' All the dates on or before the ith Sunday but after the (i-1)th Sunday
#' is considered to be the ith week. Notice that 
#'     If there are 4 or 5 weeks in total, the ith weeks is labeled as i 
#'     and the dates in the 5th week this month are actually in the same
#'     week with the dates in the 1st week next month and those dates are
#'     sparse. Thus, we assign the dates in the 5th week to the 1st week. 
#'     If there are 6 weeks in total, the 1st, 2nd, 3rd, 4th, 5th, 6th weeks
#'     are labeled as c(1, 1, 2, 3, 4, 1) which means we will merge the first,
#'     second and the last weeks together.
#' 
#' @param date Date object
#' 
#' @importFrom lubridate make_date days_in_month year month day
#' 
#' @return a integer indicating which week it is in a month
get_weekofmonth <- function(date) {
  year <- year(date)
  month <- month(date)
  day <- day(date)
  firstdayofmonth <- as.numeric(format(make_date(year, month, 1), format="%u"))
  n_days <- lubridate::days_in_month(date)
  n_weeks <- (n_days + firstdayofmonth - 1) %/% 7 + 1
  extra_check <- as.integer(n_weeks > 5)
  return (max((day + firstdayofmonth - 1) %/% 7 - extra_check, 0) %% 4 + 1)
}

#' Add one hot encoding for week of a month info in terms of issue date
#' 
#' @template df-template
#' @param wm vector of weeks of a month
#' @template time_col-template
#' 
#' @export
add_weekofmonth <- function(df, time_col, wm = WEEK_ISSUES) {
  weekofmonth <- get_weekofmonth(as.Date(df[[time_col]]))
  for (i in seq_along(wm)) {
    df[, paste0(wm[i])] <- as.numeric(weekofmonth == i)
  }
  return (df)
}

#' Add 7dav and target to the data
#' Target is the updates made ref_lag days after the first release
#' @template df-template
#' @template value_col-template
#' @template refd_col-template
#' @template lag_col-template
#' @template ref_lag-template
#' 
#' @importFrom dplyr %>%
#' @importFrom tidyr pivot_wider drop_na
#' 
#' @export
add_7davs_and_target <- function(df, value_col, refd_col, lag_col, ref_lag) {
  df$issue_date <- as.character(as.Date(df[[refd_col]]) + df[[lag_col]])
  pivot_df <- df[order(df$issue_date, decreasing=FALSE), ] %>%
    pivot_wider(id_cols=refd_col, names_from="issue_date", 
                values_from=value_col)
  
  # Add 7dav avg
  avg_df <- get_7dav(pivot_df, refd_col)
  avg_df <- add_shift(avg_df, 1, refd_col) # 7dav until yesterday
  names(avg_df)[names(avg_df) == value_col] <- 'value_7dav'
  avg_df_prev7 <- add_shift(avg_df, 7, refd_col)
  names(avg_df_prev7)[names(avg_df_prev7) == 'value_7dav'] <- 'value_prev_7dav'
  
  backfill_df <- Reduce(function(x, y) merge(x, y, all=TRUE), 
                        list(df, avg_df, avg_df_prev7))
  
  # Add target
  target_df <- df[df$lag==ref_lag, c(refd_col, value_col, "issue_date")]
  names(target_df)[names(target_df) == value_col] <- 'value_target'
  names(target_df)[names(target_df) == 'issue_date'] <- 'target_date'
  
  backfill_df <- merge(backfill_df, target_df, by=refd_col, all.x=TRUE)
  
  # Add log values
  backfill_df$log_value_raw = log(backfill_df$value_raw + 1)
  backfill_df$log_value_7dav = log(backfill_df$value_7dav + 1)
  backfill_df$log_value_target = log(backfill_df$value_target + 1)
  backfill_df$log_value_prev_7dav = log(backfill_df$value_prev_7dav + 1)
  backfill_df$log_7dav_slope = backfill_df$log_value_7dav - backfill_df$log_value_prev_7dav
  
  # Remove invalid rows
  backfill_df <- drop_na(backfill_df, c(lag_col))
  
  return (as.data.frame(backfill_df))
}

#' Add params related to date
#'
#' Target is the updates made ref_lag days after the first release
#'
#' @template df-template
#' @template refd_col-template
#' @template lag_col-template
add_params_for_dates <- function(df, refd_col, lag_col) {
  # Add columns for day-of-week effect
  df <- add_dayofweek(df, refd_col, "_ref", WEEKDAYS_ABBR)
  df <- add_dayofweek(df, "issue_date", "_issue", WEEKDAYS_ABBR)
  
  # Add columns for week-of-month effect
  df <- add_weekofmonth(df, "issue_date", WEEK_ISSUES)
  
  return (as.data.frame(df))
}
