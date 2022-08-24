## Data Preprocessing
## 
## The raw input data should have 4/5 basic columns:
## time_value: reference date
## issue_date: issue date/date of reporting
## geo_value: location
## lag: the number of days between issue date and the reference date
## counts: the number of counts used for estimation


#' Re-index, fill na, make sure all reference date have enough rows for updates
#' @param df Data Frame of aggregated counts within a single location 
#'    reported for each reference date and issue date.
#' @param refd_col column name for the column of reference date
#' @param lag_col column name for the column of lag
#' @param min_refd the earliest reference date considered in the data
#' @param max_refd the latest reference date considered in the data
#' 
#' @return df_new Data Frame with filled rows for missing lags
#'
#' @importFrom tidyr crossing
#' @importFrom stats setNames
#'
#' @export
fill_rows <- function(df, refd_col, lag_col, min_refd, max_refd, ref_lag = REF_LAG){
  lags <- min(df[[lag_col]]): ref_lag # Full list of lags
  refds <- seq(min_refd, max_refd, by="day") # Full list reference date
  row_inds_df <- as.data.frame(crossing(refds, lags)) %>%
    setNames(c(refd_col, lag_col))
  df_new = merge(x=df, y=row_inds_df,
                 by=c(refd_col, lag_col),  all.y=TRUE)
  return (df_new)
}

#' Get pivot table, filling NANs. If there is no update on issue date D but 
#' previous reports exist for issue date D_p < D, all the dates between
#' [D_p, D] are filled with with the reported value on date D_p. If there is 
#' no update for any previous issue date, fill in with 0.
#' @param df Data Frame of aggregated counts within a single location 
#'    reported for each reference date and issue date.
#' @param value_col column name for the column of counts
#' @param refd_col column name for the column of reference date
#' @param lag_col column name for the column of lag
#' 
#' @importFrom tidyr fill pivot_wider pivot_longer
#' @importFrom dplyr %>% everything select
#' 
#' @export
fill_missing_updates <- function(df, value_col, refd_col, lag_col) {
  pivot_df <- df[order(df[[lag_col]], decreasing=FALSE), ] %>%
    pivot_wider(id_cols=lag_col, names_from=refd_col, values_from=value_col)
  
  if (any(diff(pivot_df[[lag_col]]) != 1)) {
    stop("Risk exists in forward fill")
  }
  pivot_df <- pivot_df %>% fill(everything(), .direction="down")
  
  # Fill NAs with 0s
  pivot_df[is.na(pivot_df)] <- 0
  
  backfill_df <- pivot_df %>%
    pivot_longer(-lag_col, values_to="value_raw", names_to=refd_col)
  backfill_df[[refd_col]] = as.Date(backfill_df[[refd_col]])
  
  return (as.data.frame(backfill_df))
}

#' Calculate 7 day moving average for each issue date
#' The 7dav for date D reported on issue date D_i is the average from D-7 to D-1
#' @param pivot_df Data Frame where the columns are issue dates and the rows are 
#'    reference dates
#' @param refd_col column name for the column of reference date
#' 
#' @importFrom zoo rollmeanr
#' 
#' @export
get_7dav <- function(pivot_df, refd_col){
  for (col in colnames(pivot_df)){
    if (col == refd_col) next
    pivot_df[, col] <- rollmeanr(pivot_df[, col], 7, align="right", fill=NA)
  }
  backfill_df <- pivot_df %>%
    pivot_longer(-refd_col, values_to="value_raw", names_to="issue_date")
  backfill_df[[refd_col]] = as.Date(backfill_df[[refd_col]])
  backfill_df[["issue_date"]] = as.Date(backfill_df[["issue_date"]])
  return (as.data.frame(backfill_df))
}

#' Used for data shifting in terms of reference date
#' 
#' @param df Data Frame of aggregated counts within a single location 
#'    reported for each reference date and issue date.
#' @param n_day number of days to be shifted
#' @param refd_col column name for the column of reference date
#' 
#' @export
add_shift <- function(df, n_day, refd_col){
  df[, refd_col] <- as.Date(df[, refd_col]) + n_day
  return (df)
}

#' Add one hot encoding for day of a week info in terms of reference
#' and issue date
#' 
#' @param df Data Frame of aggregated counts within a single location 
#'    reported for each reference date and issue date.
#' @param wd vector of days of a week
#' @param time_col column used for the date, can be either reference date or 
#'    issue date
#' @param suffix suffix added to indicate which kind of date is used
#' 
#' @export
add_dayofweek <- function(df, wd = weekdays_abbr, time_col, suffix){
  dayofweek <- as.numeric(format(df[[time_col]], format="%u"))
  for (i in 1:6){
    df[, paste0(wd[i], suffix)] <- as.numeric(dayofweek == i)
  }
  if (suffix == "_ref"){
    df[, paste0("Sun", suffix)] <- as.numeric(dayofweek == 7)
  }
  return (df)
}

#' Get week of a month info according to a date
#' All the dates on or before the ith Sunday but after the (i-1)th Sunday
#' is considered to be the ith week. Notice that the dates in the 5th week
#' this month are actually in the same week with the dates in the 1st week
#' next month and those dates are sparse. Thus, we assign the dates in the 
#' 5th week to the 1st week.
#' 
#' @param date as.Date
#' 
#' @importFrom lubridate make_date year month day
#' 
#' @return a integer indicating which week it is in a month
get_weekofmonth <- function(date){
  year <- year(date)
  month <- month(date)
  day <- day(date)
  firstdayofmonth <- as.numeric(format(make_date(year, month, 1), format="%u"))
  return (((day + firstdayofmonth - 1) %/% 7) %% 5 + 1)
}

#' Add one hot encoding for week of a month info in terms of issue date
#' 
#' @param df Data Frame of aggregated counts within a single location 
#'    reported for each reference date and issue date.
#' @param wm vector of weeks of a month
#' @param time_col column used for the date, can be either reference date or 
#'    issue date
#' 
#' @export
add_weekofmonth <- function(df, wm = week_issues, time_col){
  weekofmonth <- get_weekofmonth(df[[time_col]])
  for (i in 1:3){
    df[, paste0(wm[i])] <- as.numeric(weekofmonth == i)
  }
  return (df)
}

#' Add 7dav and target to the data
#' Target is the updates made ref_lag days after the first release
#' @param df Data Frame of aggregated counts within a single location 
#'    reported for each reference date and issue date.
#' @param value_col column name for the column of raw value
#' @param refd_col column name for the column of reference date
#' @param lag_col column name for the column of lag
#' 
#' @importFrom dplyr %>%
#' @importFrom tidyr pivot_wider drop_na
#' 
#' @export
add_7davs_and_target <- function(df, value_col, refd_col, lag_col, ref_lag){
  
  df$issue_date <- df[[refd_col]] + df[[lag_col]]
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
  target_df <- df[df$lag==ref_lag, c(refd_col, "value_raw", "issue_date")]
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
  backfill_df <- backfill_df %>% drop_na(c(lag_col))
  
  return (as.data.frame(backfill_df))
}

#' Add params related to date
#' Target is the updates made ref_lag days after the first release
#' @param df Data Frame of aggregated counts within a single location 
#'    reported for each reference date and issue date.
#' @param refd_col column name for the column of reference date
#' @param lag_col column name for the column of lag
add_params_for_dates <- function(backfill_df, refd_col, lag_col){
  # Add columns for day-of-week effect
  backfill_df <- add_dayofweek(backfill_df, wd, refd_col, "_ref")
  backfill_df <- add_dayofweek(backfill_df, wd, "issue_date", "_issue")
  
  # Add columns for week-of-month effect
  backfill_df <- add_weekofmonth(backfill_df, wm, "issue_date")
  
  return (as.data.frame(backfill_df))
}

#' Add columns to indicate the scale of value at square root level
#' 
#' @param train_data Data Frame for training
#' @param test_data Data Frame for testing
#' @param value_col the column name of the considered value
#' @param the maximum value in the training data at square root level
add_sqrtscale <- function(train_data, test_data, max_raw, value_col){
  sqrtscale = c()
  sub_max_raw = sqrt(max(train_data$value_raw)) / 2
  
  for (split in seq(0, 3)){
    if (sub_max_raw < (max_raw * (split+1) * 0.1)) break
    train_data[paste0("sqrty", as.character(split))] = 0
    test_data[paste0("sqrty", as.character(split))] = 0
    qv_pre = max_raw * split * 0.2
    qv_next = max_raw * (split+1) * 0.2

    train_data[(train_data$value_raw <= (qv_next)^2)
               & (train_data$value_raw > (qv_pre)^2), paste0("sqrty", as.character(split))] = 1
    test_data[(test_data$value_raw <= (qv_next)^2)
              & (test_data$value_raw > (qv_pre)^2), paste0("sqrty", as.character(split))] = 1
    sqrtscale[split+1] = paste0("sqrty", as.character(split))
  }
  
  return (list(train_data, test_data, sqrtscale))
}


