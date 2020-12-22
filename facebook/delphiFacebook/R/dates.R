#' Get the date of the first day of the previous month
#'
#' @param date Date of interest
#' 
#' @return Date
#' 
#' @importFrom lubridate floor_date
#' 
#' @export
start_of_prev_full_month <- function(date) {
  return(floor_date(date, "month") - months(1))
}

#' Get the date of the last day of the previous month
#'
#' @param date Date of interest
#' 
#' @return Date
#' 
#' @importFrom lubridate ceiling_date days
#' 
#' @export
end_of_prev_full_month <- function(date) {
  if (ceiling_date(date, "month") == date) {
    return(date)
  }
  
  return(floor_date(date, "month") - days(1))
}

#' Get the date range specifying the previous month
#'
#' @param date Date of interest
#' 
#' @return list of two Dates
#' 
#' @export
get_range_prev_full_month <- function(date = Sys.Date()) {
  eom <- end_of_prev_full_month(date)
  
  if (eom == date) {
    som <- start_of_prev_full_month(date + months(1))
  } else {
    som <- start_of_prev_full_month(date)
  }
  
  return(list(som, eom))
}

#' Get the date of the first day of the previous epiweek
#'
#' @param date Date of interest
#' 
#' @return Date
#' 
#' @importFrom lubridate floor_date weeks
#' 
#' @export
start_of_prev_full_week <- function(date) {
  return(floor_epiweek(date) - weeks(1))
}

#' Get the date of the last day of the previous epiweek
#'
#' @param date Date of interest
#' 
#' @return Date
#' 
#' @importFrom lubridate ceiling_date days
#' 
#' @export
end_of_prev_full_week <- function(date) {
  if (ceiling_epiweek(date) == date) {
    return(date)
  }
  
  return(floor_epiweek(date) - days(1))
}

#' Get the date range specifying the previous week
#'
#' @param date Date of interest
#' 
#' @return list of two Dates
#' 
#' @importFrom lubridate weeks
#' 
#' @export
get_range_prev_full_week <- function(date = Sys.Date()) {
  eow <- end_of_prev_full_week(date)
  
  if (eow == date) {
    sow <- start_of_prev_full_week(date + weeks(1))
  } else {
    sow <- start_of_prev_full_week(date)
  }
  
  return(list(sow, eow))
}

#' Get the date range specifying the previous full time period
#'
#' @param date Date of interest
#' @param weekly_or_monthly_flag string "week" or "month" indicating desired
#' time period to aggregate over
#' 
#' @return list of two Dates
#' 
#' @importFrom lubridate ymd_hms
#' 
#' @export
get_range_prev_full_period <- function(date = Sys.Date(), weekly_or_monthly_flag) {
  if (weekly_or_monthly_flag == "month") {
    # Get start and end of previous full month.
    date_period_range = get_range_prev_full_month(date)
  } else if (weekly_or_monthly_flag == "week") {
    # Get start and end of previous full epiweek.
    date_period_range = get_range_prev_full_week(date)
  }
  
  date_period_range[[1]] =  ymd_hms(
    sprintf("%s 00:00:00", date_period_range[[1]]), tz = "America/Los_Angeles"
  )
  date_period_range[[2]] =  ymd_hms(
    sprintf("%s 23:59:59", date_period_range[[2]]), tz = "America/Los_Angeles"
  )
  
  return(date_period_range)
}

#' epiweek equivalent of `week<-` as shown [here](https://lubridate.tidyverse.org/reference/week.html#examples)
#' 
#' @param x a date-time object. Must be a POSIXct, POSIXlt, Date, chron, 
#' yearmon, yearqtr, zoo, zooreg, timeDate, xts, its, ti, jul, timeSeries, or 
#' fts object.
#' @param value a numeric object
#' 
#' @return date
#' 
#' @importFrom lubridate epiweek days
#' 
#' @export
"epiweek<-" <- function(x, value)
  x <- x + days((value - epiweek(x)) * 7)

#' Get date of the first day of the epiweek `x` falls in
#' 
#' @param x date
#' 
#' @return date
#' 
#' @importFrom lubridate epiweek
#' 
#' @export
floor_epiweek <- function(x) {
  epiweek(floor_x) <- epiweek(x)
  return(floor_x)
}

#' Get date of the last day of the epiweek `x` falls in
#' 
#' @param x date
#' 
#' @return date
#' 
#' @importFrom lubridate epiweek days weeks
#' 
#' @export
ceiling_epiweek <- function(x) {
  epiweek(floor_next_week) <- epiweek(x + weeks(1))
  return(floor_next_week - days(1))
}
