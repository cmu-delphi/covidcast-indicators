#' Get the date of the first day of the previous month
#'
#' @param date Date that output will be calculated relative to
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
#' @param date Date that output will be calculated relative to
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
#' @param date Date that output will be calculated relative to
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


#' Get the date of the first day of the previous week
#'
#' @param date Date that output will be calculated relative to
#' 
#' @return Date
#' 
#' @importFrom lubridate floor_date weeks
#' 
#' @export
start_of_prev_full_week <- function(date) {
  return(floor_date(date, "week") - weeks(1))
}


#' Get the date of the last day of the previous week
#'
#' @param date Date that output will be calculated relative to
#' 
#' @return Date
#' 
#' @importFrom lubridate ceiling_date days
#' 
#' @export
end_of_prev_full_week <- function(date) {
  if (ceiling_date(date, "week") == date) {
    return(date)
  }
  
  return(floor_date(date, "week") - days(1))
}


#### TODO: should be epiweeks eventually. Already exists a package to calculate?
#' Get the date range specifying the previous week
#'
#' @param date Date that output will be calculated relative to
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
#' @param date Date that output will be calculated relative to
#' @param weekly_or_monthly_flag string "weekly" or "monthly" indicating desired
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
  } else if (weekly_or_monthly_flag == "epiweek") {
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