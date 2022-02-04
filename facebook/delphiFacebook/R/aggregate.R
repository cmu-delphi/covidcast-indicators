#' Produce aggregates for all indicators.
#'
#' Writes the outputs directly to CSVs in the directory specified by `params`.
#' Produces output for all available days between `params$start_date -
#' params$backfill_days` and `params$end_date`, inclusive. (We re-output days
#' before `start_date` in case there was backfill for those days.)
#'
#' Warning: The maximum value of `smooth_days` needs to be accounted for in
#' `run.R` in the `lead_days` argument to `filter_data_for_aggregation`, so
#' the correct amount of archived data is included, plus the expected backfill
#' length.
#'
#' @param df Data frame of individual response data.
#' @param indicators Data frame with columns `name`, `var_weight`, `metric`,
#'   `smooth_days`, `compute_fn`, `post_fn`. Each row represents one indicator
#'   to report. `name` is the indicator's API name; `var_weight` is the column
#'   to use for its weights; `metric` is the column of `df` containing the
#'   response value. `smooth_days` determines how many days to aggregate to
#'   produce one day of responses. `compute_fn` is the function that computes
#'   the aggregate response given many rows of data. `post_fn` is applied to the
#'   aggregate data after megacounty aggregation, and can perform any final
#'   calculations necessary.
#' @param cw_list Named list of geographic crosswalks, each of which maps a zip5
#'   to a geographic level such as county or state. Aggregates will be produced
#'   for each geographic level.
#' @param params Named list of configuration parameters.
#' @import data.table
#' @importFrom dplyr filter mutate_at vars select
aggregate_indicators <- function(df, indicators, cw_list, params) {
  ## Keep only columns used in indicators, plus supporting columns.
  df <- select(df,
               all_of(unique(indicators$metric)),
               all_of(unique(indicators$var_weight)),
               .data$day,
               .data$zip5
  )

  ## The data frame will include more days than just [start_date, end_date], so
  ## select just the unique days contained in that interval.
  days <- unique(df$day)
  days <- days[days >= as.Date(params$start_date) - params$backfill_days &
                 days <= as.Date(params$end_date)]

  smooth_days <- unique(indicators$smooth_days)

  ## For the day range lookups we do on df, use a data.table key. This puts the
  ## table in sorted order so data.table can use a binary search to find
  ## matching dates, rather than a linear scan, and is important for very large
  ## input files.
  df <- as.data.table(df)
  setkeyv(df, "day")

  for (i in seq_along(cw_list))
  {
    geo_level <- names(cw_list)[i]
    geo_crosswalk <- cw_list[[i]]

    for (smooth in smooth_days) {
      these_inds <- filter(indicators, .data$smooth_days == smooth)

      dfs_out <- summarize_indicators(df, geo_crosswalk, these_inds, geo_level,
                                      days, params)

      for (indicator in names(dfs_out)) {
        write_data_api(dfs_out[[indicator]], params, geo_level, indicator)
      }
    }
  }
}

#' Calculate aggregates across all days for all indicators.
#'
#' The organization may seem a bit contorted, but this is designed for speed.
#' The primary bottleneck is repeatedly filtering the data frame to find data
#' for the day and geographic area of interest. To save time, we do this once
#' and then calculate all indicators for that day-area combination, rather than
#' separately filtering every time we want to calculate a new indicator. We also
#' rely upon data.table's keys and indices to allow us to do the filtering in
#' O(log n) time, which is important when the data frame contains millions of
#' rows.
#'
#' @param df a data frame of survey responses
#' @param crosswalk_data An aggregation, such as zip => county or zip => state,
#'   as a data frame with a "zip5" column to join against.
#' @param indicators Data frame of indicators.
#' @param geo_level the aggregation level, such as county or state, being used
#' @param days a vector of Dates for which we should generate response estimates
#' @param params a named list with entries "s_weight", "s_mix_coef",
#'   "num_filter"
#'
#' @importFrom dplyr inner_join bind_rows
#' @importFrom parallel mclapply
#' @export
summarize_indicators <- function(df, crosswalk_data, indicators, geo_level,
                                 days, params) {
  ## dplyr complains about joining a data.table, saying it is likely to be
  ## inefficient; profiling shows the cost to be negligible, so shut it up
  df <- suppressWarnings(inner_join(df, crosswalk_data, by = "zip5"))

  ## Set an index on the geo_id column so that the lookup by exact geo_id can be
  ## dramatically faster; data.table stores the sort order of the column and
  ## uses a binary search to find matching values, rather than a linear scan.
  setindexv(df, "geo_id")

  ## We do batches of just one smooth_days at a time, since we have to select
  ## rows based on this.
  assert( length(unique(indicators$smooth_days)) == 1 )

  smooth_days <- indicators$smooth_days[1]

  calculate_day <- function(ii) {
    target_day <- days[ii]
    # Use data.table's index to make this filter efficient
    day_df <- df[day >= target_day - smooth_days & day <= target_day, ]

    out <- summarize_indicators_day(day_df, indicators, target_day, geo_level,
                                    params)

    return(out)
  }

  if (params$parallel) {
    dfs <- mclapply(seq_along(days), calculate_day)
  } else {
    dfs <- lapply(seq_along(days), calculate_day)
  }

  ## Now we have a list, with one entry per day, each containing a list of one
  ## data frame per indicator. Rearrange it.
  dfs_out <- list()
  for (indicator in indicators$name) {
    dfs_out[[indicator]] <- bind_rows(lapply(dfs, function(day) { day[[indicator]] }))
  }

  return(dfs_out)
}

#' Produce estimates for all indicators on a specific target day.
#' @param day_df Data frame containing all data needed to estimate one day.
#'   Estimates for `target_day` will be based on all of this data, so if the
#'   estimates are meant to be moving averages, `day_df` may contain multiple
#'   days of data.
#' @param indicators Indicators to report. See `aggregate_indicators()`.
#' @param target_day A `Date` indicating the day for which these estimates are
#'   to be calculated.
#' @param geo_level Name of the geo level (county, state, etc.) for which we are
#'   aggregating.
#' @param params Named list of configuration options.
#' 
#' @importFrom dplyr mutate filter bind_rows
#' @importFrom stats setNames
#' @importFrom rlang .data
summarize_indicators_day <- function(day_df, indicators, target_day, geo_level, params) {
  ## Prepare outputs as list of lists. Saves some time and memory since lists
  ## are not copied on modify.
  geo_ids <- unique(day_df$geo_id)
  n_geo_ids <- length(geo_ids)
  fill_list <- list(geo_id = geo_ids,
                    day = rep(target_day, n_geo_ids),
                    val = rep(NA_real_, n_geo_ids),
                    se = rep(NA_real_, n_geo_ids),
                    sample_size = rep(NA_real_, n_geo_ids),
                    effective_sample_size = rep(NA_real_, n_geo_ids)
  )
  
  dfs_out <- setNames(
    rep(list(fill_list), times=length(indicators$name)),
    indicators$name)
  
  for (ii in seq_along(geo_ids))
  {
    target_geo <- geo_ids[ii]

    sub_df <- day_df[geo_id == target_geo]

    for (row in seq_len(nrow(indicators))) {
      indicator <- indicators$name[row]
      metric <- indicators$metric[row]
      var_weight <- indicators$var_weight[row]
      compute_fn <- indicators$compute_fn[[row]]

      # Prevent smoothed weighted signals from being reported for dates after
      # the latest available weight data.
      if (target_day > params$latest_weight_date &&
          indicators$smooth_days[row] > 1 &&
          indicators$var_weight[row] != "weight_unif") {
        
        next
      }

      # Copy only columns we're using.
      select_cols <- c(metric, var_weight, "weight_in_location")
      # This filter uses `x[, cols, with=FALSE]` rather than the newer
      # recommended `x[, ..cols]` format to take advantage of better
      # performance. Switch to using `..` if `with` is deprecated in the future.
      ind_df <- sub_df[!is.na(sub_df[[var_weight]]) & !is.na(sub_df[[metric]]), select_cols, with=FALSE]
      
      if (nrow(ind_df) > 0)
      {
        s_mix_coef <- params$s_mix_coef
        mixing <- mix_weights(ind_df[[var_weight]] * ind_df$weight_in_location,
                                     s_mix_coef, params$s_weight)

        sample_size <- sum(ind_df$weight_in_location)

        new_row <- compute_fn(
          response = ind_df[[metric]],
          weight = mixing$weights,
          sample_size = sample_size)

        dfs_out[[indicator]][["val"]][ii] <- new_row$val
        dfs_out[[indicator]][["se"]][ii] <- new_row$se
        dfs_out[[indicator]][["sample_size"]][ii] <- sample_size
        dfs_out[[indicator]][["effective_sample_size"]][ii] <- new_row$effective_sample_size
      }
    }
  }
  
  for (row in seq_len(nrow(indicators))) {
    indicator <- indicators$name[row]
    post_fn <- indicators$post_fn[[row]]

    # Convert list of lists to list of tibbles.
    dfs_out[[indicator]] <- bind_rows(dfs_out[[indicator]])

    dfs_out[[indicator]] <- dfs_out[[indicator]][
      rowSums(is.na(dfs_out[[indicator]][, c("val", "sample_size", "geo_id", "day")])) == 0,
    ]

    if (geo_level == "county") {
      df_megacounties <- megacounty(dfs_out[[indicator]], params$num_filter)
      dfs_out[[indicator]] <- bind_rows(dfs_out[[indicator]], df_megacounties)
    }

    dfs_out[[indicator]] <- filter(dfs_out[[indicator]],
                                   .data$sample_size >= params$num_filter,
                                   .data$effective_sample_size >= params$num_filter)

    ## *After* gluing together megacounties, apply the Jeffreys correction to
    ## the standard errors.
    dfs_out[[indicator]] <- post_fn(dfs_out[[indicator]])
  }

  return(dfs_out)
}
