#' Write binary response variables for export to the API
#'
#' @param df          a data frame of survey responses
#' @param cw_list     a named list containing geometry crosswalk files from zip5 values
#' @param params      a named list with entries "start_time", and "end_time"
#'
#' @import data.table
#' @importFrom dplyr pull
#' @export
write_binary_variables <- function(df, cw_list, params)
{
  ## TODO Temporary fix to match old pipeline
  params$s_mix_coef <- 0

  ## TODO Auto-generate these lists (or make a data frame)
  raw_indicators <- list(
    "raw_hh_cmnty_cli" = list(
      var_weight = "weight_unif",
      var_yes = "hh_community_yes"
    ),
    "raw_whh_cmnty_cli" = list(
      var_weight = "weight",
      var_yes = "hh_community_yes"
    ),
    "raw_nohh_cmnty_cli" = list(
      var_weight = "weight_unif",
      var_yes = "community_yes"
    ),
    "raw_wnohh_cmnty_cli" = list(
      var_weight = "weight",
      var_yes = "community_yes"
    )
  )

  smoothed_indicators <- list(
    "smoothed_hh_cmnty_cli" = list(
      var_weight = "weight_unif",
      var_yes = "hh_community_yes"
    ),
    "smoothed_whh_cmnty_cli" = list(
      var_weight = "weight",
      var_yes = "hh_community_yes"
    ),
    "smoothed_nohh_cmnty_cli" = list(
      var_weight = "weight_unif",
      var_yes = "community_yes"
    ),
    "smoothed_wnohh_cmnty_cli" = list(
      var_weight = "weight",
      var_yes = "community_yes"
    )
  )

  days <- unique(df$day)

  ## For the day range lookups we do on df, use a data.table key. This puts the
  ## table in sorted order so data.table can use a binary search to find
  ## matching dates, rather than a linear scan, and is important for very large
  ## input files.
  df <- as.data.table(df)
  setkey(df, day)

  for (i in seq_along(cw_list))
  {
    geo_level <- names(cw_list)[i]
    geo_crosswalk <- cw_list[[i]]

    ## Raw
    dfs_out <- summarize_binary(df, geo_crosswalk, raw_indicators, geo_level, days, params)

    for (indicator in names(dfs_out)) {
      write_data_api(dfs_out[[indicator]], params, geo_level, indicator)
    }

    ## Smoothed
    dfs_out <- summarize_binary(df, geo_crosswalk, smoothed_indicators, geo_level, days,
                                params, smooth_days = 6L)

    for (indicator in names(dfs_out)) {
      write_data_api(dfs_out[[indicator]], params, geo_level, indicator)
    }
  }
}

#' Summarize binary variables at a geographic level.
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
#'   as a data frame with "zip5" column to join against.
#' @param indicators list of lists. Each constituent list has entries
#'   `var_weight`, `var_yes`. Its name is the name of the indicator to report in
#'   the API. `var_weight` is the name of the column of `df` to use for weights;
#'   `var_yes` is the name of the column containing the binary responses.
#' @param geo_level the aggregation level, such as county or state, being used
#' @param days a vector of Dates for which we should generate response estimates
#' @param params a named list with entries controlling mixing and filtering
#' @param smooth_days integer; how many days in the past to smooth?
#'
#' @importFrom dplyr bind_rows
#' @importFrom parallel mclapply
#' @export
summarize_binary <- function(
  df, crosswalk_data, indicators, geo_level, days, params, smooth_days = 0L
  )
{
  ## dplyr complains about joining a data.table, saying it is likely to be
  ## inefficient; profiling shows the cost to be negligible, so shut it up
  df <- suppressWarnings(inner_join(df, crosswalk_data, by = "zip5"))

  ## Set an index on the geo_id column so that the lookup by exact geo_id can be
  ## dramatically faster; data.table stores the sort order of the column and
  ## uses a binary search to find matching values, rather than a linear scan.
  setindex(df, geo_id)

  calculate_day <- function(ii) {
    target_day <- days[ii]
    # Use data.table's index to make this filter efficient
    day_df <- df[day >= target_day - smooth_days & day <= target_day, ]

    out <- summarize_binary_day(day_df, indicators, target_day, geo_level,
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
  for (indicator in names(indicators)) {
    dfs_out[[indicator]] <- bind_rows(lapply(dfs, function(day) { day[[indicator]] }))
  }

  return(dfs_out)
}

#' Produce an estimate for all binary indicators on a specific target day.
#'
#' @param day_df Data frame containing all data needed to estimate one day.
#'   Estimates for `target_day` will be based on all of this data, so if the
#'   estimates are meant to be moving averages, `day_df` may contain multiple
#'   days of data.
#' @param indicators See `summarize_binary()`.
#' @param target_day A `Date` indicating the day for which these estimates are
#'   to be calculated.
#' @param geo_level Name of the geo level (county, state, etc.) for which we are
#'   aggregating.
#' @param params Named list of configuration options.
#' @importFrom dplyr inner_join filter
#' @importFrom rlang .data
summarize_binary_day <- function(day_df, indicators, target_day, geo_level,
                                 params) {
  ## Prepare outputs.
  dfs_out <- list()
  geo_ids <- unique(day_df$geo_id)
  for (indicator in names(indicators)) {
    dfs_out[[indicator]] <- tibble(
      geo_id = geo_ids,
      day = target_day,
      val = NA_real_,
      se = NA_real_,
      sample_size = NA_real_,
      effective_sample_size = NA_real_
    )
  }

  for (ii in seq_len(nrow(dfs_out[[1]]))) {
    target_geo <- geo_ids[ii]

    sub_df <- day_df[geo_id == target_geo]

    for (indicator in names(indicators)) {
      var_yes <- indicators[[indicator]]$var_yes
      var_weight <- indicators[[indicator]]$var_weight

      ind_df <- sub_df[!is.na(sub_df[[var_yes]]) & !is.na(sub_df[[var_weight]]), ]

      if (nrow(ind_df) > 0) {
        mixed_weights <- mix_weights(ind_df[[var_weight]] * ind_df$weight_in_location,
                                     params)

        sample_size <- sum(ind_df$weight_in_location)

        val <- compute_binary_response(
          response = ind_df[[var_yes]],
          weight = mixed_weights,
          sample_size = sample_size
        )

        dfs_out[[indicator]]$val[ii] <- val
        dfs_out[[indicator]]$sample_size[ii] <- sample_size
        dfs_out[[indicator]]$effective_sample_size[ii] <- sample_size
      }
    }
  }

  for (indicator in names(indicators)) {
    dfs_out[[indicator]] <- dfs_out[[indicator]][rowSums(is.na(dfs_out[[indicator]][, c("val", "sample_size", "geo_id", "day")])) == 0,]

    if (geo_level == "county") {
      df_megacounties <- megacounty(dfs_out[[indicator]], params$num_filter)
      dfs_out[[indicator]] <- bind_rows(dfs_out[[indicator]], df_megacounties)
    }

    dfs_out[[indicator]] <- filter(dfs_out[[indicator]],
                                   sample_size >= params$num_filter,
                                   effective_sample_size >= params$num_filter)

    dfs_out[[indicator]] <- mutate(dfs_out[[indicator]],
                                   val = jeffreys_percentage(.data$val, .data$sample_size),
                                   se = binary_se(.data$val, .data$sample_size))
  }

  return(dfs_out)
}

#' Returns binary response estimates
#'
#' This function takes vectors as input and computes the binary response values
#' (a point estimate named "val", a standard error named "se", and a sample size
#' named "sample_size").
#'
#' @param response a vector of binary (0 or 1) responses
#' @param weight a vector of sample weights for inverse probability weighting;
#'   invariant up to a scaling factor
#' @param sample_size The sample size to use, which may be a non-integer (as
#'   responses from ZIPs that span geographical boundaries are weighted
#'   proportionately, and survey weights may also be applied)
#'
#' @importFrom stats weighted.mean
#' @export
compute_binary_response <- function(response, weight, sample_size)
{
  assert(all( (response == 0) | (response == 1) ))
  assert(length(response) == length(weight))

  response_prop <- weighted.mean(response, weight)

  val <- 100 * response_prop

  return(val)
}

#' Adjust a percentage estimate to use the Jeffreys method.
#'
#' Takes a previously estimated percentage (calculated with num_yes / total *
#' 100) and replaces it with the Jeffreys version, where one pseudo-observation
#' with 50% yes is inserted.
#'
#' @param percentage Vector of percentages to adjust.
#' @param sample_size Vector of corresponding sample sizes.
#' @return Vector of adjusted percentages.
jeffreys_percentage <- function(percentage, sample_size) {
  return((percentage * sample_size + 50) / (sample_size + 1))
}

#' Calculate the standard error for a binary proportion (as a percentage)
#'
#' @param val Vector of estimated percentages
#' @param sample_size Vector of corresponding sample sizes
#' @return Vector of standard errors; NA when a sample size is 0.
binary_se <- function(val, sample_size) {
  return(ifelse(sample_size > 0,
                sqrt( (val * (100 - val) / sample_size) ),
                NA))
}
