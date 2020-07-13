#' Write CLI and ILI counts data for export to the API
#'
#' @param df          a data frame of survey responses
#' @param cw_list     a named list containing geometry crosswalk files from zip5 values
#' @param params      a named list with entires "s_weight", "s_mix_coef", "num_filter",
#'                    "start_time", and "end_time"
#'
#' @export
write_hh_count_data <- function(df, cw_list, params)
{
  ## TODO Auto-generate these lists (or make a data frame)
  raw_indicators <- list(
    "raw_cli" = list(
      metric = "cli",
      var_weight = "weight_unif"
    ),
    "raw_ili" = list(
      metric = "ili",
      var_weight = "weight_unif"
    ),
    "raw_wcli" = list(
      metric = "cli",
      var_weight = "weight"
    ),
    "raw_wili" = list(
      metric = "ili",
      var_weight = "weight"
    )
  )

  smoothed_indicators <- list(
    "smoothed_cli" = list(
      metric = "cli",
      var_weight = "weight_unif"
    ),
    "smoothed_ili" = list(
      metric = "ili",
      var_weight = "weight_unif"
    ),
    "smoothed_wcli" = list(
      metric = "cli",
      var_weight = "weight"
    ),
    "smoothed_wili" = list(
      metric = "ili",
      var_weight = "weight"
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
    dfs_out <- summarize_hh_count(df, geo_crosswalk, raw_indicators, geo_level, days, params)

    for (indicator in names(dfs_out)) {
      write_data_api(dfs_out[[indicator]], params, geo_level, indicator)
    }

    ## Smoothed
    dfs_out <- summarize_hh_count(df, geo_crosswalk, smoothed_indicators, geo_level, days,
                                params, smooth_days = 6L)

    for (indicator in names(dfs_out)) {
      write_data_api(dfs_out[[indicator]], params, geo_level, indicator)
    }
  }
}

#' Summarize CLI and ILI household variables at a geographic level.
#'
#' See `summarize_binary()` for a description of the architecture here and its
#' tradeoffs made in favor of high speed.
#'
#' @param df a data frame of survey responses
#' @param crosswalk_data An aggregation, such as zip => county or zip => state,
#'   as a data frame with a "zip5" column to join against.
#' @param indicators list of lists. Each constituent list has entries `metric`,
#'   `var_weight`. Its name is the name of the indicator to report in the API.
#'   `var_weight` is the name of the column of `df` to use for weights; `metric`
#'   is the name of the metric to extract from the data frame.
#' @param geo_level the aggregation level, such as county or state, being used
#' @param days a vector of Dates for which we should generate response estimates
#' @param params a named list with entries "s_weight", "s_mix_coef",
#'   "num_filter"
#' @param smooth_days integer; how many does in the past should be pooled into
#'   the estimate of a day
#'
#' @importFrom dplyr inner_join bind_rows
#' @importFrom stats weighted.mean
#' @export
summarize_hh_count <- function(
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

    out <- summarize_hh_count_day(day_df, indicators, target_day, geo_level,
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

#' Produce an estimate for all household variables on a specific target day.
#'
#' @param day_df Data frame containing all data needed to estimate one day.
#'   Estimates for `target_day` will be based on all of this data, so if the
#'   estimates are meant to be moving averages, `day_df` may contain multiple
#'   days of data.
#' @param indicators See `summarize_hh_count()`.
#' @param target_day A `Date` indicating the day for which these estimates are
#'   to be calculated.
#' @param geo_level Name of the geo level (county, state, etc.) for which we are
#'   aggregating.
#' @param params Named list of configuration options.
#' @importFrom dplyr mutate
#' @importFrom rlang .data
summarize_hh_count_day <- function(day_df, indicators, target_day, geo_level, params) {
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

  for (ii in seq_len(nrow(dfs_out[[1]])))
  {
    target_geo <- geo_ids[ii]

    sub_df <- day_df[geo_id == target_geo]

    for (indicator in names(indicators)) {
      metric <- indicators[[indicator]]$metric
      var_weight <- indicators[[indicator]]$var_weight

      ind_df <- sub_df[!is.na(sub_df[[var_weight]]), ]

      names(ind_df)[names(ind_df) == sprintf("hh_p_%s", metric)] <- "hh_p_metric"

      if (nrow(ind_df) > 0)
      {
        mixed_weights <- mix_weights(ind_df[[var_weight]] * ind_df$weight_in_location,
                                     params)

        new_row <- compute_count_response(
          response = ind_df$hh_p_metric,
          weight = mixed_weights)

        dfs_out[[indicator]]$val[ii] <- new_row$val
        dfs_out[[indicator]]$se[ii] <- new_row$se
        dfs_out[[indicator]]$sample_size[ii] <- sum(ind_df$weight_in_location)
        dfs_out[[indicator]]$effective_sample_size[ii] <- new_row$effective_sample_size
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
                                   .data$sample_size >= params$num_filter,
                                   .data$effective_sample_size >= params$num_filter)

    ## After gluing together megacounties, apply the Jeffreys correction to the
    ## standard errors.
    dfs_out[[indicator]] <- mutate(dfs_out[[indicator]],
                                   se = jeffreys_se(.data$se, .data$val,
                                                    .data$effective_sample_size))
  }

  return(dfs_out)
}


#' Returns response estimates for a single geographic area.
#'
#' This function takes vectors as input and computes the count response values
#' (a point estimate named "val", a standard error named "se", and an effective
#' sample size named "effective_sample_size").
#'
#' @param response a vector of percentages (100 * cnt / total)
#' @param weight a vector of sample weights for inverse probability weighting;
#'   invariant up to a scaling factor
#'
#' @importFrom stats weighted.mean
#' @export
compute_count_response <- function(response, weight)
{
  assert(all( response >= 0 & response <= 100 ))
  assert(length(response) == length(weight))

  weight <- weight / sum(weight)
  val <- weighted.mean(response, weight)

  effective_sample_size <- length(weight) * mean(weight)^2 / mean(weight^2)

  se <- sqrt( sum( (weight * (response - val))^2 ) )

  return(list(
    val = val,
    se = se,
    effective_sample_size = effective_sample_size
  ))
}

#' Apply Jeffreys Prior to adjust standard error values.
#'
#' The Jeffreys approach for estimating binomial proportions assumes a Beta(1/2,
#' 1/2) prior on the proportion. If x is the number of successes, the posterior
#' mean is hence (x + 0.5) / (n + 1), which prevents the estimate from ever
#' being 0 or 1. This is desirable because the typical normal approximation SE
#' would be 0 in both cases, which is both misleading and prevents reasonable
#' resampling of the data for bootstrapping.
#'
#' We apply the Jeffreys approach only to the calculation of the standard error;
#' applying it to the estimate of proportion would introduce too much bias for
#' small proportions, like we typically see for symptoms within households.
#'
#' @param old_se          a numeric vector of previous standard errors
#' @param percent         a numeric vector of the the estimated point estimates
#' @param sample_size     a numeric vector of the sample sizes
#'
#' @export
jeffreys_se <- function(old_se, percent, sample_size)
{
  sqrt((50 - percent)^2 + sample_size^2 * old_se^2) / (1 + sample_size)
}
