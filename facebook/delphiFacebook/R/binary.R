#' Write binary response variable for export to the API
#'
#' @param df          a data frame of survey responses
#' @param cw_list     a named list containing geometry crosswalk files from zip5 values
#' @param var_yes     name of the variable containing the binary response
#' @param params      a named list with entries "start_time", and "end_time"
#' @param metric      name of the metric; used in the output file
#'
#' @export
write_binary_variable <- function(df, cw_list, var_yes, params, metric)
{
  ## weighted output files can only use surveys with weights
  weight_df <- df[!is.na(df$weight), ]

  ## TODO Temporary fix to match old pipeline
  params$s_mix_coef <- 0

  for (i in seq_along(cw_list))
  {
    geo_level <- names(cw_list)[i]
    geo_crosswalk <- cw_list[[i]]

    df_out <- summarize_binary(df, geo_crosswalk, var_yes, "weight_unif",
                               geo_level, params)
    write_data_api(df_out, params, geo_level, sprintf("raw_%s", metric))

    df_out <- summarize_binary(weight_df, geo_crosswalk, var_yes, "weight",
                               geo_level, params)
    write_data_api(df_out, params, geo_level, sprintf("raw_w%s", metric))

    df_out <- summarize_binary(df, geo_crosswalk, var_yes, "weight_unif",
                               geo_level, params, 6L)
    write_data_api(df_out, params, geo_level, sprintf("smoothed_%s", metric))

    df_out <- summarize_binary(weight_df, geo_crosswalk, var_yes, "weight",
                               geo_level, params, 6L)
    write_data_api(df_out, params, geo_level, sprintf("smoothed_w%s", metric))
  }
}

#' Summarize binary variables at a geographic level
#'
#' @param df               a data frame of survey responses
#' @param crosswalk_data   a named list containing geometry crosswalk files from zip5 values
#' @param var_yes          name of the column in `df` containing the number of "yes" responses
#' @param var_weight       name of the column in `df` containing the survey weights
#' @param geo_level        the aggregation level, such as county or state, being used
#' @param params           a named list with entries "start_time", and "end_time"
#' @param smooth_days      integer; how many days in the past to smooth?
#'
#' @importFrom dplyr inner_join group_by ungroup summarize n as_tibble bind_rows
#' @importFrom stats weighted.mean
#' @importFrom rlang .data
#' @export
summarize_binary <- function(
  df, crosswalk_data, var_yes, var_weight, geo_level, params, smooth_days = 0L
)
{
  df <- inner_join(df, crosswalk_data, by = "zip5")
  df <- df[!is.na(df[[var_yes]]),]

  df_out <- as_tibble(expand.grid(
    day = unique(df$day), geo_id = unique(df$geo_id), stringsAsFactors = FALSE
  ))
  df_out$val <- NA_real_
  df_out$se <- NA_real_
  df_out$sample_size <- NA_real_
  df_out$effective_sample_size <- NA_real_

  for (i in seq_len(nrow(df_out)))
  {
    index <- which((df$day >= df_out$day[i] - smooth_days) &
                     (df$day <= df_out$day[i]) &
                     (df$geo_id == df_out$geo_id[i]) &
                     !is.na(df$A4))

    if (length(index))
    {
      mixed_weights <- mix_weights(df[[var_weight]][index] * df$weight_in_location[index],
                                   params)

      sample_size <- sum(df$weight_in_location[index])

      val <- compute_binary_response(
        response = df[[var_yes]][index],
        weight = mixed_weights,
        sample_size = sample_size)

      ## We don't bother with SE here, because it needs to be calculated after
      ## the megacounty aggregation anyway, with Jeffreys correction reapplied.
      df_out$val[i] <- val
      df_out$sample_size[i] <- sample_size
      df_out$effective_sample_size[i] <- sample_size # TODO FIXME
    }
  }

  df_out <- df_out[rowSums(is.na(df_out[, c("val", "sample_size", "geo_id", "day")])) == 0,]

  if (geo_level == "county") {
    df_megacounties <- megacounty(df_out, params$num_filter)
    df_out <- bind_rows(df_out, df_megacounties)
  }

  df_out <- df_out[df_out$sample_size >= params$num_filter &
                 df_out$effective_sample_size >= params$num_filter, ]

  ## Now we must apply the Jeffreys correction to proportions and recalculate
  ## the standard error.
  df_out <- mutate(df_out,
                   val = jeffreys_percentage(.data$val, .data$sample_size))

  df_out <- mutate(df_out,
                   se = binary_se(.data$val, .data$sample_size))

  return(df_out)
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
