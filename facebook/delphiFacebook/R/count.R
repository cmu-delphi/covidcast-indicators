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
  ## weighted output files can only use surveys with weights
  weight_df <- df[!is.na(df$weight), ]

  for (i in seq_along(cw_list))
  {
    for (metric in c("ili", "cli"))
    {
      df_out <- summarize_hh_count(df, cw_list[[i]], metric, "weight_unif", params)
      write_data_api(df_out, params, names(cw_list)[i], sprintf("raw_%s", metric))

      df_out <- summarize_hh_count(df, cw_list[[i]], metric, "weight_unif", params, 6)
      write_data_api(df_out, params, names(cw_list)[i], sprintf("smoothed_%s", metric))

      df_out <- summarize_hh_count(weight_df, cw_list[[i]], metric, "weight", params)
      write_data_api(df_out, params, names(cw_list)[i], sprintf("raw_w%s", metric))

      df_out <- summarize_hh_count(weight_df, cw_list[[i]], metric, "weight", params, 6)
      write_data_api(df_out, params, names(cw_list)[i], sprintf("smoothed_w%s", metric))
    }
  }
}

#' Summarize CLI and ILI household variables at a geographic level
#'
#' @param df               a data frame of survey responses
#' @param crosswalk_data   a named list containing geometry crosswalk files from zip5 values
#' @param metric           name of the metric to use; should be "ili" or "cli"
#' @param var_weight       name of the variable containing the survey weights
#' @param params           a named list with entries "s_weight", "s_mix_coef", "num_filter",
#'                         "start_time", and "end_time"
#' @param smooth_days      integer; how many does in the past should be pooled into the
#'                         estimate of a day
#'
#' @importFrom dplyr inner_join group_by mutate n case_when first
#' @importFrom stats weighted.mean
#' @importFrom rlang .data
#' @export
summarize_hh_count <- function(
  df, crosswalk_data, metric, var_weight, params, smooth_days = 0L
)
{
  df <- inner_join(df, crosswalk_data, by = "zip5")
  names(df)[names(df) == sprintf("hh_p_%s", metric)] <- "hh_p_metric"

  df_out <- as_tibble(expand.grid(
    day = unique(df$day), geo_id = unique(df$geo_id), stringsAsFactors = FALSE
  ))
  df_out$val <- NA_real_
  df_out$sample_size <- NA_real_
  df_out$se <- NA_real_
  df_out$effective_sample_size <- NA_real_
  past_n_days_matrix <- past_n_days(df_out$day, smooth_days)

  for (i in seq_len(nrow(df_out)))
  {
    allowed_days <- past_n_days_matrix[i,]
    index <- which(!is.na(match(df$day, allowed_days)) & (df$geo_id == df_out$geo_id[i]))
    if (length(index))
    {
      mixed_weights <- mix_weights(df[[var_weight]][index], params)
      new_row <- compute_count_response(
        response = df$hh_p_metric[index],
        weight = df[[var_weight]][index],
        sample_weight = df$weight_in_location[index]
      )
      df_out$val[i] <- new_row$val
      df_out$sample_size[i] <- new_row$sample_size
      df_out$se[i] <- new_row$se
      df_out$effective_sample_size[i] <- new_row$effective_sample_size
    }
  }

  df_out <- df_out[rowSums(is.na(df_out[, c("val", "sample_size", "geo_id", "day")])) == 0,]
  df_out <- df_out[df_out$sample_size >= params$num_filter, ]
  return(df_out)
}

#' Returns county response estimates
#'
#' This function takes vectors as input and computes the count response values (a point
#' estimate named "val", a standard error named "se", and a sample size named "sample_size")
#' Note that there are two different sets of weights that have a different effect on the
#' output.
#'
#' @param response                    a vector of percentages (100 * cnt / total)
#' @param weight                      a vector of sample weights for inverse probability
#'                                    weighting; invariant up to a scaling factor
#' @param sample_weight               a vector of sample size weights
#'
#' @importFrom stats weighted.mean
#' @export
compute_count_response <- function(response, weight, sample_weight)
{
  assert(all( response >= 0 & response <= 100 ))

  weight <- weight / sum(weight) * length(weight)
  val <- weighted.mean(response * weight, sample_weight)

  sample_size <- sum(sample_weight)
  w <- sample_weight * weight
  effective_sample_size <- mean(w)^2 / mean(w^2) * length(sample_weight)

  se <- sqrt( sum(weight^2 * (response - val)^2 ) / length(weight)^2 )
  se <- jeffreys_se(se, val, effective_sample_size)

  # EffectiveNumberResponses = dplyr::n()*mean(HouseholdMixedWeight)^2/mean(HouseholdMixedWeight^2)
  # HouseholdMixedWeight = MixingCoefficient/dplyr::n() + (1-MixingCoefficient)*HouseholdNormalizedPreweight

  return(list(
    val = val,
    se = se,
    sample_size = sample_size,
    effective_sample_size = effective_sample_size
  ))
}

#' Apply Jeffrey's Prior to correct standard error values
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
