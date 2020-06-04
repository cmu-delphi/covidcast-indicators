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

  for (i in seq_along(cw_list))
  {
    df_out <- summarize_binary(df, cw_list[[i]], var_yes, "weight_unif", params)
    write_data_api(df_out, params, names(cw_list)[i], sprintf("raw_%s", metric))

    df_out <- summarize_binary(weight_df, cw_list[[i]], var_yes, "weight", params)
    write_data_api(df_out, params, names(cw_list)[i], sprintf("raw_w%s", metric))

    df_out <- summarize_binary(df, cw_list[[i]], var_yes, "weight_unif", params, 6L)
    write_data_api(df_out, params, names(cw_list)[i], sprintf("smoothed_%s", metric))

    df_out <- summarize_binary(weight_df, cw_list[[i]], var_yes, "weight", params, 6L)
    write_data_api(df_out, params, names(cw_list)[i], sprintf("smoothed_w%s", metric))
  }
}

#' Summarize binary variables at a geographic level
#'
#' @param df               a data frame of survey responses
#' @param crosswalk_data   a named list containing geometry crosswalk files from zip5 values
#' @param var_yes          name of the column in `df` containing the number of "yes" responses
#' @param var_weight       name of the column in `df` containing the survey weights
#' @param params           a named list with entries "start_time", and "end_time"
#' @param smooth_days      integer; how many days in the past to smooth?
#'
#' @importFrom dplyr inner_join group_by ungroup summarize n as_tibble
#' @importFrom stats weighted.mean
#' @importFrom rlang .data
#' @export
summarize_binary <- function(
  df, crosswalk_data, var_yes, var_weight, params, smooth_days = 0L
)
{
  df <- inner_join(df, crosswalk_data, by = "zip5")
  df <- df[!is.na(df[[var_yes]]),]

  df_out <- as_tibble(expand.grid(
    day = unique(df$day), geo_id = unique(df$geo_id), stringsAsFactors = FALSE
  ))
  df_out$val <- NA_real_
  df_out$sample_size <- NA_real_
  df_out$se <- NA_real_
  past_n_days_matrix <- past_n_days(df_out$day, smooth_days)

  for (i in seq_len(nrow(df_out)))
  {
    allowed_days <- past_n_days_matrix[i,]
    index <- which(!is.na(match(df$day, allowed_days)) & (df$geo_id == df_out$geo_id[i]))
    if (length(index))
    {
      mixed_weights <- mix_weights(df[[var_weight]][index], params)
      new_row <- compute_binary_response(
        response = df[[var_yes]][index],
        weight = mixed_weights,
        sample_weight = df$weight_in_location[index]
      )
      df_out$val[i] <- new_row$val
      df_out$sample_size[i] <- new_row$sample_size
      df_out$se[i] <- new_row$se
    }
  }

  df_out <- df_out[rowSums(is.na(df_out[, c("val", "sample_size", "geo_id", "day")])) == 0,]
  df_out <- df_out[df_out$sample_size > params$num_filter, ]
  return(df_out)
}

#' Returns binary response estimates
#'
#' This function takes vectors as input and computes the binary response values (a point
#' estimate named "val", a standard error named "se", and a sample size named "sample_size")
#' Note that there are two different sets of weights that have a different effect on the
#' output.
#'
#' @param response                    a vector of binary (0 or 1) responses
#' @param weight                      a vector of sample weights for inverse probability
#'                                    weighting; invariant up to a scaling factor
#' @param sample_weight               a vector of sample size weights; the total sample size
#'                                    will be the sum of these values
#'
#' @importFrom stats weighted.mean
#' @export
compute_binary_response <- function(response, weight, sample_weight)
{
  assert(all( (response == 0) | (response == 1) ))

  response_sum <- weighted.mean(response * sample_weight, weight) * length(response)
  sample_size <- sum(sample_weight)

  val <- 100 * (response_sum + 0.5) / (sample_size + 1)
  se <- sqrt( (val * (100 - val) / sample_size) )

  return(list(val = val, se = se, sample_size = sample_size))
}
