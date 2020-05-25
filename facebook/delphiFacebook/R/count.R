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

  for (i in seq_along(cw_list))
  {
    for (metric in c("ili", "cli"))
    {
      df_out <- summarize_hh_count(df, cw_list[[i]], metric, "weight_unif", params)
      write_data_api(df_out, params, names(cw_list)[i], sprintf("raw_%s", metric))

      df_out <- summarize_hh_count(df, cw_list[[i]], metric, "weight_unif", params, TRUE)
      write_data_api(df_out, params, names(cw_list)[i], sprintf("smooth_%s", metric))

      df_out <- summarize_hh_count(df, cw_list[[i]], metric, "weight", params)
      write_data_api(df_out, params, names(cw_list)[i], sprintf("raw_w%s", metric))

      df_out <- summarize_hh_count(df, cw_list[[i]], metric, "weight", params, TRUE)
      write_data_api(df_out, params, names(cw_list)[i], sprintf("smooth_w%s", metric))
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
#' @param smooth           logical; should the results by smoothed?
#'
#' @importFrom dplyr inner_join group_by mutate n case_when first
#' @importFrom stats weighted.mean
#' @importFrom rlang .data
summarize_hh_count <- function(
  df, crosswalk_data, metric, var_weight, params, smooth = FALSE
)
{
  df <- inner_join(df, crosswalk_data, by = "zip5")
  names(df)[names(df) == sprintf("hh_p_%s", metric)] <- "hh_p_metric"
  names(df)[names(df) == var_weight] <- "w"

  df$hh_preweight <- df$w * df$weight_in_location

  df <- group_by(df, .data$day, .data$geo_id)
  df <- mutate(
    df,
    hh_normalize_preweight = .data$hh_preweight / sum(.data$hh_preweight),
    n = n(),
    maxp = max(.data$hh_normalize_preweight),
    precoefficient = case_when(
      maxp <= params$s_weight ~ 0,
      1 / n > params$s_weight * 0.999  ~ 1,
      TRUE ~ (.data$maxp * n - params$s_weight * 0.999 * n + 1e-6) / (n - 1 + 1e-6)
    ),
    mix_coef = pmax(.data$precoefficient, params$s_mix_coef, na.rm = FALSE),
    hh_mixed_weight = .data$mix_coef / n + (1 + .data$mix_coef) * .data$hh_normalize_preweight
  )

  df_summary <- ungroup(summarize(
    df,
    val = weighted.mean(.data$hh_p_metric, .data$hh_mixed_weight),
    se = sqrt(sum( (.data$hh_mixed_weight * (.data$hh_p_metric-.data$val))^2 )),
    se_use = diff(range(.data$hh_p_metric)) >= 1e-10,
    denominator = sum(.data$hh_number_total * .data$weight_in_location),
    hh_mixed_weight_sum = sum(.data$hh_mixed_weight),
    hh_mixed_weight_max = max(.data$hh_mixed_weight),
    mix_coef = first(.data$mix_coef),
    n_response = sum(.data$weight_in_location),
    sample_size = n() * mean(.data$hh_mixed_weight)^2 / mean(.data$hh_mixed_weight^2)
  ))

  if (smooth) { df_summary <- apply_count_smoothing(df_summary, params) }

  df_summary <- df_summary[df_summary$n_response > params$num_filter,]
  df_summary <- df_summary[df_summary$sample_size > params$num_filter,]
  df_summary <- df_summary[df_summary$hh_mixed_weight_max <= params$s_weight,]
  df_summary$se <- jeffreys_se(df_summary$se, df_summary$val, df_summary$sample_size)

  rowsum_na <- rowSums(is.na(df_summary[, c("val", "sample_size", "geo_id", "day")]))
  df_summary <- df_summary[rowsum_na == 0,]

  return(df_summary)
}

#' Applying smoothing to household counts data (CLI and ILI)
#'
#' @param df            input data frame of summerized data
#' @param params        a named list containing entried "start_time" and "end_time"
#' @param k             integer
#' @param max_window    pair of integers
#'
#' @importFrom tidyr complete
#' @importFrom dplyr group_by arrange mutate ungroup filter select
#' @importFrom zoo rollapplyr rollsumr
#' @importFrom rlang .data
#' @export
apply_count_smoothing <- function(df, params, k = 3L, max_window = c(1L, 7L))
{

  day_set <- format(
    seq(as.Date(params$start_time), as.Date(params$end_time), by = '1 day'), "%Y%m%d"
  )

  roll_sum <- function(val) rollapplyr(val, k, sum, partial = TRUE, na.rm = TRUE)
  roll_max <- function(val) {
    rollapplyr(val, k, function(x, ...) max(0, x, ...), partial = TRUE, na.rm = TRUE)
  }

  df_complete <- complete(df, day = day_set, .data$geo_id)
  df_complete <- group_by(df_complete, .data$geo_id)
  df_complete <- arrange(df_complete, .data$day)
  df_complete <- mutate(
    df_complete,
    .data$day,
    valid = !is.na(.data$sample_size),
    row_w = .data$sample_size,
    window_w = roll_sum(.data$row_w),
    val = pmax(0, roll_sum(.data$row_w * .data$val) / .data$window_w),
    se = sqrt(pmax(0, roll_sum(.data$se^2 * .data$row_w^2) / .data$window_w^2)),
    se_use = roll_sum(.data$se_use) > 1e-3,
    n_response = roll_sum(.data$n_response),
    sample_size = roll_sum(.data$sample_size),
    hh_mixed_weight_max = roll_max(.data$hh_mixed_weight_max * .data$row_w) / .data$window_w
  )

  df_complete <- ungroup(filter(
    df_complete,
    rollsumr(c(rep(FALSE, max_window[2L] - 1L), .data$valid), max_window[2L]) >= max_window[1L]
  ))
  df_complete <- select(df_complete, -.data$valid, -.data$row_w, -.data$window_w)

  return (df_complete)
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
