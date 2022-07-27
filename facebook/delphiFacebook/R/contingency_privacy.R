#' Censor aggregates to ensure privacy.
#'
#' Currently done in simple, static way: Rows with sample size less than num_filter
#' are removed; no noise is added.
#'
#' @param df a data frame of summarized response data
#' @param params a named list with entries "s_weight", "s_mix_coef",
#'   "num_filter"
#'
#' @importFrom dplyr filter
#' @importFrom rlang .data
#'
#' @export
apply_privacy_censoring <- function(df, params) {
  return(filter(df,
                .data$sample_size >= params$num_filter,
                .data$effective_sample_size >= params$num_filter))
}

#' Round sample sizes to nearest 5.
#'
#' @param df a data frame of summarized response data
#' @param params a named list with entries "s_weight", "s_mix_coef",
#'   "num_filter"
#'
#' @importFrom plyr round_any
round_n <- function(df, params) {
  return(mutate(df,
                sample_size = round_any(.data$sample_size, 5),
                effective_sample_size = round_any(.data$effective_sample_size, 5)
        ))
}
