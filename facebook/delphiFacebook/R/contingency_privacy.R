#' Censor aggregates to ensure privacy.
#'
#' Currently done in simple, static way: Rows with sample size less than 100 are
#' removed; no noise is added.
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
