## Functions used to calculate column aggregations. Each function is meant to be
## used for a specific response type (binary, numeric, multiselect, multiple choice).

#' Wrapper for `compute_count_response` that adds sample_size. Val is the mean.
#'
#' @param response a vector of percentages (100 * cnt / total)
#' @param weight a vector of sample weights for inverse probability weighting;
#'   invariant up to a scaling factor
#' @param sample_size The sample size to use, which may be a non-integer (as
#'   responses from ZIPs that span geographical boundaries are weighted
#'   proportionately, and survey weights may also be applied)
#' @param total_represented Number of people represented in sample, which may
#'   be a non-integer
#' 
#' @return a list of named means and other descriptive statistics
compute_household_binary <- function(response, weight, sample_size, total_represented)
{
  response_mean <- compute_count_response(response, weight, sample_size)
  response_mean$sample_size <- sample_size
  response_mean$represented <- total_represented
  
  return(response_mean)
}

#' Wrapper for `compute_binary_response` that adds sample_size. Val is the 
#' percent `TRUE`.
#'
#' @param response a vector of binary (0 or 1) responses
#' @param weight a vector of sample weights for inverse probability weighting;
#'   invariant up to a scaling factor
#' @param sample_size The sample size to use, which may be a non-integer (as
#'   responses from ZIPs that span geographical boundaries are weighted
#'   proportionately, and survey weights may also be applied)
#' @param total_represented Number of people represented in sample, which may
#'   be a non-integer
#'      
#' @return a list of named percentages and other descriptive statistics
compute_binary <- function(response, weight, sample_size, total_represented)
{
  response_pct <- compute_binary_response(response, weight, sample_size)
  response_pct$sample_size <- sample_size
  response_pct$represented <- total_represented
  
  return(response_pct)
}

#' Multiselect wrapper for `compute_binary`
#'
#' @param response a vector of binary (0 or 1) responses
#' @param weight a vector of sample weights for inverse probability weighting;
#'   invariant up to a scaling factor
#' @param sample_size The sample size to use, which may be a non-integer (as
#'   responses from ZIPs that span geographical boundaries are weighted
#'   proportionately, and survey weights may also be applied)
#' @param total_represented Number of people represented in sample, which may
#'   be a non-integer
#'      
#' @return a list of named percentages and other descriptive statistics
compute_multiselect <- function(response, weight, sample_size, total_represented)
{
  response_pct <- compute_binary(response, weight, sample_size, total_represented)
  
  return(response_pct)
}

#' Return multiple choice response estimates. Val is the number of people
#' represented by the survey respondents in a given response.
#'
#' This function takes vectors as input and computes the response values
#' (a point estimate named "val" and a sample size
#' named "sample_size").
#'
#' @param response a vector of multiple choice responses
#' @param weight a vector of sample weights for inverse probability weighting;
#'   invariant up to a scaling factor
#' @param sample_size The sample size to use, which may be a non-integer (as
#'   responses from ZIPs that span geographical boundaries are weighted
#'   proportionately, and survey weights may also be applied)
#' @param total_represented Number of people represented in sample, which may
#'   be a non-integer
#'
#' @return a list of named counts and other descriptive statistics
compute_multiple_choice <- function(response, weight, sample_size, total_represented)
{
  assert(all( response >= 0 ))
  assert(length(response) == length(weight))
  
  return(list(val = total_represented,
              sample_size = sample_size,
              se = NA_real_,
              effective_sample_size = sample_size,
              represented = total_represented))
}

#' Convert val column from counts to percents of total
#' 
#' Meant for use as `post_fn` with multiple choice metrics, which produce
#' weighted frequency for val by default.
#'
#' @param df Data frame
#' @return Updated data frame.
#' @importFrom dplyr mutate
post_convert_count_to_pct <- function(df) {
  return(mutate(df,
                val = 100 * .data$val / sum(.data$val, na.rm=TRUE)))
}

#' Return numeric response estimates. Val is the mean of a numeric vector.
#'
#' This function takes vectors as input and computes the response values
#' (a point estimate named "val" and 25, 50, and 75th percentiles).
#'
#' @param response a vector of multiple choice responses
#' @param weight a vector of sample weights for inverse probability weighting;
#'   invariant up to a scaling factor
#' @param sample_size The sample size to use, which may be a non-integer (as
#'   responses from ZIPs that span geographical boundaries are weighted
#'   proportionately, and survey weights may also be applied)
#' @param total_represented Number of people represented in sample, which may
#'   be a non-integer
#'
#' @return a list of named mean and other descriptive statistics
#'
#' @importFrom survey svydesign svymean svyvar oldsvyquantile
compute_numeric_mean <- function(response, weight, sample_size, total_represented)
{
  assert(length(response) == length(weight))
  
  design <- svydesign(id = ~1, weight = ~weight, data = data.frame(response, weight))
  return(list(val = as.data.frame(svymean(~response, na.rm = TRUE, design = design))[,"mean"],
              se = NA_real_,
              sd = as.data.frame(sqrt(svyvar(~response, na.rm = TRUE, design = design)))[,"variance"],
              p25 = as.data.frame(oldsvyquantile(~response, na.rm = TRUE, design = design, quantiles = 0.25))[,"0.25"],
              p50 = as.data.frame(oldsvyquantile(~response, na.rm = TRUE, design = design, quantiles = 0.5))[,"0.5"],
              p75 = as.data.frame(oldsvyquantile(~response, na.rm = TRUE, design = design, quantiles = 0.75))[,"0.75"],
              sample_size = sample_size,
              effective_sample_size = sample_size,
              represented = total_represented
  ))
}
