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
#' @param total_represented Unused
#' 
#' @return a vector of mean values
#'
#' @export
compute_numeric <- function(response, weight, sample_size, total_represented)
{
  response_mean <- compute_count_response(response, weight, sample_size)
  response_mean$sample_size <- sample_size
  
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
#' @param total_represented Unused
#'   
#' @return a vector of percentages
#'
#' @export
compute_binary_and_multiselect <- function(response, weight, sample_size, total_represented)
{
  response_pct <- compute_binary_response(response, weight, sample_size)
  response_pct$sample_size <- sample_size
  
  return(response_pct)
}

#' Returns multiple choice response estimates. Val is the number of people
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
#' @return a vector of counts
#'
#' @export
compute_multiple_choice <- function(response, weight, sample_size, total_represented)
{
  assert(all( response >= 0 ))
  assert(length(response) == length(weight))
  
  return(list(val = total_represented,
              sample_size = sample_size,
              se = NA_real_,
              effective_sample_size = sample_size))
}
