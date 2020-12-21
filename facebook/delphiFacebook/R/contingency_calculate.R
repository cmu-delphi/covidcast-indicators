#' Wrapper for `compute_count_response` that adds sample_size
#'
#' @param response a vector of percentages (100 * cnt / total)
#' @param weight a vector of sample weights for inverse probability weighting;
#'   invariant up to a scaling factor
#' @param sample_size Unused.
#' 
#' @return a vector of mean values
#'
#' @export
compute_mean <- function(response, weight, sample_size)
{
  response_mean <- compute_count_response(response, weight, sample_size)
  response_mean$sample_size <- sample_size
  
  return(response_mean)
}


#' Wrapper for `compute_binary_response` that adds sample_size
#'
#' @param response a vector of binary (0 or 1) responses
#' @param weight a vector of sample weights for inverse probability weighting;
#'   invariant up to a scaling factor
#' @param sample_size The sample size to use, which may be a non-integer (as
#'   responses from ZIPs that span geographical boundaries are weighted
#'   proportionately, and survey weights may also be applied)
#'   
#' @return a vector of percentages
#'
#' @export
compute_pct <- function(response, weight, sample_size)
{
  response_pct <- compute_binary_response(response, weight, sample_size)
  response_pct$sample_size <- sample_size
  
  return(response_pct)
}


#' Returns multiple choice response estimates
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
#'
#' @return a vector of counts
#'
#' @export
compute_count <- function(response, weight, sample_size)
{
  assert(all( response >= 0 ))
  assert(length(response) == length(weight))
  
  return(list(val = sample_size,
              sample_size = sample_size,
              se = NA_real_,
              effective_sample_size = sample_size)) # TODO effective sample size
}
