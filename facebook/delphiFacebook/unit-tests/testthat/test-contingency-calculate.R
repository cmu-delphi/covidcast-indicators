context("Testing functions for calculating weighted aggregations")

test_that("compute_multiple_choice returns people represented in group", {
  response <- c(1, 2, 5, 1, 5)
  weight <- c(0.05, 0.03, 0.05, 0.01, 0.05)
  sample_size <- 5
  
  expected_output <- list(val = 70.3,
                          sample_size = sample_size,
                          se = NA_real_,
                          effective_sample_size = sample_size,
                          represented = 70.3)
  
  out <- compute_multiple_choice(response, weight, sample_size, 70.3)
  expect_identical(out, expected_output)
})

test_that("compute_numeric_mean returns correct mean and percentiles in unweighted case", {
  response <- c(1, 2, 5, 1, 5)
  weight <- rep(1, 5)
  sample_size <- 5
  total_represented <- 5000
  
  # Use linear interpolation.
  quantiles <- quantile(response, probs = c(0.25, 0.5, 0.75), type=4)
  expected_output <- list(val = mean(response),
                          se = NA_real_,
                          sd = sd(response),
                          p25 = quantiles[["25%"]],
                          p50 = quantiles[["50%"]],
                          p75 = quantiles[["75%"]],
                          sample_size = sample_size,
                          effective_sample_size = sample_size,
                          represented = total_represented)
  out <- compute_numeric_mean(response, weight, sample_size, total_represented)
  expect_identical(out, expected_output)
})
