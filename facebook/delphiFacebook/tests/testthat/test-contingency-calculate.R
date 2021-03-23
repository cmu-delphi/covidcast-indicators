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
