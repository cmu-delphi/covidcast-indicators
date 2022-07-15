library(dplyr)

context("Testing functions for producing estimates of the household county metrics")

test_that("testing jeffreys_se command", {
  expect_true(max(abs(jeffreys_se(10, 0, 10) - 10.16395)) > 1e-6)
})

test_that("counts weighted calculations match documentation", {
  set.seed(100)
  n <- 20

  # Create and normalize weights
  weight <- runif(n)
  weight <- weight / sum(weight)

  # Create count response
  hh_number_sick <- round(runif(n, 0, 3))
  hh_number_total <- hh_number_sick + round(runif(n, 1, 5))
  response <- hh_number_sick / hh_number_total

  n_eff <- 1 / sum( weight ^ 2 )
  mean_unadj <- 100 * sum(weight * response)
  
  se_unadj <- sqrt( sum( weight ^ 2 * (response - mean_unadj / 100) ^ 2 ) )
  se_adj <- 100 * (1 / (1 + n_eff)) * sqrt( (1 / 2 - mean_unadj / 100) ^ 2 + n_eff ^ 2 * se_unadj ^ 2 )
  
  expected_output <- data.frame(val = c(mean_unadj),
              se = se_adj,
              sample_size = n,
              effective_sample_size = n_eff)

  out <- data.frame(compute_count_response(100 * response, weight, n)) %>%
    mutate(sample_size = n) %>%
    select(val, se, sample_size, effective_sample_size) %>%
    jeffreys_count()

  expect_equal(out, expected_output)
})

test_that("counts uniform calculations match documentation", {
  set.seed(100)
  n <- 20

  # Create and normalize weights
  weight <- rep(1, n)
  weight <- weight / sum(weight)

  # Create count response
  hh_number_sick <- round(runif(n, 0, 3))
  hh_number_total <- hh_number_sick + round(runif(n, 1, 5))
  response <- hh_number_sick / hh_number_total

  n_eff <- 1 / sum( weight ^ 2 )
  mean_unadj <- 100 * sum(weight * response)
  
  se_unadj <- sqrt( sum( weight ^ 2 * (response - mean_unadj / 100) ^ 2 ) )
  se_adj <- 100 * (1 / (1 + n_eff)) * sqrt( (1 / 2 - mean_unadj / 100) ^ 2 + n_eff ^ 2 * se_unadj ^ 2 )
  
  expected_output <- data.frame(val = c(mean_unadj),
              se = se_adj,
              sample_size = n,
              effective_sample_size = n_eff)

  out <- data.frame(compute_count_response(100 * response, weight, n)) %>%
    mutate(sample_size = n) %>%
    select(val, se, sample_size, effective_sample_size) %>%
    jeffreys_count()

  expect_equal(out, expected_output)
})
