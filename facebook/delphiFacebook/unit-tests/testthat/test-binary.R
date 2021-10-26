library(dplyr)

context("Testing functions for calculating binary and multinomial proportions")

test_that("testing jeffreys_binary command", {
  input <- tibble(val=c(0), sample_size=1, effective_sample_size=1)
  expected_output <- tibble(val=c(25), sample_size=1, effective_sample_size=1, se=binary_se(25, 1))

  expect_equal(jeffreys_binary(input), expected_output)
})

test_that("testing jeffreys_multinomial command", {
  jeffreys_multinomial <- jeffreys_multinomial_factory(4)
  
  input <- tibble(val=c(0), sample_size=3, effective_sample_size=3)
  expected_output <- tibble(val=c(25/4), sample_size=3, effective_sample_size=3, se=binary_se(25/4, 3))

  expect_equal(jeffreys_multinomial(input), expected_output)
})

test_that("binary weighted calculations match documentation", {
  set.seed(100)
  n <- 20

  # Create and normalize weights
  weight <- runif(n)
  weight <- weight / sum(weight)

  # Create binary response
  response <- round(runif(n))

  n_eff <- 1 / sum( weight ^ 2 )
  mean_unadj <- 100 * sum(weight * response)
  mean_adj <- (mean_unadj * n_eff + 50) / (n_eff + 1)
  se_adj <- sqrt( mean_adj * (100 - mean_adj) / (n_eff + 1) )

  expected_output <- data.frame(val = c(mean_adj),
              se = se_adj,
              sample_size = n,
              effective_sample_size = n_eff)

  out <- data.frame(compute_binary_response(response, weight, n)) %>%
    mutate(sample_size = n) %>%
    select(val, se, sample_size, effective_sample_size) %>%
    jeffreys_binary()

  expect_equal(out, expected_output)
})

test_that("binary uniform calculations match documentation", {
  set.seed(100)
  n <- 20

  # Create and normalize weights
  weight <- rep(1, n)
  weight <- weight / sum(weight)

  # Create binary response
  response <- round(runif(n))

  n_eff <- 1 / sum( weight ^ 2 )
  mean_unadj <- 100 * sum(weight * response)
  mean_adj <- (mean_unadj * n_eff + 50) / (n_eff + 1)
  se_adj <- sqrt( mean_adj * (100 - mean_adj) / (n_eff + 1) )
  
  expected_output <- data.frame(val = c(mean_adj),
              se = se_adj,
              sample_size = n,
              effective_sample_size = n_eff)

  out <- data.frame(compute_binary_response(response, weight, n)) %>%
    mutate(sample_size = n) %>%
    select(val, se, sample_size, effective_sample_size) %>%
    jeffreys_binary()

  expect_equal(out, expected_output)
})
