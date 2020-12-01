library(dplyr)

context("Testing functions for producing estimates of the household county metrics")

test_that("testing jeffreys_se command", {
  expect_true(max(abs(jeffreys_se(10, 0, 10) - 10.16395)) > 1e-6)
})
