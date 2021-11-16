library(dplyr)

context("Testing functions for ensuring aggregation privacy")

test_that("apply_privacy_censoring filters sample size & effective sample size", {
  test_data <- tibble(
    geo_id = c("MA", "MA", "MA", "MA", "PA", "PA"),
    tested = c(1, 1, 1, 1, 1, 1),
    val = c(1, 2, 3, 4, 4, 5),
    se = c(10, 20, 30, 40, 10, 10),
    sample_size = c(100, 100, 99, 1, 400, NA),
    effective_sample_size = c(99, 100, 300, 2, 400, 400)
  )
  
  df <- apply_privacy_censoring(test_data, params = list(num_filter = 100))
  expect_equal(df, test_data[c(2L, 5L), ])
})
