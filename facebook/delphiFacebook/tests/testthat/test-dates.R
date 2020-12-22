library(lubridate)

context("Testing functions for getting and manipulating dates")

test_that("testing start_of_prev_full_month command", {
  expect_that(start_of_prev_full_month(ymd("2020-01-02")), is_date("2019-12-01"))
  expect_that(start_of_prev_full_month(ymd("2020-01-31")), is_date("2019-12-01"))
  expect_that(start_of_prev_full_month(ymd("2019-02-28")), is_date("2019-01-01"))
})

test_that("testing end_of_prev_full_month command", {
  expect_that(end_of_prev_full_month(ymd("2020-01-02")), is_date("2019-12-31"))
  expect_that(end_of_prev_full_month(ymd("2020-01-31")), is_date("2019-12-31"))
  expect_that(end_of_prev_full_month(ymd("2019-02-28")), is_date("2019-01-31"))
  expect_that(end_of_prev_full_month(ymd("2019-03-01")), is_date("2019-02-28"))
})