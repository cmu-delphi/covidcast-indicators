library(lubridate)

context("Testing functions for getting and manipulating dates")

## Monthly
test_that("testing start_of_prev_full_month command", {
  expect_equal(start_of_prev_full_month(ymd("2020-01-02")), ymd("2019-12-01"))
  expect_equal(start_of_prev_full_month(ymd("2020-01-31")), ymd("2019-12-01"))
  expect_equal(start_of_prev_full_month(ymd("2019-02-28")), ymd("2019-01-01"))
})

test_that("testing end_of_prev_full_month command", {
  expect_equal(end_of_prev_full_month(ymd("2020-01-02")), ymd("2019-12-31"))
  expect_equal(end_of_prev_full_month(ymd("2020-01-31")), ymd("2019-12-31"))
  expect_equal(end_of_prev_full_month(ymd("2019-02-28")), ymd("2019-01-31"))
  expect_equal(end_of_prev_full_month(ymd("2019-03-01")), ymd("2019-02-28"))
})

test_that("testing get_range_prev_full_month command", {
  expect_equal(get_range_prev_full_month(ymd("2020-01-02")), 
               list(ymd("2019-12-01"), ymd("2019-12-31")))
  expect_equal(get_range_prev_full_month(ymd("2020-01-31")), 
               list(ymd("2019-12-01"), ymd("2019-12-31")))
  expect_equal(get_range_prev_full_month(ymd("2019-02-28")), 
               list(ymd("2019-01-01"), ymd("2019-01-31")))
  expect_equal(get_range_prev_full_month(ymd("2019-03-01")), 
               list(ymd("2019-02-01"), ymd("2019-02-28")))
})

## Weekly
test_that("testing start_of_prev_full_week command", {
  expect_equal(start_of_prev_full_week(ymd("2020-01-01")), ymd("2019-12-22"))
  expect_equal(start_of_prev_full_week(ymd("2020-01-04")), ymd("2019-12-22"))
  expect_equal(start_of_prev_full_week(ymd("2020-03-01")), ymd("2020-02-23"))
})

test_that("testing end_of_prev_full_week command", {
  expect_equal(end_of_prev_full_week(ymd("2020-01-01")), ymd("2019-12-28"))
  expect_equal(end_of_prev_full_week(ymd("2020-01-04")), ymd("2019-12-28"))
  expect_equal(end_of_prev_full_week(ymd("2020-03-01")), ymd("2020-02-29"))
})

test_that("testing get_range_prev_full_week command", {
  expect_equal(get_range_prev_full_week(ymd("2020-01-01")), 
               list(ymd("2019-12-22"), ymd("2019-12-28")))
  expect_equal(get_range_prev_full_week(ymd("2020-01-04")), 
               list(ymd("2019-12-22"), ymd("2019-12-28")))
  expect_equal(get_range_prev_full_week(ymd("2020-03-01")), 
               list(ymd("2020-02-23"), ymd("2020-02-29")))
})

## High level
test_that("testing get_range_prev_full_period command", {
  timezone <- "America/Los_Angeles"
  
  expect_equal(get_range_prev_full_period(ymd("2020-01-02"), "month"), 
               list(ymd_hms("2019-12-01 00:00:00", tz=timezone), 
                    ymd_hms("2019-12-31 23:59:59", tz=timezone)))
  expect_equal(get_range_prev_full_period(ymd("2020-01-31"), "month"), 
               list(ymd_hms("2019-12-01 00:00:00", tz=timezone), 
                    ymd_hms("2019-12-31 23:59:59", tz=timezone)))
  expect_equal(get_range_prev_full_period(ymd("2019-02-28"), "month"), 
               list(ymd_hms("2019-01-01 00:00:00", tz=timezone), 
                    ymd_hms("2019-01-31 23:59:59", tz=timezone)))
  expect_equal(get_range_prev_full_period(ymd("2019-03-01"), "month"), 
               list(ymd_hms("2019-02-01 00:00:00", tz=timezone), 
                    ymd_hms("2019-02-28 23:59:59", tz=timezone)))

  expect_equal(get_range_prev_full_period(ymd("2020-01-01"), "week"), 
               list(ymd_hms("2019-12-22 00:00:00", tz=timezone), 
                    ymd_hms("2019-12-28 23:59:59", tz=timezone)))
  expect_equal(get_range_prev_full_period(ymd("2020-01-04"), "week"), 
               list(ymd_hms("2019-12-22 00:00:00", tz=timezone), 
                    ymd_hms("2019-12-28 23:59:59", tz=timezone)))
  expect_equal(get_range_prev_full_period(ymd("2020-03-01"), "week"), 
               list(ymd_hms("2020-02-23 00:00:00", tz=timezone), 
                    ymd_hms("2020-02-29 23:59:59", tz=timezone)))

  expect_error(get_range_prev_full_period(ymd("2020-03-01"), "year"), 
               "'arg' should be one of \"month\", \"week\"")
})

## Date utilities
test_that("testing floor_epiweek command", {
  expect_equal(floor_epiweek(ymd("2019-12-29")), ymd("2019-12-29"))
  expect_equal(floor_epiweek(ymd("2020-01-01")), ymd("2019-12-29"))
  expect_equal(floor_epiweek(ymd("2020-01-04")), ymd("2019-12-29"))
  expect_equal(floor_epiweek(ymd("2020-03-01")), ymd("2020-03-01"))
})

test_that("testing ceiling_epiweek command", {
  expect_equal(ceiling_epiweek(ymd("2019-12-31")), ymd("2020-01-05"))
  expect_equal(ceiling_epiweek(ymd("2020-01-01")), ymd("2020-01-05"))
  expect_equal(ceiling_epiweek(ymd("2020-01-04")), ymd("2020-01-05"))
  expect_equal(ceiling_epiweek(ymd("2020-02-23")), ymd("2020-03-01"))
})
