library(delphiFacebook)
library(jsonlite)
library(testthat)

context("Testing utility functions")

test_that("testing assert command", {
  expect_equal(assert(TRUE), NULL)
  expect_error(assert(FALSE))
  expect_error(assert(FALSE, "Don't do that"), "Don't do that")
})

test_that("testing df message command", {
  expect_message(msg_df("Hello", data.frame(x = letters)), "Hello: 26 rows")
})

test_that("testing create dir function", {

  tdir <- tempfile()

  create_dir_not_exist(tdir)
  expect_true(dir.exists(tdir))
  expect_silent(create_dir_not_exist(tdir))
})

test_that("testing read params when missing file", {

  # expect error if missing file, since no template in test dir
  tdir <- tempfile()
  expect_warning(expect_error(read_params(tdir)))
})

test_that("testing read params when debug", {

  json_data <- list(
    debug = TRUE,
    start_date = "2020-01-01",
    end_date = "2020-02-01"
  )
  write_json(json_data, tdir <- tempfile())

  params <- read_params(tdir)
  expect_equal(params$num_filter, 2L)
  expect_equal(params$s_weight, 1)
  expect_equal(params$s_mix_coef, 0.05)
  expect_true(inherits(params$start_time, "POSIXct"))
  expect_true(inherits(params$end_time, "POSIXct"))

})


test_that("testing read params when not debug", {

  json_data <- list(
    debug = unbox(FALSE),
    start_date = unbox("2020-01-01"),
    end_date = unbox("2020-02-01")
  )
  write_json(json_data, tdir <- tempfile())

  params <- read_params(tdir)
  expect_equal(params$num_filter, 100L)
  expect_equal(params$s_weight, 0.01)
  expect_equal(params$s_mix_coef, 0.05)
  expect_true(inherits(params$start_time, "POSIXct"))
  expect_true(inherits(params$end_time, "POSIXct"))

})

test_that("testing mix weights", {
  ## When all weights are identical and smaller than the minimum threshold, the
  ## minimum mixing mixes uniform with uniform and has no effect.
  weights <- rep(1, times = 200)

  expect_equal(mix_weights(weights, list(s_mix_coef = 0.05, num_filter = 1L)),
               rep(1/200, 200))

  ## for intermediate version, check that mixing enforces the intended maximum
  for (k in seq(2, 5))
  {
    new_weights <- mix_weights(c(0.1, 0.1, 0.1, 0.1, 0.1, 0.5), list(s_mix_coef = 0.05,
                                                                     num_filter = k))
    expect_lt(max(new_weights), 1 / k)
  }

  ## for extreme value, can only mix to uniform (maximum mixing coefficient of 1
  ## applies and mixes everything to uniform)
  new_weights <- mix_weights(c(0.1, 0.1, 0.1, 0.1, 0.1, 0.5), list(s_mix_coef = 0.05,
                                                                   num_filter = 6L))
  expect_equal(new_weights, rep(1/6, 6))

  ## when mixing is not needed, only minimum mixing is applied
  new_weights <- mix_weights(c(0.1, 0.2, 0.2, 0.2, 0.3),  list(s_mix_coef = 0.05,
                                                               num_filter = 3L))
  expect_equal(new_weights, 0.05 / 5 + 0.95 * c(0.1, 0.2, 0.2, 0.2, 0.3))

})
