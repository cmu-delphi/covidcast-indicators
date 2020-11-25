library(testthat)

context("Testing response coding")

test_that("is_selected handles selections correctly", {
  expect_equal(is_selected(split_options(c("1", "", "1,2")), "1"),
               c(TRUE, NA, TRUE))

  expect_equal(is_selected(split_options(c("1", "11", "1,11")), "1"),
               c(TRUE, FALSE, TRUE))

  expect_equal(is_selected(split_options(c("", "14", "4", "4,6,8")), "1"),
               c(NA, FALSE, FALSE, FALSE))

  expect_equal(is_selected(split_options(c("1", "15", "14", NA)), "14"),
               c(FALSE, FALSE, TRUE, FALSE))

  expect_equal(is_selected(split_options(c("4,54", "3,6,2,54", "5,4,45")),
                           "54"),
               c(TRUE, TRUE, FALSE))
})
