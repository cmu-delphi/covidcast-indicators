library(delphiFacebook)

context("Testing utility functions")

test_that("testing assert command", {
  expect_equal(assert(TRUE), NULL)
  expect_error(assert(FALSE))
  expect_error(assert(FALSE, "Don't do that"), "Don't do that")
})
