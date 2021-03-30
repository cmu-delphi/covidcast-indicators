context("Testing functions for calculating binary and multinomial proportions")

test_that("testing jeffreys_binary command", {
  input <- tibble(val=c(0), sample_size=1)
  expected_output <- tibble(val=c(25), sample_size=1, se=binary_se(25, 1))

  expect_equal(jeffreys_binary(input), expected_output)
})

test_that("testing jeffreys_multinomial command", {
  jeffreys_multinomial <- jeffreys_multinomial_factory(4)
  
  input <- tibble(val=c(0), sample_size=3)
  expected_output <- tibble(val=c(25/4), sample_size=3, se=binary_se(25/4, 3))

  expect_equal(jeffreys_multinomial(input), expected_output)
})
