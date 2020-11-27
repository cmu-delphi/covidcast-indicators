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

test_that("mask items correctly coded", {
  input_data <- data.frame(
    C14 = c(NA, 1, 3, 6, 2, 4),
    C16 = c(1, NA, 6, 3, 2, 5),
    C6 = 1
  )

  out <- code_mask_contact(input_data)

  # expected result
  input_data$c_travel_state <- TRUE
  input_data$c_mask_often <- c(NA, TRUE, FALSE, NA, TRUE, FALSE)
  input_data$c_others_masked <- c(TRUE, NA, NA, FALSE, TRUE, FALSE)
  input_data$c_work_outside_5d <- NA

  expect_equal(out, input_data)
})
