library(testthat)

context("Testing response coding")

test_that("is_selected handles selections correctly", {
  expect_equal(is_selected(split_options(c("1", "", "1,2")), "1"),
               c(TRUE, NA, TRUE))

  expect_equal(is_selected(split_options(c("1", "11", "1,11")), "1"),
               c(TRUE, FALSE, TRUE))

  expect_equal(is_selected(split_options(c("", "14", "4", "4,6,8")), "1"),
               c(NA, FALSE, FALSE, FALSE))

  expect_equal(is_selected(split_options(c("1", "15", "14", NA, "")), "14"),
               c(FALSE, FALSE, TRUE, NA, NA))

  expect_equal(is_selected(split_options(c("4,54", "3,6,2,54", "5,4,45")),
                           "54"),
               c(TRUE, TRUE, FALSE))
})

test_that("activities items correctly coded", {
  # C13 only (pre-Wave 10)
  input_data <- data.frame(
    C13 = c(NA, "1,2,4", "3", "", "6", "2,4")
  )
  
  out <- code_activities(input_data)
  
  # expected result
  input_data$a_work_outside_home_1d <- c(NA, TRUE, FALSE, NA, FALSE, FALSE)
  input_data$a_shop_1d <- c(NA, TRUE, FALSE, NA, FALSE, TRUE)
  input_data$a_restaurant_1d <- c(NA, FALSE, TRUE, NA, FALSE, FALSE)
  input_data$a_spent_time_1d <- c(NA, TRUE, FALSE, NA, FALSE, TRUE)
  input_data$a_large_event_1d <- c(NA, FALSE, FALSE, NA, FALSE, FALSE)
  input_data$a_public_transit_1d <- c(NA, FALSE, FALSE, NA, TRUE, FALSE)

  input_data$a_work_outside_home_indoors_1d <- rep(NA, 6)
  input_data$a_shop_indoors_1d <- rep(NA, 6)
  input_data$a_restaurant_indoors_1d <- rep(NA, 6)
  input_data$a_spent_time_indoors_1d <- rep(NA, 6)
  input_data$a_large_event_indoors_1d <- rep(NA, 6)
  
  expect_equal(out, input_data)
  
  # C13 and C13b (pre-Wave 10 and Wave 10 mix)
  input_data <- data.frame(
    C13 = c(NA, "1,2,4", "3", "", "6", "2,4"),
    C13b = c("6", NA, NA, NA, NA, NA)
  )
  
  out <- code_activities(input_data)
  
  # expected result
  input_data$a_work_outside_home_1d <- c(NA, TRUE, FALSE, NA, FALSE, FALSE)
  input_data$a_shop_1d <- c(NA, TRUE, FALSE, NA, FALSE, TRUE)
  input_data$a_restaurant_1d <- c(NA, FALSE, TRUE, NA, FALSE, FALSE)
  input_data$a_spent_time_1d <- c(NA, TRUE, FALSE, NA, FALSE, TRUE)
  input_data$a_large_event_1d <- c(NA, FALSE, FALSE, NA, FALSE, FALSE)
  
  input_data$a_public_transit_1d <- c(TRUE, FALSE, FALSE, NA, TRUE, FALSE)
  
  input_data$a_work_outside_home_indoors_1d <- c(FALSE, NA, NA, NA, NA, NA)
  input_data$a_shop_indoors_1d <-  c(FALSE, NA, NA, NA, NA, NA)
  input_data$a_restaurant_indoors_1d <-  c(FALSE, NA, NA, NA, NA, NA)
  input_data$a_spent_time_indoors_1d <-  c(FALSE, NA, NA, NA, NA, NA)
  input_data$a_large_event_indoors_1d <-  c(FALSE, NA, NA, NA, NA, NA)
  
  expect_equal(out, input_data)
  
  # C13b only (Wave 10+)
  input_data <- data.frame(
    C13b = c(NA, "1,2,4", "3", "", "6", "2,4")
  )
  
  out <- code_activities(input_data)
  
  # expected result
  input_data$a_work_outside_home_1d <- rep(NA, 6)
  input_data$a_shop_1d <- rep(NA, 6)
  input_data$a_restaurant_1d <- rep(NA, 6)
  input_data$a_spent_time_1d <- rep(NA, 6)
  input_data$a_large_event_1d <- rep(NA, 6)
  
  input_data$a_public_transit_1d <- c(NA, FALSE, FALSE, NA, TRUE, FALSE)
  
  input_data$a_work_outside_home_indoors_1d <- c(NA, TRUE, FALSE, NA, FALSE, FALSE)
  input_data$a_shop_indoors_1d <- c(NA, TRUE, FALSE, NA, FALSE, TRUE)
  input_data$a_restaurant_indoors_1d <- c(NA, FALSE, TRUE, NA, FALSE, FALSE)
  input_data$a_spent_time_indoors_1d <- c(NA, TRUE, FALSE, NA, FALSE, TRUE)
  input_data$a_large_event_indoors_1d <- c(NA, FALSE, FALSE, NA, FALSE, FALSE)
  
  expect_equal(out, input_data)
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

test_that("household size correctly imputes zeros", {
  input_data <- data.frame(
    A5_1 = c(0, NA, 1, 2, NA),
    A5_2 = c(0, NA, NA, 1, 0),
    A5_3 = c(1, NA, NA, 1, 1)
  )

  out <- code_hh_size(input_data)

  input_data$hh_number_total <- c(1, NA, 1, 4, 1)

  expect_equal(out, input_data)
})

test_that("vaccine acceptance is coded", {
  input_data <- data.frame(
    V1 = c(2, 3, 2, NA, 1, NA),
    V3 = c(1, 2, 3, 4, NA, NA)
  )

  out <- code_vaccines(input_data)
  
  expect_equal(out$v_covid_vaccinated_or_accept,
               c(1, 1, 0, 0, 1, NA))
})
