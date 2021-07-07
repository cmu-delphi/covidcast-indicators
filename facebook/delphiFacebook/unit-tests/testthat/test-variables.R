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

  out <- code_activities(input_data, wave = 1)

  # expected result
  input_data$a_work_outside_home_1d <- c(NA, TRUE, FALSE, NA, FALSE, FALSE)
  input_data$a_shop_1d <- c(NA, TRUE, FALSE, NA, FALSE, TRUE)
  input_data$a_restaurant_1d <- c(NA, FALSE, TRUE, NA, FALSE, FALSE)
  input_data$a_spent_time_1d <- c(NA, TRUE, FALSE, NA, FALSE, TRUE)
  input_data$a_large_event_1d <- c(NA, FALSE, FALSE, NA, FALSE, FALSE)
  input_data$a_public_transit_1d <- c(NA, FALSE, FALSE, NA, TRUE, FALSE)

  input_data$a_work_outside_home_indoors_1d <- NA
  input_data$a_shop_indoors_1d <- NA
  input_data$a_restaurant_indoors_1d <- NA
  input_data$a_spent_time_indoors_1d <- NA
  input_data$a_large_event_indoors_1d <- NA

  expect_equal(out, input_data)

  # C13b only (Wave 10+)
  input_data <- data.frame(
    C13b = c(NA, "1,2,4", "3", "", "6", "2,4")
  )

  out <- code_activities(input_data, wave = 1)

  # expected result
  input_data$a_work_outside_home_1d <- NA
  input_data$a_shop_1d <- NA
  input_data$a_restaurant_1d <- NA
  input_data$a_spent_time_1d <- NA
  input_data$a_large_event_1d <- NA

  input_data$a_public_transit_1d <- c(NA, FALSE, FALSE, NA, TRUE, FALSE)

  input_data$a_work_outside_home_indoors_1d <- c(NA, TRUE, FALSE, NA, FALSE, FALSE)
  input_data$a_shop_indoors_1d <- c(NA, TRUE, FALSE, NA, FALSE, TRUE)
  input_data$a_restaurant_indoors_1d <- c(NA, FALSE, TRUE, NA, FALSE, FALSE)
  input_data$a_spent_time_indoors_1d <- c(NA, TRUE, FALSE, NA, FALSE, TRUE)
  input_data$a_large_event_indoors_1d <- c(NA, FALSE, FALSE, NA, FALSE, FALSE)

  expect_equal(out, input_data)
})

test_that("mask items correctly coded", {
  ## Pre-Wave 10
  input_data <- data.frame(
    C14 = c(NA, 1, 3, 6, 2, 4),
    C16 = c(1, NA, 6, 3, 2, 5),
    C6 = 1
  )

  out <- code_mask_contact(input_data, wave = 1)

  # expected result
  input_data$c_travel_state <- TRUE
  input_data$c_travel_state_7d <- NA
  input_data$c_mask_often <- c(NA, TRUE, FALSE, NA, TRUE, FALSE)
  input_data$c_mask_often_7d <- NA
  input_data$c_others_masked <- c(TRUE, NA, NA, FALSE, TRUE, FALSE)
  input_data$c_others_masked_public <- NA
  input_data$c_work_outside_5d <- NA

  expect_equal(out, input_data)

  input_data <- data.frame(
    C14a = c(NA, 1, 3, 6, 2, 4),
    C16 = c(1, NA, 6, 3, 2, 5),
    C6 = 1
  )

  out <- code_mask_contact(input_data, wave = 1)

  # expected result
  input_data$c_travel_state <- TRUE
  input_data$c_travel_state_7d <- NA
  input_data$c_mask_often <- NA
  input_data$c_mask_often_7d <- c(NA, TRUE, FALSE, NA, TRUE, FALSE)
  input_data$c_others_masked <- c(TRUE, NA, NA, FALSE, TRUE, FALSE)
  input_data$c_others_masked_public <- NA
  input_data$c_work_outside_5d <- NA

  expect_equal(out, input_data)

  ## Wave 10
  input_data <- data.frame(
    C14 = c(NA, 1, 3, 6, 2, 4),
    C16 = c(1, NA, 6, 3, 2, 5),
    C6a = 1
  )

  out <- code_mask_contact(input_data, wave = 10)

  # expected result
  input_data$c_travel_state <- NA
  input_data$c_travel_state_7d <- TRUE
  input_data$c_mask_often <- c(NA, TRUE, FALSE, NA, TRUE, FALSE)
  input_data$c_mask_often_7d <- NA
  input_data$c_others_masked <- c(TRUE, NA, NA, FALSE, TRUE, FALSE)
  input_data$c_others_masked_public <- NA
  input_data$c_work_outside_5d <- NA

  expect_equal(out, input_data)

  ## Wave 11+
  input_data <- data.frame(
    C14 = c(NA, 1, 3, 6, 2, 4),
    H2 = c(1, NA, 6, 3, 2, 5),
    C6a = 1
  )
  
  out <- code_mask_contact(input_data, wave = 11)
  
  # expected result
  input_data$c_travel_state <- NA
  input_data$c_travel_state_7d <- TRUE
  input_data$c_mask_often <- c(NA, TRUE, FALSE, NA, TRUE, FALSE)
  input_data$c_mask_often_7d <- NA
  input_data$c_others_masked <- NA
  input_data$c_others_masked_public <- c(FALSE, NA, NA, FALSE, FALSE, TRUE)
  input_data$c_work_outside_5d <- NA
  
  expect_equal(out, input_data)
  
})

test_that("household size correctly imputes zeros", {
  input_data <- data.frame(
    A5_1 = c(0, NA, 1, 2, NA),
    A5_2 = c(0, NA, NA, 1, 0),
    A5_3 = c(1, NA, NA, 1, 1)
  )

  out <- code_hh_size(input_data, wave = NA)

  input_data$hh_number_total <- c(1, NA, 1, 4, 1)

  expect_equal(out, input_data)
})

test_that("vaccine acceptance is correctly coded", {
  input_data <- data.frame(
    V1 = c(2, 3, 2, NA, 1, NA),
    V3 = c(1, 2, 3, 4, NA, NA)
  )

  out <- code_vaccines(input_data, wave = 1)

  expect_equal(out$v_covid_vaccinated_or_accept,
               c(1, 1, 0, 0, 1, NA))
})

test_that("mental health items are correctly coded", {
  ## Wave 1+, Pre-Wave 4
  input_data <- data.frame(
    C9 = c(1, 2, 3, 4, NA),
    C8_1 = c(1, 2, 3, 4, NA),
    C8_2 = c(1, 2, 3, 4, NA),
    C8_3 = c(1, 2, 3, 4, NA),
    C15 = c(1, 2, 3, 4, NA)
  )

  out <- code_mental_health(input_data, wave = 1)

  # expected result
  input_data$mh_worried_ill <- NA
  input_data$mh_anxious <- NA
  input_data$mh_depressed <- NA
  input_data$mh_isolated <- NA
  input_data$mh_worried_finances <- NA
  input_data$mh_anxious_7d <- NA
  input_data$mh_depressed_7d <- NA
  input_data$mh_isolated_7d <- NA

  expect_equal(out, input_data)

  ## Wave 4+, Pre-Wave 10
  input_data <- data.frame(
    C9 = c(1, 2, 3, 4, NA),
    C8_1 = c(1, 2, 3, 4, NA),
    C8_2 = c(1, 2, 3, 4, NA),
    C8_3 = c(1, 2, 3, 4, NA),
    C15 = c(1, 2, 3, 4, NA)
  )

  out <- code_mental_health(input_data, wave = 4)

  # expected result
  input_data$mh_worried_ill <- c(TRUE, TRUE, FALSE, FALSE, NA)
  input_data$mh_anxious <- c(FALSE, FALSE, TRUE, TRUE, NA)
  input_data$mh_depressed <- c(FALSE, FALSE, TRUE, TRUE, NA)
  input_data$mh_isolated <- c(FALSE, FALSE, TRUE, TRUE, NA)
  input_data$mh_worried_finances <- c(TRUE, TRUE, FALSE, FALSE, NA)
  input_data$mh_anxious_7d <- NA
  input_data$mh_depressed_7d <- NA
  input_data$mh_isolated_7d <- NA

  expect_equal(out, input_data)

  ## Wave 10+
  input_data <- data.frame(
    C9 = c(1, 2, 3, 4, NA),
    C8a_1 = c(1, 2, 3, 4, NA),
    C8a_2 = c(1, 2, 3, 4, NA),
    C8a_3 = c(1, 2, 3, 4, NA),
    C15 = c(1, 2, 3, 4, NA)
  )

  out <- code_mental_health(input_data, wave = 10)

  # expected result
  input_data$mh_worried_ill <- c(TRUE, TRUE, FALSE, FALSE, NA)
  input_data$mh_anxious <- NA
  input_data$mh_depressed <- NA
  input_data$mh_isolated <- NA
  input_data$mh_worried_finances <- c(TRUE, TRUE, FALSE, FALSE, NA)
  input_data$mh_anxious_7d <- c(FALSE, FALSE, TRUE, TRUE, NA)
  input_data$mh_depressed_7d <- c(FALSE, FALSE, TRUE, TRUE, NA)
  input_data$mh_isolated_7d <- c(FALSE, FALSE, TRUE, TRUE, NA)

  expect_equal(out, input_data)
})

test_that("tested reasons are correctly coded", {
  input_data <- data.frame(
    B10b = c("1", "2", "3", "4", "5", "6", "7", "8", NA_character_, 
             "1,2", "1,3", "3,4", "", "3,4,5,7", "3,4,5,7,2")
  )
  
  out <- code_testing(input_data, wave = 10)
  
  # expected result
  input_data$t_tested_reason_screening <- c(0, 0, 1, 1, 1, 0, 1, 0, NA_real_,
                                            0, 0, 1, 0, 1, 0)
  
  expect_equal(out$t_tested_reason_screening, input_data$t_tested_reason_screening)
})
