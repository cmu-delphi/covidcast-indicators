library(data.table)
library(tibble)

context("Testing response recoding and renaming")

test_that("testing rename_responses command", {
  input_data <- data.frame(
    C14 = c(NA, 1, 3, 6, 2),
    C16 = c(1, NA, 6, 3, 2),
    C6 = 1,
    DDD123 = 1
  )
  
  expected_output <- data.frame(
    mc_mask_often = c(NA, 1, 3, 6, 2),
    mc_cmnty_mask_prevalence = c(1, NA, 6, 3, 2),
    b_state_travel = 1,
    DDD123 = 1
  )
  
  expect_identical(rename_responses(input_data), expected_output)
})

test_that("testing remap_responses command", {
  input_data <- data.frame(
    C14 = c(NA, 1, 3, 6, 2),
    D7 = c("1", NA, "1,2", "5", "3,4,5"),
    E1_1 = c(1, 1, 5, 2, NA)
  )
  
  expected_output <- data.frame(
    C14 = c(NA, 1, 3, 6, 2),
    D7 = c("American Indian or Alaska Native", NA, "Multiracial", "White", "Multiracial"),
    E1_1 = c(1, 1, NA, 0, NA)
  )
  
  expect_identical(remap_responses(input_data), expected_output)
})

test_that("testing code_binary command", {
  input_data <- data.frame(
    C14 = c(NA, 1, 3, 6, 2),
    C4 = c(1, 1, NA, 2, 2)
  )
  
  expected_output <- data.frame(
    C14 = c(NA, 1, 3, 6, 2),
    C4 = c(1, 1, NA, 0, 0)
  )
  
  out <- code_binary(input_data, list(), "C4")

  expect_identical(out, list(expected_output, list()))
})

test_that("testing code_multiselect command", {
  input_data <- data.frame(
    C14 = c(NA, 1, 3, 6, 2),
    DDD1_23 = c("1", NA, "1,2", "3", "2,3")
  )
  
  input_aggs <- tribble(
    ~name, ~metric, ~group_by, ~id,
    "no_change", "C14", c("geo_id", "mc_race", "DDD1_23"), "1",
    "duplicated", "DDD1_23", c("geo_id", "mc_race"), "2"
  )
  
  expected_output <- data.frame(
    C14 = c(NA, 1, 3, 6, 2),
    DDD1_23 = c("1", NA, "1,2", "3", "2,3"),
    DDD1_23_1 = c(1, NA, 1, 0, 0),
    DDD1_23_2 = c(0, NA, 1, 0, 1),
    DDD1_23_3 = c(0, NA, 0, 1, 1)
  )
  
  expected_aggs <- tribble(
    ~name, ~metric, ~group_by, ~id,
    "no_change", "C14", c("geo_id", "mc_race", "DDD1_23"), "1",
    "duplicated_1", "DDD1_23_1", c("geo_id", "mc_race"), "2_1",
    "duplicated_2", "DDD1_23_2", c("geo_id", "mc_race"), "2_2",
    "duplicated_3", "DDD1_23_3", c("geo_id", "mc_race"), "2_3"
  )

  out <- code_multiselect(as.data.table(input_data), input_aggs, "DDD1_23")
  browser()
  expect_identical(out, list(as.data.table(expected_output), expected_aggs))
})

test_that("testing code_numeric_freeresponse command", {
  input_data <- data.frame(
    C14 = c(NA, 1, 3, 6, 2),
    Q40 = c("100.4", "99.6", NA, "95", "95°F")
  )
  
  expected_output <- data.frame(
    C14 = c(NA, 1, 3, 6, 2),
    Q40 = c(100.4, 99.6, NA, 95.0, NA)
  )
  
  out <- code_numeric_freeresponse(input_data, list(), "Q40")
  
  expect_identical(out, list(expected_output, list()))
})

## High level call
test_that("testing make_human_readable command", {
  input_data <- data.frame(
    C14 = c(NA, 1, 3, 6, 2),
    C16 = c(1, NA, 6, 3, 2),
    C6 = 1,
    DDD123 = 1,
    E1_1 = c(1, 1, 5, 2, NA),
    D7 = c("1", NA, "1,2", "5", "3,4,5"),
    zip5 = c("12345", "23345", "10009", NA, NA)
  )
  
  expected_output <- data.frame(
    mc_mask_often = c(NA, 1, 3, 6, 2),
    mc_cmnty_mask_prevalence = c(1, NA, 6, 3, 2),
    b_state_travel = 1,
    DDD123 = 1,
    b_children_grade_prek_k = c(1, 1, NA, 0, NA),
    mc_race = c("American Indian or Alaska Native", NA, "Multiracial", "White", "Multiracial"),
    zip5 = c("12345", "23345", "10009", NA, NA),
    t_zipcode = c("12345", "23345", "10009", NA, NA)
  )
  
  expect_identical(make_human_readable(input_data), expected_output)
})
