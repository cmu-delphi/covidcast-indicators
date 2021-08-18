library(dplyr)
library(lubridate)
library(readr)

context("Testing creation of individual response output data for external sharing")

test_that("testing basic write_individual command", {
  tdir <- tempfile()

  test_data <- tibble(var1 = LETTERS, var2 = letters, weight = 1,
                      token = LETTERS, Date = "2020-01-04",
                      geo_id = LETTERS)

  write_individual(test_data, params = list(
    individual_dir = tdir,
    end_date = as.Date("2020-01-05")
  ))
  expect_setequal(
    !!dir(tdir),
    "cvid_responses_2020_01_04_recordedby_2020_01_05.csv"
  )

  df <- read_csv(file.path(
    tdir,
    "cvid_responses_2020_01_04_recordedby_2020_01_05.csv"
  ))

  # skip token, Date columns
  expect_equivalent(df, test_data %>% select(-c(token, Date)))

})

test_that("testing write_individual produces race-ethnicity field appropriately", {
  ## raceeth microdata explicitly turned off
  tdir <- tempdir()
  individual_dir <- file.path(tdir, "individual")
  individual_raceeth_dir <- file.path(tdir, "individual_raceeth")
  
  test_data <- tibble(var1 = LETTERS, var2 = letters, weight = 1,
                      token = LETTERS, Date = "2020-01-04",
                      geo_id = LETTERS, raceethnicity = "Hispanic")
  
  params <- list(
    individual_dir = individual_dir,
    individual_raceeth_dir = individual_raceeth_dir,
    produce_individual_raceeth = FALSE, 
    end_date = as.Date("2020-01-05")
  )
  
  expect_error(write_individual(test_data, params),
               "race/ethnicity information should not be included in standard microdata output")
  
  test_data <- test_data %>% select(-raceethnicity)
  write_individual(test_data, params)
  
  expect_setequal(
    !!dir(individual_dir),
    "cvid_responses_2020_01_04_recordedby_2020_01_05.csv"
  )
  expect_false(dir.exists(individual_raceeth_dir))
  
  df <- read_csv(file.path(
    individual_dir,
    "cvid_responses_2020_01_04_recordedby_2020_01_05.csv"
  ))
  
  # skip token, Date columns
  expect_equivalent(df, test_data %>% select(-c(token, Date)))
  
  
  
  ## raceeth microdata explicitly turned on
  tdir <- tempdir()
  individual_dir <- file.path(tdir, "individual")
  individual_raceeth_dir <- file.path(tdir, "individual_raceeth")
  
  test_data$raceethnicity <- "Hispanic"
  params <- list(
    individual_dir = individual_dir,
    individual_raceeth_dir = individual_raceeth_dir,
    produce_individual_raceeth = TRUE, 
    end_date = as.Date("2020-01-05")
  )
  write_individual(test_data, params)
  expect_setequal(
    !!dir(individual_dir),
    "cvid_responses_2020_01_04_recordedby_2020_01_05.csv"
  )
  expect_setequal(
    !!dir(individual_raceeth_dir),
    "cvid_responses_2020_01_04_recordedby_2020_01_05.csv"
  )
  
  df <- read_csv(file.path(
    individual_dir,
    "cvid_responses_2020_01_04_recordedby_2020_01_05.csv"
  ))
  
  # skip token, Date columns
  expect_equivalent(df, test_data %>% select(-c(token, Date, raceethnicity)))
  
  df <- read_csv(file.path(
    individual_raceeth_dir,
    "cvid_responses_2020_01_04_recordedby_2020_01_05.csv"
  ))
  
  # skip token, Date columns
  expect_equivalent(df, test_data %>% select(-c(token, Date)))
  
})
