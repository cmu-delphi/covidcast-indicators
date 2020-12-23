library(dplyr)
library(lubridate)
library(readr)

context("Testing functions for exporting contingency table CSVs")

test_that("testing write_contingency_tables command", {
  tdir <- tempfile()
  
  write_contingency_tables(data.frame(), 
                           params = list(export_dir = tdir, 
                                         start_date = ymd("2020-05-10")), 
                           "state", 
                           c("geo_id", "tested"))
  expect_equal(!!dir(tdir), character(0))
  
  test_data <- tibble(
    geo_id = c("MA", "MA", "MA", "MA"),
    tested = c(1, 1, 1, 1),
    val_tested_pos = c(1, 2, 3, 4),
    se_tested_pos = c(10, 20, 30, 40),
    sample_size_tested_pos = c(100, 200, 300, 400),
    effective_sample_size_tested_pos = c(100, 200, 300, 400)
  )
  
  write_contingency_tables(test_data, 
                           params = list(export_dir = tdir, 
                                         start_date = ymd("2020-05-10")), 
                           "state", 
                           c("geo_id", "tested"))
  expect_setequal(!!dir(tdir), c("20200510_state_geo_id_tested.csv"))
  
  df <- read_csv(file.path(tdir, "20200510_state_geo_id_tested.csv"))
  expect_equivalent(df, test_data)
})
