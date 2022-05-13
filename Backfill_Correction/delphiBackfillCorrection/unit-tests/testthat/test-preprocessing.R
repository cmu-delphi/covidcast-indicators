library(jsonlite)
library(testthat)
library(dplyr)
library(tidyr)
library(zoo)
library(stats)
library(lubridate)
context("Testing preprocessing helper functions")

refd_col <- "time_value"
lag_col <- "lag"
value_col <- "Counts_Products_Denom"
min_refd <- as.Date("2022-01-01")
max_refd <- as.Date("2022-01-07")
ref_lag <- 7
fake_df <- data.frame(time_value = c(as.Date("2022-01-03"), as.Date("2022-01-03"),
                                     as.Date("2022-01-03"), as.Date("2022-01-03"),
                                     as.Date("2022-01-04"), as.Date("2022-01-04"),
                                     as.Date("2022-01-04"), as.Date("2022-01-05"),
                                     as.Date("2022-01-05")),
                      lag = c(0, 1, 3, 7, 0, 6, 7, 0, 7),
                      Counts_Products_Denom=c(100, 200, 500, 1000, 0, 200, 220, 50, 300))
wd <- c("Mon", "Tue", "Wed", "Thurs", "Fri", "Sat")
wm <- c("W1_issue", "W2_issue", "W3_issue")
  
  
test_that("testing rows filling for missing lags", {
  #Make sure all reference date have enough rows for updates
  df_new <- fill_rows(fake_df, refd_col, lag_col, min_refd, max_refd)
  n_refds <- as.numeric(max_refd - min_refd)+1
  
  expect_equal(dim(df_new)[1], n_refds*(ref_lag+1))
  expect_equal(df_new %>% drop_na(), fake_df)
})


test_that("testing NA filling for missing udpates", {
  #Make sure all the updates are valid integers
  
  # Assuming the input data does not have enough rows for consecutive lags
  expect_error(fill_missing_updates(fake_df, value_col, refd_col, lag_col), 
               "Risk exists in forward fill")
  
  # Assuming the input data is already prepared 
  df_new <- fill_rows(fake_df, refd_col, lag_col, min_refd, max_refd)
  backfill_df <- fill_missing_updates(df_new, value_col, refd_col, lag_col)

  expect_equal(dim(backfill_df)[1], n_refds*(ref_lag+1))
  
  for (d in seq(min_refd, max_refd, by="day")){
    expect_true(all(diff(backfill_df[backfill_df[,refd_col]==d, "value_raw"])>=0 ))
  }
})


test_that("testing the caculation of 7-day moving average", {
  df_new <- fill_rows(fake_df, refd_col, lag_col, min_refd, max_refd)
  df <- fill_missing_updates(df_new, value_col, refd_col, lag_col)
  df$issue_date <- df[[refd_col]] + df[[lag_col]]
  pivot_df <- df[order(df$issue_date, decreasing=FALSE), ] %>%
    pivot_wider(id_cols=refd_col, names_from="issue_date", 
                values_from="value_raw")
  pivot_df[is.na(pivot_df)] <- 0
  backfill_df <- get_7dav(pivot_df, refd_col)
  
  output <- backfill_df[backfill_df[[refd_col]] == as.Date("2022-01-07"), "value_raw"]
  expected <- colSums(pivot_df[, -1]) / 7
  expect_true(all(output == expected))
})

test_that("testing the data shifting", {
  shifted_df <- add_shift(fake_df, 1, refd_col)
  shifted_df[, refd_col] <- as.Date(shifted_df[, refd_col]) - 1
  
  expect_equal(fake_df, shifted_df)
})


test_that("testing adding columns for each day of a week", {
  df_new <- add_dayofweek(fake_df, wd, refd_col, "_ref")
  
  expect_equal(dim(fake_df)[2] + 7, dim(df_new)[2])
  expect_true(all(rowSums(df_new[, -c(1:dim(fake_df)[2])]) == 1))
  expect_true(all(df_new[df_new[[refd_col]] == as.Date("2022-01-03"), "Mon_ref"] == 1))
  expect_true(all(df_new[df_new[[refd_col]] == as.Date("2022-01-05"), "Wed_ref"] == 1))
})


test_that("testing the calculation of week of a month", {
  expect_equal(get_weekofmonth(as.Date("2022-01-01")), 1)
  expect_equal(get_weekofmonth(as.Date("2022-01-03")), 2)
  expect_equal(get_weekofmonth(as.Date("2022-01-10")), 3)
  expect_equal(get_weekofmonth(as.Date("2022-01-31")), 1)
  
})

test_that("testing the caculation of 7-day moving average", {
  df_new <- add_weekofmonth(fake_df, wm, refd_col)
  
  expect_equal(dim(fake_df)[2] + 3, dim(df_new)[2])
  expect_true(all(rowSums(df_new[, -c(1:dim(fake_df)[2])]) == 1))
  expect_true(all(df_new[df_new[[refd_col]] == as.Date("2022-01-03"), "W2_issue"] == 1))
})

test_that("testing adding 7 day avg and target", {
  df_new <- fill_rows(fake_df, refd_col, lag_col, min_refd, max_refd)
  backfill_df <- fill_missing_updates(df_new, value_col, refd_col, lag_col)
  df_new <- add_7davs_and_target(backfill_df, "value_raw", refd_col, lag_col)
  
  expect_equal(dim(df_new)[2], 3 + 1 + 1 + 1 + 1)
  expect_equal(dim(df_new)[1], 7 * 8)
})

