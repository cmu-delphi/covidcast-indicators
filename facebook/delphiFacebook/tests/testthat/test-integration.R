library(delphiFacebook)
library(dplyr)
library(readr)
library(stringi)
library(testthat)

context("Testing the run_facebook function")

geo_levels <- c("state", "county", "hrr", "msa")
dates <- c("20200510", "20200511", "20200512", "20200513")
metrics <- c("raw_cli", "raw_ili", "raw_community",
             "raw_wcli", "raw_wili", "raw_wcommunity",
             "smoothed_cli", "smoothed_ili", "smoothed_community",
             "smoothed_wcli", "smoothed_wili", "smoothed_wcommunity")

test_that("testing existance of csv files", {
  grid <- expand.grid(
    geo_levels = geo_levels, dates = dates, metrics = metrics, stringsAsFactors=FALSE
  )
  expected_files <- sprintf("%s_%s_%s.csv", grid$dates, grid$geo_levels, grid$metrics)
  actual_files <- dir("receiving")

  expect_setequal(expected_files, actual_files)
})

test_that("testing geo files are the same", {
  # We only have data from two zips, each in their own state/county/msa, so the values in
  # for each geographic level should be the same
  geo_levels <- c("state", "county", "hrr", "msa")
  dates <- c("20200510", "20200511", "20200512", "20200513")
  metrics <- c("raw_cli", "raw_ili", "raw_community",
               "raw_wcli", "raw_wili", "raw_wcommunity",
               "smoothed_cli", "smoothed_ili", "smoothed_community",
               "smoothed_wcli", "smoothed_wili", "smoothed_wcommunity")

  grid <- expand.grid(metrics = metrics, dates = dates, stringsAsFactors=FALSE)
  for (i in seq_len(nrow(grid)))
  {
    fnames <- file.path(
      "receiving", sprintf("%s_%s_%s.csv", grid$dates[i], geo_levels, grid$metrics[i])
    )
    dt <- lapply(fnames, read_csv)
    for (j in seq_along(dt))
    {
      expect_setequal(dt[[1L]]$val, dt[[j]]$val)
      expect_setequal(dt[[1L]]$se, dt[[j]]$se)
      expect_setequal(dt[[1L]]$sample_size, dt[[j]]$sample_size)
    }
  }
})

test_that("testing geo files containg correct number of lines", {
  grid <- expand.grid(
    geo_levels = geo_levels, dates = dates, metrics = metrics, stringsAsFactors=FALSE
  )
  fnames <- file.path(
    "receiving", sprintf("%s_%s_%s.csv", grid$dates, grid$geo_levels, grid$metrics)
  )

  dt <- lapply(fnames, read_csv)
  dt_nrow <- sapply(dt, nrow)

  expect_true(all(dt_nrow[grid$dates == "20200510"] == 1L))
  expect_true(all(dt_nrow[grid$dates == "20200511"] == 1L))
  expect_true(all(dt_nrow[grid$dates == "20200512"] == 2L))
  expect_true(all(
    dt_nrow[grid$dates == "20200513" & stri_detect(fnames, fixed = "raw")] == 1L
  ))
  expect_true(all(
    dt_nrow[grid$dates == "20200513" & stri_detect(fnames, fixed = "smoothed")] == 2L
  ))
})

test_that("testing raw community values files", {

  # there are 2 / 4 households in PA on 2020-05-11 for community
  x <- read_csv(file.path("receiving", "20200511_state_raw_community.csv"))
  expect_equal(x$geo_id, "pa")
  expect_equal(x$sample_size, 4L)
  eval <-  100 * (2 + 0.5) / ( 4 + 1 )
  ese <- sqrt( eval * (100 - eval) ) / sqrt( 4 )
  expect_equal(x$val, eval)
  expect_equal(x$se, ese)

  # there are 4 / 4 households in PA on 2020-05-11 for community
  # there are 3 / 4 households in VA on 2020-05-11 for community
  x <- read_csv(file.path("receiving", "20200512_state_raw_community.csv"))
  x <- arrange(x, geo_id)
  expect_equal(x$geo_id, c("pa", "va"))
  expect_equal(x$sample_size, c(4L, 4L))
  eval <-  100 * (c(4, 3) + 0.5) / ( c(4, 4) + 1 )
  ese <- sqrt( eval * (100 - eval) ) / sqrt( c(4, 4) )
  expect_equal(x$val, eval)
  expect_equal(x$se, ese)

})

test_that("testing smoothed community values files at endpoints", {

  # for the first datapoint, smoothing and raw should be the same
  x_smooth <- read_csv(file.path("receiving", "20200510_state_smoothed_community.csv"))
  x_raw <- read_csv(file.path("receiving", "20200510_state_raw_community.csv"))
  expect_equal(x_raw, x_smooth)

  # for the first datapoint, smoothing and raw should be the same in VA
  x_smooth <- read_csv(file.path("receiving", "20200512_state_smoothed_community.csv"))
  x_raw <- read_csv(file.path("receiving", "20200512_state_raw_community.csv"))
  expect_equal(x_raw[x_raw$geo_id == "va",], x_smooth[x_raw$geo_id == "va",])
})

test_that("testing smoothed community values files", {

  # there are 0 / 4 households in PA on 2020-05-10 for community
  # there are 2 / 4 households in PA on 2020-05-11 for community
  #    after pooling => 2 / 8
  eval <-  100 * (2 + 0.5) / ( 8 + 1 )
  ese <- sqrt( eval * (100 - eval) / 8 )
  x_smooth <- read_csv(file.path("receiving", "20200511_state_smoothed_community.csv"))
  expect_equal(eval, x_smooth$val)
  expect_equal(8L, x_smooth$sample_size)
  expect_equal(ese, x_smooth$se)

  # there are 0 / 4 households in PA on 2020-05-10 for community
  # there are 2 / 4 households in PA on 2020-05-11 for community
  # there are 4 / 4 households in PA on 2020-05-12 for community
  #    after pooling => 6 / 12
  eval <-  100 * (6 + 0.5) / ( 12 + 1 )
  ese <- sqrt( eval * (100 - eval) / 12 )
  x_smooth <- read_csv(file.path("receiving", "20200512_state_smoothed_community.csv"))
  expect_equal(eval, x_smooth$val[x_smooth$geo_id == "pa"])
  expect_equal(12L, x_smooth$sample_size[x_smooth$geo_id == "pa"])
  expect_equal(ese, x_smooth$se[x_smooth$geo_id == "pa"])

})

test_that("testing weighted community values files", {

  params <- read_params("params.json")
  input_data <- load_responses_all(params)
  input_data <- join_weights(input_data, params)

  # there are 2 / 4 households in PA on 2020-05-11 for community
  these <- input_data[input_data$date == "2020-05-11" & input_data$zip5 == "15106",]
  these_weight <- these$weight
  these_yes <- as.numeric(these$A4 > 1)

  these_val <- sum(these_yes * these_weight)
  these_val <-  weighted.mean(these_yes, these_weight) * length(these_yes)
  these_ss <- length(these_yes)
  these_val <- 100 * (these_val + 0.5) / (these_ss + 1)
  these_se <- sqrt(these_val * (100 - these_val) ) / sqrt( these_ss )

  x <- read_csv(file.path("receiving", "20200511_state_raw_wcommunity.csv"))
  expect_equal(x$geo_id, "pa")
  expect_equal(x$sample_size, these_ss)
  expect_equal(x$val, these_val)
  expect_equal(x$se, these_se)

  # there are 2 / 4 households in VA on 2020-05-13 for community
  these <- input_data[input_data$date == "2020-05-13" & input_data$zip5 == "23220",]
  these_weight <- these$weight
  these_yes <- as.numeric(these$A4 > 1)

  these_val <- sum(these_yes * these_weight)
  these_val <-  weighted.mean(these_yes, these_weight) * length(these_yes)
  these_ss <- length(these_yes)
  these_val <- 100 * (these_val + 0.5) / (these_ss + 1)
  these_se <- sqrt(these_val * (100 - these_val) ) / sqrt( these_ss )

  x <- read_csv(file.path("receiving", "20200513_state_raw_wcommunity.csv"))
  expect_equal(x$geo_id, "va")
  expect_equal(x$sample_size, these_ss)
  expect_equal(x$val, these_val)
  expect_equal(x$se, these_se)
})

test_that("testing weighted smoothed community values files", {
  params <- read_params("params.json")
  input_data <- load_responses_all(params)
  input_data <- join_weights(input_data, params)

  # there are 2 / 4 households in PA on 2020-05-11 for community
  these <- input_data[
     ((input_data$date == "2020-05-10") | (input_data$date == "2020-05-11")) &
     input_data$zip5 == "15106",]
  these_weight <- these$weight
  these_yes <- as.numeric(these$A4 > 1)

  these_val <- sum(these_yes * these_weight)
  these_val <-  weighted.mean(these_yes, these_weight) * length(these_yes)
  these_ss <- length(these_yes)
  these_val <- 100 * (these_val + 0.5) / (these_ss + 1)
  these_se <- sqrt(these_val * (100 - these_val) ) / sqrt( these_ss )

  x <- read_csv(file.path("receiving", "20200511_state_smoothed_wcommunity.csv"))
  expect_equal(x$geo_id, "pa")
  expect_equal(x$sample_size, these_ss)
  expect_equal(x$val, these_val)
  expect_equal(x$se, these_se)

})

test_that("testing raw ili/cli values files", {

  # there are 2 / 4 households in PA on 2020-05-11 with ILI (all households have size 1)
  x <- read_csv(file.path("receiving", "20200511_state_raw_ili.csv"))
  expect_equal(x$geo_id, "pa")
  expect_equal(x$val, 50)
  expect_equal(x$sample_size, 4L)
  expected_se <- sqrt(sum( (0.275 * (c(0, 0, 100, 100) - 50))^2 ))
  expected_se <- jeffreys_se(expected_se, x$val, x$sample_size)
  expect_equal(x$se, expected_se)

  # there are 1 / 4 households in PA on 2020-05-11 with CLI (all households have size 1)
  x <- read_csv(file.path("receiving", "20200511_state_raw_cli.csv"))
  expect_equal(x$geo_id, "pa")
  expect_equal(x$val, 25)
  expect_equal(x$sample_size, 4L)
  expected_se <- sqrt(sum( (0.275 * (c(0, 0, 0, 100) - 25))^2 ))
  expected_se <- jeffreys_se(expected_se, x$val, x$sample_size)
  expect_equal(x$se, expected_se)

  # there are 1 / 4 households in PA on 2020-05-12 with CLI (all households have size 1)
  # there are 0 / 4 households in VA on 2020-05-12 with CLI (all households have size 1)
  x <- read_csv(file.path("receiving", "20200512_state_raw_cli.csv"))
  expect_setequal(x$geo_id, c("va", "pa"))
  expect_setequal(x$val, c(25, 0))
  expect_setequal(x$sample_size, c(4L, 4L))
  expected_se_min <- sqrt(sum( (0.275 * (c(0, 0, 0, 0) - 0))^2 ))
  expected_se_min <- jeffreys_se(expected_se_min, 0, 4L)
  expect_equal(min(x$se), expected_se_min)

})
