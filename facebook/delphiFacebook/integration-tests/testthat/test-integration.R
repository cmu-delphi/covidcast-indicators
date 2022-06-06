### As with `test-gold.R`, this test file relies on `setup-run.R` to run the
### pipeline. This test checks the configuration listed in `params-test.json`,
### which loads `input/responses.csv`, a small selected subset of test
### responses.

library(dplyr)
library(readr)
library(stringi)
library(testthat)

context("Testing the run_facebook function")

geo_levels <- c("state", "county", "hrr", "msa", "nation")
dates <- c("20200510", "20200511", "20200512", "20200513")
metrics <- c("raw_cli", "raw_ili", "raw_hh_cmnty_cli", "raw_nohh_cmnty_cli",
             "raw_wcli", "raw_wili", "raw_whh_cmnty_cli", "raw_wnohh_cmnty_cli",
             "smoothed_cli", "smoothed_ili", "smoothed_hh_cmnty_cli",
             "smoothed_nohh_cmnty_cli", "smoothed_wcli", "smoothed_wili",
             "smoothed_whh_cmnty_cli", "smoothed_wnohh_cmnty_cli",

             # travel
             "smoothed_travel_outside_state_5d",
             "smoothed_wtravel_outside_state_5d",

             # work outside home
             # pre-wave-4
             "wip_smoothed_work_outside_home_5d",
             "wip_smoothed_wwork_outside_home_5d"
             )

test_that("testing existence of csv files", {
  grid <- expand.grid(
    geo_levels = geo_levels, dates = dates, metrics = metrics, stringsAsFactors=FALSE
  )
  expected_files <- sprintf("%s_%s_%s.csv", grid$dates, grid$geo_levels, grid$metrics)
  actual_files <- dir(test_path("receiving"))

  expect_setequal(expected_files, actual_files)
})

test_that("testing geo files are the same", {
  # We only have data from two zips, each in their own state/county/msa, so the values in
  # for each geographic level should be the same
  geo_levels <- c("state", "county", "hrr", "msa")
  dates <- c("20200510", "20200511", "20200512", "20200513")

  grid <- expand.grid(metrics = metrics, dates = dates, stringsAsFactors=FALSE)
  for (i in seq_len(nrow(grid)))
  {
    fnames <- test_path(
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

test_that("testing geo files contain correct number of lines", {
  grid <- expand.grid(
    geo_levels = geo_levels, dates = dates, metrics = metrics, stringsAsFactors=FALSE
  )
  fnames <- test_path(
    "receiving", sprintf("%s_%s_%s.csv", grid$dates, grid$geo_levels, grid$metrics)
  )

  dt <- lapply(fnames, read_csv)
  dt_nrow <- sapply(dt, nrow)

  expect_true(all(dt_nrow[grid$dates == "20200510"] == 1L))
  expect_true(all(dt_nrow[grid$dates == "20200511"] == 1L))
  expect_true(all(dt_nrow[grid$dates == "20200512" & grid$geo_levels != "nation"] == 2L))
  expect_true(all(
    dt_nrow[grid$dates == "20200513" & stri_detect(fnames, fixed = "raw")] == 1L
  ))
  expect_true(all(
    dt_nrow[grid$dates == "20200513" &
            stri_detect(fnames, fixed = "smoothed") &
            grid$geo_levels != "nation"] == 2L
  ))
  expect_true(all(dt_nrow[grid$geo_levels == "nation"] == 1L))
})

test_that("testing raw community values files", {

  # there are 2 / 4 households in PA on 2020-05-11 for community
  x <- read_csv(test_path("receiving", "20200511_state_raw_nohh_cmnty_cli.csv"))
  expect_equal(x$geo_id, "pa")
  expect_equal(x$sample_size, 4L)
  eval <-  100 * (2 + 0.5) / ( 4 + 1 )
  ese <- sqrt( eval * (100 - eval) ) / sqrt( 4 )
  expect_equal(x$val, eval)
  expect_equal(x$se, ese)

  # there are 4 / 4 households in PA on 2020-05-11 for community
  # there are 3 / 4 households in VA on 2020-05-11 for community
  x <- read_csv(test_path("receiving", "20200512_state_raw_nohh_cmnty_cli.csv"))
  x <- arrange(x, geo_id)
  expect_equal(x$geo_id, c("pa", "va"))
  expect_equal(x$sample_size, c(4L, 4L))
  eval <-  100 * (c(4, 3) + 0.5) / ( c(4, 4) + 1 )
  ese <- sqrt( eval * (100 - eval) ) / sqrt( c(4, 4) )
  expect_equal(x$val, eval)
  expect_equal(x$se, ese)

})

test_that("testing smoothed community values files at endpoints", {

  # for the first datapoint, smoothing and raw should be the same
  x_smooth <- read_csv(test_path("receiving", "20200510_state_smoothed_nohh_cmnty_cli.csv"))
  x_raw <- read_csv(test_path("receiving", "20200510_state_raw_nohh_cmnty_cli.csv"))
  expect_equal(x_raw, x_smooth)

  # for the first datapoint, smoothing and raw should be the same in VA
  x_smooth <- read_csv(test_path("receiving", "20200512_state_smoothed_nohh_cmnty_cli.csv"))
  x_raw <- read_csv(test_path("receiving", "20200512_state_raw_nohh_cmnty_cli.csv"))
  expect_equal(x_raw[x_raw$geo_id == "va",], x_smooth[x_raw$geo_id == "va",])
})

test_that("testing smoothed community values files", {

  # there are 0 / 4 households in PA on 2020-05-10 for community
  # there are 2 / 4 households in PA on 2020-05-11 for community
  #    after pooling => 2 / 8
  eval <-  100 * (2 + 0.5) / ( 8 + 1 )
  ese <- sqrt( eval * (100 - eval) / 8 )
  x_smooth <- read_csv(test_path("receiving", "20200511_state_smoothed_nohh_cmnty_cli.csv"))
  expect_equal(eval, x_smooth$val)
  expect_equal(8L, x_smooth$sample_size)
  expect_equal(ese, x_smooth$se)

  # there are 0 / 4 households in PA on 2020-05-10 for community
  # there are 2 / 4 households in PA on 2020-05-11 for community
  # there are 4 / 4 households in PA on 2020-05-12 for community
  #    after pooling => 6 / 12
  eval <-  100 * (6 + 0.5) / ( 12 + 1 )
  ese <- sqrt( eval * (100 - eval) / 12 )
  x_smooth <- read_csv(test_path("receiving", "20200512_state_smoothed_nohh_cmnty_cli.csv"))
  expect_equal(eval, x_smooth$val[x_smooth$geo_id == "pa"])
  expect_equal(12L, x_smooth$sample_size[x_smooth$geo_id == "pa"])
  expect_equal(ese, x_smooth$se[x_smooth$geo_id == "pa"])

})

test_that("testing weighted community values files", {

  params <- relativize_params(read_params(test_path("params-test.json")))

  input_data <- load_responses_all(params)
  input_data <- add_weights(input_data, params)$df

  # there are 2 / 4 households in PA on 2020-05-11 for community
  these <- input_data[input_data$date == "2020-05-11" & input_data$zip5 == "15106",]
  these_weight <- these$weight
  these_yes <- as.numeric(these$A4 > 1)

  these_val <- sum(these_yes * these_weight)
  these_val <-  weighted.mean(these_yes, these_weight) * length(these_yes)
  these_ss <- length(these_yes)
  these_val <- 100 * (these_val + 0.5) / (these_ss + 1)
  these_se <- sqrt(these_val * (100 - these_val) ) / sqrt( these_ss )

  x <- read_csv(test_path("receiving", "20200511_state_raw_wnohh_cmnty_cli.csv"))
  expect_equal(x$geo_id, "pa")
  expect_equal(x$sample_size, these_ss)
  expect_equal(x$val, these_val)
  expect_equal(x$se, these_se)

  # there are 2 / 4 households in VA on 2020-05-13 for community
  these <- input_data[input_data$date == "2020-05-13" & input_data$zip5 == "23220",]
  these_weight <- these$weight
  these_yes <- as.numeric(these$A4 > 1)

  these_val <- sum(these_yes * these_weight)
  these_val <-  weighted.mean(these_yes, these_weight) * length(these_yes)
  these_ss <- length(these_yes)
  these_val <- 100 * (these_val + 0.5) / (these_ss + 1)
  these_se <- sqrt(these_val * (100 - these_val) ) / sqrt( these_ss )

  x <- read_csv(test_path("receiving", "20200513_state_raw_wnohh_cmnty_cli.csv"))
  expect_equal(x$geo_id, "va")
  expect_equal(x$sample_size, these_ss)
  expect_equal(x$val, these_val)
  expect_equal(x$se, these_se)
})

test_that("testing weighted smoothed community values files", {

  params <- relativize_params(read_params(test_path("params-test.json")))

  input_data <- load_responses_all(params)
  input_data <- add_weights(input_data, params)$df

  # there are 2 / 4 households in PA on 2020-05-11 for community
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

  x <- read_csv(test_path("receiving", "20200511_state_smoothed_wnohh_cmnty_cli.csv"))
  expect_equal(x$geo_id, "pa")
  expect_equal(x$sample_size, these_ss)
  expect_equal(x$val, these_val)
  expect_equal(x$se, these_se)

})

test_that("testing raw ili/cli values files", {

  # there are 2 / 4 households in PA on 2020-05-10 with ILI (all households have size 1)
  x <- read_csv(test_path("receiving", "20200510_state_raw_ili.csv"))
  expect_equal(x$geo_id, "pa")
  expect_equal(x$val, 50)
  expect_equal(x$sample_size, 4L)
  ese <- sqrt(sum((c(100, 100, 0, 0) - 50)^2 * (1/4)^2))
  ese <- jeffreys_se(ese, 50, 4L)
  expect_equal(x$se, ese)

  # there are 1 / 4 households in PA on 2020-05-11 with ILI (all households have size 1)
  x <- read_csv(test_path("receiving", "20200511_state_raw_ili.csv"))
  expect_equal(x$geo_id, "pa")
  expect_equal(x$val, 25)
  expect_equal(x$sample_size, 4L)
  ese <- sqrt(sum((c(100, 0, 0, 0) - 25)^2 * (1/4)^2))
  ese <- jeffreys_se(ese, 25, 4L)
  expect_equal(x$se, ese)

  # there are 2 / 4 households in VA on 2020-05-13 with ILI (all households have size 1)
  x <- read_csv(test_path("receiving", "20200513_state_raw_ili.csv"))
  expect_equal(x$geo_id, "va")
  expect_equal(x$val, 50)
  expect_equal(x$sample_size, 4L)
  ese <- sqrt(sum((c(100, 100, 0, 0) - 50)^2 * (1/4)^2))
  ese <- jeffreys_se(ese, 50, 4L)
  expect_equal(x$se, ese)

  # there are 1 / 4 households in PA on 2020-05-10 with CLI (all households have size 1)
  x <- read_csv(test_path("receiving", "20200510_state_raw_cli.csv"))
  expect_equal(x$geo_id, "pa")
  expect_equal(x$val, 25)
  expect_equal(x$sample_size, 4L)
  ese <- sqrt(sum((c(100, 0, 0, 0) - 25)^2 * (1/4)^2))
  ese <- jeffreys_se(ese, 25, 4L)
  expect_equal(x$se, ese)

  # there are 1 / 4 households in PA on 2020-05-11 with CLI (all households have size 1)
  x <- read_csv(test_path("receiving", "20200511_state_raw_cli.csv"))
  expect_equal(x$geo_id, "pa")
  expect_equal(x$val, 25)
  expect_equal(x$sample_size, 4L)
  ese <- sqrt(sum((c(100, 0, 0, 0) - 25)^2 * (1/4)^2))
  ese <- jeffreys_se(ese, 25, 4L)
  expect_equal(x$se, ese)
})


test_that("testing pooled ili/cli values files at endpoints", {

  x_raw <- read_csv(test_path("receiving", "20200510_state_raw_ili.csv"))
  x_smooth <- read_csv(test_path("receiving", "20200510_state_smoothed_ili.csv"))
  expect_equal(x_raw, x_smooth)

  x_raw <- read_csv(test_path("receiving", "20200510_state_raw_cli.csv"))
  x_smooth <- read_csv(test_path("receiving", "20200510_state_smoothed_cli.csv"))
  expect_equal(x_raw, x_smooth)

  x_raw <- read_csv(test_path("receiving", "20200512_state_raw_ili.csv"))
  x_smooth <- read_csv(test_path("receiving", "20200512_state_smoothed_ili.csv"))
  expect_equal(x_raw[x_raw$geo_id == "va",], x_smooth[x_smooth$geo_id == "va",])

  x_raw <- read_csv(test_path("receiving", "20200512_state_raw_cli.csv"))
  x_smooth <- read_csv(test_path("receiving", "20200512_state_smoothed_cli.csv"))
  expect_equal(x_raw[x_raw$geo_id == "va",], x_smooth[x_smooth$geo_id == "va",])

})

test_that("testing pooled ili/cli values files", {

  # there are 2 / 4 households in PA on 2020-05-10 with ILI (all households have size 1)
  # there are 1 / 4 households in PA on 2020-05-11 with ILI (all households have size 1)
  #   total => 3 / 8 of households
  x <- read_csv(test_path("receiving", "20200511_state_smoothed_ili.csv"))
  expect_equal(x$geo_id, "pa")
  expect_equal(x$val, 3 / 8 * 100)
  expect_equal(x$sample_size, 4L + 4L)
  ese <- sqrt(sum((c(rep(100, 3), rep(0, 5)) - 3 / 8 * 100)^2 * (1 / 8)^2))
  ese <- jeffreys_se(ese, 3 / 8 * 100, 8L)
  expect_equal(x$se, ese)

  # there are 1 / 4 households in PA on 2020-05-10 with CLI (all households have size 1)
  # there are 1 / 4 households in PA on 2020-05-11 with CLI (all households have size 1)
  #   total => 2 / 8 of households
  x <- read_csv(test_path("receiving", "20200511_state_smoothed_cli.csv"))
  expect_equal(x$geo_id, "pa")
  expect_equal(x$val, 2 / 8 * 100)
  expect_equal(x$sample_size, 4L + 4L)
  ese <- sqrt(sum((c(rep(100, 2), rep(0, 6)) - 2 / 8 * 100)^2 * (1 / 8)^2))
  ese <- jeffreys_se(ese, 2 / 8 * 100, 8L)
  expect_equal(x$se, ese)

  # there are 2 / 4 households in PA on 2020-05-10 with ILI (all households have size 1)
  # there are 1 / 4 households in PA on 2020-05-11 with ILI (all households have size 1)
  # there are 2 / 4 households in PA on 2020-05-12 with ILI (all households have size 1)
  #   total => 5 / 12 of households
  x <- read_csv(test_path("receiving", "20200512_state_smoothed_ili.csv"))
  expect_equal(x$val[x$geo_id == "pa"], 5 / 12 * 100)
  expect_equal(x$sample_size[x$geo_id == "pa"], 4L + 4L + 4L)
  ese <- sqrt(sum((c(rep(100, 5), rep(0, 7)) - 5 / 12 * 100)^2 * (1 / 12)^2))
  ese <- jeffreys_se(ese, 5 / 12 * 100, 12L)
  expect_equal(x$se[x$geo_id == "pa"], ese)

})

test_that("testing weighted ili/cli values files", {

  params <- relativize_params(read_params(test_path("params-test.json")))

  input_data <- load_responses_all(params)
  input_data <- add_weights(input_data, params)$df
  data_agg <- create_data_for_aggregation(input_data)

  ## There are 4 households in PA on 2020-05-11, one with ILI.
  these <- data_agg[data_agg$date == "2020-05-11" & data_agg$zip5 == "15106",]
  these_weight <- mix_weights(these$weight, params$s_mix_coef, params$s_weight)$weights

  hh_p_ili <- these$hh_p_ili
  these_val <- weighted.mean(hh_p_ili, these_weight)
  these_ss <- 4
  these_ess <- length(these_weight) * mean(these_weight)^2 / mean(these_weight^2)

  x <- read_csv(test_path("receiving", "20200511_state_raw_wili.csv"))
  expect_equal(x$geo_id, "pa")
  expect_equal(x$sample_size, these_ss)
  expect_equal(x$val, these_val)
  ese <- sqrt( sum(these_weight^2 * (hh_p_ili - these_val)^2 ) )
  ese <- jeffreys_se(ese, these_val, these_ess)
  expect_equal(x$se[x$geo_id == "pa"], ese)

  ## There are 4 households in PA on 2020-05-11, one with CLI
  these <- data_agg[data_agg$date == "2020-05-11" & data_agg$zip5 == "15106",]
  these_weight <- mix_weights(these$weight, params$s_mix_coef, params$s_weight)$weights

  hh_p_cli <- these$hh_p_cli
  these_val <- weighted.mean(hh_p_cli, these_weight)
  these_ss <- 4
  these_ess <- length(these_weight) * mean(these_weight)^2 / mean(these_weight^2)

  x <- read_csv(test_path("receiving", "20200511_state_raw_wcli.csv"))
  expect_equal(x$geo_id, "pa")
  expect_equal(x$sample_size, these_ss)
  expect_equal(x$val, these_val)
  ese <- sqrt( sum(these_weight^2 * (hh_p_cli - these_val)^2 ) )
  ese <- jeffreys_se(ese, these_val, these_ess)
  expect_equal(x$se[x$geo_id == "pa"], ese)
})

test_that("testing national aggregation", {
  grid <- expand.grid(
    dates = dates,
    metrics = c("raw_cli", "raw_ili", "raw_nohh_cmnty_cli", "raw_hh_cmnty_cli"),
    stringsAsFactors=FALSE
  )
  f_state <- sprintf("%s_%s_%s.csv", grid$dates, "state", grid$metrics)
  f_national <- sprintf("%s_%s_%s.csv", grid$dates, "nation", grid$metrics)

  for(i in seq_along(f_state))
  {
    x_state <- read_csv(test_path("receiving", f_state[i]))
    x_national <- read_csv(test_path("receiving", f_national[i]))

    expect_equal(sum(x_state$sample_size), x_national$sample_size)
    if (nrow(x_state) == 1L) { expect_equal(x_state$val, x_national$val) }
  }

})

test_that("testing load_responses behavior for missing input", {
  params <- relativize_params(read_params(test_path("params-test.json")))
  params$input <- c(params$input, "file-does-not-exist.csv")
  params$parallel <- TRUE
  expect_error(load_responses_all(params), regexp="ingestion and field creation failed")
})
