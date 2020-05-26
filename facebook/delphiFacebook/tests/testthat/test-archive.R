library(delphiFacebook)
library(dplyr)
library(lubridate)
library(readr)

context("Testing creating of individual response output data for external sharing")

test_that("testing load_archive command", {
  archive <- load_archive(list(archive_dir = "archive", debug = FALSE))

  expect_setequal(names(archive), c("data_agg", "seen_tokens"))
  expect_equal(nrow(archive$seen_tokens), 20L)
})
