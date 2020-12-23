library(jsonlite)

context("Testing geographic crosswalk file creation")

static_dir <- "static"

test_that("testing zip codes metadata", {

  zip_metadata <- produce_zip_metadata(static_dir)
  expect_equal(class(zip_metadata$zip5), "character")
  expect_true(all(nchar(zip_metadata$zip5) == 5L))
  expect_equal(nrow(zip_metadata), 33099L)
  expect_equal(zip_metadata$keep_in_agg, zip_metadata$population > 100)

})

test_that("testing allowed zips function", {

  zip_metadata <- produce_zip_metadata(static_dir)
  allowed_zips <- produce_allowed_zip5(static_dir)
  expect_setequal(allowed_zips, zip_metadata$zip5[zip_metadata$keep_in_agg])

})

test_that("testing county crosswalk function", {

  zip_metadata <- produce_zip_metadata(static_dir)
  crosswalk_county <- produce_crosswalk_county(zip_metadata)

  expect_setequal(names(crosswalk_county), c("zip5", "geo_id", "weight_in_location"))

})

test_that("testing state crosswalk function", {

  zip_metadata <- produce_zip_metadata(static_dir)
  crosswalk_county <- produce_crosswalk_county(zip_metadata)
  crosswalk_state <- produce_crosswalk_state(zip_metadata, crosswalk_county)

  expect_setequal(names(crosswalk_state), c("zip5", "geo_id", "weight_in_location"))

})

test_that("testing HRR crosswalk function", {

  zip_metadata <- produce_zip_metadata(static_dir)
  crosswalk_hrr <- produce_crosswalk_hrr(zip_metadata)

  expect_setequal(names(crosswalk_hrr), c("zip5", "geo_id", "weight_in_location"))

})

test_that("testing MSA crosswalk function", {

  zip_metadata <- produce_zip_metadata(static_dir)
  crosswalk_county <- produce_crosswalk_county(zip_metadata)
  crosswalk_msa <- produce_crosswalk_msa(static_dir, crosswalk_county)

  expect_setequal(names(crosswalk_msa), c("zip5", "geo_id", "weight_in_location"))

})

test_that("testing creating all crosswalk files", {

  cw_list <- produce_crosswalk_list(static_dir)

  expect_setequal(names(cw_list), c("county", "state", "msa", "hrr", "nation"))
  expect_true(all(sapply(cw_list, function(v) inherits(v, "data.frame"))))

})
