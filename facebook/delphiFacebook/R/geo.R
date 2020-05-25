#' Returns metadata about each U.S. Zip Code
#'
#' @param static_dir     local directory containing the file "02_20_uszips.csv"
#'
#' @importFrom stringi stri_trans_tolower
#' @importFrom readr read_csv cols
produce_zip_metadata <- function(static_dir)
{
  zip_metadata <- read_csv(
    file.path(static_dir, "02_20_uszips.csv"),
    col_types = cols(zip = "i", fips = "i", hrrnum = "i"),
  )
  zip_metadata$state_id <- stri_trans_tolower(zip_metadata$state_id)
  zip_metadata$state_name <- stri_trans_tolower(zip_metadata$state_name)
  zip_metadata$county_weights <- stri_replace_all(
    zip_metadata$county_weights, "\"", fixed = "\'"
  )
  zip_metadata$fips <- sprintf("%05d", zip_metadata$fips)
  zip_metadata$zip5 <- sprintf("%05d", zip_metadata$zip)
  zip_metadata$keep_in_agg <- (zip_metadata$population > 100)

  return(zip_metadata)
}

#' Returns list of zip codes with large enough population to include in output
#'
#' For privacy reasons, we only include zip codes with a population over 100 people.
#'
#' @param static_dir     local directory containing the file "02_20_uszips.csv"
#'
#' @export
produce_allowed_zip5 <- function(static_dir)
{
  zip_metadata <- produce_zip_metadata(static_dir)

  return(zip_metadata$zip5[zip_metadata$keep_in_agg])
}

#' Produce crosswalk file relating zip codes to counties
#'
#' @param static_dir     local directory containing the file "02_20_uszips.csv"
#' @param zip_metadata   output from a call to the function produce_allowed_zip5
#'
#' @return  A tibble containing three columns: zip5, geo_id (county fips codes), and
#'          weight_in_location. The variable weight_in_location will sum to 1 when grouped
#'          by zip5.
#'
#' @importFrom dplyr tibble
#' @importFrom jsonlite fromJSON
produce_crosswalk_county <- function(static_dir, zip_metadata)
{
  cweights <- lapply(zip_metadata$county_weights, fromJSON)
  crosswalk_county <- tibble(
    zip5 = rep(zip_metadata$zip5, sapply(cweights, length)),
    geo_id = unlist(lapply(cweights, names)),
    weight_in_location = unlist(cweights) / 100
  )
  crosswalk_county <- crosswalk_county[crosswalk_county$weight_in_location > 0,]

  return(crosswalk_county)
}

#' Produce crosswalk file relating zip codes to states
#'
#' @param zip_metadata       output from a call to the function produce_allowed_zip5
#' @param crosswalk_county   output from a call to the function produce_crosswalk_county
#'
#' @return  A tibble containing three columns: zip5, geo_id (state two-letter codes), and
#'          weight_in_location. The variable weight_in_location will sum to 1 when grouped
#'          by zip5.
#'
#' @importFrom stringi stri_sub
#' @importFrom dplyr select left_join
produce_crosswalk_state <- function(zip_metadata, crosswalk_county)
{
  county_to_state <- zip_metadata
  county_to_state$state <- stri_sub(zip_metadata$fips, 1L, 2L)
  county_to_state <- unique(county_to_state[,c("state", "state_id")])

  crosswalk_state <- select(crosswalk_county, "zip5", county = "geo_id", "weight_in_location")
  crosswalk_state$state <- stri_sub(crosswalk_state$county, 1, 2)
  crosswalk_state <- left_join(crosswalk_state, county_to_state, by = "state")
  crosswalk_state <- select(crosswalk_state, "zip5", geo_id = "state_id", "weight_in_location")
  crosswalk_state <- crosswalk_state[crosswalk_state$weight_in_location > 0,]

  return(crosswalk_state)
}

#' Produce crosswalk file relating zip codes to hospital referral regions (HRRs)
#'
#' @param zip_metadata   output from a call to the function produce_allowed_zip5
#'
#' @return  A tibble containing three columns: zip5, geo_id (HRR code as character), and
#'          weight_in_location. The variable weight_in_location will sum to 1 when grouped
#'          by zip5.
#'
produce_crosswalk_hrr <- function(zip_metadata)
{
  crosswalk_hrr <- zip_metadata[!is.na(zip_metadata$hrrnum), ]
  crosswalk_hrr$geo_id <- sprintf("%03d", crosswalk_hrr$hrrnum)
  crosswalk_hrr$weight_in_location <- 1
  crosswalk_hrr <- crosswalk_hrr[, c("zip5", "geo_id", "weight_in_location")]

  return(crosswalk_hrr)
}

#' Produce crosswalk file relating zip codes to metropolitan statistical areas (MSAs)
#'
#' @param static_dir         local directory containing the file "02_20_uszips.csv"
#' @param crosswalk_county   output from a call to the function produce_crosswalk_county
#'
#' @return  A tibble containing three columns: zip5, geo_id (MSA fips codes), and
#'          weight_in_location. The variable weight_in_location will sum to 1 when grouped
#'          by zip5.
#'
#' @importFrom readr read_csv
#' @importFrom dplyr inner_join group_by summarize
#' @importFrom rlang .data
produce_crosswalk_msa <- function(static_dir, crosswalk_county)
{
  fips_to_msa <- read_csv(
    file.path(static_dir, "msa_list.csv"),
    col_types = "cccc",
    col_names = c("geo_id", "msa", "fips", "county_name"),
    skip = 1L
  )
  crosswalk_msa <- inner_join(crosswalk_county, fips_to_msa, by=c("geo_id" = "fips"))
  crosswalk_msa <- group_by(crosswalk_msa, .data$zip5, .data$geo_id)
  crosswalk_msa <- summarize(
    crosswalk_msa, weight_in_location = sum(.data$weight_in_location, na.rm = TRUE)
  )
  crosswalk_msa <- ungroup(crosswalk_msa)

  return(crosswalk_msa)
}

#' Produce a list of crosswalk files relating zip5s to geographic regions
#'
#' @param static_dir         local directory containing the file "02_20_uszips.csv"
#'
#' @return  A list containing elements for four geographic regions: county, state, msa, and
#'          hrr (hospital referral region). Each element contains a tibble with three
#'          columns named "zip5", "geo_id", and "weight_in_location"
#'
#' @export
produce_crosswalk_list <- function(static_dir)
{
  zip_metadata <- produce_zip_metadata(static_dir)

  crosswalk_county <- produce_crosswalk_county(static_dir, zip_metadata)
  crosswalk_state <- produce_crosswalk_state(zip_metadata, crosswalk_county)
  crosswalk_hrr <- produce_crosswalk_hrr(zip_metadata)
  crosswalk_msa <- produce_crosswalk_msa(static_dir, crosswalk_county)

  cw_list <- list(
    "county" = crosswalk_county,
    "state" = crosswalk_state,
    "msa" = crosswalk_hrr,
    "hrr" = crosswalk_msa
  )

  return (cw_list)
}
