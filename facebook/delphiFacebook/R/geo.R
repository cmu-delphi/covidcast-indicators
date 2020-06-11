#' Returns metadata about each U.S. Zip Code
#'
#' @param static_dir     local directory containing the file "02_20_uszips.csv"
#'
#' @importFrom stringi stri_trans_tolower
#' @importFrom readr read_csv cols
#' @export
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

#' Produce crosswalk file relating zip codes to the entire U.S.
#'
#' Every zip code is mapped to one entity, "usa".
#'
#' @param zip_metadata   output from a call to the function produce_allowed_zip5
#'
#' @return  A tibble containing three columns: zip5, geo_id (county fips codes), and
#'          weight_in_location.
#'
#' @importFrom dplyr tibble
#' @export
produce_crosswalk_national <- function(zip_metadata)
{
  crosswalk_national <- tibble(
    zip5 = zip_metadata$zip5,
    geo_id = "usa",
    weight_in_location = 1.0
  )

  return(crosswalk_national)
}

#' Produce crosswalk file relating zip codes to counties
#'
#' @param static_dir     local directory containing the file "02_20_uszips.csv"
#' @param zip_metadata   output from a call to the function produce_allowed_zip5
#'
#' @return A tibble containing three columns: zip5, geo_id (county fips codes),
#'     and weight_in_location. Since ZIP codes can span multiple counties, the
#'     weight_in_location indicates the fraction of the ZIP population contained
#'     in the county. When grouped by ZIP5, this should sum to 1.
#'
#' @importFrom dplyr tibble
#' @importFrom jsonlite fromJSON
#' @export
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
#' @importFrom dplyr select left_join summarize group_by ungroup
#' @export
produce_crosswalk_state <- function(zip_metadata, crosswalk_county)
{
  # NOTE: This is the "correct" way to do the mapping, as it does not assume that every
  # county-equivalent has a zip code that is predominantly inside of it, but for now using
  # the code below to match the reference implementation

  # state_fips <- zip_metadata
  # state_fips$state <- stri_sub(state_fips$fips, 1L, 2L)
  # state_fips <- unique(state_fips[,c("state", "state_id")])
  #
  # crosswalk_state <- select(crosswalk_county, "zip5", county = "geo_id", "weight_in_location")
  # crosswalk_state$state <- stri_sub(crosswalk_state$county, 1, 2)
  # crosswalk_state <- left_join(crosswalk_state, state_fips, by = "state")
  # crosswalk_state <- select(crosswalk_state, "zip5", geo_id = "state_id", "weight_in_location")
  # crosswalk_state <- group_by(crosswalk_state, zip5, geo_id)
  # crosswalk_state <- summarize(crosswalk_state, weight_in_location = sum(weight_in_location))
  # crosswalk_state <- ungroup(crosswalk_state)

  county_to_state <- unique(select(zip_metadata, .data$fips, .data$state_id))

  crosswalk_state <- select(crosswalk_county, "zip5", fips = "geo_id", "weight_in_location")
  crosswalk_state <- inner_join(crosswalk_state, county_to_state, by = "fips")
  crosswalk_state <- select(
    crosswalk_state, .data$zip5, geo_id = .data$state_id, .data$weight_in_location
  )

  ## Some ZIP codes span counties. Hence when we join ZIPs with counties above,
  ## we can get multiple rows for one ZIP. Then, when we join with state, those
  ## counties might be in one state -- so consolidate the rows back into one row
  ## with the correct weight. (You might think it doesn't matter, because two
  ## observations with weight 0.5 should be the same as one with weight 1, but
  ## the mixing algorithm can kick in and change the estimates.)
  crosswalk_state <- group_by(crosswalk_state, .data$zip5, .data$geo_id)
  crosswalk_state <- summarize(
    crosswalk_state, weight_in_location = sum(.data$weight_in_location, na.rm = TRUE)
  )
  crosswalk_state <- ungroup(crosswalk_state)

  return(crosswalk_state)
}

#' Produce crosswalk file relating zip codes to hospital referral regions (HRRs)
#'
#' Each ZIP5 is completely contained in a HRR, and the corresponding HRR is
#' provided in our ZIP metadata.
#'
#' @param zip_metadata   output from a call to the function produce_allowed_zip5
#'
#' @return  A tibble containing three columns: zip5, geo_id (HRR code as character), and
#'          weight_in_location. The variable weight_in_location will sum to 1 when grouped
#'          by zip5.
#' @export
produce_crosswalk_hrr <- function(zip_metadata)
{
  crosswalk_hrr <- zip_metadata[!is.na(zip_metadata$hrrnum), ]
  crosswalk_hrr$geo_id <- as.character(crosswalk_hrr$hrrnum)
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
#' @export
produce_crosswalk_msa <- function(static_dir, crosswalk_county)
{
  fips_to_msa <- read_csv(
    file.path(static_dir, "msa_list.csv"),
    col_types = "cccc",
    col_names = c("geo_id", "msa", "fips", "county_name"),
    skip = 1L
  )
  names(crosswalk_county)[2] <- "fips"
  crosswalk_msa <- inner_join(crosswalk_county, fips_to_msa, by=c("fips"))
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
  crosswalk_national <- produce_crosswalk_national(zip_metadata)

  cw_list <- list(
    "county" = crosswalk_county,
    "state" = crosswalk_state,
    "hrr" = crosswalk_hrr,
    "msa" = crosswalk_msa,
    "national" = crosswalk_national
  )

  return (cw_list)
}
