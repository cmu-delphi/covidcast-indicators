#!/usr/bin/env Rscript

## Combine and compress contingency tables by grouping variable set.
##
## Usage:
##
## Rscript contingency-combine.R path/to/individual/files/ path/to/rollup/files/
##
## Combines a set of contingency tables with a rollup CSV that contains all
## dates for a given set of grouping variables. Can also be used to combine a
## directory of tables spanning multiple time periods.

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(purrr)
  library(delphiFacebook)
})


#' Fetch all tables in a chosen directory. Combine and save according to grouping.
#'
#' @param input_dir Directory in which to look for survey CSV files, relative to
#'   the current working directory.
#' @param output_dir Directory in which to look for existing rollup files or
#'   create new ones, relative to the current working directory.
#' @param pattern Regular expression indicating which files in that directory to
#'   open. By default, selects all `.csv` files with standard table date prefix.
run_rollup <- function(input_dir, output_dir, pattern = "^[0-9]{8}_[0-9]{8}.*[.]csv.gz$") {
  if (!dir.exists(output_dir)) { dir.create(output_dir) }
  
  files <- list.files(input_dir, pattern = pattern)
  if (length(files) == 0) { stop("No matching contingency files to combine.") }

  # Get df of input files and corresponding output files. Reformat as a list
  # such that input files with same grouping variables (and thus same output
  # file) are in a character vector named with the output file.
  files <- map_dfr(files, get_file_properties)
  files <- lapply(split(files, files$rollup_name), function(x) {x$filename})

  seen_file <- file.path(output_dir, "seen.txt")
  if ( any(file.exists(names(files))) ) {
    assert(file.exists(seen_file),
           paste0("If any output file exists, ", seen_file, ", listing input ",
                  "files previously used in generating a combined table, should also exist"))
  }
  
  for (output_name in names(files)) {
    combined_output <- combine_tables(
      seen_file, 
      input_dir, 
      files[[output_name]], 
      file.path(output_dir, output_name))
    write_rollup(
      combined_output[["newly_seen_files"]],
      seen_file,
      combined_output[["output_df"]],
      file.path(output_dir, output_name))
  }
  
  return(NULL)
}

## Helper function to extract info from each file's filename.
get_file_properties <- function(filename) {
  short <- strsplit(filename, ".", fixed = TRUE)[[1]][1]
  parts <- strsplit(short, "_", fixed = TRUE)[[1]]
  
  group <- parts[3:length(parts)]
  # Specify compression format via name, to be parsed by `write_csv` later.
  partial_name <- paste0(paste0(group, collapse="_"), ".csv.gz")

  return(data.frame(
    filename=filename,
    rollup_name=partial_name))
}

## Helper function to load "seen" file.
load_seen_file <- function(seen_file) {
  if (!file.exists(seen_file)) {
    file.create(seen_file)
  }
  
  seen_files <- readLines(seen_file)
  return(seen_files)
}

#' Combine data from set of input files with existing output data.
#'
#' @param seen_file Path to file listing filenames that have been previously
#'   loaded into an output file.
#' @param input_dir Directory in which to look for survey CSV files, relative to
#'   the current working directory.
#' @param input_files Vector of paths to input files that share a set of
#'   grouping variables.
#' @param output_file Path to corresponding output file.
#' 
#' @return Named list of combined output dataframe and character vector.
combine_tables <- function(seen_file, input_dir, input_files, output_file) {
  cols <- cols(
    .default = col_guess(),
    survey_geo = col_character(),
    period_type = col_character(),
    geo_type = col_character(),
    aggregation_type = col_character(),
    country = col_character(),
    ISO_3 = col_character(),
    GID_0 = col_character(),
    region = col_character(),
    GID_1 = col_character(),
    state = col_character(),
    state_fips = col_character(),
    county = col_character(),
    county_fips = col_character()
  )
  
  # Get input data.
  input_df <- map_dfr(
    file.path(input_dir, input_files),
    function(f) {
      read_csv(f, col_types = cols)
    })
  
  seen_files <- load_seen_file(seen_file)
  if (any(input_files %in% seen_files)) {
    assert(file.exists(output_file),
           paste0("The output file ", output_file, " does not exist, but non-zero",
                  " files using the same grouping variables have been seen before."))
  }
  
  cols <- cols_condense(spec(input_df))
  if ( file.exists(output_file) ) {
    output_df <- read_csv(output_file, col_types = cols)
  } else {
    output_df <- input_df[FALSE,]
  }
  
  # Use all columns up to the first non-aggregate column to find unique rows.
  group_names <- names(output_df)
  report_names <- c("val", "se", "sample_size", "represented", "effective_sample_size")
  exclude_patterns <- paste0("^", report_names)
  exclude_map <- grepl(paste(exclude_patterns, collapse="|"), group_names)
  assert( any(exclude_map) ,
          "No value-reporting columns are available or their names have changed.")
  
  ind_first_report_col <- min(which(exclude_map))
  group_names <- group_names[ 1:ind_first_report_col-1 ]
  
  ## Deduplicate, keeping newest version by issue date of each unique row.
  # Merge the new data with the existing data, taking the last issue date for
  # any given grouping/geo level/date combo. This prevents duplication in case
  # of reissues. Note that the order matters: since arrange() uses order(),
  # which is a stable sort, ties will result in the input data being used in
  # preference over the existing rollup data.
  output_df <- bind_rows(output_df, input_df) %>%
    relocate(issue_date, .after=last_col()) %>% 
    arrange(issue_date) %>% 
    group_by(across(all_of(group_names))) %>% 
    slice_tail() %>% 
    ungroup() %>% 
    arrange(period_start)
  
  newly_seen <- setdiff(input_files, seen_files)
  
  return(list(
    output_df=output_df,
    newly_seen_files=newly_seen))
}

#' Save a combined dataframe and list of seen files to disk.
#'
#' @param newly_seen_files Character vector.
#' @param seen_file Path to file listing filenames that have been previously
#'   loaded into an output file.
#' @param output_df Output dataframe.
#' @param output_file Path to corresponding output file.
write_rollup <- function(newly_seen_files, seen_file, output_df, output_file) {
  # Automatically uses gzip compression based on output file name. Overwrites
  # existing file of the same name.
  write_csv(output_df, output_file)
  
  if (length(newly_seen_files) > 0) {
    write(newly_seen_files, seen_file, append=TRUE)
  }
  
  return(NULL)
}


args <- commandArgs(TRUE)

if (length(args) != 2) {
  stop("Usage: Rscript contingency-combine.R path/to/individual/files/ path/to/rollup/files/")
}

input_path <- args[1]
output_path <- args[2]

invisible(run_rollup(input_path, output_path))
