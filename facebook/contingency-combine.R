#!/usr/bin/env Rscript

## Combine and compress contingency tables by aggregation.
##
## Usage:
##
## Rscript contingency-combine.R path/to/individual/files/ path/to/rollup/files/
##
## Appends a set of newly-generated contingency tables to a rollup CSV that
## contains all dates for a given set of grouping variables. Can also be used to
## combine a directory of tables spanning multiple time periods.

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(purrr)
  library(delphiFacebook)
})


#' Fetch all tables in a chosen directory and combine according to grouping
#' used.
#'
#' @param input_dir Directory in which to look for survey CSV files, relative to
#'   the current working directory.
#' @param output_dir Directory in which to look for existing rollup files or
#'   create new ones, relative to the current working directory.
#' @param pattern Regular expression indicating which files in that directory to
#'   open. By default, selects all `.csv` files with standard table date prefix.
run_rollup <- function(input_dir, output_dir, pattern = "^[0-9]{8}_[0-9]{8}.*[.]csv$") {
  files <- list.files(input_dir, pattern = pattern)
  if (length(files) == 0) {
    stop("No matching data files.")
  }

  files <- map_dfr(files, get_file_properties)
  
  # Reformat files as a list such that input files with same grouping variables
  # (and thus same output file) are in a character vector named with the output
  # file.
  files <- lapply(split(files, files$rollup_name), function(x) {x$filename})
  
  if (!dir.exists(output_dir)) { dir.create(output_dir) }
  seen_file <- file.path(output_dir, "seen.txt")
  seen_files <- load_seen_file(seen_file)
  
  for (output_name in names(files)) {
    browser
    newly_seen_files <- combine_and_save_tables(
      seen_files,
      input_dir,
      files[[output_name]], 
      file.path(output_dir, output_name))
    browser()  
    write(newly_seen_files, seen_file, append=TRUE)
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

#' Combine set of input files with existing output file, and save to disk.
#'
#' If an input filename has been seen before, the input and output data are
#' deduplicated to use the newer set of data. Output is saved in gzip-compressed
#' format.
#'
#' @param seen_files Vector of filenames that have been previously loaded into
#'   an output file.
#' @param input_dir Directory in which to look for survey CSV files, relative to
#'   the current working directory.
#' @param input_files Vector of paths to input files that share a set of
#'   grouping variables.
#' @param output_file Path to corresponding output file.
#' 
#' @return Character vector of newly-seen filenames.
combine_and_save_tables <- function(seen_files, input_dir, input_files, output_file) {
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
    }
  )
  
  if (file.exists(output_file)) {
    output_names <- names(read_csv(output_file, n_max = 0L))
    assert(identical(output_names, names(input_df)),
           paste0("Column names and/or order differ between new and old input for ", output_file))
  }
  
  # If no input files have been seen before, we can append directly to the
  # output file without needing to deduplicate. File is created if it doesn't
  # already exist.
  any_prev_seen <- any(input_files %in% seen_files)
  
  if (!any_prev_seen) {
    write_csv(input_df, output_file, append=file.exists(output_file))
  } else {
    assert(file.exists(output_file),
           paste0("The output file ", output_file, " does not exist, but ",
                  "non-zero files using the same grouping have been seen before."))
    
    output_df <- read_csv(output_file, col_types = cols)
    
    # Use all columns up to the first "val" column to find unique rows.
    group_names <- names(output_df)
    ind_first_val_col <- min(which(startsWith(group_names, "val_")))
    group_names <- group_names[ 1:ind_first_val_col-1 ]
    
    ## Deduplicate, keeping newest version by issue date of each unique row.
    # Merge the new data with the existing data, taking the last issue date for
    # any given grouping/geo level/date combo. This prevents duplication in case
    # of reissues. Note that the order matters: since arrange() uses order(),
    # which is a stable sort, ties will result in the input data being used in
    # preference over the existing rollup data.
    output_df <- bind_rows(output_df, input_df) %>%
      arrange(issue_date) %>% 
      group_by(across(all_of(group_names))) %>% 
      slice_tail() %>% 
      ungroup()
    
    # Automatically uses gzip compression based on output file name.
    write_csv(output_df, output_file)
  }
  
  newly_seen <- setdiff(input_files, seen_files)
  return(newly_seen)
}


args <- commandArgs(TRUE)

if (length(args) < 2) {
  stop("Usage: Rscript contingency-combine.R path/to/individual/files/ path/to/rollup/files/")
}

input_path <- args[1]
output_path <- args[2]

invisible(run_rollup(input_path, output_path))
