## Functions for performing the aggregations in an efficient way.

#' Produce all desired aggregations.
#'
#' Writes the outputs directly to CSVs in the directory specified by `params`.
#' Produces output using all available data between `params$start_date` and
#' `params$end_date`, inclusive.
#'
#' @param df Data frame of individual response data.
#' @param aggregations Data frame with columns `name`, `var_weight`, `metric`,
#'   `group_by`, `compute_fn`, `post_fn`. Each row represents one aggregate
#'   to report. `name` is the aggregate's base column name; `var_weight` is the
#'   column to use for its weights; `metric` is the column of `df` containing the
#'   response value. `group_by` is a list of variables used to perform the
#'   aggregations over. `compute_fn` is the function that computes
#'   the aggregate response given many rows of data. `post_fn` is applied to the
#'   aggregate data after megacounty aggregation, and can perform any final
#'   calculations necessary.
#' @param cw_list Named list of geographic crosswalks, each of which maps a zip5
#'   to a geographic level such as county or state. Aggregates will be produced
#'   for each geographic level.
#' @param params Named list of configuration parameters.
#'
#' @return none
#'
#' @import data.table
#' @importFrom dplyr full_join %>% select all_of
#' @importFrom purrr reduce
#'
#' @export
produce_aggregates <- function(df, aggregations, cw_list, params) {
  msg_plain("Producing contingency aggregates...")
  ## For the date range lookups we do on df, use a data.table key. This puts the
  ## table in sorted order so data.table can use a binary search to find
  ## matching dates, rather than a linear scan, and is important for very large
  ## input files.
  df <- as.data.table(df)
  setkeyv(df, "start_dt")

  # Keep only obs in desired date range.
  df <- df[start_dt >= params$start_time & start_dt <= params$end_time]

  output <- post_process_aggs(df, aggregations, cw_list)
  df <- output[[1]]
  aggregations <- output[[2]]

  ## Keep only columns used in indicators, plus supporting columns.
  group_vars <- unique( unlist(aggregations$group_by) )
  df <- select(df,
               all_of(unique(aggregations$metric)),
               all_of(unique(aggregations$var_weight)),
               all_of( group_vars[group_vars != "geo_id"] ),
               zip5,
               start_dt)

  agg_groups <- unique(aggregations[c("group_by", "geo_level")])

  # For each unique combination of group_vars and geo level, run aggregation process once
  # and calculate all desired aggregations on the grouping. Rename columns. Save
  # to individual files
  for (group_ind in seq_along(agg_groups$group_by)) {

    agg_group <- agg_groups$group_by[group_ind][[1]]
    geo_level <- agg_groups$geo_level[group_ind]
    geo_crosswalk <- cw_list[[geo_level]]

    # Subset aggregations to keep only those grouping by the current agg_group
    # and with the current geo_level. `setequal` ignores differences in
    # ordering and only looks at unique elements.
    these_aggs <- aggregations[mapply(aggregations$group_by,
                                      FUN=function(x) {setequal(x, agg_group)
                                      }) & aggregations$geo_level == geo_level, ]

    dfs_out <- summarize_aggs(df, geo_crosswalk, these_aggs, geo_level, params)

    ## To display other response columns ("val", "sample_size", "se",
    ## "effective_sample_size", "represented"), add here.
    keep_vars <- c("val", "se", "sample_size", "represented")

    for (agg_id in names(dfs_out)) {
      if (nrow(dfs_out[[agg_id]]) == 0) {
        dfs_out[[agg_id]] <- NULL
        next
      }
      agg_metric <- aggregations$name[aggregations$id == agg_id]
      map_old_new_names <- keep_vars
      names(map_old_new_names) <- paste(keep_vars, agg_metric, sep="_")

      dfs_out[[agg_id]] <- rename(
        dfs_out[[agg_id]][, c(agg_group, keep_vars)], all_of(map_old_new_names))
    }

    if ( length(dfs_out) != 0 ) {
      df_out <- dfs_out %>% reduce(full_join, by=agg_group, suff=c("", ""))
      write_contingency_tables(df_out, params, geo_level, agg_group)
    }
  }
}

#' Process aggregations to make formatting more consistent
#'
#' Parse grouping variables for geolevel, and save to new column for easy
#' access. If none, assume national. If `metric` is a multiple choice item,
#' include it in list of grouping variables so levels are included in output
#' CSV. Alphabetize grouping variables; columns are saved to output CSV in this
#' order.
#'
#' Most columns specified in `aggregations` are converted into the appropriate
#' format (binary response codes -> logical, etc). Multi-select `metrics` are
#' turned into a series of binary columns. Each binary is given its own
#' aggregation, sharing grouping variables and other settings with the original
#' multi-select agg.
#'
#' @param df Data frame of individual response data.
#' @param aggregations Data frame with columns `name`, `var_weight`, `metric`,
#'   `group_by`, `compute_fn`, `post_fn`. Each row represents one aggregate
#'   to report. `name` is the aggregate's base column name; `var_weight` is the
#'   column to use for its weights; `metric` is the column of `df` containing the
#'   response value. `group_by` is a list of variables to aggregate
#'   over. `compute_fn` is the function that computes
#'   the aggregate response given many rows of data. `post_fn` is applied to the
#'   aggregate data after megacounty aggregation, and can perform any final
#'   calculations necessary.
#' @param cw_list Named list of geographic crosswalks, each of which maps a zip5
#'   to a geographic level such as county or state. Aggregates will be produced
#'   for each geographic level.
#'
#' @return list of data frame of individual response data and user-set data
#' frame of desired aggregations
#'
#' @export
post_process_aggs <- function(df, aggregations, cw_list) {
  aggregations$geo_level <- NA
  for (agg_ind in seq_along(aggregations$group_by)) {
    # Find geo_level, if any, present in provided group_by vars
    geo_level <- intersect(aggregations$group_by[agg_ind][[1]], names(cw_list))

    # Add implied geo_level to each group_by. Order alphabetically. Replace
    # geo_level with generic "geo_id" var. Remove duplicate grouping vars.
    if (length(geo_level) > 1) {
      stop('more than one geo type provided for a single aggregation')

    } else if (length(geo_level) == 0) {
      # Presume national grouping
      geo_level <- "nation"
      aggregations$group_by[agg_ind][[1]] <-
        sort(append(aggregations$group_by[agg_ind][[1]], "geo_id"))

    } else {
      aggregations$group_by[agg_ind][[1]][
        aggregations$group_by[agg_ind][[1]] == geo_level] <- "geo_id"
      aggregations$group_by[agg_ind][[1]] <-
        sort(unique(aggregations$group_by[agg_ind][[1]]))
    }

    aggregations$geo_level[agg_ind] <- geo_level

    # Multiple choice metrics should also be included in the group_by vars
    if (startsWith(aggregations$metric[agg_ind], "mc_")) {
      if ( !(aggregations$metric[agg_ind] %in%
             aggregations$group_by[agg_ind][[1]]) ) {
        aggregations$group_by[agg_ind][[1]] <-
          c(aggregations$group_by[agg_ind][[1]], aggregations$metric[agg_ind])
      }
    }
  }

  # Convert columns used in aggregations to appropriate format
  #   - binary columns are recoded to 0/1
  #   - numeric items are force converted to numeric
  #   - multi-select items are converted to a series of binary columns, one for
  # each unique level/response code; multi-select used for grouping are left as-is.
  #   - multiple choice items are left as-is

  #### TODO: How do we want to handle multi-select items when used for grouping?
  group_vars <- unique( unlist(aggregations$group_by) )
  group_vars <- group_vars[group_vars != "geo_id"]

  metric_cols <- unique(aggregations$metric)
  
  cols_check_available <- unique(c(group_vars, metric_cols))
  available <- cols_check_available %in% names(df)
  cols_not_available <- cols_check_available[ !available ]
  for (col_var in cols_not_available) {
    # Remove from aggregations
    aggregations <- aggregations[aggregations$metric != col_var &
                                   !mapply(aggregations$group_by,
                                           FUN=function(x) {col_var %in% x}), ]
    msg_plain(paste0(
        col_var, " is not defined. Removing all aggregations that use it. ",
        nrow(aggregations), " remaining")
    )
  }

  cols_available <- cols_check_available[ available ]
  for (col_var in cols_available) {
    if ( col_var %in% group_vars & !(col_var %in% metric_cols) & !startsWith(col_var, "b_") ) {
      next
    }

    if (startsWith(col_var, "b_")) { # Binary
      output <- code_binary(df, aggregations, col_var)
    } else if (startsWith(col_var, "n_")) { # Numeric free response
      output <- code_numeric_freeresponse(df, aggregations, col_var)
    } else if (startsWith(col_var, "ms_")) { # Multi-select
      output <- code_multiselect(df, aggregations, col_var)
    } else {
      # Multiple choice and variables that are formatted differently
      output <- list(df, aggregations)
    }
    df <- output[[1]]
    aggregations <- output[[2]]
  }

  return(list(df, aggregations))
}

#' Perform calculations across all groupby levels for all aggregations.
#'
#' @param df a data frame of survey responses
#' @param crosswalk_data An aggregation, such as zip => county or zip => state,
#'   as a data frame with a "zip5" column to join against.
#' @param aggregations Data frame with columns `name`, `var_weight`, `metric`,
#'   `group_by`, `compute_fn`, `post_fn`. Each row represents one aggregate
#'   to report. `name` is the aggregate's base column name; `var_weight` is the
#'   column to use for its weights; `metric` is the column of `df` containing the
#'   response value. `group_by` is a list of variables used to perform the
#'   aggregations over. `compute_fn` is the function that computes
#'   the aggregate response given many rows of data. `post_fn` is applied to the
#'   aggregate data after megacounty aggregation, and can perform any final
#'   calculations necessary.
#' @param geo_level a string of the current geo level, such as county or state,
#'   being used
#' @param params a named list with entries "s_weight", "s_mix_coef",
#'   "num_filter"
#'
#' @importFrom dplyr inner_join bind_rows
#' @importFrom parallel mclapply
#' @importFrom stats complete.cases
#'
#' @export
summarize_aggs <- function(df, crosswalk_data, aggregations, geo_level, params) {
  if (nrow(df) == 0) {
    return( list() )
  }
  
  ## We do batches of just one set of groupby vars at a time, since we have
  ## to select rows based on this.
  assert( length(unique(aggregations$group_by)) == 1 )

  if ( length(unique(aggregations$name)) < nrow(aggregations) ) {
    stop("all aggregations using the same set of grouping variables must have unique names")
  }

  ## dplyr complains about joining a data.table, saying it is likely to be
  ## inefficient; profiling shows the cost to be negligible, so shut it up
  df <- suppressWarnings(inner_join(df, crosswalk_data, by = "zip5"))

  group_vars <- aggregations$group_by[[1]]

  if ( !all(group_vars %in% names(df)) ) {
    msg_plain(
      sprintf(
        "not all of grouping columns %s available in data; skipping aggregation",
        paste(group_vars, collapse=", ")
      ))
    return( list( ))
  }
  
  ## Find all unique groups and associated frequencies, saved in column `Freq`.
  unique_groups_counts <- as.data.frame(
    table(df[, group_vars, with=FALSE], exclude=NULL, dnn=group_vars), 
    stringsAsFactors=FALSE
  )
  
  # Drop groups with less than threshold sample size.
  unique_groups_counts <- filter(unique_groups_counts, Freq >= params$num_filter)
  if ( nrow(unique_groups_counts) == 0 ) {
    return(list())
  }
 
  ## Convert col type in unique_groups to match that in data.
  # Filter on data.table in `calculate_group` requires that columns and filter
  # values are of the same type.
  for (col_var in group_vars) {
    if ( class(df[[col_var]]) != class(unique_groups_counts[[col_var]]) ) {
      class(unique_groups_counts[[col_var]]) <- class(df[[col_var]])
    }
  }
  
  ## Set an index on the groupby var columns so that the groupby step can be
  ## faster; data.table stores the sort order of the column and
  ## uses a binary search to find matching values, rather than a linear scan.
  setindexv(df, group_vars)

  calculate_group <- function(ii) {
    target_group <- unique_groups_counts[ii, group_vars, drop=FALSE]
    # Use data.table's index to make this filter efficient
    out <- summarize_aggregations_group(
      df[as.list(target_group), on=group_vars],
      aggregations,
      target_group,
      geo_level,
      params)

    return(out)
  }

  if (params$parallel) {
    dfs <- mclapply(seq_along(unique_groups_counts[[1]]), calculate_group)
  } else {
    dfs <- lapply(seq_along(unique_groups_counts[[1]]), calculate_group)
  }

  ## Now we have a list, with one entry per groupby level, each containing a
  ## list of one data frame per aggregation. Rearrange it.
  dfs_out <- list()
  for (aggregation in aggregations$id) {
    dfs_out[[aggregation]] <- bind_rows( lapply(dfs, function(groupby_levels) {
      groupby_levels[[aggregation]]
    }))
  }

  ## Do post-processing.
  for (row in seq_len(nrow(aggregations))) {
    aggregation <- aggregations$id[row]
    group_vars <- aggregations$group_by[[row]]
    post_fn <- aggregations$post_fn[[row]]
    
    # Keep only aggregations where the main value, `val`, is present.
    dfs_out[[aggregation]] <- dfs_out[[aggregation]][
      rowSums(is.na(dfs_out[[aggregation]][, c("val", "sample_size")])) == 0,
    ]

    dfs_out[[aggregation]] <- apply_privacy_censoring(dfs_out[[aggregation]], params)

    ## Apply the post-function
    dfs_out[[aggregation]] <- post_fn(dfs_out[[aggregation]])
  }

  return(dfs_out)
}

#' Produce estimates for all indicators in a specific target group.
#'
#' @param group_df Data frame containing all data needed to estimate one group.
#'   Estimates for `target_group` will be based on all of this data.
#' @param aggregations Aggregations to report. See `produce_aggregates()`.
#' @param target_group A `data.table` with one row specifying the grouping
#'   variable values used to select this group.
#' @param geo_level a string of the current geo level, such as county or state,
#'   being used
#' @param params Named list of configuration options.
#'
#' @importFrom tibble add_column as_tibble
#' @importFrom dplyr %>%
#'
#' @export
summarize_aggregations_group <- function(group_df, aggregations, target_group, geo_level, params) {
  ## Prepare outputs.
  dfs_out <- list()
  for (row in seq_along(aggregations$id)) {
    aggregation <- aggregations$id[row]

    dfs_out[[aggregation]] <- target_group %>%
      as.list %>%
      as_tibble %>%
      add_column(val=NA_real_,
                 se=NA_real_,
                 sample_size=NA_real_,
                 effective_sample_size=NA_real_,
                 represented=NA_real_)
  }

  for (row in seq_along(aggregations$id)) {
    aggregation <- aggregations$id[row]
    metric <- aggregations$metric[row]
    var_weight <- aggregations$var_weight[row]
    compute_fn <- aggregations$compute_fn[[row]]

    agg_df <- group_df[!is.na(group_df[[var_weight]]) & !is.na(group_df[[metric]]), ]

    if (nrow(agg_df) > 0) {
      s_mix_coef <- params$s_mix_coef
      mixing <- mix_weights(agg_df[[var_weight]] * agg_df$weight_in_location,
                            s_mix_coef, params$s_weight)

      sample_size <- sum(agg_df$weight_in_location)
      total_represented <- sum(agg_df[[var_weight]] * agg_df$weight_in_location)

      ## TODO: See issue #764
      new_row <- compute_fn(
        response = agg_df[[metric]],
        weight = if (aggregations$skip_mixing[row]) { mixing$normalized_preweights } else { mixing$weights },
        sample_size = sample_size,
        total_represented = total_represented)

      dfs_out[[aggregation]]$val <- new_row$val
      dfs_out[[aggregation]]$se <- new_row$se
      dfs_out[[aggregation]]$sample_size <- sample_size
      dfs_out[[aggregation]]$effective_sample_size <- new_row$effective_sample_size
      dfs_out[[aggregation]]$represented <- new_row$represented
    }
  }

  return(dfs_out)
}
