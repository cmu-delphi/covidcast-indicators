## Functions for performing the aggregations in an efficient way.

#' Produce aggregates for all desired aggregations.
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
#' @importFrom dplyr full_join %>%
#' @importFrom purrr reduce
#'
#' @export
aggregate_aggs <- function(df, aggregations, cw_list, params) {
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
  
  agg_groups <- unique(aggregations[c("group_by", "geo_level")])
  
  # For each unique combination of groupby_vars and geo level, run aggregation process once
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
    
    # To display other response columns ("val", "sample_size", "se", 
    # "effective_sample_size"), add here.
    keep_vars <- c("val", "sample_size")
    
    for (agg_metric in names(dfs_out)) {
      map_old_new_names <- keep_vars
      names(map_old_new_names) <- paste(keep_vars, agg_metric, sep="_")
      
      dfs_out[[agg_metric]] <- rename(
        dfs_out[[agg_metric]][, c(agg_group, keep_vars)], map_old_new_names)
    }
    
    df_out <- dfs_out %>% reduce(full_join, by=agg_group, suff=c("", ""))
    write_contingency_tables(df_out, params, geo_level, agg_group)
  }
}

#' Post-process aggregations and data to make more generic
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
  
  # Convert most columns being used in aggregations to the appropriate format.
  # Multiple choice and multi-select used for grouping are left as-is.
  #### TODO: should multi-select response be converted into set of binary cols if being used for grouping? Yeah, probably
  agg_groups <- unique(aggregations$group_by)
  group_cols_to_convert <- unique(do.call(c, agg_groups))
  group_cols_to_convert <- group_cols_to_convert[startsWith(group_cols_to_convert, "b_")]
  
  metric_cols_to_convert <- unique(aggregations$metric)
  
  #### TODO: likely good as a separate function. Can find way to remove or condense all the `output[[x]]`?
  for (col_var in c(group_cols_to_convert, metric_cols_to_convert)) {
    if ( is.null(df[[col_var]]) ) {
      # Column not defined. Remove all aggregations that use it.
      #### TODO: subset aggregations to remove rows using col_var in group_by vars as well.
      aggregations <- aggregations[aggregations$metric != col_var, ]
      next
    }
    
    if (startsWith(col_var, "b_")) { # Binary
      output <- code_binary(df, aggregations, col_var)
      df <- output[[1]]
      aggregations <- output[[2]]
      
    } else if (startsWith(col_var, "ms_")) { # Multiselect
      output <- code_multiselect(df, aggregations, col_var)
      df <- output[[1]]
      aggregations <- output[[2]]
      
    } else if (startsWith(col_var, "n_")) { # Numeric free response
      output <- code_numeric_freeresponse(df, aggregations, col_var)
      df <- output[[1]]
      aggregations <- output[[2]]
    }
  }
  
  return(list(df, aggregations))
}

#' Performs calculations across all groupby levels for all aggregations.
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
  ## dplyr complains about joining a data.table, saying it is likely to be
  ## inefficient; profiling shows the cost to be negligible, so shut it up
  # Geo group column is always named "geo_id"
  df <- suppressWarnings(inner_join(df, crosswalk_data, by = "zip5"))
  
  ## We do batches of just one set of groupby vars at a time, since we have
  ## to select rows based on this.
  assert( length(unique(aggregations$group_by)) == 1 )
  
  groupby_vars <- aggregations$group_by[[1]]
  
  if (all(groupby_vars %in% names(df))) {
    unique_group_combos <- unique(df[, groupby_vars, with=FALSE])
    unique_group_combos <- unique_group_combos[complete.cases(unique_group_combos)]
  } else {
    msg_plain(
      sprintf(
        "not all of groupby columns %s available in data; skipping this aggregation", 
        paste(groupby_vars, collapse=", ")
      ))
  }
  
  if (!exists("unique_group_combos") || nrow(unique_group_combos) == 0) {
    return(list())
  }
  
  
  ## Set an index on the groupby var columns so that the groupby step can be
  ## dramatically faster; data.table stores the sort order of the column and
  ## uses a binary search to find matching values, rather than a linear scan.
  setindexv(df, groupby_vars)

  calculate_group <- function(ii) {
    target_group <- unique_group_combos[ii]
    # Use data.table's index to make this filter efficient
    out <- summarize_aggregations_group(
      df[as.list(target_group), on=names(target_group)],
      aggregations,
      target_group,
      geo_level,
      params)
    
    return(out)
  }
  
  if (params$parallel) {
    dfs <- mclapply(seq_along(unique_group_combos[[1]]), calculate_group)
  } else {
    dfs <- lapply(seq_along(unique_group_combos[[1]]), calculate_group)
  }
  
  ## Now we have a list, with one entry per groupby level, each containing a
  ## list of one data frame per aggregation. Rearrange it.
  dfs_out <- list()
  for (aggregation in aggregations$name) {
    dfs_out[[aggregation]] <- bind_rows( lapply(dfs, function(groupby_levels) { 
      groupby_levels[[aggregation]] 
    }))
  }
  
  ## Do post-processing.
  for (row in seq_len(nrow(aggregations))) {
    aggregation <- aggregations$name[row]
    groupby_vars <- aggregations$group_by[[row]]
    post_fn <- aggregations$post_fn[[row]]
    
    dfs_out[[aggregation]] <- dfs_out[[aggregation]][
      rowSums(is.na(dfs_out[[aggregation]][, c("val", "sample_size", groupby_vars)])) == 0,
    ]
    
    if (geo_level == "county") {
      df_megacounties <- megacounty(dfs_out[[aggregation]], params$num_filter, groupby_vars)
      dfs_out[[aggregation]] <- bind_rows(dfs_out[[aggregation]], df_megacounties)
    }
    
    dfs_out[[aggregation]] <- apply_privacy_censoring(dfs_out[[aggregation]], params)
    
    ## *After* gluing together megacounties, apply the post-function
    dfs_out[[aggregation]] <- post_fn(dfs_out[[aggregation]])
  }
  
  return(dfs_out)
}

#' Produce estimates for all indicators in a specific target group.
#' 
#' @param group_df Data frame containing all data needed to estimate one group.
#'   Estimates for `target_group` will be based on all of this data.
#' @param aggregations Aggregations to report. See `aggregate_aggs()`.
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
  for (row in seq_along(aggregations$name)) {
    aggregation <- aggregations$name[row]
    
    dfs_out[[aggregation]] <- target_group %>%
      as.list %>%
      as_tibble %>%
      add_column(val=NA_real_) %>%
      add_column(se=NA_real_) %>%
      add_column(sample_size=NA_real_) %>%
      add_column(effective_sample_size=NA_real_)
  }
  
  for (row in seq_along(aggregations$name)) {
    aggregation <- aggregations$name[row]
    metric <- aggregations$metric[row]
    var_weight <- aggregations$var_weight[row]
    compute_fn <- aggregations$compute_fn[[row]]
    
    agg_df <- group_df[!is.na(group_df[[var_weight]]) & !is.na(group_df[[metric]]), ]
    
    if (nrow(agg_df) > 0)
    {
      s_mix_coef <- params$s_mix_coef
      mixing <- mix_weights(agg_df[[var_weight]] * agg_df$weight_in_location,
                            s_mix_coef, params$s_weight)
      
      sample_size <- sum(agg_df$weight_in_location)
      total_represented <- sum(agg_df[[var_weight]])
      
      ## TODO Fix this. Old pipeline for community responses did not apply
      ## mixing. To reproduce it, we ignore the mixed weights. Once a better
      ## mixing/weighting scheme is chosen, all signals should use it.
      new_row <- compute_fn(
        response = agg_df[[metric]],
        weight = if (aggregations$skip_mixing[row]) { mixing$normalized_preweights } else { mixing$weights },
        sample_size = sample_size,
        total_represented = total_represented)
      
      dfs_out[[aggregation]]$val <- new_row$val
      dfs_out[[aggregation]]$se <- new_row$se
      dfs_out[[aggregation]]$sample_size <- sample_size
      dfs_out[[aggregation]]$effective_sample_size <- new_row$effective_sample_size
    }
  }
  
  return(dfs_out)
}
