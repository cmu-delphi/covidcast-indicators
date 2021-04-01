#' Make tables specifying aggregations to output
#'
#' Each row represents one aggregate to report. `name` is the aggregate's base
#' column name.`metric` is the column of `df` containing the response value.
#' `group_by` is a list of variables used to perform the aggregations over.
#' `compute_fn` is the function that computes the aggregate response given many
#' rows of data. `post_fn` is applied to the aggregate data and can perform any
#' final calculations necessary.
#'
#' Listing no groupby vars implicitly computes aggregations at the national
#' level only. Any multiple choice and multi-select questions used in the
#' aggregations should be recoded to be descriptive in
#' `contingency_variables::reformat_responses`.
#'
#' Compute functions must be one of the `compute_*` set (or another function
#' with similar format can be created). Post-processing functions should be one
#' of the `jeffreys_*` set or post_convert_count_to_pct or the identity `I`,
#' which does not modify the data.
#'
#' @return named list
#' 
#' @importFrom tibble tribble
get_aggs <- function() {
  weekly_aggs <- tribble(
    ~name, ~metric, ~group_by, ~compute_fn, ~post_fn,
    #### Cut 1: side effects if hesitant about getting vaccine and generally
    # National
    "pct_concerned_sideeffects", "b_concerned_sideeffects", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_hesitant_sideeffects", "b_hesitant_sideeffects", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    
    # State
    "pct_concerned_sideeffects", "b_concerned_sideeffects", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_sideeffects", "b_hesitant_sideeffects", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    
    # State marginal
    "pct_concerned_sideeffects", "b_concerned_sideeffects", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_concerned_sideeffects", "b_concerned_sideeffects", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_concerned_sideeffects", "b_concerned_sideeffects", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_sideeffects", "b_hesitant_sideeffects", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_sideeffects", "b_hesitant_sideeffects", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_sideeffects", "b_hesitant_sideeffects", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    
    
    
    #### Cut 2: trust various institutions if hesitant about getting vaccine
    # National
    "pct_hesitant_trust_fam", "b_hesitant_trust_fam", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_healthcare", "b_hesitant_trust_healthcare", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_who", "b_hesitant_trust_who", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_govt", "b_hesitant_trust_govt", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_politicians", "b_hesitant_trust_politicians", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    
    # State
    "pct_hesitant_trust_fam", "b_hesitant_trust_fam", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_healthcare", "b_hesitant_trust_healthcare", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_who", "b_hesitant_trust_who", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_govt", "b_hesitant_trust_govt", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_politicians", "b_hesitant_trust_politicians", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    
    # State marginal
    "pct_hesitant_trust_fam", "b_hesitant_trust_fam", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_fam", "b_hesitant_trust_fam", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_fam", "b_hesitant_trust_fam", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_healthcare", "b_hesitant_trust_healthcare", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_healthcare", "b_hesitant_trust_healthcare", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_healthcare", "b_hesitant_trust_healthcare", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_who", "b_hesitant_trust_who", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_who", "b_hesitant_trust_who", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_who", "b_hesitant_trust_who", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_govt", "b_hesitant_trust_govt", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_govt", "b_hesitant_trust_govt", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_govt", "b_hesitant_trust_govt", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_politicians", "b_hesitant_trust_politicians", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_politicians", "b_hesitant_trust_politicians", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_politicians", "b_hesitant_trust_politicians", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    
    
    
    #### Cut 3: trust various institutions
    # National
    "pct_trust_fam", "b_vaccine_likely_friends", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_trust_healthcare", "b_vaccine_likely_local_health", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_trust_who", "b_vaccine_likely_who", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_trust_govt", "b_vaccine_likely_govt_health", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_trust_politicians", "b_vaccine_likely_politicians", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    
    # State
    "pct_trust_fam", "b_vaccine_likely_friends", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_trust_healthcare", "b_vaccine_likely_local_health", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_trust_who", "b_vaccine_likely_who", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_trust_govt", "b_vaccine_likely_govt_health", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_trust_politicians", "b_vaccine_likely_politicians", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    
    # State marginal
    "pct_trust_fam", "b_vaccine_likely_friends", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_trust_fam", "b_vaccine_likely_friends", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_trust_fam", "b_vaccine_likely_friends", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_trust_healthcare", "b_vaccine_likely_local_health", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_trust_healthcare", "b_vaccine_likely_local_health", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_trust_healthcare", "b_vaccine_likely_local_health", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_trust_who", "b_vaccine_likely_who", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_trust_who", "b_vaccine_likely_who", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_trust_who", "b_vaccine_likely_who", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_trust_govt", "b_vaccine_likely_govt_health", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_trust_govt", "b_vaccine_likely_govt_health", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_trust_govt", "b_vaccine_likely_govt_health", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_trust_politicians", "b_vaccine_likely_politicians", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_trust_politicians", "b_vaccine_likely_politicians", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_trust_politicians", "b_vaccine_likely_politicians", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    
    
    
    #### Cuts 4, 5, 6: vaccinated and accepting if senior, in healthcare, or generally
    # National
    "pct_vaccinated", "b_had_cov_vaccine", c("b_work_in_healthcare", "mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("b_65_or_older", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_work_in_healthcare", "mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_65_or_older", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    
    # State
    "pct_vaccinated", "b_had_cov_vaccine", c("b_work_in_healthcare", "mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("b_65_or_older", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_work_in_healthcare", "mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_65_or_older", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    
    # State marginal
    "pct_vaccinated", "b_had_cov_vaccine", c("b_work_in_healthcare", "mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("b_work_in_healthcare","mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("b_work_in_healthcare", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("b_65_or_older", "mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("b_65_or_older", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_work_in_healthcare", "mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_work_in_healthcare","mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_work_in_healthcare", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_65_or_older", "mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_65_or_older", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
  )
  
  monthly_aggs <- tribble(
    ~name, ~metric, ~group_by, ~compute_fn, ~post_fn,
    #### Cut 1: side effects if hesitant about getting vaccine and generally
    # National
    "pct_concerned_sideeffects", "b_concerned_sideeffects", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_hesitant_sideeffects", "b_hesitant_sideeffects", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    
    # State
    "pct_concerned_sideeffects", "b_concerned_sideeffects", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_sideeffects", "b_hesitant_sideeffects", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    
    # State marginal
    "pct_concerned_sideeffects", "b_concerned_sideeffects", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_concerned_sideeffects", "b_concerned_sideeffects", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_concerned_sideeffects", "b_concerned_sideeffects", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_sideeffects", "b_hesitant_sideeffects", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_sideeffects", "b_hesitant_sideeffects", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_sideeffects", "b_hesitant_sideeffects", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    
    
    
    #### Cut 2: trust various institutions if hesitant about getting vaccine
    # National
    "pct_hesitant_trust_fam", "b_hesitant_trust_fam", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_healthcare", "b_hesitant_trust_healthcare", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_who", "b_hesitant_trust_who", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_govt", "b_hesitant_trust_govt", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_politicians", "b_hesitant_trust_politicians", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    
    # State
    "pct_hesitant_trust_fam", "b_hesitant_trust_fam", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_healthcare", "b_hesitant_trust_healthcare", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_who", "b_hesitant_trust_who", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_govt", "b_hesitant_trust_govt", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_politicians", "b_hesitant_trust_politicians", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    
    # State marginal
    "pct_hesitant_trust_fam", "b_hesitant_trust_fam", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_fam", "b_hesitant_trust_fam", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_fam", "b_hesitant_trust_fam", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_healthcare", "b_hesitant_trust_healthcare", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_healthcare", "b_hesitant_trust_healthcare", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_healthcare", "b_hesitant_trust_healthcare", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_who", "b_hesitant_trust_who", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_who", "b_hesitant_trust_who", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_who", "b_hesitant_trust_who", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_govt", "b_hesitant_trust_govt", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_govt", "b_hesitant_trust_govt", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_govt", "b_hesitant_trust_govt", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_politicians", "b_hesitant_trust_politicians", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_politicians", "b_hesitant_trust_politicians", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_hesitant_trust_politicians", "b_hesitant_trust_politicians", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    
    
    
    #### Cut 3: trust various institutions
    # National
    "pct_trust_fam", "b_vaccine_likely_friends", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_trust_healthcare", "b_vaccine_likely_local_health", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_trust_who", "b_vaccine_likely_who", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_trust_govt", "b_vaccine_likely_govt_health", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_trust_politicians", "b_vaccine_likely_politicians", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    
    # State
    "pct_trust_fam", "b_vaccine_likely_friends", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_trust_healthcare", "b_vaccine_likely_local_health", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_trust_who", "b_vaccine_likely_who", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_trust_govt", "b_vaccine_likely_govt_health", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_trust_politicians", "b_vaccine_likely_politicians", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    
    # State marginal
    "pct_trust_fam", "b_vaccine_likely_friends", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_trust_fam", "b_vaccine_likely_friends", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_trust_fam", "b_vaccine_likely_friends", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_trust_healthcare", "b_vaccine_likely_local_health", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_trust_healthcare", "b_vaccine_likely_local_health", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_trust_healthcare", "b_vaccine_likely_local_health", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_trust_who", "b_vaccine_likely_who", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_trust_who", "b_vaccine_likely_who", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_trust_who", "b_vaccine_likely_who", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_trust_govt", "b_vaccine_likely_govt_health", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_trust_govt", "b_vaccine_likely_govt_health", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_trust_govt", "b_vaccine_likely_govt_health", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_trust_politicians", "b_vaccine_likely_politicians", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_trust_politicians", "b_vaccine_likely_politicians", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_trust_politicians", "b_vaccine_likely_politicians", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    
    
    
    #### Cuts 4, 5, 6: vaccinated and accepting if senior, in healthcare, or generally
    # National
    "pct_vaccinated", "b_had_cov_vaccine", c("b_work_in_healthcare", "mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("b_65_or_older", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_work_in_healthcare", "mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_65_or_older", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "nation"), compute_binary, jeffreys_binary,
    
    # State
    "pct_vaccinated", "b_had_cov_vaccine", c("b_work_in_healthcare", "mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("b_65_or_older", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_work_in_healthcare", "mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_65_or_older", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("mc_age", "mc_gender", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    
    # State marginal
    "pct_vaccinated", "b_had_cov_vaccine", c("b_work_in_healthcare", "mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("b_work_in_healthcare","mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("b_work_in_healthcare", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("b_65_or_older", "mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("b_65_or_older", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_vaccinated", "b_had_cov_vaccine", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_work_in_healthcare", "mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_work_in_healthcare","mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_work_in_healthcare", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_65_or_older", "mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("b_65_or_older", "mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("mc_age", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("mc_gender", "state"), compute_binary, jeffreys_binary,
    "pct_accepting", "b_accept_cov_vaccine", c("mc_race", "b_hispanic", "state"), compute_binary, jeffreys_binary,
    
  )
  
  return(list("week"=weekly_aggs, "month"=monthly_aggs))
}
