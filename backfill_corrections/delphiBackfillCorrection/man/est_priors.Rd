% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/beta_prior_estimation.R
\name{est_priors}
\alias{est_priors}
\title{Main function for the beta prior approach
Estimate the priors for the beta distribution based on data for 
a certain day of a week}
\usage{
est_priors(
  train_data,
  prior_test_data,
  geo,
  value_type,
  dw,
  taus,
  covariates,
  response,
  lp_solver,
  lambda,
  indicator,
  signal,
  geo_level,
  signal_suffix,
  training_end_date,
  training_start_date,
  model_save_dir,
  start = c(0, log(10)),
  base_pseudo_denom = 1000,
  base_pseudo_num = 10,
  train_models = TRUE,
  make_predictions = TRUE
)
}
\arguments{
\item{train_data}{Data Frame containing training data}

\item{prior_test_data}{Data Frame for testing}

\item{geo}{string specifying the name of the geo region (e.g. FIPS
code for counties)}

\item{value_type}{string describing signal type. Either "count" or "fraction".}

\item{dw}{column name to indicate which day of a week it is}

\item{taus}{numeric vector of quantiles to be predicted. Values
must be between 0 and 1.}

\item{covariates}{character vector of column names serving as the covariates for the model}

\item{response}{the column name of the response variable}

\item{lp_solver}{string specifying the lp solver to use in
Quantgen fitting. Either "glpk" or "gurobi". For faster
optimization, use Gurobi (requires separate installation
of the `gurobi` package).}

\item{lambda}{the level of lasso penalty}

\item{indicator}{string specifying the name of the indicator as used in
`parquet` input data filenames. One indicator can be associated
with multiple signals.}

\item{signal}{string specifying the name of the signal as used in
`parquet` input data filenames. One indicator can be associated
with multiple signals.}

\item{geo_level}{string describing geo coverage of input data. Either "state"
or "county".}

\item{signal_suffix}{string specifying value column name
ending to be appended to standard value column names from
`params$num_col` and `params$denom_col`. Used for non-standard
value column names and when processing multiple signals from a
single input dataframe, as with `quidel`'s age buckets.}

\item{training_end_date}{the latest target date and issue date included in training data}

\item{training_start_date}{the earliest target date included in training data}

\item{model_save_dir}{directory containing trained models}

\item{start}{the initialization of the the points in nlm}

\item{base_pseudo_denom}{the pseudo counts added to denominator if little data for training}

\item{base_pseudo_num}{the pseudo counts added to numerator if little data for training}

\item{train_models}{boolean indicating whether to train models (TRUE). If
FALSE previously trained models (stored locally) will be used instead.
Default is TRUE.}

\item{make_predictions}{boolean indicating whether to generate and save
corrections (TRUE) or not. Default is TRUE.}
}
\description{
Main function for the beta prior approach
Estimate the priors for the beta distribution based on data for 
a certain day of a week
}
