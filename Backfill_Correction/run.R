#!/usr/bin/env Rscript

## Run backfill corrections pipeline.
##
## Usage:
##
## Rscript run.R [options]

suppressPackageStartupMessages({
  library(delphiBackfillCorrection)
  library(argparser)
})


parser <- arg_parser(description='Run backfill corrections pipeline')
# Default to FALSE if not specified.
parser <- add_argument(parser, arg="--train_models", flag=TRUE)
parser <- add_argument(parser, arg="--make_predictions", flag=TRUE)
args = parse_args(parser)

params <- read_params("params.json")
params$train_models <- args.train_models
params$make_predictions <- args.make_predictions

delphiBackfillCorrection::main(params)
message("backfill correction completed successfully")
