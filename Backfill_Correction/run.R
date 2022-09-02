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
# Both flags default to FALSE (do not train/predict) if not specified.
parser <- add_argument(parser, arg="--train_models", flag=TRUE)
parser <- add_argument(parser, arg="--make_predictions", flag=TRUE)
args = parse_args(parser)

params <- read_params(
  "params.json",
  train_models = args.train_models, make_predictions = args.make_predictions
)

delphiBackfillCorrection::main(params)
message("backfill correction completed successfully")
