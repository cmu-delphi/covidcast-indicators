#!/usr/bin/env Rscript

## Run backfill corrections on a single signal + geo type combination from local data.
##
## Usage:
##
## Rscript correct_local_signal.R [options]

suppressPackageStartupMessages({
  library(delphiBackfillCorrection)
  library(argparser)
})


parser <- arg_parser(description='Run backfill corrections on a single signal + geo type combination from local data')
parser <- add_argument(parser, arg="--input_dir", type="character", help = "Path to the input file")
parser <- add_argument(parser, arg="--export_dir", type="character", default = "../export_dir", help = "Pth to the export directory")
parser <- add_argument(parser, arg="--test_start_date", type="character", help = "Should be in the format as 'YYYY-MM-DD'")
parser <- add_argument(parser, arg="--test_end_date", type="character", help = "Should be in the format as 'YYYY-MM-DD'")
parser <- add_argument(parser, arg="--testing_window", type="integer", default = 1, help = "The number of issue dates for testing per trained model")
parser <- add_argument(parser, arg="--value_type", type="character", default = "fraction", help = "Can be 'count' or 'fraction'")
parser <- add_argument(parser, arg="--num_col", type="character", default = "num", help = "The column name for the numerator")
parser <- add_argument(parser, arg="--denum_col", type="character", default = "den", help = "The column name for the denominator")
parser <- add_argument(parser, arg="--lambda", type="character", default = 0.1, help = "The parameter lambda for the lasso regression")
parser <- add_argument(parser, arg="--training_days", type="integer", default = 270, help = "The number of issue dates used for model training")
parser <- add_argument(parser, arg="--ref_lag", type="integer", default = 60, help = "The lag that is set to be the reference")
args = parse_args(parser)

main_local(args.input_dir, args.export_dir,
     args.test_start_date, args.test_end_date,
     args.num_col, args.denom_col,
     args.value_type,
     args.training_days, args.testing_window,
     args.lambda, args.ref_lag)
