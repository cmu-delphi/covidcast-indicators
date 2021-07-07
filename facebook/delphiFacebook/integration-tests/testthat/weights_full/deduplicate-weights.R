## The pipeline should be robust against duplicated weights, so the synthetic
## data contains multiple weights for some tokens. The pipeline is meant to use
## the first weight.
##
## However, the *old* pipeline implements this de-duplication outside the R code
## in a preprocessing step. So we need deduplicated versions of the weight files
## to give to the old pipeline to generate the gold files.
##
## This matters primarily for the individual output, not aggregated, since
## duplicate weights cause duplicate rows in the individual output (one row per
## weight for that token).

weights <- read.csv("2020-05-13_covid19_dap_adults_finish_full_survey_weights.csv")

dup_rows <- duplicated(weights$cid)

weights <- weights[!dup_rows,]

write.csv(weights,
          file = "2020-05-13_deduplicated_covid19_dap_adults_finish_full_survey_weights.csv",
          row.names = FALSE)
