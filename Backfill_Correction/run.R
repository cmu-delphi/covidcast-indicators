library(delphiBackfillCorrection)

params <- read_params("params.json")
delphiBackfillCorrection::main(params)
message("backfill correction completed successfully")
