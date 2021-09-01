library(delphiFacebook)

params <- read_contingency_params("params.json")
run_contingency_tables(params)
message("run_contingency_tables completed successfully")
