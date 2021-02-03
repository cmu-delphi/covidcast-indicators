library(tibble)
library(delphiFacebook)

params <- read_contingency_params("params.json")
run_contingency_tables(params)