library(tibble)
library(delphiFacebook)

params <- read_params("contingency_params.json", "contingency_params.json.template")
run_contingency_tables(params)
