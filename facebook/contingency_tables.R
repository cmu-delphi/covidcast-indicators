library(delphiFacebook)

#Rprof(interval = 0.5)
params <- read_contingency_params("params.json")
run_contingency_tables(params)
