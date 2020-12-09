library(delphiFacebook)

#Rprof(interval = 0.005)
params <- read_params("params.json")
run_contingency_tables(params)
