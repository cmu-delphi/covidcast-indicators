library(delphiFacebook)

#Rprof(interval = 0.005)
params <- read_params("params.json", "contingency_params.json.template")
run_contingency_tables(params)
