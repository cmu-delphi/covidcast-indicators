library(delphiFacebook)

Rprof(interval = 0.005)
params <- read_params("params.json")
delphiFacebook::run_facebook(params)
