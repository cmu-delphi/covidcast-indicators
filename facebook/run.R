library(delphiFacebook)

params <- read_params("params.json")
delphiFacebook::run_facebook(params)
message("run_facebook completed successfully")
