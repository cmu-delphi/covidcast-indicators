library(delphiFacebook)
library(testthat)

#teardown({
  files <- c(
    dir("individual", full.names = TRUE, all.files = TRUE, no.. = TRUE),
    dir("weights_out", full.names = TRUE, all.files = TRUE, no.. = TRUE),
    dir("receiving", full.names = TRUE, all.files = TRUE, no.. = TRUE),
    dir("archive", full.names = TRUE, all.files = TRUE, no.. = TRUE)
  )
  sapply(files, file.remove)

  file.remove("individual")
  file.remove("weights_out")
  file.remove("receiving")
  file.remove("archive")
#})
