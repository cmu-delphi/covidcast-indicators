
files <- c(
  dir(test_path("individual"), full.names = TRUE, all.files = TRUE, no.. = TRUE),
  dir(test_path("weights_out"), full.names = TRUE, all.files = TRUE, no.. = TRUE),
  dir(test_path("receiving"), full.names = TRUE, all.files = TRUE, no.. = TRUE),
  dir(test_path("receiving_full"), full.names = TRUE, all.files = TRUE, no.. = TRUE),
  dir(test_path("individual_full"), full.names = TRUE, all.files = TRUE, no.. = TRUE),
  dir(test_path("archive"), full.names = TRUE, all.files = TRUE, no.. = TRUE),
  dir(test_path("receiving_contingency_full"), full.names = TRUE, all.files = TRUE, no.. = TRUE),
  dir(test_path("receiving_contingency_test"), full.names = TRUE, all.files = TRUE, no.. = TRUE)
)
sapply(files, file.remove)

file.remove(test_path("individual"))
file.remove(test_path("weights_out"))
file.remove(test_path("receiving"))
file.remove(test_path("archive"))
file.remove(test_path("receiving_full"))
file.remove(test_path("individual_full"))
file.remove(test_path("receiving_contingency_full"))

if ( dir.exists(test_path("receiving_contingency_test")) ) {
  file.remove(test_path("receiving_contingency_test"))
}
