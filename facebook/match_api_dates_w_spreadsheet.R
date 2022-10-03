library(readr)
library(dplyr)

input_dates <- read_csv("Downloads/api_signal_dates.csv")
spreadsheet_raw <- read_csv("Downloads/ctis_api_codebook - Sheet 1.csv")
spreadsheet <- spreadsheet_raw %>%  mutate(in_cmu = !is.na(cmu_description)) %>% 
  filter(in_cmu) %>% 
  select(cmu_indicator, umd_indicator, in_cmu, cmu_start_date, cmu_end_date)

# Need to add entries to docs.
need_to_add_to_doc <- c()

# Need to define and generate for full history, then join onto existing tables.
need_to_add_to_tables <- c()


## Test if all of our signals are accounted for
dates_added <- full_join(spreadsheet, input_dates, by = c("cmu_indicator" = "signal"))
# View(dates_added)
unmatched_items <- dates_added %>%
  filter(is.na(in_cmu) | (in_cmu & is.na(start_date))) %>% 
  filter(!(cmu_indicator %in% need_to_add_to_doc)) %>% 
  filter(!(cmu_indicator %in% need_to_add_to_tables))
View(unmatched_items)

## Now actually do the join.
dates_added <- full_join(spreadsheet_raw, input_dates, by = c("cmu_indicator" = "signal"))
if (nrow(dates_added) != nrow(spreadsheet_raw)) {
  stop("must be mismatched rows somewhere")
}

# Save result
write_csv(dates_added, "Downloads/ctis_api_spreadsheet_dates_added.csv")

# Sanity check
dates_added %>% filter(cmu_start_date > start_date) %>% nrow()
dates_added %>% filter(cmu_end_date != end_date) %>% nrow()
dates_added %>% filter(cmu_start_date != start_date | cmu_end_date != end_date) %>% View()


# For reference
wave_date_map <- c(
  "1" = as.Date("2020-04-06"),
  "2" = as.Date("2020-04-15"),
  "3" = as.Date("2020-05-21"),
  "4" = as.Date("2020-09-08"),
  "5" = as.Date("2020-11-24"),
  "6" = as.Date("2020-12-19"),
  "7" = as.Date("2021-01-12"),
  "8" = as.Date("2021-02-08"),
  "9" = as.Date("2021-03-02"),
  "10" = as.Date("2021-03-02"),
  "11" = as.Date("2021-05-20"),
  "12" = as.Date("2021-12-19"),
  "13" = as.Date("2022-01-30"),
  "14" = as.Date("2022-06-25")
)