library(readr)
library(dplyr)

old_changelog <- read_csv("cmu_changelog_final.csv", col_types = cols(
  .default = col_character(),
  new_version = col_double(),
  old_version = col_double()
))

new_changelog <- read_csv("cmu_changelog_20220603.csv", col_types = cols(
  .default = col_character(),
  new_version = col_double(),
  old_version = col_double()
))
new_missing <- new_changelog %>% filter(is.na(notes))
browser()
new_changelog <- anti_join(new_changelog, new_missing)

result <- list()

for (i in seq_len(nrow(new_missing))) {
  curr = new_missing[i,]
  var_name <- curr %>% pull(variable_name)
  new_v <- curr %>% pull(new_version)
  old_v <- curr %>% pull(old_version)
  new_base <- curr %>% pull(new_originating_question)
  old_base <- curr %>% pull(old_originating_question)
  
  tmp <- old_changelog %>%
    filter(
      old_version == old_v & new_version == new_v &
        ( variable_name == var_name | variable_name == new_base | variable_name == old_base)
    )

  browser()
  # new_missing[i, "notes"] <- tmp$notes[1]
  result[[i]] <- new_missing[i,]
}
new_changelog <- rbind(
  new_changelog,
  bind_rows(result)
)

write_csv(new_changelog, "cmu_changelog_20220603.csv")
# write_csv(rbind(new_changelog, bind_rows(result)), "umd_combined_test_changelog_with_rationales_1157.csv")
