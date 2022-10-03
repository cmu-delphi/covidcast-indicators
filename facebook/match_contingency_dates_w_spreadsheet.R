library(readr)
library(dplyr)

input_dates <- read_csv("Downloads/contingency_signal_dates.csv") %>%
  rename(cmu_indicator_start = start_date, cmu_indicator_end = end_date) %>% 
  filter(signal != "issue_date")

input_dates$signal <- gsub(".*_mean_", "", input_dates$signal)
input_dates$signal <- gsub(".*_pct_", "", input_dates$signal)
input_dates <- distinct(input_dates)

spreadsheet_raw <- read_csv("Downloads/ctis_contingency_tables_codebook - Sheet 1.csv")
spreadsheet <- spreadsheet_raw %>%  mutate(in_cmu = !is.na(cmu_definition)) %>% 
  select(indicator_name, in_cmu)


# US and Global names will disagree for these.
spreadsheet$indicator_name[spreadsheet$indicator_name == "had_covid_ever [US]\nhad_covid [GLOBAL]"] <- "had_covid_ever"
spreadsheet$indicator_name[spreadsheet$indicator_name == "restaurant_1d [US]\nrestaurant_bar_1d [GLOBAL]"] <- "restaurant_1d"
spreadsheet$indicator_name[spreadsheet$indicator_name == "appointment_not_vaccinated [US]\nappointment_have_not_vaccinated [GLOBAL]"] <- "appointment_not_vaccinated"

spreadsheet$indicator_name[spreadsheet$indicator_name == "barrier_reason_wait [US]\nbarrier_reason_waitlater [GLOBAL]"] <- "barrier_reason_wait"
spreadsheet$indicator_name[spreadsheet$indicator_name == "barrier_reason_distrust_gov [US]\nbarrier_reason_government [GLOBAL]"] <- "barrier_reason_distrust_gov"
spreadsheet$indicator_name[spreadsheet$indicator_name == "barrier_reason_religion [US]\nbarrier_reason_religious [GLOBAL]"] <- "barrier_reason_religion"
spreadsheet$indicator_name[spreadsheet$indicator_name == "barrier_reason_sideeffect [US]\nbarrier_reason_side_effects [GLOBAL]"] <- "barrier_reason_sideeffect"

spreadsheet$indicator_name[spreadsheet$indicator_name == "barrier_distrust_vaccines [US]\nbarrier_reason_covidvax [GLOBAL]"] <- "barrier_distrust_vaccines"
spreadsheet$indicator_name[spreadsheet$indicator_name == "barrier_dislike_vaccines_generally [US]\nbarrier_reason_vaxgeneral [GLOBAL]"] <- "barrier_dislike_vaccines_generally"

spreadsheet$indicator_name[spreadsheet$indicator_name == "symp_anosmia [US]\nsymp_loss_smell_taste [GLOBAL]"] <- "symp_anosmia"

spreadsheet$indicator_name[spreadsheet$indicator_name == "reason_not_tested_tried [US]\nnotest_not_able [GLOBAL]"] <- "reason_not_tested_tried"
spreadsheet$indicator_name[spreadsheet$indicator_name == "reason_not_tested_location [US]\nnotest_where [GLOBAL]"] <- "reason_not_tested_location"
spreadsheet$indicator_name[spreadsheet$indicator_name == "reason_not_tested_cost [US]\nnotest_cant_afford [GLOBAL]"] <- "reason_not_tested_cost"
spreadsheet$indicator_name[spreadsheet$indicator_name == "reason_not_tested_time [US]\nnotest_no_time [GLOBAL]"] <- "reason_not_tested_time"
spreadsheet$indicator_name[spreadsheet$indicator_name == "reason_not_tested_travel [US]\nnotest_travel [GLOBAL]"] <- "reason_not_tested_travel"
spreadsheet$indicator_name[spreadsheet$indicator_name == "reason_not_tested_stigma [US]\nnotest_worry [GLOBAL]"] <- "reason_not_tested_stigma"

spreadsheet$indicator_name[spreadsheet$indicator_name == "symp_unusual_given_fever [US]\nsymp_fever_unusual [GLOBAL]"] <- "symp_unusual_given_fever"
spreadsheet$indicator_name[spreadsheet$indicator_name == "symp_unusual_given_cough [US]\nsymp_cough_unusual [GLOBAL]"] <- "symp_unusual_given_cough"
spreadsheet$indicator_name[spreadsheet$indicator_name == "symp_unusual_given_diff_breathing [US]\nsymp_diff_breathing_unusual [GLOBAL]"] <- "symp_unusual_given_diff_breathing"
spreadsheet$indicator_name[spreadsheet$indicator_name == "symp_unusual_given_fatigue [US]\nsymp_fatigue_unusual [GLOBAL]"] <- "symp_unusual_given_fatigue"
spreadsheet$indicator_name[spreadsheet$indicator_name == "symp_unusual_given_stuffy_nose [US]\nsymp_stuffy_nose_unusual [GLOBAL]"] <- "symp_unusual_given_stuffy_nose"
spreadsheet$indicator_name[spreadsheet$indicator_name == "symp_unusual_given_aches [US]\nsymp_aches_unusual [GLOBAL]"] <- "symp_unusual_given_aches"
spreadsheet$indicator_name[spreadsheet$indicator_name == "symp_unusual_given_sore_throat [US]\nsymp_sore_throat_unusual [GLOBAL]"] <- "symp_unusual_given_sore_throat"
spreadsheet$indicator_name[spreadsheet$indicator_name == "symp_unusual_given_chest_pain [US]\nsymp_chest_pain_unusual [GLOBAL]"] <- "symp_unusual_given_chest_pain"
spreadsheet$indicator_name[spreadsheet$indicator_name == "symp_unusual_given_nausea [US]\nsymp_nausea_unusual [GLOBAL]"] <- "symp_unusual_given_nausea"
spreadsheet$indicator_name[spreadsheet$indicator_name == "symp_unusual_given_anosmia [US]\nanosmia_unusual [GLOBAL]"] <- "symp_unusual_given_anosmia"
spreadsheet$indicator_name[spreadsheet$indicator_name == "symp_unusual_given_eye_pain [US]\nsymp_eye_pain_unusual [GLOBAL]"] <- "symp_unusual_given_eye_pain"
spreadsheet$indicator_name[spreadsheet$indicator_name == "symp_unusual_given_headache [US]\nsymp_headache_unusual [GLOBAL]"] <- "symp_unusual_given_headache"
spreadsheet$indicator_name[spreadsheet$indicator_name == "symp_unusual_given_chills [US]\nsymp_chills_unusual [GLOBAL]"] <- "symp_unusual_given_chills"
spreadsheet$indicator_name[spreadsheet$indicator_name == "symp_unusual_given_sleep_changes [US]\nsymp_sleep_changes_unusual [GLOBAL]"] <- "symp_unusual_given_sleep_changes"
spreadsheet$indicator_name[spreadsheet$indicator_name == "symp_unusual_given_shortness_breath [US]\nsymp_shortness_breath_unusual [GLOBAL]"] <- "symp_unusual_given_shortness_breath"
spreadsheet$indicator_name[spreadsheet$indicator_name == "symp_unusual_given_nasal_congestion [US]\nsymp_nasal_congestion_unusual [GLOBAL]"] <- "symp_unusual_given_nasal_congestion"
spreadsheet$indicator_name[spreadsheet$indicator_name == "symp_unusual_given_runny_nose [US]\nsymp_runny_nose_unusual [GLOBAL]"] <- "symp_unusual_given_runny_nose"
spreadsheet$indicator_name[spreadsheet$indicator_name == "symp_unusual_given_diarrhea [US]\nsymp_diarrhea_unusual [GLOBAL]"] <- "symp_unusual_given_diarrhea"
spreadsheet$indicator_name[spreadsheet$indicator_name == "symp_unusual_given_other [US]\nsymp_other_unusual [GLOBAL]"] <- "symp_unusual_given_other"

spreadsheet$indicator_name[spreadsheet$indicator_name == "barrier_reason_dontneed_had_covid [US]\nbarrier_reason_dontneed_alreadyhad [GLOBAL]"] <- "barrier_reason_dontneed_had_covid"
spreadsheet$indicator_name[spreadsheet$indicator_name == "barrier_reason_dontneed_dont_spend_time [US]\nbarrier_reason_dontneed_dontspendtime [GLOBAL]"] <- "barrier_reason_dontneed_dont_spend_time"
spreadsheet$indicator_name[spreadsheet$indicator_name == "barrier_reason_dontneed_not_beneficial [US]\nbarrier_reason_dontneed_notbeneficial [GLOBAL]"] <- "barrier_reason_dontneed_not_beneficial"
spreadsheet$indicator_name[spreadsheet$indicator_name == "barrier_reason_dontneed_not_high_risk [US]\nbarrier_reason_dontneed_nothighrisk [GLOBAL]"] <- "barrier_reason_dontneed_not_high_risk"
spreadsheet$indicator_name[spreadsheet$indicator_name == "barrier_reason_dontneed_not_serious [US]\nbarrier_reason_dontneed_notserious [GLOBAL]"] <- "barrier_reason_dontneed_not_serious"
spreadsheet$indicator_name[spreadsheet$indicator_name == "barrier_reason_dontneed_precautions [US]\nbarrier_reason_dontneed_takeprecautions [GLOBAL]"] <- "barrier_reason_dontneed_precautions"

# Need to add entries to docs based on how ^^ turns out.
need_to_add_to_doc <- c()


# Need to rename in tables.
# spreadsheet$indicator_name[spreadsheet$indicator_name == "symptom_hospital"] <- "unusual_symptom_hospital"
# spreadsheet$indicator_name[spreadsheet$indicator_name == "symptom_hospital_tried"] <- "unusual_symptom_hospital_tried"


# Need to define and generate for full history, then join onto existing tables.
need_to_add_to_tables <- c(
  # "language_home_portuguese",
  # 
  # "unusual_symptom_medical_care_none",
  # 
  # # Variables defined but not turned into indicators.
  # "barrier_reason_dontneed_alreadyhad",
  # "barrier_reason_dontneed_dontspendtime",
  # "barrier_reason_dontneed_nothighrisk",
  # "barrier_reason_dontneed_takeprecautions",
  # "barrier_reason_dontneed_notserious",
  # "barrier_reason_dontneed_notbeneficial",
  # "barrier_reason_dontneed_other"
)


final_underscore <- endsWith(spreadsheet$indicator_name, "_")
spreadsheet$indicator_name[final_underscore] <- substr(
  spreadsheet$indicator_name[final_underscore], 1, nchar(spreadsheet$indicator_name[final_underscore]) - 1
)


dates_added <- full_join(spreadsheet, input_dates, by = c("indicator_name" = "signal"))
# View(dates_added)
unmatched_items <- dates_added %>% 
  filter(is.na(in_cmu) | (in_cmu & is.na(cmu_indicator_start))) %>% 
  filter(!(indicator_name %in% need_to_add_to_doc)) %>% 
  filter(!(indicator_name %in% need_to_add_to_tables))
# View(unmatched_items)

write_csv(dates_added, "Downloads/contingency_doc_signal_dates_added.csv")
