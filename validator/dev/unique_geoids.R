library(covidcast)

geo_types = c("county", "state", "hrr", "msa")
for(type in geo_types){
  dtf = covidcast_signal("indicator-combination", "confirmed_7dav_incidence_num", start_day = "2020-10-01", end_day = "2020-10-01", geo_type = type)
  file_name = paste0("csv/", type, "_geo.csv")
  write.table(unique(dtf$geo_value), file = file_name, row.names = F, col.names = "geo_id")
}

dtf = covidcast_signal("ght", "raw_search", start_day = "2020-10-01", end_day = "2020-10-01", geo_type = "dma")
file_name = paste0("csv/dma_geo.csv")
write.table(unique(dtf$geo_value), file = file_name, row.names = F, col.names = "geo_id")