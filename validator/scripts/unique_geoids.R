library(covidcast)

geo_types = c("county", "state", "hrr", "msa")
for(type in geo_types){
  dtf_deaths = covidcast_signal("jhu-csse" ,"deaths_incidence_num", start_day = "2020-10-01", end_day = "2020-10-01", geo_type = type)
  dtf_cases = covidcast_signal("indicator-combination", "confirmed_7dav_incidence_num", start_day = "2020-10-01", end_day = "2020-10-01", geo_type = type)
  unique_geo_values = sort(union(dtf_deaths$geo_value, dtf_cases$geo_value))
  file_name = paste0("../static/", type, "_geo.csv")
  write.table(unique_geo_values, file = file_name, row.names = F, col.names = "geo_id")
}

dtf = covidcast_signal("ght", "raw_search", start_day = "2020-10-01", end_day = "2020-10-01", geo_type = "dma")
file_name = "../static/dma_geo.csv"
write.table(unique(dtf$geo_value), file = file_name, row.names = F, col.names = "geo_id")

national_file = "../static/national_geo.csv"
write.table("us", file = national_file, row.names = F, col.names = "geo_id")
