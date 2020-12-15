library(covidcast)
library(dplyr)
meta_info = covidcast_meta()
locations_by_type = meta_info %>% group_by(geo_type) %>% summarize(Value = max(num_locations))

results = list()
for (i in 1:nrow(locations_by_type)){
  type = locations_by_type$geo_type[i]
  max_locations = locations_by_type$Value[i]
  max_row = with(meta_info, meta_info[geo_type == type & num_locations == max_locations,][1,])
  data_source = max_row$data_source
  signal = max_row$signal
  results[[i]] = covidcast_signal(data_source, signal, geo_type = type)
  geo_values = sort(unique(results[[i]]$geo_value))
  file_name = paste0("../static/", type, "_geo.csv")
  write.table(geo_values, file = file_name, row.names = F, col.names = "geo_id")
}

