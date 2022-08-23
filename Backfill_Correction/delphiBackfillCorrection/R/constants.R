import(tibble)

# Constants for the backfill correction model
taus <- c(0.01, 0.025, 0.1, 0.25, 0.5, 0.75, 0.9, 0.975, 0.99)
ref_lag <- 60
test_lags <- c(1:14, 21, 35, 51)
training_days <- 270
testing_window <- 14
lag_window <- 5
lambda <- 0.1
lp_solver = "gurobi" # LP solver to use in quantile_lasso(); "gurobi" or "glpk"

yitl = "log_value_raw"
slope = "log_7dav_slope"
y7dav = "log_value_7dav"
wd = c("Mon", "Tue", "Wed", "Thurs", "Fri", "Sat")
wm <- c("W1_issue", "W2_issue", "W3_issue")

#sqrtscale = c('sqrty0', 'sqrty1', 'sqrty2', 'sqrty3')
sqrtscale_covid = c('sqrty0_covid', 'sqrty1_covid', 'sqrty2_covid')
sqrtscale_total = c('sqrty0_total', 'sqrty1_total', 'sqrty2_total')
sqrtscale = c('sqrty0', 'sqrty1', "sqrty2")
log_lag = "inv_log_lag"

today = Sys.Date()

indicators_and_signals <- tribble(
  ~indicator, ~signal, ~name_suffix, ~value_type, ~sub_dir,
  "changehc", "covid", "", "count", "chng",
  "changehc", "flu", "", "count", "chng",
  "claims_hosp", "", "", "count", "claims_hosp",
  # "dv",,,
  "quidel", "covidtest", c("total", "age_0_4", "age_5_17", "age_18_49", "age_50_64", "age_65plus", "age_0_17"), "count", "quidel_covidtest"
)
