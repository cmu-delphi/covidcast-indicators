# Constants for the backfill correction model
TAUS <- c(0.01, 0.025, 0.1, 0.25, 0.5, 0.75, 0.9, 0.975, 0.99)
REF_LAG <- 60
TEST_LAGS <- c(1:14, 21, 35, 51)
TRAINING_DAYS <- 270
TESTING_WINDOW <- 1
LAG_WINDOW <- 5
LAMBDA <- 0.1
LAG_PAD <- 2
LP_SOLVER <-"gurobi" # LP solver to use in quantile_lasso(); "gurobi" or "glpk"

YITL <-"log_value_raw"
SLOPE <-"log_7dav_slope"
Y7DAV <-"log_value_7dav"

SQRTSCALE_COVID <-c('sqrty0_covid', 'sqrty1_covid', 'sqrty2_covid')
SQRTSCALE_TOTAL <-c('sqrty0_total', 'sqrty1_total', 'sqrty2_total')
SQRTSCALE <-c('sqrty0', 'sqrty1', "sqrty2")
LOG_LAG <-"inv_log_lag"

# Dates
DATE_FORMAT <- "%Y-%m-%d"
WEEKDAYS_ABBR <- c("Mon", "Tue", "Wed", "Thurs", "Fri", "Sat") # wd
WEEK_ISSUES <- c("W1_issue", "W2_issue", "W3_issue") # wm
TODAY <- Sys.Date()

# Signals we want to make predictions for
INDICATORS_AND_SIGNALS <- tibble::tribble(
  ~indicator, ~signal, ~name_suffix, ~sub_dir,
  "changehc", "covid", "", "chng",
  "changehc", "flu", "", "chng",
  "claims_hosp", "", "", "claims_hosp",
  # "dv",,,
  "quidel", "covidtest", c("total", "age_0_4", "age_5_17", "age_18_49", "age_50_64", "age_65plus", "age_0_17"), "quidel_covidtest"
)
