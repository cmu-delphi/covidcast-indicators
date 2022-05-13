# Constants for the backfill correction model

taus <- c(0.01, 0.025, 0.1, 0.25, 0.5, 0.75, 0.9, 0.975, 0.99)
ref_lag <- 60
lag_knots <- c(1, 2, 3, 4, 5, 7, 10, 14) 
upper_lag <- 15
training_days <- 270
testing_window <- 14
lag_window <- 5
lambda <- 0.1

ld_name = "01"
yitl = "log_value_raw"
slope = "log_7dav_slope"
y7dav = "log_value_7dav"
wd = c("Mon", "Tue", "Wed", "Thurs", "Fri", "Sat")
wd2 = c("Mon2", "Tue2", "Wed2", "Thurs2", "Fri2", "Sat2")
wm <- c("W1_issue", "W2_issue", "W3_issue")
#sqrtscale = c('sqrty0', 'sqrty1', 'sqrty2', 'sqrty3')
sqrtscale_covid = c('sqrty0_covid', 'sqrty1_covid', 'sqrty2_covid')
sqrtscale_total = c('sqrty0_total', 'sqrty1_total', 'sqrty2_total')
sqrtscale = c('sqrty0', 'sqrty1', "sqrty2")
log_lag = "inv_log_lag"