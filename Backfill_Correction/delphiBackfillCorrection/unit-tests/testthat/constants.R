# Constants for the backfill correction model

taus <- c(0.01, 0.025, 0.1, 0.25, 0.5, 0.75, 0.9, 0.975, 0.99)
ref_lag <- 60
lag_knots <- c(1, 2, 3, 4, 5, 7, 10, 14) 
upper_lag <- 15
training_days <- 270
testing_window <- 14
lag_window <- 5
lambda <- 0.1

yitl <- "log_value_raw"
slope <- "log_7dav_slope"
y7dav <- "log_value_7dav"
wd <- c("Mon_issue", "Tue_issue", "Wed_issue", "Thurs_issue", "Fri_issue", "Sat_issue")
wd2 <- c("Mon_ref", "Tue_ref", "Wed_ref", "Thurs_ref", "Fri_ref", "Sat_ref")
wm <- c("W1_issue", "W2_issue", "W3_issue")
#sqrtscale = c('sqrty0', 'sqrty1', 'sqrty2', 'sqrty3')
sqrtscale_num <- c('sqrty0_num', 'sqrty1_num', 'sqrty2_num')
sqrtscale_denom <- c('sqrty0_denom', 'sqrty1_denom', 'sqrty2_denom')
sqrtscale <- c('sqrty0', 'sqrty1', "sqrty2")
log_lag <- "inv_log_lag"