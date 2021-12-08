from datetime import datetime, timedelta
import os
import logging


def regen(issue_date: datetime):
    fake_date = datetime.strftime(issue_date, '%Y%m%d')
    fake_datetime = datetime.strftime(issue_date, '%Y-%m-%d %H:%M:%S')

    out_dir = f"/home/maria/Delphi/HospClaims/regen/issue_{fake_date}"
    out_dir_no_se = out_dir + "/hospital-admissions"
    #if os.path.isdir(out_dir_no_se) and len(os.listdir(out_dir_no_se)) > 0:
    #    logging.info(f"files in output dir, skipping {issue_date}")
    #    return False

    os.makedirs(out_dir_no_se, exist_ok=True)
    os.system(
        f"faketime '{fake_datetime}' /home/maria/Delphi/HospClaims/automate/hosp_claims_regen_script.sh {out_dir_no_se}")

    logging.info(str(issue_date.date()))


def main():
    hour = 23

    start_date = datetime(2021, 6, 12, hour)
    end_date = datetime(2021, 6, 13, hour)
    #start_date = datetime(2020, 6, 2, hour)
    #end_date = datetime(2020, 8, 4, hour)
    n_dates = (end_date - start_date).days + 1
    date_range = [start_date + timedelta(days=a) for a in range(n_dates)]

    logging.basicConfig(level=logging.DEBUG, filename="out.log",
                        filemode="a+",
                        format="%(asctime)-15s %(levelname)-8s %(message)s")

    #date_range = [datetime(2020, 6, 21, hour)]
    for date in date_range:
        try:
            regen(date)
        except Exception as e:
            logging.info(e)
            continue


if __name__ == "__main__":
    main()
