import glob
import os
from datetime import datetime
import pandas as pd

FILTER_OUT_UNWEIGHTED = False

input_files = glob.glob("Downloads/20220919_ctis_api_backfill/glosa_api_backfill_20200406-20210131/202*.csv.gz") + \
    glob.glob("Downloads/20220919_ctis_api_backfill/sijo_api_backfill_20210129-20211128/202*.csv.gz") + \
    glob.glob("Downloads/20220919_ctis_api_backfill/yadu_api_backfill_20211202-20220626/202*.csv.gz")
input_files.sort()

results = dict()
weighted_signals = set()
for file in input_files:
    # Get date and signal name from file name.
    # file = "20220613_hrr_smoothed_wtrust_covid_info_friends.csv.gz"
    # Drop file extension.
    file = os.path.basename(file)[:-7]
    file_parts = file.split("_")

    day = datetime.strptime(file_parts[0], "%Y%m%d").date()
    signal = "_".join(file_parts[2:])

    file_parts[3] = "w" + file_parts[3]
    weighted_signals.add("_".join(file_parts[2:]))

    if signal not in results.keys():
        results[signal] = (day, day)
    else:
        old_start = results[signal][0]
        old_end = results[signal][1]
        results[signal] = (min(day, old_start), max(day, old_end))


output_name = "Downloads/weighted_and_unweighted_api_signal_dates.csv"

if FILTER_OUT_UNWEIGHTED:
    signals_wo_unweighted = set([
        "smoothed_wchild_vaccine_already",
        "smoothed_wchild_vaccine_no_def",
        "smoothed_wchild_vaccine_no_prob",
        "smoothed_wchild_vaccine_yes_def",
        "smoothed_wchild_vaccine_yes_prob",
        "smoothed_wflu_vaccinated_2021",
        "smoothed_winitial_dose_one_of_one",
        "smoothed_winitial_dose_one_of_two",
        "smoothed_winitial_dose_two_of_two",
        "smoothed_wremote_school_fulltime_oldest",
        "smoothed_wschool_safety_measures_cafeteria",
        "smoothed_wschool_safety_measures_dont_know",
        "smoothed_wschool_safety_measures_extracurricular",
        "smoothed_wschool_safety_measures_mask_students",
        "smoothed_wschool_safety_measures_mask_teachers",
        "smoothed_wschool_safety_measures_restricted_entry",
        "smoothed_wschool_safety_measures_separators",
        "smoothed_wschool_safety_measures_symptom_screen",
        "smoothed_wschool_safety_measures_testing_staff",
        "smoothed_wschool_safety_measures_testing_students",
        "smoothed_wschool_safety_measures_vaccine_staff",
        "smoothed_wschool_safety_measures_vaccine_students",
        "smoothed_wschool_safety_measures_ventilation",
        "smoothed_wvaccinated_at_least_one_booster",
        "smoothed_wvaccinated_booster_accept",
        "smoothed_wvaccinated_booster_defno",
        "smoothed_wvaccinated_booster_defyes",
        "smoothed_wvaccinated_booster_hesitant",
        "smoothed_wvaccinated_booster_probno",
        "smoothed_wvaccinated_booster_probyes",
        "smoothed_wvaccinated_no_booster",
        "smoothed_wvaccinated_one_booster",
        "smoothed_wvaccinated_two_or_more_boosters"
    ])

    for key in results.copy().keys():
        if key in signals_wo_unweighted:
            continue
        if key not in weighted_signals:
            # Must be unweighted, drop it.
            del results[key]

    output_name = "Downloads/api_signal_dates.csv"

pd.DataFrame.from_dict(
    results, orient="index"
).reset_index(
).rename(
    columns={"index":"signal",0:"start_date", 1:"end_date"}
).sort_values("signal"
).to_csv(
    output_name,
    index=False
)
