"""
Authors: Dmitry Shemetov, James Sharpnack

cd _delphi_utils/data_proc/geomap
python geo_data_proc.py
"""

from io import BytesIO
from os import remove, listdir
from os.path import join, isfile
from zipfile import ZipFile

import requests
import pandas as pd
import numpy as np


# Source files
YEAR = 2020
INPUT_DIR = "./old_source_files"
OUTPUT_DIR = f"../../delphi_utils/data/{YEAR}"
FIPS_BY_ZIP_POP_URL = "https://www2.census.gov/geo/docs/maps-data/data/rel/zcta_county_rel_10.txt?#"
ZIP_HSA_HRR_URL = "https://atlasdata.dartmouth.edu/downloads/geography/ZipHsaHrr18.csv.zip"
ZIP_HSA_HRR_FILENAME = "ZipHsaHrr18.csv"
FIPS_MSA_URL = "https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2018/delineation-files/list1_Sep_2018.xls"
STATE_CODES_URL = "http://www2.census.gov/geo/docs/reference/state.txt?#"
FIPS_POPULATION_URL = f"https://www2.census.gov/programs-surveys/popest/datasets/2010-{YEAR}/counties/totals/co-est{YEAR}-alldata.csv"
FIPS_PUERTO_RICO_POPULATION_URL = "https://www2.census.gov/geo/docs/maps-data/data/rel/zcta_county_rel_10.txt?"
STATE_HHS_FILE = "hhs.txt"
ZIP_POP_MISSING_FILE = "zip_pop_filling.csv"
CHNG_COUNTY_GROUPS_FILE = "chng_county_groups.csv"

# Out files
FIPS_STATE_OUT_FILENAME = "fips_state_table.csv"
FIPS_MSA_OUT_FILENAME = "fips_msa_table.csv"
FIPS_HRR_OUT_FILENAME = "fips_hrr_table.csv"
FIPS_ZIP_OUT_FILENAME = "fips_zip_table.csv"
FIPS_HHS_FILENAME = "fips_hhs_table.csv"
FIPS_CHNGFIPS_OUT_FILENAME = "fips_chng-fips_table.csv"
FIPS_POPULATION_OUT_FILENAME = "fips_pop.csv"
CHNGFIPS_STATE_OUT_FILENAME = "chng-fips_state_table.csv"
ZIP_HSA_OUT_FILENAME = "zip_hsa_table.csv"
ZIP_HRR_OUT_FILENAME = "zip_hrr_table.csv"
ZIP_FIPS_OUT_FILENAME = "zip_fips_table.csv"
ZIP_MSA_OUT_FILENAME = "zip_msa_table.csv"
ZIP_POPULATION_OUT_FILENAME = "zip_pop.csv"
ZIP_STATE_CODE_OUT_FILENAME = "zip_state_code_table.csv"
ZIP_HHS_FILENAME = "zip_hhs_table.csv"
STATE_OUT_FILENAME = "state_codes_table.csv"
STATE_HHS_OUT_FILENAME = "state_code_hhs_table.csv"
STATE_POPULATION_OUT_FILENAME = "state_pop.csv"
HHS_POPULATION_OUT_FILENAME = "hhs_pop.csv"
NATION_POPULATION_OUT_FILENAME = "nation_pop.csv"


def create_fips_zip_crosswalk():
    """Build (weighted) crosswalk tables for FIPS to ZIP and ZIP to FIPS."""
    pop_df = pd.read_csv(FIPS_BY_ZIP_POP_URL).rename(columns={"POPPT": "pop"})
    # Create the FIPS column by combining the state and county codes
    pop_df["fips"] = pop_df["STATE"].astype(str).str.zfill(2) + pop_df["COUNTY"].astype(str).str.zfill(3)
    # Create the ZIP column by adding leading zeros to the ZIP
    pop_df["zip"] = pop_df["ZCTA5"].astype(str).str.zfill(5)
    pop_df = pop_df[["zip", "fips", "pop"]]

    # Find the population fractions (the heaviest computation, takes about a minute)
    # Note that the denominator in the fractions is the source population
    pop_df.set_index(["fips", "zip"], inplace=True)
    fips_zip: pd.DataFrame = pop_df.groupby("fips", as_index=False).apply(lambda g: g["pop"] / g["pop"].sum())
    zip_fips: pd.DataFrame = pop_df.groupby("zip", as_index=False).apply(lambda g: g["pop"] / g["pop"].sum())

    # Rename and write to file
    fips_zip = fips_zip.reset_index(level=["fips", "zip"]).rename(columns={"pop": "weight"}).query("weight > 0.0")
    fips_zip.sort_values(["fips", "zip"]).to_csv(join(OUTPUT_DIR, FIPS_ZIP_OUT_FILENAME), index=False)

    zip_fips = zip_fips.reset_index(level=["fips", "zip"]).rename(columns={"pop": "weight"}).query("weight > 0.0")
    zip_fips.sort_values(["zip", "fips"]).to_csv(join(OUTPUT_DIR, ZIP_FIPS_OUT_FILENAME), index=False)


def create_zip_hsa_hrr_crosswalk():
    """Build a crosswalk table for ZIP to HSA and for ZIP to HRR."""
    with ZipFile(BytesIO(requests.get(ZIP_HSA_HRR_URL).content)) as zipped_csv:
        zip_df = pd.read_csv(zipped_csv.open(ZIP_HSA_HRR_FILENAME))

    hsa_df = zip_df[["zipcode18", "hsanum"]].rename(columns={"zipcode18": "zip", "hsanum": "hsa"})
    hsa_df["zip"] = hsa_df["zip"].astype(str).str.zfill(5)
    hsa_df["hsa"] = hsa_df["hsa"].astype(str)
    hsa_df.sort_values(["zip", "hsa"]).to_csv(join(OUTPUT_DIR, ZIP_HSA_OUT_FILENAME), index=False)

    hrr_df = zip_df[["zipcode18", "hrrnum"]].rename(columns={"zipcode18": "zip", "hrrnum": "hrr"})
    hrr_df["zip"] = hrr_df["zip"].astype(str).str.zfill(5)
    hrr_df["hrr"] = hrr_df["hrr"].astype(str)
    hrr_df.sort_values(["zip", "hrr"]).to_csv(join(OUTPUT_DIR, ZIP_HRR_OUT_FILENAME), index=False)


def create_fips_msa_crosswalk():
    """Build a crosswalk table for FIPS to MSA."""
    # Requires xlrd.
    msa_df = pd.read_excel(FIPS_MSA_URL, skiprows=2, skipfooter=4, dtype={"CBSA Code": int, "Metropolitan/Micropolitan Statistical Area": str, "FIPS State Code": str, "FIPS County Code": str}).rename(columns={"CBSA Code": "msa"})
    msa_df = msa_df[msa_df["Metropolitan/Micropolitan Statistical Area"] == "Metropolitan Statistical Area"]

    # Combine state and county codes into a single FIPS code
    msa_df["fips"] = msa_df["FIPS State Code"].str.cat(msa_df["FIPS County Code"])

    msa_df.sort_values(["fips", "msa"]).to_csv(join(OUTPUT_DIR, FIPS_MSA_OUT_FILENAME), columns=["fips", "msa"], index=False)



def create_state_codes_crosswalk():
    """Build a State ID -> State Name -> State code crosswalk file."""
    df = pd.read_csv(STATE_CODES_URL, delimiter="|").drop(columns="STATENS").rename(columns={"STATE": "state_code", "STUSAB": "state_id", "STATE_NAME": "state_name"})
    df["state_code"] = df["state_code"].astype(str).str.zfill(2)
    df["state_id"] = df["state_id"].astype(str).str.lower()

    # Add a few extra US state territories manually
    territories = pd.DataFrame(
        [
            {
                "state_code": "70",
                "state_name": "Republic of Palau",
                "state_id": "pw",
            },
            {
                "state_code": "68",
                "state_name": "Marshall Islands",
                "state_id": "mh",
            },
            {
                "state_code": "64",
                "state_name": "Federated States of Micronesia",
                "state_id": "fm",
            },
        ]
    )
    df = pd.concat((df, territories))
    df.sort_values("state_code").to_csv(join(OUTPUT_DIR, STATE_OUT_FILENAME), index=False)


def create_state_hhs_crosswalk():
    """Build a state to HHS crosswalk."""
    if not isfile(join(OUTPUT_DIR, STATE_OUT_FILENAME)):
        create_state_codes_crosswalk()

    ss_df = pd.read_csv(join(OUTPUT_DIR, STATE_OUT_FILENAME), dtype={"state_code": str, "state_name": str, "state_id": str})

    with open(STATE_HHS_FILE) as temp_file:
        temp = temp_file.readlines()

    # Process text from https://www.hhs.gov/about/agencies/iea/regional-offices/index.html
    temp = [int(s[7:9]) if "Region" in s else s for s in temp]
    temp = [s.strip().split(", ") if isinstance(s, str) else s for s in temp]
    temp = {temp[i]: temp[i + 1] for i in range(0, len(temp), 2)}
    temp = {key: [x.lstrip(" and") for x in temp[key]] for key in temp}
    temp = [[(key, x) for x in temp[key]] for key in temp]
    hhs_state_pairs = [x for y in temp for x in y]

    # Make naming adjustments
    hhs_state_pairs.remove((2, "the Virgin Islands"))
    hhs_state_pairs.append((2, "U.S. Virgin Islands"))
    hhs_state_pairs.remove((9, "Commonwealth of the Northern Mariana Islands"))
    hhs_state_pairs.append((9, "Northern Mariana Islands"))

    # Make dataframe
    hhs_df = pd.DataFrame(hhs_state_pairs, columns=["hhs", "state_name"], dtype=str)

    ss_df = ss_df.merge(hhs_df, on="state_name", how="left").dropna()
    ss_df.sort_values("state_code").to_csv(join(OUTPUT_DIR, STATE_HHS_OUT_FILENAME), columns=["state_code", "hhs"], index=False)


def create_fips_population_table():
    """Build a table of populations by FIPS county codes.

    Uses US Census Bureau population data as determined by the YEAR variable, with 2010 population data for Puerto Rico and a few exceptions.
    """
    census_pop = pd.read_csv(FIPS_POPULATION_URL, encoding="ISO-8859-1")
    census_pop["fips"] = census_pop.apply(lambda x: f"{x['STATE']:02d}{x['COUNTY']:03d}", axis=1)
    census_pop = census_pop.rename(columns={f"POPESTIMATE{YEAR}": "pop"})[["fips", "pop"]]

    # Set population for Dukes and Nantucket combo county
    dukes_pop = int(census_pop.loc[census_pop["fips"] == "25007", "pop"])
    nantu_pop = int(census_pop.loc[census_pop["fips"] == "25019", "pop"])
    hand_modified_pop = pd.DataFrame(
        [
            # Dukes and Nantucket combo county
            {"fips": "70002", "pop": dukes_pop + nantu_pop},
            # Kansas City
            {"fips": "70003", "pop": 491918},
        ]
    )
    census_pop = pd.concat([census_pop, hand_modified_pop])
    census_pop = census_pop.reset_index(drop=True)

    # Get the file with Puerto Rico populations
    df_pr = pd.read_csv(FIPS_PUERTO_RICO_POPULATION_URL).rename(columns={"POPPT": "pop"})
    df_pr["fips"] = df_pr["STATE"].astype(str).str.zfill(2) + df_pr["COUNTY"].astype(str).str.zfill(3)
    df_pr = df_pr[["fips", "pop"]]
    # Create the Puerto Rico megaFIPS
    df_pr = df_pr[df_pr["fips"].isin([str(x) for x in range(72000, 72999)])]
    df_pr = pd.concat([df_pr, pd.DataFrame([{"fips": "72000", "pop": df_pr["pop"].sum()}])])
    # Fill the missing Puerto Rico data with 2010 information
    df_pr = df_pr.groupby("fips").sum().reset_index()
    df_pr = df_pr[~df_pr["fips"].isin(census_pop["fips"])]
    census_pop_pr = pd.concat([census_pop, df_pr])

    # Filled from https://www.census.gov/data/tables/2010/dec/2010-island-areas.html
    territories_pop = pd.DataFrame(
        {
            "fips": ["60010", "60020", "60030", "60040", "60050", "66010", "78010", "78020", "78030", "69085", "69100", "69110", "69120"],
            "pop": [23030, 1143, 0, 17, 31329, 159358, 50601, 4170, 51634, 0, 2527, 48220, 3136],
        }
    )
    census_pop_territories = pd.concat([census_pop_pr, territories_pop])
    non_megafips_mask = ~census_pop_territories.fips.str.endswith("000")
    census_pop_territories = census_pop_territories.loc[non_megafips_mask]
    census_pop_territories.sort_values("fips").to_csv(join(OUTPUT_DIR, FIPS_POPULATION_OUT_FILENAME), index=False)


def create_state_population_table():
    """Build a state population table."""
    if not isfile(join(OUTPUT_DIR, FIPS_POPULATION_OUT_FILENAME)):
        create_fips_population_table()

    if not isfile(join(OUTPUT_DIR, FIPS_STATE_OUT_FILENAME)):
        derive_fips_state_crosswalk()

    census_pop = pd.read_csv(join(OUTPUT_DIR, FIPS_POPULATION_OUT_FILENAME), dtype={"fips": str, "pop": int})
    state: pd.DataFrame = pd.read_csv(join(OUTPUT_DIR, FIPS_STATE_OUT_FILENAME), dtype=str)
    state_pop = state.merge(census_pop, on="fips").groupby(["state_code", "state_id", "state_name"], as_index=False).sum()
    state_pop.sort_values("state_code").to_csv(join(OUTPUT_DIR, STATE_POPULATION_OUT_FILENAME), index=False)


def create_hhs_population_table():
    """Build an HHS population table."""
    if not isfile(join(OUTPUT_DIR, STATE_POPULATION_OUT_FILENAME)):
        create_state_population_table()

    if not isfile(join(OUTPUT_DIR, STATE_HHS_OUT_FILENAME)):
        create_state_hhs_crosswalk()

    state_pop = pd.read_csv(join(OUTPUT_DIR, STATE_POPULATION_OUT_FILENAME), dtype={"state_code": str, "hhs": int}, usecols=["state_code", "pop"])
    state_hhs = pd.read_csv(join(OUTPUT_DIR, STATE_HHS_OUT_FILENAME), dtype=str)
    hhs_pop = state_pop.merge(state_hhs, on="state_code").groupby("hhs", as_index=False).sum()

    hhs_pop.sort_values("hhs").to_csv(join(OUTPUT_DIR, HHS_POPULATION_OUT_FILENAME), index=False)


def create_nation_population_table():
    """Build a nation population table."""
    if not isfile(join(OUTPUT_DIR, FIPS_POPULATION_OUT_FILENAME)):
        create_fips_population_table()

    census_pop = pd.read_csv(join(OUTPUT_DIR, FIPS_POPULATION_OUT_FILENAME), dtype={"fips": str, "pop": int})
    nation_pop = pd.DataFrame({"nation": ["us"], "pop": [census_pop["pop"].sum()]})
    nation_pop.to_csv(join(OUTPUT_DIR, NATION_POPULATION_OUT_FILENAME), index=False)


def derive_zip_population_table():
    """Build a table of populations by ZIP code by translating from FIPS populations."""
    if not isfile(join(OUTPUT_DIR, FIPS_POPULATION_OUT_FILENAME)):
        create_fips_population_table()

    if not isfile(join(OUTPUT_DIR, FIPS_ZIP_OUT_FILENAME)):
        create_fips_zip_crosswalk()

    census_pop = pd.read_csv(join(OUTPUT_DIR, FIPS_POPULATION_OUT_FILENAME), dtype={"fips": str, "pop": int})
    fz_df = pd.read_csv(join(OUTPUT_DIR, FIPS_ZIP_OUT_FILENAME), dtype={"fips": str, "zip": str, "weight": float})

    df = census_pop.merge(fz_df, on="fips", how="left")
    df["pop"] = df["pop"].multiply(df["weight"], axis=0)
    df = df.drop(columns=["fips", "weight"]).groupby("zip").sum().dropna().reset_index()

    ## loading populatoin of some zips- #Issue 0648
    zip_pop_missing = pd.read_csv(
        ZIP_POP_MISSING_FILE,sep=",",
        dtype={"zip":str,"pop":np.int32}
        )
    ## cheking if each zip still missing, and concatenating if True
    for x_zip in zip_pop_missing['zip']:
        if x_zip not in df['zip']:
            df = pd.concat([df, zip_pop_missing[zip_pop_missing['zip'] == x_zip]],
                          ignore_index=True)

    df["pop"] = df["pop"].astype(int)
    df.sort_values("zip").to_csv(join(OUTPUT_DIR, ZIP_POPULATION_OUT_FILENAME), index=False)


def derive_fips_hrr_crosswalk():
    """Derive a crosswalk file from FIPS to HRR through FIPS -> ZIP -> HRR."""
    if not isfile(join(OUTPUT_DIR, FIPS_ZIP_OUT_FILENAME)):
        create_fips_zip_crosswalk()

    if not isfile(join(OUTPUT_DIR, ZIP_HRR_OUT_FILENAME)):
        create_zip_hsa_hrr_crosswalk()

    fz_df = pd.read_csv(join(OUTPUT_DIR, FIPS_ZIP_OUT_FILENAME), dtype={"fips": str, "zip": str, "weight": float})
    zh_df = pd.read_csv(join(OUTPUT_DIR, ZIP_HRR_OUT_FILENAME), dtype={"zip": str, "hrr": str})

    fz_df = fz_df.merge(zh_df, on="zip", how="left").drop(columns="zip").groupby(["fips", "hrr"]).sum().reset_index()
    fz_df.sort_values(["fips", "hrr"]).to_csv(join(OUTPUT_DIR, FIPS_HRR_OUT_FILENAME), index=False)


def derive_fips_state_crosswalk():
    """Derive a crosswalk between FIPS county codes and state information (number, abbreviation, name)."""
    fips_pop = pd.read_csv(join(OUTPUT_DIR, FIPS_POPULATION_OUT_FILENAME), dtype={"fips": str, "pop": int})

    megafips = pd.DataFrame({"fips": [fips + "000" for fips in fips_pop.fips.str[:2].unique()], "pop": np.nan})
    fips_pop = pd.concat([fips_pop, megafips])

    state_codes = pd.read_csv(join(OUTPUT_DIR, STATE_OUT_FILENAME), dtype={"state_code": str, "state_id": str, "state_name": str})
    fips_pop["state_code"] = fips_pop["fips"].str[:2]
    fips_pop = fips_pop.merge(state_codes, on="state_code", how="left").drop(columns="pop")
    fips_pop.sort_values(["fips", "state_code"]).to_csv(join(OUTPUT_DIR, FIPS_STATE_OUT_FILENAME), index=False)


def derive_zip_msa_crosswalk():
    """Derive a crosswalk file from ZIP to MSA through ZIP -> FIPS -> HRR."""
    if not isfile(join(OUTPUT_DIR, ZIP_FIPS_OUT_FILENAME)):
        create_fips_zip_crosswalk()

    if not isfile(join(OUTPUT_DIR, FIPS_MSA_OUT_FILENAME)):
        create_fips_msa_crosswalk()

    zf_df = pd.read_csv(join(OUTPUT_DIR, ZIP_FIPS_OUT_FILENAME), dtype={"zip": str, "fips": str, "weight": float})
    fm_df = pd.read_csv(join(OUTPUT_DIR, FIPS_MSA_OUT_FILENAME), dtype={"fips": str, "msa": str})

    zf_df = zf_df.merge(fm_df, on="fips").drop(columns="fips").groupby(["msa", "zip"]).sum().reset_index()
    zf_df.sort_values(["zip", "msa"]).to_csv(join(OUTPUT_DIR, ZIP_MSA_OUT_FILENAME), index=False)


def derive_zip_to_state_code():
    """Derive a crosswalk between ZIP codes and state information (number, abbreviation, name)."""
    if not isfile(join(OUTPUT_DIR, STATE_OUT_FILENAME)):
        create_state_codes_crosswalk()

    if not isfile(join(OUTPUT_DIR, ZIP_FIPS_OUT_FILENAME)):
        create_fips_zip_crosswalk()

    sdf = pd.read_csv(join(OUTPUT_DIR, STATE_OUT_FILENAME), dtype={"state_code": str, "state_id": str, "state_name": str})
    zf_cf = pd.read_csv(join(OUTPUT_DIR, ZIP_FIPS_OUT_FILENAME), dtype={"zip": str, "fips": str})

    zf_cf["state_code"] = zf_cf["fips"].str[:2]
    zf_cf = zf_cf.merge(sdf, left_on="state_code", right_on="state_code", how="left").drop(columns=["fips"])
    zf_cf.sort_values(["zip", "state_code"]).to_csv(join(OUTPUT_DIR, ZIP_STATE_CODE_OUT_FILENAME), index=False)


def derive_fips_hhs_crosswalk():
    """Derive a crosswalk between FIPS county codes and HHS regions."""
    if not isfile(join(OUTPUT_DIR, STATE_HHS_OUT_FILENAME)):
        create_state_hhs_crosswalk()

    if not isfile(join(OUTPUT_DIR, FIPS_POPULATION_OUT_FILENAME)):
        create_fips_population_table()

    fips_pop = pd.read_csv(join(OUTPUT_DIR, FIPS_POPULATION_OUT_FILENAME), dtype={"fips": str, "pop": int})
    megafips = pd.DataFrame({"fips": [fips + "000" for fips in fips_pop.fips.str[:2].unique()], "pop": np.nan})
    fips_pop = pd.concat([fips_pop, megafips])

    state_hhs = pd.read_csv(join(OUTPUT_DIR, STATE_HHS_OUT_FILENAME), dtype={"state_code": str, "hhs": str})

    fips_pop["state_code"] = fips_pop["fips"].str[:2]
    fips_pop = fips_pop.merge(state_hhs, on="state_code", how="left").drop(columns=["state_code", "pop"])
    fips_pop.sort_values(["fips", "hhs"]).to_csv(join(OUTPUT_DIR, FIPS_HHS_FILENAME), index=False)


def derive_zip_hhs_crosswalk():
    """Derive a crosswalk between zip code and HHS regions."""
    if not isfile(join(OUTPUT_DIR, STATE_HHS_OUT_FILENAME)):
        create_state_hhs_crosswalk()

    if not isfile(join(OUTPUT_DIR, ZIP_STATE_CODE_OUT_FILENAME)):
        derive_zip_to_state_code()

    zip_state = pd.read_csv(join(OUTPUT_DIR, ZIP_STATE_CODE_OUT_FILENAME), dtype={"zip": str, "pop": int, "state_code": str})
    state_hhs = pd.read_csv(join(OUTPUT_DIR, STATE_HHS_OUT_FILENAME), dtype={"state_code": str, "hhs": str})

    zip_state = zip_state.merge(state_hhs, on="state_code", how="left").drop(columns=["state_code", "state_id", "state_name"])
    zip_state.sort_values(["zip", "hhs"]).to_csv(join(OUTPUT_DIR, ZIP_HHS_FILENAME), index=False)


def derive_fips_chngfips_crosswalk():
    """Build a crosswalk table for FIPS to CHNG FIPS."""
    if not isfile(join(OUTPUT_DIR, FIPS_STATE_OUT_FILENAME)):
        derive_fips_state_crosswalk()

    assign_county_groups()
    county_groups = pd.read_csv(CHNG_COUNTY_GROUPS_FILE, dtype="string", index_col=False)
    # Split list of county FIPS codes into separate columns.
    county_groups = pd.concat(
            [county_groups, county_groups.fips_list.str.split("|", expand=True)],
            axis=1
        ).drop(
            columns = "fips_list"
        )

    # Change to long format.
    county_groups = pd.melt(
            county_groups,
            id_vars = ["state_fips", "group"],
            var_name = "county_num",
            value_name = "fips"
        ).drop(
            columns="county_num"
        ).dropna()

    county_groups["state_fips"] = county_groups["state_fips"].str.zfill(2)
    county_groups["group"] = county_groups["group"].str.zfill(2)
    county_groups["fips"] = county_groups["fips"].str.zfill(5).astype("string")
    # Combine state codes and group ids into a single FIPS code.
    county_groups["chng-fips"] = county_groups["state_fips"] + "g" + county_groups["group"]

    county_groups = county_groups[["fips", "chng-fips"]]
    fips_to_state = pd.read_csv(join(OUTPUT_DIR, FIPS_STATE_OUT_FILENAME), dtype="string", index_col=False)

    # Get all the fips that aren't included in the chng groupings.
    extra_fips_list = list(set(fips_to_state.fips) - set(county_groups.fips))
    # Normal fips codes and CHNG fips codes are the same for ungrouped counties.
    extra_fips_df = pd.DataFrame({"fips" : extra_fips_list, "chng-fips" : extra_fips_list}, dtype="string")

    # Combine grouped and ungrouped counties.
    pd.concat(
        [county_groups, extra_fips_df]
    ).sort_values(
        ["fips", "chng-fips"]
    ).to_csv(
        join(OUTPUT_DIR, FIPS_CHNGFIPS_OUT_FILENAME), index=False
    )


def derive_chngfips_state_crosswalk():
    """Build a crosswalk table for FIPS to CHNG FIPS."""
    if not isfile(join(OUTPUT_DIR, FIPS_STATE_OUT_FILENAME)):
        derive_fips_state_crosswalk()

    if not isfile(join(OUTPUT_DIR, FIPS_CHNGFIPS_OUT_FILENAME)):
        derive_fips_chngfips_crosswalk()

    fips_to_group = pd.read_csv(join(OUTPUT_DIR, FIPS_CHNGFIPS_OUT_FILENAME), dtype="string", index_col=False)
    fips_to_state = pd.read_csv(join(OUTPUT_DIR, FIPS_STATE_OUT_FILENAME), dtype="string", index_col=False)

    group_to_state = fips_to_group.join(
            fips_to_state.set_index("fips"), on="fips", how="left"
        ).drop(
            columns = "fips"
        ).drop_duplicates(
        ).sort_values(
            ["chng-fips", "state_code"]
        )
    group_to_state.to_csv(join(OUTPUT_DIR, CHNGFIPS_STATE_OUT_FILENAME), index=False)


def fetch_county_groups_spreadsheet():
    # County mapping file is derived from
    # https://docs.google.com/spreadsheets/d/1PEce4CjjHbRM1Z5xEMNI6Xsq_b2kkCh0/edit#gid=871427657
    sheet_id = "1PEce4CjjHbRM1Z5xEMNI6Xsq_b2kkCh0"
    sheet_name = "groupings"
    # Request sheet in CSV format via tag in URL.
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

    county_groups = pd.read_csv(
            url, dtype="string", index_col=False
        ).dropna(
            how="all", axis=1
        )
    county_groups["state FIPS"] = county_groups["state FIPS"].astype(int)

    # Counties belonging to each group are listed (as FIPS codes) in the "county
    # FIPS grouping" column, concatenated and separated by the pipe "|".
    new_names = {
        "state FIPS": "state_fips",
        "county FIPS grouping": "fips_list"
    }

    county_groups = county_groups.rename(
            columns=new_names
        )[new_names.values()]

    return county_groups


def assign_county_groups():
    county_groups = fetch_county_groups_spreadsheet()

    # If a county groups mapping file already exists in `data_proc/geomap`, we
    # have to be careful to not reassign a group number to a different group.
    # Group numbers must remain fixed, even if a given county group is no longer
    # being used.
    if isfile(CHNG_COUNTY_GROUPS_FILE):
        old_county_groups = pd.read_csv(CHNG_COUNTY_GROUPS_FILE, dtype="string", index_col=False)
        old_county_groups.group = old_county_groups.group.astype(int)
        old_county_groups.state_fips = old_county_groups.state_fips.astype(int)

        # Remove rows from county_groups if that `fips_list` value already
        # exists in old_county_groups.
        county_groups = county_groups[
            ~county_groups.fips_list.isin(old_county_groups.fips_list)
        ]

        # If grouping file has no new rows, no need to process again.
        if county_groups.empty:
            return
        # Grouping spreadsheet contains rows not seen in old, on-disk county
        # groupings file. Combining the two is delicate. While the code below
        # appears to work, it has not been formally tested and could be
        # invalid for even small changes to the format of the input county
        # groupings file.
        else:
            raise NotImplementedError(
                "Can't combine old and new county groupings automatically, "
                "code below is not tested or robust to changes in input format."
                "We recommend manually working with the code below and the new"
                "data in a REPL."
            )

        # Assign an incrementing integer to be the group id of each remaining
        # county grouping within a state using the given sort order.
        county_groups["group"] = county_groups.groupby("state_fips").cumcount() + 1

        # Find max group number by state in old_county_groups, join on, and
        # add max group number to group number.
        max_group_by_state = old_county_groups.groupby(
                "state_fips"
            ).group.max(
            ).reset_index(
            ).rename(
                columns = {"group": "max_group"}
            )
        county_groups = county_groups.join(
                max_group_by_state.set_index("state_fips"),
                how="left",
                on="state_fips"
            ).assign(
                group = lambda x: x.group + x.max_group
            ).drop(
                ["max_group"], axis=1
            )

        # Combine old_county_groups and county_groups
        county_groups = pd.concat([old_county_groups, county_groups])
    else:
        # Group numbers are 1-indexed.
        county_groups["group"] = county_groups.groupby("state_fips").cumcount() + 1

    county_groups.sort_values(
            ["state_fips"], kind="stable"
        ).to_csv(
            CHNG_COUNTY_GROUPS_FILE, index=False
        )


def clear_dir(dir_path: str):
    for fname in listdir(dir_path):
        remove(join(dir_path, fname))


if __name__ == "__main__":
    clear_dir(OUTPUT_DIR)

    create_fips_zip_crosswalk()
    create_zip_hsa_hrr_crosswalk()
    create_fips_msa_crosswalk()
    create_state_codes_crosswalk()
    create_state_hhs_crosswalk()
    create_fips_population_table()
    create_nation_population_table()
    create_state_population_table()
    create_hhs_population_table()

    derive_fips_hrr_crosswalk()
    derive_zip_msa_crosswalk()
    derive_zip_to_state_code()
    derive_fips_state_crosswalk()
    derive_zip_population_table()
    derive_fips_hhs_crosswalk()
    derive_zip_hhs_crosswalk()
    derive_fips_chngfips_crosswalk()
    derive_chngfips_state_crosswalk()
