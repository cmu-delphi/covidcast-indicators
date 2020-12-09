"""
Authors: Dmitry Shemetov @dshemetov, James Sharpnack @jsharpna
"""

from io import BytesIO
from os.path import join, isfile
from zipfile import ZipFile

import requests
import pandas as pd


# Source files
INPUT_DIR = "./old_source_files"
OUTPUT_DIR = "../../delphi_utils/data"
FIPS_BY_ZIP_POP_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/rel/zcta_county_rel_10.txt?#"
)
ZIP_HSA_HRR_URL = (
    "https://atlasdata.dartmouth.edu/downloads/geography/ZipHsaHrr18.csv.zip"
)
ZIP_HSA_HRR_FILENAME = "ZipHsaHrr18.csv"
FIPS_MSA_URL = "https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2018/delineation-files/list1_Sep_2018.xls"
JHU_FIPS_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/UID_ISO_FIPS_LookUp_Table.csv"
STATE_CODES_URL = "http://www2.census.gov/geo/docs/reference/state.txt?#"
FIPS_POPULATION_URL = "https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/totals/co-est2019-alldata.csv"
FIPS_PUERTO_RICO_POPULATION_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/rel/zcta_county_rel_10.txt?"
)
STATE_HHS_FILE = "hhs.txt"

# Out files
FIPS_STATE_OUT_FILENAME = "fips_state_table.csv"
FIPS_MSA_OUT_FILENAME = "fips_msa_table.csv"
FIPS_HRR_OUT_FILENAME = "fips_hrr_table.csv"
FIPS_ZIP_OUT_FILENAME = "fips_zip_table.csv"
FIPS_HHS_FILENAME = "fips_hhs_table.csv"
FIPS_POPULATION_OUT_FILENAME = "fips_pop.csv"

ZIP_HSA_OUT_FILENAME = "zip_hsa_table.csv"
ZIP_HRR_OUT_FILENAME = "zip_hrr_table.csv"
ZIP_FIPS_OUT_FILENAME = "zip_fips_table.csv"
ZIP_MSA_OUT_FILENAME = "zip_msa_table.csv"
ZIP_POPULATION_OUT_FILENAME = "zip_pop.csv"
ZIP_STATE_CODE_OUT_FILENAME = "zip_state_code_table.csv"
ZIP_HHS_FILENAME = "zip_hhs_table.csv"
STATE_OUT_FILENAME = "state_codes_table.csv"
STATE_HHS_OUT_FILENAME = "state_code_hhs_table.csv"
JHU_FIPS_OUT_FILENAME = "jhu_uid_fips_table.csv"


def create_fips_zip_crosswalk():
    """
    Creates the (weighted) crosswalk tables between FIPS to ZIP and ZIP to FIPS
    from source.
    """
    pop_df = pd.read_csv(FIPS_BY_ZIP_POP_URL)

    # Create the FIPS column by combining the state and county codes
    state_codes = pop_df["STATE"].astype(str).str.zfill(2)
    county_codes = pop_df["COUNTY"].astype(str).str.zfill(3)
    pop_df["fips"] = state_codes + county_codes

    # Create the ZIP column by adding leading zeros to the ZIP
    pop_df["zip"] = pop_df["ZCTA5"].astype(str).str.zfill(5)

    # Pare down the dataframe to just the relevant columns: zip, fips, and population
    pop_df = pop_df[["zip", "fips", "POPPT"]].rename(columns={"POPPT": "pop"})

    # Find the population fractions (the heaviest computation, takes about a minute)
    # Note that the denominator in the fractions is the source population
    pop_df.set_index(["fips", "zip"], inplace=True)
    fips_zip = pop_df.groupby("fips", as_index=False).apply(
        lambda g: g["pop"] / g["pop"].sum()
    )
    zip_fips = pop_df.groupby("zip", as_index=False).apply(
        lambda g: g["pop"] / g["pop"].sum()
    )

    # Rename and write to file
    fips_zip = fips_zip.reset_index(level=["fips", "zip"]).rename(
        columns={"pop": "weight"}
    )
    fips_zip = fips_zip[fips_zip["weight"] > 0.0]
    fips_zip.to_csv(join(OUTPUT_DIR, FIPS_ZIP_OUT_FILENAME), index=False)
    zip_fips = zip_fips.reset_index(level=["fips", "zip"]).rename(
        columns={"pop": "weight"}
    )
    zip_fips = zip_fips[zip_fips["weight"] > 0.0]
    zip_fips.to_csv(join(OUTPUT_DIR, ZIP_FIPS_OUT_FILENAME), index=False)


def create_zip_hsa_hrr_crosswalk():
    """Creates the crosswalk table from ZIP to HSA and from ZIP to HRR from source."""
    zipped_csv = ZipFile(BytesIO(requests.get(ZIP_HSA_HRR_URL).content))
    zip_df = pd.read_csv(zipped_csv.open(ZIP_HSA_HRR_FILENAME))

    # Build the HSA table
    hsa_df = zip_df[["zipcode18", "hsanum"]].rename(
        columns={"zipcode18": "zip", "hsanum": "hsa"}
    )

    # Build the HRR table
    hrr_df = zip_df[["zipcode18", "hrrnum"]].rename(
        columns={"zipcode18": "zip", "hrrnum": "hrr"}
    )

    # Convert to zero-padded strings
    hrr_df["zip"] = hrr_df["zip"].astype(str).str.zfill(5)
    hrr_df["hrr"] = hrr_df["hrr"].astype(str)
    hsa_df["zip"] = hsa_df["zip"].astype(str).str.zfill(5)
    hsa_df["hsa"] = hsa_df["hsa"].astype(str)

    hsa_df.to_csv(join(OUTPUT_DIR, ZIP_HSA_OUT_FILENAME), index=False)
    hrr_df.to_csv(join(OUTPUT_DIR, ZIP_HRR_OUT_FILENAME), index=False)


def create_fips_msa_crosswalk():
    """Creates the crosswalk table from FIPS to MSA from source."""
    msa_cols = {
        "CBSA Code": int,
        "Metropolitan/Micropolitan Statistical Area": str,
        "FIPS State Code": str,
        "FIPS County Code": str,
    }
    # The following line requires the xlrd package.
    msa_df = pd.read_excel(
        FIPS_MSA_URL,
        skiprows=2,
        skipfooter=4,
        usecols=msa_cols.keys(),
        dtype=msa_cols,
    )

    metro_bool = (
        msa_df["Metropolitan/Micropolitan Statistical Area"]
        == "Metropolitan Statistical Area"
    )
    msa_df = msa_df[metro_bool]

    # Combine state and county codes into a single FIPS code
    msa_df["fips"] = msa_df["FIPS State Code"].str.cat(msa_df["FIPS County Code"])

    msa_df.rename(columns={"CBSA Code": "msa"})[["fips", "msa"]].to_csv(
        join(OUTPUT_DIR, FIPS_MSA_OUT_FILENAME), index=False
    )


def create_jhu_uid_fips_crosswalk():
    """Creates the crosswalk table from JHU UID to FIPS from source."""
    # These are hand modifications that need to be made to the translation
    # between JHU UID and FIPS. See below for the special cases information
    # https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/jhu-csse.html#geographical-exceptions
    hand_additions = pd.DataFrame(
        [
            # Split aggregation of Dukes and Nantucket, Massachusetts
            {
                "jhu_uid": "84070002",
                "fips": "25007",
                "weight": 16535 / (16535 + 10172),
            },  # Population: 16535
            {
                "jhu_uid": "84070002",
                "fips": "25019",
                "weight": 10172 / (16535 + 10172),
            },  # 10172
            # Kansas City, Missouri
            {
                "jhu_uid": "84070003",
                "fips": "29095",
                "weight": 674158 / 1084897,
            },  # Population: 674158
            {
                "jhu_uid": "84070003",
                "fips": "29165",
                "weight": 89322 / 1084897,
            },  # 89322
            {
                "jhu_uid": "84070003",
                "fips": "29037",
                "weight": 99478 / 1084897,
            },  # 99478
            {
                "jhu_uid": "84070003",
                "fips": "29047",
                "weight": 221939 / 1084897,
            },  # 221939
            # Kusilvak, Alaska
            {"jhu_uid": "84002158", "fips": "02270", "weight": 1.0},
            # Oglala Lakota
            {"jhu_uid": "84046102", "fips": "46113", "weight": 1.0},
            # Aggregate Utah territories into a "State FIPS"
            {"jhu_uid": "84070015", "fips": "49000", "weight": 1.0},
            {"jhu_uid": "84070016", "fips": "49000", "weight": 1.0},
            {"jhu_uid": "84070017", "fips": "49000", "weight": 1.0},
            {"jhu_uid": "84070018", "fips": "49000", "weight": 1.0},
            {"jhu_uid": "84070019", "fips": "49000", "weight": 1.0},
            {"jhu_uid": "84070020", "fips": "49000", "weight": 1.0},
        ]
    )
    unassigned_states = pd.DataFrame(
        [
            # Map the Unassigned category to a custom megaFIPS XX000
            {"jhu_uid": str(x), "fips": str(x)[-2:].ljust(5, "0"), "weight": 1.0}
            for x in range(84090001, 84090057)
        ]
    )
    out_of_state = pd.DataFrame(
        [
            # Map the Out of State category to a custom megaFIPS XX000
            {"jhu_uid": str(x), "fips": str(x)[-2:].ljust(5, "0"), "weight": 1.0}
            for x in range(84080001, 84080057)
        ]
    )
    puerto_rico_unassigned = pd.DataFrame(
        [
            # Map the Unassigned and Out of State categories to the cusom megaFIPS 72000
            {"jhu_uid": "63072888", "fips": "72000", "weight": 1.0},
            {"jhu_uid": "63072999", "fips": "72000", "weight": 1.0},
        ]
    )
    cruise_ships = pd.DataFrame(
        [
            {"jhu_uid": "84088888", "fips": "88888", "weight": 1.0},
            {"jhu_uid": "84099999", "fips": "99999", "weight": 1.0},
        ]
    )

    jhu_df = (
        pd.read_csv(JHU_FIPS_URL, dtype={"UID": str, "FIPS": str})
        .query("Country_Region == 'US'")[["UID", "FIPS"]]
        .rename(columns={"UID": "jhu_uid", "FIPS": "fips"})
        .dropna(subset=["fips"])
    )

    # FIPS Codes that are just two digits long should be zero filled on the right.
    # These are US state codes (XX) and the territories Guam (66), Northern Mariana Islands (69),
    # Virgin Islands (78), and Puerto Rico (72).
    fips_st = jhu_df["fips"].str.len() <= 2
    jhu_df.loc[fips_st, "fips"] = jhu_df.loc[fips_st, "fips"].str.ljust(5, "0")

    # Drop the JHU UIDs that were hand-modified
    dup_ind = jhu_df["jhu_uid"].isin(
        pd.concat(
            [hand_additions, unassigned_states, out_of_state, puerto_rico_unassigned, cruise_ships]
        )["jhu_uid"].values
    )
    jhu_df.drop(jhu_df.index[dup_ind], inplace=True)

    # Add weights of 1.0 to everything not in hand additions, then merge in hand-additions
    # Finally, zero fill FIPS
    jhu_df["weight"] = 1.0
    jhu_df = pd.concat(
        (
            jhu_df,
            hand_additions,
            unassigned_states,
            out_of_state,
            puerto_rico_unassigned,
        )
    )
    jhu_df["fips"] = jhu_df["fips"].astype(int).astype(str).str.zfill(5)
    jhu_df.to_csv(join(OUTPUT_DIR, JHU_FIPS_OUT_FILENAME), index=False)


def create_state_codes_crosswalk():
    """Create the State ID -> State Name -> State code crosswalk file."""
    df = (
        pd.read_csv(STATE_CODES_URL, delimiter="|")
        .drop(columns="STATENS")
        .rename(
            columns={
                "STATE": "state_code",
                "STUSAB": "state_id",
                "STATE_NAME": "state_name",
            }
        )
    )
    df["state_code"] = df["state_code"].astype(str).str.zfill(2)
    df["state_id"] = df["state_id"].astype(str).str.lower()

    # Add a few extra US state territories manually
    territories = pd.DataFrame(
        [
            {
                "state_code": 70,
                "state_name": "Republic of Palau",
                "state_id": "pw",
            },
            {
                "state_code": 68,
                "state_name": "Marshall Islands",
                "state_id": "mh",
            },
            {
                "state_code": 64,
                "state_name": "Federated States of Micronesia",
                "state_id": "fm",
            },
        ]
    )
    df = pd.concat((df, territories))

    df.to_csv(join(OUTPUT_DIR, STATE_OUT_FILENAME), index=False)


def create_state_hhs_crosswalk():
    """
    Create the state to hhs crosswalk.
    """
    if not isfile(join(OUTPUT_DIR, STATE_OUT_FILENAME)):
        create_state_codes_crosswalk()

    ss_df = pd.read_csv(
        join(OUTPUT_DIR, STATE_OUT_FILENAME),
        dtype={"state_code": str, "state_name": str, "state_id": str},
    )

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
    hhs_df = pd.DataFrame(hhs_state_pairs, columns=["hhs", "state_name"])
    hhs_df["hhs"] = hhs_df["hhs"].astype(str)

    (
        ss_df.merge(hhs_df, on="state_name", how="left")
        .dropna()[["state_code", "hhs"]]
        .to_csv(join(OUTPUT_DIR, STATE_HHS_OUT_FILENAME), index=False)
    )


def create_fips_population_table():
    """
    Build a table of populations by FIPS county codes. Uses US Census Bureau population
    data from 2019, supplemented with 2010 population data for Puerto Rico, and a few
    small counties.
    """
    census_pop = pd.read_csv(FIPS_POPULATION_URL, encoding="ISO-8859-1")
    census_pop["fips"] = census_pop.apply(
        lambda x: f"{x['STATE']:02d}{x['COUNTY']:03d}", axis=1
    )
    census_pop["pop"] = census_pop["POPESTIMATE2019"]
    census_pop = census_pop[["fips", "pop"]]
    census_pop = pd.concat(
        [
            census_pop,
            pd.DataFrame(
                {
                    "fips": ["70002", "70003"],
                    "pop": [0, 0],
                }
            ),
        ]
    )
    census_pop = census_pop.reset_index(drop=True)

    # Set population for Dukes and Nantucket
    dn_fips = "70002"
    dukes_fips = "25007"
    nantu_fips = "25019"

    census_pop.loc[census_pop["fips"] == dn_fips, "pop"] = (
        census_pop.loc[census_pop["fips"] == dukes_fips, "pop"].values
        + census_pop.loc[census_pop["fips"] == nantu_fips, "pop"].values
    )

    # Set population for Kansas City
    census_pop.loc[census_pop["fips"] == "70003", "pop"] = 491918  # via Google

    # Get the file with Puerto Rico populations
    df_pr = pd.read_csv(FIPS_PUERTO_RICO_POPULATION_URL)
    df_pr["fips"] = df_pr["STATE"].astype(str).str.zfill(2) + df_pr["COUNTY"].astype(
        str
    ).str.zfill(3)
    df_pr["pop"] = df_pr["POPPT"]
    df_pr = df_pr[["fips", "pop"]]
    # Create the Puerto Rico megaFIPS
    df_pr = df_pr[df_pr["fips"].isin([str(x) for x in range(72000, 72999)])]
    df_pr = pd.concat(
        [df_pr, pd.DataFrame([{"fips": "72000", "pop": df_pr["pop"].sum()}])]
    )

    # Fill the missing Puerto Rico data with 2010 information
    df_pr = df_pr.groupby("fips").sum().reset_index()
    df_pr = df_pr[~df_pr["fips"].isin(census_pop["fips"])]
    census_pop_pr = pd.concat([census_pop, df_pr])

    census_pop_pr.to_csv(join(OUTPUT_DIR, FIPS_POPULATION_OUT_FILENAME), index=False)


def derive_zip_population_table():
    """
    Builds a table of populations by ZIP code. Combines the tble of populations by
    FIPS code with the FIPS to ZIP code mapping.
    """
    if not isfile(join(OUTPUT_DIR, FIPS_POPULATION_OUT_FILENAME)):
        create_fips_population_table()

    if not isfile(join(OUTPUT_DIR, FIPS_ZIP_OUT_FILENAME)):
        create_fips_zip_crosswalk()

    census_pop = pd.read_csv(
        join(OUTPUT_DIR, FIPS_POPULATION_OUT_FILENAME), dtype={"fips": str, "pop": int}
    )
    fz_df = pd.read_csv(
        join(OUTPUT_DIR, FIPS_ZIP_OUT_FILENAME),
        dtype={"fips": str, "zip": str, "weight": float},
    )

    df = census_pop.merge(fz_df, on="fips", how="left")
    df["pop"] = df["pop"].multiply(df["weight"], axis=0)
    df = df.drop(columns=["fips", "weight"]).groupby("zip").sum().dropna().reset_index()
    df["pop"] = df["pop"].astype(int)
    df.to_csv(join(OUTPUT_DIR, ZIP_POPULATION_OUT_FILENAME), index=False)


def derive_fips_hrr_crosswalk():
    """Derives a crosswalk file from FIPS to HRR through FIPZ -> ZIP -> HRR
    from the crosswalk files made by the functions above."""
    if not isfile(join(OUTPUT_DIR, FIPS_ZIP_OUT_FILENAME)):
        create_fips_zip_crosswalk()

    if not isfile(join(OUTPUT_DIR, ZIP_HRR_OUT_FILENAME)):
        create_zip_hsa_hrr_crosswalk()

    fz_df = pd.read_csv(
        join(OUTPUT_DIR, FIPS_ZIP_OUT_FILENAME),
        dtype={"fips": str, "zip": str, "weight": float},
    )
    zh_df = pd.read_csv(
        join(OUTPUT_DIR, ZIP_HRR_OUT_FILENAME),
        dtype={"zip": str, "hrr": str},
    )

    (
        fz_df.merge(zh_df, on="zip", how="left")
        .drop(columns="zip")
        .groupby(["fips", "hrr"])
        .sum()
        .reset_index()
        .to_csv(join(OUTPUT_DIR, FIPS_HRR_OUT_FILENAME), index=False)
    )


def derive_fips_state_crosswalk():
    """
    Builds a crosswalk between FIPS county codes and state information (number,
    abbreviation, name).
    """
    fips_pop = pd.read_csv(
        join(OUTPUT_DIR, FIPS_POPULATION_OUT_FILENAME), dtype={"fips": str, "pop": int}
    )
    state_codes = pd.read_csv(
        join(OUTPUT_DIR, STATE_OUT_FILENAME),
        dtype={"state_code": str, "state_id": str, "state_name": str},
    )

    fips_pop["state_code"] = fips_pop["fips"].str[:2]
    (
        fips_pop.merge(state_codes, on="state_code", how="left")
        .drop(columns="pop")
        .to_csv(join(OUTPUT_DIR, FIPS_STATE_OUT_FILENAME), index=False)
    )


def derive_zip_msa_crosswalk():
    """
    Derives a crosswalk file from ZIP to MSA through ZIP -> FIPS -> HRR
    from the crosswalk files made by the functions above.
    """
    if not isfile(join(OUTPUT_DIR, ZIP_FIPS_OUT_FILENAME)):
        create_fips_zip_crosswalk()

    if not isfile(join(OUTPUT_DIR, FIPS_MSA_OUT_FILENAME)):
        create_fips_msa_crosswalk()

    zf_df = pd.read_csv(
        join(OUTPUT_DIR, ZIP_FIPS_OUT_FILENAME),
        dtype={"zip": str, "fips": str, "weight": float},
    )
    fm_df = pd.read_csv(
        join(OUTPUT_DIR, FIPS_MSA_OUT_FILENAME), dtype={"fips": str, "msa": str}
    )

    (
        zf_df.merge(fm_df, on="fips")
        .drop(columns="fips")
        .groupby(["msa", "zip"])
        .sum()
        .reset_index()
        .to_csv(join(OUTPUT_DIR, ZIP_MSA_OUT_FILENAME), index=False)
    )


def derive_zip_to_state_code():
    """
    Builds a crosswalk between ZIP codes and state information (number, abbreviation,
    name).
    """
    if not isfile(join(OUTPUT_DIR, STATE_OUT_FILENAME)):
        create_state_codes_crosswalk()
    if not isfile(join(OUTPUT_DIR, ZIP_FIPS_OUT_FILENAME)):
        create_fips_zip_crosswalk()

    sdf = pd.read_csv(
        join(OUTPUT_DIR, STATE_OUT_FILENAME),
        dtype={"state_code": str, "state_id": str, "state_name": str},
    )
    zf_cf = pd.read_csv(
        join(OUTPUT_DIR, ZIP_FIPS_OUT_FILENAME), dtype={"zip": str, "fips": str}
    )

    zf_cf["state_code"] = zf_cf["fips"].str[:2]
    (
        zf_cf.merge(sdf, left_on="state_code", right_on="state_code", how="left")
        .drop(columns=["fips"])
        .to_csv(join(OUTPUT_DIR, ZIP_STATE_CODE_OUT_FILENAME), index=False)
    )


def derive_fips_hhs_crosswalk():
    """
    Builds a crosswalk between FIPS county codes and HHS regions.
    """
    if not isfile(join(OUTPUT_DIR, STATE_HHS_OUT_FILENAME)):
        create_state_hhs_crosswalk()
    if not isfile(join(OUTPUT_DIR, FIPS_POPULATION_OUT_FILENAME)):
        create_fips_population_table()

    fips_pop = pd.read_csv(
        join(OUTPUT_DIR, FIPS_POPULATION_OUT_FILENAME), dtype={"fips": str, "pop": int}
    )
    state_hhs = pd.read_csv(
        join(OUTPUT_DIR, STATE_HHS_OUT_FILENAME),
        dtype={"state_code": str, "hhs": str},
    )

    fips_pop["state_code"] = fips_pop["fips"].str[:2]
    (
        fips_pop.merge(state_hhs, on="state_code", how="left")
        .drop(columns=["state_code", "pop"])
        .to_csv(join(OUTPUT_DIR, FIPS_HHS_FILENAME), index=False)
    )


def derive_zip_hhs_crosswalk():
    """
    Builds a crosswalk between zip code and HHS regions.
    """
    if not isfile(join(OUTPUT_DIR, STATE_HHS_OUT_FILENAME)):
        create_state_hhs_crosswalk()
    if not isfile(join(OUTPUT_DIR, ZIP_STATE_CODE_OUT_FILENAME)):
        derive_zip_to_state_code()

    zip_state = pd.read_csv(
        join(OUTPUT_DIR, ZIP_STATE_CODE_OUT_FILENAME),
        dtype={"zip": str, "pop": int, "state_code": str}
    )
    state_hhs = pd.read_csv(
        join(OUTPUT_DIR, STATE_HHS_OUT_FILENAME),
        dtype={"state_code": str, "hhs": str},
    )

    (
        zip_state.merge(state_hhs, on="state_code", how="left")
        .drop(columns=["state_code", "state_id", "state_name"])
        .to_csv(join(OUTPUT_DIR, ZIP_HHS_FILENAME), index=False)
    )


if __name__ == "__main__":
    create_fips_zip_crosswalk()
    create_zip_hsa_hrr_crosswalk()
    create_fips_msa_crosswalk()
    create_jhu_uid_fips_crosswalk()
    create_state_codes_crosswalk()
    create_state_hhs_crosswalk()
    create_fips_population_table()

    derive_fips_hrr_crosswalk()
    derive_zip_msa_crosswalk()
    derive_zip_to_state_code()
    derive_fips_state_crosswalk()
    derive_zip_population_table()
    derive_fips_hhs_crosswalk()
    derive_zip_hhs_crosswalk()