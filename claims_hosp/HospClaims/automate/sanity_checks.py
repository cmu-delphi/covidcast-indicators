"""Sanity check results from generating DV estimates.

Author: Maria Jahja
Created: 2020-05-12

Plotting code modified from: http://blog.marmakoide.org/?p=94
"""

# standard packages
import logging
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

# third party
import click
import matplotlib.dates as mpld
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages

# first party
EPIDATA_DIR = Path.home() / "Delphi/delphi-epidata/src/client"
FIPS_DIR = Path.home() / "Delphi/covid-19/doctor-visits/maria/data/fips_full.csv"
sys.path.append(str(EPIDATA_DIR))
from delphi_epidata import Epidata


class EMRHospChecks:
    DATE_FORMAT = mpld.DateFormatter('%m-%d')

    def __init__(self, data_path, level, se):
        self.level = level
        self.data = self.get_data(data_path, level, se)
        self.locs = list(sorted(set(self.data["adj"]["val"].keys()) | \
                                set(self.data["nadj"]["val"].keys())))
        self.se = se

        # read in geo file for fips
        self.geo = pd.read_csv(FIPS_DIR, dtype={"FIPS": int})
        self.geo.drop_duplicates('FIPS', inplace=True)

    @staticmethod
    def get_data(data_path, level, se):
        """
        Compile data values and dates for given data_path and geographic level

        Args:
          data_path: path to the data files
          level: geographic level to pull
          se: bool if se's are included in the file

        Returns:
          dictionary with data
        """

        def extract(all_files, all_dates):
            """Extract data from the files."""
            res = {"val": defaultdict(list),
                   "se": defaultdict(list),
                   "dates": defaultdict(list)}
            for f, d in zip(all_files, all_dates):
                df = pd.read_csv(open(f, "rb"), dtype={"geo_id": str}).to_numpy()
                for row in df:
                    geo = row[0]
                    res["val"][geo].append(row[1])
                    res["se"][geo].append(row[2])
                    res["dates"][geo].append(d)
            return res

        data = {"adj_files": [], "nadj_files": [], "dates": []}
        for f in sorted(data_path.glob("*")):
            name = f.name.split("_")
            if f.suffix == ".csv" and name[1] == level:
                name_idx = -2 if se else 3
                if name[name_idx] == "adj":
                    data["adj_files"].append(f)
                else:
                    data["nadj_files"].append(f)
                data["dates"].append(name[0])

        # extract data
        data["dates"] = sorted(list(set(data["dates"])))
        data["adj"] = extract(data["adj_files"], data["dates"])
        data["nadj"] = extract(data["nadj_files"], data["dates"])

        # convert dates
        data["dates"] = pd.to_datetime(data["dates"])
        data["first_date"] = data["dates"].min()
        data["last_date"] = data["dates"].max()
        data["first_plot_date"] = data["last_date"] - timedelta(days=30)
        data["epidata_date_range"] = Epidata.range(
            str(data["first_plot_date"].date()).replace('-', ''),
            str(data["last_date"].date()).replace('-', ''))

        return data

    def check_se_na(self):
        """
        Checks that all SE are reported as 'NA' due to
        privacy concerns from the company.

        Returns:
          true if pass, false otherwise
        """

        for kind in ["adj", "nadj"]:
            for geo, ses in self.data[kind]["se"].items():
                for se in ses:
                    if not np.isnan(se):
                        logging.error(f"{geo}, {se} not nan")
                        return False
        return True

    def check_range(self):
        """
        Checks that all percentages are within [0, 100].

        Returns:
          true if pass, false otherwise
        """
        for kind in ["adj", "nadj"]:
            for geo, vals in self.data[kind]["val"].items():
                for val in vals:
                    if not (0 <= val <= 100):
                        logging.error(f"{geo}, {val} not in [0, 100]")
                        return False
        return True

    def check_quantity(self):
        """Checks how many geographies were generated."""
        n_geos = {}
        logging.info(f"geographies generated for {self.level}")
        for kind in ["adj", "nadj"]:
            for geo, vals in self.data[kind]['val'].items():
                n_geos[geo] = len(vals)

            min_geo = np.min([v for k, v in n_geos.items()])
            max_geo = np.max([v for k, v in n_geos.items()])
            avg_geo = np.mean([v for k, v in n_geos.items()])
            std_geo = np.std([v for k, v in n_geos.items()])
            logging.info(f"\t{kind}"
                         f"\nmin:\t{min_geo}\nmax:\t{max_geo}"
                         f"\navg:\t{avg_geo:.2f}\nstd:\t{std_geo:.2f}")

    def get_filled_df(self, loc, kind):
        df = pd.DataFrame({"val": self.data[kind]["val"][loc]},
                          index=pd.to_datetime(self.data[kind]["dates"][loc]))

        if self.data["first_plot_date"] not in df.index:
            df = df.append(
                pd.DataFrame({"val": np.nan}, index=[self.data["first_plot_date"]]))
        if self.data["last_date"] not in df.index:
            df = df.append(pd.DataFrame({"val": np.nan}, index=[self.data["last_date"]]))
        df.sort_index(inplace=True)
        df = df.asfreq('D', fill_value=np.nan)
        return df[df.index > self.data["first_plot_date"]]

    def get_epidata_df(self, loc, kind):
        epi_kind = "smoothed_adj_covid19_from_claims" if kind == "adj" else "smoothed_covid19_from_claims"
        if self.level == "msa":
            loc = int(float(loc))

        rows = Epidata.covidcast("hospital-admissions", epi_kind, "day",
                                 self.level, self.data["epidata_date_range"], loc)
        vals = []
        obs_dates = []
        for row in rows['epidata']:
            vals.append(row['value'])
            obs_dates.append(row['time_value'])

        obs_dates = [datetime.strptime(str(d), "%Y%m%d") for d in obs_dates]
        df = pd.DataFrame({'date': obs_dates, 'val': vals})
        df = df.set_index('date')
        return df

    def get_county_name(self, fips_code):
        """Return name of a county given it's fips code."""
        loc = self.geo[self.geo["FIPS"] == fips_code]
        if len(loc) == 0:
            return fips_code
        return f'{loc["Name"].iloc[0]} County, {loc["State"].iloc[0]}'

    def plot(self, outname):
        """ Create PDF plots of the generated values by location.

        Args:
          outname: name for the output pdf file
        """

        # start pdf document
        pdf_pages = PdfPages(f'{outname}-{self.level}-hosp-claims-plots.pdf')
        n_plot = len(self.locs)
        n_plots_per_page = 25

        # init plotting axis and counter
        fig, axs = None, None
        j = 0

        for i, loc in enumerate(self.locs):

            # start new page if needed
            if i % n_plots_per_page == 0:
                fig, axs = plt.subplots(5, 5, figsize=(10, 10), sharex=True)
                axs = axs.ravel()
                j = 0

            # plot
            adj_ts = self.get_filled_df(loc, "adj")
            axs[j].plot(adj_ts.index, adj_ts["val"], label="New (Adj)", color="blue")

            if not self.se:
                nadj_ts = self.get_filled_df(loc, "nadj")
                axs[j].plot(nadj_ts.index, nadj_ts["val"], label="New", color="green")

            # current data. left unlabeled to clear clutter, but colors correspond to
            # the "new" lines. only plot first 52 cases (it's rather slow to run otherwise)
            if self.level == "state" or \
                    ((self.level == "county") and (loc in ["53033", "36061"])):
                try:
                    epi_adj_ts = self.get_epidata_df(loc, "adj")
                    axs[j].plot(epi_adj_ts.index, epi_adj_ts["val"],
                                color="lightskyblue", linestyle="--")
                    if not self.se:
                        epd_nadj_ts = self.get_epidata_df(loc, "nadj")
                        axs[j].plot(epd_nadj_ts.index, epd_nadj_ts["val"],
                                    color="lightgreen", linestyle="--")
                except:
                    logging.warning(f"could not retrieve {loc} in epidata, skipping")

            # set title
            if self.level == "county":
                axs[j].set_title(self.get_county_name(int(loc)), fontsize=10)
            else:
                axs[j].set_title(loc)

            # set legend and format
            if i == 0 or j == 0:
                axs[j].legend()

            axs[j].xaxis.set_major_formatter(self.DATE_FORMAT)
            axs[j].tick_params(axis='both', which='major', labelsize=5, labelrotation=90)

            # close the page if needed
            if (i + 1) % n_plots_per_page == 0 or (i + 1) == n_plot:
                plt.tight_layout()
                pdf_pages.savefig(fig)
                plt.close()
            j += 1

        pdf_pages.close()
        logging.info(f"plotted to '{outname}-{self.level}-hosp-claims-plots.pdf'")


def run(respath, geo, se, plot):
    """Run sanity checks and produce plots.

    Args:
      respath: path to result csvs
      geo: geo level, one of state, msa, hrr, county
      se: boolean whether data includes se or not
      plot: boolean whether to plot or not
    """
    assert geo in ["state", "msa", "hrr", "county"], f"{geo} is invalid"

    ehc = EMRHospChecks(Path(respath), geo, se)
    assert ehc.check_range(), "range failed"
    if not se:
        assert ehc.check_se_na(), "se is all na failed"
    ehc.check_quantity()
    if plot:
        ehc.plot(str(datetime.today().date()))
    logging.info("finished checks")


@click.command()
@click.argument('respath')
@click.argument('geo')
@click.option('--se', is_flag=True, default=False)
@click.option('--plot', '-p', is_flag=True, default=False)
def run_cli(respath, geo, se, plot):
    logging.basicConfig(level=logging.INFO)
    run(respath, geo, se, plot)


if __name__ == "__main__":
    run_cli()
