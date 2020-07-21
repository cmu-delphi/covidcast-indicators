from dataclasses import dataclass
from typing import List

import pandas as pd

@dataclass
class Complaint:
    message: str
    data_source: str
    signal: str
    geo_types: List[str]
    last_updated: pd.Timestamp
    maintainers: List[str]

    def __str__(self):
        """Plain text string form of complaint."""

        return "{source}::{signal} ({geos}) {message}; last updated {updated}".format(
            source=self.data_source, signal=self.signal, geos=", ".join(self.geo_types),
            message=self.message, updated=self.last_updated.strftime("%Y-%m-%d"))

    def to_md(self):
        """Markdown formatted form of complaint."""

        return "*{source}* `{signal}` ({geos}) {message}; last updated {updated}.".format(
            source=self.data_source, signal=self.signal, geos=", ".join(self.geo_types),
            message=self.message, updated=self.last_updated.strftime("%Y-%m-%d"))

def check_source(data_source, meta, params):
    """Iterate over all signals from a source and check if they exceed max age."""

    source_config = params[data_source]

    signals = meta[meta.data_source == data_source]

    now = pd.Timestamp.now()

    complaints = {}

    for _, row in signals.iterrows():
        if "retired-signals" in source_config and \
           row["signal"] in source_config["retired-signals"]:
            continue

        age = (now - row["max_time"]).days

        if age > source_config["max_age"]:
            if row["signal"] not in complaints:
                complaints[row["signal"]] = Complaint(
                    "is more than {age} days old".format(age=age),
                    data_source,
                    row["signal"],
                    [row["geo_type"]],
                    row["max_time"],
                    source_config["maintainers"])
            else:
                complaints[row["signal"]].geo_types.append(row["geo_type"])

    return list(complaints.values())
