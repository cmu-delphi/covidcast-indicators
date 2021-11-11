"""Registry for variations."""
from collections.abc import Callable as function
from dataclasses import dataclass

URL_PREFIX = "https://healthdata.gov/api/views/gqxm-d9w9"
DOWNLOAD_ATTACHMENT = URL_PREFIX + "/files/{assetId}?download=true&filename={filename}"
DOWNLOAD_LISTING = URL_PREFIX + ".json"

@dataclass
class Transform:
    """Transformation filters for interpreting a particular sheet in the workbook."""

    name: str = None
    level: str = None
    row_filter: function = None
    geo_id_select: function = None
    geo_id_apply: function = None

T_FIRST = lambda df: df[df.columns[0]]
TRANSFORMS = {
    t.name: t for t in [
        Transform(
            name="Regions",
            level="hhs",
            geo_id_select=lambda df: df.index.to_series(),
            geo_id_apply=lambda x: x.replace("Region ", "")
        ),
        Transform(
            name="States",
            level="state",
            geo_id_select=T_FIRST,
            geo_id_apply=lambda x: x.lower()
        ),
        Transform(
            name="CBSAs",
            level="msa",
            row_filter=lambda df: df['CBSA type'] == "Metropolitan",
            geo_id_select=T_FIRST,
            geo_id_apply=lambda x: f"{x}"
        ),
        Transform(
            name="Counties",
            level="county",
            geo_id_select=T_FIRST,
            geo_id_apply=lambda x: f"{x:05}"
        )
    ]}

SIGNALS = [
    "total",
    "positivity"
]

def make_signal_name(key):
    """Convert a signal key to the corresponding signal name for the API."""
    return f"naats_{key}_7dav"
