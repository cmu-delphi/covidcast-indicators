# Dataset layout

The Data Strategy and Execution Workgroup (DSEW) publishes a Community Profile
Report each weekday, comprising a pair of files: an Excel workbook (.xlsx) and a
PDF which shows select metrics from the workbook as time series charts and
choropleth maps. These files are listed as attachments on the healthdata.gov
site:

https://healthdata.gov/Health/COVID-19-Community-Profile-Report/gqxm-d9w9

Each Excel file attachment has a filename. The filename contains a date,
presumably the publish date. The attachment also has an alphanumeric
assetId. Both the filename and the assetId are required for downloading the
file. Whether this means that updated versions of a particular file may be
uploaded by DSEW at later times is not known. The attachment does not explicitly
list an upload timestamp. To be safe, we cache our downloads using both the
assetId and the filename.

# Workbook layout

Each Excel file is a workbook with multiple sheets. The exemplar file used in
writing this indicator is "Community Profile Report 20211102.xlsx". The sheets
include:

- User Notes: Instructions for using the workbook
- Overview: US National figures for the last 5 weeks, plus monthly peaks back to
  April 2020
- Regions*: Figures for FEMA regions (double-checked: they match HHS regions
  except that FEMA 2 does not include Palau while HHS 2 does)
- States*: Figures for US states and territories
- CBSAs*: Figures for US Census Block Statistical Areas
- Counties*: Figures for US counties
- Weekly Transmission Categories: Lists of high, substantial, and moderate
  transmission states and territories
- National Peaks: Monthly national peaks back to April 2020
- National Historic: Daily national figures back to January 22 2020
- Data Notes: Source and methods information for all metrics
- Color Thresholds: Color-coding is used extensively in all sheets; these are
  the keys

The starred sheets above have nearly-identical column layouts, and together
cover the county, MSA, state, and HHS geographical levels used in
covidcast. Rather than aggregate them ourselves and risk a mismatch, this
indicator lifts these geographical aggregations directly from the corresponding
sheets of the workbook. 

GeoMapper _is_ used to generate national figures from
state, due to architectural differences between the starred sheets and the
Overview sheet. If we discover that our nation-level figures differ too much
from those listed in the Overview sheet, we can add dedicated parsing for the
Overview sheet and remove GeoMapper from this indicator altogether.

# Sheet layout

## Headers

Each starred sheet has two rows of headers. The first row uses merged cells to
group several columns together under a single "overheader". This overheader
often includes the reference period for that group of columns, such as:

- CASES/DEATHS: LAST WEEK (October 26-November 1)
- TESTING: LAST WEEK (October 24-30, Test Volume October 20-26)
- TESTING: PREVIOUS WEEK (October 17-23, Test Volume October 13-19)

Overheaders have changed periodically since the first report. For example, the
"TESTING: LAST WEEK" overheader above has also appeared as "VIRAL (RT-PCR) LAB
TESTING: LAST WEEK", with and without a separate reference date for Test
Volume. All known overheader forms are checked in test_pull.py.

The second row contains a header for each column. The headers uniquely identify
each column included in the sheet. Column headers include spaces, and typically
specify both the metric and the reference period over which it was calculated,
such as:

- Total NAATs - last 7 days (may be an underestimate due to delayed reporting)
- NAAT positivity rate - previous 7 days (may be an underestimate due to delayed
  reporting)

Columns headers have also changed periodically since the first report. For
example, the "Total NAATs - last 7 days" header above has also appeared as
"Total RT-PCR diagnostic tests - last 7 days".

## Contents

Each starred sheet contains test positivity and total test volume figures for
two reference periods, "last [week]" and "previous [week]". In some reports, the
reference periods for test positivity and total test volume are the same; in
others, they are different, such that the report contains figures for four
distinct reference periods, two for each metric we extract.

# Time series conversions and parsing notes

## Reference date

The reference period in the overheader never includes the year. We guess the
reference year by picking the same year as the publish date (i.e., the date
extracted from the filename), and if the reference month is greater than the
publish month, subtract 1 from the reference year. This adequately covers the
December-January boundary.

We select as reference date the end date of the reference period for each
metric. Reference periods are always 7 days, so this indicator produces
seven-day averages. We divide the total testing volume by seven and leave the
test positivity alone.

## Geo ID

The Counties sheet lists FIPS codes numerically, such that FIPS with a leading
zero only have four digits. We fix this by zero-filling to five characters.

MSAs are a subset of CBSAs. We fix this by selecting only CBSAs with type
"Metropolitan".

Most of the starred sheets have the geo id as the first non-index column. The
Region sheet has no such column. We fix this by generating the HHS ids from the
index column instead.

## Combining multiple reports

Each report file generates two reference dates for each metric, up to four
reference dates total. Since it's not clear whether new versions of past files
are ever made available, the default mode (params.indicator.reports="new")
fetches any files that are not already in the input cache, then combines the
results into a single data frame before exporting. This will generate correct
behavior should (for instance) a previously-downloaded file get a new assetId.

For the initial run on an empty input cache, and for runs configured to process
a range of reports (using params.indicator.reports=YYYY-mm-dd--YYYY-mm-dd), this
indicator makes no distinction between figures that came from different
reports. That may not be what you want. If the covidcast issue date needs to
match the date on the report filename, then the indicator must instead be run
repeatedly, with equal start and end dates, keeping the output of each run
separate.
