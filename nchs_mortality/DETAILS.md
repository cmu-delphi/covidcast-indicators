# NCHS Mortality Data

We import the NCHS Mortality Data from CDC website and export
the state-level data as-is.  

In order to avoid confusing public consumers of the data, we maintain
consistency how USA Facts reports the data, please refer to [Exceptions](#Exceptions).

## Exceptions

At the State level, we report the data _exactly_ as NCHS reports their
mortality data, to prevent confusing public consumers of the data.
The visualization and modeling teams should take note of these exceptions.

### New York City

New York City is considered as a special state in the NCHS Mortality data,
but we don't consider NYC separately. The death counts for NYC would be included
 in New York State in our reports.

