# Delphi Python Utilities

This package provides various utilities used by the [Delphi group](https://delphi.cmu.edu/) at [Carnegie Mellon
University](https://www.cmu.edu) for its data pipelines and analyses.

Submodules:
- `archive`: Diffing and archiving CSV files.
- `export`: DataFrame to CSV export.
- `geomap`: Mappings between geographic resolutions.
- `logger`: Structured JSON logger.
- `nancodes`: Enum constants encoding not-a-number cases.
- `runner`: Orchestrator for running an indicator pipeline.
- `signal`: Indicator (signal) naming.
- `slack_notifier`:  Slack notification integration.
- `smooth`: Data smoothing functions.
- `utils`: JSON parameter interactions.
- `validator`: Data sanity checks and anomaly detection.


Source code can be found here:
[https://github.com/cmu-delphi/covidcast-indicators/](https://github.com/cmu-delphi/covidcast-indicators/)

## Logger Usage

To make our structured logging as useful as it can be, particularly within the context of how we use logs in Elastic, the `event` argument (typically the first unnamed arg) should be a static string (to make filtering easier), and each dynamic/varying value should be specified in an individual meaningfully- and consistently-named argument to the logger call (for use in filtering, thresholding, grouping, visualization, etc).

### Commonly used argument names:
- data_source
- geo_type
- signal
- issue_date
- filename

Single-thread usage.

```py
from delphi_utils.logger import get_structured_logger

logger = get_structured_logger('my_logger')
logger.info('Hello', name='World')
```

Multi-thread usage.

```py
from delphi_utils.logger import get_structured_logger, pool_and_threadedlogger

def f(x, threaded_logger):
    threaded_logger.info(f'x={x}')
    return x*x

logger = get_structured_logger('my_logger')
logger.info('Hello, world!')
with pool_and_threadedlogger(logger, n_cpu) as (pool, threaded_logger):
    for i in range(10):
        pool.apply_async(f, args=(i, threaded_logger))
```
