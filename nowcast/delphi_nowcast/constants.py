"""Registry for constants."""
import datetime
from functools import partial

import numpy as np

from .data_containers import SensorConfig
from .deconvolution import deconvolution
from .nowcast_fusion import covariance

# todo: update tests once placeholders are replaced

# Ground truth parameters
GROUND_TRUTH_INDICATOR = SensorConfig("placeholder", "placeholder", "placeholder", 0)

# Delay distribution
DELAY_DISTRIBUTION = []

# Deconvolution parameters
FIT_FUNC = "placeholder"

# AR Sensor parameters
AR_ORDER = 3
AR_LAMBDA = 0.1

# Regression Sensor parameters
REG_SENSORS = [SensorConfig("placeholder", "placeholder", "placeholder", 0), ]
REG_INTERCEPT = True


class Default:
    # Starting date of training data
    FIRST_DATA_DATE = datetime.date(2020, 5, 1)

    # Ground truth parameters
    GROUND_TRUTH_INDICATOR = SensorConfig('jhu-csse',
                                          'confirmed_incidence_prop',
                                          'test_truth',
                                          1)

    # Delay distribution
    DELAY_DISTRIBUTION = [0.06879406, 0.08521725, 0.08916559, 0.08706704, 0.08185865,
                          0.07514035, 0.06784274, 0.06051728, 0.05348628, 0.04692885,
                          0.04093326, 0.03553024, 0.03071456, 0.02645928, 0.02272524,
                          0.01946721, 0.01663802, 0.01419111, 0.01208213, 0.01026984,
                          0.00871658, 0.00738839, 0.00625499, 0.00528958, 0.00446862,
                          0.00377153, 0.00318041, 0.00267979, 0.00225628, 0.00189837,
                          0.00159619, 0.0013413, 0.00112646, 0.00094553, 0.00079325,
                          0.00066518, 0.00055753, 0.0004671, 0.00039119, 0.00032748,
                          0.00027405, 0.00022925, 0.00019172, 0.00016028]

    # Deconvolution parameters
    DECONV_CV_LAMBDA_GRID = np.logspace(1, 3.5, 10)
    DECONV_CV_GAMMA_GRID = np.r_[np.logspace(0, 0.2, 6) - 1, [1, 5, 10, 50]]
    DECONV_FIT_FUNC = partial(deconvolution.deconvolve_double_smooth_tf_cv,
                              k=3,
                              fit_func=deconvolution.deconvolve_double_smooth_tf_fast,
                              lam_cv_grid=DECONV_CV_LAMBDA_GRID,
                              gam_cv_grid=DECONV_CV_GAMMA_GRID)

    # AR Sensor parameters
    AR_ORDER = 3
    AR_LAMBDA = 0.5

    # Regression Sensor parameters
    REG_SENSORS = [SensorConfig('doctor-visits', 'smoothed_adj_cli', 'dv', 5),
                   SensorConfig('fb-survey', 'smoothed_hh_cmnty_cli', 'fb', 3),
                   ]

    REG_INTERCEPT = True

    # Sensor fusion parameters
    COVARIANCE_SHRINKAGE_FUNC = covariance.BlendDiagonal2
