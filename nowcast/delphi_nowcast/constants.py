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
    GROUND_TRUTH_INDICATOR = SensorConfig('usa-facts',
                                          'confirmed_cumulative_prop',
                                          'test_truth',
                                          3)

    # Delay distribution
    DELAY_DISTRIBUTION = [0.10433699530360944, 0.1164050460454959,
                          0.11263783954722739, 0.10282199376456386,
                          0.09091150093659907, 0.07877333969057407,
                          0.06731098615392934, 0.05693268942518374,
                          0.04778077926102103, 0.03985430495478942,
                          0.033077867254940246, 0.027341126390615762,
                          0.022521524455583707, 0.018497107908338244,
                          0.015153420501967751, 0.012386833763592104,
                          0.010105757496047115, 0.00823062036514499,
                          0.0066931720930924424, 0.0054354476113001255,
                          0.0044086003488996986, 0.003571727414852407,
                          0.002890755960943001, 0.0023374263865391246,
                          0.0018883872165984359, 0.0015244039186347949,
                          0.001229676677676723, 0.0009912582963990607,
                          0.0007985616696101179, 0.0006429458723861884,
                          0.000517370261025661, 0.00041610676780554323,
                          0.0003345015528110231, 0.0002687782211213798,
                          0.00021587583877147285, 0.00017331593958353224,
                          0.00013909358378928944, 0.0001115882995705128,
                          8.949141039108261e-05, 7.174682959399864e-05,
                          5.750289722848023e-05, 4.6073251649411315e-05,
                          3.690507947765323e-05, 2.9553381025720584e-05]

    # Deconvolution parameters
    DECONV_CV_GRID = np.logspace(1, 3.5, 10)
    DECONV_CV_N_FOLDS = 10
    DECONV_FIT_FUNC = partial(deconvolution.deconvolve_tf_cv,
                              cv_grid=DECONV_CV_GRID,
                              n_folds=DECONV_CV_N_FOLDS)

    # AR Sensor parameters
    AR_ORDER = 3
    AR_LAMBDA = 0.1

    # Regression Sensor parameters
    REG_SENSORS = [SensorConfig('doctor-visits', 'smoothed_adj_cli', 'dv', 5),
                   SensorConfig('fb-survey', 'smoothed_hh_cmnty_cli', 'fb', 3), ]
    REG_INTERCEPT = True

    # Sensor fusion parameters
    COVARIANCE_SHRINKAGE_FUNC = covariance.BlendDiagonal2