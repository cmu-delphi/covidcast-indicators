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
                                          'confirmed_incidence_prop',
                                          'test_truth',
                                          3)

    # Delay distribution
    DELAY_DISTRIBUTION = [0.18551895697233228, 0.1941066495086408,
                          0.16520501934031295, 0.12920924392117733,
                          0.09648227738507063, 0.06996445615525929,
                          0.04971948655412928, 0.03481359416493926,
                          0.0241025167140563, 0.01653879544426984,
                          0.011267114592750368, 0.0076301810825091035,
                          0.005141441201879286, 0.003449729717977421,
                          0.0023061678373436944, 0.0015367681453701096,
                          0.00102118832856722, 0.0006768975353013901,
                          0.0004476890903554279, 0.0002955054319390296,
                          0.0001947037026011559, 0.0001280782416366335,
                          8.412630941129853e-05, 5.518214051706147e-05,
                          3.615127673997327e-05, 2.365645278258695e-05,
                          1.546372715707464e-05, 1.0098360559143073e-05,
                          6.588548430892157e-06, 4.294946581300064e-06,
                          2.7975590958413676e-06, 1.8208587521310088e-06,
                          1.18431902913557e-06, 7.697944990377115e-07,
                          5.000473723154425e-07, 3.2463321002673375e-07,
                          2.1063674939459064e-07, 1.3659901800571488e-07,
                          8.854113399735155e-08, 5.736375694691623e-08,
                          3.714798173147707e-08, 2.4046251305384954e-08,
                          1.55590397896853e-08, 1.0063512268054604e-08]

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
                   SensorConfig('fb-survey', 'smoothed_hh_cmnty_cli', 'fb', 3),
                   ]

    REG_INTERCEPT = True

    # Sensor fusion parameters
    COVARIANCE_SHRINKAGE_FUNC = covariance.BlendDiagonal2
