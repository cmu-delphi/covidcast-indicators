"""Registry for constants."""
from .data_containers import SensorConfig


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
REG_SENSORS = [SensorConfig("placeholder", "placeholder", "placeholder", 0),]
REG_INTERCEPT = True
