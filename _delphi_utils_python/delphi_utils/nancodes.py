"""Unified not-a-number codes for CMU Delphi codebase."""

from enum import IntEnum

class Nans(IntEnum):
    """An enum of not-a-number codes for the indicators."""

    NOT_MISSING = 0
    NOT_APPLICABLE = 1
    REGION_EXCEPTION = 2
    CENSORED = 3
    DELETED = 4
    OTHER = 5
