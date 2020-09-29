# -*- coding: utf-8 -*-
"""
Custom validator exceptions.
"""

class APIDataFetchError(Exception):
    """Exception raised for errors during validation.

    Attributes:
        custom_msg -- parameters which caused the error
    """

    def __init__(self, custom_msg):
        self.custom_msg = custom_msg
        super().__init__(self.custom_msg)

    def __str__(self):
        return '{}'.format(self.custom_msg)
