# -*- coding: utf-8 -*-
"""
Custom validator exceptions.
"""


class APIDataFetchError(Exception):
    """Exception raised when reading API data goes wrong.

    Attributes:
        custom_msg -- parameters which caused the error
    """

    def __init__(self, custom_msg):
        self.custom_msg = custom_msg
        super().__init__(self.custom_msg)

    def __str__(self):
        return '{}'.format(self.custom_msg)


class ValidationError(Exception):
    """ Error raised when validation check fails. """

    def __init__(self, check_data_id, expression, message):
        """
        Arguments:
            - check_data_id: str or tuple/list of str uniquely identifying the
            check that was run and on what data
            - expression: relevant variables to message, e.g., if a date doesn't
            pass a check, provide the date
            - message: str explaining why an error was raised
        """
        self.check_data_id = (check_data_id,) if not isinstance(
            check_data_id, tuple) and not isinstance(check_data_id, list) else tuple(check_data_id)
        self.expression = expression
        self.message = message
        super().__init__(self.check_data_id, self.expression, self.message)
