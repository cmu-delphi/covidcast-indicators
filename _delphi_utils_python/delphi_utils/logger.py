"""Temporary migration compatibility file.

Can be removed once this line

    https://github.com/cmu-delphi/delphi-epidata/blob/69835d1d7795eaf9a710d9f4903fef22a07e8fdf/src/client/delphi_epidata.py#L19

no longer imports from `delphi_utils.logger` directly.
"""

from delphi_logger import get_structured_logger  # pylint: disable=unused-import
