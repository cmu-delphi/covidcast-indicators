"""Tests for runner.py."""

import mock
import pytest

from delphi_utils.validator.report import ValidationReport
from delphi_utils.validator.errors import ValidationFailure
from delphi_utils.runner import run_indicator_pipeline


@pytest.fixture
def mock_indicator_fn():
    """Set up a mock indicator function."""
    yield mock.Mock()

@pytest.fixture
def mock_validator_fn():
    """Set up a mock validator function."""
    validator_fn = mock.Mock()
    validator = mock.Mock()
    validator_fn.return_value = validator
    validator.validate.return_value = ValidationReport([])
    yield validator_fn

@pytest.fixture
def mock_archiver_fn():
    """Set up a mock archiver function."""
    archiver_fn = mock.Mock()
    archiver = mock.Mock()
    archiver_fn.return_value = archiver
    yield archiver_fn


class TestRunIndicator:
    """Fixture for running indicators."""
    # arbitrary params to pass to function generators
    PARAMS = {
        "common": {},
        "indicator": {"a": 1},
        "validation": {"b": 2},
        "archive": {"c": 3}
    }

    @mock.patch("delphi_utils.runner.read_params")
    def test_full_run(self, mock_read_params,
                      mock_indicator_fn, mock_validator_fn, mock_archiver_fn):
        """Test that pipeline runs with validation and archiving."""
        mock_read_params.return_value = self.PARAMS

        run_indicator_pipeline(mock_indicator_fn, mock_validator_fn, mock_archiver_fn)

        mock_indicator_fn.assert_called_once_with(self.PARAMS)
        mock_validator_fn.assert_called_once_with(self.PARAMS)
        mock_archiver_fn.assert_called_once_with(self.PARAMS)
        mock_validator_fn.return_value.validate.assert_called_once()
        mock_archiver_fn.return_value.run.assert_called_once()

    @mock.patch("delphi_utils.runner.delete_move_files")
    @mock.patch("delphi_utils.runner.read_params")
    def test_failed_validation(self, mock_read_params, mock_delete_move_files,
                               mock_indicator_fn, mock_validator_fn, mock_archiver_fn):
        """Test that archiving is not called when validation fails."""
        mock_read_params.return_value = self.PARAMS
        report = mock_validator_fn.return_value.validate.return_value
        report.add_raised_error(ValidationFailure("", "2020-10-10", ""))

        run_indicator_pipeline(mock_indicator_fn, mock_validator_fn, mock_archiver_fn)

        mock_delete_move_files.assert_called_once()
        mock_indicator_fn.assert_called_once_with(self.PARAMS)
        mock_validator_fn.assert_called_once_with(self.PARAMS)
        mock_archiver_fn.assert_called_once_with(self.PARAMS)
        mock_validator_fn.return_value.validate.assert_called_once()
        mock_archiver_fn.return_value.run.assert_not_called()

    @mock.patch("delphi_utils.runner.read_params")
    def test_indicator_only(self, mock_read_params, mock_indicator_fn):
        """Test that pipeline runs without validation or archiving."""
        mock_read_params.return_value = self.PARAMS
        mock_null_validator_fn = mock.Mock(return_value=None)
        mock_null_archiver_fn = mock.Mock(return_value=None)

        run_indicator_pipeline(mock_indicator_fn, mock_null_validator_fn, mock_null_archiver_fn)

        mock_indicator_fn.assert_called_once_with(self.PARAMS)
        mock_null_validator_fn.assert_called_once_with(self.PARAMS)
        mock_null_archiver_fn.assert_called_once_with(self.PARAMS)

        # calling again with no extra arguments should not fail
        run_indicator_pipeline(mock_indicator_fn)

    @mock.patch("delphi_utils.runner.read_params")
    def test_no_validation(self, mock_read_params, mock_indicator_fn, mock_archiver_fn):
        """Test that pipeline run without validation."""
        mock_read_params.return_value = self.PARAMS

        run_indicator_pipeline(mock_indicator_fn, archiver_fn=mock_archiver_fn)

        mock_indicator_fn.assert_called_once_with(self.PARAMS)
        mock_archiver_fn.assert_called_once_with(self.PARAMS)
        mock_archiver_fn.return_value.run.assert_called_once()

    @mock.patch("delphi_utils.runner.read_params")
    def test_no_archive(self, mock_read_params, mock_indicator_fn, mock_validator_fn):
        """Test that pipeline runs without archiving."""
        mock_read_params.return_value = self.PARAMS

        run_indicator_pipeline(mock_indicator_fn, validator_fn=mock_validator_fn)

        mock_indicator_fn.assert_called_once_with(self.PARAMS)
        mock_validator_fn.assert_called_once_with(self.PARAMS)
        mock_validator_fn.return_value.validate.assert_called_once()
