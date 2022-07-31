# -*- coding: utf-8 -*-
"""Module to flagging interesting or unusual data points

This file defines the functions that are made public by the module. As the
module is intended to be executed though the main method, these are primarily
for testing.
"""
from __future__ import absolute_import

from .generate_ar import ar_results, calculate_report_flags
from .generate_reference import identify_correct_spikes, weekend_corr
from .flag_io import flagger_df, flagger_io, rel_files_table
