# standard
from unittest.mock import Mock
from pathlib import Path

import pandas as pd

# third party
from delphi_doctor_visits.modify_claims_drops import (modify_and_write)


class TestDropsModification:
    
    def test_modify_and_write(self):
        data_path = Path('./test_data/SYNEDI_AGG_OUTPATIENT_07022020_1455CDT.csv.gz')
        logger = Mock()
        df = modify_and_write(data_path, logger, test_mode=True)
        expected_colnames = ['PatCountyFIPS', 'Pat HRR Name', 'Pat HRR ID', 'PatAgeGroup']
        
        test_df = pd.read_csv(data_path, dtype={"PatCountyFIPS": str,
                                "patCountyFIPS": str})
        
        assert set(expected_colnames).issubset(set(df.columns))
        assert df.shape[0] == test_df.shape[0]
        assert df.shape[1] == test_df.shape[1]
        assert df.shape[1] == 10
