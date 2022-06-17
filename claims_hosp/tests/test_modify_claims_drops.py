# standard
from unittest.mock import Mock
from pathlib import Path

# third party
from delphi_claims_hosp.modify_claims_drops import (modify_and_write)


class TestDropsModification:
    
    def test_modify_and_write(self):
        data_path = "./test_data/"
        logger = Mock()
        files, dfs_list = modify_and_write(data_path, logger, force=False)
        expected_colnames = ['PatCountyFIPS', 'Pat HRR Name', 'Pat HRR ID', 'PatAgeGroup']
        assert len(files) == 1
        assert len(dfs_list) == 1
        assert files[0] == Path('./test_data/SYNEDI_AGG_INPATIENT_11062020_1451CDT.csv.gz')
        assert set(expected_colnames).issubset(set(dfs_list[0].columns))
