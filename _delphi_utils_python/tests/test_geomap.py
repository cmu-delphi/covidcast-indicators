from delphi_utils.geomap import GeoMapper
import pandas as pd
import numpy as np


class TestGeoMapper:
    fips_data = pd.DataFrame({
        "fips":[1123,2340,98633,18181],
        "num": [2,0,20,10021],
        "den": [4,0,400,100001]
    })
    fips_data_2 = pd.DataFrame({
        "fips":[1123,2340,2002,18633,18181],
        "date":[pd.Timestamp('2018-01-01')]*5,
        "num": [2,1,20,np.nan,10021],
        "den": [4,1,400,np.nan,100001]
    })
    fips_data_3 = pd.DataFrame({
        "fips":[48059, 48253, 48441, 72003, 72005, 10999],
        "date": [pd.Timestamp('2018-01-01')]*3 + [pd.Timestamp('2018-01-03')]*3,
        "num": [1,2,3,4,8,5],
        "den": [2,4,7,11,100,10]
    })
    zip_data = pd.DataFrame({
        "zip":[45140,95616,95618]*2,
        "date": [pd.Timestamp('2018-01-01')]*3 + [pd.Timestamp('2018-01-03')]*3,
        "count": [99,345,456,100,344,442]
    })
    zip_data["total"] = zip_data["count"] * 2
    jan_month = pd.bdate_range('2018-01-01','2018-02-01')
    mega_data = pd.concat((
        pd.DataFrame({
        'fips': [1001]*len(jan_month),
        'date': jan_month,
        'count': np.arange(len(jan_month)),
        'visits': np.arange(len(jan_month)),
        }),
        pd.DataFrame({
            'fips': [1002]*len(jan_month),
            'date': jan_month,
            'count': np.arange(len(jan_month)),
            'visits': 2*np.arange(len(jan_month)),
        })))
    jhu_data = pd.DataFrame({
        "fips_jhu":[48059, 48253, 72005, 10999, 90010, 70002],
        "date": [pd.Timestamp('2018-01-01')]*3 + [pd.Timestamp('2018-01-03')]*3,
        "num": [1,2,3,4,8,5],
        "den": [2,4,7,11,100,10]
    })
    jhu_uid_data = pd.DataFrame({
        "jhu_uid":[84048315, 84048137, 84013299, 84013299, 84070002, 84000013, 84090002],
        "date": [pd.Timestamp('2018-01-01')]*3 + [pd.Timestamp('2018-01-03')]*3 + [pd.Timestamp('2018-01-01')],
        "num": [1,2,3,4,8,5,20],
        "den": [2,4,7,11,100,10,40]
    })


    def test_load_zip_fips_cross(self):
        gmpr = GeoMapper()
        gmpr.load_zip_fips_cross()
        fips_data = gmpr.zip_fips_cross
        assert (fips_data.groupby('zip').sum()['weight'] == 0).sum() == 144
        assert set(fips_data.columns) == set(['zip', 'fips', 'weight'])
        assert tuple(fips_data.dtypes) == (('object','object','float64'))

    def test_load_state_cross(self):
        gmpr = GeoMapper()
        gmpr.load_state_cross()
        state_data = gmpr.stcode_cross
        assert tuple(state_data.columns) == ('st_code', 'state_id', 'state_name')
        assert state_data.shape[0] == 52

    def test_load_fips_msa_cross(self):
        gmpr = GeoMapper()
        gmpr.load_fips_msa_cross()
        msa_data = gmpr.fips_msa_cross
        assert tuple(msa_data.columns) == ('fips','msa')

    def test_load_jhu_fips_cross(self):
        gmpr = GeoMapper()
        gmpr.load_jhu_fips_cross()
        jhu_data = gmpr.jhu_fips_cross
        assert (jhu_data.groupby('fips_jhu').sum() == 1).all()[0]

    def test_load_jhu_uid_fips_cross(self):
        gmpr = GeoMapper()
        gmpr.load_jhu_uid_fips_cross()
        jhu_data = gmpr.jhu_uid_fips_cross
        assert (jhu_data.groupby('jhu_uid').sum() == 1).all()[0]

    def test_load_zip_hrr_cross(self):
        gmpr = GeoMapper()
        gmpr.load_zip_hrr_cross()
        zip_data = gmpr.zip_hrr_cross
        assert pd.api.types.is_string_dtype(zip_data['zip'])
        assert pd.api.types.is_integer_dtype(zip_data['hrr'])

    def test_convert_intfips_to_str(self):
        gmpr = GeoMapper()
        new_data = gmpr.convert_intfips_to_str(self.fips_data)
        assert new_data['fips'].dtype=='O'
        assert new_data.shape == self.fips_data.shape

    def test_convert_fips_to_stcode(self):
        gmpr = GeoMapper()
        new_data = gmpr.convert_fips_to_stcode(self.fips_data)
        assert new_data['st_code'].dtype=='O'
        assert new_data.loc[1,"st_code"] == new_data.loc[1,"fips"][:2]

    def test_convert_stcode_to_state_id(self):
        gmpr = GeoMapper()
        new_data = gmpr.convert_fips_to_stcode(self.fips_data)
        new_data = gmpr.convert_stcode_to_state_id(new_data)
        assert new_data['state_id'].isnull()[2] == True
        assert new_data['state_id'][3] == 'IN'
        new_data = gmpr.convert_fips_to_stcode(self.fips_data)
        new_data = gmpr.convert_stcode_to_state_id(new_data,full=True)
        assert len(pd.unique(new_data['state_id'])) == 53

    def test_convert_fips_to_state_id(self):
        gmpr = GeoMapper()
        new_data = gmpr.convert_fips_to_state_id(self.fips_data)
        assert new_data['state_id'].isnull()[2] == True
        assert new_data['state_id'][3] == 'IN'

    def test_county_to_state(self):
        gmpr = GeoMapper()
        new_data = gmpr.county_to_state(self.fips_data_2)
        assert new_data.shape[0] == 3

    def test_convert_fips_to_msa(self):
        gmpr = GeoMapper()
        new_data = gmpr.convert_fips_to_msa(self.fips_data_3)
        assert new_data['msa'][2] == "10180"
        new_data = gmpr.convert_fips_to_msa(self.fips_data_3,create_mega=True)
        assert new_data['den'].sum() == self.fips_data_3['den'].sum()

    def test_county_to_msa(self):
        gmpr = GeoMapper()
        new_data = gmpr.county_to_msa(self.fips_data_3)
        assert new_data.shape[0] == 2
        new_data = gmpr.county_to_msa(self.fips_data_3, create_mega=True)
        assert new_data[['num']].sum()[0] == self.fips_data_3['num'].sum()

    def test_convert_zip_to_fips(self):
        gmpr = GeoMapper()
        new_data = gmpr.convert_zip_to_fips(self.zip_data)
        assert new_data.shape[0] == 12
        assert (new_data.groupby('zip').sum()['weight'] == 2).sum() == 3

    def test_zip_to_county(self):
        gmpr = GeoMapper()
        new_data = gmpr.zip_to_county(self.zip_data)
        assert new_data.shape[0] == 10
        assert (new_data[['count','total']].sum()-self.zip_data[['count','total']].sum()).sum() < 1e-3

    def test_megacounty(self):
        gmpr = GeoMapper()
        new_data = gmpr.county_to_megacounty(self.mega_data,6,50)
        assert (new_data[['count','visits']].sum()-self.mega_data[['count','visits']].sum()).sum() < 1e-3

    def test_convert_jhu_fips_to_mega(self):
        gmpr = GeoMapper()
        new_data = gmpr.convert_jhu_fips_to_mega(self.jhu_data)
        assert not (new_data['fips_jhu'].astype(int) > 90000).any()

    def test_convert_jhu_fips_to_fips(self):
        gmpr = GeoMapper()
        new_data = gmpr.convert_jhu_fips_to_fips(self.jhu_data)
        assert new_data.eval('weight * den').sum() == self.jhu_data['den'].sum()

    def test_jhu_fips_to_county(self):
        gmpr = GeoMapper()
        new_data = gmpr.jhu_fips_to_county(self.jhu_data)
        assert not (new_data['fips'].astype(int) > 90000).any()
        assert new_data['den'].sum() == self.jhu_data['den'].sum()

    def test_jhu_fips_to_state(self):
        gmpr = GeoMapper()
        new_data = gmpr.jhu_fips_to_state(self.jhu_data)
        assert new_data['state_id'][2] == 'DE'
        assert new_data.shape == (4,4)

    def test_jhu_fips_to_msa(self):
        gmpr = GeoMapper()
        new_data = gmpr.jhu_fips_to_msa(self.jhu_data)
        assert new_data.shape == (2,4)

    def test_convert_zip_to_hrr(self):
        gmpr = GeoMapper()
        new_data = gmpr.convert_zip_to_hrr(self.zip_data)
        assert len(pd.unique(new_data['hrr'])) == 2
        assert (new_data[['count','total']].sum()-self.zip_data[['count','total']].sum()).sum() < 1e-3

    def test_zip_to_hrr(self):
        gmpr = GeoMapper()
        new_data = gmpr.zip_to_hrr(self.zip_data)
        assert len(pd.unique(new_data['hrr'])) == 2
        assert (new_data[['count','total']].sum()-self.zip_data[['count','total']].sum()).sum() < 1e-3

    def test_convert_jhu_uid_to_fips(self):
        gmpr = GeoMapper()
        new_data = gmpr.convert_jhu_uid_to_fips(self.jhu_uid_data.astype({'jhu_uid':str}))

    def test_jhu_uid_to_county(self):
        gmpr = GeoMapper()
        new_data = gmpr.jhu_uid_to_county(self.jhu_uid_data)
        assert not (new_data['fips'].astype(int) > 90000).any()
        assert new_data['den'].sum() == self.jhu_uid_data['den'].sum()

    def test_convert_fips_to_zip(self):
        gmpr = GeoMapper()
        new_data = gmpr.convert_fips_to_zip(self.fips_data_3)
        # assert new_data.eval('weight * den').sum() == self.fips_data_3['den'].sum()

    def test_county_to_zip(self):
        gmpr = GeoMapper()
        new_data = gmpr.county_to_zip(self.fips_data_3)

    def test_county_to_hrr(self):
        gmpr = GeoMapper()
        new_data = gmpr.county_to_hrr(self.fips_data_3)
        assert new_data.shape == (2,4)