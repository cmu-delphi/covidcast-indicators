##
## Short script for comparing the files in the receiving directories
##

import pandas as pd
import os

rec_pattern = ""
# rec_pattern = "county_deaths_incidence_num"

def load_files(pattern = "", num = 1000):
    rec_dir = os.listdir('../receiving')
    suff = "stable"
    rec_stable_dir = os.listdir(f'../receiving_{suff}')
    rec_common = list(set(rec_dir) & set(rec_stable_dir))
    print(set(rec_dir).symmetric_difference(rec_stable_dir))
    num_iter = 0
    for rec in rec_common:
        if num_iter <= num:
            num_iter += 1
            df_rec = pd.read_csv(f'../receiving/{rec}').set_index('geo_id')
            df_stable = pd.read_csv(f'../receiving_{suff}/{rec}').set_index('geo_id')
            try:
                df_join = df_rec.join(df_stable, rsuffix='_stable' )
            except:
                print(df_rec.info())
                print(df_stable.info())
                assert False, f"failed join on {rec}"
            yield rec, df_join

def main():
    load_iter = load_files(rec_pattern)
    for rec, df in load_iter:
        if ('msa' in rec) and False:
            msa_ds = (df['val'] - df['val_stable']).sum()
            print(f'{msa_ds} value diff')
        if (df.eval('abs(val - val_stable)').sum() > 0.01):
            print(f'Printing {rec} difference')
            df_diff = df[df.eval('val != val_stable')]
            print(df_diff.shape)
            df_diff.to_csv(f'rec_diffs/diff_{rec}.csv')
            # assert "county_confirmed_7dav_incidence_num" not in rec, f"{rec}!!!"
            #input('w')

if __name__ == "__main__":
    main()
