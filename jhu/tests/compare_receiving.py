##
## Short script for comparing the files in the receiving directories
##

import pandas as pd
import os

def load_files():
    rec_dir = os.listdir('../receiving')
    rec_stable_dir = os.listdir('../receiving_stable')
    rec_common = set(rec_dir) & set(rec_stable_dir)
    for rec in rec_common:
        df_rec = pd.read_csv(f'../receiving/{rec}').set_index('geo_id')
        df_stable = pd.read_csv(f'../receiving_stable/{rec}').set_index('geo_id')
        try:
            df_join = df_rec.join(df_stable, rsuffix='_stable' )
        except:
            print(df_rec.info())
            print(df_stable.info())
            assert False, f"failed join on {rec}"
        yield rec, df_join

def main():
    load_iter = load_files()
    for rec, df in load_iter:
        if df.eval('abs(val - val_stable)').sum() > 0.01:
            print(f'Printing {rec} difference')
            # print(df.head())

main()