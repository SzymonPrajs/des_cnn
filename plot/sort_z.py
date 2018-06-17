import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('/Users/szymon/Dropbox/SLSN_z.csv')
df = df.sort_values(['SNID', 'DLR'])

for folder in ['SLSN_99', 'SLSN_90', 'SLSN_50']:
    SLSN = np.loadtxt('/Users/szymon/Dropbox/'+folder+'.csv', unpack=True)
    engine = create_engine('postgresql://szymon:supernova@localhost:5432/thesis')

    for snid in SLSN:
        query = "SELECT * FROM sngals WHERE snid="+str(int(snid))
        df = conn.query_to_pandas(query)
        if not df.shape[0] > 0:
            continue

        # df = df.iloc[0]

        print(str(int(snid)))
        print(df['SNID'], df['SNID'], df['SNID'], df['SNID'], df['SNID']
        #           'DLR',
        #           'PHOTOZ',
        #           'SPECZ',
        #           'SPECZ_CATALOG']])


df_all.to_csv('/Users/szymon/Dropbox/SLSN_z.csv', index=False)
