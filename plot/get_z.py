import sys
import numpy as np
import pandas as pd
import easyaccess as ea
import tools.des_tools as des
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from tools.des_tools import query_localdb, query_desdm


conn = ea.connect()
df_all = pd.DataFrame()

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
        # print(df[['SNID',
        #           'DLR',
        #           'PHOTOZ',
        #           'SPECZ',
        #           'SPECZ_CATALOG']])

        if df_all.shape[0] > 0:
            df_all = pd.concat([df_all, df])
        else:
            df_all = df.to_frame()

df_all.to_csv('/Users/szymon/Dropbox/SLSN_z.csv', index=False)
