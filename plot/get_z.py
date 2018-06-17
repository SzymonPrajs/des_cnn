import sys
import numpy as np
import pandas as pd
import easyaccess as ea
import tools.des_tools as des
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from tools.des_tools import query_localdb, query_desdm

conn = ea.connect()
for folder in ['SLSN_99', 'SLSN_90', 'SLSN_50']:
    SLSN = np.loadtxt('/Users/szymon/Dropbox/'+folder+'.csv', unpack=True)

    for snid in SLSN:
        query = "SELECT * FROM sngals WHERE snid="+str(int(snid))
        df = conn.query_to_pandas(query)
        if not df.shape[0] > 0:
            continue

        print(df['SNID'], df['DLR'], df['PHOTOZ'], df['SPECZ'], df['SPECZ_CATALOG']
