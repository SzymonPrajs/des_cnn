import sys
import numpy as np
import pandas as pd
import tools.des_tools as des
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from tools.des_tools import query_localdb, query_desdm

for folder in ['SLSN_99', 'SLSN_90', 'SLSN_50']:
    SLSN = np.loadtxt('/Users/szymon/Dropbox/'+folder+'.csv', unpack=True)
    engine = create_engine('postgresql://szymon:supernova@localhost:5432/thesis')

    for snid in SLSN:
        query = "SELECT * FROM sngals WHERE snid="+str(int(snid))
        df = query_desdm(query)

        print(str(int(snid)))
        print(df[['DLR', 'PHOTOZ', 'SPECZ', 'SPECZ_CATALOG']])
