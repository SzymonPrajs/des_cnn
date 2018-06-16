import sys
import numpy as np
import pandas as pd
import tools.des_tools as des
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from tools.des_tools import query_localdb

fig, ax = plt.subplots(1, 1, figsize=(20, 20))

for folder in ['SLSN_99'] #, 'SLSN_90', 'SLSN_50']:
    SLSN = np.loadtxt('/Users/szymon/Dropbox/'+folder+'.csv', unpack=True)
    engine = create_engine('postgresql://szymon:supernova@localhost:5432/thesis')

    for snid in SLSN:
        print(snid)
        query = "SELECT * FROM real_des_interp_46 WHERE snid="+str(snid)
        df = query_localdb(query)

        season = df['season'][df['flux'] == df['flux'].max()].values[0]
        df = df[df['season'] == season]
        gdf = df.groupby('band')
        g = gdf.get_group('g')
        z = gdf.get_group('z')

        ax.plot(g['flux']/z['flux'], g['flux'], c='b')

plt.savefig('/Users/szymon/Dropbox/Plots/SLSN_Colours.pdf', bbox_inches='tight')
