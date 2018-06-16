import sys
import numpy as np
import pandas as pd
import tools.des_tools as des
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from tools.des_tools import query_localdb

fig, ax = plt.subplots(1, 1, figsize=(20, 20))

for folder in ['SLSN_99']: #, 'SLSN_90', 'SLSN_50']:
    SLSN = np.loadtxt('/Users/szymon/Dropbox/'+folder+'.csv', unpack=True)
    engine = create_engine('postgresql://szymon:supernova@localhost:5432/thesis')

    for snid in SLSN:
        print(snid)
        query = "SELECT * FROM real_des_interp_46 WHERE snid="+str(snid)
        df = query_localdb(query)
        df['mag'] = 2.5 * np.log10(df['flux']) - 31.4

        season = df['season'][df['mag'] == df['mag'].min()].values[0]
        df = df[df['season'] == season]
        gdf = df.groupby('band')
        g = gdf.get_group('g')
        z = gdf.get_group('z')
        g_mag = g['mag'].values
        z_mag = z['mag'].values
        g_mask = g_mag < 26
        z_mask = z_mag < 26
        mask = g_mask * z_mask

        ax.plot(g_mag[mask] / z_mag[mask],
                g_mag[mask], c='black')

plt.savefig('/Users/szymon/Dropbox/Plots/SLSN_Colours.pdf', bbox_inches='tight')
