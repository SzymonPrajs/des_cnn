import sys
import numpy as np
import pandas as pd
import tools.des_tools as des
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from tools.des_tools import query_localdb

fig, ax = plt.subplots(1, 1, figsize=(20, 20))
slsn_list = [1279517., 1279780., 1281880., 1290780., 1298914.,
             1320152., 1336197., 1340054., 1370309., 1372087.,
             1385229., 1562075.]

def line(y):
    return (y - 22.0) / 2

for folder in ['SLSN_99']: #, 'SLSN_90', 'SLSN_50']:
    SLSN = np.loadtxt('/Users/szymon/Dropbox/'+folder+'.csv', unpack=True)
    engine = create_engine('postgresql://szymon:supernova@localhost:5432/thesis')

    for snid in SLSN:
        query = "SELECT * FROM real_des_interp_46 WHERE snid="+str(snid)
        df = query_localdb(query)
        df['mag'] = 31.4 - 2.5 * np.log10(df['flux'])

        season = df['season'][df['mag'] == df['mag'].min()].values[0]
        df = df[df['season'] == season]
        gdf = df.groupby('band')
        g = gdf.get_group('g')
        r = gdf.get_group('r')
        g_mag = g['mag'].values
        r_mag = r['mag'].values
        g_mask = g_mag < 25
        r_mask = r_mag < 25
        mask = g_mask & r_mask

        col = ''
        if snid in slsn_list:
            col = 'blue'
        else:
            col='black'

        ax.plot(g_mag[mask] - r_mag[mask],
                g_mag[mask], c=col)

        if sum((g_mag[mask] - r_mag[mask]) > line(g_mag[mask]))) > 0:
            print(snid)

plt.savefig('/Users/szymon/Dropbox/Plots/SLSN_Colours.pdf', bbox_inches='tight')
