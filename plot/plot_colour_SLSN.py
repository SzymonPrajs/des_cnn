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
    return (y - 21.5) / 1.75

for folder in ['SLSN_99', 'SLSN_90', 'SLSN_50']:
    SLSN = np.loadtxt('/Users/szymon/Dropbox/'+folder+'.csv', unpack=True)
    engine = create_engine('postgresql://szymon:supernova@localhost:5432/thesis')

    for snid in SLSN:
        # query = "SELECT * FROM real_des_interp_46 WHERE snid="+str(snid)
        # df = query_localdb(query)
        # df['mag'] = 31.4 - 2.5 * np.log10(df['flux'])
        df.from_csv('/Users/szymon/Dropbox/Plots/Data/'+str(int(snid))+'.csv', index=False)

        season = df['season'][df['mag'] == df['mag'].min()].values[0]
        df = df[df['season'] == season]
        gdf = df.groupby('band')
        g = gdf.get_group('g')
        z = gdf.get_group('z')
        g_mag = g['mag'].values
        z_mag = z['mag'].values
        g_mask = g_mag < 25
        z_mask = z_mag < 25
        mask = g_mask & z_mask

        col = ''
        if snid in slsn_list:
            ax.plot(g_mag[mask], z_mag[mask], c='red', lw=2)
        else:
            ax.plot(g_mag[mask], z_mag[mask], c='black', alpha=0.4)

        # if sum((g_mag[mask] - z_mag[mask]) > line(g_mag[mask])) > 0:
        #     print(snid)

# y = np.linspace(20, 25, 100)
# x = line(y)
# plt.plot(x, y, c='blue', lw=2)
plt.savefig('/Users/szymon/Dropbox/Plots/SLSN_Colours_2.pdf', bbox_inches='tight')
