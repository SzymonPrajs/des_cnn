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

snii_list = [1251622, 1279573, 1295744, 1315968, 1502119, 1255005,
             1279665, 1297218, 1334244, 1532068, 1257895, 1281640,
             1300562, 1334397, 1537093, 1258194, 1288107, 1302627,
             1334551, 1576032, 1258210, 1290108, 1306147, 1336197,
             1576128, 1260282, 1291019, 1309222, 1368286, 1592088,
             1278517, 1294979, 1309600, 1371110, 1633048, 1279503,
             1295519, 1310406, 1433017]

def line(y):
    return (y - 21.5) / 1.75

for folder in ['SLSN_99', 'SLSN_90', 'SLSN_50']:
    SLSN = np.loadtxt('/Users/szymon/Dropbox/'+folder+'.csv', unpack=True)
    engine = create_engine('postgresql://szymon:supernova@localhost:5432/thesis')

    for snid in SLSN:
        # query = "SELECT * FROM real_des_interp_46 WHERE snid="+str(snid)
        # df = query_localdb(query)
        # df['mag'] = 31.4 - 2.5 * np.log10(df['flux'])
        df = pd.read_csv('/Users/szymon/Dropbox/Plots/Data/'+str(int(snid))+'.csv')

        season = df['season'][df['mag'] == df['mag'].min()].values[0]
        df = df[df['season'] == season]
        gdf = df.groupby('band')
        g = gdf.get_group('g')
        r = gdf.get_group('r')
        i = gdf.get_group('i')
        z = gdf.get_group('z')

        g_mag = g['mag'].values
        r_mag = r['mag'].values
        i_mag = i['mag'].values
        z_mag = z['mag'].values

        g_mask = g_mag < 27
        r_mask = r_mag < 27
        i_mask = i_mag < 27
        z_mask = z_mag < 27

        mask = g_mask & r_mask & i_mask & z_mask

        col = ''
        if snid in slsn_list:
            ax.plot(r_mag[mask] - i_mag[mask],
                    z_mag[mask],
                    c='red', lw=3)

        elif snid in snii_list:
            ax.plot(r_mag[mask] - i_mag[mask],
                    z_mag[mask],
                    c='green', lw=3)
        else:
            ax.plot(g_mag[mask] - i_mag[mask],
                    z_mag[mask],
                    c='black', alpha=0.2)

        # if sum((g_mag[mask] - z_mag[mask]) > line(g_mag[mask])) > 0:
        #     print(snid)

# y = np.linspace(20, 25, 100)
# x = line(y)
# plt.plot(x, y, c='blue', lw=2)
plt.savefig('/Users/szymon/Dropbox/Plots/SLSN_Colours_all.pdf', bbox_inches='tight')
