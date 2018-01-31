import george
import numpy as np
import pandas as pd
import tools.plot_des as plot
import tools.des_tools as des
import matplotlib.pyplot as plt
import scipy.optimize as opt
from sqlalchemy import create_engine
from george.kernels import Matern32Kernel


edges = pd.read_csv('/Users/szymon/Dropbox/Projects/DES/data/season_edge.csv')
engine = create_engine('postgresql://szymon:supernova@localhost:5432/des')


def ll(p, gp, df):
    gp.kernel[:] = p
    return -gp.lnlikelihood(df['flux'], quiet=True)


def grad_ll(p, gp, df):
    gp.kernel[:] = p
    return -gp.grad_lnlikelihood(df['flux'], quiet=True)


def gp_des_lc(data):
    df = pd.DataFrame({'mjd': [], 'flux': [], 'season': [], 'band': [], 'snid': []})

    gdf = data.groupby(('season', 'band'))
    for group, obs in gdf:
        edge_query = edges.query('season == {} and field == "{}"'.format(group[0], obs['field'].values[0]))
        min_edge = edge_query['min_mjd'].values[0]

        if (obs['mjd'].min() > (min_edge + 2)) or ((min_edge + 147) > obs['mjd'].max()):
            print(min_edge + 2, min_edge + 147)
            print(obs['mjd'].min(), obs['mjd'].max())
            print(obs['snid'].values[0])
            return

        flux_norm = obs['flux'].mean()
        obs['flux'] /= flux_norm
        obs['fluxerr'] /= flux_norm

        if flux_norm == 0.0000:
            return

        gp = george.GP(Matern32Kernel(np.exp(10)))
        gp.compute(obs['mjd'], obs['fluxerr'])
        p0 = gp.kernel.vector
        opt.minimize(ll, p0, jac=grad_ll, args=(gp, obs))

        t = np.linspace(0, 149, 23) + min_edge
        mu, cov = gp.predict(obs['flux'], t)
        std = np.sqrt(np.diag(cov))

        mu *= flux_norm
        temp_df = pd.DataFrame({'mjd': t, 'flux': mu})
        temp_df['season'] = group[0]
        temp_df['band'] = group[1]
        temp_df['snid'] = obs['snid'].values[0]

        df = pd.concat((df, temp_df))

    df.to_sql('real_non_ia_interp', engine, if_exists='append', index=False)

# query = "SELECT SNID FROM SNCAND WHERE spec_type='SNIa'"
# Ia = des.query_desdm(query)['SNID'].values

# query = "SELECT snid FROM SNCAND WHERE spec_type!='SNIa' and spec_type!='none'"
# non_Ia = des.query_desdm(query)['SNID'].values


# query = "SELECT DISTINCT snid FROM agn_test"
# fake_ia = des.query_localdb(query)['snid'].values
#
# for i, snid in enumerate(non_Ia):
#     query = "SELECT * FROM des_obs WHERE snid={}".format(int(snid))
#     data = des.query_localdb(query)
#
#     if data is None:
#         continue
#
#     data.dropna(inplace=True)
#
#     print(i, snid)
#     gp_des_lc(data)

query = "SELECT * FROM des_obs WHERE snid=1370624"
data = des.query_localdb(query)
gp_des_lc(data)
