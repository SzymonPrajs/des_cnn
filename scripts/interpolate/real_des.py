import george
import numpy as np
import pandas as pd
import tools.des_tools as des
import scipy.optimize as opt
from sqlalchemy import create_engine
from george.kernels import Matern32Kernel


edges = pd.read_csv('/Users/szymon/Dropbox/Projects/DES/data/season_edge.csv')
engine = create_engine('postgresql://szymon:supernova@localhost:5432/thesis')


def ll(p, gp, df):
    gp.kernel[:] = p
    return -gp.lnlikelihood(df['flux'], quiet=True)


def grad_ll(p, gp, df):
    gp.kernel[:] = p
    return -gp.grad_lnlikelihood(df['flux'], quiet=True)


def gp_des_lc(data):
    df = pd.DataFrame({'mjd': [],
                       'flux': [],
                       'season': [],
                       'band': [],
                       'snid': []})

    gdf = data.groupby(('season', 'band'))
    for group, obs in gdf:
        mask = ((edges['season'] == group[0]) &
                (edges['field'] == obs['field'].values[0]))
        edge_query = edges[mask]
        min_edge = edge_query['min_mjd'].values[0]

        if (int(obs['mjd'].min()) > min_edge or
           int(obs['mjd'].max()) < (min_edge + 149)):
            print(min_edge, min_edge + 149)
            print(obs['mjd'].min(), obs['mjd'].max())
            return

        flux_norm = obs['flux'].mean()
        obs['flux'] /= flux_norm
        obs['fluxerr'] /= flux_norm

        if flux_norm == 0.0000:
            print('Flux avarage is 0')
            return

        gp = george.GP(Matern32Kernel(np.exp(10)))
        gp.compute(obs['mjd'], obs['fluxerr'])
        p0 = gp.kernel.vector
        opt.minimize(ll, p0, jac=grad_ll, args=(gp, obs))

        t = np.linspace(0, 149, 46) + min_edge
        try:
            mu, cov = gp.predict(obs['flux'], t)
        except:
            print('Could not interpolate LC')
            return

        mu *= flux_norm
        mu *= (10**1.56)
        temp_df = pd.DataFrame({'mjd': t, 'flux': mu})
        temp_df['season'] = group[0]
        temp_df['band'] = group[1]
        temp_df['snid'] = obs['snid'].values[0]

        df = pd.concat((df, temp_df))

    df.to_sql('real_des_interp_46', engine, if_exists='append', index=False)


query = "SELECT DISTINCT snid FROM real_des_obs_corr"
real_des = des.query_localdb(query)['snid'].values

query = "SELECT distinct(snid) FROM real_des_interp_46"
skip_id = des.query_localdb(query)['snid'].values

for i, snid in enumerate(real_des):
    print(i, snid)

    if snid in skip_id:
        print('Already in the database')
        continue

    query = "SELECT * FROM real_des_obs_corr WHERE snid={}".format(int(snid))
    data = des.query_localdb(query)

    if data is None:
        print('No data')
        continue
    data.dropna(inplace=True)

    try:
        gp_des_lc(data)
    except:
        continue
