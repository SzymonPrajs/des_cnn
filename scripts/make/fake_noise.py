"""
Make fake SNIb/c light curves.

Ingest fake CCSN light curves into a PSQL database after applying observing
logs and correcting for template offsets.
"""
import datetime
import numpy as np
import pandas as pd
import psycopg2 as db
import pycoco as pcc
import pyCoCo as pccsim

import lsst_tools as lsstt
from lcsim.simlib import SIMLIBReader
from lcsim.lcsim import LCSim

from sqlalchemy import create_engine
import tools.des_tools as dest

run = 1
n_sne_req = 100000
if __name__ == "__main__":
    conn = db.connect(host='localhost',
                      user='szymon',
                      password='supernova',
                      database='thesis')
    cur = conn.cursor()
    sql = 'postgresql://szymon:supernova@localhost:5432/thesis'
    engine = create_engine(sql)

    lc = LCSim()
    simlib_path = '/Users/szymon/Dropbox/Projects/SigNS/'
    simlib = SIMLIBReader(simlib_path + 'DES_20170316.SIMLIB')

    filter_names = ["DES_g", "DES_r", "DES_i", "DES_z"]
    zp_dict = {}
    for flt in filter_names:
        zp_dict[flt] = pcc.kcorr.calc_AB_zp(flt)

    n_sne = 0
    snid = 0
    printable = True
    while n_sne < n_sne_req:
        snid += 1
        field, ccd = dest.random_field()

        df = pd.DataFrame({'snid': [],
                           'name': [],
                           'mjd': [],
                           'band': [],
                           'field': [],
                           'flux': [],
                           'fluxerr': [],
                           'zp': [],
                           'psf': [],
                           'skysig': [],
                           'skysig_t': [],
                           'gain': [],
                           'season': [],
                           'status': [],
                           'ccd': []
                           })

        obs = simlib.get_obslog(field, ccd)
        mjd_sim = np.random.choice(obs['mjd'].values)
        mag = 3*np.random.random() + 18

        for flt in ['g', 'r', 'i', 'z']:
            obs = simlib.get_obslog(field, ccd, band=flt)
            idx = np.argmin(np.abs(obs['mjd'].values - mjd_sim))

            flux = obs['mjd'].values * 0
            if np.abs(obs['mjd'].values[idx] - mjd_sim) < 3:
                flux[idx] += 10**((31.4 - mag) / 2.5)

            if np.random.random() <= 0.25:
                diff = obs['mjd'].values - mjd_sim
                idx_2 = np.where((diff > 0) & (diff < 30))
                if idx_2[0].size > 0:
                    mjd_2_sim = np.random.choice(obs['mjd'].values[idx_2])
                    idx_3 = np.argmin(np.abs(obs['mjd'].values - mjd_2_sim))
                    flux[idx_3] += 10**((31.4 - mag) / 2.5)

            fluxcal, errcal = lc.simulate(flux, obs)

            temp_df = pd.DataFrame()
            temp_df['mjd'] = obs['mjd']
            temp_df['band'] = obs['flt']
            temp_df['field'] = field
            temp_df['flux'] = fluxcal
            temp_df['fluxerr'] = errcal
            temp_df['zp'] = obs['zps']
            temp_df['psf'] = obs['psf1']
            temp_df['skysig'] = obs['skysigs']
            temp_df['skysig_t'] = obs['skysigt']
            temp_df['gain'] = obs['gain']
            temp_df['season'] = temp_df['mjd'].map(dest.mjd_to_season)
            temp_df['status'] = 1
            temp_df['ccd'] = ccd
            temp_df['snid'] = snid
            temp_df['name'] = 'fake_noise_' + str(snid)

            df = pd.concat((df, temp_df))

        detected = df[df['flux'] / df['fluxerr'] > 5]
        if detected.shape[0] > 2 and detected['band'].unique().size > 1:
            sorted_mjd = detected['mjd'].values
            sorted_mjd.sort()
            sorted_separation = np.diff(sorted_mjd)
            sorted_separation.sort()

            if sorted_separation.sum() == 0:
                print(sorted_mjd)

            if sorted_separation.sum() > 0 and sorted_separation[0] < 30:
                n_sne += 1
                df.to_sql('fake_noise',
                          engine,
                          if_exists='append',
                          index=False)

        if n_sne % 100 == 0:
            now = datetime.datetime.now()
            now = now.isoformat().split('T')[1].split('.')[0]
            if printable:
                print(now, '- Progress:', str(n_sne) + '/' + str(n_sne_req))
                printable = False

        elif not printable:
            printable = True

    conn.close()
