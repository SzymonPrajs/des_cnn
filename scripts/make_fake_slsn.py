"""
Make fake SLSN light curves.

Ingest fake SLSN light curves into a PSQL database after applying observing
logs and correcting for template offsets.
"""
import sqlite3
import datetime
import numpy as np
import pandas as pd
import psycopg2 as db
from lcsim.lcsim import LCSim
from pyMagnetar import Magnetar
from lcsim.simlib import SIMLIBReader
from sqlalchemy import create_engine
import tools.des_tools as dest


z_max = 3.0
run = 1
if __name__ == "__main__":
    conn = db.connect(host='localhost',
                      user='szymon',
                      password='supernova',
                      database='thesis')
    cur = conn.cursor()
    sql = 'postgresql://szymon:supernova@localhost:5432/thesis'
    engine = create_engine(sql)

    conn_sqlite = sqlite3.connect('/Users/szymon/Dropbox/Projects/DES/SLSN.db',
                                  detect_types=sqlite3.PARSE_DECLTYPES |
                                  sqlite3.PARSE_COLNAMES)
    cur_sqlite = conn_sqlite.cursor()

    query = """
    SELECT * FROM four_ops WHERE run_id=7 AND pass=1 and t0=0
    """
    cur_sqlite.execute(query)
    df_slsn = pd.DataFrame(cur_sqlite.fetchall(),
                           columns=np.array(cur_sqlite.description)[:, 0])

    if run == 1:
        df_slsn = df_slsn.query('index <= 50000')
    else:
        df_slsn = df_slsn.query('index > 50000')

    magnetar = Magnetar(b"/Users/szymon/Projects/pyMagnetar/filters")

    pdf = dest.get_sfr_z_pdf(z_max, 0.01)

    prop = []
    obs = []
    snid = run * 10000000
    printable = True
    for index, slsn in df_slsn.iterrows():
        lc = LCSim()
        simlib_path = '/Users/szymon/Dropbox/Projects/SigNS/'
        simlib = SIMLIBReader(simlib_path + 'DES_20170316.SIMLIB')

        for _ in range(10):
            snid += 1
            field, ccd = dest.random_field()
            z = dest.random_redshift_sfr(pdf, z_max)

            magnetar.setup(slsn['T_M'], slsn['B'], slsn['P'], 0.0, z)

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
                               'ccd': [],
                               'z': [],
                               't0': [],
                               'param_index': []
                               })

            t0 = dest.random_explosion_mjd(70)
            for flt in ['g', 'r', 'i', 'z']:
                obs = simlib.get_obslog(field, ccd, band=flt)
                mjd = obs['mjd'].values.astype(int)

                flux = np.array(magnetar.flux((obs['mjd'] - t0).values,
                                              str.encode("DES_" + flt)))
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
                temp_df['z'] = z
                temp_df['t0'] = t0
                temp_df['param_index'] = index
                temp_df['name'] = 'fake_slsn_'+str(snid)

                df = pd.concat((df, temp_df))

            detected = df[df['flux'] / df['fluxerr'] > 5]
            if detected.shape[0] > 2 and detected['band'].unique().size > 1:
                sorted_mjd = detected['mjd'].values
                sorted_mjd.sort()
                sorted_separation = np.diff(sorted_mjd)
                sorted_separation.sort()

                if sorted_separation.sum() > 0 and sorted_separation[0] < 30:
                    df.to_sql('slsn_5_realisations_2',
                              engine,
                              if_exists='append',
                              index=False)

        if index % 100 == 0:
            now = datetime.datetime.now()
            now = now.isoformat().split('T')[1].split('.')[0]
            if printable:
                print(now, '- Progress:', str(index)+'/'+str(df_slsn.shape[0]))
                printable = False

        elif not printable:
            printable = True

    conn.close()
