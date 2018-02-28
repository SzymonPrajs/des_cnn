"""
Make fake SLSN light curves and ingest into a PSQL database after
applying observing logs and correcting for template offsets
"""
import os
import sqlite3
import datetime
import numpy as np
import pandas as pd
import scipy.io as io
import psycopg2 as db
from lcsim.lcsim import LCSim
from pyMagnetar import Magnetar
from lcsim.simlib import SIMLIBReader
from sqlalchemy import create_engine
from tools.des_tools import random_field, random_redshift, mjd_to_season


def create_tables(cursor):
    # drop object properties table
    drop_props_table = "DROP TABLE IF EXISTS slsn_props"
    cursor.execute(drop_props_table)

    # create new object properties table
    create_props_table = """
    CREATE TABLE slsn_props (
        snid INTEGER,
        name VARCHAR(12),
        fake SMALLINT,
        field CHAR(2),
        ccd SMALLINT,
        num_detected SMALLINT,
        specz REAL,
        photz REAL,
        specz_err REAL,
        gal_mag_g REAL,
        gal_mag_r REAL,
        gal_mag_i REAL,
        gal_mag_z REAL,
        sb_flux_g REAL,
        sb_flux_r REAL,
        sb_flux_i REAL,
        sb_flux_z REAL
    )
    """
    cursor.execute(create_props_table)

    # drop observations table
    drop_obs_table = "DROP TABLE IF EXISTS slsn_obs"
    cursor.execute(drop_obs_table)

    # create new observations table
    create_obs_table = """
    CREATE TABLE slsn_obs (
        snid INTEGER,
        name VARCHAR(12),
        mjd DOUBLE PRECISION,
        band CHAR(1),
        field CHAR(2),
        flux DOUBLE PRECISION,
        fluxerr DOUBLE PRECISION,
        phot_flag SMALLINT,
        phot_prob REAL,
        zp REAL,
        psf REAL,
        skysig REAL,
        skysig_t REAL,
        gain REAL,
        season SMALLINT,
        status SMALLINT,
        ccd SMALLINT
    )
    """
    cursor.execute(create_obs_table)


if __name__ == "__main__":
    conn = db.connect(host='localhost',
                      user='szymon',
                      password='supernova',
                      database='thesis')
    cur = conn.cursor()

    create_tables(cur)
    conn.commit()

    engine = create_engine('postgresql://szymon:supernova@localhost:5432/thesis')

    template_csv = '/Users/szymon/Dropbox/Projects/DES/data/refListMJD.txt'
    template = pd.read_csv(template_csv)

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

    magnetar = Magnetar(b"/Users/szymon/Projects/pyMagnetar/filters")

    prop = []
    obs = []
    snid = 0
    for index, slsn in df_slsn.iterrows():
        lc = LCSim()
        simlib_path = '/Users/szymon/Dropbox/Projects/SigNS/'
        simlib = SIMLIBReader(simlib_path + 'DES_20170316.SIMLIB')

        for _ in range(10):
            snid += 1
            field, ccd = random_field()
            z = random_redshift(3.0)

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
                               'param_index': []
                               })

            t0 = np.random.randint(56500, 58150)
            for flt in ['g', 'r', 'i', 'z']:
                obs = simlib.get_obslog(field, ccd, band=flt)
                mjd = obs['mjd'].values.astype(int)

                flux = np.array(magnetar.flux((obs['mjd']-t0).values,
                                              str.encode("DES_"+flt)))
                mask = ((template['field'] == field) &
                        (template['filter'] == flt) &
                        (template['season'] == 1))
                T1 = template[mask]['mjd'].astype(int).unique()
                mask = ((template['field'] == field) &
                        (template['filter'] == flt) &
                        (template['season'] == 2))
                T2 = template[mask]['mjd'].astype(int).unique()

                idx = np.searchsorted(mjd, T1)
                T1_median = np.median(flux[idx])
                idx = np.searchsorted(mjd, T2)
                T2_median = np.median(flux[idx])

                obs = simlib.get_obslog(field, ccd, band=flt)
                mask = (obs['mjd'] < 56700) | (obs['mjd'] > 57200)
                Y1 = np.searchsorted(mjd, obs.loc[mask, 'mjd'].astype(int))
                mask = (obs['mjd'] > 56700) & (obs['mjd'] < 57200)
                Y2 = np.searchsorted(mjd, obs[mask]['mjd'].astype(int))
                np.put(flux, Y2, flux[Y2] - T1_median)
                np.put(flux, Y1, flux[Y1] - T2_median)

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
                temp_df['season'] = temp_df['mjd'].map(mjd_to_season)
                temp_df['status'] = 1
                temp_df['ccd'] = ccd
                temp_df['snid'] = snid
                temp_df['z'] = z
                temp_df['param_index'] = index
                temp_df['name'] = 'fake_slsn_'+str(snid)

                df = pd.concat((df, temp_df))

            detected = df[df['flux'] / df['fluxerr'] > 5]
            if detected.shape[0] > 1:
                sorted_mjd = detected['mjd'].values
                sorted_mjd.sort()
                sorted_separation = np.diff(sorted_mjd)
                sorted_separation.sort()

                if sorted_separation[0] < 30:
                    df.to_sql('slsn_5_realisations',
                              engine,
                              if_exists='append',
                              index=False)

        if index % 100 == 0:
            now = datetime.datetime.now()
            now = now.isoformat().split('T')[1].split('.')[0]
            print(now, '- SNID progress:', str(index)+'/'+str(df_slsn.shape[0]))

    conn.close()
