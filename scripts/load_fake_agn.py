"""
Load fake AGN light curves into a PSQL database after applying observing logs
and correcting for template offsets
"""
import os
import datetime
import numpy as np
import pandas as pd
import scipy.io as io
import psycopg2 as db
from lcsim.lcsim import LCSim
from lcsim.simlib import SIMLIBReader
from sqlalchemy import create_engine
from tools.des_tools import random_field, mjd_to_season


def create_tables(cursor):
    # drop object properties table
    drop_props_table = "DROP TABLE IF EXISTS agn_props"
    cursor.execute(drop_props_table)

    # create new object properties table
    create_props_table = """
    CREATE TABLE agn_props (
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
    drop_obs_table = "DROP TABLE IF EXISTS agn_obs"
    cursor.execute(drop_obs_table)

    # create new observations table
    create_obs_table = """
    CREATE TABLE agn_obs (
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
    path = '/Users/szymon/Dropbox/Projects/DES/data/fake_agn/'
    agn_files = os.listdir(path)

    if '.DS_Store' in agn_files:
        agn_files.remove('.DS_Store')

    conn = db.connect(host='localhost',
                      user='szymon',
                      password='supernova',
                      database='des')
    cur = conn.cursor()

    create_tables(cur)
    conn.commit()

    engine = create_engine('postgresql://szymon:supernova@localhost:5432/des')

    template_csv = '/Users/szymon/Dropbox/Projects/DES/data/refListMJD.txt'
    template = pd.read_csv(template_csv)

    prop = []
    obs = []
    snid = 0
    for agn_file in agn_files:
        now = datetime.datetime.now().isoformat().split('T')[1].split('.')[0]
        print(now, '- Reading', agn_file)
        agn = io.readsav(path + agn_file)['lc_agn']
        lc = LCSim()
        simlib_path = '/Users/szymon/Dropbox/Projects/SigNS/'
        simlib = SIMLIBReader(simlib_path + 'DES_20170316.SIMLIB')
        now = datetime.datetime.now().isoformat().split('T')[1].split('.')[0]
        print(now, '- Loaded', agn_file)

        for i in range(agn.size):
            for _ in range(10):
                snid += 1
                field, ccd = random_field()
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

                for flt in ['G', 'R', 'I', 'Z']:
                    mjd = agn[i][flt]['epoch'][0]
                    mjd += np.random.randint(56400, 56500)
                    flux = 10**(0.4*(31.4 - agn[i][flt]['mag'][0]))

                    mask = ((template['field'] == field) &
                            (template['filter'] == flt.lower()) &
                            (template['season'] == 1))
                    T1 = template[mask]['mjd'].astype(int).unique()
                    mask = ((template['field'] == field) &
                            (template['filter'] == flt.lower()) &
                            (template['season'] == 2))
                    T2 = template[mask]['mjd'].astype(int).unique()

                    idx = np.searchsorted(mjd, T1)
                    T1_median = np.median(flux[idx])
                    idx = np.searchsorted(mjd, T2)
                    T2_median = np.median(flux[idx])

                    obs = simlib.get_obslog(field, ccd, band=flt.lower())
                    mask = (obs['mjd'] < 56700) | (obs['mjd'] > 57200)
                    Y1 = np.searchsorted(mjd, obs.loc[mask, 'mjd'].astype(int))
                    mask = (obs['mjd'] > 56700) & (obs['mjd'] < 57200)
                    Y2 = np.searchsorted(mjd, obs[mask]['mjd'].astype(int))
                    np.put(flux, Y2, flux[Y2] - T1_median)
                    np.put(flux, Y1, flux[Y1] - T2_median)

                    idx = np.searchsorted(mjd, obs['mjd'].astype(int))
                    fluxcal, errcal = lc.simulate(flux[idx], obs)

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
                    temp_df['name'] = 'fake_agn_'+str(snid)

                    df = pd.concat((df, temp_df))

                detected = df[df.flux/df.fluxerr > 5]
                if detected.shape[0] > 1:
                    sorted_mjd = detected.mjd.values
                    sorted_mjd.sort()
                    sorted_separation = np.diff(sorted_mjd)
                    sorted_separation.sort()

                    if sorted_separation[0] < 30:
                        df.to_sql('agn_10_realisations',
                                  engine,
                                  if_exists='append',
                                  index=False)

        now = datetime.datetime.now().isoformat().split('T')[1].split('.')[0]
        print(now, '- Parsed', agn_file)
    conn.close()
