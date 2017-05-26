"""
Load fake AGN light curves into a PSQL database after applying observing logs
and correcting for template offsets
"""
import os
import numpy as np
import pandas as pd
import scipy.io as io
import psycopg2 as db
import psycopg2.extras
import matplotlib.pyplot as plt
from lcsim.lcsim import LCSim
from lcsim.simlib import SIMLIBReader
from tools.des_tools import random_field


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

    template_csv = '/Users/szymon/Dropbox/Projects/DES/data/refListMJD.txt'
    template = pd.read_csv(template_csv)

    prop = []
    obs = []
    for agn_file in agn_files:
        agn = io.readsav(path + agn_file)['lc_agn']
        lc = LCSim()
        simlib = SIMLIBReader('/Users/szymon/Dropbox/Projects/SigNS/DES_20170316.SIMLIB')
        print('loaded', agn_file)

        for i in range(agn.size):
            field, ccd = random_field()

            for flt in ['G', 'R', 'I', 'Z']:
                mjd = agn[i][flt]['epoch'][0] + 56450
                flux = 10**(0.4*(31.4 - agn[i][flt]['mag'][0]))

                T1 = template.query("field == '{}' and filter == '{}' and season == 1".format(field, flt.lower()))['mjd'].astype(int).unique()
                T2 = template.query("field == '{}' and filter == '{}' and season == 2".format(field, flt.lower()))['mjd'].astype(int).unique()
                idx = np.searchsorted(mjd, T1)
                T1_median = np.median(flux[idx])
                idx = np.searchsorted(mjd, T2)
                T2_median = np.median(flux[idx])

                obs = simlib.get_obslog(field, ccd, band=flt.lower())
                Y1 = np.searchsorted(mjd, obs.query('mjd < 56700 or mjd > 57200')['mjd'].astype(int))
                Y2 = np.searchsorted(mjd, obs.query('mjd > 56700 and mjd < 57200')['mjd'].astype(int))
                np.put(flux, Y2, flux[Y2] - T1_median)
                np.put(flux, Y1, flux[Y1] - T2_median)

                idx = np.searchsorted(mjd, obs['mjd'].astype(int))
                fluxcal, errcal = lc.simulate(flux[idx], obs)
                plt.errorbar(mjd[idx], fluxcal, yerr=errcal, fmt='o')
            break

        print('parsed', agn_file)
        break
    conn.close()
    plt.show()
