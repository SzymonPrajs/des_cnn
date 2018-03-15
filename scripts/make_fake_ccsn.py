"""
Make fake CCSN light curves.

Ingest fake CCSN light curves into a PSQL database after applying observing
logs and correcting for template offsets.
"""
import datetime
import numpy as np
import pandas as pd
import psycopg2 as db
import pycoco as pcc
import pyCoCo as pccsim

from astropy.cosmology import Planck15 as cosmo
from scipy.interpolate import InterpolatedUnivariateSpline

import matplotlib.pyplot as plt

import lsst_tools as lsstt
from lcsim.simlib import SIMLIBReader
from lcsim.lcsim import LCSim

from sqlalchemy import create_engine
from tools.des_tools import random_field, random_redshift, mjd_to_season


binsize = 0.01
z_max = 0.8
n_sne_req = 100000

if __name__ == "__main__":
    conn = db.connect(host='localhost',
                      user='szymon',
                      password='supernova',
                      database='thesis')
    cur = conn.cursor()
    sql = 'postgresql://szymon:supernova@localhost:5432/thesis'
    engine = create_engine(sql)

    simlib_path = '/Users/szymon/Dropbox/Projects/SigNS/'
    simlib = SIMLIBReader(simlib_path + 'DES_20170316.SIMLIB')
    lc = LCSim()

    z = np.arange(0.0, z_max, binsize)
    z_dz = np.arange(0.0 + binsize, z_max + binsize, binsize)

    v_z = cosmo.comoving_volume(z)
    v_z_dz = cosmo.comoving_volume(z_dz)

    v_dz = v_z_dz - v_z
    norm_v_dz = v_dz / np.nanmax(v_dz)

    sfr_z = lsstt.sims.calculate_SFR(z)
    sfr_norm = sfr_z / np.nanmax(sfr_z)

    volumetric_rate = norm_v_dz * sfr_norm
    normed_volumetric_rate = volumetric_rate / np.nanmax(volumetric_rate)
    pdf = InterpolatedUnivariateSpline(z, normed_volumetric_rate)

    info = pcc.classes.InfoClass()
    info.load(path="/Users/szymon/Projects/CoCo/data/info/info_good.dat")
    filter_names = ["DES_g", "DES_r", "DES_i", "DES_z"]

    zp_dict = {}
    for flt in filter_names:
        zp_dict[flt] = pcc.kcorr.calc_AB_zp(flt)

    fltPath = pcc.utils.b(pcc.defaults._default_filter_dir_path)
    rootPath = pcc.utils.b(pcc.defaults._default_coco_dir_path)
    coco = pccsim.pyCoCo(fltPath, rootPath)

    snid = 0
    n_sne = 0
    while n_sne < n_sne_req:
        snid += 1

        field, ccd = random_field()

        z_sim = 0
        while True:
            x = np.random.random() * z_max
            y = np.random.random()

            if y <= pdf(x):
                z_sim = x
                break

        mag_offset = lsstt.sims.choose_magoffset(n=1)[0]
        p0 = np.random.randint(56500, 58150)
        MW_EBV = 0.0
        host_EBV = lsstt.sims.choose_extinction_host(n=1)

        subtype = lsstt.sims.choose_subtype()
        idx = np.where(info.table["Type"] == subtype)[0]
        snindex = np.random.choice(idx)
        snname = pcc.utils.b(info.table["snname"].data[snindex])

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
                           'p0': []
                           })

        runaway = False
        for flt in ['g', 'r', 'i', 'z']:
            obs = simlib.get_obslog(field, ccd, band=flt)
            obs["flt_b"] = obs["flt"].map(lambda x: str.encode("DES_"+x))

            flux, flux_err = coco.simulate(snname, z_sim, mag_offset,
                                           MW_EBV, host_EBV, 3.1, p0,
                                           obs['mjd'].values,
                                           obs['flt_b'].values)
            flux *= 10.0**(0.4 * (zp_dict['DES_' + flt] + 31.4))
            fluxcal, errcal = lc.simulate(flux, obs)

            if flux.max() > 1e6:
                runaway = True

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
            temp_df['z'] = z_sim
            temp_df['p0'] = p0
            temp_df['name'] = 'fake_ccsn_'+str(snid)

            df = pd.concat((df, temp_df))

        if runaway:
            continue

        detected = df[df['flux'] / df['fluxerr'] > 5]
        if detected.shape[0] > 1:
            sorted_mjd = detected['mjd'].values
            sorted_mjd.sort()
            sorted_separation = np.diff(sorted_mjd)
            sorted_separation.sort()

            if sorted_separation[0] < 30:
                n_sne += 1
                df.to_sql('fake_ibc_obs',
                          engine,
                          if_exists='append',
                          index=False)

        if n_sne % 100 == 0:
            now = datetime.datetime.now()
            now = now.isoformat().split('T')[1].split('.')[0]
            print(now, '- Progress:', str(n_sne)+'/'+str(n_sne_req))

    conn.close()
