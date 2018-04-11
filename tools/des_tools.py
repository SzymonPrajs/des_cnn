"""Miscellenious helper functions specifically for the use with DES."""
import numpy as np
import pandas as pd
import psycopg2 as db
from numba import jit
import easyaccess as ea
from itertools import chain

from astropy.cosmology import Planck15 as cosmo
from scipy.interpolate import InterpolatedUnivariateSpline

import lsst_tools as lsstt


@jit
def mjd_to_season(mjd):
    """
    Find a corresponding DES observing season for a given MJD.

    Parameters
    ----------
    mjd : float
        MJD of an observation

    Returns
    -------
    season : int
        Observing season corresponding to a given MJD
    """
    if 56000 < mjd < 56750:
        return 1

    elif 56750 < mjd < 57150:
        return 2

    elif 57150 < mjd < 57550:
        return 3

    elif 57550 < mjd < 57950:
        return 4

    elif 57950 < mjd < 58350:
        return 5

    else:
        return 0


@jit
def parse_status_code(status_code):
    """
    Parse DES image quality status code into an array of individual flags.

    Parameters
    ----------
    status_code : int
        DES image quality status code

    Returns
    -------
    status_flags : `np.array`
        Array of error flags
    """
    status_flags = np.zeros(14)
    for i in range(13, -1, -1):
        if status_code == status_code % 2**i:
            status_flags[i] = 0

        else:
            status_flags[i] = 1

        status_code = status_code % 2**i

    return status_flags


@jit
def get_status_quality(status_code):
    """
    Convert photometric quality status code into a simpler output:
    0 : bad photometry
    1 : good photometry
    2 : good photometry + real-time detection

    Parameters
    ----------
    status_code : int
        DES image quality status code

    Returns
    -------
    status_quality : int
        Quality flag
    """
    status_flags = parse_status_code(status_code)

    if status_flags[3:-2].sum() > 0:
        return 0

    elif status_flags[-2:].sum() > 0:
        return 2

    else:
        return 1


def query_desdm(query):
    """
    Very basic wrapper around easy access to perform queries returning
    pandas.DataFrame objects.

    Parameters
    ----------
    query : str
        Query to be executed

    Returns
    -------
    data : `pandas.DataFrame`
        DataFrame object containing queried data
    """
    conn = ea.connect()

    return conn.query_to_pandas(query)


def query_localdb(query):
    """
    Simple engine for querying a local database containing the DES data (both
    real and fakes).

    Parameters
    ----------
    query : str
        Query to be executed

    Returns
    -------
    data : `pandas.DataFrame`
        DataFrame object containing queried data
    """
    conn = db.connect(host='localhost',
                      user='szymon',
                      password='supernova',
                      database='thesis')
    cur = conn.cursor()

    cur.execute(query)
    header = [columns[0] for columns in cur.description]
    data = pd.DataFrame(cur.fetchall())

    if data.shape[1] != len(header):
        return None

    data.columns = header
    if 'name' in header:
        data.drop('name', axis=1, inplace=True)
        data.drop('ccd', axis=1, inplace=True)
    data.dropna(inplace=True)

    conn.close()
    return data


def band_colour(band):
    """
    Return a pretty colour for a DES photometric band

    Parameters
    ----------
    band : char
        Photometric band. Must be griz

    Returns
    -------
    col : str
        Colour hex value for a given band
    """
    col = {
        'g': "#59D11D",
        'r': "#F2990A",
        'i': "#A61C00",
        'z': "#000000"
    }

    return col[band]


def random_field():
    field = ['C1', 'C2', 'C3', 'E1', 'E2', 'S1', 'S2', 'X1', 'X2', 'X3']
    ccd_range = chain(range(1, 2), range(3, 31), range(32, 61), range(62, 63))
    return field[np.random.randint(10)], np.random.choice(list(ccd_range))


def random_redshift(max_z):
    x = np.random.random() * (1 + max_z)**3.0 + 1
    return x**(1.0 / 3.0) - 1


def random_explosion_mjd(offset):
    min_mjd = np.array([56534, 56877, 57248, 57614]) - offset
    max_mjd = np.array([56698, 57067, 57408, 57791])

    s = np.random.randint(4)
    return min_mjd[s] + (np.random.random() * (max_mjd[s] - min_mjd[s]))


def get_sfr_z_pdf(z_max, binsize):
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

    return InterpolatedUnivariateSpline(z, normed_volumetric_rate)


def random_redshift_sfr(pdf, z_max):
    while True:
        x = np.random.random() * z_max
        y = np.random.random()

        if y <= pdf(x):
            return x


def choose_subtype_II(return_string=True):
    """
    1|SNIIn | 3|0.3|
    2|SNIIP | 7|0.7|
    total - 10
    """
    n = np.random.random()

    if n <= 0.3:
        if return_string:
            return "SNIIn"
        else:
            return 1

    else:
        if return_string:
            return "SNIIP"
        else:
            return 2


def max_z_SNII(snname):
    max_z = {"SN2011hs": 0.26,
             "SN2007pk": 0.75,
             "SN2009E": 0.30,
             "SN2013ej": 0.36,
             "SN2002gd": 0.26,
             "SN2000cb": 0.36,
             "SN1999el": 0.4,
             "SN2010al": 0.9,
             "SN2006V": 0.45,
             "SN2012ec": 0.36,
             "SN2000eo": 0.78}

    if snname in max_z:
        return max_z[snname]
    else:
        return 0.9
