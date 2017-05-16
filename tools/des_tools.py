"""
Miscellenious helper functions specifically for the use with DES
"""
import numpy as np
from numba import jit


@jit
def mjd_to_season(mjd):
    """
    Find a corresponding DES observing season for a given MJD

    Properties
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
    Parse DES image quality status code into an array of individual flags

    Properies
    ---------
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

    Properties
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
