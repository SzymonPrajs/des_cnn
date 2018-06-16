"""Plot DES light curves in many different configurations."""
import matplotlib.pyplot as plt
from tools.des_tools import band_colour

import george
import numpy as np
import scipy.optimize as opt
from george.kernels import Matern32Kernel


def ll(p, gp, df):
    gp.kernel[:] = p
    return -gp.lnlikelihood(df['flux'], quiet=True)


def grad_ll(p, gp, df):
    gp.kernel[:] = p
    return -gp.grad_lnlikelihood(df['flux'], quiet=True)


def plot_all_seasons(data):
    """
    Plot a horizontal multi-panel light curve plot for DES objects.

    Parameters
    ----------
    data : `pandas.DataFrame`
        DataFrame object contaning a full DES light curve. This should be
        obtained though [NAME OF LOCAL QUERY FUNC] or [NAME OF EASYACCESS
        WRAPPER]

    Returns
    -------
    fig : `plt.figure`
        PyPlot canvas object, if originally provided as a parameter the
        original object will be returned, otherwise new object is created.
    """
    fig, ax = plt.subplots(1, 4, figsize=(20, 5), sharey=True)

    gdf = data.groupby(('season', 'band'))
    for group, obs in gdf:
        i = int(group[0]) - 1

        if i == 3:
            label = group[1]

        else:
            label = None

        ax[i].errorbar(obs['mjd'],
                       obs['flux'],
                       yerr=obs['fluxerr'],
                       fmt='o',
                       c=band_colour(group[1]),
                       label=label)

    fig.tight_layout()

    detected = data.query("flux / fluxerr > 5")
    noise = data.query("flux / fluxerr < 5")
    plt.ylim(-5 * np.median(noise['fluxerr'].values),
             1.2 * np.max(data['flux'].values))

    plt.legend(fontsize=16, loc='best')

    return ax


def plot_gp_all_seasons(data):
    """
    Plot a horizontal multi-panel light curve plot for DES objects.

    Parameters
    ----------
    data : `pandas.DataFrame`
        DataFrame object contaning a full DES light curve. This should be
        obtained though [NAME OF LOCAL QUERY FUNC] or [NAME OF EASYACCESS
        WRAPPER]

    Returns
    -------
    fig : `plt.figure`
        PyPlot canvas object, if originally provided as a parameter the
        original object will be returned, otherwise new object is created.
    """
    fig, ax = plt.subplots(1, 4, figsize=(20, 5), sharey=True)

    gdf = data.groupby(('season', 'band'))
    for group, obs in gdf:
        i = int(group[0]) - 1

        if i == 3:
            label = group[1]

        else:
            label = None

        ax[i].errorbar(obs['mjd'],
                       obs['flux'],
                       yerr=obs['fluxerr'],
                       fmt='o',
                       c=band_colour(group[1]),
                       label=label)

        flux_norm = obs['flux'].mean()
        obs['flux'] /= flux_norm
        obs['fluxerr'] /= flux_norm

        gp = george.GP(Matern32Kernel(np.exp(10)))
        gp.compute(obs['mjd'], obs['fluxerr'])
        p0 = gp.kernel.vector
        opt.minimize(ll, p0, jac=grad_ll, args=(gp, obs))

        t = np.linspace(obs['mjd'].min(), obs['mjd'].max(), 500)
        mu, cov = gp.predict(obs['flux'], t)
        std = np.sqrt(np.diag(cov))

        mu *= flux_norm
        std *= flux_norm

        ax[i].fill_between(t, mu+std, mu-std,
                           color=band_colour(group[1]),
                           alpha=0.4)
        ax[i].plot(t, mu, color=band_colour(group[1]))

    fig.tight_layout()
    plt.legend(fontsize=16, loc='best')

    return ax


def plot_gp_only_all_seasons(data):
    """
    Plot a horizontal multi-panel light curve plot for DES objects.

    Parameters
    ----------
    data : `pandas.DataFrame`
        DataFrame object contaning a full DES light curve. This should be
        obtained though [NAME OF LOCAL QUERY FUNC] or [NAME OF EASYACCESS
        WRAPPER]

    Returns
    -------
    fig : `plt.figure`
        PyPlot canvas object, if originally provided as a parameter the
        original object will be returned, otherwise new object is created.
    """
    fig, ax = plt.subplots(1, 4, figsize=(20, 5), sharey=True)

    gdf = data.groupby(('season', 'band'))
    for group, obs in gdf:
        i = int(group[0]) - 1

        if i == 3:
            label = group[1]

        else:
            label = None

        ax[i].scatter(obs['mjd'],
                      obs['flux'],
                      c=band_colour(group[1]),
                      label=label)

    fig.tight_layout()
    plt.legend(fontsize=16, loc='best')

    return ax


def plot_gp_one_season(data):
    """
    Plot DES light curve as a single season.

    Parameters
    ----------
    data : `pandas.DataFrame`
        DataFrame object contaning a full DES light curve. This should be
        obtained though [NAME OF LOCAL QUERY FUNC] or [NAME OF EASYACCESS
        WRAPPER]

    Returns
    -------
    fig : `plt.figure`
        PyPlot canvas object, if originally provided as a parameter the
        original object will be returned, otherwise new object is created.
    """
    flux_arr = []

    gdf = data.groupby('band')
    for group, obs in gdf:
        plt.errorbar(obs['mjd'],
                     obs['flux'],
                     yerr=obs['fluxerr'],
                     fmt='o',
                     c=band_colour(group),
                     label=group)

        flux_norm = obs['flux'].mean()
        obs['flux'] /= flux_norm
        obs['fluxerr'] /= flux_norm

        gp = george.GP(Matern32Kernel(np.exp(10)))
        gp.compute(obs['mjd'], obs['fluxerr'])
        p0 = gp.kernel.vector
        opt.minimize(ll, p0, jac=grad_ll, args=(gp, obs))

        t = np.linspace(data['mjd'].min(), data['mjd'].max(), 500)
        mu, cov = gp.predict(obs['flux'], t)
        std = np.sqrt(np.diag(cov))

        mu *= flux_norm
        std *= flux_norm
        flux_arr.append(mu)

        plt.fill_between(t, mu+std, mu-std,
                         color=band_colour(group),
                         alpha=0.4)
        plt.plot(t, mu, color=band_colour(group))

    plt.tight_layout()
    plt.legend(fontsize=16, loc='best')
