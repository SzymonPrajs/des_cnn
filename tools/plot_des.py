"""
Plot DES light curves in many different configurations
"""
import matplotlib.pyplot as plt
from tools.des_tools import band_colour


def plot_all_seasons(data):
    """
    Plot a horizontal multi-panel light curve plot for DES objects

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
        i = group[0] - 1

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
    plt.legend(fontsize=16, loc='best')

    return ax
