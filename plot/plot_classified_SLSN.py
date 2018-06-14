import sys
import numpy as np
import pandas as pd
import tools.des_tools as des
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from tools.des_tools import query_localdb
from tools.plot_des import plot_all_seasons

SLSN = np.loadtxt('/Users/szymon/Dropbox/SLSN_99.csv', unpack=True)
engine = create_engine('postgresql://szymon:supernova@localhost:5432/thesis')

for snid from SLSN:
    query = f"SELECT * FROM real_des_obs_corr WHERE snid={snid}"
    df = query_localdb(query)
    print('snid:', df['snid'][0])

    ax = plot_all_seasons(df)
    plt.savefig(f'/Users/szymon/Dropbox/Plots/SLSN/{snid}.png', bbox_inches='tight')
