import sys
import numpy as np
import pandas as pd
import tools.des_tools as des
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from tools.des_tools import query_localdb
from tools.plot_des import plot_all_seasons


for folder in ['SLSN_99', 'SLSN_90', 'SLSN_50']:
    SLSN = np.loadtxt('/Users/szymon/Dropbox/'+folder+'.csv', unpack=True)
    engine = create_engine('postgresql://szymon:supernova@localhost:5432/thesis')

    for snid in SLSN:
        query = "SELECT * FROM real_des_obs_corr WHERE snid="+str(snid)
        df = query_localdb(query)
        print('snid:', df['snid'][0])

        ax = plot_gp_all_seasons(df)
        plt.savefig('/Users/szymon/Dropbox/Plots/GP_'+folder+'/'+str(snid)+'.png', bbox_inches='tight')


        query = "SELECT * FROM real_des_interp_46 WHERE snid="+str(snid)
        df = query_localdb(query)
        print('snid:', df['snid'][0])

        ax = plot_gp_only_all_seasons(df)
        plt.savefig('/Users/szymon/Dropbox/Plots/GP_ONLY_'+folder+'/'+str(snid)+'.png', bbox_inches='tight')
