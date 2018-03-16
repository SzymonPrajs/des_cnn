import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import tools.des_tools as dest
from astropy.cosmology import Planck15 as c

sql = 'postgresql://szymon:supernova@localhost:5432/thesis'
engine = create_engine(sql)


"""   Ib/c   """
query = """
SELECT snid, MAX(flux) AS peak, MAX(z) AS z
FROM fake_ibc_obs WHERE flux/fluxerr > 8
GROUP BY snid
"""
df = pd.read_sql_query(query, engine)

plt.cla()
plt.scatter(df['z'], -2.5*np.log10(df['peak']) + 31.4 - c.distmod(df['z'].values).value)
plt.savefig('/Users/szymon/Dropbox/Plots/SNIbc_peak_lum.png', bbox_inches='tight')


"""   SLSN   """
query = """
SELECT snid, MAX(flux) AS peak, MAX(z) AS z
FROM slsn_5_realisations_2 WHERE flux/fluxerr > 8
GROUP BY snid
"""
df = pd.read_sql_query(query, engine)

plt.cla()
plt.scatter(df['z'], -2.5*np.log10(df['peak']) + 31.4 - c.distmod(df['z'].values).value)
plt.savefig('/Users/szymon/Dropbox/Plots/SLSN_peak_lum.png', bbox_inches='tight')
