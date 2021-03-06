import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import tools.des_tools as dest
plt.rcParams['figure.figsize'] = 10, 6

sql = 'postgresql://szymon:supernova@localhost:5432/thesis'
engine = create_engine(sql)


"""   Ia   """
query = "SELECT snid, specz FROM fake_ia_props"
df = pd.read_sql_query(query, engine)
# z = np.linspace(0, 1.4, 1000)
# pdf = dest.get_sfr_z_pdf(0.8, 0.01)

plt.cla()
plt.hist(df['specz'].values, bins=20, normed=1, label='Observed distribution')
plt.plot([2.0, 2.0], [0.5, 0.5], label=None)
plt.xlabel('Redshift', fontsize=14)
plt.ylabel('Normalised count', fontsize=14)
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)

plt.legend(fontsize=12)
plt.xlim(0, 1.5)
plt.savefig('/Users/szymon/Dropbox/Plots/SNIa_z_dist.png', bbox_inches='tight')


"""   II   """
query = "SELECT distinct(snid), z FROM fake_ii_obs"
df = pd.read_sql_query(query, engine)
z = np.linspace(0, 0.8, 1000)
pdf = dest.get_sfr_z_pdf(0.8, 0.01)

plt.cla()
plt.hist(df['z'].values, bins=20, normed=1, label='Observed distribution')
plt.plot(z, pdf(z), label='Input distribution')
plt.xlabel('Redshift', fontsize=14)
plt.ylabel('Normalised count', fontsize=14)
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)

plt.legend(fontsize=12)
plt.savefig('/Users/szymon/Dropbox/Plots/SNII_z_dist.png', bbox_inches='tight')


"""   Ib/c   """
query = "SELECT distinct(snid), z FROM fake_ibc_obs_2"
df = pd.read_sql_query(query, engine)
z = np.linspace(0, 0.8, 1000)
pdf = dest.get_sfr_z_pdf(0.8, 0.01)

plt.cla()
plt.hist(df['z'].values, bins=20, normed=1, label='Observed distribution')
plt.plot(z, pdf(z), label='Input distribution')
plt.xlabel('Redshift', fontsize=14)
plt.ylabel('Normalised count', fontsize=14)
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)

plt.legend(fontsize=12)
plt.savefig('/Users/szymon/Dropbox/Plots/SNIbc_z_dist.png', bbox_inches='tight')


"""   SLSN   """
query = "SELECT distinct(snid), z FROM slsn_5_realisations_2"
df = pd.read_sql_query(query, engine)

z = np.linspace(0, 3, 1000)
pdf = dest.get_sfr_z_pdf(3.0, 0.01)

plt.cla()
plt.hist(df['z'].values, bins=20, normed=True, label='Observed distribution')
plt.plot(z, pdf(z), label='Input distribution')
plt.xlabel('Redshift', fontsize=14)
plt.ylabel('Normalised count', fontsize=14)
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)

plt.legend(fontsize=12)
plt.savefig('/Users/szymon/Dropbox/Plots/SLSN_z_dist.png', bbox_inches='tight')
