import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from tools.des_tools import query_localdb
from tools.plot_des import plot_all_seasons


"""   SLSN   """
query = """
WITH id AS (
    SELECT DISTINCT(snid) FROM slsn_5_realisations_2
    WHERE t0 BETWEEN 56900 and 56950 AND
          z BETWEEN 0.5 AND 1.0
    ORDER BY snid ASC LIMIT 1
)
SELECT a.* FROM slsn_5_realisations_2 a
JOIN id ON a.snid=id.snid
"""
df = query_localdb(query)
print('Fake SLSN -  snid:', df['snid'][0], ' -  z:', df['z'][0])

ax = plot_all_seasons(df)
plt.savefig('/Users/szymon/Desktop/example_SLSN.png', bbox_inches='tight')


"""   AGN   """
query = """
WITH id AS (
    SELECT DISTINCT(snid) FROM agn_10_realisations
    WHERE flux/fluxerr > 8 AND flux > 5000
    ORDER BY snid ASC LIMIT 1
)
SELECT a.* FROM agn_10_realisations a
JOIN id ON a.snid=id.snid
"""
df = query_localdb(query)
print('Fake AGN -  snid:', df['snid'][0])

ax = plot_all_seasons(df)
plt.savefig('/Users/szymon/Desktop/example_AGN.png', bbox_inches='tight')


"""   Ia   """
query = """
WITH id AS (
    SELECT DISTINCT(snid) FROM fake_ia_obs
    WHERE flux/fluxerr > 8 AND flux > 5000
    ORDER BY snid ASC LIMIT 1
)
SELECT a.* FROM fake_ia_obs a
JOIN id ON a.snid=id.snid
"""
df = query_localdb(query)
print('Fake AGN -  snid:', df['snid'][0])

ax = plot_all_seasons(df)
plt.savefig('/Users/szymon/Desktop/example_SNIa.png', bbox_inches='tight')
