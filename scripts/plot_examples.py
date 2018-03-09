import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from tools.des_tools import query_localdb
from tools.plot_des import plot_all_seasons


query = """
WITH id AS (
    SELECT DISTINCT(snid) FROM slsn_5_realisations
    WHERE z<0.5 ORDER BY snid ASC LIMIT 1
)
SELECT a.* FROM slsn_5_realisations a
JOIN id ON a.snid=id.snid
"""
df = query_localdb(query)
print('Fake SLSN -  snid:', df['snid'][0], ' -  z:', df['z'][0])

ax = plot_all_seasons(df)
plt.savefig('/Users/szymon/Desktop/example_SLSN.png', bbox_inches='tight')
