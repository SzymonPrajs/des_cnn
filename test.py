import tools.plot_des as plot
import tools.des_tools as des
import matplotlib.pyplot as plt

# query = "SELECT transient_name FROM SNCAND WHERE spec_type='SNIa'"
# query = "SELECT transient_name FROM SNCAND WHERE spec_type!='SNIa' and spec_type!='none'"
# query = "SELECT transient_name, spec_eval FROM SNSPECT where spec_eval LIKE 'S%' AND telescope!='AAT'"
# Ia = des.query_desdm(query)

# for i, name in enumerate(Ia['TRANSIENT_NAME'].values):
#     query = "SELECT * FROM des_obs WHERE name='{}'".format(name)
#     data = des.query_localdb(query)
#
#     if data is None:
#         continue
#
#     ax = plot.plot_gp_all_seasons(data)
#     plt.title("{} - {}".format(name, Ia['SPEC_EVAL'].values[i]))
#     plt.savefig('/Users/szymon/Desktop/NonAAT/{}.png'.format(name))

# query = "SELECT * FROM agn_test WHERE snid=9766"
# data = des.query_localdb(query)
#
# plot.plot_gp_all_seasons(data)
# plt.savefig('/Users/szymon/Desktop/AGN_3.png')
#
# plt.show()

query = "SELECT DISTINCT snid FROM agn_test"
AGNS = des.query_localdb(query)['snid'].values

for i, snid in enumerate(AGNS):
    query = "SELECT * FROM agn_test WHERE snid={}".format(int(snid))
    data = des.query_localdb(query)

    if data is None:
        continue

    ax = plot.plot_gp_all_seasons(data)
    plt.savefig('/Users/szymon/Desktop/fake_agn/{}.png'.format(snid))

# query = "SELECT * FROM des_obs WHERE name='DES16C3cv' AND season=4"
# data = des.query_localdb(query)
#
# plot.plot_one_season(data)
# plt.savefig('/Users/szymon/Desktop/CV.png', figsize=(15,5))

# plt.show()
