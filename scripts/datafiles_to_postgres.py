"""
Read DES data files (SNANA formatted) and push important data into a local
PostgeSQL database.
"""
import os
import sys
import psycopg2 as db
import psycopg2.extras
from time import time
from tools.read_snana_files import read_des_photometry_file


# TODO : Move to a separate misc tools file
def progress(count, total, status=''):
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + ' ' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ... %s\r' % (bar, percents, '%', status))
    sys.stdout.flush()


def create_tables(prefix, cursor):
    # drop object properties table
    drop_props_table = "DROP TABLE IF EXISTS {}_props".format(prefix)
    cursor.execute(drop_props_table)

    # create new object properties table
    create_props_table = """
    CREATE TABLE {}_props (
        snid INTEGER,
        name VARCHAR(12),
        fake SMALLINT,
        field CHAR(2),
        ccd SMALLINT,
        num_detected SMALLINT,
        specz REAL,
        photz REAL,
        specz_err REAL,
        gal_mag_g REAL,
        gal_mag_r REAL,
        gal_mag_i REAL,
        gal_mag_z REAL,
        sb_flux_g REAL,
        sb_flux_r REAL,
        sb_flux_i REAL,
        sb_flux_z REAL
    )
    """
    cursor.execute(create_props_table.format(prefix))

    # drop observations table
    drop_obs_table = "DROP TABLE IF EXISTS {}_obs".format(prefix)
    cursor.execute(drop_obs_table)

    # create new observations table
    create_obs_table = """
    CREATE TABLE {}_obs (
        snid INTEGER,
        name VARCHAR(12),
        mjd DOUBLE PRECISION,
        band CHAR(1),
        field CHAR(2),
        flux DOUBLE PRECISION,
        fluxerr DOUBLE PRECISION,
        phot_flag SMALLINT,
        phot_prob REAL,
        zp REAL,
        psf REAL,
        skysig REAL,
        skysig_t REAL,
        gain REAL,
        season SMALLINT,
        status SMALLINT,
        ccd SMALLINT
    )
    """
    cursor.execute(create_obs_table.format(prefix))


def create_indexs(prefix, cursor):
    query = "CREATE INDEX {0}_snid on {0}_obs(snid)"
    cursor.execute(query.format(prefix))

    query = "CREATE INDEX {0}_name on {0}_obs(name)"
    cursor.execute(query.format(prefix))

    query = "CREATE INDEX {0}_obslog on {0}_obs(mjd, field, ccd, band)"
    cursor.execute(query.format(prefix))

    query = "CREATE INDEX {0}_idsearch on {0}_obs(snid, mjd, band)"
    cursor.execute(query.format(prefix))

    query = "CREATE INDEX {0}_namesearch on {0}_obs(name, mjd, band)"
    cursor.execute(query.format(prefix))

    query = "CREATE UNIQUE INDEX {0}_prop_snid on {0}_props(snid)"
    cursor.execute(query.format(prefix))

    query = "CREATE INDEX {0}_prop_name on {0}_props(name)"
    cursor.execute(query.format(prefix))


if __name__ == "__main__":
    table_prefix = 'non_Ia'
    path = '/Users/szymon/Dropbox/Projects/DES/data/Non_Ia/'
    files = os.listdir(path)

    if '.DS_Store' in files:
        files.remove('.DS_Store')
    no_files = len(files)

    conn = db.connect(host='localhost',
                      user='szymon',
                      password='supernova',
                      database='des')
    cur = conn.cursor()

    create_tables(table_prefix, cur)
    conn.commit()

    t1 = time()
    t2 = time()
    prop = []
    obs = []
    for i in range(no_files):
        if i > 0 and i % 100 == 0:
            t2 = time()
            progress(i, no_files, "{0:>6.2f}/s".format(100/(t2-t1)))
            t1 = t2

        prop, obs = read_des_photometry_file(path + files[i])

        query = "INSERT INTO "+table_prefix+"_props VALUES %s"
        cur.execute(query, (prop,))

        query = "INSERT INTO "+table_prefix+"_obs VALUES %s"
        psycopg2.extras.execute_values(cur, query, obs)
        conn.commit()

    create_indexs(table_prefix, cur)
    conn.close()
