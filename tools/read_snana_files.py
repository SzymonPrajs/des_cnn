"""
Collection of useful tools used for reading and parsing various types of SNANA
data files including DES SN light curve, DES fake SN light curves and SIMLIB
files
"""
from .des_tools import mjd_to_season, get_status_quality


def split_textfile_line(text_line):
    """
    Clear a line of text from spaces and linebreak characters the split to list
    """
    out = text_line.rstrip(' \n')
    while '  ' in out:
        out = out.replace('  ', ' ')
    out = out.split(' ')

    return out[0][:-1], out[1:]


def parse_snana_line(snana_line):
    """
    Parse a line from SNANA

    Parameters
    ----------
    """
    key, value = split_textfile_line(snana_line)

    if key == '':
        return None, value

    if key == 'OBS':
        value = value[:12]
        value.append(mjd_to_season(float(value[0])))
        value.append(get_status_quality(int(value[5])))

    return key, value


def read_fake_file(file_path):
    """
    Read a single fake file

    Parameters
    ----------
    file_path : str
        Path to the fake SN data file
    """
    obs = []

    with open(file_path, 'r') as fake_file:
        for fake_line in fake_file:
            key, value = parse_snana_line(fake_line)

            if key == 'OBS':
                obs.append(tuple(value))

    return obs
