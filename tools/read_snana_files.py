"""
Collection of useful tools used for reading and parsing various types of SNANA
data files including DES SN light curve, DES fake SN light curves and SIMLIB
files
"""
from .des_tools import mjd_to_season, get_status_quality


def split_textfile_line(text_line):
    """
    Clear a line of text from spaces and linebreak characters the split to
    list. This function returns row pairs of (key, values) and should not be
    used to on its own. See `parse_snana_line()` for a clean output
    Parameters
    ----------
    text_line : str
        Raw text line extracted from a SNANA formatted file.

    Returns
    -------
    key : str
        First word in the line, in SNANA formatted files this acts as a key

    values : list
        List of the remaining words associated with a `key`.
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
    snana_line : str
        Raw text line extracted from a SNANA formatted file.

    Returns
    -------
    key : str
        Key indicating the type of output returned by `value`. Returns the
        first entry in the `value` array unless a special key where the `value`
        list gets modified. Special keys: OBS, HOSTGAL_MAG, HOSTGAL_SB_FLUXCAL,
        PRIVATE(DES_fake_z)

    value : list
        List of values associated with a `key`. Always returns a list even if
        only one element is present in it.
    """
    key, value = split_textfile_line(snana_line)

    if key == '':
        return None, value

    if key == 'OBS':
        value = value[:12]
        value.append(mjd_to_season(float(value[0])))
        value.append(get_status_quality(int(value[5])))

    elif key == 'HOSTGAL_MAG':
        value = value[0:4]

    elif key == 'HOSTGAL_SB_FLUXCAL':
        value = value[0:4]

    elif key == 'PRIVATE(DES_fake_z)':
        key = 'HOSTGAL_SPECZ'
        value = value[:1]

    elif key == 'HOSTGAL_PHOTOZ':
        value = [value[0], value[2]]

    else:
        value = value[:1]

    return key, value


def read_des_photometry_file(file_path):
    """
    Read a single fake file

    Parameters
    ----------
    file_path : str
        Path to the fake SN data file

    Returns
    -------
    observations : list
        List of tuples containing: (mjd, band, field, flux, fluxerr, phot_prob,
            zp, psf, skysig, template_skysig, gain, season, phot_quality, ccd)

    properties : tuple
        List containing: (snid, IAUC_name, fake_flag, field, ccd_num,
            num_detected, spec_z, phot_z, phot_z_err, host_mag x4[griz],
            host_SB_fluxcal x4[griz])
    """
    observations = []
    properties = [None]*17
    properties[1] = 'None'  # Set default if IAUC key not present

    with open(file_path, 'r') as fake_file:
        for fake_line in fake_file:
            key, value = parse_snana_line(fake_line)

            if key == 'SNID':
                properties[0] = value[0]

            elif key == 'IAUC':
                properties[1] = value[0]

            elif key == 'FAKE':
                properties[2] = value[0]

            elif key == 'PRIVATE(DES_ccdnum)':
                properties[4] = value[0]

            elif key == 'PRIVATE(DES_numepochs)':
                properties[5] = value[0]

            elif key == 'HOSTGAL_SPECZ':
                properties[6] = value[0]

            elif key == 'HOSTGAL_PHOTOZ':
                properties[7:9] = value

            elif key == 'HOSTGAL_MAG':
                properties[9:13] = value

            elif key == 'HOSTGAL_SB_FLUXCAL':
                properties[13:17] = value

            elif key == 'OBS':
                value.append(properties[4])  # CCD number
                observations.append(tuple(value))

        properties[3] = observations[0][2]

    return tuple(properties), observations
