# init file for package

from sqlalchemy import join, desc

from .calibration import Calibration
from .calibration_gmos import CalibrationGMOS
from .calibration_niri import CalibrationNIRI
from .calibration_gnirs import CalibrationGNIRS
from .calibration_nifs import CalibrationNIFS
from .calibration_michelle import CalibrationMICHELLE
from .calibration_f2 import CalibrationF2
from .calibration_gsaoi import CalibrationGSAOI
from .calibration_nici import CalibrationNICI
from .calibration_gpi import CalibrationGPI
from .calibration_ghost import CalibrationGHOST

from gemini_obs_db.file import File
from gemini_obs_db.diskfile import DiskFile
from gemini_obs_db.header import Header


"""
Mapping from instrument name to the appropriate `Calibration` implementation.
"""
inst_class = {
    'F2':       CalibrationF2,
    'GHOST':    CalibrationGHOST,
    'GMOS':     CalibrationGMOS,
    'GMOS-S':   CalibrationGMOS,
    'GMOS-N':   CalibrationGMOS,
    'GNIRS':    CalibrationGNIRS,
    'GPI':      CalibrationGPI,
    'GSAOI':    CalibrationGSAOI,
    'michelle': CalibrationMICHELLE,
    'NICI':     CalibrationNICI,
    'NIFS':     CalibrationNIFS,
    'NIRI':     CalibrationNIRI,
}


def get_cal_object(session, filename, header=None, procmode=None, descriptors=None, types=None, full_query=False):
    """
    This function returns an appropriate calibration object for the given dataset.
    Need to pass in a sqlalchemy session that should already be open, the class will not close it
    Also pass either a filename or a header object instance.

    Parameters
    ----------

    session : :class:`sqlalchemy.orm.session.Session`
        The open session to use for querying data

    filename : string
        The filename to search for.  This is required if header and descriptors are not provided.

    header : :class:`fits_storage.orm.header.Header`, optional
        A header to get the appropriate calibration object for

    descriptors : dict, optional
        A dictionary of descriptive fields to use in the calibration object.

    types : list of string, optional
        The types of this data, such as `MOS`

    full_query :boolean, defaults to `False`
        If `True`, query will pull in the `DiskFile` and `File` records as well as the `Header`

    procmode : str, defaults to None
        Either 'sq', 'ql', or None to indicate the proc mode.  'ql' will also accept 'sq' calibrations as matches

    Returns
    -------
    :class:`fits_storage.cal.calibration.Calibration`
        An instance of the appropriate calibration object, initialized with the passed header (or match by filename)
        and descriptors.
    """

    # Did we get a header?
    if header is None and descriptors is None:
        # Get the header object from the filename
        query = session.query(Header).select_from(join(Header, join(DiskFile, File)))
        query = query.filter(File.name == filename).order_by(desc(DiskFile.lastmod))
        header = query.first()

    # OK, now instantiate the appropriate Calibration object and return it
    if header:
        instrument = header.instrument
    else:
        instrument = descriptors['instrument']

    cal_class = inst_class.get(instrument, Calibration)
    cal = cal_class(session, header, descriptors, types, procmode=procmode, full_query=full_query)

    return cal
