"""
This is the gemini_metadata_utils module. It provides a number of utility
classes and functions for parsing the metadata in Gemini FITS files.

"""
import re
import time
import datetime
import dateutil.parser

from . import fits_storage_config

# ------------------------------------------------------------------------------
DATE_LIMIT_LOW = dateutil.parser.parse('19900101')
DATE_LIMIT_HIGH = dateutil.parser.parse('20500101')
ZERO_OFFSET = datetime.timedelta()
ONEDAY_OFFSET = datetime.timedelta(days=1)
# ------------------------------------------------------------------------------
# Compile some regular expressions here. This is fairly complex, so I've
# split it up in substrings to make it easier to follow.
# Also these substrings are used directly by the classes

# This re matches a program id like GN-CAL20091020 with no groups
calengre = r'G[NS]-(?:(?:CAL)|(?:ENG))20\d\d[01]\d[0123]\d'
# This re matches a program id like GN-2009A-Q-23 with no groups
scire = r'G[NS]-20\d\d[AB]-[A-Z]*-\d*'

# This matches a program id
progre = r'(?:^%s$)|(?:^%s$)' % (calengre, scire)

# This matches an observation id with the project id and obsnum as groups
obsre = r'((?:^%s)|(?:^%s))-(\d*)$' % (calengre, scire)

# Here are some lists of defined detector settings
gemini_gain_settings = ('high', 'low')
gemini_readspeed_settings = ('fast', 'slow')
gemini_welldepth_settings = ('Shallow', 'Deep', 'Invalid')
gemini_readmode_settings = ('Classic',
                            'NodAndShuffle',
                            'Faint',
                            'Faint_Object',
                            'Faint_Objects',
                            'Very_Faint_Objects',
                            'Medium',
                            'Medium_Object',
                            'Bright',
                            'Bright_Object',
                            'Bright_Objects',
                            'Very_Bright_Objects',
                            'Low_Background',
                            'Medium_Background',
                            'High_Background')

gemini_telescopes = {
    'gemini-north': 'Gemini-North',
    'gemini-south': 'Gemini-South'
    }
# ------------------------------------------------------------------------------
def gemini_telescope(string):
    """
    If the string argument matches a gemini telescope name, then returns the 
    "official" (ie same as in the fits headers) name of the telesope. Otherwise 
    returns None.

    Parameters
    ----------
    string : <str>
        A string representing a Gemini telescope.

    Return
    ------
    <str> or <NoneType>
        The "official" name of the telesope, as found in Gemini
        fits headers.

    """
    try:
        return gemini_telescopes.get(string.lower())
    except AttributeError:
        return None

# A utility function for matching instrument names
hqcre = re.compile(r'^[Hh][Oo][Kk][Uu][Pp]([Aa])+(\+)*[Qq][Uu][Ii][Rr][Cc]$')
gemini_instrument_dict = {
    'niri':       'NIRI',
    'nifs':       'NIFS',
    'gmos-n':     'GMOS-N',
    'gmos-s':     'GMOS-S',
    'michelle':   'michelle',
    'gnirs':      'GNIRS',
    'phoenix':    'PHOENIX',
    'texes':      'TEXES',
    'trecs':      'TReCS',
    'nici':       'NICI',
    'igrins':     'IGRINS',
    'gsaoi':      'GSAOI',
    'oscir':      'OSCIR',
    'f2':         'F2',
    'gpi':        'GPI',
    'abu':        'ABU',
    'bhros':      'bHROS',
    'hrwfs':      'hrwfs',
    'flamingos':  'FLAMINGOS',
    'cirpass':    'CIRPASS',
    'graces':     'GRACES',
    'alopeke':    'ALOPEKE',
    'zorro':      'ZORRO'
    }

def gemini_instrument(string, gmos=False, other=False):
    """
    If the string argument matches a gemini instrument name, then returns the 
    "official" (ie same as in the fits headers) name of the instrument. Otherwise
    returns None.

    If the gmos argument is True, this also recognises GMOS as a valid instrument
    name. If the 'other' is True, it will pass through unknown instrument
    names that don't look like an official instrument rather than return None

    Parameters
    ----------
    string : <str>
        A string representing a Gemini instrument name.

    gmos : <bool>
        If True, this recognises 'GMOS' as a valid instrument. 
        Default is False.

    other: <bool>
        If True, it will pass through unknown instrument names that don't look 
        like an official instrument.
        Default is False.

    Return
    ------
    retary: <str> or <NoneType>
        The "official" name of the instrument, as found in Gemini
        fits headers.

    """
    retary = None

    if other:
        retary = string

    try:
        retary = gemini_instrument_dict[string.lower()]
    except KeyError:
        if string:
            if hqcre.match(string):
                retary = 'Hokupaa+QUIRC'
            elif gmos and string.lower() == 'gmos':
                retary = 'GMOS'

    return retary

def get_fake_ut(transit="14:00:00"):
    """
    Generate the fake UT date used to name Gemini data.

    At Gemini the transit time is set to 14:00:00 local time.  For GN, that
    corresponds to midnight UT so the dataset name is not faked, but for
    GS, a transit of 14hr is totally artificial.

    Before transit, UT of last night
    After transit, UT of coming night

    Note that the transit time is not hardcoded and the code should continue
    to work if the Gemini's policy regarding the transit time were to change.

    Parameters
    ----------
    transit : <str>
        UT transit time to use.  Format: "hh:mm:ss".  Default: "14:00:00"

    Returns
    -------
    fake_ut: <str>
        Formatted date string: 'yyyymmdd'

    --------
    Original author:  Kathleen Labrie  31.10.2008  Based on CL script.
    Original   code:  gempylocal.ops_suppor.ops_utils.get_fake_ut().

    """
    # Convert the transit time string into a datetime.time object
    transittime = datetime.datetime.strptime(transit, "%H:%M:%S").time()

    # Get the local and UTC date and time
    dtlocal = datetime.datetime.now()
    dtutc = datetime.datetime.utcnow()

    # Generate the fake UT date
    if dtlocal.time() < transittime:
        # Before transit
        if dtutc.date() == dtlocal.date():
            fake_ut = ''.join(str(dtutc.date()).split('-'))
        else:
            # UT has changed before transit => fake the UT
            oneday = datetime.timedelta(days=1)
            fake_ut = ''.join(str(dtutc.date() - oneday).split('-'))
    else:
        # After or at transit
        if dtutc.date() == dtlocal.date():
            # UT has not changed yet; transit reached => fake the UT
            oneday = datetime.timedelta(days=1)
            fake_ut = ''.join(str(dtutc.date() + oneday).split('-'))
        else:
            fake_ut = ''.join(str(dtutc.date()).split('-'))

    return fake_ut


def gemini_date(string, as_datetime=False, offset=ZERO_OFFSET):
    """
    A utility function for matching dates of the form YYYYMMDD
    also supports today/tonight, yesterday/lastnight
    returns the YYYYMMDD string, or '' if not a date
    
    Parameters
    ----------
    string: <str>
        A string moniker indicating a day to convert to a gemini_date.
        One of 'today', tomorrow', 'yesterday', 'lastnight' OR an actual 
        'yyyymmdd' string.

    as_datetime: <bool>
        return is a datetime object.
        Default is False

    offset: <datetime>
        timezone offset from UT.
        default is ZERO_OFFSET

    Returns
    -------
    <datetime>, <str>, <NoneType>
        One of a datetime object; a Gemini date of the form 'YYYYMMDD';
        None.

    """
    dt_to_text = lambda x: x.date().strftime('%Y%m%d')
    dt_to_text_full = lambda x: x.strftime('%Y-%m-%dT%H:%M:%S')

    if string in {'today', 'tonight'}:
        string = get_fake_ut()
        # string = dt_to_text(datetime.datetime.utcnow())
    elif string in {'yesterday', 'lastnight'}:
        past = dateutil.parser.parse(get_fake_ut()) -  ONEDAY_OFFSET
        string = dt_to_text(past)
        # string = dt_to_text(datetime.datetime.utcnow() - ONEDAY_OFFSET)

    if len(string) == 8 and string.isdigit():
        # What we want here is to bracket from 2pm yesterday through 2pm today.
        # That is, 20200415 should convert to 2020-04-14 14:00 local time, but
        # in UTC.  The offset we are passed is what we need to add, including
        # the 2pm offset as well as the timezone adjustment to convert back to
        # UTC.
        # Example (HST): 2020-04-15 0:00 -10 hrs = 2020-04-14 2pm + 10 hrs = 2020-04-15 0:00
        # Example (CL): 2020-04-15 0:00 -10 hrs = 2020-04-14 2pm + 4 hrs = 2020-04-14 18:00
        # offset (HST) = -10 + 10 = 0
        # offset (CL) = -10 + 4 = -6
        try:
            dt = dateutil.parser.parse(string) + offset
            dt = dt.replace(tzinfo=None)
            if DATE_LIMIT_LOW <= dt < DATE_LIMIT_HIGH:
                return dt_to_text(dt) if not as_datetime else dt
        except ValueError:
            pass

    if len(string) >= 14 and 'T' in string and '=' not in string:
        # Parse an ISO style datestring, so 2019-12-10T11:22:33.444444
        try:
            # TODO this is dateutil bug #786, so for now we truncate to 6 digits
            if '.' in string:
                lastdot = string.rindex('.')
                if len(string)-lastdot > 6:
                    string = string[:lastdot-len(string)]
            # TODO end of workaround
            dt = dateutil.parser.isoparse("%sZ" % string) + offset
            # strip back out time zone as the rest of the code does not support it
            dt = dt.replace(tzinfo=None)
            if DATE_LIMIT_LOW <= dt < DATE_LIMIT_HIGH:
                return dt_to_text_full(dt) if not as_datetime else dt
        except ValueError as ve:
            pass

    return '' if not as_datetime else None


racre = re.compile(r'^([012]\d):([012345]\d):([012345]\d)(\.?\d*)$')
def ratodeg(string):
    """
    A utility function that recognises an RA: HH:MM:SS.sss
    Or a decimal degrees RA value
    Returns a float in decimal degrees if it is valid, None otherwise
    """
    re_match = racre.match(string)
    if re_match is None:
        # Not HH:MM:SS. Maybe it's decimal degrees already
        try:
            value = float(string)
            if value <= 360.0 and value >= 0.0:
                return value
        except:
            return None
        return None
    hours = float(re_match.group(1))
    mins = float(re_match.group(2))
    secs = float(re_match.group(3))
    frac = re_match.group(4)
    if frac:
        frac = float(frac)
    else:
        frac = 0.0

    secs += frac
    mins += secs/60.0
    hours += mins / 60.0

    degs = 15.0 * hours

    return degs

deccre = re.compile(r'^([+-]?)(\d\d):([012345]\d):([012345]\d)(\.?\d*)$')
def dectodeg(string):
    """
    A utility function that recognises a Dec: [+-]DD:MM:SS.sss
    Returns a float in decimal degrees if it is valid, None otherwise
    """
    re_match = deccre.match(string)
    if re_match is None:
        # Not DD:MM:SS. Maybe it's decimal degrees already
        try:
            value = float(string)
            if value >= -90.0 and value <= 90.0:
                return value
        except:
            return None
        return None
    sign = 1
    if re_match.group(1) == '-':
        sign = -1

    degs = float(re_match.group(2))
    mins = float(re_match.group(3))
    secs = float(re_match.group(4))
    frac = re_match.group(5)
    if frac:
        frac = float(frac)
    else:
        frac = 0.0

    secs += frac
    mins += secs/60.0
    degs += mins / 60.0

    degs *= sign

    return degs

def degtora(decimal):
    """
    Convert decimal degrees to RA HH:MM:SS.ss string
    """
    decimal /= 15.0
    hours = int(decimal)
    decimal -= hours

    decimal *= 60.0
    minutes = int(decimal)
    decimal -= minutes

    decimal *= 60.0
    seconds = decimal

    return "%02d:%02d:%05.2f" % (hours, minutes, seconds)

def degtodec(decimal):
    """
    Convert decimal degrees to Dec +-DD:MM:SS.ss string
    """
    sign = '+' if decimal >= 0.0 else '-'
    decimal = abs(decimal)
    degrees = int(decimal)
    decimal -= degrees

    decimal *= 60.0
    minutes = int(decimal)
    decimal -= minutes

    decimal *= 60.0
    seconds = decimal

    return "%s%02d:%02d:%05.2f" % (sign, degrees, minutes, seconds)



dmscre = re.compile(r'^([+-]?)(\d*):([012345]\d):([012345]\d)(\.?\d*)$')
def dmstodeg(string):
    """
    A utility function that recognises a generic [+-]DD:MM:SS.sss
    Returns a float in decimal degrees if it is valid, None otherwise
    """
    string = string.replace(' ', '')
    re_match = dmscre.match(string)
    if re_match is None:
        # Not DD:MM:SS. Maybe it's decimal degrees already
        try:
            value = float(string)
            return value
        except:
            return None
        return None
    sign = 1
    if re_match.group(1) == '-':
        sign = -1
    degs = float(re_match.group(2))
    mins = float(re_match.group(3))
    secs = float(re_match.group(4))
    frac = re_match.group(5)
    if frac:
        frac = float(frac)
    else:
        frac = 0.0

    secs += frac
    mins += secs/60.0
    degs += mins / 60.0

    degs *= sign

    return degs

srcre = re.compile(r"([\d.]+)\s*(d|D|degs|Degs)?")
def srtodeg(string):
    """
    Converts a Search Radius in arcseconds to decimal degrees.

    Assume arcseconds unless the string ends with 'd' or 'degs'

    Parameters
    ----------
    string : <str>
        A string representing a search radius in arcseconds

    Return
    ------
    value: <float> or <NoneType>
        A search radius in decimal degrees, None if invalid.

    """
    match = srcre.match(string)
    try:
        value = float(match.group(1))
        degs = match.group(2)
    except:
        return None

    if degs is None:
        # Value is in arcseconds, convert to degrees
        value /= 3600.0

    return value

def gemini_daterange(string, as_datetime=False, offset=ZERO_OFFSET):
    """
    A utility function for matching date ranges of the form YYYYMMDD-YYYYMMDD
    Does not support 'today', yesterday', ...

    Also this does not yet check for sensible date ordering returns the 
    YYYYMMDD-YYYYMMDD string, or '' if not a daterange.

    Parameters
    ----------
    string: <str>
        date range of the form YYYYMMDD-YYYYMMDD.

    as_datetime: <bool>
        If True, return a recognized daterange as a pair of datetime objects,
        None if it's not a daterange.
        Default is False.

    offset: <datetime>
        timezone offset from UT.
        default is ZERO_OFFSET

    Returns
    -------
    <datetime>, <str>, <NoneType>
        One of a <datetime> object; a Gemini date of the form 'YYYYMMDD';
        None.

    """
    datea, sep, dateb = string.partition('-')
    da = gemini_date(datea, as_datetime=True, offset=offset)
    db = gemini_date(dateb, as_datetime=True, offset=offset)
    if da and db:
        if as_datetime:
            return da, db

        return string

    return '' if not as_datetime else None


procmode_codes = ('sq', 'ql', 'qa')
def gemini_procmode(string):
    """
    A utility function for matching Gemini Processed Mode.

    If the string argument matches a gemini Processed Mode Code then we return the
    code else return None

    Parameters
    ----------
    string : <str>
        Name of a processed mode code.

    Return
    ------
    string : <str> or <NoneType>
        The name of the processed mode code or None.

    """
    return string if string in procmode_codes else None

obs_types = ('DARK', 'ARC', 'FLAT', 'BIAS', 'OBJECT', 'PINHOLE', 'RONCHI', 'CAL', 'FRINGE', 'MASK', 'STANDARD',
             'SLITILLUM')
def gemini_observation_type(string):
    """
    A utility function for matching Gemini ObsTypes.

    We add the unofficial values PINHOLE for GNIRS pinhole mask observations
    and RONCHI for NIFS Ronchi mask observations.

    If the string argument matches a gemini ObsType then we return the
    observation_type else return None

    Parameters
    ----------
    string : <str>
        Name of a gemini observation type as returned by AstroData
        descriptor, observation_type().

    Return
    ------
    string : <str> or <NoneType>
        The name of the observation type or None.

    """
    return string if string in obs_types else None

obs_classes = ('dayCal', 'partnerCal', 'acqCal', 'acq', 'science', 'progCal')
def gemini_observation_class(string):
    """
    A utility function matching Gemini ObsClasses.

    If the string argument matches a gemini ObsClass then we return the 
    observation_class, else return None

    Parameters
    ----------
    string : <str>
        Name of a gemini observation class as returned by AstroData
        descriptor, observation_class().

    Return
    ------
    string : <str> or <NoneType>
        The name of the observation class or None.

    """
    return string if string in obs_classes else None

reduction_states = ('RAW', 'PREPARED', 'PROCESSED_FLAT', 'PROCESSED_BIAS',
                    'PROCESSED_FRINGE', 'PROCESSED_ARC', 'PROCESSED_DARK',
                    'PROCESSED_TELLURIC', 'PROCESSED_SCIENCE', 'PROCESSED_STANDARD',
                    'PROCESSED_SLITILLUM', 'PROCESSED_UNKNOWN')
def gemini_reduction_state(string):
    """
    A utility function matching Gemini reduction states.

    If the string argument matches a gemini reduction state then we return the 
    reduction state else return None.

    Parameters
    ----------
    string : <str>
        Name of a reduction state as enumerated in 'reduction_states'.

    Return
    ------
    string : <str> or <NoneType>
        The name of reduction state or None.

    """
    return string if string in reduction_states else None

cal_types = (
    'bias', 'dark', 'flat', 'arc', 'processed_bias', 'processed_dark',
    'processed_flat', 'processed_arc', 'processed_fringe', 'pinhole_mask',
    'ronchi_mask', 'spectwilight', 'lampoff_flat', 'qh_flat', 'specphot', 
    'photometric_standard', 'telluric_standard', 'domeflat', 'lampoff_domeflat',
    'mask', 'polarization_standard', 'astrometric_standard', 'polarization_flat',
    'processed_standard', 'processed_slitillum', 'slitillum',
    )
def gemini_caltype(string):
    """
    A utility function matching Gemini calibration types.
    If the string argument matches a gemini calibration type then we return
    the calibration type, otherwise we return None

    The list of calibration types is somewhat arbitrary, it's not coupled
    to the DHS or ODB, it's more or less defined by the Fits Storage project

    These should all be lower case so as to avoid conflict with gemini_observation_type

    Parameters
    ----------
    string : <str>
        Name of a calibration type.

    Return
    ------
    string : <str> or <NoneType>
        The name of calibration type or None.

    """
    return string if string in cal_types else None

gmos_gratings = ('MIRROR', 'B480', 'B600', 'R600', 'R400', 'R831', 'R150', 'B1200')
def gmos_gratingname(string):
    """
    A utility function matching a GMOS Grating name. This could be expanded to
    other instruments, but for many instruments the grating name is too ambiguous and
    could be confused with a filter or band (eg 'H'). Also most of the use cases
    for this are where gratings are swapped.

    This function doesn't match or return the component ID.

    If the string argument matches a grating, we return the official name for
    that grating.

    Parameters
    ----------
    string : <str>
        A grating name.

    Return
    ------
    string : <str> or <NoneType>
         A grating name or None.

    """
    return string if string in gmos_gratings else ''

gmosfpmaskcre = re.compile(r'^G[NS](20\d\d)[AB](.)(\d\d\d)-(\d\d)$')
gmos_facility_plane_masks = (
    'NS2.0arcsec', 'IFU-R', 'IFU-B', 'focus_array_new', 'Imaging', '2.0arcsec',
    'NS1.0arcsec', 'NS0.75arcsec', '5.0arcsec', '1.5arcsec', 'IFU-2', 'NS1.5arcsec',
    '0.75arcsec', '1.0arcsec', '0.5arcsec'
    )
def gmos_focal_plane_mask(string):
    """
    A utility function matching gmos focal plane mask names. This could be expanded to
    other instruments. Most of the uses cases for this are for masks that are swapped.
    This function knows the names of the facility masks (long slits, NSlongslits and IFUs)
    Also it knows the form of the MOS mask names and will return a mosmask name if the string
    matches that format, even if that maskname does not actually exist

    If the string matches an focal_plane_mask, we return the focal_plane_mask.
    """

    if (string in gmos_facility_plane_masks) or gmosfpmaskcre.match(string):
        return string

    return None

fitsfilenamecre = re.compile(r'^([NS])(20\d\d)([01]\d[0123]\d)(S)(\d\d\d\d)([\d-]*)(\w*)(?P<fits>.fits)?$')
vfitsfilenamecre = re.compile(r'^(20)?(\d\d)(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(\d\d)_(\d+)(?P<fits>.fits)?$')
def gemini_fitsfilename(string):
    """
    A utility function matching Gemini data fits filenames
    If the string argument matches the format of a gemini
    data filename, with or without the .fits on the end, this
    function will return the filename, with the .fits on the end.

    If the string does not look like a filename, we return an empty string.
    """
    retval = ''
    m = fitsfilenamecre.match(string) or vfitsfilenamecre.match(string)
    if m:
        # Yes, but does it not have a .fits?
        if m.group('fits') == None:
            retval = "%s.fits" % string
        else:
            retval = string

    return retval

def gemini_binning(string):
    """
    A utility function that matches a binning string -
    for example 1x1, 2x2, 1x4
    """

    valid = '124'
    a, sep, b = string.partition('x')

    return string if (a and b and (a in valid) and (b in valid)) else ''

def percentilestring(num, type):
    """
    A utility function that converts a numeric percentile
    number, and the site condition type, into a compact string,
    eg (20, 'IQ') -> IQ20. Maps 100 onto 'Any' and gives
    'Unknown' if the num is None
    """

    if num is None:
        return 'Undefined'

    if num == 100:
        return type + "Any"

    return "%s%02d" % (type, num)

# The Gemini Data Label Class

# This re matches program_id-obsum-dlnum - ie a datalabel,
# With 3 groups - program_id, obsnum, dlnum
# This also allows for an optional -blah on the end (processed biases etc)

dlcre = re.compile(r'^((?:%s)|(?:%s))-(\d*)-(\d*)(?:-(\w*))?$' % (calengre, scire))

class GeminiDataLabel:
    """
    The Gemini Data Label Class. This class parses various
    useful things from a Gemini Data Label.

    Simply instantiate the class with the datalabel, then
    reference the following data members:

    * datalabel: The datalabel provided. If the class could cannot
                             make sense of the datalabel string passed in,
                             this field will be empty.
    * projectid: The Project ID
    * observation_id: The Observation ID
    * obsnum: The Observation Number within the project
    * dlnum: The Dataset Number within the observation
    * project: A GeminiProgram object for the project this is part of
    """
    datalabel = ''
    projectid = ''
    observation_id = ''
    obsnum = ''
    dlnum = ''
    extension = ''
    project = ''

    def __init__(self, dl):
        self.datalabel = dl
        self.projectid = ''
        self.observation_id = ''
        self.obsnum = ''
        self.dlnum = ''
        self.extension = ''
        self.datalabel_noextension = ''
        if self.datalabel:
            self.parse()

    def parse(self):
        dlm = dlcre.match(self.datalabel)
        if dlm:
            self.projectid = dlm.group(1)
            self.obsnum = dlm.group(2)
            self.dlnum = dlm.group(3)
            self.extension = dlm.group(4)
            self.project = GeminiProgram(self.projectid)
            self.observation_id = '%s-%s' % (self.projectid, self.obsnum)
            self.datalabel_noextension = '%s-%s-%s' % (self.projectid, self.obsnum, self.dlnum)
            self.valid = True
        else:
            # Match failed - Null the datalabel field
            self.datalabel = ''
            self.valid = False

class GeminiObservation:
    """
    The GeminiObservation class parses an observation ID

    Simply instantiate the class with an observation id string
    then reference the following data members:

    * observation_id: The observation ID provided. If the class cannot
                     make sense of the string passed in, this field will
                     be empty
    * project: A GeminiProgram object for the project this is part of
    * obsnum: The observation numer within the project
    """
    observation_id = ''
    program = ''
    obsnum = ''

    def __init__(self, observation_id):
        if observation_id:
            match = re.match(obsre, observation_id)
            if match:
                self.observation_id = observation_id
                self.program = GeminiProgram(match.group(1))
                self.obsnum = match.group(2)
                self.valid = True
            else:
                self.observation_id = ''
                self.project = ''
                self.obsnum = ''
                self.valid = False
        else:
            self.observation_id = ''


class GeminiProgram:
    """
    The GeminiProgram class parses a Gemini Program ID and provides
    various useful information deduced from it.

    Simply instantiate the class with a program ID string, then
    reference the following data members:

    * program_id: The program ID passed in.
    * valid: a Boolean saying whether the program_id is a valid standard format
    * is_cal: a Boolean that is true if this is a CAL program
    * is_eng: a Boolean that is true if this is an ENG program
    * is_q: a Boolean that is true if this is a Queue program
    * is_c: a Boolean that is true if this is a Classical program
    * is_sv: a Boolean that is true if this is an SV (Science Verification) program
    * is_qs: a Boolean that is true if this is an QS (Quick Start) program
    * is_dd: a Boolean that is true if this is an DD (Directors Discretion) program
    * is_lp: a Boolean that is true if this is an LP (Large Program) program
    * is_ft: a Boolean that is true if this is an FT (Fast Turnaround) program
    * is_ds: a Boolean that is true if this is an DS (Demo Science) program

    This could be easily expanded to extract semester, hemisphere, program number etc
    if required.
    """
    program_id = None
    valid = None
    is_cal = False
    is_eng = False
    is_q = False
    is_c = False
    is_sv = False
    is_qs = False
    is_dd = False
    is_lp = False
    is_ft = False
    is_ds = False


    def __init__(self, program_id):
        self.program_id = program_id

        # Check for the CAL / ENG form
        ec_match = re.match(r"^(G[NS])-((?:CAL)|(?:ENG))(20[012]\d[01]\d[0123]\d)$", program_id)
        sci_match = re.match(r"^(G[NS])-(20[012]\d[AB])-(Q|C|SV|QS|DD|LP|FT|DS|ENG|CAL)-(\d+)$", program_id)
        if ec_match:
            # Valid eng / cal form
            self.valid = True
            self.is_eng = ec_match.group(2) == 'ENG'
            self.is_cal = ec_match.group(2) == 'CAL'
        elif sci_match:
            # Valid science form
            self.valid = True
            self.is_q = sci_match.group(3) == 'Q'
            self.is_c = sci_match.group(3) == 'C'
            self.is_sv = sci_match.group(3) == 'SV'
            self.is_ft = sci_match.group(3) == 'FT'
            self.is_ds = sci_match.group(3) == 'DS'
            self.is_eng = sci_match.group(3) == 'ENG'
            self.is_cal = sci_match.group(3) == 'CAL'

            # If the program number contained leading zeros, strip them out of
            # the official program_id
            if sci_match.group(4)[0] == '0':
                prog_num = int(sci_match.group(4))
                self.program_id = "%s-%s-%s-%s" % (sci_match.group(1),
                                                   sci_match.group(2),
                                                   sci_match.group(3),
                                                   prog_num)

        else:
            # Not a valid format. Probably some kind of engineering test program
            # that someone just made up.
            self.valid = False
            self.is_eng = True

def get_date_offset():
    """
    This function is used to add set offsets to the dates. The aim is to get the
    "current date" adjusting for the local time, taking into account the different
    sites where Gemini is based.

    """
    if fits_storage_config.use_as_archive:
        return ZERO_OFFSET

    # Calculate the proper offset to add to the date
    # We consider the night boundary to be 14:00 local time
    # This is midnight UTC in Hawaii, completely arbitrary in Chile
    zone = time.altzone if time.daylight else time.timezone
    #print datetime.timedelta(hours=16)
    #print datetime.timedelta(seconds=zone)
    #print ONEDAY_OFFSET

    #return datetime.timedelta(hours=16) + datetime.timedelta(seconds=zone) - ONEDAY_OFFSET
    # I think this interacted with the Chile today changes to cause the missing starts in Hawaii for summary
    return datetime.timedelta(hours=14) + datetime.timedelta(seconds=zone) - ONEDAY_OFFSET

def get_time_period(start, end=None, as_date=False):
    startdt = gemini_date(start, offset=get_date_offset(), as_datetime=True)
    if end is None:
        enddt = startdt
    else:
        enddt = gemini_date(end, offset=get_date_offset(), as_datetime=True)
        # Flip them round if reversed
        if startdt > enddt:
            startdt, enddt = enddt, startdt
    if enddt == startdt:
        # only add day offset if we are identical
        enddt += ONEDAY_OFFSET

    if as_date:
        return startdt.date(), enddt.date()

    return startdt, enddt

def gemini_time_period_from_range(rng, as_date=False):
    a, _, b = gemini_daterange(rng).partition('-')
    return get_time_period(a, b, as_date)

def gemini_semester(date):
    """ Return the semester name that contains date """
    if date.month >= 2 and date.month <= 7:
        letter = 'A'
        year = date.year
    else:
        letter = 'B'
        if date.month == 1:
            year = date.year - 1
        else:
            year = date.year

    return str(year) + letter

semester_re = r'(20\d\d)([AB])'
def previous_semester(semester):
    """
    Given a semester string, e.g., 2016A, return the previous semester.
    E.g., 2015B

    Parameters
    ----------
    semester: <str> Semester name

    Returns
    -------
    semester - 1, <str>,
        the semester name prior to the passed semester.

    """
    m = re.match(semester_re, semester)
    if m is None:
        return None
    year = m.group(1)
    bsem = m.group(2) == 'B'

    if bsem:
        return year + 'A'
    else:
        year = int(year) - 1
        return str(year) + 'B'


def site_monitor(string):
    """
    Parameters
    ----------
    string: <str>
        The name of the instrument that is a sky monitor. Currently, this
        supports only GS_ALLSKYCAMERA. The string will generally be that
        returned by the astrodata descriptor, ad.instrument().

    Return
    ------
    yes: <bool>
        Returns True when GS_ALLSKYCAMERA is passed.

    """
    if string == 'GS_ALLSKYCAMERA':
        return True
    else:
        return False
