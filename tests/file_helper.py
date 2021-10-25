#! python


import os
from datetime import datetime

# import fits_storage
import gemini_obs_db
# from fits_storage import fits_storage_config
from gemini_obs_db import db_config

# have to override this here for now, before diskfile import
db_config.storage_root = '/tmp/jenkins_pytest/dataflow'

from gemini_obs_db.orm.diskfile import DiskFile
from gemini_obs_db.orm.f2 import F2
from gemini_obs_db.orm.file import File
from gemini_obs_db.orm.gmos import Gmos
from gemini_obs_db.orm.gnirs import Gnirs
from gemini_obs_db.orm.gpi import Gpi
from gemini_obs_db.orm.gsaoi import Gsaoi
from gemini_obs_db.orm.header import Header
from gemini_obs_db.orm.michelle import Michelle
from gemini_obs_db.orm.nici import Nici
from gemini_obs_db.orm.nifs import Nifs
from gemini_obs_db.orm.niri import Niri


def ensure_file(filename, path=None):
    import requests
    import bz2

    # Thinking of dropping the arg, fsc storage root is already set properly
    path = None

    if path is None:
        db_config.storage_root = '/tmp/jenkins_pytest/dataflow'
        path = db_config.storage_root

    if os.path.isfile(os.path.join(path, filename)):
        return

    # Make sure the folder exists.  On Jenkins, it can be transient
    os.makedirs(path, exist_ok=True)

    getfile = filename
    if getfile.endswith(".bz2"):
        getfile = getfile[:-4]
    url = 'https://archive.gemini.edu/file/%s' % getfile
    r = requests.get(url, allow_redirects=True)
    if r.status_code == 200:
        diskfile = os.path.join(path, filename)
        if diskfile.endswith(".bz2"):
            bz2.BZ2File(diskfile, 'w').write(r.content)
        else:
            open(diskfile, 'wb').write(r.content)


def mock_get_file_size(path):
    return 0


def mock_get_file_md5(path):
    return ''


def mock_get_lastmod(path):
    return datetime.now()


def mock_populate_fits(hdr, df, log):
    pass


def mock_populate(ftxthdr, df):
    pass


def setup_mock_file_stuff(monkeypatch):
    monkeypatch.setattr(gemini_obs_db.orm.diskfile.DiskFile, 'get_file_size', mock_get_file_size)
    monkeypatch.setattr(gemini_obs_db.orm.diskfile.DiskFile, 'get_file_md5', mock_get_file_md5)
    monkeypatch.setattr(gemini_obs_db.orm.diskfile.DiskFile, 'get_lastmod', mock_get_lastmod)
    monkeypatch.setattr(gemini_obs_db.orm.header.Header, 'populate_fits', mock_populate_fits)
    monkeypatch.setattr(gemini_obs_db.orm.fulltextheader.FullTextHeader, 'populate', mock_populate)


class DiskFileReport(object):
    pass


_default_observation_class = {
    'OBJECT': 'science',
    'ARC': 'dayCal',
    'BIAS': 'dayCal',
    'FLAT': 'dayCal'
}


class MockAstroData(object):
    def __init__(self, tags, instrument=None, program_id=None, observation_id=None, data_label=None,
                 telescope=None, ut_datetime=None, observation_type=None, observation_class=None,
                 object='', ra=None, dec=None, azimuth=None, elevation=None, cass_rotator_pa=None,
                 raw_iq=None, raw_cc=None, raw_wv=None, raw_bg=None, requested_iq=100,
                 requested_cc=100, requested_wv=100, requested_bg=100, exposure_time=None,
                 disperser=None, wavelength_band=None, detector_binning=None, filter_name=None,
                 focal_plane_mask=None
                 ):
        self.tags = tags if tags is not None else list()
        self.instrument = instrument
        self.program_id = program_id
        self.observation_id = observation_id
        self.data_label = data_label
        self._telescope = telescope
        self.ut_datetime = ut_datetime
        self.local_time = ut_datetime  # fake it out
        self.observation_type = observation_type
        if observation_class is not None:
            self.observation_class = observation_class
        else:
            if observation_type in _default_observation_class:
                self.observation_class = _default_observation_class[observation_type]
            else:
                self.observation_class = None
        self.object = object
        self.ra = ra
        self.dec = dec
        self.azimuth = azimuth
        self.elevation = elevation
        self.cass_rotator_pa = cass_rotator_pa
        self.raw_iq = raw_iq
        self.raw_cc = raw_cc
        self.raw_wv = raw_wv
        self.raw_bg = raw_bg
        self.requested_iq = requested_iq
        self.requested_cc = requested_cc
        self.requested_wv = requested_wv
        self.requested_bg = requested_bg
        self.exposure_time = exposure_time
        self.disperser = lambda: disperser
        self.wavelength_band = wavelength_band
        self.detector_binning = detector_binning
        self.detector_x_bin = None
        self.detector_y_bin = None
        if detector_binning is not None:
            xb, yb = detector_binning.split('x')
            if xb and yb:
                self.detector_x_bin = int(xb)
                self.detector_y_bin = int(yb)
        self.gain_setting = 'low'
        self._read_speed_setting = 'slow'
        self.well_depth_setting = None
        self.detector_roi_setting = "Central Spectrum"
        self.coadds = None
        self.phu = {
            'AOFOLD': ''
        }
        self.wavefront_sensor = "OIWFS"
        self.qa_state = "Undefined"
        self.gcal_lamp = None
        self._filter_name = filter_name
        self._focal_plane_mask = focal_plane_mask

    def read_speed_setting(self):
        return self._read_speed_setting

    def filter_name(self, stripID=False, pretty=False):
        return self._filter_name

    def focal_plane_mask(self):
        return self._focal_plane_mask

    def telescope(self):
        return self._telescope

    def pupil_mask(self, stripID=False, pretty=False):
        """
        Returns the name of the pupil mask used for the observation

        Returns
        -------
        str
            the pupil mask
        """
        try:
            filter3 = self.phu['FILTER3']
        except KeyError:
            return None
        if filter3.startswith('pup'):
             return gmu.removeComponentID(filter3) if pretty or stripID \
                else filter3
        else:
            return 'MIRROR'


def dummy_ingest_file(filename, tags, instrument=None, program_id=None, observation_id=None, data_label=None,
                      telescope=None, ut_datetime=None, observation_type=None, object='', ra=None, dec=None,
                      azimuth=None, elevation=None, cass_rotator_pa=None, raw_iq=None,
                      raw_cc=None, raw_wv=None, raw_bg=None, requested_iq=100, requested_cc=100, requested_wv=100,
                      requested_bg=100, exposure_time=None, disperser=None, wavelength_band=None,
                      detector_binning=None):
    instrument_table = {
        # Instrument: (Name for debugging, Class)
        'F2': ("F2", F2),
        'GMOS-N': ("GMOS", Gmos),
        'GMOS-S': ("GMOS", Gmos),
        'GNIRS': ("GNIRS", Gnirs),
        'GPI': ("GPI", Gpi),
        'GSAOI': ("GSAOI", Gsaoi),
        'michelle': ("MICHELLE", Michelle),
        'NICI': ("NICI", Nici),
        'NIFS': ("NIFS", Nifs),
        'NIRI': ("NIRI", Niri),
    }

    fileobj = File(filename)
    path = ""
    diskfile = DiskFile(fileobj, filename, path)
    diskfile.ad_object = MockAstroData(tags, instrument=instrument, program_id=program_id,
                                       observation_id=observation_id, data_label=data_label,
                                       telescope=telescope, ut_datetime=ut_datetime,
                                       observation_type=observation_type, object=object,
                                       ra=ra, dec=dec, azimuth=azimuth, elevation=elevation,
                                       cass_rotator_pa=cass_rotator_pa, raw_iq=raw_iq,
                                       raw_cc=raw_cc, raw_wv=raw_wv, raw_bg=raw_bg,
                                       requested_cc=requested_cc, requested_iq=requested_iq, requested_wv=requested_wv,
                                       requested_bg=requested_bg, exposure_time=exposure_time, disperser=disperser,
                                       wavelength_band=wavelength_band, detector_binning=detector_binning)
    print(f"Instrument in dummy_ingest_file: {instrument}")
    print(f"Astrodata version: {diskfile.ad_object.instrument}")
    # astrodata.open(diskfile.fullpath())
    dfr = DiskFileReport()  # diskfile, True, True)
    header = Header(diskfile)
    print(f"Parsed header, it has instrument {header.instrument}")
    name, instClass = instrument_table[header.instrument]
    entry = instClass(header, diskfile.ad_object)
