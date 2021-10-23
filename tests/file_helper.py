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


class MockAstroData(object):
    def __init__(self, tags, instrument=None, program_id=None, observation_id=None, data_label=None,
                 telescope=None, ut_datetime=None):
        self.tags = tags
        self.instrument = instrument
        self.program_id = program_id
        self.observation_id = observation_id
        self.data_label = data_label
        self._telescope = telescope
        self.ut_datetime = ut_datetime

    def telescope(self):
        return self._telescope


def dummy_ingest_file(filename, tags, instrument=None, program_id=None, observation_id=None, data_label=None,
                      telescope=None, ut_datetime=None):
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
                                       telescope=telescope, ut_datetime=ut_datetime)
    # astrodata.open(diskfile.fullpath())
    dfr = DiskFileReport()  # diskfile, True, True)
    header = Header(diskfile)
    name, instClass = instrument_table[header.instrument]
    entry = instClass(header, diskfile.ad_object)
