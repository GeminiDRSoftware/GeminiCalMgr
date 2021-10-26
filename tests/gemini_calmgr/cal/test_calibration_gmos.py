from datetime import datetime

import pytest
from gemini_calmgr.cal import CalibrationGMOS
from gemini_obs_db.orm.diskfile import DiskFile
from gemini_obs_db.orm.header import Header

from gemini_obs_db import db_config as fsc
from tests.file_helper import ensure_file, dummy_ingest_file


# def _mock_sendmail(fromaddr, toaddr, message):
#     pass


def _init_gmos(session):
    session.rollback()


@pytest.mark.usefixtures("rollback")
def test_arc(monkeypatch, session):
    monkeypatch.setattr(fsc, "storage_root", "/tmp")

    _init_gmos(session)

    raw_arc_file = 'N20181113S0262.fits'
    data_file = 'N20191002S0080.fits'

    ensure_file(raw_arc_file, '/tmp')
    ensure_file(data_file, '/tmp')

    # iq = IngestQueueUtil(session, EmptyLogger())
    dummy_ingest_file(session, raw_arc_file, ['GMOS'], instrument="GMOS-N", program_id="GN-2018B-FT-207",
                      observation_id="GN-2018B-FT-207-29", data_label="GN-2018B-FT-207-29-001",
                      telescope="Gemini-North",
                      ut_datetime=datetime.strptime('2018-11-13 16:59:10', '%Y-%m-%d %H:%M:%S'),
                      observation_type='ARC', azimuth=74.99, elevation=89.99, cass_rotator_pa=176.91,
                      exposure_time=11.0, disperser="R400", wavelength_band="r", detector_binning="2x2")
    dummy_ingest_file(session, data_file, ['GMOS'], instrument="GMOS-N", program_id="GN-2019B-ENG-51",
                      observation_id="GN-2019B-ENG-51-23", data_label="GN-2019B-ENG-51-23-001",
                      telescope="Gemini-North",
                      ut_datetime=datetime.strptime('2019-10-02 11:10:09', '%Y-%m-%d %H:%M:%S'),
                      observation_type='OBJECT', ra=349.99, dec=-5.16, azimuth=235.74, elevation=49.36,
                      cass_rotator_pa=180.38, raw_cc=50, raw_wv=20, raw_bg=20, exposure_time=120.0, disperser="R400",
                      wavelength_band="r", detector_binning="2x2")
    # iq.ingest_file(raw_arc_file, "", False, True)
    # iq.ingest_file(data_file, "", False, True)

    df = session.query(DiskFile).filter(DiskFile.filename == data_file)\
        .filter(DiskFile.canonical == True).one()
    header = session.query(Header).filter(Header.diskfile_id == df.id).one()
    # cache_associations(session, header.id)

    df = session.query(DiskFile).filter(DiskFile.filename == raw_arc_file)\
        .filter(DiskFile.canonical == True).one()
    cal_header = session.query(Header).filter(Header.diskfile_id == df.id).one()

    descriptors = None
    types = list()
    c = CalibrationGMOS(session, header, descriptors, types)
    arc = c.arc()
    pass


@pytest.mark.usefixtures("rollback")
def test_dark(monkeypatch, session):
    monkeypatch.setattr(fsc, "storage_root", "/tmp")

    _init_gmos(session)

    # TODO pick different example, this  is hybridized off of a NIRI observation
    raw_dark_file = 'N20180329S0376.fits'
    data_file = 'N20180329S0134.fits'

    ensure_file(raw_dark_file, '/tmp')
    ensure_file(data_file, '/tmp')

    # iq = IngestQueueUtil(session, EmptyLogger())
    #
    # iq.ingest_file(raw_dark_file, "", False, True)
    # iq.ingest_file(data_file, "", False, True)
    dummy_ingest_file(session, raw_dark_file, ['GMOS'], instrument="GMOS-N", program_id="GN-2018A-FT-103",
                      observation_id="GN-2018A-FT-103-16", data_label="GN-2018A-FT-103-16-029",
                      telescope="Gemini-North",
                      ut_datetime=datetime.strptime('2018-03-29 18:57:51', '%Y-%m-%d %H:%M:%S'),
                      observation_type='DARK', azimuth=75, elevation=89.99, cass_rotator_pa=69.65,
                      exposure_time=60.0, disperser="MIRROR", wavelength_band=None, detector_binning="1x1"
                      )
    dummy_ingest_file(session, data_file, ['GMOS'], instrument="GMOS-N", program_id="GN-2018A-FT-103",
                      observation_id="GN-2018A-FT-103-13", data_label="GN-2018A-FT-103-13-003",
                      telescope="Gemini-North",
                      ut_datetime=datetime.strptime('2018-03-29 08:07:47', '%Y-%m-%d %H:%M:%S'),
                      observation_type='OBJECT', azimuth=162.9, elevation=51.65, cass_rotator_pa=-73.6,
                      exposure_time=60.0, disperser="MIRROR", wavelength_band="j", detector_binning="1x1"
                      )

    df = session.query(DiskFile).filter(DiskFile.filename == data_file)\
        .filter(DiskFile.canonical == True).one()
    header = session.query(Header).filter(Header.diskfile_id == df.id).one()
    # cache_associations(session, header.id)

    df = session.query(DiskFile).filter(DiskFile.filename == raw_dark_file)\
        .filter(DiskFile.canonical == True).one()
    cal_header = session.query(Header).filter(Header.diskfile_id == df.id).one()

    descriptors = None
    types = list()
    c = CalibrationGMOS(session, header, descriptors, types)
    dark = c.dark()


@pytest.mark.usefixtures("rollback")
def test_bias(monkeypatch, session):
    monkeypatch.setattr(fsc, "storage_root", "/tmp")

    _init_gmos(session)

    raw_bias_file = 'N20180122S0002.fits'
    data_file = 'N20180117S0078.fits'

    ensure_file(raw_bias_file, '/tmp')
    ensure_file(data_file, '/tmp')

    # iq = IngestQueueUtil(session, EmptyLogger())
    #
    # iq.ingest_file(raw_bias_file, "", False, True)
    # iq.ingest_file(data_file, "", False, True)
    dummy_ingest_file(session, raw_bias_file, ['GMOS'], instrument="GMOS-N", program_id="GN-CAL20180122",
                      observation_id="GN-CAL20180122-2", data_label="GN-CAL20180122-2-001",
                      telescope="Gemini-North",
                      ut_datetime=datetime.strptime('2018-01-22 14:26:54', '%Y-%m-%d %H:%M:%S'),
                      observation_type='BIAS', azimuth=75, elevation=89.99, cass_rotator_pa=24.14,
                      exposure_time=0.0, disperser="R831", wavelength_band=None, detector_binning="1x1"
                      )
    dummy_ingest_file(session, data_file, ['GMOS'], instrument="GMOS-N", program_id="GN-CAL20180117",
                      observation_id="GN-CAL20180117-25", data_label="GN-CAL20180117-25-002",
                      telescope="Gemini-North",
                      ut_datetime=datetime.strptime('2018-01-17 03:10:22', '%Y-%m-%d %H:%M:%S'),
                      observation_type='FLAT', azimuth=75, elevation=89.99, cass_rotator_pa=69.65,
                      exposure_time=1.0, disperser="MIRROR", wavelength_band='r', detector_binning="1x1"
                      )

    df = session.query(DiskFile).filter(DiskFile.filename == data_file)\
        .filter(DiskFile.canonical == True).one()
    header = session.query(Header).filter(Header.diskfile_id == df.id).one()
    # cache_associations(session, header.id)

    df = session.query(DiskFile).filter(DiskFile.filename == raw_bias_file)\
        .filter(DiskFile.canonical == True).one()
    cal_header = session.query(Header).filter(Header.diskfile_id == df.id).one()

    descriptors = None
    types = list()
    c = CalibrationGMOS(session, header, descriptors, types)
    bias = c.bias()


@pytest.mark.usefixtures("rollback")
def test_spectral_flat(monkeypatch, session):
    monkeypatch.setattr(fsc, "storage_root", "/tmp")

    _init_gmos(session)

    raw_flat_file = 'N20180101S0157.fits'
    data_file = 'N20180101S0122.fits'

    ensure_file(raw_flat_file, '/tmp')
    ensure_file(data_file, '/tmp')

    # iq = IngestQueueUtil(session, EmptyLogger())

    # iq.ingest_file(raw_flat_file, "", False, True)
    # iq.ingest_file(data_file, "", False, True)
    dummy_ingest_file(session, raw_flat_file, ['GMOS', 'SPECTROSCOPY'], instrument="GMOS-N", program_id="GN-2017B-Q-62",
                      observation_id="GN-2017B-Q-62-286", data_label="GN-2017B-Q-62-286-029",
                      telescope="Gemini-North",
                      ut_datetime=datetime.strptime('2018-01-01 09:16:57', '%Y-%m-%d %H:%M:%S'),
                      observation_type='FLAT', azimuth=-111, elevation=34.89, cass_rotator_pa=90.96,
                      exposure_time=3.25, disperser="32_mm&SXD", wavelength_band='H', detector_binning="1x1"
                      )
    dummy_ingest_file(session, data_file, ['GMOS', 'SPECTROSCOPY'], instrument="GMOS-N", program_id="GN-2017B-Q-62",
                      observation_id="GN-2017B-Q-62-286", data_label="GN-2017B-Q-62-286-003",
                      telescope="Gemini-North",
                      ut_datetime=datetime.strptime('2018-01-01 06:53:13', '%Y-%m-%d %H:%M:%S'),
                      observation_type='OBJECT', azimuth=-149.72, elevation=61.45, cass_rotator_pa=115.96,
                      exposure_time=300.0, disperser="32_mm&SXD", wavelength_band='H', detector_binning="1x1"
                      )

    df = session.query(DiskFile).filter(DiskFile.filename == data_file)\
        .filter(DiskFile.canonical == True).one()
    header = session.query(Header).filter(Header.diskfile_id == df.id).one()
    # cache_associations(session, header.id)

    df = session.query(DiskFile).filter(DiskFile.filename == raw_flat_file)\
        .filter(DiskFile.canonical == True).one()
    cal_header = session.query(Header).filter(Header.diskfile_id == df.id).one()

    # TODO make sure we are setup for spectral data
    descriptors = None
    types = list()
    c = CalibrationGMOS(session, header, descriptors, types)
    flat = c.flat()


@pytest.mark.usefixtures("rollback")
def test_imaging_flat(monkeypatch, session):
    monkeypatch.setattr(fsc, "storage_root", "/tmp")

    _init_gmos(session)
    raw_flat_file = 'N20180330S0199.fits'
    data_file = 'N20180317S0019.fits'

    ensure_file(raw_flat_file, '/tmp')
    ensure_file(data_file, '/tmp')

    # iq = IngestQueueUtil(session, EmptyLogger())
    #
    # iq.ingest_file(raw_flat_file, "", False, True)
    # iq.ingest_file(data_file, "", False, True)
    dummy_ingest_file(session, raw_flat_file, ['GMOS'], instrument="GMOS-N", program_id="GN-CAL20180330",
                      observation_id="GN-CAL20180330-19", data_label="GN-CAL20180330-19-006",
                      telescope="Gemini-North",
                      ut_datetime=datetime.strptime('2018-03-30 15:46:48', '%Y-%m-%d %H:%M:%S'),
                      observation_type='OBJECT', azimuth=-106.56, elevation=68.21, cass_rotator_pa=157.46,
                      exposure_time=64.0, disperser="MIRROR", wavelength_band='g', detector_binning="1x1"
                      )
    dummy_ingest_file(session, data_file, ['GMOS'], instrument="GMOS-N", program_id="GN-2018A-Q-118",
                      observation_id="GN-2018A-Q-118-210", data_label="GN-2018A-Q-118-210-005",
                      telescope="Gemini-North",
                      ut_datetime=datetime.strptime('2018-03-17 12:36:08', '%Y-%m-%d %H:%M:%S'),
                      observation_type='OBJECT', azimuth=-161.65, elevation=53.64, cass_rotator_pa=17.92,
                      exposure_time=300.0, disperser="MIRROR", wavelength_band='g', detector_binning="1x1"
                      )

    df = session.query(DiskFile).filter(DiskFile.filename == data_file)\
        .filter(DiskFile.canonical == True).one()
    header = session.query(Header).filter(Header.diskfile_id == df.id).one()
    # cache_associations(session, header.id)

    df = session.query(DiskFile).filter(DiskFile.filename == raw_flat_file)\
        .filter(DiskFile.canonical == True).one()
    cal_header = session.query(Header).filter(Header.diskfile_id == df.id).one()

    # TODO make sure we are setup for imaging data
    descriptors = None
    types = list()
    c = CalibrationGMOS(session, header, descriptors, types)
    flat = c.flat()


@pytest.mark.usefixtures("rollback")
def test_processed_fringe(monkeypatch, session):
    monkeypatch.setattr(fsc, "storage_root", "/tmp")

    _init_gmos(session)

    raw_flat_file = 'N20110313S0188_fringe.fits'
    data_file = 'N20110311S0296.fits'

    ensure_file(raw_flat_file, '/tmp')
    ensure_file(data_file, '/tmp')

    # iq = IngestQueueUtil(session, EmptyLogger())
    #
    # iq.ingest_file(raw_flat_file, "", False, True)
    # iq.ingest_file(data_file, "", False, True)
    dummy_ingest_file(session, raw_flat_file, ['GMOS', 'PROCESSED_FRINGE'], instrument="GMOS-N", program_id="GN-CAL20110313",
                      observation_id="GN-CAL20110313-900", data_label="GN-CAL20110313-900-188",
                      telescope="Gemini-North",
                      ut_datetime=datetime.strptime('2011-03-13 12:28:35', '%Y-%m-%d %H:%M:%S'),
                      observation_type='FRINGE', azimuth=13.74, elevation=55.41, cass_rotator_pa=158.32,
                      exposure_time=150, disperser="MIRROR", wavelength_band='Z', detector_binning="2x2"
                      )
    dummy_ingest_file(session, data_file, ['GMOS'], instrument="GMOS-N", program_id="GN-CAL20110311",
                      observation_id="GN-CAL20110311-14", data_label="GN-CAL20110311-14-016",
                      telescope="Gemini-North",
                      ut_datetime=datetime.strptime('2011-03-11 15:49:45', '%Y-%m-%d %H:%M:%S'),
                      observation_type='OBJECT', azimuth=190.57, elevation=79.76, cass_rotator_pa=-10.02,
                      exposure_time=1, disperser="MIRROR", wavelength_band='Z', detector_binning="2x2"
                      )

    df = session.query(DiskFile).filter(DiskFile.filename == data_file)\
        .filter(DiskFile.canonical == True).one()
    header = session.query(Header).filter(Header.diskfile_id == df.id).one()
    # cache_associations(session, header.id)

    df = session.query(DiskFile).filter(DiskFile.filename == raw_flat_file)\
        .filter(DiskFile.canonical == True).one()
    cal_header = session.query(Header).filter(Header.diskfile_id == df.id).one()

    descriptors = None
    types = list()
    c = CalibrationGMOS(session, header, descriptors, types)
    fringe = c.fringe(processed=True)

