"""
This module holds the CalibrationNIRI class
"""
import datetime

from gemini_obs_db.diskfile import DiskFile
from gemini_obs_db.header import Header
from gemini_obs_db.niri import Niri
from .calibration import Calibration, not_processed

from sqlalchemy import or_


class CalibrationNIRI(Calibration):
    """
    This class implements a calibration manager for NIRI.
    It is a subclass of Calibration
    """
    instrClass = Niri
    instrDescriptors = (
        'data_section',
        'read_mode',
        'well_depth_setting',
        'filter_name',
        'camera',
        'focal_plane_mask',
        'disperser'
        )

    def _parse_section(self, section):
        if section is not None and section[0] in '([':
            arr = section[1:-1].split(",")
            if len(arr) == 4:
                x1 = arr[0].strip()
                x2 = arr[1].strip()
                y1 = arr[2].strip()
                y2 = arr[3].strip()
                return "Section(x1=%s, x2=%s, y1=%s, y2=%s)" % (x1, x2, y1, y2)
        return section

    def set_applicable(self):
        # Return a list of the calibrations applicable to this NIRI dataset
        self.applicable = []

        # Science Imaging OBJECTs require a DARK and FLAT, and photometric_standard
        if (self.descriptors['observation_type'] == 'OBJECT' and
                self.descriptors['spectroscopy'] == False):
            self.applicable.append('processed_flat')
            if self.descriptors['observation_class'] == 'partnerCal':
                if self.descriptors['filter_name'] not in ['Lprime_G0207', 'Mprime_G0208', 'Bra_G0238', 'Bracont_G0237',
                                                           'hydrocarb_G0231']:
                    self.applicable.append('flat')
            if self.descriptors['observation_class'] == 'science':
                self.applicable.append('dark')
                # No flats for L', M' Br(alpha) or Br(alpha) continuum, hydrocarbon as per AS 20130514, confirmed 20160516
                if self.descriptors['filter_name'] not in ['Lprime_G0207', 'Mprime_G0208', 'Bra_G0238', 'Bracont_G0237', 'hydrocarb_G0231']:
                    self.applicable.append('flat')
                self.applicable.append('photometric_standard')

        # Imaging Lamp-on Flat fields require a lampoff_flat
        if (self.descriptors['observation_type'] == 'FLAT' and
                self.descriptors['spectroscopy'] == False and
                self.descriptors['gcal_lamp'] != 'Off'):
            self.applicable.append('lampoff_flat')

        # Spectroscopy OBJECTs require a flat and arc
        if self.descriptors['observation_type'] == 'OBJECT' and self.descriptors['spectroscopy'] == True:
            self.applicable.append('flat')
            self.applicable.append('arc')
            # science Spectroscopy OBJECTs require a telluric
            if self.descriptors['observation_class'] == 'science':
                self.applicable.append('telluric_standard')
                self.applicable.append('processed_flat')

    def dark(self, processed=False, howmany=None):
        """
        Method to find the darks

        This will find NIRI darks with a matching read mode, well depth setting, and coadds.
        It also matches on the data_section and exposure time (within 0.01s for float fuzzyness)
        and within 180 days.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw darks.
        howmany : int, default 1 if processed, else 10
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 10

        return (
            self.get_query()
                .dark(processed)
                .add_filters(Niri.data_section == self._parse_section(self.descriptors['data_section']))
                .match_descriptors(Niri.read_mode,
                                   Niri.well_depth_setting,
                                   Header.coadds)
                # Exposure time must match to within 0.01 (nb floating point match). Coadds must also match.
                # nb exposure_time is really exposure_time * coadds, but if we're matching both, that doesn't matter
                .tolerance(exposure_time = 0.01)
                # Absolute time separation must be within 6 months
                .max_interval(days=180)
                .all(howmany)
            )

    def flat(self, processed=False, howmany=None):
        """
        Method to find the flats

        This will find NIRI flats with a matching well depth setting, filter name, camera,
        focal plane mask, and disperser.
        It also matches on the data_section looks for gcal_lamp of IRhigh, IRlow or QM.
        If this is spectroscopy, it matches on a central wavelength within 0.0001 microns.
        It only matches within 180 days.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw flats.
        howmany : int, default 1 if processed, else 10
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 10

        return (
            self.get_query()
                .flat(processed)
                # GCAL lamp should be on - these flats will then require lamp-off flats to calibrate them
                .add_filters(or_(Header.gcal_lamp == 'IRhigh', Header.gcal_lamp == 'IRlow', Header.gcal_lamp == 'QH'))
                .add_filters(Niri.data_section == self._parse_section(self.descriptors['data_section']))
                # Must totally match: data_section, well_depth_setting, filter_name, camera, focal_plane_mask, disperser
                # Update from AS 20130320 - read mode should not be required to match, but well depth should.
                .match_descriptors(Niri.well_depth_setting,
                                   Niri.filter_name,
                                   Niri.camera,
                                   Niri.focal_plane_mask,
                                   Niri.disperser)
                .tolerance(central_wavelength = 0.001, condition=self.descriptors['spectroscopy'])
                # Absolute time separation must be within 6 months
                .max_interval(days=180)
                .all(howmany)
            )

    def arc(self, processed=False, howmany=None):
        """
         Method to find the arcs

         This will find NIRI arcs with a matching filter name, camera,
         focal plane mask, and disperser.
         It also matches on the data_section and,
         if this is spectroscopy, it matches on a central wavelength within 0.0001 microns.
         It only matches within 180 days.

         Parameters
         ----------

         processed : bool
             Indicate if we want to retrieve processed or raw arcs.
         howmany : int, default 1
             How many matches to return

         Returns
         -------
             list of :class:`fits_storage.orm.header.Header` records that match the criteria
         """
        # Default number to associate
        howmany = howmany if howmany else 1

        return (
            self.get_query()
                .arc(processed)
                .add_filters(Niri.data_section == self._parse_section(self.descriptors['data_section']))
                .match_descriptors(Niri.filter_name,
                                   Niri.camera,
                                   Niri.focal_plane_mask,
                                   Niri.disperser)
                .tolerance(central_wavelength = 0.001)
                # Absolute time separation must be within 6 months
                .max_interval(days=180)
                .all(howmany)
            )

    @not_processed
    def lampoff_flat(self, processed=False, howmany=None):
        """
         Method to find the lamp off flats

         This will find NIRI lamp off flats with a matching well depth setting,
         filter name, camera, and disperser.
         It also matches on the data_section and a gcal_lamp of "Off"
         It only matches within 1 hour.

         Parameters
         ----------

         processed : bool
             Indicate if we want to retrieve processed or raw lamp off flats.
         howmany : int, default 10
             How many matches to return

         Returns
         -------
             list of :class:`fits_storage.orm.header.Header` records that match the criteria
         """
        # Default number to associate
        howmany = howmany if howmany else 10

        return (
            self.get_query()
                .flat()
                .add_filters(Header.gcal_lamp == 'Off')
                .add_filters(Niri.data_section == self._parse_section(self.descriptors['data_section']))
                .match_descriptors(Niri.well_depth_setting,
                                   Niri.filter_name,
                                   Niri.camera,
                                   Niri.disperser)
                # Absolute time separation must be within 1 hour of the lamp on flats
                .max_interval(seconds=3600)
                .all(howmany)
            )

    @not_processed
    def photometric_standard(self, processed=False, howmany=None):
        """
         Method to find the photometric standards

         This will find NIRI photometric standards with a matching
         filter name and camera.  It also expects the match to be
         an OBJECT, not spectroscopy, and have phot_standard set to
         True.
         It only matches within 1 day.

         Parameters
         ----------

         processed : bool
             Indicate if we want to retrieve processed or raw photometric standards.
         howmany : int, default 10
             How many matches to return

         Returns
         -------
             list of :class:`fits_storage.orm.header.Header` records that match the criteria
         """
        # Default number to associate
        howmany = howmany if howmany else 10

        return (
            self.get_query()
                # Phot standards are OBJECT imaging frames
                .raw().OBJECT().spectroscopy(False)
                # Phot standards are phot standards
                .add_filters(Header.phot_standard == True)
                .match_descriptors(Niri.filter_name,
                                   Niri.camera)
                # Absolute time separation must be within 24 hours of the science
                .max_interval(days=1)
                .all(howmany)
            )

    @not_processed
    def telluric_standard(self, processed=False, howmany=None):
        """
         Method to find the telluric standards

         This will find NIRI telluric standards with a matching
         filter name, camera, focal plane mask, and disperser.  It also expects the match to be
         an OBJECT and partnerCal.  The central wavelength needs to match within 0.001 microns.
         It only matches within 1 day.

         Parameters
         ----------

         processed : bool
             Indicate if we want to retrieve processed or raw telluric standards.
         howmany : int, default 10
             How many matches to return

         Returns
         -------
             list of :class:`fits_storage.orm.header.Header` records that match the criteria
         """
        # Default number to associate
        howmany = howmany if howmany else 10

        return (
            self.get_query()
                # Telluric standards are OBJECT spectroscopy partnerCal frames
                .telluric_standard(OBJECT=True, partnerCal=True)
                .match_descriptors(Niri.filter_name,
                                   Niri.camera,
                                   Niri.focal_plane_mask,
                                   Niri.disperser)
                .tolerance(central_wavelength = 0.001)
                # Absolute time separation must be within 24 hours of the science
                .max_interval(days=1)
                .all(howmany)
            )
