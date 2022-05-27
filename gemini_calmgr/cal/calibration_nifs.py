"""
This module holds the CalibrationNIFS class
"""
import datetime

from sqlalchemy import or_

from gemini_obs_db.orm.diskfile import DiskFile
from gemini_obs_db.orm.header import Header
from gemini_obs_db.orm.nifs import Nifs
from .calibration import Calibration

class CalibrationNIFS(Calibration):
    """
    This class implements a calibration manager for NIFS.
    It is a subclass of Calibration
    """
    instrClass = Nifs
    instrDescriptors = (
        'read_mode',
        'disperser',
        'focal_plane_mask',
        'filter_name',
        )

    def set_applicable(self):
        # Return a list of the calibrations applicable to this NIFS dataset
        self.applicable = []

        # Science Imaging OBJECTs require a DARK
        if (self.descriptors['observation_type'] == 'OBJECT' and
                self.descriptors['spectroscopy'] == False and
                self.descriptors['observation_class'] == 'science'):
            self.applicable.append('dark')

        # Science spectroscopy that is not a progcal or partnercal requires a flat, arc, ronchi_mask and telluric_standard
        if (self.descriptors['observation_type'] == 'OBJECT' and
                self.descriptors['observation_class'] not in ['partnerCal', 'progCal', 'acqCal', 'acq'] and
                self.descriptors['spectroscopy'] == True):
            self.applicable.append('flat')
            self.applicable.append('processed_flat')
            self.applicable.append('arc')
            self.applicable.append('ronchi_mask')
            self.applicable.append('telluric_standard')

        # Flats require lampoff_flats
        if self.descriptors['observation_type'] == 'FLAT' and self.descriptors['gcal_lamp'] != 'Off':
            self.applicable.append('lampoff_flat')

        self.applicable.append('processed_bpm')

    @staticmethod
    def common_descriptors():
        return (Nifs.disperser, Nifs.focal_plane_mask, Nifs.filter_name)

    def bpm(self, processed=False, howmany=None, return_query=False):
        """
        This method identifies the best BPM to use for the target
        dataset.

        This will match on bpms for the same instrument

        Parameters
        ----------

        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Default 1 bpm
        howmany = howmany if howmany else 1

        filters = [Header.ut_datetime <= self.descriptors['ut_datetime'],]
        query = self.get_query(include_engineering=True) \
                    .bpm(processed) \
                    .add_filters(*filters) \
                    .match_descriptors(Header.instrument,)

        if return_query:
            return query.all(howmany), query
        else:
            return query.all(howmany)

    def dark(self, processed=False, howmany=None, return_query=False):
        """
        Find the optimal NIFS Dark for this target frame

        This will find NIFS darks with a matching exposure time, read mode, coadds,
        and disperser taken within 90 days.

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

        query =self.get_query() \
                .dark(processed) \
                .match_descriptors(Header.exposure_time,
                                   Nifs.read_mode,
                                   Header.coadds,
                                   Nifs.disperser) \
                .max_interval(days=90)
        if return_query:
            return query.all(howmany), query
        else:
            return query.all(howmany)

    def flat(self, processed=False, howmany=None, return_query=False):
        """
        Find the optimal NIFS Flat for this target frame

        This will find NIFS flats with a gcal_lamp of 'IRhigh' or 'QH' with a matching focal plane mask, filter name,
        central wavelength, and disperser taken within 10 days.

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

        # GCAL lamp must be IRhigh or QH
        # NIFS flats are always taken in short / high readmode. Don't match against readmode (inst sci Email 2013-03-13)
        query = self.get_query() \
                .flat(processed) \
                .add_filters(or_(Header.gcal_lamp == 'IRhigh', Header.gcal_lamp.like('QH%'))) \
                .match_descriptors(*CalibrationNIFS.common_descriptors()) \
                .tolerance(central_wavelength=0.001) \
            .max_interval(days=10)
        if return_query:
            return query.all(howmany), query
        else:
            return query.all(howmany)

    def lampoff_flat(self, howmany=None, return_query=False):
        """
        Find the optimal NIFS Lamp-off Flat for this target frame

        This will find NIFS lamp-off flats with a gcal_lamp of 'Off' with a matching focal plane mask, filter name,
        central wavelength, and disperser taken within 1 hour.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw lamp-off flats.
        howmany : int, default 10
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Default number of processed flats to associate
        howmany = howmany if howmany else 10

        # GCAL lamp must be IRhigh or QH
        # NIFS flats are always taken in short / high readmode. Don't match against readmode (inst sci Email 2013-03-13)
        query = self.get_query() \
                .flat() \
                .add_filters(Header.gcal_lamp == 'Off') \
                .match_descriptors(*CalibrationNIFS.common_descriptors()) \
            .tolerance(central_wavelength=0.001) \
            .max_interval(seconds=3600)
        if return_query:
            return query.all(howmany), query
        else:
            return query.all(howmany)

    def arc(self, howmany=None, return_query=False):
        """
        Find the optimal NIFS Arc for this target frame

        This will find NIFS arcs with a matching focal plane mask, filter name,
        central wavelength, and disperser taken within 1 year.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw arcs.
        howmany : int, default 10
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Always associate 1 arc by default
        howmany = howmany if howmany else 1

        query = self.get_query() \
                .arc() \
                .match_descriptors(*CalibrationNIFS.common_descriptors()) \
            .tolerance(central_wavelength=0.001) \
            .max_interval(days=365)
        if return_query:
            return query.all(howmany), query
        else:
            return query.all(howmany)

    def ronchi_mask(self, processed=False, howmany=None, return_query=False):
        """
        Find the optimal NIFS Ronchi Mask for this target frame

        This will find NIFS ronchi masks by searching on observation_type of 'RONCHI' with a matching
        central wavelength, and disperser.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw ronchi masks.
        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Always associate 1 ronchi by default
        howmany = howmany if howmany else 1

        query = self.get_query() \
                .observation_type('RONCHI') \
                .match_descriptors(Header.central_wavelength,
                                   Nifs.disperser)
        if return_query:
            return query.all(howmany), query
        else:
            return query.all(howmany)

    def telluric_standard(self, processed=False, howmany=None, return_query=False):
        """
        Find the optimal NIFS Telluric Standards for this target frame

        This will find NIFS telluric standards by searching parnterCal data on focal plane mask, filter name,
        central wavelength, and disperser taken within 1 day.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw telluric standards.
        howmany : int, default 1 if processed, else 12
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 12

        # Telluric standards are OBJECT spectroscopy partnerCal frames
        query = self.get_query() \
                .telluric_standard(OBJECT=True, partnerCal=True) \
                .match_descriptors(*CalibrationNIFS.common_descriptors()) \
            .tolerance(central_wavelength=0.001) \
            .max_interval(days=1)
        if return_query:
            return query.all(howmany), query
        else:
            return query.all(howmany)
