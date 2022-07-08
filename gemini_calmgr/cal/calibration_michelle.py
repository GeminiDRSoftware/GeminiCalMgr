"""
This module holds the CalibrationMICHELLE class
"""
import datetime

from gemini_obs_db.orm.diskfile import DiskFile
from gemini_obs_db.orm.header import Header
from gemini_obs_db.orm.michelle import Michelle
from .calibration import Calibration

class CalibrationMICHELLE(Calibration):
    """
    This class implements a calibration manager for MICHELLE.
    It is a subclass of Calibration
    """
    instrClass = Michelle
    instrDescriptors = (
        'read_mode',
        'disperser',
        'filter_name',
        'focal_plane_mask'
        )

    def set_applicable(self):
        # Return a list of the calibrations applicable to this MICHELLE dataset
        self.applicable = []

        if self.descriptors['observation_type'] == 'BPM':
            return

        # Science Imaging OBJECTs require a DARK
        if (self.descriptors['observation_type'] == 'OBJECT' and
                self.descriptors['spectroscopy'] == False and
                self.descriptors['observation_class'] == 'science'):
            self.applicable.append('dark')

        # Science spectroscopy OBJECTs require a FLAT
        if (self.descriptors['observation_type'] == 'OBJECT' and
                self.descriptors['spectroscopy'] == True and
                self.descriptors['observation_class'] == 'science'):
            self.applicable.append('flat')

        self.applicable.append('processed_bpm')

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
                    .match_descriptors(Header.instrument, Header.detector_binning)

        if return_query:
            return query.all(howmany), query
        else:
            return query.all(howmany)

    def dark(self, processed=False, howmany=None, return_query=False):
        """
        Find the optimal Michelle Dark for this target frame

        This will find Michelle darks with a matching read mode, exposure time, and coadds
        within 1 day.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw darks.
        howmany : int, default 10
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Default number to associate
        howmany = howmany if howmany else 10

        query = (
            self.get_query()
                .dark()
                .match_descriptors(Header.exposure_time,
                                   Michelle.read_mode,
                                   Header.coadds)
                # Absolute time separation must be within 1 day
                .max_interval(days=1)
            )
        if return_query:
            return query.all(howmany), query
        else:
            return query.all(howmany)

    def flat(self, processed=False, howmany=None, return_query=False):
        """
        Find the optimal Michelle Flat for this target frame

        This will find Michelle flats with a matching read mode, filter name, disperser, and focal plane mask.
        It also matches the central wavelength within 0.001 microns.  Flats must be taken within 1 day.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw flats.
        howmany : int, default 10
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Default number to associate
        howmany = howmany if howmany else 10

        query = self.get_query().flat().match_descriptors(Michelle.read_mode, Michelle.filter_name)

        if self.descriptors['spectroscopy'] == True:
            query = (query.match_descriptors(Michelle.disperser, Michelle.focal_plane_mask)
                          .tolerance(central_wavelength=0.001))

        # Absolute time separation must be within 1 day
        query = query.max_interval(days=1)
        if return_query:
            return query.all(howmany), query
        else:
            return query.all(howmany)