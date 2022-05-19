"""
This module holds the CalibrationGSAOI class
"""
import datetime

from gemini_obs_db.orm.diskfile import DiskFile
from gemini_obs_db.orm.header import Header
from gemini_obs_db.orm.gsaoi import Gsaoi
from .calibration import Calibration, not_processed

class CalibrationGSAOI(Calibration):
    """
    This class implements a calibration manager for GSAOI.
    It is a subclass of Calibration
    """
    instrClass = Gsaoi
    instrDescriptors = (
        'filter_name',
        'read_mode'
        )

    def set_applicable(self):
        # Return a list of the calibrations applicable to this GSAOI dataset
        self.applicable = []

        # Science OBJECTs require DomeFlats and photometric_standards
        if self.descriptors['observation_type'] == 'OBJECT' and self.descriptors['observation_class'] == 'science':
            self.applicable.append('domeflat')
            self.applicable.append('lampoff_domeflat')
            self.applicable.append('processed_flat')
            self.applicable.append('photometric_standard')

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
                    .match_descriptors(Header.instrument,)

        if return_query:
            return query.all(howmany), query
        else:
            return query.all(howmany)

    def domeflat(self, processed=False, howmany=None):
        """
        Find the optimal GSAOI dome flat for this target frame

        This will match dayCal data with object name of 'Domeflat'.  For processed
        data it matches "PROCESSED_FLAT" reduction state instead.  Then, for either
        case, it looks for a matching GSAOI filter name taken within 30 days.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw dome flats.
        howmany : int, default 1 if processed, else 20
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 20

        if processed:
            query = self.get_query().PROCESSED_FLAT()
        else:
            query = (self.get_query().raw().OBJECT()
                         .observation_class('dayCal')
                         # Notice that object() is observation_type=='OBJECT', in case the next confuses you...
                         .add_filters(Header.object == 'Domeflat'))

        return (
                # Common filter, with absolute time separation within a month
            query.match_descriptors(Gsaoi.filter_name)
                 .max_interval(days=30)
                 .all(howmany)
            )

    def lampoff_domeflat(self, processed=False, howmany=None):
        """
        Find the optimal GSAOI lamp off flat for this target frame

        This will match dayCal data with object name of 'Domeflat OFF'.  For
        processed data, it looks for a reduction state of 'PROCESSED_FLAT' instead.
        Then, in either case, it looks for a matching GSAOI filter name
        taken within 30 days.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw dome flats.
        howmany : int, default 1 if processed, else 20
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 20

        if processed:
            query = self.get_query().PROCESSED_FLAT()
        else:
            query = (self.get_query().raw().OBJECT()
                         .observation_class('dayCal')
                         # Notice that object() is observation_type=='OBJECT', in case the next confuses you...
                         .add_filters(Header.object == 'Domeflat OFF'))

        return (
                # Common filter, with absolute time separation within a month
            query.match_descriptors(Gsaoi.filter_name)
                 .max_interval(days=30)
                 .all(howmany)
            )

    # For gsaoi, domeflats are the only flats
    flat = domeflat
    lampoff_flat = lampoff_domeflat

    # Processed photometric standards haven't been implemented
    @not_processed
    def photometric_standard(self, processed=False, howmany=None):
        """
        Find the optimal GSAOI photometric standard for this target frame

        This will match partnerCal data with a matching GSAOI filter name
        taken within 30 days.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw dome flats.
        howmany : int, default 1 if processed, else 8
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Default number to associate
        howmany = howmany if howmany else 8

        return (
            self.get_query()
                .raw().OBJECT().partnerCal()
                # Common filter, with absolute time separation within a month
                .match_descriptors(Gsaoi.filter_name)
                .max_interval(days=30)
                .all(howmany)
            )
