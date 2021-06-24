"""
This module holds the CalibrationGPI class
"""
from gemini_obs_db.diskfile import DiskFile
from gemini_obs_db.header import Header
from gemini_obs_db.gpi import Gpi
from .calibration import Calibration

from sqlalchemy.orm import join

class CalibrationGPI(Calibration):
    """
    This class implements a calibration manager for GPI.
    It is a subclass of Calibration
    """
    instrClass = Gpi
    instrDescriptors = (
        'disperser',
        'focal_plane_mask',
        'filter_name',
        )

    def set_applicable(self):
        """
        This method determines the list of applicable calibration types
        for this GPI frame and writes the list into the class
        applicable variable.
        It is called from the subclass init method.
        """
        self.applicable = []

        # Science OBJECTs require: dark, telluric_standard, astrometric_standard
        if ((self.descriptors['observation_type'] == 'OBJECT') and
                (self.descriptors['spectroscopy'] == True) and
                (self.descriptors['observation_class'] not in ['acq', 'acqCal'])):
            self.applicable.append('dark')
            self.applicable.append('astrometric_standard')
            # If spectroscopy require arc and telluric_standard
            # Otherwise polarimetry requres polarization_flat and polarization_standard
            if self.descriptors['spectroscopy'] == True:
                self.applicable.append('arc')
                self.applicable.append('telluric_standard')
            else:
                self.applicable.append('polarization_standard')
                self.applicable.append('polarization_flat')

    @staticmethod
    def common_descriptors():
        # Must Totally Match: disperser, filter_name
        # Apparently FPM doesn't have to match...
        return (Gpi.disperser, Gpi.filter_name)

    def dark(self, processed=False, howmany=None):
        """
        Find the optimal GPI DARK for this target frame

        This will match on darks with an exposure time within 10 seconds and taken within 1 year.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw darks.
        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        #  default to 1 dark for now
        howmany = howmany if howmany else 1

        return (
            self.get_query()
                .dark(processed)
                # exposure time must be within 10 seconds difference (Paul just made that up)
                .tolerance(exposure_time=10.0)
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
                .all(howmany)
            )

    def arc(self, processed=False, howmany=None):
        """
        Find the optimal GPI ARC for this target frame

        This will match on disperser and filter name, taken within 1 year.

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
        # Always default to 1 arc
        howmany = howmany if howmany else 1

        return (
            self.get_query()
                .arc(processed)
                .match_descriptors(*CalibrationGPI.common_descriptors())
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
                .all(howmany)
            )

    def telluric_standard(self, processed=False, howmany=None):
        """
        Find the optimal GPI telluric standard for this target frame

        This will match on disperser and filter name, taken within 1 year.  For processed, it matches against
        calibration programs only.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw telluric standards.
        howmany : int, default 1 if processed, else 8
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 8
        filters = []
        if not processed:
            filters = [Header.calibration_program == True]

        return (
            self.get_query()
                .telluric_standard(OBJECT=True, science=True)
                .add_filters(*filters)
                .match_descriptors(*CalibrationGPI.common_descriptors())
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
                .all(howmany)
            )

    def polarization_standard(self, processed=False, howmany=None):
        """
        Find the optimal GPI polarization standard for this target frame

        It matches on non-spectroscopy science with a calibration program and where the GPI wollaston is set.
        For processed data, it matches a reduction state of 'PROCESSED_POLSTANDARD' instead.  Then, in either case,
        it matches data on disperser and filter name taken within 1 year.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw polarization standards.
        howmany : int, default 1 if processed, else 8
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 8

        # NOTE: polarization standards are only found in GPI. We won't bother moving this to CalQuery - yet
        if processed:
            # TODO I can't find this method?!
            query = self.get_query().PROCESSED_POLSTANDARD()
        else:
            query = (self.get_query().raw().science().spectroscopy(False)
                                     .add_filters(Header.calibration_program==True,
                                                  Gpi.wollaston == True))

        return (
            query.match_descriptors(*CalibrationGPI.common_descriptors())
                 .max_interval(days=365)
                 .all(howmany)
            )

    def astrometric_standard(self, processed=False, howmany=None):
        """
        Find the optimal GPI astrometric standard field for this target frame

        This will match any data with the GPI astrometric standard flag set.  For processed
        data, it instead looks for the reduction state of 'PROCESSED_ASTROMETRIC'.  Then,
        in either case, it looks for data taken within 1 year.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw astrometric standards.
        howmany : int, default 1 if processed, else 8
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 8

        # NOTE: astrometric standards are only found in GPI. We won't bother moving this to CalQuery - yet

        if processed:
            # TODO where does this live?
            query = self.get_query().PROCESSED_ASTROMETRIC()
        else:
            query = (self.get_query().raw().OBJECT()
                         .add_filters(Gpi.astrometric_standard==True))

        return (
            # Looks like we don't care about matching the usual descriptors...
            # Absolute time separation must be within 1 year
            query.max_interval(days=365)
                 .all(howmany)
            )

    def polarization_flat(self, processed=False, howmany=None):
        """
        Find the optimal GPI polarization flat for this target frame

        This will match partnerCal datawith the GPI wollaston flag set.  For
        processed data, it looks for a reduction state of 'PROCESSED_POLFLAT' instead.
        Then, in either case, it matches on disperser and filter name  and taken within 1 year.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw polarization flats.
        howmany : int, default 1 if processed, else 8
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 8

        # NOTE: polarization flats are only found in GPI. We won't bother moving this to CalQuery - yet

        query = self.session.query(Header).select_from(join(join(Gpi, Header), DiskFile))

        if processed:
            # TODO where does this live? document behavior above..
            query = self.get_query().PROCESSED_POLFLAT()
        else:
            query = (self.get_query().flat().partnerCal()
                         .add_filters(Gpi.wollaston == True))

        return (
            query.match_descriptors(*CalibrationGPI.common_descriptors())
                 # Absolute time separation must be within 1 year
                 .max_interval(days=365)
                 .all(howmany)
            )
