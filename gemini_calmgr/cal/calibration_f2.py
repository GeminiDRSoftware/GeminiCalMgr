"""
This module holds the CalibrationF2 class
"""

from gemini_obs_db.header import Header
from gemini_obs_db.f2 import F2
from .calibration import Calibration, not_processed


class CalibrationF2(Calibration):
    """
    This class implements a calibration manager for F2.
    It is a subclass of Calibration
    """
    instrClass = F2
    instrDescriptors = (
        'read_mode',
        'disperser',
        'focal_plane_mask',
        'filter_name',
        'lyot_stop'
        )

    def set_applicable(self):
        """
        Get the list of applicable calibration types, such as "flat", "processed_dark", etc.

        Returns
        -------

        list of str
        """
        # Return a list of the calibrations applicable to this dataset
        self.applicable = []

        # Imaging OBJECTs require a DARK and a flat except acq images
        if (self.descriptors['observation_type'] == 'OBJECT' and
                self.descriptors['spectroscopy'] == False and
                self.descriptors['observation_class'] not in ['acq', 'acqCal']):
            self.applicable.append('dark')
            self.applicable.append('processed_dark')
            self.applicable.append('flat')
            self.applicable.append('processed_flat')
            # And if they're science frames, they require a photometric_standard
            if self.descriptors['observation_class'] == 'science':
                self.applicable.append('photometric_standard')

        # Spectroscopy OBJECTs require a dark, flat and arc
        if (self.descriptors['observation_type'] == 'OBJECT') and (self.descriptors['spectroscopy'] == True):
            self.applicable.append('dark')
            self.applicable.append('processed_dark')
            self.applicable.append('flat')
            self.applicable.append('processed_flat')
            self.applicable.append('arc')
            # And if they're science frames, they require a telluric_standard
            if self.descriptors['observation_class'] == 'science':
                self.applicable.append('telluric_standard')

        # FLAT frames require DARKs
        if self.descriptors['observation_type'] == 'FLAT':
            self.applicable.append('dark')
            self.applicable.append('processed_dark')

        # ARCs require DARKs and FLATs
        if self.descriptors['observation_type'] == 'ARC':
            self.applicable.append('dark')
            self.applicable.append('processed_dark')
            self.applicable.append('flat')
            self.applicable.append('processed_flat')

    def dark(self, processed=False, howmany=None):
        """
        Get a query for darks appropriate for F2.

        Returns
        -------
        :class:`sqlalchemy.orm.query.Query` matching the `exposure_time` and F2 `read_mode` within 90 days
        """
        if howmany is None:
            howmany = 1 if processed else 10

        return (
            self.get_query()
                .dark(processed=processed)
                .match_descriptors(Header.exposure_time,
                                   F2.read_mode)
                # Must totally match: read_mode, exposure_time
                .max_interval(days=90)
                .all(howmany)
            )

        return query.all()

    @staticmethod
    def common_descriptors():
        """
        Get the main descriptors for the F2 instrument

        Returns
        -------
        list of :class:`sqlalchemy.schema.Column`
        """
        return (F2.disperser, F2.lyot_stop, F2.filter_name, F2.focal_plane_mask)

    def flat(self, processed=False, howmany=None):
        """
        Get matching flats for an F2 observation.

        Parameters
        ----------

        processed : bool
            Query for processed flats, or not

        howmany : int, defaults to `None`
            How many results to return, or all if `None`

        Returns
        -------
        :class:`sqlalchemy.orm.query.Query` for flats matching the F2 read_mode and the :meth:`fits_storage.cal.calibration_f2.CalibrationF2.common_descriptors` within 90 days.  Also within 0.001 wavelength for spectroscopy
        """
        if howmany is None:
            howmany = 1 if processed else 10

        return (
            self.get_query()
                .flat(processed=processed)
                # Must totally match: disperser, central_wavelength (spect only), focal_plane_mask, filter_name, lyot_stop, read_mode
                .match_descriptors(F2.read_mode,
                                   *CalibrationF2.common_descriptors())
                .tolerance(central_wavelength=0.001, condition=self.descriptors['spectroscopy'])
                # Absolute time separation must be within 3 months
                .max_interval(days=90)
                .all(howmany)
            )

    def arc(self, processed=False, howmany=None):
        """
        Get matching arcs for an F2 observation.

        Parameters
        ----------

        processed : bool
            Query for processed flats, or not

        howmany : int, defaults to `None`
            How many results to return, or all if `None`

        Returns
        -------
        :class:`sqlalchemy.orm.query.Query` for arcs matching the F2 :meth:`fits_storage.cal.calibration_f2.CalibrationF2.common_descriptors` within 0.001 wavelength and 90 days
        """
        # Default number to associate is 1
        howmany = howmany if howmany else 1

        return (
            self.get_query()
                .arc(processed=processed)
                # Must Totally Match: disperser, central_wavelength, focal_plane_mask, filter_name, lyot_stop
                .match_descriptors(*CalibrationF2.common_descriptors())
                .tolerance(central_wavelength=0.001)
                # Absolute time separation must be within 3 months
                .max_interval(days=90)
                .all(howmany)
            )

    @not_processed
    def photometric_standard(self, processed=False, howmany=None):
        """
        Get matching photometric standards for an F2 observation.

        This matches photometric standards that are `OBJECT` s and `partnerCal`, matching the F2 `filter_name` and
        `lyot_stop`and within a day

        Parameters
        ----------

        processed : bool
            Query for processed standards, or not

        howmany : int, defaults to `None`
            How many results to return, or all if `None`

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Default number to associate
        howmany = howmany if howmany else 10

        return (
            self.get_query()
                # Photometric standards are OBJECT imaging partnerCal frames
                .photometric_standard(OBJECT=True, partnerCal=True)
                .match_descriptors(F2.filter_name,
                                   F2.lyot_stop)
                # Absolute time separation must be within 24 hours of the science
                .max_interval(days=1)
                .all(howmany)
            )

    @not_processed
    def telluric_standard(self, processed=False, howmany=None):
        """
        Get matching telluric standards for an F2 observation.

        This searches for telluric standards that are `OBJECT` s and `partnerCal`, matching the
        F2 :meth:`fits_storage.cal.calibration_f2.CalibrationF2.common_descriptors` within 0.001
        `central_wavelength` and within a day

        Parameters
        ----------

        processed : bool
            Query for telluric standards, or not
        howmany : int, defaults to 10
            How many results to return, or 10 if `None`

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
                .match_descriptors(*CalibrationF2.common_descriptors())
                .tolerance(central_wavelength=0.001)
                # Absolute time separation must be within 24 hours of the science
                .max_interval(days=1)
                .all(howmany)
            )
