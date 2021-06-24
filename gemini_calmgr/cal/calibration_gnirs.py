"""
This module holds the CalibrationGNIRS class
"""
from gemini_obs_db.diskfile import DiskFile
from gemini_obs_db.header import Header
from gemini_obs_db.gnirs import Gnirs
from .calibration import Calibration, not_processed

from sqlalchemy import or_, desc

class CalibrationGNIRS(Calibration):
    """
    This class implements a calibration manager for GNIRS.
    It is a subclass of Calibration
    """
    instrClass = Gnirs
    instrDescriptors = (
        'read_mode',
        'well_depth_setting',
        'disperser',
        'focal_plane_mask',
        'camera',
        'filter_name'
        )

    def set_applicable(self):
        """
        This method determines the list of applicable calibration types
        for this GNIRS frame and writes the list into the class
        applicable variable.
        It is called from the subclass init method.
        """
        self.applicable = []

        # Science Imaging OBJECTs that are not acq or acqCal require a DARK and a FLAT
        if ((self.descriptors['observation_type'] == 'OBJECT') and
                (self.descriptors['observation_class'] not in ['acq', 'acqCal']) and
                (self.descriptors['spectroscopy'] == False)):
            self.applicable.append('dark')
            self.applicable.append('flat')
            self.applicable.append('lampoff_flat')
            self.applicable.append('processed_flat')

        # Spectroscopy OBJECT frames require a flat and arc (if < L band) and telluric_standard
        if (self.descriptors['observation_type'] == 'OBJECT') and (self.descriptors['spectroscopy'] == True):
            self.applicable.append('telluric_standard')
            if self.descriptors['central_wavelength'] < 2.8:
                self.applicable.append('arc')
            # GNIRS spectroscopy flats are a little complex
            if self.descriptors['disperser'] and 'XD' in self.descriptors['disperser']:
                # If they are XD, they need an IR flat, a Quartz-Halogen flat (qh_flat) and pinhole.
                self.applicable.append('flat')
                self.applicable.append('qh_flat')
                self.applicable.append('pinhole_mask')
            else:
                # non-XD, Long Camera ranges
                if 'Short' in self.descriptors['camera']:
                    if self.descriptors['central_wavelength'] < 1.8:
                        self.applicable.append('qh_flat')
                    elif self.descriptors['central_wavelength'] < 2.7:
                        self.applicable.append('flat')
                    else:
                        self.applicable.append('lampoff_flat')

                elif 'Long' in self.descriptors['camera'] and '32/mm' in self.descriptors['disperser']:
                    # non-XD, long Camera, 32/mm grating
                    if self.descriptors['central_wavelength'] < 4.25:
                        self.applicable.append('flat')
                    else:
                        self.applicable.append('lampoff_flat')

                elif 'Long' in self.descriptors['camera']:
                    # non-XD, long camera, 10/mm and 111/mm grating
                    if self.descriptors['central_wavelength'] < 1.8:
                        self.applicable.append('qh_flat')
                    elif self.descriptors['central_wavelength'] < 4.3:
                        self.applicable.append('flat')
                    else:
                        self.applicable.append('lampoff_flat')


        # IR lamp-on flats can use lamp-off flats
        if self.descriptors['observation_type'] == 'FLAT' and self.descriptors['gcal_lamp'] == 'IRhigh':
            self.applicable.append('lampoff_flat')

    def dark(self, processed=False, howmany=None):
        """
        Find the optimal GNIRS Dark for this target frame

        This will find GNIRS darks with a matching read mode, well depth setting, exposure time, and coadds
        within 180 days.

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
        # Default number of processed darks to associate
        if howmany is None:
            howmany = 1 if processed else 10

        return (
            self.get_query()
                .dark(processed=processed)
                # Must totally match: read_mode, well_depth_setting, exposure_time, coadds
                .match_descriptors(Header.exposure_time,
                                   Gnirs.read_mode,
                                   Gnirs.well_depth_setting,
                                   Header.coadds)
                # Absolute time separation must be within 3 months
                .max_interval(days=90)
                .all(howmany)
            )

    def get_gnirs_flat_query(self, processed):
        """
        Utility method for getting a query for GNIRS flats

        This will find GNIRS flats with a matching disperser, focal plane mask, camera, filter name,
        and well depth setting.  It also matches the central wavelength within 0.001 microns.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw darks.

        Returns
        -------
        :class:`fits_storage.cal.calibration.CalQuery` setup for flats as described that can be
            further refined
        """
        return (
            self.get_query()
            .flat(processed=processed)
            # Must totally match: disperser, central_wavelength, focal_plane_mask, camera, filter_name, well_depth_setting
            # update from RM 20130321 - read mode should not be required to match, but well depth should.
            # For imaging, central wavelength and disperser are not required to match
            .match_descriptors(Gnirs.disperser,
                               Gnirs.focal_plane_mask,
                               Gnirs.camera,
                               Gnirs.filter_name,
                               Gnirs.well_depth_setting)
            .if_(self.descriptors['spectroscopy'], 'match_descriptors', Gnirs.disperser)
            .if_(self.descriptors['spectroscopy'], 'tolerance', central_wavelength=0.001)
        )

    def flat(self, processed=False, howmany=None):
        """
        Utility method for getting a query for GNIRS flats

        This will find GNIRS flats with a matching disperser, focal plane mask, camera, filter name,
        and well depth setting.  It also matches the central wavelength within 0.001 microns.

        Finally, it matches gcal_lamp of IRhigh and within 90 days.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw flats.
        howmany : int, default 1 if processed else 10
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # GNIRS mostly uses GCAL flats with the IRhigh lamp.
        # Sometimes, eg thermal wavelengths, it just uses GCAL with the lamp off (shutter closed),
        # and the thermal background of gcal is adequate.
        # But also it sometimes (eg some imaging) uses a lamp-on flat and wants to substract a lamp-off flat
        # And then also in XD modes, it also wants a QH lamp flat for the shorter wavelengths.
        #
        # So, this cal association will give you either IRhigh flats. In some cases, we make
        # a lamp-off flat applicable to the lamp-on flat to give the subtraciton pairs. 
        # We also have lamp-off flats directly applicable to the science at thermal wavelengths.
        # and we consider QH flats a separate thing.
        #
        # We prioritize flats that have the same observation ID as the science, but don't *require* this

        if howmany is None:
            howmany = 1 if processed else 10

        return (
            self.get_gnirs_flat_query(processed)
                # Lamp selection (see comments above)
                .add_filters(Header.gcal_lamp == 'IRhigh')
                # Absolute time separation must be within 3 months
                .max_interval(days=90)
                .all(howmany, extra_order_terms=[desc(Header.observation_id == self.descriptors['observation_id'])])
            )

    def arc(self, processed=False, howmany=None):
        """
        Find the optimal GNIRS ARC for this target frame

        This will find GNIRS arc with a matching central wavelength, disperser, focal plane mask, camera,
        and filter name.  It matches within a year.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw arcs.
        howmany : int, default 1 if processed else 10
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Always default to 1 arc
        howmany = howmany if howmany else 1

        return (
            self.get_query()
                .arc(processed=processed)
                # Must Totally Match: disperser, central_wavelength, focal_plane_mask, filter_name, camera
                .match_descriptors(Header.central_wavelength,
                                   Gnirs.disperser,
                                   Gnirs.focal_plane_mask,
                                   Gnirs.filter_name,
                                   Gnirs.camera)
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
                .all(howmany)
            )

    def pinhole_mask(self, processed=False, howmany=None):
        """
        Find the optimal GNIRS Pinhole Mask for this target frame

        This will find GNIRS pinhole mask with a matching central wavelength, disperser, and camera.
        It matches within a year.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw pinhole mask.
        howmany : int, default 1 if processed else 10
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 5

        return (
            self.get_query()
                .pinhole(processed)
                # Must totally match: disperser, central_wavelength, camera, (only for cross dispersed mode?)
                .match_descriptors(Header.central_wavelength,
                                   Gnirs.disperser,
                                   Gnirs.camera)
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
                .all(howmany)
            )

    @not_processed
    def lampoff_flat(self, processed=False, howmany=None):
        """
        Find the optimal lamp-off flats to go with the lamp-on flat

        This will find GNIRS lamp off flats with a matching disperser, focal plane mask, camera, filter name,
        and well depth setting.  It also matches the central wavelength within 0.001 microns.  Finally, it
        matches against gcal_lamp of 'Off'.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw lamp-off flats
        howmany : int, default 1 if processed else 10
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Default number of raw lampoff flats
        howmany = howmany if howmany else 10

        return (
            self.get_gnirs_flat_query(processed=False) # lampoff flats are just Raw flats...
                .add_filters(Header.gcal_lamp == 'Off')
                # Absolute time separation must be within 1 day
                .max_interval(days=1)
                .all(howmany, extra_order_terms=[desc(Header.observation_id == self.descriptors['observation_id'])])
            )

    def qh_flat(self, processed=False, howmany=None):
        """
        Find the optimal GNIRS QH flat field for this target frame

        This will find GNIRS flats with a gcal_lamp of 'QH'.  It looks for a matching disperser, focal plane mask,
        camera, filter name, and well depth setting.  It also matches the central wavelength within 0.001 microns.
        It matches within 90 days.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw QH flats
        howmany : int, default 1 if processed else 10
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # GNIRS mostly uses GCAL flats with the IRhigh lamp.
        # Sometimes, eg thermal wavelengths, it just uses GCAL with the lamp off (shutter closed),
        # and the thermal background of gcal is adequate.
        # But also it sometimes (eg some imaging) uses a lamp-on flat and wants to substract a lamp-off flat
        # And then also in XD modes, it also wants a QH lamp flat for the shorter wavelengths.
        #
        # So, this cal association will give you either IRhigh flats or lamp-Off flats. In some cases, we make
        # a lamp-off flat applicable to the lamp-on flat to give the subtraciton pairs. 
        # and we consider QH flats a separate thing.
        if howmany is None:
            howmany = 1 if processed else 10

        return (
            self.get_gnirs_flat_query(processed) # QH flats are just flats...
                # ... with QH GCAL lamp
                .add_filters(Header.gcal_lamp == 'QH')
                # Absolute time separation must be within 3 months
                .max_interval(days=90)
                .all(howmany, extra_order_terms=[desc(Header.observation_id == self.descriptors['observation_id'])])
            )

    def telluric_standard(self, processed=False, howmany=None):
        """
        Find the optimal GNIRS telluric observations for this target frame

        This will find GNIRS telluric standards with matching wavelength, disperser, focal plane mask,
        camera, and filter name.  It looks only for a qa_state of 'Pass' or 'Undefined'.  It matches
        within 1 day.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw telluric standards
        howmany : int, default 1 if processed else 10
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 8

        return (
            self.get_query()
                .telluric_standard(processed=processed, OBJECT=True, partnerCal=True)
                # Must totally match: disperser, central_wavelength, focal_plane_mask, camera, filter_name
                .match_descriptors(Header.central_wavelength,
                                   Gnirs.disperser,
                                   Gnirs.focal_plane_mask,
                                   Gnirs.camera,
                                   Gnirs.filter_name)
                # Usable is not OK for these - may be partly saturated for example
                .add_filters(or_(Header.qa_state == 'Pass', Header.qa_state == 'Undefined'))
                # Absolute time separation must be within 1 day
                .max_interval(days=1)
                .all(howmany)
            )
