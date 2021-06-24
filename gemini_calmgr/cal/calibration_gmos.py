"""
The CalibrationGMOS class

"""
import datetime
import math

from gemini_obs_db.diskfile import DiskFile
from gemini_obs_db.header import Header
from gemini_obs_db.gmos import Gmos

from .calibration import Calibration
from .calibration import not_imaging
from .calibration import not_processed
from .calibration import not_spectroscopy

from sqlalchemy.orm import join

from gempy.utils import logutils

log = logutils.get_logger(__name__)


class CalibrationGMOS(Calibration):
    """
    This class implements a calibration manager for GMOS, a subclass of
    Calibration.

    """
    instrClass = Gmos
    instrDescriptors = (
        'disperser',
        'filter_name',
        'focal_plane_mask',
        'detector_x_bin',
        'detector_y_bin',
        'amp_read_area',
        'read_speed_setting',
        'gain_setting',
        'nodandshuffle',
        'nod_count',
        'nod_pixels',
        'prepared',
        'overscan_trimmed',
        'overscan_subtracted'
        )

    def set_applicable(self):
        """
        This method determines which calibration types are applicable
        to the target data set, and records the list of applicable
        calibration types in the class applicable variable.
        All this really does is determine whether what calibrations the
        /calibrations feature will look for. Just because a caltype isn't
        applicable doesn't mean you can't ask the calmgr for one.

        """
        self.applicable = []

        if self.descriptors:

            # MASK files do not require anything,
            if self.descriptors['observation_type'] == 'MASK':
                return

            # PROCESSED_SCIENCE files do not require anything
            if 'PROCESSED_SCIENCE' in self.types:
                return
            
            # Do BIAS. Most things require Biases.
            require_bias = True

            if self.descriptors['observation_type'] in ('BIAS', 'ARC'):
                # BIASes and ARCs do not require a bias.
                require_bias = False

            elif self.descriptors['observation_class'] in ('acq', 'acqCal'):
                # acq images don't require a BIAS.
                require_bias = False

            elif self.descriptors['detector_roi_setting'] == 'Central Stamp':
                # Anything that's ROI = Central Stamp does not require a bias
                require_bias = False

            if require_bias:
                self.applicable.append('bias')
                self.applicable.append('processed_bias')

            if ((self.descriptors['spectroscopy'] == True) and
                (self.descriptors['observation_type'] == 'FLAT')):

                self.applicable.append('arc')
                self.applicable.append('processed_arc')

            # If it (is spectroscopy) and
            # (is an OBJECT) and
            # (is not a Twilight) and
            # (is not a specphot)
            # then it needs an arc, flat, spectwilight, specphot
            if ((self.descriptors['spectroscopy'] == True) and
                (self.descriptors['observation_type'] == 'OBJECT') and
                (self.descriptors['object'] != 'Twilight')):

                self.applicable.append('arc')
                self.applicable.append('processed_arc')
                self.applicable.append('flat')
                self.applicable.append('processed_flat')

                if self.descriptors['observation_class'] not in ['partnerCal', 'progCal']:
                    self.applicable.append('spectwilight')
                    self.applicable.append('specphot')

                    if self.descriptors['central_wavelength'] is not None:
                        self.applicable.append('processed_standard')
                        self.applicable.append('processed_slitillum')
                        self.applicable.append('slitillum')

            # If it (is imaging) and (is Imaging focal plane mask) and
            # (is an OBJECT) and (is not a Twilight) and is not acq or acqcal
            # ==> needs flats, processed_fringe, processed_standard

            if ((self.descriptors['spectroscopy'] == False) and
                (self.descriptors['focal_plane_mask'] == 'Imaging') and
                (self.descriptors['observation_type'] == 'OBJECT') and
                (self.descriptors['object'] != 'Twilight') and
                (self.descriptors['observation_class'] not in ['acq', 'acqCal'])):

                self.applicable.append('flat')
                self.applicable.append('processed_flat')
                self.applicable.append('processed_fringe')
                if self.descriptors['central_wavelength'] is not None:
                    self.applicable.append('processed_standard')
                # If it's all that and obsclass science, then it needs a photstd
                # need to take care that phot_stds don't require phot_stds for recursion
                if self.descriptors['observation_class'] == 'science':
                    self.applicable.append('photometric_standard')

            # If it (is nod and shuffle) and (is an OBJECT), then it needs a dark
            if ((self.descriptors['nodandshuffle'] == True) and
                (self.descriptors['observation_type'] == 'OBJECT')):
                # Hack - for now, using a datetime filter.  For recent data we don't need darks
                # replace this with a check against the Gmos.detector once we track it
                ut_datetime = None
                if 'ut_datetime' in self.descriptors.keys():
                    ut_datetime = self.descriptors['ut_datetime']
                    if not hasattr(ut_datetime, 'year'):
                        ut_datetime = None
                if ut_datetime is None or ut_datetime.year < 2020:
                    self.applicable.append('dark')
                    self.applicable.append('processed_dark')

            # If it is MOS then it needs a MASK
            if 'MOS' in self.types:
                self.applicable.append('mask')

# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------

    @not_imaging
    def arc(self, processed=False, howmany=None):
        """
        This method identifies the best GMOS ARC to use for the target
        dataset.

        This will match on arcs for the same instrument, disperser and filter_name
        with the same x and y binning and a wavelength within 0.001 microns tolerance taken
        within a year of the observation.

        This method will also match on `focal_plane_mask`.  If the `focal_plane_mask` is
        5.0arcsec, this is a special case and will will simply match on any value
        ending in "arcsec".

        Finally, we look for a match on `amp_read_area`.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw arcs
        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Default 1 arc
        howmany = howmany if howmany else 1
        filters = []
        # Must match focal_plane_mask only if it's not the 5.0arcsec slit in the
        # target, otherwise any longslit is OK
        if self.descriptors['focal_plane_mask'] != '5.0arcsec':
            filters.append(Gmos.focal_plane_mask == self.descriptors['focal_plane_mask'])
        else:
            filters.append(Gmos.focal_plane_mask.like('%arcsec'))

        # The science amp_read_area must be equal or substring of the cal amp_read_area
        # If the science frame uses all the amps, then they must be a direct match as
        # all amps must be there - this is more efficient for the DB as it will use
        # the index. Otherwise, the science frame could have a subset of the amps
        # thus we must do the substring match

        # IF looking for processed thing
        #     Full frame is always ok
        #     Central Spectrum can match Central Spectrum as well
        #     Custom detector_roi_
        if processed:
            if self.descriptors['detector_roi_setting'] == 'Full Frame':
                filters.append(Header.detector_roi_setting == 'Full Frame')
            elif self.descriptors['detector_roi_setting'] == 'Central Spectrum':
                filters.append(Header.detector_roi_setting.in_(['Full Frame', 'Central Spectrum']))
            else:
                filters.append(Header.detector_roi_setting == 'Full Frame')
        else:
            if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
                filters.append(Gmos.amp_read_area == self.descriptors['amp_read_area'])
            elif self.descriptors['amp_read_area'] is not None:
                    filters.append(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))

        return (
            self.get_query()
                .arc(processed)
                .add_filters(*filters)
                .match_descriptors(Header.instrument,
                                   Gmos.disperser,
                                   Gmos.filter_name,    # Must match filter (KR 20100423)
                                   Gmos.detector_x_bin, # Must match ccd binning
                                   Gmos.detector_y_bin)
                                   # Gmos.grating_order) # match on grating order
                .tolerance(central_wavelength=0.001)
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
                .all(howmany)
            )
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------

    def dark(self, processed=False, howmany=None):
        """
        Method to find best GMOS Dark frame for the target dataset.

        This will match on darks for the same instrument, read speed, gain, and nod/shuffle
        with the same x and y binning and an exposure time within 50s tolerance taken
        within a year of the observation.

        Finally, we look for a match on `amp_read_area`.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw darks
        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """

        if howmany is None:
            howmany = 1 if processed else 15

        filters = []
        # The science amp_read_area must be equal or substring of the cal amp_read_area
        # If the science frame uses all the amps, then they must be a direct match as
        # all amps must be there - this is more efficient for the DB as it will use
        # the index. Otherwise, the science frame could have a subset of the amps thus
        # we must do the substring match
        
        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            filters.append(Gmos.amp_read_area == self.descriptors['amp_read_area'])
        elif self.descriptors['amp_read_area'] is not None:
                filters.append(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))

        # Must match exposure time. For some strange reason, GMOS exposure times
        # sometimes come out a few 10s of ms different between the darks and science
        # frames

        # K.Roth 20110817 told PH just make it the nearest second, as you can't
        # demand non integer times anyway. Yeah, and GMOS-S ones come out a few 10s
        # of *seconds* different - going to choose darks within 50 secs for now...
        # That's why we're using a tolerance to match the exposure time

        return (
            self.get_query()
                .dark(processed)
                .add_filters(*filters)
                .match_descriptors(Header.instrument,
                                   Gmos.detector_x_bin,
                                   Gmos.detector_y_bin,
                                   Gmos.read_speed_setting,
                                   Gmos.gain_setting,
                                   Gmos.nodandshuffle)
                .tolerance(exposure_time = 50.0)
                .if_(self.descriptors['nodandshuffle'], 'match_descriptors', Gmos.nod_count, Gmos.nod_pixels)
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
                .all(howmany)
            )

    def bias(self, processed=False, howmany=None):
        """
        Method to find the best bias frames for the target dataset

        This will match on biases for the same instrument, read speed, and gain
        with the same x and y binning and
        within a 90 days of the observation.

        For `prepared` data, we also look for a match on `overscan_trimmed` and
        `overscan_subtracted`.

        Finally, we look for a match on `amp_read_area`.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw biases
        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """

        if howmany is None:
            howmany = 1 if processed else 50

        filters = []
        # The science amp_read_area must be equal or substring of the cal
        # amp_read_area If the science frame uses all the amps, then they must be a
        # direct match as all amps must be there - this is more efficient for the DB
        # as it will use the index. Otherwise, the science frame could have a subset
        # of the amps thus we must do the substring match
        
        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            filters.append(Gmos.amp_read_area == self.descriptors['amp_read_area'])
        elif self.descriptors['amp_read_area'] is not None:
            filters.append(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))

        # The Overscan section handling: this only applies to processed biases
        # as raw biases will never be overscan trimmed or subtracted, and if they're
        # processing their own biases, they ought to know what they want to do.
        if processed:
            if self.descriptors['prepared'] == True:
                # If the target frame is prepared, then we match the overscan state. 
                filters.append(Gmos.overscan_trimmed == self.descriptors['overscan_trimmed'])
                filters.append(Gmos.overscan_subtracted == self.descriptors['overscan_subtracted'])
            else:
                # If the target frame is not prepared, then we don't know what
                # their procesing intentions are. We could go with the default
                # (which is trimmed and subtracted).
                # But actually it's better to just send them what we have, as we has
                # a mishmash of both historically
                #
                #filters.append(Gmos.overscan_trimmed == True)
                #filters.append(Gmos.overscan_subtracted == True)
                pass

        return (
            self.get_query()
                .bias(processed)
                .add_filters(*filters)
                .match_descriptors(Header.instrument,
                                  Gmos.detector_x_bin,
                                  Gmos.detector_y_bin,
                                  Gmos.read_speed_setting,
                                  Gmos.gain_setting)
                # Absolute time separation must be within 3 months
                .max_interval(days=90)
                .all(howmany)
            )

    def imaging_flat(self, processed, howmany, flat_descr, filt):
        """
        Method to find the best imaging flats for the target dataset

        For unprocessed imaging flats, we look for a target type of `Twilight`.

        This will match on imaging flats for the provided `flat_descr` elements
        within 180 days of the observation.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw flats
        howmany : int, default 1
            How many matches to return
        flat_descr : list of descriptors
            The list of descriptors to match against
        filt : list of filters
            Additional list of filters to apply to the query

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 20

        if processed:
            query = self.get_query().PROCESSED_FLAT()
        else:
            # Imaging flats are twilight flats
            # Twilight flats are dayCal OBJECT frames with target Twilight
            query = self.get_query().raw().dayCal().OBJECT().object('Twilight')
        return (
            query.add_filters(*filt)
                 .match_descriptors(*flat_descr)
                 # Absolute time separation must be within 6 months
                 .max_interval(days=180)
                 .all(howmany)
            )

    def spectroscopy_flat(self, processed, howmany, flat_descr, filt):
        """
        Method to find the best spectroscopy flats for the target dataset

        This will match on spectroscopy flats for the provided `flat_descr` elements
        and `filt` filters with a `central_wavelength` tolerance of 0.001 microns
        within 180 days of the observation.  It will also do some fuzzy matching
        on the elevation.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw flats
        howmany : int, default 1
            How many matches to return
        flat_descr : list of descriptors
            The list of descriptors to match against
        filt : list of filters
            Additional list of filters to apply to the query

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 2

        ifu = mos_or_ls = False
        el_thres = 0.0
        under_85 = False
        crpa_thres = 0.0
        # QAP might not give us these for now. Remove this 'if' later when it does
        if self.descriptors.get('elevation') is not None:
            # Spectroscopy flats also have to somewhat match telescope position
            # for flexure, as follows this is from FitsStorage TRAC #43 discussion
            # with KR 20130425. This code defines the thresholds and the conditions
            # where they apply.

            try:
                ifu = self.descriptors['focal_plane_mask'].startswith('IFU')
                # For IFU, elevation must we within 7.5 degrees
                el_thres = 7.5
            except AttributeError:
                # focal_plane_mask came as None. Leave 'ifu' as False
                pass
            try:
                mos_or_ls = self.descriptors['central_wavelength'] > 0.55 or self.descriptors['disperser'].startswith('R150')
                # For MOS and LS, elevation must we within 15 degrees
                el_thres = 15.0
            except AttributeError:
                # Just in case disperser is None
                pass
            under_85 = self.descriptors['elevation'] < 85

            # crpa*cos(el) must be within el_thres degrees, ie crpa must be within
            # el_thres / cos(el) when el=90, cos(el) = 0 and the range is infinite.
            # Only check at lower elevations.
            
            if under_85:
                crpa_thres = el_thres/math.cos(math.radians(self.descriptors['elevation']))

        return (
            self.get_query()
                .flat(processed)
                .add_filters(*filt)
                .match_descriptors(*flat_descr)
            # Central wavelength is in microns (by definition in the DB table).
                .tolerance(central_wavelength=0.001)

            # Spectroscopy flats also have to somewhat match telescope position
            # for flexure, as follows this is from FitsStorage TRAC #43 discussion with
            # KR 20130425.  See the comments above to explain the thresholds.
            
                .tolerance(condition = ifu, elevation=el_thres)
                .tolerance(condition = mos_or_ls, elevation=el_thres)
                .tolerance(condition = under_85, cass_rotator_pa=crpa_thres)

            # Absolute time separation must be within 6 months
                .max_interval(days=180)
                .all(howmany)
            )

    def flat(self, processed=False, howmany=None):
        """
        Method to find the best GMOS FLAT fields for the target dataset

        This will match on flats for the same instrument, read speed, filter,
        focal plane mask, disperser, amp read area, and gain with the same
        x and y binning.

        It will then do the matching using either :meth:`spectroscopy_flat` or
        :meth:`imaging_flat` as appropriate.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw flats
        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        filters = []

        # Common descriptors for both types of flat
        # Must totally match instrument, detector_x_bin, detector_y_bin, filter
        flat_descriptors = (
            Header.instrument,
            Gmos.detector_x_bin,
            Gmos.detector_y_bin,
            Gmos.filter_name,
            Gmos.read_speed_setting,
            Gmos.gain_setting,
            Header.spectroscopy,
            # Focal plane mask must match for imaging too... To avoid daytime
            # thru-MOS mask imaging "flats"
            Gmos.focal_plane_mask,
            Gmos.disperser,     # this can be common-mode as imaging is always 'MIRROR'
            )

        # The science amp_read_area must be equal or substring of the cal
        # amp_read_area. If the science frame uses all the amps, then they must
        # be a direct match as all amps must be there - this is more efficient for
        # the DB as it will use the index. Otherwise, the science frame could
        # have a subset of the amps thus we must do the substring match.

        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            flat_descriptors = flat_descriptors + (Gmos.amp_read_area,)
        elif self.descriptors['amp_read_area'] is not None:
                filters.append(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))

        if self.descriptors['spectroscopy']:
            return self.spectroscopy_flat(processed, howmany, flat_descriptors, filters)
        else:
            return self.imaging_flat(processed, howmany, flat_descriptors, filters)

    def processed_fringe(self, howmany=None):
        """
        Method to find the best processed_fringe frame for the target dataset.
        Note that the concept of a raw fringe frame is meaningless.

        This will match on amp read area, filter name, and x and y binning.  It matches
        within 1 year.

        Parameters
        ----------

        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Default number to associate
        howmany = howmany if howmany else 1

        filters = []
        # The science amp_read_area must be equal or substring of the cal amp_read_area
        # If the science frame uses all the amps, then they must be a direct match as
        # all amps must be there - this is more efficient for the DB as it will use
        # the index. Otherwise, the science frame could have a subset of the amps thus
        # we must do the substring match.
        
        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            filters.append(Gmos.amp_read_area == self.descriptors['amp_read_area'])
        elif self.descriptors['amp_read_area'] is not None:
                filters.append(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))

        return (
            self.get_query()
                .PROCESSED_FRINGE()
                .add_filters(*filters)
                .match_descriptors(Header.instrument,
                                   Gmos.detector_x_bin,
                                   Gmos.detector_y_bin,
                                   Gmos.filter_name)
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
                .all(howmany)
            )

    def standard(self, processed=False, howmany=None):
        """
        Method to find the best standard frame for the target dataset.

        This will match on flats for the same instrument, read speed, filter,
        focal plane mask, disperser, amp read area, and gain with the same
        x and y binning.

        It will then do the matching using either :meth:`spectroscopy_flat` or
        :meth:`imaging_flat` as appropriate.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw standards
        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Default number to associate
        howmany = howmany if howmany else 1

        filters = []

        # Find the dispersion, assume worst case if we can't match it
        n = 1200.0
        disperser_values = ['1200', '600', '831', '400', '150']
        for dv in disperser_values:
            if dv in self.descriptors['disperser']:
                n = float(dv)
        dispersion = 0.03/n
        # Replace with this if we start storing dispersion in the gmos table and map
        # it in above in the list of `instrDescriptors` to copy.
        # dispersion = float(self.descriptors['dispersion'])

        # per conversation with Chris Simpson
        tolerance = 200 * dispersion

        central_wavelength = float(self.descriptors['central_wavelength'])
        lower_bound = central_wavelength - tolerance
        upper_bound = central_wavelength + tolerance

        filters.append(Header.central_wavelength.between(lower_bound, upper_bound))

        # we get 1000 rows here to have a limit of some sort, but in practice
        # we get all the cals, then sort them below, then limit it per the request
        results = (
            self.get_query() 
                .standard(processed)
                .add_filters(*filters) 
                .match_descriptors(Header.instrument,
                                   Gmos.disperser,
                                   Gmos.detector_x_bin,
                                   Gmos.detector_y_bin,
                                   Gmos.filter_name) 
                # Absolute time separation must be within 1 year
                .max_interval(days=183)
                .all(1000))

        ut_datetime = self.descriptors['ut_datetime']
        wavelength = float(self.descriptors['central_wavelength'])

        # we score it based on the wavelength deviation expressed as a fraction of the wavelength itself,
        # plus the difference in date as a fraction of the allowed 365 day interval.  This is a placeholder
        # and we can do something else, add some weighting, add a squaring of the deviation, or other terms
        def score(header):
            if not isinstance(header, Header):
                header = header[0]
            wavelength_score = abs(float(header.central_wavelength) - wavelength) / tolerance
            ut_datetime_score = abs((header.ut_datetime - ut_datetime).seconds) / (30.0*24.0*60.0*60.0)
            return wavelength_score + ut_datetime_score

        retval = [r for r in results]

        # do the actual sort and return our requested max results
        retval.sort(key=score)
        if len(retval) > howmany:
            return retval[0:howmany]
        else:
            return retval


    # We don't handle processed ones (yet)
    @not_processed
    @not_imaging
    def spectwilight(self, processed=False, howmany=None):
        """
        Method to find the best spectwilight - ie spectroscopy twilight
        ie MOS / IFU / LS twilight

        This will match on 'Twilight' spectroscopy for the same instrument, filter,
        focal plane mask, disperser, and amp read area with the same
        x and y binning and a central wavelength within 0.02 microns within 1 year.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw spectwilights.  Currently processed are not supported.
        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Default number to associate
        howmany = howmany if howmany else 2

        filters = []
        # The science amp_read_area must be equal or substring of the
        # cal amp_read_area. If the science frame uses all the amps, then they
        # must be a direct match as all amps must be there - this is more efficient
        # for the DB as it will use the index. Otherwise, the science frame could
        # have a subset of the amps thus we must do the substring match.

        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            filters.append(Gmos.amp_read_area == self.descriptors['amp_read_area'])
        elif self.descriptors['amp_read_area'] is not None:
                filters.append(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))

        return (
            self.get_query()
                # They are OBJECT spectroscopy frames with target twilight
                .raw().OBJECT().spectroscopy(True).object('Twilight')
                .add_filters(*filters)
                .match_descriptors(Header.instrument,
                                   # Gmos.detector_x_bin, ### no CCDSUM in the files I got from Bruno
                                   # Gmos.detector_y_bin,
                                   Gmos.filter_name,
                                   Gmos.disperser,
                                   Gmos.focal_plane_mask)
                # Must match central wavelength to within some tolerance.
                # We don't do separate ones for dithers in wavelength?
                # tolerance = 0.02 microns
                .tolerance(central_wavelength=0.02)
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
                .all(howmany)
            )

    # We don't handle processed ones (yet)
    @not_processed
    @not_imaging
    def specphot(self, processed=False, howmany=None):
        """
        Method to find the best specphot observation

        This will match on non-'Twilight' partner cal or program cal for the same instrument, filter,
        disperser, focal plane mask, and amp read area with  a central wavelength within 0.05
        (or 0.1 for MOS) microns within 1 year.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw specphots.
        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Default number to associate
        howmany = howmany if howmany else 4

        filters = []
        # Must match the focal plane mask, unless the science is a mos mask in
        # which case the specphot is longslit.
        
        if 'MOS' in self.types:
            filters.append(Gmos.focal_plane_mask.contains('arcsec'))
            tol = 0.10 # microns
        else:
            filters.append(Gmos.focal_plane_mask == self.descriptors['focal_plane_mask'])
            tol = 0.05 # microns

        # The science amp_read_area must be equal or substring of the
        # cal amp_read_area. If the science frame uses all the amps, then they must
        # be a direct match as all amps must be there - this is more efficient for
        # the DB as it will use the index. Otherwise, the science frame could
        # have a subset of the amps thus we must do the substring match.

        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            filters.append(Gmos.amp_read_area == self.descriptors['amp_read_area'])
        elif self.descriptors['amp_read_area'] is not None:
                filters.append(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))

        return (
            self.get_query()
                # They are OBJECT partnerCal or progCal spectroscopy frames with
                # target not twilight.
                .raw().OBJECT().spectroscopy(True)
                .add_filters(Header.observation_class.in_(['partnerCal', 'progCal']),
                             Header.object != 'Twilight',
                             *filters)
                # Found lots of examples where detector binning does not match,
                # so we're not adding those
                .match_descriptors(Header.instrument,
                                   Gmos.filter_name,
                                   Gmos.disperser)
                # Must match central wavelength to within some tolerance.
                # We don't do separate ones for dithers in wavelength?
                .tolerance(central_wavelength=tol)
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
                .all(howmany)
            )

    # We don't handle processed ones (yet)
    @not_processed
    @not_spectroscopy
    def photometric_standard(self, processed=False, howmany=None):
        """
        Method to find the best photometric standard observation

        This will match on partner cal files with a 'CAL' program id and matching filter name
        taken within a day.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw photometric standards.
        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Default number to associate
        howmany = howmany if howmany else 4

        return (
            self.get_query()
                # They are OBJECT imaging partnerCal frames taken from CAL program IDs
                .photometric_standard(OBJECT=True, partnerCal=True)
                .add_filters(Header.program_id.like('G_-CAL%'))
                .match_descriptors(Header.instrument,
                                   Gmos.filter_name)
                # Absolute time separation must be within 1 days
                .max_interval(days=1)
                .all(howmany)
            )

    # We don't handle processed ones (yet)
    @not_processed
    def mask(self, processed=False, howmany=None):
        """
        Method to find the best mask

        This will match on GMOS 'MASK' observation type data with a matching focal plane mask.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw masks.
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
                # They are MASK observation type
                # The focal_plane_mask of the science file must match the
                # data_label of the MASK file (yes, really...)
                # Cant force an instrument match as sometimes it just says GMOS
                # in the mask...
            
                .add_filters(Header.observation_type == 'MASK',
                             Header.data_label == self.descriptors['focal_plane_mask'],
                             Header.instrument.startswith('GMOS'))
                .all(howmany)
            )

    @not_imaging
    def slitillum(self, processed=False, howmany=None):
        """
        Method to find the best slit response for the target dataset.
        """
        # Default number to associate
        howmany = howmany if howmany else 1

        filters = []

        lower_bound, upper_bound, tolerance = self._get_fuzzy_wavelength()
        filters.append(Header.central_wavelength.between(lower_bound, upper_bound))

        # we get 1000 rows here to have a limit of some sort, but in practice
        # we get all the cals, then sort them below, then limit it per the request
        results = (
            self.get_query()
                .slitillum(processed)
                .add_filters(*filters)
                .match_descriptors(Header.instrument,
                                   Gmos.disperser,
                                   Gmos.detector_x_bin,
                                   Gmos.detector_y_bin,
                                   Gmos.filter_name)
                # Absolute time separation must be within 1 year
                .max_interval(days=183)
                .all(1000))

        ut_datetime = self.descriptors['ut_datetime']
        wavelength = float(self.descriptors['central_wavelength'])

        # we score it based on the wavelength deviation expressed as a fraction of the wavelength itself,
        # plus the difference in date as a fraction of the allowed 365 day interval.  This is a placeholder
        # and we can do something else, add some weighting, add a squaring of the deviation, or other terms
        def score(header):
            if not isinstance(header, Header):
                header = header[0]
            wavelength_score = abs(float(header.central_wavelength) - wavelength) / tolerance
            ut_datetime_score = abs((header.ut_datetime - ut_datetime).seconds) / (30.0*24.0*60.0*60.0)
            return wavelength_score + ut_datetime_score

        retval = [r for r in results]

        # do the actual sort and return our requested max results
        retval.sort(key=score)
        if len(retval) > howmany:
            return retval[0:howmany]
        else:
            return retval

    def _get_fuzzy_wavelength(self):
        """
        Get a fuzzy match for wavelengths.

        This uses a simplified calculation for the disperser to get a tolerance
        for the wavelength.  Then it calculates the upper and lower wavelength
        to match.  It returns these bounds, plus the tolerance so the caller can
        use that to judge "closeness" of matches in terms of the tolerance.
        That can be used for scoring the quality of the wavelength match.

        Returns
        -------
        float, float, float : lower wavelength,  upper wavelength, tolerance
        """
        # Find the dispersion, assume worst case if we can't match it
        n = 1200.0
        disperser_values = ['1200', '600', '831', '400', '150']
        for dv in disperser_values:
            if dv in self.descriptors['disperser']:
                n = float(dv)
        dispersion = 0.03/n
        # Replace with this if we start storing dispersion in the gmos table and map
        # it in above in the list of `instrDescriptors` to copy.
        # dispersion = float(self.descriptors['dispersion'])

        # per conversation with Chris Simpson
        tolerance = 200 * dispersion

        central_wavelength = float(self.descriptors['central_wavelength'])
        lower_bound = central_wavelength - tolerance
        upper_bound = central_wavelength + tolerance

        return lower_bound, upper_bound, tolerance
