"""
This module holds the CalibrationGHOST class
"""
from sqlalchemy import or_

from gemini_obs_db.orm.header import Header
from gemini_obs_db.orm.ghost import Ghost, ArmFieldDispatcher, GHOST_ARMS, GHOST_ARM_DESCRIPTORS
from .calibration import Calibration, not_processed, not_imaging, not_spectroscopy, CalQuery

import math


class DictDictFieldDispatcher(ArmFieldDispatcher):
    def __init__(self, d):
        super().__init__()
        self.d = d

    def arm(self):
        return self.d.get('arm', None)

    def get_field(self, field_name):
        return self.d.get(field_name, None)

    def set_field(self, field_name, value):
        self.d[field_name] = value


ARM_DESCRIPTORS = [f"{descr}_{arm}" for descr in GHOST_ARM_DESCRIPTORS for arm in GHOST_ARMS]


class CalibrationGHOST(Calibration):
    """
    This class implements a calibration manager for GHOST.
    It is a subclass of Calibration
    """
    instrClass = Ghost
    instrDescriptors = [
        'arm',
        'disperser',
        'filter_name',
        'focal_plane_mask',
        'detector_x_bin',
        'detector_y_bin',
        'amp_read_area',
        'read_speed_setting',
        'gain_setting',
        'res_mode',
        'prepared',
        'overscan_trimmed',
        'overscan_subtracted',
        'want_before_arc',
        ]
    instrDescriptors.extend(ARM_DESCRIPTORS)

    def __init__(self, session, header, descriptors, *args, **kwargs):
        # Need to super the parent class __init__ to get want_before_arc
        # keyword in
        super(CalibrationGHOST, self).__init__(session, header, descriptors, *args, **kwargs)

        if descriptors is None and self.instrClass is not None:
            iC = self.instrClass
            query = session.query(iC).filter(
                iC.header_id == self.descriptors['header_id'])
            inst = query.first()
            self.descriptors['want_before_arc'] = inst.want_before_arc
        if header is None:
            # non-DB source of data, so we need to parse out dictionaries
            # if the data came from the header+instrument tables then this was done during ingest and came pre-done
            dispatcher = DictDictFieldDispatcher(self.descriptors)
            dispatcher.populate_arm_fields()

        # Set the list of applicable calibrations
        self.set_applicable()

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

            if require_bias:
                self.applicable.append('bias')
                self.applicable.append('processed_bias')

            # If it (is spectroscopy) and * Note: tweaked for GHOST to ignore flag, it's basically always spectroscopy
            # * TBD how/what to change in the AstroDataGhost for DRAGONS master
            # (is an OBJECT) and
            # (is not a Twilight) and
            # (is not a specphot)
            # then it needs an arc, flat, spectwilight, specphot
            if (  # (self.descriptors['spectroscopy'] == True) and
                    (self.descriptors['observation_type'] == 'OBJECT') and
                    (self.descriptors['object'] != 'Twilight') and
                    (self.descriptors['observation_class'] not in ['partnerCal', 'progCal'])):
                self.applicable.append('arc')
                self.applicable.append('processed_arc')
                self.applicable.append('flat')
                self.applicable.append('processed_flat')
                # self.applicable.append('spectwilight')
                self.applicable.append('specphot')

    # @not_imaging
    def arc(self, processed=False, howmany=2, return_query=False):
        """
        This method identifies the best GHOST ARC to use for the target
        dataset.

        This will find GHOST arcs with matching wavelength within 0.001 microns, disperser, and filter name.
        If "want_before_arc" is set and true, it limits to 1 result and only matches observations prior to the
        ut_datetime.  If it is set and false, it limits to 1 result after the ut_datetime.  Otherwise, it keeps
        the `howmany` as specified with a default of 2 and has no restriction on ut_datetime.
        It matches within 1 year.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw arcs
        howmany : int, default 2 if `want_before_arc` is not set, or 1 if it is
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        ab = self.descriptors.get('want_before_arc', None)
        # Default 2 arcs, hopefully one before and one after
        if ab is not None:
            howmany = 1
        else:
            howmany = howmany if howmany else 2
        filters = []
        # # Must match focal_plane_mask only if it's not the 5.0arcsec slit in the target, otherwise any longslit is OK
        # if self.descriptors['focal_plane_mask'] != '5.0arcsec':
        #     filters.append(Ghost.focal_plane_mask == self.descriptors['focal_plane_mask'])
        # else:
        #     filters.append(Ghost.focal_plane_mask.like('%arcsec'))

        if ab:
            # Add the 'before' filter
            filters.append(Header.ut_datetime < self.descriptors['ut_datetime'])
        elif ab is None:
            # No action required
            pass
        else:
            # Add the after filter
            filters.append(Header.ut_datetime > self.descriptors['ut_datetime'])

        query = (
            self.get_query()
                .arc(processed)
                .add_filters(*filters)
                .match_descriptors(Header.instrument,
                                   Header.camera,
                                   # Ghost.disperser,
                                   # Ghost.filter_name,
                                   Ghost.res_mode)
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
            )
        if return_query:
            return query.all(howmany), query
        else:
            return query.all(howmany)

    def dark(self, processed=False, howmany=None, return_query=False):
        """
        Method to find best GHOST Dark frame for the target dataset.

        This will find GHOST darks with matching read speed setting, gain setting, and within 50 seconds
        exposure time.  It matches within 1 year.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw darks
        howmany : int, default 1 if processed, else 15

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 5

        query = (
            self.get_query()
                .dark(processed)
                .match_descriptors(Header.instrument,
                                   Ghost.read_speed_setting,
                                   Ghost.gain_setting)
                .tolerance(exposure_time = 50.0)
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
            )
        if return_query:
            return query.all(howmany), query
        else:
            return query.all(howmany)

    def bias(self, processed=False, howmany=None, return_query=False):
        """
        Method to find the best bias frames for the target dataset

        This will find GHOST biases with matching read speed setting, gain setting, amp read area, and x and y binning.
        If it's 'prepared' data, it will match overscan trimmed and overscan subtracted.

        It matches within 90 days

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw biases
        howmany : int, default 1 if processed, else 50

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 5

        filters = []

        # The Overscan section handling: this only applies to processed biases
        # as raw biases will never be overscan trimmed or subtracted, and if they're
        # processing their own biases, they ought to know what they want to do.
        if processed:
            if self.descriptors['prepared']:
                # If the target frame is prepared, then we match the overscan state.
                filters.append(Ghost.overscan_trimmed == self.descriptors['overscan_trimmed'])
                filters.append(Ghost.overscan_subtracted == self.descriptors['overscan_subtracted'])
            else:
                # If the target frame is not prepared, then we don't know what thier procesing intentions are.
                # we could go with the default (which is trimmed and subtracted).
                # But actually it's better to just send them what we have, as we has a mishmash of both historically
                #filters.append(Ghost.overscan_trimmed == True)
                #filters.append(Ghost.overscan_subtracted == True)
                pass

        query = (
            self.get_query()
                .bias(processed)
                .add_filters(*filters)
                .match_descriptors(Header.instrument,
                                   Header.camera,
                                   Ghost.detector_x_bin,
                                   Ghost.detector_y_bin,
                                   Ghost.read_speed_setting,
                                   Ghost.gain_setting)
                # Absolute time separation must be within 3 months
                .max_interval(days=90)
            )
        if return_query:
            return query.all(howmany), query
        else:
            return query.all(howmany)

    def bpm(self, processed=False, howmany=None, return_query=False):
        """
        This method identifies the best GHOST BPM to use for the target
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

        filters = [Header.ut_datetime <= self.descriptors['ut_datetime']]

        query = self.get_query(include_engineering=True) \
                    .bpm(processed) \
                    .add_filters(*filters) \
                    .match_descriptors(Header.instrument,
                                       Ghost.arm)
        results = query.all(howmany)

        if return_query:
            return results, query
        else:
            return results

    def imaging_flat(self, processed, howmany, flat_descr, filt, return_query=False):
        """
        Method to find the best imaging flats for the target dataset

        This will find imaging flats that are either obervation type of 'FLAT' or
        are both dayCal and 'Twilight'.  This also adds a large set of flat filters
        in flat_descr from the higher level flat query.

        This will find GHOST imaging flats with matching read speed setting, gain setting, filter name,
        res mode, focal plane mask, and disperser.

        It matches within 180 days

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw imaging flats
        howmany : int, default 1 if processed, else 20
            How many do we want results
        flat_descr: list
            set of filter parameters from the higher level function calling into this helper method
        filt: list
            Additional filter terms to apply from the higher level method

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 20

        if processed:
            query = self.get_query().PROCESSED_FLAT()
        else:
            # Find the relevant slit flat
            query = self.get_query().spectroscopy(
                False).observation_type('FLAT')
        query = (
            query.add_filters(*filt)
                 .match_descriptors(*flat_descr)
                 # Absolute time separation must be within 6 months
                 .max_interval(days=180)
            )
        if return_query:
            return query.all(howmany), query
        else:
            return query.all(howmany)

    def spectroscopy_flat(self, processed, howmany, flat_descr, filt, return_query=False):
        """
        Method to find the best imaging flats for the target dataset

        This will find spectroscopy flats with a central wavelength within 0.001 microns, a matching elevation, and
        matching cass rotator pa (for elevation under 85).  The specific tolerances for elevation
        depend on factors such as the type of focal plane mask.  The search also adds a large set of flat filters
        in flat_descr and filt from the higher level flat query.

        It matches within 180 days

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw imaging flats
        howmany : int, default 1 if processed, else 2
            How many do we want results
        flat_descr: list
            set of filter parameters from the higher level function calling into this helper method
        filt: list
            Additional filter terms to apply from the higher level method

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 2

        query = (
            self.get_query()
                .flat(processed)
                .add_filters(*filt)
                .match_descriptors(*flat_descr)

            # Absolute time separation must be within 6 months
                .max_interval(days=180)
            )
        if return_query:
            return query.all(howmany), query
        else:
            return query.all(howmany)

    def flat(self, processed=False, howmany=None, return_query=False):
        """
        Method to find the best GHOST FLAT fields for the target dataset

        This will find GHOST (echelle) flats with matching read speed setting,
        gain setting, filter name, and res mode.
        Then additional filtering is done as per :meth:`spectroscopy_flat`.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw imaging flats
        howmany : int
            How many do we want

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        filters = []

        # Common descriptors for both types of flat
        # Must totally match instrument, detector_x_bin, detector_y_bin, filter
        flat_descriptors = (
            Header.instrument,
            Header.camera,
            Ghost.read_speed_setting,
            Ghost.gain_setting,
            Ghost.res_mode,
            Header.spectroscopy,
            )

        # as above, GHOST is spect
        if self.descriptors['spectroscopy']:
            # Only 1x1 flats should be used
            #filters = [Ghost.detector_x_bin == 1,
            #          Ghost.detector_y_bin == 1]
            return self.spectroscopy_flat(processed, howmany, flat_descriptors, filters, return_query=return_query)
        else:
            return self.imaging_flat(processed, howmany, flat_descriptors, filters, return_query=return_query)

    def processed_slitflat(self, howmany=None, return_query=False):
        """
        Method to find the best GHOST SLITFLAT for the target dataset

        If the type is 'SLITV', this method falls back to the regular :meth:`flat` logic.

        This will find GHOST imaging flats with matching read speed setting, gain setting, filter name,
        res mode, and disperser.  It filters further on the logic in :meth:`imaging_flat`.

        It matches within 180 days

        Parameters
        ----------

        howmany : int, default 1
            How many do we want results
        flat_descr: list
            set of filter parameters from the higher level function calling into this helper method
        filt: list
            Additional filter terms to apply from the higher level method
        sf: bool
            True for slit flats, else False

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if 'SLITV' in self.types:
            return self.flat(True, howmany, return_query=return_query)

        filters = (Ghost.arm == 'slitv',)

        # Common descriptors for both types of flat
        # Must totally match instrument, detector_x_bin, detector_y_bin, filter
        flat_descriptors = (
            Header.instrument,
            Ghost.res_mode,
            )

        return self.imaging_flat(False, howmany, flat_descriptors, filters,
                                 return_query=return_query)

    def processed_slit(self, howmany=None, return_query=False):
        """
        Method to find the best processed GHOST SLIT for the target dataset

        This will find GHOST processed slits.  It matches the observation
        type, res mode, and within 30 seconds.  For 'ARC' observation type it matches
        'PROCESSED_UNKNOWN' data, otherwise it matches 'PREPARED' data.

        Parameters
        ----------

        howmany : int
            How many do we want results

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        descripts = [
            Header.instrument,
            Header.observation_type,
            Ghost.res_mode
            ]

        filters = [Ghost.arm == 'slitv']

        # We need to match exposure time for on-sky observations
        # (the exposure time has been munged in the processed_slit to match
        # the science exposure that needs it)
        if self.descriptors['observation_type'] not in ('ARC', 'BIAS', 'FLAT'):
            filters.append(self.descriptors['exposure_time'] == Ghost.exposure_time_slitv)

        query = (
            self.get_query()
                .reduction(  # this may change pending feedback from Kathleen
                    'PROCESSED_ARC' if
                    self.descriptors['observation_type'] == 'ARC' else
                    'PROCESSED_UNKNOWN'
                )
                .spectroscopy(False)
                .match_descriptors(*descripts)
                .add_filters(*filters)
                # Need to use the slit image that matches the input observation;
                # needs to match within 30 seconds!
                .max_interval(seconds=30)
            )
        if return_query:
            return query.all(howmany), query
        else:
            return query.all(howmany)


    def processed_fringe(self, howmany=None, return_query=False):
        """
        GHOST fringe frames don't exist (yet?)

        Method to find the best GHOST processed fringe for the target dataset

        This will find GHOST processed fringes matching the x and y binning.
        It matches within 1 year.

        Parameters
        ----------

        howmany : int, default 1
            How many do we want results

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Default number to associate
        howmany = howmany if howmany else 1

        query = (
            self.get_query()
                .PROCESSED_FRINGE()
                .match_descriptors(Header.instrument,
                                   Ghost.detector_x_bin,
                                   Ghost.detector_y_bin,
                                   Ghost.res_mode)
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
            )
        if return_query:
            return query.all(howmany), query
        else:
            return query.all(howmany)

    # We don't handle processed ones (yet)
    # @not_processed
    # @not_imaging
    def specphot(self, processed=False, howmany=None, return_query=False):
        """
        Method to find the best specphot observation

        This will find GHOST spec photometry matching the amp read area, filter name, and disperser.
        The data must be partnerCal or progCal and not be Twilight.  If the focal plane mask is measured
        in arcsec, it will match the central wavelength to within 0.1 microns, else it matches within 0.05
        microns.

        It matches within 1 year.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw spec photometry
        howmany : int, default 1
            How many do we want results

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Default number to associate
        howmany = howmany if howmany else 1

        query = (self.get_query()
                 .match_descriptors(Header.instrument,
                                    Header.camera,
                                    Ghost.res_mode)
                 .max_interval(days=365))
        if processed:
            query = query.standard(processed)
        else:
            query = query.add_filters([Header.observation_class.in_(['partnerCal', 'progCal'])]).raw().OBJECT()

        if return_query:
            return query.all(howmany), query
        else:
            return query.all(howmany)

    standard = specphot  # because everything's spectroscopy

    def get_query(self, include_engineering=False):
        """
        Returns a ``GHOSTCalQuery`` object, populated with the current session,
        instrument class, descriptors and the setting for full/not-full query.
        """
        return GHOSTCalQuery(self.session, self.instrClass, self.descriptors,
                             procmode=self.procmode, full_query=self.full_query,
                             include_engineering=include_engineering)


class GHOSTCalQuery(CalQuery):

    def match_descriptors(self, *args):
        """
        Takes a numbers of expressions (E.g., ``Header.foo``, ``Instrument.bar``),
        figures out the descriptor to use from the column name, and adds the
        right filter to the query, replacing boilerplate code like the following:
        ::

            Header.foo == descriptors['foo']
            Instrument.bar == descriptors['bar']

        """
        for arg in args:
            field = arg.expression.name
            if field in GHOST_ARM_DESCRIPTORS:
                if 'arm' not in self.descr or self.descr['arm'] is None:
                    # need to group match any arm
                    self.query = self.query.filter(or_(
                        *[getattr(arg.class_, field + arm) == self.descr[field + arm]
                          for arm in (f'_{arm}' for arm in GHOST_ARMS)]
                    ))
                else:
                    # need to match arm variant of descriptor
                    arm = '_' + self.descr['arm']
                    self.query = self.query.filter(
                        getattr(arg.class_, field + arm) == self.descr[field + arm]
                    )
            else:
                self.query = self.query.filter(arg == self.descr[field])

        return self
