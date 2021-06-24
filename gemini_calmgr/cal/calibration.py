"""
This module holds the Calibration superclass
"""
import os

import functools
from gemini_obs_db.file     import File
from gemini_obs_db.diskfile import DiskFile
from gemini_obs_db.header import Header

from sqlalchemy import func, desc, case
from sqlalchemy.orm import join
from datetime import timedelta

from .. import gemini_metadata_utils as gmu
# from fits_storage.utils.render_query import render_query

DEFAULT_ORDER_BY_FIRST = 0
DEFAULT_ORDER_BY_LAST  = 1
DEFAULT_ORDER_BY_NONE  = 2

# A common theme across calibrations is that some of them don't handle processed
# data and will just return an empty list of calibs. This decorator is just some
# syntactic sugar for that common pattern.
def not_processed(f):
    """
    not_processed(self, processed=False, *args, **kw)

    Decorator for association methods that do not implement the search for
    processed calibs. It defaults to `processed` = ``False``. If `processed`
    is ``True`` it returns an empty list, instead of calling the real method

    """
    @functools.wraps(f)
    def wrapper(self, processed=False, *args, **kw):
        if processed:
            return []
        return f(self, processed=processed, *args, **kw)

    return wrapper

# Another common pattern: calibrations that don't apply for imaging
def not_imaging(f):
    """
    not_imaging(self, *args, **kw)

    Decorator for association methods that do not implement the search for
    imaging calibs.

    If ``self.descriptors['spectroscopy']`` is ``False`` it returns an empty
    list instead of calling the real method.

    """
    @functools.wraps(f)
    def wrapper(self, *args, **kw):
        if self.descriptors['spectroscopy'] == False:
            return []
        return f(self, *args, **kw)

    return wrapper

# Another common pattern: calibrations that don't apply for spectroscopy
def not_spectroscopy(f):
    """
    not_spectroscopy(self, *args, **kw)

    Decorator for association methods that do not implement the searchfor
    spectroscopy calibs.

    If ``self.descriptors['spectroscopy']`` is ``True`` it returns an empty
    list instead of calling the real method.

    """
    @functools.wraps(f)
    def wrapper(self, *args, **kw):
        if self.descriptors['spectroscopy'] == True:
            return []
        return f(self, *args, **kw)

    return wrapper

class CalQuery(object):
    """
    Class that helps to build the queries for calibration.

    Using CalQuery reduces a great deal of the boilerplate code and provides a
    kind of DSL (Domain Specific Language) that should make query building more
    natural -or easier to understand- for the calibrations.
    
    The object is initialized with a valid `session` object, the ORM class
    associated to the relevant instrument (`instrClass`), and a list of the
    possible `descriptors` accepted by the instrument.

    If `full_query ` is ``True``, the query will return tuples of the form,

        (Header, DiskFile, File)

    If it is ``False`` (default), it will return ``(Header,)`` tuples.

    Activate the full query if will need access to the ``DiskFile/File``
    associated with a Header.

    """
    def __init__(self, session, instrClass, descriptors, procmode=None, full_query=False):
        # Keep a copy of the instrument descriptors and start the query with some
        # common filters
        self.procmode = procmode
        self.descr = descriptors
        if full_query:
            query = (session.query(Header, DiskFile, File)
                            .select_from(join(join(join(instrClass, Header), DiskFile), File))
                            .filter(DiskFile.id == Header.diskfile_id)
                            .filter(File.id == DiskFile.file_id))
        else:
            query = (session.query(Header)
                            .select_from(join(join(instrClass, Header), DiskFile)))
        if procmode == 'sq':
            query = query.filter(Header.procmode == procmode)
        # elif procmode == 'ql':
        #     query = query.filter(Header.procmode.in_(['ql', 'sq']))

        self.query = (query.filter(DiskFile.canonical == True) # Search canonical entries
                           .filter(Header.qa_state != 'Fail')  # Knock out FAILs
			   .filter(Header.engineering == False)) # No engineering data


    def __call_through(self, query_method, *args, **kw):
        """
        For internal arbitrary calls to the internal SQLAlchemy query object.

        """
        self.query = query_method(*args, **kw)
        return self


    def add_filters(self, *args):
        """
        Takes a number of arguments (`args`) which are SQLAlchemy expressions.

        Adds each one of those expressions to the internal query as filters,
        effectively adding conditions like this: ``args[0] AND args[1] AND ...``

        """
        for arg in args:
            self.query = self.query.filter(arg)

        return self

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
            self.query = self.query.filter(arg == self.descr[field])

        return self

    def max_interval(self, **kw):
        """
        Max interval requires keyword arguments. The arguments it accepts are the
        same as

           :py:class:`datetime.timedelta` (`days`, `seconds`, ...)

        Using those arguments, it will construct a timedelta and a filter will
        be added that constrains the returned calibrations so that their datetimes
        fit in
        ::

            header.ut_datetime-delta < cal.ut_datetime < header.ut_datetime+delta

        """
        max_int = timedelta(**kw)
        datetime_lo = self.descr['ut_datetime'] - max_int
        datetime_hi = self.descr['ut_datetime'] + max_int

        self.query = (self.query.filter(Header.ut_datetime > datetime_lo) # abs time sep.
                                .filter(Header.ut_datetime < datetime_hi))

        return self

    def if_(self, condition, methodname, *args, **kw):
        """
        A convenience for filters that are added conditionally. To be used like
        in this example from GMOS:

            >> if_(self.descriptors['nodandshuffle'], 'match_descriptors', Gmos.nod_count, Gmos.nod_pixels)

        This method exists only so that we can construct a whole query just by chaining.

        """
        if condition:
            return getattr(self, methodname)(*args, **kw)

        return self

    def __getattr__(self, name):
        """
        The CalQuery instances define only a handful of methods that are used
        often, but we would still want to expose other elements. Instead of
        creating methods for them, we try to figure out what the user wanted from
        the name of the attribute. The attribute search follows this order,
        returning the first that matches:

        1) See if the the internal SQLAlchemy query has that attribute. In this
           way we allow arbitrary queries to be formed, but this shouldn't be abused.
        2) See if the attribute is one of the observation types. If it is, then
           invoking the returned object will be the same as doing 
               ``.observation_type('attribute_name')``
        3) Same as 2), but with observation classes and the `observation_class` method
        4) Same as 2), but with reduction states and the `reduction` method
        5) If nothing matches, raise an AttributeError

        """
        attmsg = "'{}' object has no attribute '{}'"
        try:
            return functools.partial(self.__call_through, getattr(self.query, name))
        except AttributeError:
            if name in gmu.obs_types:
                return functools.partial(self.observation_type, name)
            elif name in gmu.obs_classes:
                return functools.partial(self.observation_class, name)
            elif name in gmu.reduction_states:
                return functools.partial(self.reduction, name)

            raise AttributeError(attmsg.format(self.__class__.__name__, name))

    def all(self, limit, extra_order_terms=None, default_order = DEFAULT_ORDER_BY_LAST):
        """
        Returns a list of results, limited in number by the `limit` argument.

        The function will optionally sort the data according to certain criteria,
        according to the values of `extra_order_terms` and `default_order`.

        `extra_order_terms` is by default ``None``, and accepts an iterable 
        (tuple, list, ...) of SQLAlchemy sorting terms.
        E.g.,

            ``desc(Header.blah == foo)``)

        which reflects additional sorting terms out of our "default".

        `default_order` defaults to ``DEFAULT_ORDER_BY_LAST`` and accepts also
        ``DEFAULT_ORDER_BY_FIRST`` and ``DEFAULT_ORDER_BY_NONE``. It affects the
        application of the "default" sorting, which means "closer in time first".

        If `default_order` is **not** ``DEFAULT_ORDER_BY_NONE``, then our default
        sorting will be applied either at the beginning or the end of the terms
        supplied in `extra_order_terms`. If `extra_order_terms` is ``None``, only
        the ``default_order`` setting will apply.

        Examples:

        # Returns up to 5 objects, applying only the default order
        >> query_object.all(limit=5)

        # Returns up to 5 objects, sorting by "Header.program_id == BLAH" (matching first)
        # and then by the default order
        >> query_object.all(limit=5, extra_order_terms=[desc(Header.program_id=='BLAH')])

        # Returns up to 5 objects, sorting by the default order criteria, and then
        # "Header.program_id == BLAH"   (matching first)
        >> query_object.all(limit=5, extra_order_terms=[desc(Header.program_id=='BLAH')], default_order=DEFAULT_ORDER_BY_FIRST)

        """
        # Enforcing deterministic sort of procmode by a case query, postgresql was exhibiting reverse alpha sort
        # on natural order and that surprised me
        enums = Header.procmode.type.enums
        whens = {pm: str(pm) if pm else 'AAA' for pm in
                 enums}
        sort_logic = case(value=Header.procmode, whens=whens, else_='AAA').label("procmode_sortkey")

        order = () if extra_order_terms is None else tuple(extra_order_terms)

        if default_order is not DEFAULT_ORDER_BY_NONE:
            # Order by absolute time separation.
            targ_ut_dt_secs = int((self.descr['ut_datetime'] - Header.UT_DATETIME_SECS_EPOCH).total_seconds())
            def_order = func.abs(Header.ut_datetime_secs - targ_ut_dt_secs)
            present_order = desc(DiskFile.present)
            procmode_order = desc(sort_logic)  # Header.procmode
            if default_order == DEFAULT_ORDER_BY_LAST:
                order = order + (present_order, def_order, procmode_order)
            else:
                order = (present_order, def_order, procmode_order) + order

        if order:
            self.query = self.query.order_by(*order)

        # if os.getenv("CAL_QUERY_DEBUG", False):
        #     print("Query: %s" % render_query(self.query))

        return self.query.limit(limit).all()

    # The following add filters specific to certain types of calibs

    def tolerance(self, condition=True, **kw):
        """
        Takes a number of descriptors as keyword arguments (only valid for the
        common header, not the instrument specific descriptors), in the form 
        `descriptor=tolerance`.

        The internal query will get added filters for each descriptor to ensure
        that the results match within the specified tolerance.

        There's an extra keyword argument, `condition`: if condition is false,
        the tolerances won't be added (by default it is ``True``). This is so
        that we can add or skip the tolerance tests to the query using just call
        chaining, to keep everything compact.

        """
        rt_err = "No such descriptor '{}' defined in the header"
        if condition:
            for descriptor, tol in list(kw.items()):
                # Occassionally we get a None for some descriptors, so run this
                # in a try except
                try:
                    lo = float(self.descr[descriptor]) - tol
                    hi = float(self.descr[descriptor]) + tol
                    # This may raise AttributeError, we let it go through
                    column = getattr(Header, descriptor)
                    self.query = (self.query.filter(column > lo).filter(column < hi))
                except KeyError:
                    raise RuntimeError(rt_err.format(descriptor))
                except TypeError:
                    pass

        return self

    def raw(self):
        """
        Filter: shorthand for ``reduction('RAW')``

        """
        return self.reduction('RAW')

    def reduction(self, red):
        """
        Filter: only images with ``Header.reduction`` = `red`

        """
        self.query = self.query.filter(Header.reduction == red)
        return self

    def observation_type(self, ot):
        """
        Filter: only images with ``Header.observation_type`` = `ot`

        """
        self.query = self.query.filter(Header.observation_type == ot)
        return self

    def observation_class(self, oc):
        """
        Filter: only images with ``Header.observation_class`` = `oc`

        """
        self.query = self.query.filter(Header.observation_class == oc)
        return self

    def object(self, ob):
        """
        Filter: only images with ``Header.object`` = `ob`

        """
        self.query = self.query.filter(Header.object == ob)
        return self

    def spectroscopy(self, status):
        """
        Filter: only images with ``Header.spectroscopy`` = `status`

        """
        self.query = self.query.filter(Header.spectroscopy == status)
        return self

    def raw_or_processed(self, name, processed):
        """
        Filter: If processed is ``True``, it is a shorthand for

        ``reduction("PROCESSED_" + name)``

        If not processed, then it is equivalent to

        ``raw().observation_type(name)``

        """
        if processed:
            return self.reduction('PROCESSED_' + name)
        else:
            return self.raw().observation_type(name)

    def raw_or_processed_by_types(self, name, processed):
        """
        Filter: If processed is ``True``, it is a shorthand for

        ``reduction("PROCESSED_" + name)``

        If not processed, then we look at the types (from AstroData tags).
        Not all calibrations are noted in the OBSTYPE header and this is
        an alternate way to distinguish them.

        ``raw().filter(Header.types.like('%{0}%'.format(name)))``

        """
        if processed:
            return self.reduction('PROCESSED_' + name)
        else:
            return self.raw().filter(Header.types.like('%''{0}''%'.format(name)))

    def bias(self, processed=False):
        """
        Filter: shorthand for ``raw_or_processed('BIAS', processed)``

        """
        return self.raw_or_processed('BIAS', processed)

    def dark(self, processed=False):
        """
        Filter: shorthand for ``raw_or_processed('DARK', processed)``

        """
        return self.raw_or_processed('DARK', processed)

    def flat(self, processed=False):
        """
        Filter: shorthand for ``raw_or_processed('FLAT', processed)``

        """
        return self.raw_or_processed('FLAT', processed)

    def arc(self, processed=False):
        """
        Filter: shorthand for ``raw_or_processed('ARC', processed)``

        """
        return self.raw_or_processed('ARC', processed)

    def standard(self, processed=False):
        """
        Filter: shorthand for ``raw_or_processed('STANDARD', processed)``
        """
        return self.raw_or_processed('STANDARD', processed)

    def pinhole(self, processed=False):
        """
        Filter: shorthand for ``raw_or_processed('PINHOLE', processed)``

        """
        return self.raw_or_processed('PINHOLE', processed)

    def photometric_standard(self, processed=False, **kw):
        """
        Filter: when `processed` is ``True`` this works as a shorthand for
        ``reduction('PROCESSED_PHOTSTANDARD')``.

        When not `processed`, it is equivalent to taking all the keyword
        arguments' names, using them to obtain custom property-name-based
        filters, and chaining them, like in this example:
        ::

               photometric_standard(OBJECT=True, partnerCal=True)

        is a shorthand for:
        ::

               OBJECT().partnerCal()

        Note that the values for the arguments are discarded, but we still need
        to provide something to be able to pass keyword arguments

        """
        # NOTE: PROCESSED_PHOTSTANDARDS are not used anywhere; this is an advance...
        if processed:
            return self.reduction('PROCESSED_PHOTSTANDARD')
        else:
            ret = self.raw().spectroscopy(False)
            for key in kw:
                ret = getattr(self, key)()
            return ret

    def telluric_standard(self, processed=False, **kw):
        """
        Filter: when `processed` is ``True`` this works as a shorthand for
        ``reduction('PROCESSED_TELLURIC')``.

        When not `processed`, it is equivalent to taking all the keyword
        arguments' names, using them to obtain custom property-name-based
        filters, and chaining them, like in this example:
        ::

               telluric_standard(OBJECT=True, science=True)

        is a shorthand for:
        ::

               OBJECT().science()

        Note that the values for the arguments are discarded, but we still need
        to provide something to be able to pass keyword arguments

        """
        if processed:
            return self.reduction('PROCESSED_TELLURIC')
        else:
            ret = self.raw().spectroscopy(True)
            for key in kw:
                ret = getattr(self, key)()
            return ret

    def slitillum(self, processed=False):
        """
        Filter: shorthand for ``raw_or_processed('SLITILLUM', processed)``
        """
        return self.raw_or_processed_by_types('SLITILLUM', processed)


class Calibration(object):
    """
    This class provides a basic Calibration Manager
    This is the superclass from which the instrument specific variants subclass.

    """
    session = None
    header = None
    descriptors = None
    types = None
    applicable = []
    instrClass = None
    instrDescriptors = ()

    def __init__(self, session, header, descriptors, types, procmode=None, full_query=False):
        """
        Initialize a calibration manager for a given header object (ie data file)
        Need to pass in an sqlalchemy session that should already be open, this
        class will not close it. Also pass in a header object

        """
        self.session = session
        self.header = header
        self.descriptors = descriptors
        self.types = types
        self.from_descriptors = False
        self.procmode = procmode
        self.full_query = full_query

        # Populate the descriptors dictionary for header
        if self.descriptors is None and self.instrClass is not None:
            self.from_descriptors = True
            self.types = eval(self.header.types)
            self.descriptors = {
                'header_id':            self.header.id,
                'observation_id':       self.header.observation_id,
                'observation_type':     self.header.observation_type,
                'observation_class':    self.header.observation_class,
                'spectroscopy':         self.header.spectroscopy,
                'object':               self.header.object,
                'instrument':           self.header.instrument,
                'central_wavelength':   self.header.central_wavelength,
                'program_id':           self.header.program_id,
                'ut_datetime':          self.header.ut_datetime,
                'exposure_time':        self.header.exposure_time,
                'detector_roi_setting': self.header.detector_roi_setting,
                'coadds':               self.header.coadds,
                'reduction':            self.header.reduction,
                'elevation':            self.header.elevation,
                'cass_rotator_pa':      self.header.cass_rotator_pa,
                'gcal_lamp':            self.header.gcal_lamp
                }

            iC = self.instrClass
            query = session.query(iC).filter(iC.header_id == self.descriptors['header_id'])
            inst = query.first()

            # Populate the descriptors dictionary for the instrument
            for descr in self.instrDescriptors:
                self.descriptors[descr] = getattr(inst, descr, None)
        elif self.descriptors is not None:
            # The data_section comes over as a native python array, needs to be a string
            if self.descriptors['data_section']:
                self.descriptors['data_section'] = str(self.descriptors['data_section'])

        # Set the list of applicable calibrations
        self.set_applicable()

    def get_query(self):
        """
        Returns an ``CalQuery`` object, populated with the current session,
        instrument class, descriptors and the setting for full/not-full query.

        """
        return CalQuery(self.session, self.instrClass, self.descriptors, procmode=self.procmode, full_query=self.full_query)

    def set_applicable(self):
        """
        This method determines which calibration types are applicable
        to the target data set, and records the list of applicable
        calibration types in the class applicable variable.
        All this really does is determine whether what calibrations the
        /calibrations feature will look for. Just because a caltype isn't
        applicable doesn't mean you can't ask the calmgr for one.

        The return value is a list of strings with the names of the applicable
        methods (eg. ``['bias', 'dark', 'flat']``)

        The base implementation returns an empty list. It **must** be overriden
        by every subclass of ``Calibration``.

        """
        self.applicable = []

    def bias(self, processed=False, howmany=None):
        # Not defined for this instrument
        return []

    def dark(self, processed=False, howmany=None):
        # Not defined for this instrument
        return []

    def flat(self, processed=False, howmany=None):
        # Not defined for this instrument
        return []

    def arc(self, processed=False, howmany=None):
        # Not defined for this instrument
        return []

    def fringe(self, processed=False, howmany=None):
        # Not defined for this instrument
        return []

    def ronchi_mask(self, processed=False, howmany=None):
        # Not defined for this instrument
        return []

    def pinhole_mask(self, processed=False, howmany=None):
        # Not defined for this instrument
        return []

    def spectwilight(self, processed=False, howmany=None):
        # Not defined for this instrument
        return []

    def lampoff_flat(self, processed=False, howmany=None):
        # Not defined for this instrument
        return []

    def domeflat(self, processed=False, howmany=None):
        # Not defined for this instrument
        return []

    def lampoff_domeflat(self, processed=False, howmany=None):
        # Not defined for this instrument
        return []

    def processed_fringe(self, howmany=None):
        # Not defined for this instrument
        return []

    def specphot(self, processed=False, howmany=None):
        # Not defined for this instrument
        return []

    def photometric_standard(self, processed=False, howmany=None):
        # Not defined for this instrument
        return []

    def qh_flat(self, processed=False, howmany=None):
        # Not defined for this instrument
        return []

    def telluric_standard(self, processed=False, howmany=None):
        # Not defined for this instrument
        return []

    def polarization_standard(self, processed=False, howmany=None):
        # Not defined for this instrument
        return []

    def astrometric_standard(self, processed=False, howmany=None):
        # Not defined for this instrument
        return []

    def polarization_flat(self, processed=False, howmany=None):
        # Not defined for this instrument
        return []

    def mask(self, processed=False, howmany=None):
        # Not defined for this instrument
        return []

    def slitillum(self, processed=False, howmany=None):
        # Not defined for this instrument
        return []
