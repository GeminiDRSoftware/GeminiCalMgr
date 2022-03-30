import re

import sys
import logging
from os.path import basename

from _pytest import monkeypatch

from gemini_calmgr.utils import dbtools
from gemini_calmgr.utils.dbtools import REQUIRED_TAG_DICT, instrument_table
from gemini_obs_db.db import sessionfactory
from recipe_system.cal_service.localmanager import extra_descript, args_for_cals, LocalManager
from recipe_system.cal_service.calrequestlib import get_cal_requests
from gemini_calmgr.cal import get_cal_object

from sqlalchemy.sql.elements import BooleanClauseList, BinaryExpression

import astrodata
import gemini_instruments

from gemini_obs_db.orm.header import Header
from gemini_obs_db.orm.diskfile import DiskFile
from gemini_obs_db.orm.gmos import Gmos
from gemini_obs_db.orm.niri import Niri

from gemini_calmgr.utils.debugging import get_status, get_calibration_type


def show_line(table_name, key, cal_value, value, expr):
    ascii_codes = {
        "pass": ("\u001b[32m", "\u001b[37m"),
        "fail": ("\u001b[31m", "\u001b[37m"),
        "unknown": ("\u001b[33m", "\u001b[37m")
    }
    status = get_status(value, cal_value, expr)
    start_code, stop_code = ascii_codes[status]
    if (not isinstance(cal_value, str) or len(cal_value) <= 28) \
            and (not isinstance(value, str) or len(value) <= 28):
        print("%s%9s | %18s | %30s | %30s | %s%s" % (start_code, table_name, key, cal_value, value, expr, stop_code))
    else:
        print("%s%9s | %18s | cal: %58s | %s%s" % (start_code, table_name, key, cal_value, expr, stop_code))
        print("%s%9s | %18s | val: %58s | %s%s" % (start_code, '', '', value, '', stop_code))


def debug_binary_expression(clause, cal_obj, header, diskfile, instr):
    if hasattr(clause.left, 'table'):  # isinstance(clause.left, AnnotatedColumn):
        table = clause.left.table
        key = clause.left.key
        val = clause.right.value if hasattr(clause.right, 'value') else None
        if val is None:
            if hasattr(clause.right, 'clauses') and len(clause.right.clauses) > 0:
                vals = []
                for cl in clause.right.clauses:
                    if hasattr(cl, 'value') and cl.value is not None:
                        vals.append("%s" % cl.value)
                val = ', '.join(vals)
            else:
                val = ''
        expr = "%s" % clause
        if table.name == 'header':
            show_line(table.name, key, getattr(header, key), val, expr)
        if table.name == 'diskfile':
            show_line(table.name, key, getattr(diskfile, key), val, expr)
        if table.name == 'gmos':
            show_line(table.name, key, getattr(instr, key), val, expr)


def debug_boolean_clause_list(clause, cal_obj, header, diskfile, instr):
    for clause in clause.clauses:
        for x in debug_dispatch(clause, cal_obj, header, diskfile, instr):
            yield x


def debug_dispatch(clause, cal_obj, header, diskfile, instr):
    if isinstance(clause, BooleanClauseList):
        debug_boolean_clause_list(clause, cal_obj, header, diskfile, instr)
    elif isinstance(clause, BinaryExpression):
        debug_binary_expression(clause, cal_obj, header, diskfile, instr)


def debug_parser(query, cal_obj, header, diskfile, instr):
    for clause in query.query.whereclause.clauses:
        debug_dispatch(clause, cal_obj, header, diskfile, instr)


def build_descripts(rq):
    descripts = rq.descriptors
    for (type_, desc) in list(extra_descript.items()):
        descripts[desc] = type_ in rq.tags
    return descripts


# Copying this from the normal calibration code so I can add non-processed calibrations
# TODO maybe make the dbtools behavior configurable to allow this without duplication
def modified_add_diskfile_entry(session, fileobj, filename, path, fullpath):
    """
    Add a DiskFile record for the given file.

    Parameters
    ----------
    session : :class:`~sqlalchemy.org.session.Session`
        Database session to operate on
    fileobj : :class:`~gemini_obs_db.orm.file.File`
        File record to associate to
    filename : str
        Name of the file
    path : str
        Path of the file within the storage folder
    fullpath : str
        Full path of the file (no longer used)
    """
    # Instantiating the DiskFile object with a bzip2 filename will trigger
    # creation of the unzipped cache file too.
    diskfile = DiskFile(fileobj, filename, path)
    session.add(diskfile)

    # Instantiate an astrodata object here and pass it in to the things that
    # need it. These are expensive to instantiate each time.
    if diskfile.uncompressed_cache_file:
        fullpath_for_ad = diskfile.uncompressed_cache_file
    else:
        fullpath_for_ad = diskfile.fullpath()

    try:
        diskfile.ad_object = astrodata.open(fullpath_for_ad)
    except Exception as e:
        # Failed to open astrodata object
        print(e)
        session.rollback()
        return

    # For the script, we do not require it to be a calibration per the required tags
    # Check that it has the correct tags to be identified as a retrievable
    # type of calibration (otherwise it will never be retrieved)
    # tags = diskfile.ad_object.tags
    # print(tags)
    # for valid_tags in REQUIRED_TAG_DICT.values():
    #     if tags.issuperset(valid_tags):
    #         break
    # else:
    #     session.rollback()
    #     raise ValueError("Tags do not indicate this is a valid calibration file.")

    # commit DiskFile before we make the header
    session.commit()

    # This will use the diskfile ad_object if it exists, else
    # it will use the DiskFile unzipped cache file if it exists
    header = Header(diskfile)
    session.add(header)
    inst = header.instrument
    session.commit()

    # Add the instrument specific tables
    # These will use the DiskFile unzipped cache file if it exists
    try:
        name, instClass = instrument_table[inst]
        entry = instClass(header, diskfile.ad_object)
        session.add(entry)
        session.commit()
    except KeyError:
        # Unknown instrument. Maybe we should put a message?
        pass

    session.commit()

    return True


dbtools.add_diskfile_entry = modified_add_diskfile_entry


def why_not_matching(filename, processed, cal_type, calibration):
    try:
        filead = astrodata.open(filename)
    except Exception as ex:
        logging.error(f"Unable to open {filename} with DRAGONS")
        exit(1)
    try:
        calad = astrodata.open(calibration)
        if cal_type == "auto":
            processed, cal_type = get_calibration_type(calad)
    except:
        logging.error(f"Unable to open {calibration} with DRAGONS")
        exit(2)
    try:
        mgr = LocalManager(":memory:")
        mgr.init_database(wipe=True)
    except:
        logging.error("Unable to setup in-memory calibration manager")
        exit(3)
    try:
        mgr.ingest_file(calibration)
    except Exception as ingestex:
        logging.error("Unable to ingest calibration file")
        raise
        exit(4)

    rqs = get_cal_requests([filead,], cal_type, procmode=None)
    if not rqs:
        logging.error("Unexpected error creating cal requests")
        exit(5)

    reasons = list()
    for idx in range(len(rqs)):
        rq = rqs[idx]
        descripts = build_descripts(rq)
        types = rq.tags
        cal_obj = get_cal_object(mgr.session, filename=None, header=None,
                                 descriptors=descripts, types=types, procmode=rq.procmode)
        method, args = args_for_cals.get(cal_type, (cal_type, {}))

        # Obtain a list of calibrations and check if we matched
        args["return_query"] = True
        if processed:
            args["processed"] = True

        if not hasattr(cal_obj, method):
            print(f"Instrument {calad.instrument()} has no matching rule for {cal_type}")
        else:
            cals, query_result = getattr(cal_obj, method)(**args)

            for cal in cals:
                if cal.diskfile.filename == basename(calibration):
                    logging.info("Calibration matched")
                    exit(0)

            header = mgr.session.query(Header).first()
            diskfile = mgr.session.query(DiskFile).first()
            instr = mgr.session.query(Gmos).first()
            print('Relevant fields from calibration:\n')
            print('Table     | Key                | Cal Value                      '
                  '| Value                          | Expr')
            print('----------+--------------------+--------------------------------'
                  '+--------------------------------+-------------------')
            debug_parser(query_result, cal_obj, header, diskfile, instr)

    if reasons:
        logging.info(reasons)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        logging.error("Useage: why_not_matching <filename> <cal_type> <calibrationfilename>")
    filename = sys.argv[1]
    cal_type = sys.argv[2]
    if cal_type.startswith('processed_'):
        processed = True
        cal_type = cal_type[10:]
    else:
        processed = False
    calibration = sys.argv[3]

    why_not_matching(filename, processed, cal_type, calibration)

