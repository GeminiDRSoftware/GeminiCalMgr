#! python3

import sys
import logging
from os.path import basename

from gemini_calmgr.utils.dbtools import REQUIRED_TAG_DICT
from recipe_system.cal_service.localmanager import extra_descript, args_for_cals, LocalManager
from recipe_system.cal_service.calrequestlib import get_cal_requests
from gemini_calmgr.cal import get_cal_object

from sqlalchemy.sql.elements import BooleanClauseList, BinaryExpression, Grouping

import astrodata
import gemini_instruments

from gemini_obs_db.orm.header import Header
from gemini_obs_db.orm.diskfile import DiskFile
from gemini_obs_db.orm.gmos import Gmos
from gemini_obs_db.orm.niri import Niri
from gemini_obs_db.orm.f2 import F2
from gemini_obs_db.orm.nifs import Nifs
from gemini_obs_db.orm.gnirs import Gnirs
from gemini_obs_db.orm.ghost import Ghost
from gemini_obs_db.orm.nici import Nici
from gemini_obs_db.orm.michelle import Michelle
from gemini_obs_db.orm.gsaoi import Gsaoi
from gemini_obs_db.orm.gpi import Gpi

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
        elif table.name == 'diskfile':
            show_line(table.name, key, getattr(diskfile, key), val, expr)
        else:
            show_line(table.name, key, getattr(instr, key), val, expr)


def debug_boolean_clause_list(clause, cal_obj, header, diskfile, instr, is_or=False):
    for clause in clause.clauses:
        debug_dispatch(clause, cal_obj, header, diskfile, instr)
        # yield x


def debug_dispatch(clause, cal_obj, header, diskfile, instr):
    if isinstance(clause, BooleanClauseList):
        debug_boolean_clause_list(clause, cal_obj, header, diskfile, instr)
    elif isinstance(clause, BinaryExpression):
        debug_binary_expression(clause, cal_obj, header, diskfile, instr)
    elif isinstance(clause, Grouping):
        if 'OR' in str(clause) and isinstance(clause.element, BooleanClauseList):
            # ew, need to debug an OR
            print("\u001b[33mOR Expression:\u001b[37m")
            debug_boolean_clause_list(clause.element, cal_obj, header, diskfile, instr, is_or=True)
            print("\u001b[33mOR Expression Complete\u001b[37m")
        else:
            print("Unsupported query element: %s" % str(clause))


def debug_parser(query, cal_obj, header, diskfile, instr):
    for clause in query.query.whereclause.clauses:
        debug_dispatch(clause, cal_obj, header, diskfile, instr)


def build_descripts(rq):
    descripts = rq.descriptors
    for (type_, desc) in list(extra_descript.items()):
        descripts[desc] = type_ in rq.tags
    return descripts


# TODO maybe make the dbtools behavior configurable to allow this without duplication
# This is a hack that convinces the cal code to allow ingests of unprocessed calibrations for the DB
# useful for testing matches
REQUIRED_TAG_DICT["__dummy__"] = []


def why_not_matching(filename, processed, cal_type, calibration):
    filead = astrodata.open(filename)
    calad = astrodata.open(calibration)

    if filead.ut_datetime() is None:
        print("NO UT in file!")
        exit(1)
    if calad.ut_datetime() is None:
        print("NO UT in cal!")
        exit(2)

    if cal_type == "auto":
        processed, cal_type = get_calibration_type(calad)
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
        if processed and "processed" not in method:
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

            if calad.instrument().lower().startswith("gmos"):
                instr = mgr.session.query(Gmos).first()
            elif calad.instrument().lower() == "f2":
                instr = mgr.session.query(F2).first()
            elif calad.instrument().lower() == "nifs":
                instr = mgr.session.query(Nifs).first()
            elif calad.instrument().lower() == "niri":
                instr = mgr.session.query(Niri).first()
            elif calad.instrument().lower() == "gnirs":
                instr = mgr.session.query(Gnirs).first()
            elif calad.instrument().lower() == "ghost":
                instr = mgr.session.query(Ghost).first()
            elif calad.instrument().lower() == "nici":
                instr = mgr.session.query(Nici).first()
            elif calad.instrument().lower() == "michelle":
                instr = mgr.session.query(Michelle).first()
            elif calad.instrument().lower() == "gsaoi":
                instr = mgr.session.query(Gsaoi).first()
            elif calad.instrument().lower() == "gpi":
                instr = mgr.session.query(Gpi).first()

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
        logging.error("Useage: calcheck <filename> <cal_type> <calibrationfilename>")
    filename = sys.argv[1]
    cal_type = sys.argv[2]
    if cal_type.startswith('processed_'):
        processed = True
        # cal_type = cal_type[10:]
    else:
        processed = False
    calibration = sys.argv[3]

    why_not_matching(filename, processed, cal_type, calibration)

