import re

import sys
import logging
from os.path import basename

from gemini_obs_db.db import sessionfactory
from recipe_system.cal_service.localmanager import extra_descript, args_for_cals
from gemini_calmgr.cal import get_cal_object

from sqlalchemy.sql.elements import BooleanClauseList, BinaryExpression
from gemini_obs_db.orm.header import Header
from gemini_obs_db.orm.diskfile import DiskFile
from gemini_obs_db.orm.gmos import Gmos
from gemini_obs_db.orm.niri import Niri


__all__ = ["get_status", "get_calibration_type"]


def _check_equals_true(val, calval):
    if calval is True:
        return "pass"
    else:
        return "fail"


def _check_equals_false(val, calval):
    if calval is False:
        return "pass"
    else:
        return "fail"


def _check_equals(val, calval):
    if calval == val:
        return "pass"
    else:
        return "fail"


def _check_not_equals(val, calval):
    if calval != val:
        return "pass"
    else:
        return "fail"


def _check_greater_than(val, calval):
    if calval is not None and val < calval:
        return "pass"
    else:
        return "fail"


def _check_less_than(val, calval):
    if calval is not None and val > calval:
        return "pass"
    else:
        return "fail"


def _check_greater_than_or_equal(val, calval):
    if calval is not None and val <= calval:
        return "pass"
    else:
        return "fail"


def _check_less_than_or_equal(val, calval):
    if calval is not None and val >= calval:
        return "pass"
    else:
        return "fail"


def _check_like(val, calval):
    if len(val) > 2 and '%' in val[1:-1]:
        return "unknown"
    if val is None:
        return "fail"
    if val == '':
        if calval == '':
            return "pass"
        else:
            return "Fail"
    if val == '%' or val == '%%':
        return "pass"
    if val.startswith('%') and val.endswith('%'):
        if calval is not None and val[1:-1] in calval:
            return "pass"
        else:
            return "fail"
    elif val.startswith('%'):
        if calval is not None and calval.endswith(val[1:]):
            return "pass"
        else:
            return "fail"
    elif val.endswith('%'):
        if calval is not None and calval.startswith(val[:-1]):
            return "pass"
        else:
            return "fail"
    if val == calval:
        return "pass"
    else:
        return "fail"


_re_equals_true = re.compile(r'\w+ = true')
_re_equals_false = re.compile(r'\w+ = false')
_re_equals = re.compile(r'\w+ = :\w+')
_re_not_equals = re.compile(r'\w+ != :\w+')
_re_greater_than = re.compile(r'\w+ > :\w+')
_re_less_than = re.compile(r'\w+ < :\w+')
_re_greater_than_or_equal = re.compile(r'\w+ >= :\w+')
_re_less_than_or_equal = re.compile(r'\w+ <= :\w+')
_re_like = re.compile(r'\w+ LIKE :\w+')

_checks = [
    (_re_equals_false, _check_equals_false),
    (_re_equals_true, _check_equals_true),
    (_re_equals, _check_equals),
    (_re_not_equals, _check_not_equals),
    (_re_greater_than, _check_greater_than),
    (_re_less_than, _check_less_than),
    (_re_greater_than_or_equal, _check_greater_than_or_equal),
    (_re_less_than_or_equal, _check_less_than_or_equal),
    (_re_like, _check_like),
]


def get_status(val, calval, expr):
    for check in _checks:
        if check[0].search(expr):
            return check[1](val, calval)
    return "unknown"


def get_calibration_type(obj):
    if isinstance(obj, Header):
        observation_type = obj.observation_type
        types = obj.types
    else:
        observation_type = obj.observation_type()
        types = obj.tags

    def add_processed(retval, types):
        if 'PROCESSED' in types:
            return True, retval
        return False, retval

    if observation_type == 'FLAT':
        return add_processed('flat', types)
    if observation_type == 'ARC':
        return add_processed('arc', types)
    if observation_type == 'BIAS':
        return add_processed('bias', types)
    if observation_type == 'DARK':
        return add_processed('dark', types)
    if observation_type == 'STANDARD':
        return add_processed('standard', types)
    if 'SLITILLUM' in types:
        return add_processed('slitillum', types)
    return None
