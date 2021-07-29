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
    if calval != val:
        return "pass"
    else:
        return "fail"


def _check_less_than(val, calval):
    if calval != val:
        return "pass"
    else:
        return "fail"


_re_equals_true = re.compile(r'\w+ = true')
_re_equals_false = re.compile(r'\w+ = false')
_re_equals = re.compile(r'\w+ = :\w+')
_re_not_equals = re.compile(r'\w+ != :\w+')
_re_greater_than = re.compile(r'\w+ > :\w+')
_re_less_than = re.compile(r'\w+ < :\w+')

_checks = [
    (_re_equals_false, _check_equals_false),
    (_re_equals_true, _check_equals_true),
    (_re_equals, _check_equals),
    (_re_not_equals, _check_not_equals),
    (_re_greater_than, _check_greater_than),
    (_re_less_than, _check_less_than)
]


def get_status(val, calval, expr):
    for check in _checks:
        if check[0].search(expr):
            return check[1](val, calval)
    return "unknown"


def get_calibration_type(obj):
    if obj.observation_type == 'FLAT':
        return 'flat'
    if obj.observation_type == 'ARC':
        return 'arc'
    if obj.observation_type == 'BIAS':
        return 'bias'
    if obj.observation_type == 'DARK':
        return 'dark'
    if obj.observation_type == 'STANDARD':
        return 'standard'
    if 'SLITILLUM' in obj.types:
        return 'slitillum'
    return None
