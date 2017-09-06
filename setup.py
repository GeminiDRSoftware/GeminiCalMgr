#!/usr/bin/env python

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand
import subprocess
import sys
import os
from src import __version__

setup(
    name='GeminiCalMgr',
    version=__version__,
    # The following is need only if publishing this under PyPI or similar
    #description = '...',
    #author = 'Paul Hirst',
    #author_email = 'phirst@gemini.edu',
    license = 'License :: OSI Approved :: BSD License',
    packages = ['gemini_calmgr',
                'gemini_calmgr.orm',
                'gemini_calmgr.cal',
                'gemini_calmgr.utils'],
    package_dir = {'gemini_calmgr': 'src'},
#    install_requires = ['sqlalchemy >= 0.9.9', 'pyfits']
)
