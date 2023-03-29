#!/usr/bin/env python

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand
import subprocess
import sys
import os

# Get __version__ without having to import run-time deps at build time:
with open(os.path.join('gemini_calmgr', 'version.py')) as f:
    exec(f.read())

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
    package_dir = {'gemini_calmgr': 'gemini_calmgr'},
    install_requires = ['sqlalchemy>=1.3,<2.0.0a0'],
    scripts = ['gemini_calmgr/scripts/calcheck']
)
