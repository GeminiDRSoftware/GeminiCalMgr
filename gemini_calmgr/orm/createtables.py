"""
This module provides various utility functions for create_tables.py
in the Fits Storage System.
"""
import sqlalchemy

from gemini_obs_db import pg_db
from gemini_obs_db.file import File
from gemini_obs_db.diskfile import DiskFile
from gemini_obs_db.header import Header
from gemini_obs_db.gmos import Gmos
from gemini_obs_db.niri import Niri
from gemini_obs_db.gnirs import Gnirs
from gemini_obs_db.nifs import Nifs
from gemini_obs_db.f2 import F2
from gemini_obs_db.ghost import Ghost
from gemini_obs_db.gpi import Gpi
from gemini_obs_db.gsaoi import Gsaoi
from gemini_obs_db.nici import Nici
from gemini_obs_db.michelle import Michelle
from gemini_obs_db.calcache import CalCache

def create_tables(session):
    """
    Creates the database tables and grants the apache user
    SELECT on the appropriate ones
    """
    # Create the tables
    File.metadata.create_all(bind=pg_db)
    DiskFile.metadata.create_all(bind=pg_db)
    Header.metadata.create_all(bind=pg_db)
    Gmos.metadata.create_all(bind=pg_db)
    Niri.metadata.create_all(bind=pg_db)
    Nifs.metadata.create_all(bind=pg_db)
    Gnirs.metadata.create_all(bind=pg_db)
    F2.metadata.create_all(bind=pg_db)
    Ghost.metadata.create_all(bind=pg_db)
    Gpi.metadata.create_all(bind=pg_db)
    Gsaoi.metadata.create_all(bind=pg_db)
    Michelle.metadata.create_all(bind=pg_db)
    Nici.metadata.create_all(bind=pg_db)
    CalCache.metadata.create_all(bind=pg_db)


def drop_tables(session):
    """
    Drops all the database tables. Very unsubtle. Use with caution
    """
    File.metadata.drop_all(bind=pg_db)
