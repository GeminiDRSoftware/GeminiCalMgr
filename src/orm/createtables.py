"""
This module provides various utility functions for create_tables.py
in the Fits Storage System.
"""
import sqlalchemy

from . import pg_db
from .file import File
from .diskfile import DiskFile
from .header import Header
from .gmos import Gmos
from .niri import Niri
from .gnirs import Gnirs
from .nifs import Nifs
from .f2 import F2
from .gpi import Gpi
from .gsaoi import Gsaoi
from .nici import Nici
from .michelle import Michelle
from .calcache import CalCache

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
