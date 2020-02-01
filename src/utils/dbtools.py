#
#                                                                        DRAGONS
#
#                                                                     dbtools.py
# ------------------------------------------------------------------------------
import os
from os.path import dirname
from os.path import abspath
from os.path import basename

from sqlalchemy.orm.exc import NoResultFound

import astrodata
import gemini_instruments

from ..fits_storage_config import storage_root

from ..orm.file import File
from ..orm.diskfile import DiskFile
from ..orm.header import Header
from ..orm.gmos import Gmos
from ..orm.gnirs import Gnirs
from ..orm.niri import Niri
from ..orm.nifs import Nifs
from ..orm.michelle import Michelle
from ..orm.f2 import F2
from ..orm.gsaoi import Gsaoi
from ..orm.nici import Nici
from ..orm.gpi import Gpi
from ..orm.ghost import Ghost

# ------------------------------------------------------------------------------
instrument_table = {
    # Instrument: (Name for debugging, Class)
    'F2':       ("F2", F2),
    'GHOST':    ("GHOST", Ghost),
    'GMOS-N':   ("GMOS", Gmos),
    'GMOS-S':   ("GMOS", Gmos),
    'GNIRS':    ("GNIRS", Gnirs),
    'GPI':      ("GPI", Gpi),
    'GSAOI':    ("GSAOI", Gsaoi),
    'michelle': ("MICHELLE", Michelle),
    'NICI':     ("NICI", Nici),
    'NIFS':     ("NIFS", Nifs),
    'NIRI':     ("NIRI", Niri),
    }
# ------------------------------------------------------------------------------

def check_present(session, filename):
    """
    Check to see if the named file is present in the database and
    marked as present in the diskfile table.
    If so, checks to see if it's actually on disk and if not
    marks it as not present in the diskfile table
    """

    # Search for file object
    query = session.query(File).filter(File.name == filename)
    try:
        # Assume that there's a file entry for this one
        fileobj = query.one()
        # OK, is there a diskfile that's present for it
        query = session.query(DiskFile).filter(DiskFile.file_id == fileobj.id).filter(DiskFile.present == True)

        # Assume that there's a diskfile entry for this
        diskfile = query.one()
        # Is the file actually present on disk?
        if not diskfile.exists():
            diskfile.present = False
            session.commit()
    except NoResultFound:
        pass

def need_to_add_diskfile(session, fileobj):
    # See if a diskfile for this file already exists and is present
    query = session.query(DiskFile)\
                .filter(DiskFile.file_id == fileobj.id)\
                .filter(DiskFile.present == True)

    result = False

    try:
        # Assume that the file is there (will raise an exception otherwise)
        diskfile = query.one()
        # Ensure there's only one and get an instance of it

        def need_to_add_diskfile_p(md5):
            # If md5 remains the same, we're good (unless we're forcing it)
            if diskfile.file_md5 == md5:
                # No change
                return False
            else:
                # We could fetch the file and do a local md5 check here if we
                # want. Set the present and canonical flags on the current one
                # to false and create a new entry.
                diskfile.present = False
                diskfile.canonical = False
                session.commit()
                return True

        # Has the file changed since we last ingested it?
        # By default check lastmod time first. There is a subtlety wrt timezones.
        if (diskfile.lastmod.replace(tzinfo=None) != diskfile.get_lastmod()):
            # The flags suggest file modification
            result = need_to_add_diskfile_p(diskfile.get_file_md5())

    except NoResultFound:
        # No not present, insert into diskfile table
        result = True

        # Check to see if there is are older non-present but canonical versions
        # to mark non-canonical.
        olddiskfiles = session.query(DiskFile)\
                            .filter(DiskFile.canonical == True)\
                            .filter(DiskFile.file_id == fileobj.id)\
                            .filter(DiskFile.present == False)

        for olddiskfile in olddiskfiles:
            # The old diskfile id is no longer canonical
            olddiskfile.canonical = False
        session.commit()

    return result

def ingest_file(session, filename, path):
    """
    Ingests a file into the database. If the file isn't known to the database
    at all, all three (file, diskfile, header) table entries are created.
    If the file is already in the database but has been modified, the
    existing diskfile entry is marked as not present and new diskfile
    and header entries are created. If the file is in the database and
    has not been modified since it was last ingested, then this function
    does not modify the database.

    Parameters
    ----------
    filename: <str> 
        Filename of the file to ingest

    path: <str>
        Path to the file to ingest


    Return
    ------
    <bool>,  Success or fail on adding a new diskfile or not.

    """

    # First, sanity check if the file actually exists
    fullpath = os.path.join(storage_root, path, filename)
    exists = os.access(fullpath, os.F_OK | os.R_OK) and os.path.isfile(fullpath)
    if not exists:
        check_present(session, filename)
        return

    try:
        # Assume that there exists a file table entry for this
        trimmed_name = File.trim_name(filename)
        fileobj = session.query(File).filter(File.name == trimmed_name).one()
    except NoResultFound:
        # Make a file instance
        fileobj = File(filename)
        session.add(fileobj)
        session.commit()

    # At this point, 'fileobj' should by a valid DB object.

    if need_to_add_diskfile(session, fileobj):
        return add_diskfile_entry(session, fileobj, filename, path, fullpath)

    return False

def add_diskfile_entry(session, fileobj, filename, path, fullpath):
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

def remove_file(session, path):
    not_found = "Could not find any {} file in the database"
    directory = abspath(dirname(path))
    filename = basename(path)

    objects_to_delete = []
    try:
        file_obj = session.query(File).filter(File.name == filename).one()
        objects_to_delete.append(file_obj)
    except NoResultFound:
        raise IOError(not_found.format(filename))
    else:
        # Look up all diskfile entries related to the target filename,
        # add them to remove list.
        diskfiles = session.query(DiskFile).filter(DiskFile.file_id == file_obj.id).all()
        objects_to_delete.extend(diskfiles)
        # Look up all headers pointing to the selected diskfiles, add them
        headers = []
        for df_obj in diskfiles:
            headers.extend(session.query(Header).filter(Header.diskfile_id == df_obj.id).all())
        objects_to_delete.extend(headers)
        # Finally, look up the instrument-specific tables
        instruments = []
        for hd_obj in headers:
            try:
                name, instClass = instrument_table[hd_obj.instrument]
                instruments.extend(session.query(instClass).filter(instClass.header_id == hd_obj.id).all())
            except KeyError:
                # This instrument has no specific table
                pass
        objects_to_delete.extend(instruments)
        for obj in reversed(objects_to_delete):
            session.delete(obj)
        session.commit()
