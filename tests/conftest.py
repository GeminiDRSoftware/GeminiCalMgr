import pytest
import datetime as dt
import os

from bz2 import BZ2File

from gemini_obs_db.db import sessionfactory

now = dt.datetime.now()

# RESTORE='/usr/bin/pg_restore'
RESTORE='pg_restore'
DUMP_FILE='fitsdata_test.pg_dump'
full_path_dump=os.path.join(os.path.dirname(__file__), DUMP_FILE)

# Monkeypatch the database name and a few other things before doing anything...
# We'll use the current date and time to generate new databases. We don't expect
# tests to last under a second, so this should be safe... (otherwise, something is
# really, really wrong)


import gemini_obs_db.db_config as fsc

_create_test_db = True
dbname = 'test_{0}_{1}'.format('geminicalmgrtests', now.strftime('%Y%m%d%H%M%S'))
fsc.database_url = 'sqlite:///' + dbname

TEST_IMAGE_PATH=os.getenv('TEST_IMAGE_PATH', '/mnt/hahalua')
TEST_IMAGE_CACHE=os.getenv('TEST_IMAGE_CACHE', os.path.expanduser('~/tmp/cache'))


# ORM imports must occur AFTER we have patched the db config settings
import sqlalchemy
from gemini_obs_db import orm
from gemini_obs_db.utils import createtables
from gemini_obs_db.utils.createtables import create_tables


class DatabaseCreation(object):
    def __init__(self):
        self.conn = None

    def create_db(self, dbname):
        if self.conn is None:
            if _create_test_db:
                eng = sqlalchemy.create_engine(fsc.database_url) # 'postgresql://%s/postgres' % fsc.pytest_database_server)
                conn = eng.connect()
                # conn.execute('COMMIT') # Make sure we're not inside a transaction
                #                        # as CREATE DATABASE can't run inside one
                # conn.execute('CREATE DATABASE ' + dbname)

                # Trying to fix test_wsgi.py
                conn.close()
            eng = sqlalchemy.create_engine(fsc.database_url) # 'postgresql://%s/%s' % (fsc.pytest_database_server, dbname))
            conn = eng.connect()
            # end of my hackery

            self.conn = conn
        else:
            conn = self.conn

        s = sessionfactory()

        return conn, s

    def drop_db(self, dbname):
        return
        # if self.conn:
        #     if not _create_test_db:
        #         return
        #     conn = self.conn
        #     #O orm.pg_db.dispose()
        #
        #     conn.execute('COMMIT')
        #     # Kill any other pending connection. Shouldn't be needed, but...
        #     #conn.execute("SELECT pg_terminate_backend(procpid) FROM pg_stat_activity WHERE datname = '%s' AND procpid <> pg_backend_pid()" % (fsc.fits_dbname,))
        #     #conn.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '%s' AND pid <> pg_backend_pid()" % (fsc.fits_dbname,))
        #     # try:
        #     #     conn.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '%s'" % (fsc.fits_dbname,))
        #     # except:
        #     #     pass
        #     conn.close()
        #     self.conn = None
        #
        #     orm.pg_db.dispose()
        #
        #     eng = sqlalchemy.create_engine(fsc.database_url) # 'postgresql://%s/postgres' % fsc.pytest_database_server)
        #     conn = eng.connect()
        #     conn.execute('COMMIT') # Make sure we're not inside a transaction
        #                            # as CREATE DATABASE can't run inside one
        #
        #     # conn.execute('DROP DATABASE ' + dbname)
        #     conn.close()
        #
        #     #O moved down here from above

dbcreation = DatabaseCreation()

@pytest.yield_fixture(scope='session')
def session(request):
    'Creates a fresh database, with empty tables'
    # dbname = fsc.fits_dbname
    conn, s = dbcreation.create_db('')
    createtables.create_tables(s)

    yield s

    s.close()
    dbcreation.drop_db("")

@pytest.yield_fixture(scope='session')
def min_session(request):
    'Creates a fresh database, with empty tables'
    dbname = fsc.fits_dbname
    conn, s = dbcreation.create_db(dbname)
    #call([RESTORE, '-d', dbname, DUMP_FILE])
    if fsc.pytest_database_server != '':
        # Need to strip off
        hostname = fsc.pytest_database_server
        if '@' in hostname:
            hostname = hostname[hostname.index('@')+1:]
        # Note, password comes from env var PGPASSWORD, which is set in Dockerfile (or add it to your env to suit your needs)
        #call([RESTORE, '-h', hostname, '--username', 'fitsdata', '-d', dbname, full_path_dump])
    else:
        #call([RESTORE, '-d', dbname, full_path_dump])
        pass

    if _create_test_db:
        # instead, let's create tables
        create_tables(s)

    # users = s.query(User).filter(User.username == 'user1').all()
    # if len(users) > 1:
    #     # bug, need to clear
    #     for user in users:
    #         s.delete(user)
    #     s.commit()
    #     users = None
    # elif len(users) == 1:
    #     user = users[0]
    # if not users:
    #     user = User(username='user1')
    #     user.email = 'unknown1@gemini.edu'
    #     user.fullname = 'User 1'
    #     user.gemini_staff = True
    #     user.superuser = True
    #     user.cookie = 'oYgq13TfqRt+x1xrNnGNMZJIMZj+p1GyIWV/Ebm3/BsD05dCf5KKQOvtrGim9YG5XgsVCn8sDSBeaBHuh1I6A9st5CLr5auN9tYOlLzCFo15i64RUVfByFmqaxgJuHHAim4HBKdOlq/Mo4YHhMNAQKgUJnkEj27xoL6+YXSsNfmEDzB/PmmNzc+jz3sMCYuxt/NVftEo0FB1xk3xvCj5kkkE9DjRiSibtaD5EIluv2nYkmaSIxThfqpilj9UJhg4uc3pLN2I+R15IWa3h8HskqyjBL3tiq0paVWDv8BoOgeBwK24Igw0Vnn8vQQ8Ys6a4DZ2c84YaIjXEaL26VSw5A=='
    #     s.add(user)
    #     s.commit()
    #
    # user_programs = s.query(UserProgram).filter(UserProgram.user_id == user.id).all()
    # if not user_programs:
    #     # give this user a program
    #     user_program = UserProgram(user_id=user.id, program_id="SPARKYTHEGECKO")
    #     s.add(user_program)
    #     s.commit()
    #
    # users = s.query(User).filter(User.username == 'user2').all()
    # if len(users) > 1:
    #     # bug, need to clear
    #     for user in users:
    #         s.delete(user)
    #     s.commit()
    #     users = None
    # if not users:
    #     user = User(username='user2')
    #     user.email = 'unknown2@gemini.edu'
    #     user.fullname = 'User 2'
    #     user.gemini_staff = True
    #     user.superuser = True
    #     user.cookie = 'Jaab1A1SVbDOCjGpYOPsDElBorBt58JXWJMRcg2EYsDKd9PA8W8W5SCn/R6baUFXVGHbE2QCHHlbHG+yfwoTGQZDTQgqD4X1IA+WzlCSLBQnoej+1t8iK/tYqTyHnHr2FVJ3U+ijwFPmxloplcqa/fWO17SFLQB5GiLrPgsNouKgj8M9vAK/IyzpYY2nSdXTo038k2S/OWm8JDMPr6Qp+FIByfvP4cEMdL3nHcCu6PhQsKtc+fbSt14Ie4UjJ6uu1rqzJc1iBThD3PwnYyRjuLtE3eiO7otThhwhbIf4gZrdrTvByROrtr5l2G45GBigFqxc+0TatfiPjszTaQiK/A=='
    #     s.add(user)
    #     s.commit()

    yield s

    s.close()
    dbcreation.drop_db(dbname)

@pytest.yield_fixture(scope='session')
def rollback(request, session):
    '''This will be used from most other tests, to make sure that a
       database failure won't interfere with other functions, and that
       unintended changes don't get passed to other tests'''
    yield session
    session.rollback()

@pytest.yield_fixture(scope='session')
def min_rollback(request, min_session):
    '''This will be used from most other tests, to make sure that a
       database failure won't interfere with other functions, and that
       unintended changes don't get passed to other tests'''
    yield min_session
    min_session.rollback()

@pytest.yield_fixture(scope='session')
def testfile_path(request):
    added_to_cache = []
    def return_image_path(filename):
        if filename.endswith('.bz2'):
            nobz2 = filename[:-4]
        else:
            nobz2 = filename
        cached_path = os.path.join(TEST_IMAGE_CACHE, nobz2)
        if not os.path.exists(cached_path):
            orig_path = os.path.join(TEST_IMAGE_PATH, filename)
            if not orig_path.endswith('.bz2'):
                os.symlink(orig_path, cached_path)
            else:
                with BZ2File(orig_path) as org, open(cached_path, 'wb') as dst:
                    while True:
                        data = org.read(8192)
                        if not data:
                            break
                        dst.write(data)

            added_to_cache.append(cached_path)

        return cached_path

    yield return_image_path

    for cached in added_to_cache:
        os.unlink(cached)

# Slow test handling
#
# This section sets up behavior to avoid slow tests by default.

def pytest_addoption(parser):
    parser.addoption(
        "--runslow", action="store_true", default=False, help="run slow tests"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        skip_slow = pytest.mark.skip(reason="need --runslow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)

# End Slow test handling