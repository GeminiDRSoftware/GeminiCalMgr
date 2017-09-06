from os.path import join as opjoin

using_apache = False
use_as_archive = False
z_staging_area = ''
storage_root = '/tmp'
fits_dbname = 'fitsdata.fdb'
db_path = opjoin(storage_root, fits_dbname)
fits_database = 'sqlite:///' + db_path
