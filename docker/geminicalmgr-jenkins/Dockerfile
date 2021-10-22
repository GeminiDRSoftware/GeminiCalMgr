FROM centos:centos8.1.1911
LABEL maintainer="oliver.oberdorf@noirlab.edu"

# TODO base this off the FitsStorageDB Dockerfile?

# ENABLE EPEL
RUN rm -r /var/cache/dnf
RUN dnf -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm

# INSTALL HTTPD AND POSTGRESQL
RUN yum -y install openssl \
                   postgresql \
                   postgresql-devel \
                   epel-release \
                   gcc \
                   gcc-c++ \
                   gcc-gfortran \
                   cfitsio-devel \
                   git \
                   libjpeg-devel \
                   python3-pillow \
                   redhat-rpm-config \
                   python3-devel \
                   bzip2 \
                   wget

# WOW, Docker sucks.  You can't modify the hosts file except like this, just in time to run a command.  Because magical.  Or, magically bad.
# IT Workaround - we can't resolve this host within Gemini network
RUN echo "140.211.169.196 mirrors.fedoraproject.org" >> /etc/hosts && yum -y --nogpgcheck install python3-pip

# Fix Centos 8 broken Locale
RUN yum -y --nogpgcheck install glibc-langpack-en glibc-common

# SETUP PYTHON PACKAGES - MOVE THESE TO REQUIREMENTS FILE
RUN python3 -m pip install -U pip
RUN pip3 install psycopg2-binary && \
    pip3 install sqlalchemy && \
    pip3 install pyyaml && \
    pip3 install jinja2 && \
    pip3 install pyfits && \
    pip3 install dateutils && \
    pip3 install requests && \
    pip3 install matplotlib && \
    pip3 install scipy && \
    pip3 install pandas && \
    pip3 install cython && \
    pip3 install astropy && \
    pip3 install future && \
    pip3 install psutil && \
    pip3 install boto3 && \
    pip3 install pytest && \
    pip3 install coverage && \
    pip3 install mysql-connector && \
    pip3 install simplejson && \
    pip3 install suds-jurko && \
    pip3 install ephem && \
    pip3 install sphinx && \
    pip3 install requests-toolbelt && \
    pip3 install packaging && \
    pip3 install smart-open

# Needed for imexam, which is needed for DRAGONS
RUN pip3 install photutils

RUN yum -y install make
RUN pip3 install imexam

# DRAGONS now also needs specutils
RUN pip3 install specutils


# CREATE FITSDATA USER AND GROUP
RUN /usr/sbin/groupadd -g 5179 fitsdata
RUN /usr/sbin/useradd -c 'FITS data' -u 5179 -g 5179 fitsdata

RUN echo "geminidata:x:502:fitsdata" >> /etc/group

# DATA FOLDERS
RUN mkdir -p /data/logs && \
    chown fitsdata /data/logs && \
    chmod oug+rwx /data/logs && \
    mkdir -p /data/backups && \
    chown fitsdata /data/backups && \
    mkdir -p /data/upload_staging && \
    chown fitsdata /data/upload_staging && \
    chmod oug+rwx /data/upload_staging && \
    mkdir -p /data/z_staging && \
    chown fitsdata /data/z_staging && \
    mkdir -p /data/s3_staging && \
    chown fitsdata /data/s3_staging && \
    chmod oug+rwx /data/s3_staging

# BECAUSE SPECUTILS (SIGH)
RUN mkdir -p /usr/share/httpd/.specutils && \
    chmod oug+rwx /usr/share/httpd/.specutils

# COPY STUFFS
ENV PYTHONPATH /opt/DRAGONS:/opt/FitsStorageDB:/opt/GeminiCalMgr
RUN cd /opt && git clone --branch master https://github.com/GeminiDRSoftware/DRAGONS.git

# Install the configuration file for the Jenkins FitsStore containers
#COPY FitsStorageConfig/configs/jenkins_fitsstorage.conf /etc/fitsstorage.conf

COPY FitsStorageDB /opt/FitsStorageDB
COPY GeminiCalMgr /opt/GeminiCalMgr
WORKDIR /opt/GeminiCalMgr

#RUN yum -y install libnsl
#RUN ln -s /usr/lib64/libnsl.so.2 /usr/lib64/libnsl.so
#COPY FitsStorage/fitsverify /opt/fitsverify
#RUN cd /opt/fitsverify && gcc -o fitsverify ftverify.c fvrf_data.c fvrf_file.c fvrf_head.c \
#    fvrf_key.c fvrf_misc.c -DSTANDALONE -L. -L/usr/lib64 -lcfitsio -lm -lnsl \
#    -I /usr/include/cfitsio -L/usr/lib64/

# COPY FitsStorage/fitsverify /opt/fitsverify
COPY FitsStorageDB/docker/wait-for-postgres.sh /
RUN chmod a+x /wait-for-postgres.sh

# CYTHON STEP FOR DRAGONS
RUN cd /opt/DRAGONS/gempy/library/ && cythonize -i cyclip.pyx

# For specutils, wow this is dumb
RUN mkdir -p /.specutils && \
    chmod oug+rwx /.specutils

# Setup env
ENV GEMINI_OBS_DB_URL postgresql://fitsdata:fitsdata@geminicaldb-jenkins/fitsdata
ENV PGPASSWORD fitsdata
ENV STORAGE_ROOT /tmp/jenkins_pytest/dataflow
ENV TEST_IMAGE_PATH /tmp/archive_test_images
ENV TEST_IMAGE_CACHE /tmp/cached_archive_test_images
ENV CREATE_TEST_DB False

CMD ["bash"]
