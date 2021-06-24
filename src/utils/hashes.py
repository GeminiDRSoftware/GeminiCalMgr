"""
This is the hashes module. It provides a convenience interface to hashing.
Currently, the only hash function we use is md5sum

"""
import hashlib
import bz2


def md5sum_size_fp(fobj):
    """
    Generates the md5sum and size of the data returned by the file-like object fobj, returns
    a tuple containing the hex string md5 and the size in bytes.
    f must be open. It will not be closed. We will read from it until we encounter EOF.
    No seeks will be done, fobj will be left at eof

    Parameters
    ----------
    fobj : file-like object
        File to perform checksum/sizing on

    Returns
    -------
    str, int : md5 checksum and file size
    """
    # This is the block size by which we read chunks from the file, in bytes
    block = 1000000 # 1 MB

    hashobj = hashlib.md5()

    size = 0

    while True:
        data = fobj.read(block)
        if not data:
            break
        size += len(data)
        hashobj.update(data)

    return (hashobj.hexdigest(), size)


def md5sum(filename):
    """
    Generates the md5sum of the data in filename, returns the hex string.

    Parameters
    ----------
    filename : str
        File name of an uncompressed file

    Returns
    -------
    str, int : md5 sum and size of the data
    """

    with open(filename, 'rb') as filep:
        (md5, size) = md5sum_size_fp(filep)
        return md5


def md5sum_size_bz2(filename):
    """
    Generates the md5sum and size of the uncompressed data in a bzip2 file.

    Parameters
    ----------
    filename : str
        Filename of a `.bz2` file

    Returns
    -------
    str, int : md5 sum and size of the uncompressed data
    """

    with bz2.BZ2File(filename, 'rb') as filep:
        return md5sum_size_fp(filep)
