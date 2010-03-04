## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""os -- operating system utilities"""

from __future__ import absolute_import
import os, errno, contextlib as ctx, tempfile

__all__ = (
    'errno',
    'exists', 'join', 'dirname', 'basename',
    'mkstemp', 'mkdtemp', 'unlink',
    'makedirs', 'mkdir',
    'contents', 'load', 'atomic', 'put', 'dump', 'delete'
)


### Path

exists = os.path.exists
join = os.path.join
dirname = os.path.dirname
basename = os.path.basename
mkstemp = tempfile.mkstemp
mkdtemp = tempfile.mkdtemp
unlink = os.unlink


### Folders

def makedirs(path, *mode):
    """Recursively make directories until the directory path exists
    (like unix mkdir -p path), but don't fail if the leaf directory
    exists."""

    try:
        os.makedirs(path, *mode)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            raise

def mkdir(path, *mode):
    """Create the directory path with mode, but don't fail if it
    already exists."""

    try:
        os.mkdir(path, *mode)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            raise


### Files

def load(path, load, *default):
    """Open a file, passing the port to load().  If the file does not
    exist, return the default value."""

    try:
        with ctx.closing(open(path, 'r')) as port:
            return load(port)
    except IOError as exc:
        if exc.errno == errno.ENOENT and default:
            return default[0]
        else:
            raise exc

def contents(path, *default):
    """Return the entire contents of a path.  If the file does not
    exists, return the default value."""

    return load(path, read, *default)

@ctx.contextmanager
def atomic(path):
    """Yields a file object.  Data written to this object will
    appear at path if the context is successfully exited."""

    (fd, temp_path) = mkstemp('.new', 'atomic-', dirname(path))
    port = os.fdopen(fd, 'w')
    try:
        yield port
    except:
        unlink(temp_path)
        raise
    finally:
        port.close()
    os.rename(temp_path, path)

def put(path, data):
    """Atomically overwrite the file at path with data."""

    return dump(path, write, data)

def dump(path, dump, data):
    """Atomically write the contents of dump(data, port) to path."""

    with atomic(path) as port:
        dump(data, port)

def delete(path):
    """An unlink that suppresses errors about path not existing.
    Return True if the file was unlinked; False if it didn't exist."""

    try:
        unlink(path)
        return True
    except OSError as exc:
        if exc.errno != errno.ENOENT:
            raise
    return False
