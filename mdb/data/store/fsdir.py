## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""fsdir -- store data in individual files"""

from __future__ import absolute_import
import threading, hashlib, zlib, gzip, random
from .. import os
from ..prelude import *
from .interface import *

__all__ = ('fsdir', )

class fsdir(object):
    """A backing store that uses lots of little files in a
    directory."""

    CASType = dict

    def __init__(self, path):
        self._path = path
        self._lock = threading.RLock()
        self._cas = None

    def __repr__(self):
        return '%s(%r)' % (type(self).__name__, self._path)

    def open(self):
        if self._cas is None:
            if not os.exists(self._path):
                os.mkdirs(self._path)
            self._cas = self.CASType()
        return self

    def close(self):
        if self._cas is not None:
            self._cas = None
        return self

    def destroy(self):
        self.close()
        if os.exists(self._path):
            import shutil
            shutil.rmtree(self._path)

    def get(self, key):
        return self._get(key)

    def mget(self, keys):
        return ((k, self._get(k)) for k in keys)

    def gets(self, key):
        return self._make_cas_token(key, self._get(key))

    def set(self, key, value):
        self._set(key, value)

    def mset(self, pairs):
        for (key, value) in pairs:
            self._set(key, value)

    def add(self, key, value):
        with self._lock:
            if self._exists(key):
                raise NotStored(key)
            self._set(key, value)

    def madd(self, pairs):
        errors = set()
        with self._lock:
            for (key, value) in pairs:
                if self._exists(key):
                    errors.add(key)
                    continue
                self._set(key, value)
        if errors:
            raise NotStored(errors)

    def replace(self, key, value):
        with self._lock:
            if not self._exists(key):
                raise NotStored(key)
            self._set(key, value)

    def mreplace(self, pairs):
        errors = set()
        with self._lock:
            for (key, value) in pairs:
                if not self._exists(key):
                    errors.add(key)
                    continue
            self._set(key, value)
        if errors:
            raise NotStored(errors)

    def cas(self, key, value, token):
        with self._lock:
            if not self._valid_cas_token(key, token):
                raise NotStored(key)
            self._set(key, value)

    def delete(self, key):
        with self._lock:
            if not self._delete(key):
                raise NotFound(key)


    def mdelete(self, keys):
        errors = set()
        with self._lock(key):
            for key in keys:
                if self._delete(key):
                    errors.add(key)
        if errors:
            raise NotFound(errors)

    def _exists(self, key):
        return os.exists(self._key_path(key))

    def _get(self, key):
        return self._read(self._key_path(key))

    def _set(self, key, value):
        path = self._key_path(key)
        os.mkdir(os.dirname(path))
        os.dump(path, self._gzwrite, value)
        if key in self._cas:
            self._cas[key] += random.getrandbits(16)

    def _delete(self, key):
        return os.delete(self._key_path(key))

    def _make_cas_token(self, key, value):
        if value is Undefined:
            return (value, None)
        with self._lock:
            try:
                token = self._cas[key]
            except KeyError:
                token = self._cas[key] = random.getrandbits(16)
        return (value, token)

    def _valid_cas_token(self, key, token):
        try:
            return token == self._cas[key]
        except KeyError:
            return False

    def _key_path(self, key, digest=None):
        digest = digest or self._address(key)
        return os.join(self._path, digest[0:2], digest[2:])

    def _address(self, key):
        return hashlib.sha1(key).hexdigest()

    def _read(self, path):
        return os.load(path, self._gzread, Undefined)

    def _gzread(self, port):
        return gzip.GzipFile(mode='rb', fileobj=port).read()

    def _gzwrite(self, data, port):
        with closing(gzip.GzipFile(mode='wb', fileobj=port)) as gz:
            gz.write(data)

