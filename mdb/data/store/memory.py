## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""memory -- in-memory backing store"""

from __future__ import absolute_import
from .interface import *
from ..prelude import *

__all__ = ('memory', )

class memory(object):
    """A "backing store" that keeps data in memory."""

    DataType = dict
    CasType = dict

    def __init__(self):
        self._data = None
        self._cas = None

    def __repr__(self):
        return '%s()' % type(self).__name__

    def open(self):
        if self._data is None:
            self._data = self.DataType()
            self._cas = self.CasType()
        return self

    def close(self):
        if self._data is not None:
            self._data = None
            self._cas = None
        return self

    def destroy(self):
        return self.close()

    def get(self, key):
        return self._data.get(key, Undefined)

    def mget(self, keys):
        return ((k, self._data.get(k, Undefined)) for k in keys)

    def gets(self, key):
        value = self._data.get(key, Undefined)
        token = None if value is Undefined else self._cas.setdefault(key, 0)
        return (value, token)

    def set(self, key, value):
        self._data[key] = value
        if key in self._cas:
            self._cas[key] += 1

    def mset(self, pairs):
        for (key, value) in pairs:
            self.set(key, value)

    def add(self, key, value):
        if key in self._data:
            raise NotStored(key)
        self.set(key, value)

    def madd(self, pairs):
        errors = set()
        for (key, value) in pairs:
            if key in self._data:
                errors.add(key)
                continue
            self.set(key, value)
        if errors:
            raise NotStored(errors)

    def replace(self, key, value):
        if key not in self._data:
            raise NotStored(key)
        self.set(key, value)

    def mreplace(self, pairs):
        errors = set()
        for (key, value) in pairs:
            if key not in self._data:
                errors.add(key)
                continue
            self.set(key, value)
        if errors:
            raise NotStored(errors)

    def cas(self, key, value, token):
        if key not in self._cas or self._cas[key] != token:
            raise NotStored(key)
        self.set(key, value)

    def delete(self, key):
        if key not in self._data:
            raise NotFound(key)
        del self._data[key]

    def mdelete(self, keys):
        errors = set()
        for key in keys:
            if key not in self._data:
                errors.add(key)
                continue
            del self._data[key]
        if errors:
            raise NotFound(errors)




