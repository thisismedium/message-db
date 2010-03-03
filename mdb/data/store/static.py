## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""static -- write-only, statically addressed backing store"""

from __future__ import absolute_import
from hashlib import sha1
from .interface import *
from ..prelude import *

DEFAULT_CACHE_SIZE = 1000

class static(object):

    CacheType = dict

    def __init__(self, back, marshall, prefix='', cache=DEFAULT_CACHE_SIZE):
        self._back = back
        self._marshall = marshall
        self._cache = None
        self._cache_size = cache
        self._prefix = prefix

    def __repr__(self):
        name = getattr(self._marshall, '__name__', None) or repr(self._marshall)
        return '%s(%r, %s)' % (type(self.__name__), self._back, name)

    def open(self):
        if self._cache is None:
            self._back.open()
            self._cache = self.CacheType()
        return self

    def close(self):
        if self._cache is not None:
            self._back.close()
            self._cache = None
        return self

    def destroy(self):
        self.close()
        self._back.destroy()

    def get(self, address):
        try:
            return self._cache[address]
        except KeyError:
            return self._load(address, self._back.get(self._key(address)))

    def mget(self, *addresses):
        need = {}
        for address in addresses:
            try:
                yield self._cache[address]
            except KeyError:
                need[self._key(address)] = address
        for (key, data) in self._back.mget(*need.keys()):
            address = need[key]
            yield (address, self._load(address, data))

    def add(self, address, value):
        self._store(address, value)

    def put(self, value):
        return self._store(None, value)

    def mput(self, values):
        return (self._store(None, v) for v in values)

    def _key(self, address):
        return self._prefix + address

    def _load(self, address, value):
        if value is not Undefined:
            value = self._marshall.load(value)
        return self._cached(address, value)

    def _cached(self, address, value):
        if len(self._cache) > self._cache_size:
            self._cache.clear()
        return self._cache.setdefault(address, value)

    def _store(self, address, value):
        try:
            (address, data) = self._dump(address, value)
            self._back.add(self._key(address), data)
        except NotStored:
            ## The value was already stored, but it's identical so
            ## supress any errors.
            pass
        return (address, self._cached(address, value))

    def _dump(self, address, value):
        data = self._marshall.dumps(value)
        static = self._digest(data)
        if address and address != static:
            raise NotStored(
                "Inconsistent static identity %r, expected %r for %r." % (
                    address, static, obj
            ))
        return (static, data)

    def _digest(self, data):
        return sha1(data).hexdigest()




