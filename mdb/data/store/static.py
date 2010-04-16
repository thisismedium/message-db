## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""static -- write-once, statically addressed backing store"""

from __future__ import absolute_import
from hashlib import sha1
from md.prelude import *
from md import abc
from .interface import *

__all__ = ('static', )

DEFAULT_CACHE_SIZE = 1000

@abc.implements(Logical)
class static(object):
    """Static storage is content-addressed.  When objects are put into
    the store, they are serialized.  The serialized value is hashed;
    this hash is the address of the object.  This means that the
    objects must be immutable and load/dump must be idempotent."""

    CacheType = dict

    def __init__(self, back, marshall, prefix='', cache=DEFAULT_CACHE_SIZE):
        if isinstance(back, Logical):
            back = back._back
        self._back = back
        self._marshall = marshall
        self._cache = None
        self._cache_size = cache
        self._prefix = prefix

    def __repr__(self):
        name = getattr(self._marshall, '__name__', None) or repr(self._marshall)
        return '%s(%r, %s)' % (type(self).__name__, self._back, name)

    def exists(self):
        return self._back.exists()

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

    def mget(self, addresses):
        need = {}
        for address in addresses:
            try:
                yield (address, self._cache[address])
            except KeyError:
                need[self._key(address)] = address
        for (key, data) in self._back.mget(need):
            address = need[key]
            yield (address, self._load(address, data))

    def add(self, address, value):
        self._store(address, value)

    def madd(self, pairs):
        self._mstore(pairs)

    def put(self, value):
        return self._store(None, value)

    def mput(self, values):
        return self._mstore((None, v) for v in values)

    def _key(self, address):
        return self._prefix + address

    def _load(self, address, value):
        if value is not Undefined:
            if __debug__:
                probe = self._digest(value)
                if probe != address:
                    raise BadObject(
                        "Inconsistent static identity %r, expected %r." % (
                            probe, address
                    ))
            value = self._marshall.loads_binary(value)
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

    def _mstore(self, pairs):
        data = [(v, self._dump(a, v)) for (a, v) in pairs]
        try:
            self._back.madd((self._key(a), d) for (_, (a, d)) in data)
        except NotStored:
            pass
        return ((a, v) for (v, (a, _)) in data)

    def _dump(self, address, value):
        data = self._marshall.dumps_binary(value)
        static = self._digest(data)
        if address and address != static:
            raise NotStored(
                "Inconsistent static identity %r, expected %r for %r." % (
                    address, static, obj
            ))
        return (static, data)

    def _digest(self, data):
        return sha1(data).hexdigest()





