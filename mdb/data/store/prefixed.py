## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""prefixed -- prefix keys for a real backing store"""

from __future__ import absolute_import
from md import abc
from .interface import *
from ..prelude import *

__all__ = ('prefixed', )

@abc.implements(Prefixed)
class prefixed(object):
    """A "backing store" that manages adding a prefix to any keys
    keys.  This can be used to logically partition the keyspace of a
    real backing store."""

    def __init__(self, back, prefix):
        if isinstance(back, Prefixed):
            prefix = back._prefix + prefix
            back = back._back
        self._back = back
        self._prefix = prefix

    def __repr__(self):
        return '%s(%r, %r)' % (type(self.__name__), self._back, self._prefix)

    def open(self):
        self._back.open()
        return self

    def close(self):
        self._back.close()
        return self

    def destroy(self):
        self._back.destroy()
        return self

    def get(self, key):
        return self._back.get(self._key(key))

    def mget(self, keys):
        keymap = dict((self._key(k), k) for k in keys)
        return ((keymap[k], v) for (k, v) in self._back.mget(keymap.iterkeys()))

    def gets(self, key):
        return self._back.gets(self._key(key))

    def set(self, key, value):
        self._back.set(self._key(key), value)

    def mset(self, pairs):
        self._back.mset((self._key(k), v) for (k, v) in pairs)

    def add(self, key, value):
        self._back.add(self._key(key), value)

    def replace(self, key, value):
        self._back.replace(self._key(key), value)

    def cas(self, key, value, token):
        self._back.cas(self._key(key), value, token)

    def _key(self, key):
        return self._prefix + key
