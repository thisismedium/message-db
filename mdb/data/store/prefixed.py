## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""prefixed -- prefix keys for a real backing store"""

from __future__ import absolute_import
from md.prelude import *
from md import abc
from .interface import *

__all__ = ('prefixed', )

@abc.implements(Logical)
class prefixed(object):
    """A "backing store" that adds a prefix to any keys.  This can be
    used to logically partition the keyspace of a real backing store."""

    def __init__(self, back, prefix='', marshall=None):
        if isinstance(back, Logical):
            prefix = back._prefix + prefix
            marshall = marshall or back._marshall
            back = back._back
        self._back = back
        self._prefix = prefix
        self._marshall = marshall

    def __repr__(self):
        return '%s(%r, %r)' % (type(self).__name__, self._back, self._prefix)

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
        return self._load(self._back.get(self._key(key)))

    def mget(self, keys):
        keymap = dict((self._key(k), k) for k in keys)
        return (
            (keymap[k], self._load(v))
            for (k, v) in self._back.mget(keymap.iterkeys())
        )

    def gets(self, key):
        (val, tok) = self._back.gets(self._key(key))
        return (self._load(val), tok)

    def set(self, key, value):
        self._back.set(self._key(key), self._dump(value))

    def mset(self, pairs):
        self._back.mset((self._key(k), self._dump(v)) for (k, v) in pairs)

    def add(self, key, value):
        self._back.add(self._key(key), self._dump(value))

    def madd(self, pairs):
        self._back.madd((self._key(k), self._dump(v)) for (k, v) in pairs)

    def replace(self, key, value):
        self._back.replace(self._key(key), self._dump(value))

    def mreplace(self, pairs):
        self._back.mreplace((self._key(k), self._dump(v)) for (k, v) in pairs)

    def cas(self, key, value, token):
        self._back.cas(self._key(key), self._dump(value), token)

    def delete(self, key):
        return self._back.delete(self._key(key))

    def mdelete(self, keys):
        return self._back.mdelete(self._key(k) for k in keys)

    def _key(self, key):
        return self._prefix + key

    def _load(self, value):
        return value if value is Undefined else self._marshall.loads_binary(value)

    def _dump(self, value):
        return self._marshall.dumps_binary(value)
