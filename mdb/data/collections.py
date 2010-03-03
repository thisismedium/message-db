## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""collections -- collection data types"""

from __future__ import absolute_import
import copy, bisect, collections as coll, itertools as it
from md import abc
from .prelude import *

__all__ = (
    'Tree', 'MutableTree', 'tree',
    'OrderedMap', 'MutableOrderedMap', 'omap'
)


### Tree

class Tree(Mapping):
    __slots__ = ()

class MutableTree(Tree, MutableMapping):
    __slots__ = ()

## FIXME: optimize this using PyJudy.

@abc.implements(MutableTree)
class tree(dict):
    """A tree that keeps items sorted by key order.

    >>> t1 = tree(a=1, z=2, m=3, b=4, y=5); t1
    tree([('a', 1), ('b', 4), ('m', 3), ('y', 5), ('z', 2)])

    The *keys, *values, and *items methods are extended to accept
    range parameters with the same semantics as islice().

    >>> t1.items('m')
    [('a', 1), ('b', 4)]
    >>> t1.items(None, 'y')
    [('a', 1), ('b', 4), ('m', 3)]
    >>> t1.items('b', 'z')
    [('b', 4), ('m', 3), ('y', 5)]

    Unlike a dict, the serialized representation of a tree is stable.

    >>> from cPickle import dumps, loads
    >>> from hashlib import sha1
    >>> t2 = loads(dumps(t1)); t2
    tree([('a', 1), ('b', 4), ('m', 3), ('y', 5), ('z', 2)])
    >>> sha1(dumps(t1)).digest() == sha1(dumps(t2)).digest()
    True
    """
    __slots__ = ('_index', )

    def __new__(cls, seq=(), **kwargs):
        self = dict.__new__(cls)
        self._index = []
        return self

    def __init__(self, seq=(), **kwargs):
        dict.__init__(self)
        if seq or kwargs:
            self.update(seq, **kwargs)

    def __repr__(self):
        return '%s([%s])' % (
            type(self).__name__,
            ', '.join(repr(i) for i in self.iteritems())
        )

    def __copy__(self):
        obj = type(self).__new__(type(self))
        dict.update(obj, self)
        obj._index = copy.copy(self._index)
        return obj

    def __getstate__(self):
        return self.items()

    def __setstate__(self, state):
        dict.update(self, state)
        self._index = [k for (k, _) in state]

    ## MutableMapping / dict

    # __len__
    # __contains__
    # __getitem__

    def __setitem__(self, key, value):
        if key not in self:
            bisect.insort_left(self._index, key)
        dict.__setitem__(self, key, value)

    def __delitem__(self, key):
        dict.__delitem__(self, key)
        self._index.remove(key)

    def __iter__(self):
        return iter(self._index)

    def iterkeys(self, *offsets):
        """Generate a list of all keys or keys in a certain range.
        The offset semantics are the same as islice()."""

        if not offsets:
            return iter(self)
        elif len(offsets) == 1:
            start = None; (end,) = offsets
        else:
            (start, end) = offsets

        if start is None:
            ## One argument: assume it is "end".  Generate the range
            ## [leftmost, end)
            keys = self
        else:
            pivot = bisect.bisect_left(self._index, start)
            keys = islice(self._index, pivot, None)

        if end is None:
            ## Generate the range [start, rightmost]
            return keys
        ## Generate the range [start, end)
        return it.takewhile(lambda k: k < end, keys)

    def itervalues(self, *offsets):
        if not offsets:
            return (self[k] for k in self._index)
        return (self[k] for k in self.iterkeys(*offsets))

    def iteritems(self, *offsets):
        if not offsets:
            return ((k, self[k]) for k in self._index)
        return ((k, self[k]) for k in self.iterkeys(*offsets))

    def keys(self, *offsets):
        if not offsets:
            return list(self._index)
        return list(self.keys(*offsets))

    def values(self, *offsets):
        return list(self.itervalues(*offsets))

    def items(self, *offsets):
        return list(self.iteritems(*offsets))

    def clear(self):
        dict.clear(self)
        del self._index[:]

    def copy(self):
        return self.__copy__()

    def pop(self, key, default=None):
        result = dict.pop(self, key, default)
        try:
            self._index.remove(key)
        except ValueError:
            pass
        return result

    def popitem(self):
        key = self._index.pop()
        return (key, dict.pop(self, key))

    def setdefault(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            self[key] = default
            return default

    def update(self, seq=(), **kwargs):
        for (key, value) in chain_items(seq, kwargs):
            self[key] = value

    ## Sequence

    def append(self, (key, value)):
        self[key] = value

    def extend(self, items):
        self.update(items)


### OrderedDict

try:
    OrderedDict = coll.OrderedDict
except AttributeError:
    from ._odict import OrderedDict

class OrderedMap(Mapping):
    __slots__ = ()

class MutableOrderedMap(OrderedMap, MutableMapping):
    __slots__ = ()

@abc.implements(MutableOrderedMap)
class omap(OrderedDict):

    ## Sequence

    def append(self, (key, value)):
        self[key] = value

    def extend(self, items):
        self.update(items)

