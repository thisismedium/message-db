## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""prelude -- additional builtins"""

from __future__ import absolute_import
import collections as coll, itertools as it, functools as fn, contextlib as ctx

__all__ = (
    'partial', 'wraps', 'closing',
    'Iterator', 'Sequence', 'chain', 'ichain', 'islice', 'takewhile',
    'Mapping', 'MutableMapping', 'items', 'chain_items',
    'namedtuple', 'Sentinal', 'Undefined'
)


## Procedures

partial = fn.partial
wraps = fn.wraps
closing = ctx.closing


### Sequences

Iterator = coll.Iterator
Sequence = coll.Sequence
chain = it.chain
islice = it.islice
takewhile = it.takewhile

def ichain(seq):
    return (x for s in seq for x in s)


### Mapping

Mapping = coll.Mapping
MutableMapping = coll.MutableMapping

def items(obj):
    return obj.iteritems() if isinstance(obj, Mapping) else obj

def chain_items(*obj):
    return ichain(items(o) for o in obj if o is not None)


### Types

namedtuple = coll.namedtuple

class Sentinal(object):
    """A constructor for sentinal objects.  The optional nonzero
    argument determines how the Sentinal behaves in logical
    operations."""

    __slots__ = ('name', 'nonzero')

    def __init__(self, name, nonzero=True):
        self.name = name.upper()
        self.nonzero = nonzero

    def __nonzero__(self):
        return self.nonzero

    def __repr__(self):
        return '%s' % self.name

Undefined = Sentinal('<undefined>', nonzero=False)
