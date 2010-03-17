## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""interfaces -- abstract interface"""

from __future__ import absolute_import
from md import stm, abc

__all__ = (
    'properties', 'describe', 'description',
    'Described', 'Descriptor', 'PCursor'
)

def properties(obj):
    return obj.__properties__

def describe(obj, names=None):
    """Like dir(), but produce a iterator over <key, value>
    items."""
    return obj.__describe__(names)

def description(obj, names=None):
    """Produce a list of <key, value> items instead of an iterator."""
    return list(describe(obj, names))

class Described(object):
    __slots__ = ()
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def __properties__(self):
        """A mapping of (name, descriptor) items."""

    def __dir__(self):
        """Support the built-in dir() operation."""
        return self.__properties__.keys()

    def __describe__(self, names=None):
        """Like dir(), but produce a iterator over <key value>
        items."""
        names = names or self.__properties__.iterkeys()
        return ((n, getattr(self, n)) for n in names)

class Descriptor(object):
    """A Python data descriptor with an additional required attribute,
    type, that indicates the type of data being described."""

    __slots__ = ()
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def type(self):
        pass

    @abc.abstractmethod
    def __get__(self, obj, cls):
        pass

    @abc.abstractmethod
    def __set__(self, obj, value):
        pass

class PCursor(stm.Cursor, Described):
    pass
