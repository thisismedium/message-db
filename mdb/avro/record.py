## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""record -- Python types for Avro records"""

from __future__ import absolute_import
import copy
from md.prelude import *
from md import collections as coll
from . import marshall, types

__all__ = ('structure', 'Structure')

# A structure is a simple Python type for an Avro record schema.
# Structures have a slot for each field in the schema.  Like a
# namedtuple, values for all the fields must be passed to the
# constructor in field-order.  No type-checking is done for the
# fields.

def structure(name, weak=False):
    """Make a structure base class for an externally defined
    schema."""

    return type(name, (Structure, ), {
        '__kind__': name,
        '__abstract__': True,
        '__slots__': ('__weakref__', ) if weak else ()
    })

class RecordType(type):
    """A metaclass for avro Record schemas."""

    def __new__(mcls, name, bases, attr):
        abstract = attr.setdefault('__abstract__', False)

        if '__kind__' in attr:
            mcls.use_schema(attr)
        else:
            attr.setdefault('__slots__', ())

        cls = type.__new__(mcls, name, bases, attr)
        cls.__all__ = tuple(coll.slots(cls))

        if not abstract:
            types.declare(cls)

        return cls

    @classmethod
    def use_schema(mcls, attr):
        obj = attr['__schema__'] = types.get_schema(attr['__kind__'])
        attr.setdefault('__doc__', obj.props.get('doc', ''))
        attr['__slots__'] += tuple(f.name for f in obj.fields)

    def __getstate__(cls):
        return marshall.json.loads(str(cls.__schema__))

class Structure(object):
    __metaclass__ = RecordType
    __abstact__ = True

    def __init__(self, *args):
        for (name, val) in izip(self.__all__, args):
            setattr(self, name, val)

    ## Basic object interface

    def __repr__(self):
        return '%s(%s)' % (
            type(self).__name__,
            ', '.join('%s=%r' % (n, getattr(self, n)) for n in self.__all__)
        )

    def __iter__(self):
        return (getattr(self, n) for n in self.__all__)

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return all(a == b for (a, b) in izipl(self, other, fillvalue=Undefined))
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, type(self)):
            return not self == other
        return NotImplemented

    ## Serialization

    def __getstate__(self):
        return dict((n, marshall.getstate(getattr(self, n))) for n in self.__all__)

    def __setstate__(self, state):
        self.update(state)

    @classmethod
    def __adapt__(cls, val):
        if isinstance(val, dict):
            return cls.__restore__(val)

    @classmethod
    def __restore__(cls, state):
        obj = cls.__new__(cls)
        for field in cls.__schema__.fields:
            name = field.name
            cls = types.get_type(types.type_name(field))
            setattr(obj, name, types.cast(state[name], cls))
        return obj

    ## Extra Methods

    def replace(self, seq=(), **kw):
        return copy.copy(self).update(seq, **kw)

    def update(self, seq=(), **kw):
        for (name, val) in chain_items(seq, kw):
            setattr(self, name, val)
        return self
