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

def structure(name, weak=False, base=None):
    """Make a structure base class for an externally defined
    schema."""

    ## Structures may be defined with a "base" property that names a
    ## parent record to inherit from.  If not given, inherit from
    ## Structure.
    schema = types.get_schema(name)
    base_name = schema.get_prop('base')
    base = types.get_type(base_name) if base_name else (base or Structure)

    slots = ()
    if weak and '__weakref__' not in base.__all__:
        slots = ('__weakref__', )

    return type(name, (base, ), {
        '__kind__': name,
        '__abstract__': True,
        '__slots__': slots
    })

class RecordType(type):
    """A metaclass for avro Record schemas."""

    def __new__(mcls, name, bases, attr):
        abstract = attr.setdefault('__abstract__', False)

        if '__kind__' in attr:
            mcls.use_schema(bases, attr)
        else:
            attr.setdefault('__slots__', ())

        cls = type.__new__(mcls, name, bases, attr)
        cls.__all__ = tuple(coll.slots(cls))
        cls.__name__ = types.type_name(cls)

        if not abstract:
            types.declare(cls)

        return cls

    @classmethod
    def use_schema(mcls, bases, attr):
        ## Find the right schema.
        obj = attr['__schema__'] = types.get_schema(attr['__kind__'])

        ## Use the schema doc property as the class docstring.
        attr.setdefault('__doc__', obj.props.get('doc', ''))

        ## Add new slots to the tuple of slots.  Fields may be
        ## redeclared in a child schema; exclude them from the slots
        ## since Python will complain about duplicate slot names.
        used = set(s for b in bases for s in getattr(b, '__all__', ()))
        attr['__slots__'] += tuple(f.name for f in obj.fields if f.name not in used)

    def __getstate__(cls):
        return marshall.json.loads(str(cls.__schema__))

    def __json__(cls):
        try:
            return cls.__json
        except AttributeError:
            cls.__json = unqualify_schema(type(cls).__getstate__(cls))
            return cls.__json

    @property
    def __fields__(cls):
        try:
            return cls.__fields
        except AttributeError:
            fields = cls.__schema__.fields
            cls.__fields = omap((f.name, types.from_schema(f)) for f in fields)
            return cls.__fields

def unqualify_schema(data):
    """Hack around qualified names embedded in exported Avro schema."""

    if isinstance(data, basestring):
        return types.unqualified(data)
    elif isinstance(data, list):
        return map(unqualify_schema, data)
    elif isinstance(data, dict):
        for name in ('base', 'name', 'items', 'values'):
            val = data.get(name)
            if val:
                data[name] = unqualify_schema(val)
        fields = data.get('fields', ())
        for field in fields:
            field['type'] = unqualify_schema(field['type'])
    return data

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

    def __json__(self):
        return self.__getstate__()

    def __getstate__(self):
        return dict((n, getattr(self, n)) for n in self.__all__)

    def __setstate__(self, state):
        self.update(state)

    @classmethod
    def __adapt__(cls, val):
        if isinstance(val, dict):
            return cls.__restore__(val)

    @classmethod
    def __restore__(cls, state):
        obj = object.__new__(cls)
        for field in cls.__schema__.fields:
            name = field.name
            cls = types.from_schema(field)
            setattr(obj, name, types.cast(state[name], cls))
        return obj

    ## Extra Methods

    def replace(self, seq=(), **kw):
        return copy.copy(self).update(seq, **kw)

    def update(self, seq=(), **kw):
        for (name, val) in chain_items(seq, kw):
            setattr(self, name, val)
        return self
