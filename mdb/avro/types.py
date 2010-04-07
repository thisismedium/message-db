## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""types -- Python types for primitive and non-named Avro types"""

from __future__ import absolute_import
import weakref
from md.prelude import *
from avro import schema as _s

__all__ = (
    'cast',
    'null', 'string', 'boolean', 'bytes', 'int', 'long', 'float', 'double',
    'mapping', 'array'
)

## Support type casting using the Adaptation protocol.  The adapt()
## method is defined in md._prelude.  See also:
##
##   Adaptation <http://www.python.org/dev/peps/pep-0246/>
##   PEAK Protocols <http://peak.telecommunity.com/protocol_ref/module-protocols.html>

cast = adapt

## When a type is declared, it's added to a global registry.  This is
## used in various places, especially by marshall, to map type names
## to Python types or visa versa.

def declare(type):
    """Add a new type to the global type namespace."""

    key = name(type)
    if TYPES.setdefault(key, type) is not type:
        raise TypeError('Type %r has already been declared.' % key)
    return type

def get(name):
    """Get a type by name."""

    probe = TYPES.get(name)
    if probe is None:
        raise NameError('Undefiend type: %r.' % name)
    return probe

def name(type):
    """Get a type's name."""

    if type is None:
        return PRIMITIVE[type.__class__]

    probe = PRIMITIVE.get(type)
    if probe is not None:
        return probe

    ## FIXME: do something better than this ugly while loop.
    while isinstance(type, (_s.Schema, _s.Field)):
        if isinstance(type, _s.NamedSchema):
            return type.fullname
        elif isinstance(type, _s.ArraySchema):
            return complex_name(type.type, type.items)
        elif isinstance(type, _s.MapSchema):
            return complex_name(type.type, type.values)
        type = type.type

    return getattr(type, '__kind__', None) or getattr(type, '__name__', type)

## The Avro implementation uses Schema objects to arbitrate
## validation, reading and writing.

def from_schema(schema):
    """Find a Python type for an Avro schema object."""

    return get(name(schema))

def to_schema(cls):
    """Find an Avro schema object using a Python type."""

    if not isinstance(cls, type):
        cls = type(cls)
    try:
        return cls.__schema__
    except AttributeError:
        probe = PRIMITIVE.get(cls)
        if probe is None:
            raise TypeError('Cannot convert %r to schema.' % cls)
        return _s.PrimitiveSchema(probe)


### Primitive Types

## Avro maps its primitive type names onto builtin Python types.  Use
## some more specific types instead to avoid confusion and to interact
## with the generalized read/write dispatch.

null = type(None)
boolean = bool
int = int
long = long
float = float

class string(unicode):

    @classmethod
    def __adapt__(cls, val):
        if isinstance(val, cls) or val is None:
            return val
        elif isinstance(val, str):
            return cls(val.decode('ascii'))
        return cls(val)

class bytes(str):
    pass

class double(float):
    pass


### Complex Types

## Complex types are parameterized by other types and are not named.
## A naming convention of complex<type> is used to memoize Python
## types and to box serialized data.

def complex_name(kind, value):
    return '%s<%s>' % (kind, name(value))

## The keys for mapping types must be strings.  A tree is used as a
## base type to ensure the keys are kept in a consistent order.  This
## is necessary because static data storage hashes the serialized data
## to make a key for it.  If normal dictionaries are used, the items
## may be written in random order and equal objects would not hash to
## the same value.

Mapping = tree

def mapping(values):
    kind = complex_name('map', values)
    try:
        return get(kind)
    except NameError:
        return declare(type(kind, (Mapping, ), {
            'type': values,
            '__kind__': kind,
            '__schema__': mapping_schema(values),
            '__getstate__': lambda s: s
        }))

def mapping_schema(values):
    schema = _s.MapSchema('null', SCHEMATA)
    schema.set_prop('values', to_schema(values))
    return schema

## Arrays are implemented as lists.  The special base class allows
## the reader to create the correct type of object with a cast().

class Array(list):

    @classmethod
    def __adapt__(cls, obj):
        if isinstance(obj, Iterable):
            return cls(obj)

def array(items):
    kind = complex_name('array', items)
    try:
        return get(kind)
    except NameError:
        return declare(type(kind, (Array, ), {
            'type': items,
            '__kind__': kind,
            '__schema__': array_schema(items)
        }))

def array_schema(items):
    schema = _s.ArraySchema('null', SCHEMATA)
    schema.set_prop('items', to_schema(items))
    return schema


### Private

## Since primitive types don't have __schema__ attributes, this
## PRIMITIVE mapping allows to_schema() to create an Avro schema
## object from one of these types.  The BUILTIN mapping is used to
## initialize the global type registry.

BUILTIN = {
    null: 'null',
    boolean: 'boolean',
    string: 'string',
    bytes: 'bytes',
    int: 'int',
    long: 'long',
    float: 'float',
    double: 'double'
}

PRIMITIVE = update({ unicode: 'string' }, BUILTIN)

## The SCHEMATA mapping tracks each schema by name.  This is
## maintained separately from TYPES because the BUILTIN type mapping
## confuses Avro.  See schema.get().

SCHEMATA = weakref.WeakValueDictionary()

## The TYPES mapping is a global registry that tracks Python types by
## name.  See get() above.  The clear() method is used to reset the
## registry; this is helpful for unit tests.

TYPES = weakref.WeakValueDictionary()

def clear():
    """Reset the global type namespace."""

    SCHEMATA.clear()
    TYPES.clear()
    TYPES.update((n, t) for (t, n) in BUILTIN.iteritems())

clear()
