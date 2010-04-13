## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""types -- Python types for primitive and non-named Avro types"""

from __future__ import absolute_import
import weakref
from md.prelude import *
from avro import schema as _s

__all__ = (
    'cast', 'get_type', 'type_name',
    'null', 'string', 'boolean', 'bytes', 'int', 'long', 'float', 'double',
    'primitive', 'fixed', 'union',
    'map', 'omap', 'array'
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

    key = type_name(type)
    if TYPES.setdefault(key, type) is not type:
        raise TypeError('Type %r has already been declared.' % key)
    return type

def get_type(name):
    """Get a type by name."""

    probe = TYPES.get(name)
    if probe is None:
        raise NameError('Undefiend type: %r.' % name)
    return probe

def type_name(type):
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
        elif isinstance(type, _s.UnionSchema):
            return complex_name(type.type, *[type_name(s) for s in type.schemas])
        type = type.type

    return getattr(type, '__kind__', None) or getattr(type, '__name__', type)

## The Avro implementation uses Schema objects to arbitrate
## validation, reading and writing.

def from_schema(schema):
    """Find a Python type for an Avro schema object."""

    return get_type(type_name(schema))

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

## Once a Schema is loaded, it's sometimes necessary to find it by
## name.  Often, get_type() is better since it returns the Python type
## associated with a schema name rather than a Schema object.

def get_schema(name):
    probe = SCHEMATA.get(name)
    if probe is None:
        raise NameError('Undefined schema: %r.' % name)
    return probe


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
    __slots__ = ()

    @classmethod
    def __adapt__(cls, val):
        if isinstance(val, cls) or val is None:
            return val
        elif isinstance(val, str):
            return cls(val.decode('ascii'))
        return cls(val)

class bytes(str):
    __slots__ = ()

class double(float):
    __slots__ = ()

def primitive(name, base):
    """Declare a named, primitive type."""

    return PrimitiveType(name, (base, ), {
        '__kind__': name,
        '__module__': __name__,
        '__abstract__': True
    })

class PrimitiveType(type):

    def __new__(mcls, name, bases, attr):
        abstract = attr.setdefault('__abstract__', False)

        if '__kind__' in attr:
            mcls.use_schema(attr)

        cls = type.__new__(mcls, name, bases, attr)

        if not abstract:
            declare(cls)

        return cls

    @classmethod
    def use_schema(mcls, attr):
        obj = attr['__schema__'] = get_schema(attr['__kind__'])
        attr.setdefault('__doc__', obj.props.get('doc', ''))

class NamedPrimitive(_s.NamedSchema):

    def __str__(self):
        return json.dumps(self.props)

    def __eq__(self, other):
        if isinstance(other, _s.Schema):
            return self.props == other.props
        return NotImplemented


### Fixed

## Fixed schema are named types with a specific length.

def fixed(name):
    """Make a fixed base class for an externally defined schema."""

    return primitive(name, Fixed)

class Fixed(str):
    __abstract__ = True
    __metaclass__ = PrimitiveType

    @classmethod
    def __adapt__(cls, value):
        if isinstance(value, str):
            return cls(value)


### Complex Types

## Complex types are parameterized by other types and are not named.
## A naming convention of complex<type> is used to memoize Python
## types and to box serialized data.

def complex_name(kind, *values):
    return '%s<%s>' % (kind, ', '.join(type_name(v) for v in values))

## The keys for mapping types must be strings.  A tree is used as a
## base type to ensure the keys are kept in a consistent order.  This
## is necessary because static data storage hashes the serialized data
## to make a key for it.  If normal dictionaries are used, the items
## may be written in random order and equal objects would not hash to
## the same value.

def map(values):
    return mapping(complex_name('map', values), tree, values, _s.MapSchema)

_omap = omap

def omap(values):
    return mapping(complex_name('omap', values), _omap, values, OMapSchema)

def mapping(kind, base, values, schema):
    try:
        return get_type(kind)
    except NameError:
        return declare(type(kind, (base, ), {
            'type': values,
            '__module__': __name__,
            '__kind__': kind,
            '__schema__': mapping_schema(schema, values),
            '__getstate__': lambda s: s
        }))

def mapping_schema(cls, values):
    schema = cls('null', SCHEMATA)
    schema.set_prop('values', to_schema(values))
    return schema

class OMapSchema(_s.MapSchema):

    def __init__(self, values, names=None):
        super(OMapSchema, self).__init__(values, names)
        self.set_prop('type', 'omap')

## Arrays are implemented as lists.  The special base class allows
## the reader to create the correct type of object with a cast().

def array(items):
    kind = complex_name('array', items)
    try:
        return get_type(kind)
    except NameError:
        return declare(type(kind, (Array, ), {
            'type': items,
            '__module__': '__name__',
            '__kind__': kind,
            '__schema__': array_schema(items)
        }))

class Array(list):

    @classmethod
    def __adapt__(cls, obj):
        if isinstance(obj, Iterable):
            return cls(obj)

def array_schema(items):
    schema = _s.ArraySchema('null', SCHEMATA)
    schema.set_prop('items', to_schema(items))
    return schema

## Unions represent more than one possible type.

def union(*types):
    kind = complex_name('union', *types)
    try:
        return get_type(kind)
    except NameError:
        return declare(type(kind, (Union, ), {
            'type': types,
            '__module__': '__name__',
            '__kind__': kind,
            '__schema__': union_schema(types)
        }))

class Union(object):

    @classmethod
    def __adapt__(cls, val):
        if isinstance(val, cls.type):
            return val

        for kind in cls.type:
            try:
                return cast(val, kind)
            except AdaptationFailure:
                pass

def union_schema(types):
    schema = _s.UnionSchema([], SCHEMATA)
    schema._schemas = list(to_schema(t) for t in types)
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

## The TYPES mapping is a global registry that tracks Python types by
## name.  See get_type() above.  The clear() method is used to reset
## the registry; this is helpful for unit tests.

TYPES = weakref.WeakValueDictionary()

## The SCHEMATA mapping tracks each schema by name.  This is
## maintained separately from TYPES because the BUILTIN type mapping
## confuses Avro.  See get_schema() above.

SCHEMATA = weakref.WeakValueDictionary()

def clear():
    """Reset the global type namespace."""

    SCHEMATA.clear()
    TYPES.clear()
    TYPES.update((n, t) for (t, n) in BUILTIN.iteritems())

clear()
