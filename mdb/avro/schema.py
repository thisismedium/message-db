## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""schema -- load and declare Avro schemata"""

from __future__ import absolute_import
import os, sys, weakref, json
from md.prelude import *
from avro import schema as _s
from . import types

__all__ = ('load', 'require')

def load(data):
    """Load schema(s) declared in a JSON format.  The data may be a
    string or a file-like object; more than one schema may be declared
    in a data source.  A list of avro Schema objects is returned.

    >>> load('''{
    ...     "type": "record",
    ...     "name": "Example.Animal",
    ...     "fields": [
    ...         { "name": "kind", "type": "string" },
    ...         { "name": "age", "type": "int" }
    ...     ]
    ... }''')
    [<avro.schema.RecordSchema ...>]
    """

    result = []
    for val in scan(make_buffer(data)):
        schema = LOADED[types.name(schema)] = declare(val)
        result.append(schema)
    return result

def require(*paths, **kw):
    """Load schema declared in an external file.

    For example, this will load the schema declared in "schema.avro"
    from the same folder the source file is located in.

        require('schema.avro')
    """

    ## Expect that require() is used in the top-level; use stack frame
    ## introspection to find the file name of the calling source file.
    ## If require() is wrapped up by another library method, use depth
    ## > 1!
    depth = kw.pop('depth', 1)
    context = sys._getframe(depth).f_globals.get('__file__')
    if context is None:
        raise ValueError('No __file__ in frame %d.' % depth)

    base = os.path.dirname(context)
    for path in paths:
        try:
            with closing(open(os.path.join(base, path))) as port:
                load(port)
        except IOError as exc:
            raise NameError('require(%r) failed: %s' % (path, exc))


### Private

## Schemata that are loaded are mapped here to prevent them from being
## garbage collected.

LOADED = {}

## A Schema object is created by loading a JSON object, then calling
## the avro make_avsc_object() method.  To support in-Python schema
## declarations, this declare() method is defined separately from
## load().

def declare(definition):
    """Declare a schema using Python data as the definition."""

    try:
        schema = _s.make_avsc_object(definition, types.SCHEMATA)
    except _s.SchemaParseException as exc:
        raise SyntaxError('%s while declaring %r.' % (exc, definition))

    key = types.name(schema)
    if types.SCHEMATA.setdefault(key, schema) is not schema:
        raise TypeError('Schema %r has already been declared.' % key)
    return schema

## Once a Schema is loaded, it's sometimes necessary to find it by
## name.  Often, types.get() is better since it returns the Python
## type associated with a schema name rather than a Schema object.

def get(name):
    probe = types.SCHEMATA.get(name)
    if probe is None:
        raise NameError('Undefined schema: %r.' % name)
    return probe

## The clear() method is here to allow unit tests to reset the global
## environment between test cases.

def clear():
    types.clear()
    LOADED.clear()

## The json.loads() method only allows a data source to contain one
## JSON object.  The scan() and iload() methods use low-level json
## methods to generate a sequence of loaded JSON objects from a data
## source.

def scan(buff):
    """Iterate over the schema declarations in the buffer.  JSON-level
    errors are re-raised with more information."""

    try:
        for val in iload(buff):
            yield val
    except Exception as exc:
        raise SyntaxError('%s while loading %s.' % (exc, buff))

def iload(buff, decoder=None, _w=json.decoder.WHITESPACE.match):
    """Generate a sequence of top-level JSON values declared in the
    buffer.

    >>> list(iload('[1, 2] "a" { "c": 3 }'))
    [[1, 2], u'a', {u'c': 3}]
    """

    decoder = decoder or json._default_decoder
    idx = _w(buff, 0).end()
    end = len(buff)

    try:
        while idx != end:
            (val, idx) = decoder.raw_decode(buff, idx=idx)
            yield val
            idx = _w(buff, idx).end()
    except ValueError as exc:
        raise ValueError('%s (%r at position %d).' % (exc, buff[idx:], idx))

def make_buffer(obj):
    """Convert a string or file-like object into an in-memory buffer."""
    if isinstance(obj, basestring):
        return obj
    return obj.read()
