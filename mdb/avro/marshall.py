## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""marshall -- dump or load Avro data"""

from __future__ import absolute_import
import json, zlib, cStringIO
from avro import io, schema as _s
from md.prelude import *
from . import types

__all__ = (
    'json', 'dumps', 'loads',
    'dump_binary', 'load_binary', 'dumps_binary', 'loads_binary',
    'box_type', 'unbox_type', 'getstate'
)


### JSON

## The JSON representation of Avro data corresponds exactly to the
## binary representation.  This implementation allows classes to
## implement a special __json__() method to help produce the correct
## representation.

def dumps(obj):
    """Serialize an object to a JSON string."""

    return _encoder.encode(obj)

def loads(data, cls):
    """Unserialize and object from a JSON string.  The second argument
    is the object's type."""

    return types.cast(json.loads(data), cls)

class JSONEncoder(json.JSONEncoder):

    def _iterencode(self, obj, markers=None):

        ## Try to use the __json__() method to transform this object
        ## into a serializable value.
        to_json = getattr(type(obj), '__json__', None)
        if to_json:
            obj = to_json(obj)

        ## Treat iterators as lists.
        if isinstance(obj, Iterator):
            return self._iterencode_list(obj, markers)

        ## Fall back on the builtin behavior.
        return super(JSONEncoder, self)._iterencode(obj, markers)

_encoder = JSONEncoder(
    skipkeys=False,
    ensure_ascii=True,
    check_circular=True,
    allow_nan=True,
    indent=None,
    separators=None,
    encoding='utf-8',
    sort_keys=True
)


### Binary

BINARY_VERSION = 1
BINARY_CODEC = { 'null': 0 }

## Avro is a statically typed storage format arbitrated by schema.  In
## some cases, it's useful to store Python data with a "type tag" so
## it can be loaded later when the schema isn't known a priori.

## The box_type() and unbox_type() procedures are the default way to
## find an object's type tag.

def box_type(obj):
    return types.type_name(type(obj))

def unbox_type(name):
    return types.get_type(name)

## The null codec does nothing to the serialized data.  The compress
## codec isn't implemented yet.

def dump_null(obj, port, box=box_type, header=True):
    """Serialize an object to a binary stream.

    If box is None, no type tag is written.  Otherwise, it can be a
    procedure that accepts obj and returns a string."""

    dw = DatumWriter(types.to_schema(obj))
    be = BinaryEncoder(port)

    ## Header
    if header:
        _write_header(be, 'null')
    if box:
        be.write_utf8(box(obj))

    ## Body
    dw.write(obj, be)
    return port

def load_null(port, cls=None, unbox=unbox_type, header=True):
    """Unserialize an object from a binary stream.

    If unbox is None, the object must not be tagged with a type.
    Otherwise, it can be a procedure that accepts a string and returns
    a type object."""

    bd = BinaryDecoder(port)

    if header:
        (version, codec) = _read_header(bd)
        assert version == BINARY_VERSION
        assert codec == BINARY_CODEC['null']

    if unbox:
        cls = unbox(bd.read_utf8())
    elif cls is None:
        raise TypeError('Missing required argument: cls or unbox.')

    dr = DatumReader(types.to_schema(cls))
    return dr.read(bd)

## When more than one codec is implemented, these will accept an
## optional parameter to choose a codec.

dump_binary = dump_null
load_binary = load_null

def dumps_binary(obj, **kw):
    with closing(cStringIO.StringIO()) as port:
        return dump_binary(obj, port, **kw).getvalue()

def loads_binary(data, **kw):
    with closing(cStringIO.StringIO(data)) as port:
        return load_binary(port, **kw)

## BINARY_VERSION 1 just writes its version number and the codec used.

def _write_header(be, codec):
    be.write_int(BINARY_VERSION)
    be.write_int(BINARY_CODEC[codec])
    return be

def _read_header(bd):
    return (bd.read_int(), bd.read_int())

## Avro's default readers and writers are written in a way that makes
## extending them to support new types impossible.  These override key
## methods to generalize the reading process to use cast() and the
## writing process to use getstate().

class BinaryEncoder(io.BinaryEncoder):
    """Extend the encoder to support write_string() so generalized
    dispatching can work."""

    def write_string(self, datum):
        return self.write_utf8(datum)

class DatumWriter(io.DatumWriter):
    """Use generalized dispatch and add a getstate() before
    dispatching to a complex type handler."""

    def write_data(self, writers_schema, datum, encoder):

        ## NOTE: no schema validation

        method = 'write_%s' % writers_schema.type
        if hasattr(encoder, method):
            getattr(encoder, method)(datum)
        elif method == 'write_union':
            ## Don't getstate() since write_union() needs to know the
            ## Python type of datum.
            self.write_union(writers_schema, datum, encoder)
        elif hasattr(self, method):
            getattr(self, method)(writers_schema, getstate(datum), encoder)
        else:
            raise _s.AvroException('Unknown type: %r.' % writers_schema.type)

    def write_omap(self, *args):
        return self.write_map(*args)

    def write_set(self, *args):
        return self.write_array(*args)

    def write_union(self, union, datum, encoder):
        index, schema = self.union_schema(union, datum)
        encoder.write_long(index)
        self.write_data(schema, datum, encoder)

    def union_schema(self, union, datum):
        for index, cls in enumerate(types.from_schema(s) for s in union.schemas):
            if isinstance(datum, cls):
                return index, union.schemas[index]
        raise io.AvroTypeException(union, datum)

    def write_error(self, *args):
        return self.write_record(*args)

    def write_request(self, *args):
        return self.write_record(*args)

class BinaryDecoder(io.BinaryDecoder):
    """Extend the decorder to support read_string() so generalized
    dispatching can work."""

    def read_string(self):
        return self.read_utf8()

class DatumReader(io.DatumReader):
    """Use generalized dispatch and add a types.cast() for complex
    types."""

    def read_data(self, writers_schema, readers_schema, decoder):

        ## NOTE: no schema reconciliation

        method = 'read_%s' % writers_schema.type
        if hasattr(decoder, method):
            return getattr(decoder, method)()
        elif hasattr(self, method):
            datum = getattr(self, method)(writers_schema, readers_schema, decoder)
            return types.cast(datum, types.from_schema(writers_schema))
        else:
            raise _s.AvroException('Unknown type: %r.' % writers_schema.type)

    def read_map(self, ws, *args):
        return types.from_schema(ws)(self._map(ws, *args))

    read_omap = read_map

    def read_array(self, ws, *args):
        return types.from_schema(ws)(self._array(ws, *args))

    read_set = read_array

    def read_union(self, ws, rs, decoder):
        ## NOTE: work around lack of schema reconciliation.
        index = int(decoder.read_long())
        return self.read_data(ws.schemas[index], rs.schemas[index], decoder)

    def read_error(self, *args):
        return self.read_record(*args)

    def read_request(self, *args):
        return self.read_record(*args)

    def _map(self, ws, rs, decoder):
        count = decoder.read_long()
        while count != 0:
            if count < 0:
                count = -count
                size = decoder.read_long()
            for i in xrange(count):
                key = decoder.read_utf8()
                val = self.read_data(ws.values, rs.values, decoder)
                yield (key, val)
            count = decoder.read_long()

    def _array(self, ws, rs, decoder):
        count = decoder.read_long()
        while count != 0:
            if count < 0:
                count = -block_count
                size = decoder.read_long()
            for i in xrange(count):
                yield self.read_data(ws.items, rs.items, decoder)
            count = decoder.read_long()


### Aux

def getstate(obj):
    """Allow objects to export themselves in a format that the Avro
    writer can use."""

    try:
        getstate = type(obj).__getstate__
    except AttributeError:
        return obj
    return getstate(obj)
