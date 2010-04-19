## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""tree -- bootstrap tree and api; see tree.py"""

from __future__ import absolute_import
from md.prelude import *
from .. import avro, data

__all__ = ('get_type', 'type_name', 'Key', 'text', 'html')


### Types

## Re-export get_type() and type_name() from Avro.  This gives the db
## package a layer of indirection if necessary.

get_type = avro.get_type

def type_name(obj, *args, **kw):
    if isinstance(obj, basestring):
        return obj
    return avro.type_name(obj, *args, **kw)

## Re-export the Key type from the data package.

Key = data.Key

## Built-in types are declared in an external schema for now.  The
## record types defined in this package are principly focused on
## representing web content hierarchically.

avro.require('tree.json')

def content(name):
    """Define a new Content structure."""

    return avro.structure('M.%s' % name, base=Content)

class Content(avro.Structure):
    """This is an abstract base class for content structures.  See
    tree.py for concrete classes."""

    __abstract__ = True
    __slots__ = ('_key', )

    def __init__(self, **kw):
        ## Use the field definitions to determine types and default
        ## values for the keyword arguments if necessary.
        for field in self.__schema__.fields:
            val = kw.pop(field.name, Undefined)
            if val is Undefined:
                if not field.has_default:
                    raise ValueError('%r is a required field.' % field.name)
                val = avro.cast(field.default, avro.types.from_schema(field))
            setattr(self, field.name, val)
        if kw:
            raise TypeError('%r does not support extra properties: %r.')

    def __json__(self):
        return update(self.__getstate__(), _kind=self.kind, _key=self.key)

    @property
    def kind(self):
        return type(self).__name__

    @property
    def key(self):
        return self._key

    def update(self, seq=(), **kw):
        fields = type(self).__fields__
        for key, val in chain_items(seq, kw):
            cls = fields.get(key)
            if cls is not None:
                val = avro.cast(val, cls)
            setattr(self, key, val)
        return self


### Extra Types

## These types aren't used directly, but are defined here because they
## are declared in the external schema.  If the avro package supports
## automatic creation of non-record types in the future, these can be
## removed.

class text(avro.primitive('M.text', avro.string)):
    """Text is a multiline string type."""

class html(avro.primitive('M.html', avro.string)):
    """Marked-up text."""

_folder = avro.union(Key, None)

_content = avro.omap(Key)
