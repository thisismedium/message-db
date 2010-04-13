## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""tree -- bootstrap tree and api; see tree.py"""

from __future__ import absolute_import
import weakref, uuid, base64
from md.prelude import *
from .. import avro

__all__ = ('get_type', 'type_name', 'Key', 'text', 'html')


### Types

get_type = avro.get_type

def type_name(obj):
    if isinstance(obj, basestring):
        return obj
    return avro.type_name(obj)

## Built-in types are declared in an external schema for now.

avro.require('tree.json')

def content(name):
    return avro.structure('M.%s' % name, base=Content)

class Content(avro.Structure):
    __abstract__ = True
    __slots__ = ('_key', )

    def __init__(self, **kw):
        for field in self.__schema__.fields:
            val = kw.pop(field.name, Undefined)
            if val is Undefined:
                if not field.has_default:
                    raise ValueError('%r is a required field.' % field.name)
                val = avro.cast(field.default, avro.types.from_schema(field))
            setattr(self, field.name, val)
        if kw:
            raise TypeError('%r does not support extra properties: %r.')

    @property
    def key(self):
        return self._key


### Key

class Key(avro.structure('M.key', weak=True)):
    """A Key identifies a model instance.

    >>> k1 = Key.make('Foo')
    >>> k2 = Key.make('Foo')
    >>> k3 = Key.make('Foo', 'bar'); k3
    Key('BkZvbwIGYmFy')
    >>> k1 is not k2
    True
    >>> k2 is Key(str(k2))
    True
    >>> k3 is Key.make('Foo', 'bar')
    True
    >>> k3 is Key('BkZvbwIGYmFy')
    True
    >>> k3.kind
    u'Foo'
    >>> k3.id
    u'bar'
    >>> avro.cast('BkZvbwIGYmFy', Key)
    Key('BkZvbwIGYmFy')
    """

    __slots__ = ('_encoded', )

    ## There are lots of Keys; intern them to avoid some object
    ## allocation.

    INTERNED = weakref.WeakValueDictionary()

    def __new__(cls, encoded):
        encoded = str(encoded)
        try:
            return cls.INTERNED[encoded]
        except KeyError:
            return cls._decode(encoded)

    ## By default, the constructor would set self.kind to the first
    ## argument given.  Do nothing, initialization happens in make().

    def __init__(self, encoded):
        pass

    def __repr__(self):
        return '%s(%r)' % (type(self).__name__, str(self))

    def __str__(self):
        if self._encoded is None:
            self._encoded = self._encode()
        return self._encoded

    ## Most of the time, a Key is treated opaquely.  Use its string
    ## representation for hashing and equality.

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        if isinstance(other, Key):
            return (self.kind == other.kind and self.id == other.id)
        elif isinstance(other, basestring):
            return str(self) == other
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, (basestring, Key)):
            return not self == other
        return NotImplemented

    ## Since Keys are interned, copying is a no-op.

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self

    @classmethod
    def __adapt__(cls, obj):
        if isinstance(obj, basestring):
            return cls(obj)
        elif isinstance(obj, dict):
            return cls.__restore__(obj)._intern()

    ## Creating a Key directly is unusual, so make() is a second-class
    ## constructor

    @classmethod
    def make(cls, kind, id=None):
        self = object.__new__(cls)
        self.kind = avro.string(type_name(kind))
        self.id = avro.string(id) if id else _uuid(uuid.uuid4().bytes)
        return self._intern()

    def _intern(self):
        self._encoded = None
        return self.INTERNED.setdefault(str(self), self)

    ## Use a base64 encoded binary Avro value as the opaque
    ## representation.

    @staticmethod
    def _decode(enc):
        pad = len(enc) % 4
        enc = str(enc) + '=' * (4 - pad) if pad else enc
        data = base64.urlsafe_b64decode(enc)
        return avro.loads_binary(data, unbox=None, header=False)

    def _encode(self):
        data = avro.dumps_binary(self, box=None, header=False)
        return base64.urlsafe_b64encode(data).rstrip('=')

class _uuid(avro.fixed('M.uuid')):
    """The id field of most keys is a uuid."""

class _id(avro.union(_uuid, avro.string)):
    """But it may also be a name."""

class text(avro.primitive('M.text', avro.string)):
    """Text is a multiline string type."""

class html(avro.primitive('M.html', avro.string)):
    """Marked-up text."""

_folder = avro.union(Key, None)

_content = avro.omap(Key)
