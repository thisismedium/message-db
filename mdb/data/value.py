## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""value -- uniform behavior for key/value items in the logical space."""

from __future__ import absolute_import
import weakref, operator, uuid, base64
from md.prelude import *
from . import avro

__all__ = ('Key', 'value', 'Value')


### Key

avro.require('value.json')

## A key could simply be a string.  To facilite direct references and
## indexing, this Key type may be used instead.  It's primary
## representation is a base64-encoded binary avro <type-name, id>
## pair.

class Key(avro.structure('M.key', weak=True)):
    """A Key identifies values in the logical address space.

    >>> k1 = Key.make('Foo')
    >>> k2 = Key.make('Foo')
    >>> k3 = Key.make('Foo', 'bar'); k3
    key('BkZvbwIGYmFy')
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
    key('BkZvbwIGYmFy')
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

    def __json__(self):
        return str(self)

    ## Define encode() to allow the avro DatumWriter to write this as
    ## a string.

    def encode(self, encoding):
        assert encoding == 'utf-8', 'Expected UTF-8, not: %r.' % encoding
        return str(self)

    ## Most of the time, a Key is treated opaquely.  Use its string
    ## representation for hashing and equality.

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return self._compare(operator.eq, other)

    def __ne__(self, other):
        return self._compare(operator.ne, other)

    def __lt__(self, other):
        return self._compare(operator.lt, other)

    def __le__(self, other):
        return self._compare(operator.le, other)

    def __gt__(self, other):
        return self._compare(operator.gt, other)

    def __ge__(self, other):
        return self._compare(operator.ge, other)

    def _compare(self, op, other):
        if isinstance(other, Key):
            other = str(other)
        if isinstance(other, basestring):
            return op(str(self), other)
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

    ## Extra properties

    @property
    def type(self):
        return avro.get_type(self.kind)

    ## Creating a Key directly is unusual, so make() is a second-class
    ## constructor

    @classmethod
    def make(cls, kind, id=None, name=None):
        self = object.__new__(cls)
        if not isinstance(kind, basestring):
            kind = avro.type_name(kind, True)
        self.kind = avro.string(kind)
        if name is not None:
            self.id = avro.string(name)
        else:
            self.id = avro.cast(id, _id) if id else _uuid(uuid.uuid4().bytes)
        return self._intern()

    def _intern(self):
        self._encoded = None
        return self.INTERNED.setdefault(str(self), self)

    ## Use a base64 encoded binary Avro value as the opaque
    ## representation.

    @classmethod
    def _decode(cls, enc):
        pad = len(enc) % 4
        enc = str(enc) + '=' * (4 - pad) if pad else enc
        data = base64.urlsafe_b64decode(enc)
        return avro.loads_binary(data, cls=cls, unbox=None, header=False)

    def _encode(self):
        data = avro.dumps_binary(self, box=None, header=False)
        return base64.urlsafe_b64encode(data).rstrip('=')

class _uuid(avro.fixed('M.uuid')):
    """The id field of most keys is a uuid."""

class _id(avro.union(_uuid, avro.string)):
    """But it may also be a name."""


### Value

def value(name):
    """Define a new Value structure."""

    return avro.structure('M.%s' % name, base=Value)

class Value(avro.Structure):
    """This is an abstract base class for value structures.  See
    repo.py."""

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
