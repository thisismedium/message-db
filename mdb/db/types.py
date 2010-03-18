## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""properties -- properties and types for models"""

from __future__ import absolute_import
import weakref
from md.prelude import *
from . import models as _m, stm as _stm, api as _api

__all__ = (
    'String', 'StringProperty', 'Text', 'TextProperty',
    'ReferenceProperty',
    'Directory', 'DirectoryProperty'
)

def serialize(cls, dump=None, load=None):
    from ..data import yaml

    yaml.represent(cls.__name__, cls)(dump or cls._dump)
    yaml.construct(cls.__name__)(load or cls._load)

    return cls


### Atomic Types

@serialize
class String(unicode):
    """A unicode value."""

    @classmethod
    def __adapt__(cls, val):
        if isinstance(val, cls) or val is None:
            return val
        elif isinstance(val, str):
            return cls(val.decode('ascii'))
        return cls(val)

    @classmethod
    def _dump(cls, value):
        return unicode(value)

    @classmethod
    def _load(cls, value):
        return cls(value)

class StringProperty(_m.Property):
    type = String

@serialize
class Text(String):
    pass

class TextProperty(_m.Property):
    type = Text

serialize(_m.Key, str, _m.Key)

class Ref(_m.Key):

    DERIVATIVES = weakref.WeakValueDictionary()

    @classmethod
    def derive(cls, Type):
        name = '%s.%s' % (cls.__name__, Type.__name__)

        probe = cls.DERIVATIVES.get(name)
        if probe is None:
            probe = cls.DERIVATIVES[name] = serialize(cls._derive(name, Type))
        return probe

    @classmethod
    def _derive(cls, name, Type):
        return type(name, (cls,), {
            'INTERNED': weakref.WeakValueDictionary()
        })

    @classmethod
    def _dump(cls, val):
        return str(val)

    @classmethod
    def _load(cls, val):
        return cls(val)

class ReferenceProperty(_m.Property):
    type = Ref

    def __init__(self, kind, *args, **kwargs):
        self.kind = kind
        self.collection = kwargs.pop('collection', None)
        super(ReferenceProperty, self).__init__(*args, **kwargs)

    def __config__(self, cls, name):
        self.kind = _model(self.kind)
        if self.type is Ref:
            self.type = self.type.derive(_model(self.kind))
        super(ReferenceProperty, self).__config__(cls, name)
        self._collection_set()

    def load(self, obj, val):
        return _api.get(val)

    def _collection(self, obj):
        raise NotImplementedError('Make Query API.')

    def _collection_set(self):
        if not self.collection:
            return
        name = self._collection_name()
        if hasattr(cls, name):
            raise AttributeError('%r: %s.%s already exists.' % (
                self, self.model.__name__, name
            ))
        setattr(cls, name, property(self._collection))

    def _collection_name(self):
        if isinstance(self.collection, basestring):
            return self.collection
        return '%s_set' % self.model.__name__.lower()

def _model(obj):
    return _m.model(obj) if isinstance(obj, basestring) else obj


### Containers

class Directory(_stm.omap):
    type = (str, object)

    INTERNED = weakref.WeakValueDictionary()

    @classmethod
    def derive(cls, (Key, Value)):
        name = '%s.%s.%s' % (cls.__name__, Key.__name__, Value.__name__)

        probe = cls.INTERNED.get(name)
        if probe is None:
            probe = cls.INTERNED[name] = serialize(cls._derive(name, Key, Value))
        return probe

    @classmethod
    def _derive(cls, name, Key, Value):
        bases = (RefValues, cls) if issubclass(Value, _m.Model) else (cls,)
        return type(name, bases, {
            'type': (Key, Value),
        })

    @classmethod
    def __adapt__(cls, val):
        if (isinstance(val, Directory)
            and issubclass(val.type[0], self.type[0])
            and issubclass(val.type[1], self.type[1])):
            return cls(val)
        return cls(cls._icast(items(val)))

    @classmethod
    def _icast(cls, seq):
        (Key, Value) = self.type
        return ((self._key(k, Key), self._value(v, Value)) for (k, v) in seq)

    @staticmethod
    def _key(key, Key):
        return Key(key)

    _value = staticmethod(adapt)

    ## Override methods that mutate the mapping with some type checks.

    @classmethod
    def fromkeys(cls, seq, value=None):
        (Key, Value) = self.type
        return super(Directory).fromkeys(
            (self._key(k, Key) for k in seq),
            self._value(value, Value)
        )

    def __setitem__(self, key, val):
        (Key, Value) = self.type
        super(Directory, self).__setitem__(
            self._key(key, Key),
            self._value(val, Value)
        )

    def setdefault(self, key, val):
        (Key, Value) = self.type
        return super(Directory, self).setdefault(
            self._key(key, Key),
            self._value(val, Value)
        )

    def update(self, seq=(), **kwargs):
        super(Directory, self).update(self._icast(chain_items(seq, kwargs)))

    ## Serialize

    @classmethod
    def _dump(cls, val):
        data = _stm.readable(val)
        return [[k, data[k]] for k in data]

    @classmethod
    def _load(cls, val):
        obj = object.__new__(cls)
        yield obj
        _stm.allocate(obj, cls.StateType(val))

class RefValues(object):

    @classmethod
    def _value(cls, val, Value):
        if isinstance(val, Value):
            return val.key
        key = adapt(val, _m.Key)
        if issubclass(key.model, Value):
            return key
        raise ValueError('Expected %r key, not %r key %r.' % (
            Value, key.model, key
        ))

    _deref = staticmethod(_api.get)

    def __repr__(self):
        return '%s([%s])' % (
            type(self).__name__,
            ', '.join(repr(i) for i in self.iteritems())
        )

    def __getitem__(self, key):
        return self._deref(_stm.readable(self)[key])

    def get(self, key, default=None):
        probe = _stm.readable(self).get(key)
        if probe is not None:
            probe = self._deref(probe)
        return default if probe is None else probe

    def items(self, *args):
        return list(self.iteritems(*args))

    def values(self, *args):
        return list(self.itervalues(*args))

    def iteritems(self, *args):
        return (
            (k, self._deref(v))
            for (k, v) in _stm.readable(self).iteritems(*args)
        )

    def itervalues(self, *args):
        return imap(self._deref, _stm.readable(self).itervalues(*args))

    def pop(self, key, default=None):
        probe = _stm.writable(self).pop(key, default)
        if probe is not default:
            probe = self._deref(v)
        return probe

    def popitem(self):
        return self._deref(self.writable(self).popitem())

class DirectoryProperty(_m.Property):
    type = Directory

    def __init__(self, kind, *args, **kwargs):
        self.type = self.type.derive((str, _model(kind)))
        super(DirectoryProperty, self).__init__(*args, **kwargs)

    def default_value(self, obj):
        if callable(self.default):
            return self.default(obj)
        return self.default or self.type()

