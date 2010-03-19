## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""models -- high-level persistence API"""

from __future__ import absolute_import
import re, time, weakref, base64, uuid
from md import abc
from md.prelude import *
from .interfaces import *
from . import stm

__all__ = ('Property', 'Model', 'Key', 'model')


### Properties

class PropertyType(type):

    def __new__(mcls, name, bases, attr):
        abstract = attr.setdefault('__abstract__', False)
        cls = type.__new__(mcls, name, bases, attr)
        if not abstract:
            cls.kind = classify(name, cls)
        return cls

@abc.implements(Descriptor)
class Property(object):
    """A Property describes a model attribute."""

    __metaclass__ = PropertyType
    __abstract__ = True
    __all__ = ('name', 'type', 'title', 'doc', 'default', 'required')

    type = object

    def __init__(self, title=None, default=None, required=False, doc=None):
        self.title = title
        self.required = required
        self.doc = doc
        self.default = default
        self._created = time.time()

    def __repr__(self):
        if hasattr(self, 'model'):
            return '<%s.%s>' % (self.model.kind, self.name)
        return '%s()' % type(self).__name__

    def __config__(self, model, name):
        self.model = model
        self.name = name

    def __dir__(self):
        return list(self.__all__)

    def __describe__(self, names=None):
        return ((n, getattr(self, n)) for n in names or self.__all__)

    def __get__(self, obj, cls):
        try:
            return self.load(obj, stm.partof(obj, stm.readable(obj)[self.name]))
        except KeyError:
            try:
                return self.default_value(obj)
            except stm.NeedsTransaction:
                raise AttributeError(
                    '%r must be initialized in a transaction.' % self
                )

    def __set__(self, obj, val):
        stm.writable(obj)[self.name] = self.dump(obj, self.validate(obj, val))

    def default_value(self, obj):
        if callable(self.default):
            return self.default(obj)
        return self.default

    def validate(self, obj, val):
        if self.required and self.empty(val):
            raise ValueError('%r is a required property' % self)

        if val is not None and not isinstance(val, self.type):
            val = self.adapt(obj, val)

        return val

    def empty(self, val):
        return not val

    def adapt(self, obj, val):
        try:
            return adapt(val, self.type)
        except AdaptationFailure:
            raise ValueError('%r: expected %r, got %r.' % (self, self.type, val))

    def load(self, obj, val):
        return val

    def dump(self, obj, val):
        return val

_RESERVED_WORDS = set(['key_name'])
RESERVED_PROPERTY_NAME = re.compile('^__.*__$');

def check_reserved_word(name):
    if RESERVED_PROPERTY_NAME.match(name):
        raise SyntaxError(
            "Cannot define property.  All names both beginning and "
            "ending with '__' are reserved.")

    if name in _RESERVED_WORDS:
        raise SyntaxError(
            "Cannot define property using reserved word '%(name)s'. "
            "If you would like to use this name in the datastore consider "
            "using a different name like %(name)s_ and adding "
            "name='%(name)s' to the parameter list of the property "
            "definition." % locals())

    return True

def valid_property(name, prop):
    return isinstance(prop, Property) and check_reserved_word(name)


### Models

class ModelType(type):

    def __new__(mcls, name, bases, attr):
        abstract = attr.setdefault('__abstract__', False)
        cls = type.__new__(mcls, name, bases, attr)
        if not abstract:
            cls.kind = classify(name, cls)
        ## Process properties after classification to allow
        ## self-referential properties.
        cls.__properties__ = def_props(bases, attr, cls.__config__)
        return cls

    def __call__(cls, *args, **kw):
        if cls.__abstract__:
            raise TypeError('This model is abstract: %r.' % cls)
        obj = allocate(cls, kw.pop('key_name', None), cls.StateType())
        if hasattr(cls, '__init__'):
            obj.__init__(*args, **kw)
        return obj

    def __config__(cls, name, prop):
        prop.__config__(cls, name)
        return prop

    def __dir__(cls):
        return properties(cls).keys()

    def __describe__(cls, names=None):
        props = properties(cls)
        if names:
            ((n, props[n]) for n in names)
        return props.iteritems()

def allocate(cls, id, state):
    return stm.allocate(stm.new(Key.make(cls.kind, id)), state)

def def_props(bases, attr, config):
    """Produce a fresh group of properties by merging properties in
    base classes together with newly declared property attributes.
    Configure each property once they are merged."""

    props = sorted_props(declare_props(base_props(bases), attr))
    for item in props.iteritems():
        config(*item)
    return props

def base_props(bases):
    """Merge properties declared in base classes together."""

    return reduce(
        merge_unique_prop,
        (p for b in bases
         if isinstance(b, ModelType)
         for p in properties(b).iteritems()),
        {}
    )

def merge_unique_prop(props, (name, prop)):
    """Merge a single property into a group of properties.  The
    property name must not be used yet."""

    if name in props:
        raise ValueError('Duplicate property name: %r.' % name)
    props[name] = prop
    return props

def declare_props(props, attr):
    """Merge newly declared properties into a group of previously
    declared properties."""

    return reduce(
        merge_prop,
        (i for i in attr.iteritems() if valid_property(*i)),
        props
    )

def merge_prop(props, (name, prop)):
    """Merge a single property into a group of properties.  If a
    property with the same name already exists, the new property must
    be a subtype of the existing property."""

    parent = props.get(name)
    if not parent:
        props[name] = prop
    elif isinstance(prop, type(parent)):
        ## Copy parent properties so the descriptor can be
        ## reconfigured for a new class.
        props[name] = copy.copy(prop)
    else:
        raise ValueError('Property %r shadows %r, but is not a subtype.' % (
            name, parent
        ))
    return props

def sorted_props(props):
    """Sort a group of properties by creation (declaration) time."""

    return omap(sorted(props.iteritems(), None, item_created))

def item_created((name, prop)):
    return prop._created

@abc.implements(PCursor)
class Model(object):
    __metaclass__ = ModelType
    __abstract__ = True

    def __init__(self, **kw):
        for (name, prop) in properties(self).iteritems():
            value = kw.pop(name) if name in kw else prop.default_value(self)
            setattr(self, name, value)
        if kw:
            self.update(kw)

    ## Cursor Interface

    StateType = tree

    # The key property is set by stm.new()
    __id__ = property(stm.pid)

    def __deepcopy__(self, memo):
        return self

    def __copy__(self):
        return allocate(type(self), None, stm.copy_state(readable(self)))

    ## Described Interface

    def __dir__(self):
        return readable(self).keys()

    def __describe__(self, names=None):
        names = names or stm.readable(self).iterkeys()
        return ((n, getattr(self, n)) for n in names)

    ## Model Interface

    def __repr__(self):
        return '%s(%s)' % (
            type(self).__name__,
            ', '.join(('%s=%r' % i) for i in describe(self))
        )

    def __hash__(self):
        return hash(self.__id__)

    def __eq__(self, obj):
        if isinstance(obj, PCursor):
            return self.__id__ == obj.__id__
        return NotImplemented

    def __ne__(self, obj):
        if isinstance(obj, PCursor):
            return not self == obj
        return NotImplemented

    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError(name)

        prop = properties(self).get(name)
        if prop is not None:
            return pop.__get__(self, type(self))

        try:
            return stm.readable(self)[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        if name.startswith('_'):
            return obj.__setattr__(self, name, value)

        prop = properties(self).get(name)
        if prop is not None:
            return prop.__set__(self, value)

        stm.writable(self)[name] = value

    def __delattr__(self, name):
        if name.startswith('_'):
            return object.__delattr__(self, name)
        elif name in properties(self):
            raise AttributeError(name)

        try:
            del stm.writable(self)[name]
        except KeyError:
            raise AttributeError(name)

    def update(self, seq=(), **kw):
        for (name, value) in chain_items(seq, kw):
            setattr(self, name, value)
        return self


### Key

class Key(object):
    """A Key identifies a model instance.

    >>> k1 = Key.make('Foo')
    >>> k2 = Key.make('Foo')
    >>> k3 = Key.make('Foo', 'bar'); k3
    Key('Rm9vAGJhcg')
    >>> k1 is not k2
    True
    >>> k2 is Key(str(k2))
    True
    >>> k3 is Key.make('Foo', 'bar')
    True
    >>> k3.kind
    'Foo'
    >>> k3.id
    'bar'
    """
    __slots__ = ('kind', 'id', '_encoded', '__weakref__')

    INTERNED = weakref.WeakValueDictionary()

    def __new__(cls, encoded):
        encoded = str(encoded)
        try:
            self = cls.INTERNED[encoded]
        except KeyError:
            self = cls.make(*cls._decode(encoded))
        return self

    def __repr__(self):
        return '%s(%r)' % (type(self).__name__, str(self))

    def __hash__(self):
        return hash(str(self))

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self

    def __str__(self):
        if self._encoded is None:
            self._encoded = self._encode(self.kind, self.id)
        return self._encoded

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

    @classmethod
    def __adapt__(cls, obj):
        if isinstance(obj, basestring):
            return cls(obj)
        elif isinstance(obj, Model):
            return obj.key

    @classmethod
    def make(cls, kind, id=None):
        self = object.__new__(cls)
        self.kind = kind
        self.id = id or uuid.uuid4().bytes
        self._encoded = None
        return cls.INTERNED.setdefault(str(self), self)

    ## FIXME! use binary Avro encoding instead of \x00 glue since the
    ## UUID bytes might contain a null.

    @staticmethod
    def _decode(enc):
        pad = len(enc) % 4
        enc = str(enc) + '=' * (4 - pad) if pad else enc
        data = base64.urlsafe_b64decode(enc)
        return data.split('\x00', 1)

    @staticmethod
    def _encode(*args):
        return base64.urlsafe_b64encode('\x00'.join(args)).rstrip('=')

    def model(self):
        return model(self.kind)


### Classification

MODELS = weakref.WeakValueDictionary()
PROPERTIES = weakref.WeakValueDictionary()

def classify(name, cls):
    if issubclass(cls, Property):
        return _classify(PROPERTIES, name, cls)
    return _classify(MODELS, name, cls)

def _classify(taxonomy, name, cls):
    probe = taxonomy.setdefault(name, cls)
    if probe is not cls:
        raise ValueError(
            '%r could not be defined because %r already exists: %r.' %
            (name, probe)
        )
    return name

def model(name):
    if isinstance(name, ModelType):
        return name
    try:
        return MODELS[name]
    except KeyError:
        raise ValueError('Undefined model: %r.' % name)
