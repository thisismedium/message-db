## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""api -- high-level operations"""

from __future__ import absolute_import
from md.prelude import *
from md import fluid
from .. import avro, data
from . import _tree

__all__ = ('branch', 'init', 'get', 'find', 'new', 'update', 'delete', 'delta')

## The current branch is accessible in the dynamic context.  It can be
## set globally using the init() method.

BRANCH = fluid.cell(None, type=fluid.acquired)
branch = fluid.accessor(BRANCH)

def init(zs):
    """Set the global branch; this should be done near the beginning
    of a program."""

    BRANCH.set(_Branch(zs.open()))
    return zs

def get(key, zs=None):
    """Resolve a key or sequence of keys.  If a key cannot be
    resolved, Undefined is returned.  Currently, the result of getting
    sequence of keys is not guaranteed to keep its objects in the
    key-order; this may be changed in the future."""

    if key is None or isinstance(key, _tree.Content):
        return key
    elif isinstance(key, (basestring, _tree.Key)):
        return (zs or branch()).get(str(key))
    else:
        return (zs or branch()).mget(str(k) for k in key)

def find(cls):
    return branch().find(cls)


### Changes

def new(*args):
    return branch().new(*args)

def update(*args):
    return branch().changed(*args)

def delete(key):
    """A low-level method for deleting the item associated with a key.
    If you want to delete an item in the content tree, use
    tree.remove() instead."""

    if isinstance(key, _tree.Content):
        key = key.key
    if isinstance(key, (basestring, _tree.Key)):
        branch().delete(key)
    else:
        for k in key: branch().delete(k)

## A branch is updated transactionally by passing it a changeset.  The
## delta() method establishes a "shadow" branch in its dynamic extend
## that collects changes made to the content tree.  At the end of the
## context, the checkpoint() method can be used to write the changes
## to the branch.

@contextmanager
def delta(message, zs=None):
    """Replace the _Branch with a _Delta in the calling context.
    Methods that use branch() (such as get()) will use this delta
    instead."""

    delta = (zs or branch()).begin(message)
    with branch(delta):
        yield delta

class _Branch(object):
    """A _Branch is a thin wrapper that acts as an interface between
    this api and the data api."""

    def __init__(self, zs):
        self._zs = zs

    def get(self, key):
        val = self._zs.get(key)
        return val and val.update(_key=_tree.Key(key))

    def mget(self, keys):
        for (key, val) in self._zs.mget(keys):
            yield val and val.update(_key=_tree.Key(key))

    def begin(self, message):
        return _Delta(message, self, self._zs.begin_transaction())

    def checkpoint(self, mark, message, delta):
        with data.message(message):
            changes = self._persist(delta)
            self._zs.end_transaction(mark, self._zs.checkpoint(changes))
        return self

    def find(self, cls):
        return get(self._scan(cls), self)

    def _scan(self, cls):
        for key in self._zs:
            if issubclass(_tree.Key(key).type, cls):
                yield key

    def _persist(self, delta):
        for key in delta:
            if delta[key] is Undefined:
                delta[key] = data.Deleted
        return delta

class _Delta(object):
    """A delta is a set of changes about to be committed to a
    zipper."""

    def __init__(self, message, source, mark):
        self._message = message
        self._source = source
        self._data = {}
        self._mark = mark

    def new(self, cls, state):
        key = (state.pop('key', None)
               or _tree.Key.make(cls, state.pop('key_name', None)))
        return self.changed(cls(**state).update(_key=key))

    def changed(self, *objects):
        for obj in objects:
            self._data[str(obj.key)] = obj
        return obj

    def get(self, key):
        probe = self._data.get(key, Undefined)
        if probe is not Undefined:
            return probe
        return self._source.get(key)

    def mget(self, keys):
        need = []

        for key in keys:
            probe = self._data.get(key, Undefined)
            if probe is not Undefined:
                yield probe
            else:
                need.append(key)

        if need:
            for obj in self._source.mget(need):
                yield obj

    def delete(self, key):
        self._data[key] = Undefined

    def checkpoint(self):
        self._source.checkpoint(self._mark, self._message, self._data)
        return self
