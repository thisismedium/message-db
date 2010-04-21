## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""api -- high-level operations"""

from __future__ import absolute_import
from md.prelude import *
from md import fluid
from .. import avro, data

__all__ = (
    'RepoError', 'repository', 'repository_transaction', 'source', 'use',
    'branches', 'make_branch', 'open_branch', 'get_branch', 'save_branch',
    'remove_branch',
    'get', 'find', 'new', 'update', 'delete', 'delta'
)

RepoError = data.RepoError

## The current branch and repository are accessible in the dynamic
## context.  They can be set globally using the init() method.

REPOSITORY = fluid.cell(None, type=fluid.acquired)
repository = fluid.accessor(REPOSITORY)

SOURCE = fluid.cell(None, type=fluid.acquired)
source = fluid.accessor(SOURCE)

def init_api(zs, created=False):
    """Set the global branch; this should be done near the beginning
    of a program.  See load.init()."""

    REPOSITORY.set(zs.open())
    if created:
        with repository_transaction('Make initial branches.'):
            make_branch('live', publish='')
            make_branch('staging', publish='live')

    use('staging')

    return zs

def use(name):
    branch = open_branch(name)
    SOURCE.set(branch)
    return branch

def get(key, zs=None):
    """Resolve a key or sequence of keys.  If a key cannot be
    resolved, Undefined is returned.  Currently, the result of getting
    sequence of keys is not guaranteed to keep its objects in the
    key-order; this may be changed in the future."""

    if key is None or isinstance(key, data.Value):
        return key
    elif isinstance(key, (basestring, data.Key)):
        return best(zs).get(data.Key(key))
    else:
        return best(zs).mget(data.Key(k) for k in key)

def find(cls, zs=None):
    return best(zs).find(cls)

def best(zs):
    src = source()
    if zs is None:
        return src
    if isinstance(src, _Delta) and src._zs == zs:
        return src
    return zs


### Branches

## Branch descriptors are stored in the repository.  They behave
## similarly to Users and other content types.  Making new branches is
## the exception because additional work as to be done to create() the
## branch before a descriptor is made.

def branches():
    return repository().branches()

def make_branch(name, force=True, owner=None, **kw):
    zs = repository()
    if owner and not isinstance(owner, basestring):
        owner = owner.name
    setdefault(kw, publish='staging', owner=owner)
    return update(zs.configure(zs.branch(name).create(force=force), **kw))

def open_branch(name):
    return repository().branch(name).open()

def get_branch(name):
    return repository().branch(name).config

def save_branch(config, *args, **kw):
    return update(config.update(*args, **kw))

def remove_branch(config):
    return delete(config.key)


### Changes

def new(*args):
    return source().new(*args)

def update(*args):
    return source().changed(*args)

def delete(key):
    """A low-level method for deleting the item associated with a key.
    If you want to delete an item in the content tree, use
    tree.remove() instead."""

    if isinstance(key, data.Value):
        key = key.key
    if isinstance(key, (basestring, data.Key)):
        source().delete(key)
    else:
        for k in key: source().delete(k)

## A source is updated transactionally by passing it a changeset.  The
## delta() method establishes a "shadow" source in its dynamic extend
## that collects changes made to the content tree.  At the end of the
## context, a method like checkpoint() can be used to write the
## changes to the source.

@contextmanager
def delta(message, zs=None):
    """Replace the _Branch with a _Delta in the calling context.
    Methods that use branch() (such as get()) will use this delta
    instead."""

    delta = _Delta(message, best(zs))
    with source(delta):
        yield delta

@contextmanager
def repository_transaction(message):
    with delta(message, repository()) as d:
        yield
        try:
            d.commit()
        except Exception:
            print 'FAILED', d._data
            raise

class _Delta(object):
    """A delta is a set of changes about to be committed to a
    zipper."""

    def __init__(self, message, zs):
        self._message = message
        self._zs = zs
        self._data = {}
        self._mark = zs.begin_transaction()

    def new(self, cls, state):
        return self.changed(self._zs.new(cls, state))

    def changed(self, *objects):
        for obj in objects:
            self._data[obj.key] = obj
        return obj

    def get(self, key):
        if key in self._data:
            return self._data[key]
        return self._zs.get(key)

    def mget(self, keys):
        need = []

        for key in keys:
            probe = self._data.get(key, Undefined)
            if probe is not Undefined:
                yield probe
            else:
                need.append(key)

        if need:
            for obj in self._zs.mget(need):
                yield obj

    def delete(self, key):
        self._data[key] = Undefined

    def checkpoint(self):
        return self._end(self._zs.checkpoint)

    def commit(self):
        return self._end(self._zs.commit)

    def _end(self, method):
        with data.message(self._message):
            self._zs.end_transaction(self._mark, method(self._persist()))
        return self

    def _persist(self):
        delta = self._data
        for key in delta:
            if delta[key] is Undefined:
                delta[key] = data.Deleted
        return delta
