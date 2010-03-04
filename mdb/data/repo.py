## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""repo -- versioned key/value datastore"""

from __future__ import absolute_import
import datetime, weakref, time
from md import abc, fluid
from . import store, yaml
from .prelude import *

__all__ = ('RepoErro', 'zipper')

class RepoError(store.StoreError):
    """General-case exception for this module."""

class TransactionError(RepoError):
    """Something is wrong with the transaction."""

class TransactionFailed(RepoError):
    """Another process committed a transaction while this one was
    being processed."""


### High-level interface

class zipper(object):
    """A manifest-driven versioning implementation.

    >>> zs = zipper(store.back.memory()).create().open()
    >>> zs
    zipper(...)
    >>> zs.transactionally(zs.update, a=1, b=2).items()
    tree([('a', 1), ('b', 2)])
    >>> zs.transactionally(zs.update, a=Deleted, b=3, c=4).items()
    tree([('b', 3), ('c', 4)])
    """

    HEAD = 'HEAD'

    def __init__(self, state, objects=None, marshall=yaml, author=None):
        self._state = state
        self._objects = store.back.static(state, marshall, 'objects')
        self.author = author or anonymous
        self.head = None

    def __repr__(self):
        return '%s(%r, %r)' % (type(self).__name__, self._state, self._objects)

    def __iter__(self):
        return iter(self._refs)

    def is_open(self):
        return bool(self.head)

    def open(self):
        if not self.is_open():
            self._open()
            head = self._state.get(self.HEAD)
            if head is Undefined:
                raise RepoError('Created %s first.' % self)
            self._move_head(head)
        return self

    def _open(self):
        self._state.open()
        self._objects.open()

    def close(self):
        if self.is_open():
            self.head = None
            self._state.close()
            self._objects.close()
        return self

    def create(self):
        if self.is_open():
            raise RepoError('Cannot create an open repository.')
        try:
            self._open()
            head = refput(self, empty_checkpoint(self, [empty_commit(self)]))
            self._state.add(self.HEAD, head)
            return self
        except store.NotStored:
            raise RepoError('Repository already exists.')

    def destroy(self):
        self._state.delete(self.HEAD)
        self.close()

    def get(self, key):
        addr = self._address(key)
        if addr is Undefined:
            return Undefined
        return self._get(addr)

    def mget(self, keys):
        amap = {}
        for key in keys:
            addr = self._address(key)
            if addr is Undefined:
                yield (key, Undefined)
                continue
            amap[addr] = key
        for (addr, value) in self._mget(amap):
            yield (amap[addr], value)

    def deref(self, ref):
        return self._get(ref.address)

    def mderef(self, refs):
        return ((sref(a), v) for (a, v) in self._mget(r.address for r in refs))

    def put(self, value):
        (addr, value) = self._objects.put(value)
        return (sref(addr), value)

    def mput(self, values):
        return ((sref(a), v) for (a, v) in self._objects.mput(values))

    def transactionally(self, proc, *args, **kw):
        (head, token) = self._state.gets(self.HEAD)

        check = proc(*args, **kw)
        if not isinstance(check, checkpoint):
            raise TransactionError('A transaction must return a checkpoint.')

        new_head = refput(self, check)
        if new_head == head:
            return self

        try:
            self._state.cas(self.HEAD, new_head, token)
            ## FIXME: pass check in to prvent unnecessary lookups.
            self._move_head(new_head)
            return self
        except store.NotStored:
            raise TransactionFailed('Try again.')

    def update(self, seq=(), **kw):
        refs = ((k, reference(self, v)) for (k, v) in chain_items(seq, kw))
        return next_checkpoint(self, make_changeset(self._changes, refs))

    def items(self):
        return tree(self.iteritems())

    def iteritems(self):
        amap = dict((r.address, k) for (k, r) in self._refs.iteritems())
        return ((amap[a], v) for (a, v) in self._mget(amap))

    def _move_head(self, head):
        assert isinstance(head, sref), 'Expected sref, got %r.' % head
        if head != self.head:
            self.head = head
            self._refs = self._rebuild_index()
            return True
        return False

    def _rebuild_index(self):
        self._manifest = last_manifest(self)
        self._changes = last_changeset(self)
        return working(self._changes, self._manifest)

    def _ref(self, key):
        return self._refs.get(key)

    def _address(self, key):
        probe = self._refs.get(key)
        return probe and probe.address

    def _get(self, address):
        return self._objects.get(address)

    def _mget(self, addresses):
        return self._objects.mget(addresses)


### Marshalling

BUILTIN = '.'

def represent(*args, **kw):
    return yaml.represent(*args, **kw)

def construct(*args, **kw):
    return yaml.construct(*args, **kw)

def constant(name):
    """Declare a constant and register it for marshalling in the
    "built-in" namespace.

    >>> foo = constant('foo')
    >>> data = yaml.dumps(foo); data
    "!!.foo ''\\n"
    >>> yaml.loads(data)
    <foo>
    """

    value = sentinal('<%s>' % name)

    @represent(name, type(value), ns=BUILTIN)
    def repr_const(_):
        return ''

    @construct(name, ns=BUILTIN)
    def make_const(_):
        return value

    return value

def struct(cls):
    """Marshall a named tuple in the "built-in" namespace.

    >>> @struct
    ... class foo(namedtuple('foo', 'a b')):
    ...     pass
    >>> data = yaml.dumps(foo(1, 2)); data
    '!!.foo [1, 2]\\n'
    >>> yaml.loads(data)
    foo(a=1, b=2)
    """

    name = cls.__name__

    @represent(name, cls, ns=BUILTIN)
    def repr_struct(value):
        return list(value)

    @construct(name, ns=BUILTIN)
    def make_struct(value):
        return cls(*value)

    return cls


### History

## A manifest is a mapping of <logical-key, static-reference> items.
## The logical-key is a key in the mutable keyspace.  A
## static-reference is some object that refers to an object in the
## static (write-only) keyspace.
manifest = tree

## A changeset is like a manifest, but the value may indicate a
## deleted or conflicted state.
changeset = tree

class Reference(object):
    __metaclass__ = abc.ABCMeta

@struct
@abc.implements(Reference)
class sref(namedtuple('sref', 'address')):
    """A reference to a value in the static keyspace."""

    # __slots__ = ('__weakref__', )
    # INTERNED = weakref.WeakValueDictionary()

    # def __new__(cls, address):
    #     obj = cls.INTERNED.get(address)
    #     if obj is None:
    #         value = cls.__bases__[0].__new__(cls, address)
    #         obj = cls.INTERNED.setdefault(address, value)
    #     return obj

    def __hash__(self):
        return hash(self.address)

@struct
class commit(namedtuple('commit', 'author when message changes prev')):
    """A commit history is a linked list of commits.  An individual
    commit records a manifest (i.e. changes)"""

    __slots__ = ()

    def __repr__(self):
        return '<%s %s on %s: %r>' % (
            type(self).__name__,
            self.author,
            self.when,
            self.message
        )

    @property
    def date(self):
        return datetime.fromtimestamp(self.when)

@struct
class checkpoint(namedtuple('checkpoint', 'author when message changes commits prev')):
    __slots__ = ()

    def __repr__(self):
        return '<%s %s on %s: %r>' % (
            type(self).__name__,
            self.author,
            self.when,
            self.message
        )

    @property
    def date(self):
        return datetime.fromtimestamp(self.when)

def anonymous():
    """zippers optionally accept a procedure that returns the email
    address of the current user.  This is the default."""

    return 'Anonymous <nobody@example.net>'

def now():
    """The default method for determining the time a commit is
    made."""

    return time.time()

## The commit message is dynamically parameterized.
MESSAGE = fluid.cell(None, type=fluid.private)
message = fluid.accessor(MESSAGE)


### Working Manifest

Deleted = constant('deleted')
abc.implements(Reference)(type(Deleted))
Done = sentinal('<done>')

@abc.implements(Tree)
class working(object):
    """A logical "working" manifest generated from shadowing a real
    manifest with some changes.

    >>> manifest = tree(a=1, b=2)
    >>> changes = tree(a=3, b=Deleted, c=4)
    >>> index = working(changes, manifest)
    >>> index['a']
    3
    >>> index.get('b')
    <undefined>
    >>> index.items()
    [('a', 3), ('c', 4)]
    """

    __slots__ = ('_changes', '_manifest')

    def __init__(self, changes, manifest):
        self._changes = changes
        self._manifest = manifest

    def __nonzero__(self):
        return any(self.iteritems())

    def __len__(self):
        return sum(1 for _ in self.iteritems())

    def __contains__(self, key):
        return self.get(key) is not Undefined

    def __getitem__(self, key):
        value = self.get(key)
        if value is Undefined:
            raise KeyError(key)
        return value

    def __iter__(self):
        return self.iterkeys()

    def get(self, key, default=Undefined):
        try:
            value = self._changes[key]
        except KeyError:
            value = self._manifest.get(key, default)
        return default if value is Deleted else value

    def iteritems(self):
        return (
            i for i in tree_merge(self._changes, self._manifest)
            if i[1] is not Deleted
        )

    def items(self):
        return list(self.iteritems())

    def iterkeys(self):
        return keys(self.iteritems())

    def keys(self):
        return list(self.iterkeys())

    def itervalues(self):
        return values(self.iteritems())

    def values(self):
        return list(self.itervalues())

def tree_merge(mine, yours):
    """Merge two trees together; mine wins.

    Trees are kept in key-order.  Iterate over each, advancing
    whichever sequence has the lower key.

    >>> t1 = tree(a=1, b=2, z=3)
    >>> t2 = tree(a=4, c=5)
    >>> list(tree_merge(t1, t2))
    [('a', 1), ('b', 2), ('c', 5), ('z', 3)]
    """

    mine = items(mine); me = next(mine, Done)
    yours = items(yours); you = next(yours, Done)

    while me is not Done and you is not Done:
        mk = me[0]; yk = you[0]

        if mk == yk:
            yield me
            me = next(mine, Done); you = next(yours, Done)
        elif mk < yk:
            yield me
            me = next(mine, Done)
        else:
            yield you
            you = next(yours, Done)

    if me is not Done:
        (item, rest) = (me, mine)
    elif you is not Done:
        (item, rest) = (you, yours)
    else:
        return

    yield item
    for item in rest:
        yield item


### Operations

def zop(proc):
    if not __debug__:
        return proc
    @wraps(proc)
    def internal(zs, *args, **kwargs):
        assert zs.is_open(), 'Not open: %r.' % zs
        return proc(zs, *args, **kwargs)
    return internal

@zop
def last_checkpoint(zs):
    return zs.deref(zs.head)

@zop
def last_commit(zs):
    check = last_checkpoint(zs)
    if not (check and check.commits):
        return Undefined
    return zs.deref(check.commits[0])

@zop
def last_changeset(zs):
    check = last_checkpoint(zs)
    return zs.deref(check.changes) if check else changeset()

@zop
def last_manifest(zs):
    commit = last_commit(zs)
    return zs.deref(commit.changes) if commit else manifest()

def reference(zs, obj):
    return obj if isinstance(obj, Reference) else refput(zs, obj)

def refput(zs, obj):
    return zs.put(obj)[0]

def make_checkpoint(zs, changes, commits, *prev):
    return checkpoint(
        zs.author(),
        now(),
        message(),
        reference(zs, changes),
        list(reference(zs, c) for c in commits),
        list(reference(zs, p) for p in prev)
    )

def make_commit(zs, changes, *prev):
    return commit(
        zs.author(),
        now(),
        message(),
        reference(zs, changes),
        list(reference(zs, p) for p in prev)
    )

def empty_commit(zs, *prev):
    return make_commit(zs, manifest(), *prev)

def empty_checkpoint(zs, commits, *prev):
    return make_checkpoint(zs, changeset(), commits, *prev)

def make_changeset(orig, *args, **kwargs):
    return update(orig.copy(), *args, **kwargs)

@zop
def next_checkpoint(zs, changes):
    check = last_checkpoint(zs)
    return make_checkpoint(zs, changes, check.commits, check)
