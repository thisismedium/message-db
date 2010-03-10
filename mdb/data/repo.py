## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""repo -- versioned key/value datastore"""

from __future__ import absolute_import
import datetime, weakref, time
from md.prelude import *
from md import abc, fluid
from . import store, yaml

__all__ = ('RepoErro', 'TransactionError', 'TransactionFailed', 'zipper')

class RepoError(store.StoreError):
    """General-case exception for this module."""

class TransactionError(RepoError):
    """Something is wrong with the transaction."""

class TransactionFailed(RepoError):
    """Another process committed a transaction while this one was
    being processed."""


### High-level interface

class zipper(object):
    """A manifest-driven versioned keyspace.  Logical keys are mapped
    to static references in a write-once keyspace through a manifest.

    As the logical keyspace is "mutated", the manifest is updated.
    The updated manifest is tracked through a linked list of
    checkpoints and commits.  A commit encapsulates an entire
    manifest.  A checkpoint encapsulates a delta against a
    commit/manifest.

    Checkpoints are private to the branch and may contain
    "intermediate" references such as Deleted sentinal or Conflict
    markers.  Commits are public; they may be cloned into new branches
    or merged into other branches.

    >>> zs = zipper(store.back.memory()).create().open()
    >>> zs
    zipper(...)
    >>> zs.transactionally(zs.checkpoint, a=1, b=2).items()
    tree([('a', 1), ('b', 2)])
    >>> zs.transactionally(zs.checkpoint, a=Deleted, b=3, c=4).items()
    tree([('b', 3), ('c', 4)])
    >>> print '\\n'.join(repr(c) for c in checkpoints(zs))
    <checkpoint Anonymous <nobody@example.net> ...>
    <checkpoint Anonymous <nobody@example.net> ...>
    <checkpoint Anonymous <nobody@example.net> ...>

    >>> zs.transactionally(zs.commit, d=5).items()
    tree([('b', 3), ('c', 4), ('d', 5)])
    >>> print '\\n'.join(repr(c) for c in commits(zs))
    <commit Anonymous <nobody@example.net> ...>
    <commit Anonymous <nobody@example.net> ...>
    >>> print '\\n'.join(repr(c) for c in checkpoints(zs))
    <checkpoint Anonymous <nobody@example.net> ...>
    """

    HEAD = 'HEAD'

    def __init__(self, state, objects=None, marshall=yaml, author=None):
        self._state = store.back.prefixed(state, '', marshall)
        self._objects = store.back.static(state, marshall, 'objects/')
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
            head = refput(self, empty_checkpoint(self, self._create()))
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

    def checkpoint(self, seq=(), **kw):
        refs = mref(self, chain_items(seq, kw))
        changes = make_changeset(self._manifest, self._changes, refs)
        return next_checkpoint(self, changes)

    def commit(self, seq=(), **kw):
        refs = mref(self, chain_items(seq, kw))
        manifest = make_manifest(self._manifest, self._changes, refs)
        return empty_checkpoint(self, next_commit(self, manifest))

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

    def _create(self):
        return empty_commit(self)

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

class repo(zipper):
    """A repository tracks branches over a common static space.  Each
    branch is given a private keyspace for state.  The keyspace of the
    "real" backing store is partioned in this way:

       repo state:   ...
       branch state: refs/{{branch}}/...
       static space: objects/...

    >>> r = repo(store.back.memory()).create().open()
    >>> b = r.branch('foo').create().open()
    >>> print '\\n'.join(str(c) for c in commits(r))
    <commit Anonymous <nobody@example.net> ...: "Add branch 'foo'.">
    <commit Anonymous <nobody@example.net> ...>
    >>> b.transactionally(b.checkpoint, a='a-value').items()
    tree([('a', 'a-value')])
    """

    def branch(self, name):
        name = str(name)
        state = store.back.prefixed(self._state, self._qualify(name))
        return branch(name, self, state, self._objects)

    def branches(self):
        return list(self.branch(n) for n in self._branches())

    def add(self, branch):
        return self.transactionally(self._add, branch)

    def remove(self, branch):
        return self.transactionally(self._remove, branch)

    def _create(self):
        return init_commit(self, branches=tree())

    def _branches(self):
        return self.get('branches')

    def _add(self, branch):
        with message('Add branch %r.' % branch.name):
            branches = self._branches().copy()
            branches[branch.name] = self._config(branch)
            return self.commit(branches=branches)

    def _remove(self, branch):
        with message('Remove branch %r.' % branch.name):
            branches = self._branches().copy()
            del branches[branch.name]
            return self.commit(branches=branches)

    def _qualify(self, name):
        if not name.startswith('refs/'):
            name = 'refs/%s/' % name.strip('/')
        return name

    def _config(self, branch):
        return tree(owner=self.author(), config=tree())

class branch(zipper):
    """A branch keeps state in a private keyspace and uses a common
    static space shared among all branches in a repository."""

    def __init__(self, name, repo, *args, **kw):
        super(branch, self).__init__(*args, **kw)
        self.name = name
        self.repo = repo

    def __repr__(self):
        return '%s(%r, %r)' % (type(self).__name__, self.name, self.repo)

    def create(self):
        self.repo.add(super(branch, self).create())
        return self


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

class StructureType(StructType):
    def __new__(mcls, name, bases, attr):
        cls = StructType.__new__(mcls, name, bases, attr)

        @represent(name, cls, ns=BUILTIN)
        def repr(value):
            return list(value)

        @construct(name, ns=BUILTIN)
        def make(value):
            return cls(*value)

        return cls

def structure(name, *args, **kw):
    """Marshall a named tuple in the "built-in" namespace.

    >>> class foo(structure('foo', 'a b')):
    ...     pass
    >>> data = yaml.dumps(foo(1, 2)); data
    '!!.foo [1, 2]\\n'
    >>> yaml.loads(data)
    foo(a=1, b=2)
    """

    kw.setdefault('metaclass', StructureType)
    return struct(name, *args, **kw)

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

class Static(Reference):
    pass

@abc.implements(Static)
class sref(structure('_sref', 'address', weak=True)):
    """A reference to a value in the static keyspace."""

    INTERNED = weakref.WeakValueDictionary()

    def __new__(cls, address):
        obj = cls.INTERNED.get(address)
        if obj is None:
            value = cls.__bases__[0].__new__(cls, address)
            obj = cls.INTERNED.setdefault(address, value)
        return obj

    def __hash__(self):
        return hash(self.address)

class commit(structure('commit', 'author when message changes prev')):
    """A commit history is a linked list of commits.  An individual
    commit records a manifest (i.e. changes) that keys in a logical
    keyspace to static identifiers in a write-once keyspace."""

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

class checkpoint(structure('checkpoint', 'author when message changes commits prev')):
    """A checkpoint is a partial manifest that represents changes
    since the last commit.  The changeset may contain non-static
    references such as Deleted or Conflict sentinals.  A checkpoint
    may be made against multiple commits; this is a merge."""

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

## Repository Information

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

def checkpoints(zs):
    """Checkpoints since the last commit."""

    return (c for (r, c) in ancestors(zs, [zs.head]))

def commits(zs):
    """Iterate over the commit history (most recent commits are
    first)."""

    check = last_checkpoint(zs)
    if not (check and check.commits):
        return ()
    return (c for (r, c) in ancestors(zs, check.commits))

@zop
def ancestors(zs, refs):
    """Breadth-first traversal over unique ancestors."""

    queue = deque(zs.mderef(refs))
    visited = set()

    while queue:
        (ref, commit) = queue.popleft()
        if ref.address in visited:
            continue
        else:
            queue.extend(zs.mderef(commit.prev))
            yield (ref, commit)
            visited.add(ref.address)

## Repository Manipulation

def ref(zs, obj):
    return obj if isinstance(obj, Reference) else refput(zs, obj)

def mref(zs, pairs):
    ## FIXME: do performance testing against
    ##  return ((k, ref(zs, v)) for (k, v) in pairs)

    vmap = {}
    for item in items(pairs):
        if isinstance(item[1], Reference):
            yield item
        else:
            vmap[id(item[1])] = item

    for (ref, val) in zs.mput(v for (_, v) in vmap.itervalues()):
        (key, _) = vmap[id(val)]
        yield (key, ref)

def refput(zs, obj):
    return zs.put(obj)[0]

def make_checkpoint(zs, changes, commits, *prev):
    return checkpoint(
        zs.author(),
        now(),
        message(),
        ref(zs, changes),
        list(ref(zs, c) for c in commits),
        list(ref(zs, p) for p in prev)
    )

def make_commit(zs, changes, *prev):
    return commit(
        zs.author(),
        now(),
        message(),
        ref(zs, changes),
        list(ref(zs, p) for p in prev)
    )

def empty_commit(zs, *prev):
    return make_commit(zs, manifest(), *prev)

def empty_checkpoint(zs, commit):
    return make_checkpoint(zs, changeset(), [commit])

def init_commit(zs, **changes):
    return make_commit(zs, manifest(mref(zs, changes)))

def make_changeset(manifest, changes, updates):
    changes = changes.copy()
    for (key, ref) in items(updates):
        if ref is Deleted and key not in manifest:
            changes.pop(key, None)
        else:
            changes[key] = ref
    return changes

def make_manifest(manifest, changes, updates):
    changes = make_changeset(manifest, changes, updates)
    manifest = manifest.copy()
    for (key, ref) in changes.iteritems():
        if ref is Deleted:
            del manifest[key]
        else:
            assert isinstance(ref, Static), 'Not static: <%r, %r>.' % (key, ref)
            manifest[key] = ref
    return manifest

@zop
def next_checkpoint(zs, changes):
    check = last_checkpoint(zs)
    return make_checkpoint(zs, changes, check.commits, check)

@zop
def next_commit(zs, changes):
    check = last_checkpoint(zs)
    return make_commit(zs, changes, *check.commits)
