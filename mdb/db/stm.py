## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""stm -- transactional memory integration"""

from __future__ import absolute_import
import threading
from md import stm as _stm, abc as _abc, collections as _coll
from md.stm import *
from .. import data as _data
from . import interfaces as _i

__all__ = (
    'initialize', 'transaction', 'rollback', 'commit', 'abort',
    'state', 'changed'
)


### Override some of the STM interface

def new(key):
    """Create a new cursor for a key."""

    obj = object.__new__(key.model())
    object.__setattr__(obj, 'key', key)
    return obj

def allocated(cls, state):
    return allocate(object.__new__(cls), state)

def pid(cursor):
    """Return the persistent id of a persistent cursor."""

    try:
        return cursor.key
    except AttributeError:
        name = type(cursor).__name__
        if isinstance(cursor, _i.PCursor):
            raise AttributeError('%r cursor (%d) is unallocated.' % (
                name,
                id(cursor)
            ))
        raise AttributeError('%r cursor is not a persistent cursor.' % name)

def initialize(zs):
    """Initialize the transactional memory with a zipper."""

    _stm.initialize(_memory(zs.open()))

state = _stm.readable


### Transactional Memory implementation

@_abc.implements(_stm.Journal)
class _journal(object):
    """A journal tracks changes that are about to be transactionally
    committed to a memory."""

    LogType = _stm.log.log

    def __init__(self, name, source, mark):
        self.name = name
        self.source = source
        self.mark = mark
        self._read_log = self.LogType()
        self._write_log = self.LogType()

    def __repr__(self):
        return '%s(%r, %r)' % (type(self).__name__, self.name, self.source)

    def make_journal(self, name):
        return type(self)(name, self, self._mark)

    def allocate(self, cursor, state):
        self.original_state(cursor) # Transactional integrity.
        self._write_log.allocate(cursor, state)
        return cursor

    def readable_state(self, cursor):
        try:
            return self._write_log[cursor]
        except KeyError:
            return self.original_state(cursor)

    def original_state(self, cursor):
        try:
            return self._read_log[cursor]
        except KeyError:
            state = _stm.good(self.source.readable_state, cursor, _stm.Inserted)
            self._read_log[cursor] = state
            return state

    def writable_state(self, cursor):
        try:
            return self._write_log[cursor]
        except KeyError:
            state = stm.copy_state(self.original_state(cursor))
            self._write_log[cursor] = state
            return state

    def delete_state(self, cursor):
        self.original_state(cursor) # Transactional integrity.
        self._write_log[cursor] = Deleted

    def rollback_state(self, cursor):
        self.write_log.pop(cursor, None)

    def commit_transaction(self, trans):
        self._write_log.update(trans.written())

    def original(self):
        return iter(self._read_log)

    def written(self):
        return iter(self._write_log)

    def changed(self):
        return (
            _stm.change(c, self._read_log[c], v)
            for (c, v) in self._write_log
        )

    ## Dereference keys

    def get(self, key):
        (cursor, _) = self._fetch(key)
        return cursor

    def _fetch(self, key):
        probe = self._read_log.entry(key)
        if probe is not None:
            return probe
        probe = self.source._fetch(key)
        if probe.cursor is not None:
            self._read_log[probe.cursor] = probe.state
        return probe

    def mget(self, keys):
        return (c for (c, _) in self._mfetch(keys))

    def _mfetch(self, keys):
        need = []

        for key in keys:
            probe = self._read_log.entry(key)
            if probe is not None:
                yield probe
            else:
                need.append(key)

        if not need:
            return

        for item in self.source._mfetch(need):
            (cursor, state) = item
            if cursor is not None:
                self._read_log[cursor] = state
            yield item

def _needs_transaction(*args, **kw):
    raise _stm.NeedsTransaction(
        'This operation needs to be run in a transaction.'
    )

@_abc.implements(_stm.Memory)
class _memory(object):
    """A memory supports read and transactional-update operations
    against a zipper."""

    JournalType = _journal
    LogType = _stm.log.weaklog

    NO_ENTRY = _stm.log.entry(None, None)

    def __init__(self, zs):
        self._active = self.LogType()
        self._zs = zs
        self.name = repr(zs)
        self.source = None
        self._lock = threading.RLock()

    def __repr__(self):
        return '%s(%s)' % (type(self).__name__, self.name)

    def make_journal(self, name):
        return self.JournalType(name, self, self._zs.begin_transaction())

    def allocate(self, cursor, state):
        self._active.allocate(cursor, state)
        return cursor

    def readable_state(self, cursor):
        return self._active[cursor]

    original_state = readable_state

    def commit_transaction(self, trans):
        with self._lock:
            self._end(trans, self._verify(trans))

    writable_state = _needs_transaction
    delete_state = _needs_transaction
    rollback_state = _needs_transaction
    original = _needs_transaction
    changed = _needs_transaction

    ## Dereference keys

    def get(self, key):
        (cursor, _) = self._fetch(key)
        return cursor

    def _fetch(self, key):
        probe = self._active.entry(key)
        if probe is not None:
            return probe

        state = self._zs.get(str(key))
        if state is None:
            return self.NO_ENTRY
        return (self.allocate(new(key), state), state)

    def mget(self, keys):
        return (c for (c, _) in self._mfetch(keys))

    def _mfetch(self, keys):
        keymap = {}

        for key in keys:
            probe = self._active.entry(key)
            if probe is not None:
                yield probe
            else:
                keymap[str(key)] = key

        if not keymap:
            return

        alloc = self._active.allocate
        for (key, state) in self._zs.mget(keymap):
            if state is None:
                yield self.NO_ENTRY
            else:
                yield (self.allocate(new(keymap[key]), state), state)

    ## Commit

    def _verify(self, trans):
        (head, _) = trans.mark
        if head != self._zs.head:
            _stm.verify_read(self._active, trans.original())
            return _stm.verify_write(self._active, trans.changed())
        else:
            return _stm.unverified_write(trans.changed())

    def _end(self, trans, delta):
        with _data.message(trans.name):
            check = self._zs.checkpoint(self._persist(delta))
        if self._zs.end_transaction(trans.mark, check):
            self._commit(delta)

    def _persist(self, delta):
        for (cursor, state) in delta:
            if isinstance(cursor, _i.PCursor):
                if state is _stm.Deleted:
                    state = _data.Deleted
                yield (str(cursor.key), state)

    def _commit(self, delta):
        for (cursor, state) in delta:
            if state is _stm.Deleted:
                self._active.pop(cursor, None)
            else:
                self._active[cursor] = state

