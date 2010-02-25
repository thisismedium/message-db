## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

from __future__ import absolute_import
from ..query import tree, compiler as comp, parse
from . import query_ast, ops, datastore as ds

__all__ = ('query', 'Query', 'PathQuery')

def query(expr):
    """Begin a procedural query or compile a path query.

    For example, this will compile a path query into and object that
    can be called against some context item:

       db.query('//Page')(db.root())

    These are two ways to create a similar query using a Query object:

       db.query(db.root()).children('news')
       db.query().children('news')(db.root())

    """

    if isinstance(expr, basestring):
        return PathQuery(expr)
    return Query(expr)


### Procedural Query API

class Query(object):
    """Incrementally create a query."""

    __all__ = ('_items', '_steps', '_query')

    def __init__(self, items=None, _steps=None):
        self._items = items and tree.sequence(items)
        self._steps = _steps or []
        self._query = None

    def __iter__(self):
        return iter(self.query(self._items))

    def __call__(self, items):
        return type(self)(items, self._steps)

    @property
    def query(self):
        if self._query is None:
            self._query = tree.Path(tree.steps(*self._steps))
        return self._query

    def filter(self, proc):
        return self._step(tree.filter, proc)

    def self(this, *args):
        return self._axis(tree.self, *args)

    def children(self, *args):
        return self._axis(tree.child, *args)

    def find(self, *args):
        return self._axis(tree.descendant, *args)

    def parent(self, *args):
        return self._axis(tree.parent, *args)

    def parents(self, *args):
        return self._axis(tree.ancestor, *args)

    def prevAll(self, *args):
        return self._axis(tree.preceeding, *args)

    def nextAll(self, *args):
        return self._axis(tree.following, *args)

    def _step(self, step, expr):
        return type(self)(self._items, self._steps + [step(expr)])

    def _axis(self, axis, test=None):
        return self._step(axis, self._test(test))

    def _test(self, test):
        if test and isinstance(test, basestring) and test[0].isupper():
            return ds.kind(test)
        return test


### Compile Path Queries

PathQuery = comp.Evaluator(
    parse.PathParser(query_ast),
    comp.builtin(comp.use(ops))
)
