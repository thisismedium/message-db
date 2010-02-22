from __future__ import absolute_import
from . import tree, compiler, datastore as ds

__all__ = ('query', 'Query', 'PathQuery')

def query(expr):
    if isinstance(expr, basestring):
        return PathQuery(expr)
    return Query(expr)

def PathQuery(expr):
    return compiler.evaluate(expr)

class Query(object):
    __all__ = ('items', )

    def __init__(self, items):
        self.items = tree.NodeSet(items)

    def __iter__(self):
        return self.items()

    def filter(self, proc):
        return type(self)(tree.Reduce(fn.partial(it.ifilter, proc), self.items))

    def map(self, proc):
        return type(self)(tree.Map(proc, self.items))

    def axis(self, axis, test=None):
        return type(self)(axis(self.test(test))(self.items))

    def test(self, test):
        if test and isinstance(test, basestring) and test[0].isupper():
            return ds.kind(test)
        return test

    def self(this, *args):
        return self.axis(tree.self, *args)

    def children(self, *args):
        return self.axis(tree.child, *args)

    def find(self, *args):
        return self.axis(tree.descendant, *args)

    def parent(self, *args):
        return self.axis(tree.parent, *args)

    def parents(self, *args):
        return self.axis(tree.ancestor, *args)

    def prevAll(self, *args):
        return self.axis(tree.preceeding, *args)

    def nextAll(self, *args):
        return self.axis(tree.following, *args)

