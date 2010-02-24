## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""tree -- data tree interface"""

from __future__ import absolute_import
import collections as coll, functools as fn, itertools as it
from md import abc, fluid

__all__ = (
    'Node', 'InnerNode', 'leaf', 'collection', 'focus', 'index',
    'XPath', 'Sequence', 'sequence', 'path', 'filter', 'predicate',
    'self', 'parent', 'child', 'attribute', 'ancestor', 'ancestor_or_self',
    'descendant', 'descendant_or_self', 'following_sibling', 'following',
    'preceeding_sibling', 'preceeding'
)


### Abstract Interface

class Node(coll.Hashable):
    __slots__ = ()

    def __leaf__(self):
        return True

class InnerNode(Node, coll.Iterable, coll.Sized):
    __slots__ = ()

    def __leaf__(self):
        return False

    @abc.abstractmethod
    def before(self, child):
        """Return an iterator over the preceeding siblings of child."""

    @abc.abstractmethod
    def after(self, child):
        """Return an iterator over the following siblings of child."""

def leaf(obj):
    try:
        return obj.__leaf__()
    except AttributeError:
        return True


### Dynamic Context

COLLECTION = fluid.cell(())
collection = fluid.accessor(COLLECTION)

FOCUS = fluid.cell()
focus = fluid.accessor(FOCUS)

INDEX = fluid.cell()
index = fluid.accessor(INDEX)


### Top Level

def XPath(exprs):
    if isinstance(exprs, Sequence):
        def xpath(items):
            items = sequence(items)
            with collection(items):
                return expand(exprs, items)
    else:
        def xpath(items):
            items = sequence(items)
            with collection(items):
                return tuple(expand(e, items) for e in exprs)
    return xpath

def expand(expr, items):
    return unique(iter(items) if expr is None else focused(expr, items))

def focused(expr, items):
    for (index, focus) in enumerate(items):
        with fluid.let((INDEX, index), (FOCUS, focus)):
            for item in expr():
                yield item


### Tree Traversal

class Sequence(object):
    __slots__ = ('expr', )

    def __init__(self, items):
        if isinstance(items, coll.Iterator):
            items = list(items)
        if isinstance(items, (list, tuple)):
            self.expr = lambda: items
        elif callable(items):
            self.expr = items
        else:
            self.expr = lambda: [items]

    def __eq__(self, other):
        if isinstance(other, Sequence):
            return all(
                a == b for (a, b) in
                it.izip_longest(self, other, fillvalue=fluid.UNDEFINED)
            )
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, Sequence):
            return not self == other
        return NotImplemented

    def __call__(self):
        return iter(self)

    def __iter__(self):
        return iter(self.expr())

    def __nonzero__(self):
        return bool(next(iter(self), False))

def sequence(obj):
    if isinstance(obj, Sequence):
        return obj
    return Sequence(obj)

class Step(Sequence):
    __slots__ = ('next', 'name')

    def __init__(self, expr, next, make=None):
        self.name = expr
        self.expr = make(expr) if make else expr
        self.next = next

    def __repr__(self):
        return '<%s %r>' % (type(self).__name__, self.name)

    def __iter__(self):
        return expand(self.next, self.expr(focus()))

def path(*steps):
    return reduce(lambda a, s: s(a), reversed(steps), None)

def step(expr, make=None):
    return fn.partial(Step, expr, make=make)

class Filter(Step):
    __slots__ = ()

    def __iter__(self):
        return expand(self.next, sequence(self.expr()))

def filter(expr):
    return fn.partial(Filter, expr)

class Predicate(Step):
    __slots__ = ()

    def __iter__(self):
        if self.expr():
            if self.next:
                for x in self.next():
                    yield x
            else:
                yield focus()

def predicate(pred):
    if isinstance(pred, int):
        return fn.partial(Predicate, lambda: index() == pred)
    return fn.partial(Predicate, pred)


### Axis

def axis(SeqType):
    def decorator(expand):
        make = fn.partial(standard, expand)

        @fn.wraps(expand)
        def partial(test):
            return fn.partial(SeqType, test, make=make)

        return partial
    return decorator

def standard(expand, test):
    if not test:
        test = bool
    elif isinstance(test, basestring):
        test = named(test)
    elif isinstance(test, type):
        test = instance(test)
    elif not callable(test):
        raise ValueError('Unexpected node test: %r' % test)
    return lambda item: it.ifilter(test, expand(item))

def identity(x):
    return x

def named(name):
    return lambda i: i.name == name

def instance(cls):
    return lambda i: isinstance(i, cls)

@axis(Step)
def self(item):
    yield item

@axis(Step)
def parent(item):
    yield item.folder

def child(test):
    if isinstance(test, basestring):
        return step(test, _child)
    return _children(test)

def _child(name):
    return fn.partial(__child, name)

def __child(name, item):
    probe = not leaf(item) and item.child(name)
    if probe:
        yield probe

@axis(Step)
def _children(item):
    return () if leaf(item) else item

def attribute(test):
    assert not test or isinstance(test, basestring)
    if isinstance(test, basestring):
        return step(test, _attr)
    return _attributes(test)

def _attr(name):
    return fn.partial(__attr, name)

def __attr(name, item):
    try:
        yield getattr(item, name)
    except AttributeError:
        pass

@axis(Step)
def _attributes(item):
    for key in item.properties().iterkeys():
        yield (key, getattr(item, key))
    for key in item.dynamic_properties():
        yield (key, getattr(item, key))

@axis(Step)
def ancestor(item):
    return ascend(item)

@axis(Step)
def ancestor_or_self(item):
    return orself(item, ascend)

@axis(Step)
def descendant(item):
    return descend(item)

@axis(Step)
def descendant_or_self(item):
    return orself(item, descend)

@axis(Step)
def following_sibling(item):
    return after(item)

@axis(Step)
def following(item):
    return (d for i in after(item) for d in orself(i, descend))

@axis(Step)
def preceeding_sibling(item):
    return before(item)

@axis(Step)
def preceeding(item):
    return (d for i in before(item) for d in orself(i, descend))


### Aux

def orself(item, walk):
    yield item
    for x in walk(item):
        yield x

def ascend(item):
    probe = item.folder
    while probe:
        yield probe
        probe = probe.folder

def descend(item):
    if leaf(item):
        return
    queue = coll.deque(item)
    while queue:
        item = queue.popleft()
        yield item
        if not leaf(item):
            queue.extend(item)

def before(item):
    parent = item.parent
    return parent.before(item) if parent else ()

def after(item):
    parent = item.parent
    return parent.after(item) if parent else ()

def unique(seq):
    seen = set()
    for item in seq:
        if item not in seen:
            seen.add(item)
            yield item
