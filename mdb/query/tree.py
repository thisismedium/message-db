## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""tree -- data tree interface"""

from __future__ import absolute_import
import collections as coll, functools as fn, itertools as it
from md import abc, fluid

__all__ = (
    'Node', 'InnerNode', 'leaf', 'collection', 'focus', 'index',
    'Path', 'Sequence', 'sequence', 'steps', 'filter', 'predicate',
    'self', 'parent', 'child', 'attribute', 'ancestor', 'ancestor_or_self',
    'descendant', 'descendant_or_self', 'following_sibling', 'following',
    'preceeding_sibling', 'preceeding'
)


### Abstract Interface

class Node(coll.Hashable):
    """An abstract interface for leaf nodes."""

    __slots__ = ()

    def __leaf__(self):
        return True

class InnerNode(Node, coll.Iterable, coll.Sized):
    """An abstract interface for non-leaf nodes."""

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
    """Is this object a leaf object?"""

    try:
        return obj.__leaf__()
    except AttributeError:
        ## This is a little weird, but the default value is True
        ## because being a non-leaf node is opt-in.
        return True


### Dynamic Context

# The original input sequence
COLLECTION = fluid.cell(())
collection = fluid.accessor(COLLECTION)

# The currently focused item
FOCUS = fluid.cell()
focus = fluid.accessor(FOCUS)

# The index of the currently focused item.
INDEX = fluid.cell()
index = fluid.accessor(INDEX)


### Top Level

def Path(exprs):
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

def sequence(obj):
    """Lift any value into a Sequence that can be used as input to a
    compiled path query."""

    if isinstance(obj, Sequence):
        return obj
    return Sequence(obj)

class Sequence(object):
    """A sequence wraps a Python object with an interface compatible
    with path queries."""

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

def steps(*steps):
    """Reduce a sequence of steps to a single expression that can be
    used to expand an input sequence."""

    return reduce(lambda a, s: s(a), reversed(steps), None)

def step(expr, make=None):
    """Delay the binding of a single step expression to the next step
    expression."""

    return fn.partial(Step, expr, make=make)

class Step(Sequence):
    """A path query is a linked list of steps."""

    __slots__ = ('next', 'name')

    def __init__(self, expr, next, make=None):
        self.name = expr
        self.expr = make(expr) if make else expr
        self.next = next

    def __repr__(self):
        return '<%s %r>' % (type(self).__name__, self.name)

    def __iter__(self):
        return expand(self.next, self.expr(focus()))

def filter(expr):
    """Lift a Python thunk to a Step."""

    return fn.partial(Filter, expr)

class Filter(Step):
    """A Filter is a way to use a procedure in the middle of a path.
    The procedure is called with no values and may use the dynamic
    environment to expand the current item in some way."""

    __slots__ = ()

    def __iter__(self):
        return expand(self.next, sequence(self.expr()))

def predicate(pred):
    """Lift a nullary Python predicate procedure or an integer
    (representing an index) to a Step."""

    if isinstance(pred, int):
        return fn.partial(Predicate, lambda: index() == pred)
    return fn.partial(Predicate, pred)

class Predicate(Step):
    """A Predicate is a step that tests the currently focused item.
    If the test is successful, the item is expanded.  Since the
    predicate is nullary, it can use the dynamic environment to test
    the currently focused item."""

    __slots__ = ()

    def __iter__(self):
        if self.expr():
            if self.next:
                for x in self.next():
                    yield x
            else:
                yield focus()


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
        test = filtered
    elif isinstance(test, basestring):
        test = named(test)
    elif isinstance(test, type):
        test = instance(test)
    elif not callable(test):
        raise ValueError('Unexpected node test: %r' % test)
    return lambda item: test(expand(item))

def filtered(items):
    return it.ifilter(bool, items)

def named(name):
    return lambda items: (i for i in items if i.name == name)

def instance(cls):
    return lambda items: (i for i in items if isinstance(i, cls))

@axis(Step)
def self(item):
    yield item

@axis(Step)
def parent(item):
    probe = item.folder
    if probe:
        yield probe

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
    ## FIXME: hashable() is a workaround for ListProperty
    for item in hashable(seq):
        if item not in seen:
            seen.add(item)
            yield item

def hashable(seq):
    for item in seq:
        if isinstance(item, (list, set)):
            item = tuple(item)
        yield item


