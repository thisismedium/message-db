## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""tree -- a content tree"""

from __future__ import absolute_import
import re
from md import abc
from md.prelude import *
from ..query import tree
from .. import avro, data
from . import api, path_query

__all__ = (
    'Key', 'Item', 'Folder', 'Site', 'Subdomain', 'Page',
    'get_type', 'type_name', 'text', 'html',
    'root', 'query', 'path', 'resolve',
    'make', 'add', 'save', 'remove'
)


### Types

avro.require('tree.json')

Key = data.Key
content = data.value

get_type = avro.get_type

def type_name(obj, *args, **kw):
    if isinstance(obj, basestring):
        return obj
    return avro.type_name(obj, *args, **kw)


### Content Tree

## The content tree is made up of Items.  A Folder is an Item that has
## children; otherwise an Item is a leaf node in the tree.  Methods
## defined in these classes support basic manipulation operations and
## implement the query-tree interface.

@abc.implements(tree.Node)
class Item(content('Item')):

    def __repr__(self):
        return '%s(name=%r)' % (type(self).__name__, self.name)

    def __leaf__(self):
        return True

    def __json__(self):
        return update(super(Item, self).__json__(), _path=path(self))

    @property
    def parent(self):
        return api.get(self.folder)

@abc.implements(tree.InnerNode)
class Folder(content('Folder')):

    def __init__(self, **kw):
        super(Folder, self).__init__(**kw)

    def __nonzero__(self):
        return True

    def __len__(self):
        return len(self.contents)

    def __contains__(self, name):
        return name in self.contents

    ## FIXME: api.get() doesn't return multiple keys in request-order.
    ## Look into doing something about this.  For now, workaround by
    ## calling api.get() on individual keys.

    def __iter__(self):
        return (api.get(k) for k in self.contents.itervalues())

    def __leaf__(self):
        return False

    def before(self, item):
        return (api.get(k) for k in self.contents.itervalues(item.name))

    def after(self, item):
        seq = self.contents.itervalues(item.name, None)
        # The sequence begins with item; skip it.
        next(seq, None)
        return (api.get(k) for k in seq)

    def child(self, name, default=None):
        key = self.contents.get(name)
        return api.get(key) if key else default

    def add(self, item):
        if item.name in self:
            raise ValueError('Child already exists: %r.' % item.name)
        elif item.folder:
            raise ValueError('Child already in folder: %r' % item.parent)
        self.contents[item.name] = item.key
        item.folder = self.key
        return self

    def remove(self, item):
        if self.contents.get(item.name) == item.key:
            del self.contents[item.name]
            item.folder = None
        return self

class Site(content('Site')):
    pass

class Subdomain(content('Subdomain')):
    pass

class Page(content('Page')):
    pass


### Extra Types

## These types aren't used directly, but are defined here because they
## are declared in the external schema.  If the avro package supports
## automatic creation of non-record types in the future, these can be
## removed.

class text(avro.primitive('M.text', avro.string)):
    """Text is a multiline string type."""

class html(avro.primitive('M.html', avro.string)):
    """Marked-up text."""

_folder = avro.union(Key, None)

_content = avro.omap(Key)


### Traversal

## The query() method is a generalized way to traverse the content
## tree using XPath-style queries.  Additionally, resolve() can be
## used to resolve a simple path to a specific item.

ROOT = data.Key.make(Site, 'root')

def root():
    return api.get(ROOT)

def query(path, base=None):
    return path_query.compile(path)(root() if base is None else base)

def path(item):
    up = (i.name for i in tree.orself(item, tree.ascend) if i.folder)
    return '/%s' % '/'.join(reversed(list(up)))

def resolve(expr, base=None):
    steps = expr.strip('/')
    if not steps:
        return root() if expr.startswith('/') else (base or root())

    base = base or root()
    for name in steps.split('/'):
        probe = base.child(name)
        if not probe:
            raise ValueError('Bad expr: %r (%r has no child %r).' % (
                expr, path(base), name
            ))
        base = probe

    return base

def walk(item):
    return tree.orself(item, tree.descend)


### Manipulation

## These manipulation primitives expect to be run in the context of a
## Delta (see api.py).  Together, they allow items to be created,
## added to a folder, updated, and removed.

def make(cls, **kw):
    name = _slug(kw.get('name') or kw.get('title', ''))
    if not name:
        raise ValueError('Missing required title or name.')

    folder = kw.pop('folder', None)
    item = api.new(cls, update(
        kw,
        name=name,
        title=(kw.get('title', '').strip() or _title(name))
    ))

    return add(folder, item) if folder else item

def add(folder, item):
    return api.update(folder.add(item), item)

def save(item, *args, **kw):
    return api.update(item.update(*args, **kw))

def remove(child):
    if child == root():
        raise ValueError('Cannot remove the root item.')
    folder = api.update(child.parent.remove(child))
    api.delete(list(walk(child)))
    return folder

SLUG = re.compile(r'[^a-z0-9]+')
def _slug(name):
    return SLUG.sub('-', name.lower()).strip('-')

def _title(name):
    return name.replace('-', ' ').title()
