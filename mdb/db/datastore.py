## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""datastore -- datastore stub implemented with the AppEngine SDK"""

from __future__ import absolute_import
import os, glob, re, itertools as it, contextlib as ctx
from md import abc
from ..query import tree
from .. import data
from . import models, types, stm, api

__all__ = (
    'Item', 'Folder', 'Page',
    'root', 'path', 'resolve', 'add_child', 'remove',
    'setup'
)


### Models

@abc.implements(tree.Node)
class Item(models.Model):

    def __init__(self, **kw):
        name = make_slug(kw.get('name') or kw.get('title', ''))
        if not name:
            raise ValueError('An Item requires a title or a name.')
        super(Item, self).__init__(**update(
            kw,
            name=name,
            title=(kw.get('title', '').strip() or make_title(name))
        ))

    def __repr__(self):
        return '<%s %s>' % (type(self).__name__, self.title)

    def __leaf__(self):
        return True

    name = types.StringProperty()
    title = types.StringProperty()
    folder = types.ReferenceProperty('Item')

@abc.implements(tree.InnerNode)
class Folder(Item):
    contents = types.DirectoryProperty(Item)

    def __nonzero__(self):
        return True

    def __len__(self):
        return len(self.contents)

    def __contains__(self, name):
        return name in self.contents

    def __iter__(self):
        return self.contents.itervalues()

    def __leaf__(self):
        return False

    def before(self, item):
        return self.contents.itervalues(item.name)

    def after(self, item):
        seq = self.contents.itervalues(item.name, None)
        next(seq, None) # seq begins with item; skip it.
        return seq

    def child(self, name, default=None):
        return self.contents.get(name, default)

    def add(self, item):
        if item.name in self:
            raise ValueError('Child already exists: %r.' % item.name)
        elif item.folder:
            raise ValueError('Child already in folder: %r' % item.folder)
        self.contents[item.name] = item
        item.folder = self
        return self

    def remove(self, item):
        if self.child(item.name) == item:
            del self.contents[item.name]
            item.folder = None
        return self

class Page(Item):
    description = types.StringProperty()
    content = types.TextProperty()


### Content Tree

def root():
    return api.get(models.Key.make('Folder', 'root'))

def find_root(item):
    while item.parent:
        item = item.parent
    return item

parents = tree.ascend

def add_child(folder, child):
    folder.add(child)
    return child

def remove(child):
    if child == root():
        raise ValueError('Cannot remove the root item.')
    folder = child.folder
    folder.remove(child)
    api._delete(list(tree.orself(child, tree.descend)))
    return folder

def walk(item):
    return tree.orself(item, tree.descend)

def path(item):
    up = (i.name for i in tree.orself(item, tree.ascend) if i.folder)
    return '/%s' % '/'.join(reversed(list(up)))

def resolve(expr, top=None):
    steps = expr.strip('/')
    if not steps:
        return root() if expr.startswith('/') else (top or root())

    top = top or root()
    for name in steps.split('/'):
        probe = top.child(name)
        if not probe:
            raise ValueError('Bad expr: %r (%r has no child %r).' % (
                expr, path(top), name
            ))
        top = probe

    return top


### Utilities

SLUG = re.compile(r'[^a-z0-9]+')
def make_slug(name):
    return SLUG.sub('-', name.lower()).strip('-')

def make_title(name):
    return name.replace('-', ' ').title()

def update(data, *args, **kw):
    data.update(*args, **kw)
    return data

def setdefault(data, **kw):
    for (key, val) in kw.iteritems():
        data.setdefault(key, val)
    return data


### Persist

def setup(base, app_id):
    path = os.path.join(base, '%s.data' % app_id)
    zs = data.zipper(data.back.fsdir(path))
    if not os.path.exists(path):
        stm.initialize(zs.create())
        return import_yaml(base)
    else:
        stm.initialize(zs)
        return root()

def import_yaml(path):
    with stm.transaction('Imported YAML'):
        root = built_root(os.path.basename(path))
        for name in glob.iglob('%s/*.yaml' % path):
            with ctx.closing(open(name, 'r')) as port:
                build_item(root, os.path.basename(name), data.yaml.load(port))
        return root

def built_root(name):
    return Folder(name=name, key_name='root')

def build_item(root, name, data):
    model = models.model(data.pop('kind', 'Item'))
    names = os.path.splitext(name)[0].split('--')
    return add_child(
        build_folders(root, names[0:-1]),
        model(**setdefault(data, name=names[-1]))
    )

def build_folders(top, names):
    for name in names:
        probe = top.child(name)
        if not probe:
            probe = add_child(top, Folder(name=name))
        top = probe
    return top
