## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""tree -- a content tree"""

from __future__ import absolute_import
from md import abc
from md.prelude import *
from ..query import tree
from . import api, _tree
from ._tree import *; from ._tree import content

__all__ = _tree.__all__ + ('Item', 'Folder', 'Site')


### Content Tree

@abc.implements(tree.Node)
class Item(content('Item')):

    def __repr__(self):
        return '%s(name=%r)' % (type(self).__name__, self.name)

    def __leaf__(self):
        return True

    @property
    def parent(self):
        return api.get(self.folder)

@abc.implements(tree.InnerNode)
class Folder(content('Folder')):

    def __init__(self, **kw):
        kw['contents'] = omap(kw.get('contents', {}))
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
        return (api.get(k) for k in next(seq, None))

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
