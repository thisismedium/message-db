## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""load -- bootstrap a datastore"""

from __future__ import absolute_import
import os
from md.prelude import *
from .. import data
from . import tree, api

__all__ = ('memory', 'fsdir')


### Datastore Types

def memory(base, app_id):
    """Create a datastore in memory.  Once a program
    terminates, the data is destroyed."""

    zs = data.zipper(data.back.memory())
    api.init(zs.create())
    return load_yaml(base)

def fsdir(base, app_id):
    """Create a datastore in the filesystem using a git-style
    object store format."""

    path = os.path.join(base, '%s.data' % app_id)
    zs = data.zipper(data.back.fsdir(path))
    if not os.path.exists(path):
        api.init(zs.create())
        return load_yaml(base)
    else:
        api.init(zs)
        return tree.root()


### Import

def load_yaml(path):
    """Load yaml files found in path.

    A file named 'foo--bar--baz.yaml' would be interpreted as the item
    '/foo/bar/baz' in the content tree.  If the folders '/foo' and
    '/foo/bar' don't exist, they will be created first.

    The contents of the file should be a mapping of field names to
    values.  The special property "kind" indicates what type of item
    to create.  For example:

        kind: Page
        description: A page about this demo.
        content:
          <p>This is some content!</p>

    If no "name" is given, the name is taken from the filename.  If no
    "title" is given, the "name" is transformed into a title.
    """

    import yaml, glob

    with api.delta('Imported %r.' % path) as delta:
        root = _root(os.path.basename(path))

        for name in glob.iglob('%s/*.yaml' % path):
            with closing(open(name, 'r')) as port:
                _item(root, os.path.basename(name), yaml.load(port))

        delta.checkpoint()
    return root

def _root(name):
    return tree.make(tree.Site, name=name, key=tree.ROOT)

def _item(root, name, data):
    cls = tree.get_type(data.pop('kind', 'Item'))
    names = os.path.splitext(name)[0].split('--')

    return tree.make(
        cls,
        folder=_folder(root, names[0:-1]),
        **setdefault(data, name=names[-1])
    )

def _folder(top, names):
    for name in names:
        probe = top.child(name)
        if not probe:
            probe = tree.make(tree.Folder, name=name, folder=top)
        top = probe
    return top

