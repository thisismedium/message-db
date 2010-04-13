## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""load -- bootstrap a datastore"""

from __future__ import absolute_import
import os, glob, yaml
from md.prelude import *
from .. import data
from . import tree, api

__all__ = ('memory', 'fsdir')

def memory(base, app_id):
    zs = data.zipper(data.back.memory())
    api.init(zs.create())
    return load_yaml(base)

def fsdir(base, app_id):
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
    with api.delta('Imported %r.' % path) as delta:
        root = _root(os.path.basename(path))

        for name in glob.iglob('%s/*.yaml' % path):
            with closing(open(name, 'r')) as port:
                _item(root, os.path.basename(name), yaml.load(port))

        delta.checkpoint()
    return root

def _root(name):
    return api.make(tree.Site, name=name, key_name='root')

def _item(root, name, data):
    cls = api.kind(data.pop('kind', 'Item'))
    names = os.path.splitext(name)[0].split('--')

    return api.make(
        cls,
        folder=_folder(root, names[0:-1]),
        **setdefault(data, name=names[-1])
    )

def _folder(top, names):
    for name in names:
        probe = top.child(name)
        if not probe:
            probe = api.make(tree.Folder, name=name, folder=top)
        top = probe
    return top

