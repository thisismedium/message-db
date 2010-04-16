## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""load -- bootstrap a datastore"""

from __future__ import absolute_import
import os
from md.prelude import *
from .. import data
from . import tree, api, auth as auth_

__all__ = ('init', 'backing', 'loader')


### Initialization

def init(app_id, path=None, load=None, create=None,
         auth=None, service=None, host=None):

    zs = zipper(app_id, path)
    created = not zs.exists()
    if created:
        zs.create()

    api.init_api(zs)
    auth_.init_auth(auth, service=service, host=host)
    if created:
        load and loader(load)
        create and create()

    return zs

def zipper(app_id, path):
    return data.zipper(backing(path))


### Extensible initialization

class Dispatch(object):

    def __init__(self, name):
        self.name = name
        self.registry = {}

    def __repr__(self):
        return '<%s>' % self.name

    def __call__(self, path):
        try:
            (scheme, rest) = path.split(':', 1)
        except ValueError:
            raise ValueError("Expected 'scheme:path' format, not %r." % path)

        method = self.registry.get(scheme)
        if method is None:
            raise ValueError('Unrecognized scheme %r in %r.' % (scheme, path))
        return method(rest)

    def define(self, name):
        def decorator(proc):
            self.registry[name] = proc
            return proc
        return decorator

loader = Dispatch('loader')

backing = Dispatch('backing')


### Backing Store

@backing.define('memory')
def memory(path):
    return data.back.memory()

backing.define('fsdir')(data.back.fsdir)


### YAML

@loader.define('yaml')
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

