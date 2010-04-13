## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""tests -- unit tests"""

from __future__ import absolute_import
import os, unittest
from md.prelude import *
from . import *

def load():
    from . import load

    data = os.path.join(os.path.dirname(__file__), 'test')
    return load.memory(data, 'test')

class TestTree(unittest.TestCase):

    def setUp(self):
        self.root = load()

    def test_root(self):
        self.assertEqual(self.root.name, 'test')
        self.assertEqual(type(self.root.contents), omap)
        self.assertEqual(self.root.contents.keys(), ['about', 'news'])

    def test_structure(self):
        self.assertEqual(self._structure(self.root),
                         ('test',
                          ['about',
                           ('news', ['article-1', 'article-2', 'article-3'])]))

    def _structure(self, top):
        if not isinstance(top, Folder):
            return top.name
        return (top.name, [self._structure(c) for c in top])
