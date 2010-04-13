## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""tests -- unit tests"""

from __future__ import absolute_import
import os, unittest
from md.prelude import *
from . import *; from . import _tree

def load():
    from . import load

    data = os.path.join(os.path.dirname(__file__), 'test')
    return load.memory(data, 'test')

class TestTree(unittest.TestCase):

    def setUp(self):
        self.root = load()

    def test_root(self):
        self.assertEqual(self.root.name, 'test')
        self.assertEqual(type(self.root.contents), _tree._content)
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

class TestQuery(unittest.TestCase):

    def setUp(self):
        self.root = load()

    def test_root(self):
        self._check('/', (Site, 'test'))
        self._check('.', (Site, 'test'))
        self._check('*', (Page, 'about'), (Folder, 'news'))

    def test_simple(self):
        self._check('/news', (Folder, 'news'))
        self._check('/news/*', (Page, 'article-1'), (Page, 'article-2'), (Page, 'article-3'))

    def test_axis(self):
        self._check('//.',
                    (Site, 'test'), (Page, 'about'), (Folder, 'news'),
                    (Page, 'article-1'), (Page, 'article-2'), (Page, 'article-3'))
        self._check('/news/article-1/parent::*', (Folder, 'news'))

    def test_kind(self):
        self._check('/Folder', (Folder, 'news'))
        self._check('//Page',
                    (Page, 'about'), (Page, 'article-1'),
                    (Page, 'article-2'), (Page, 'article-3'))

    def _check(self, path, *result):
        self.assertEqual(tuple((type(r), r.name) for r in query(path)), result)

