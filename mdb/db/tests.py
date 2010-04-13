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

    def test_path(self):
        self.assertEqual(path(resolve('/news/article-2')), '/news/article-2')

    def test_add(self):
        with delta('Add Page') as d:
            add(self.root, make(Page, name='hello'))
            d.checkpoint()
        self.assertEqual([c.name for c in self.root], ['about', 'news', 'hello'])

    def test_update(self):
        with delta('Update Page') as d:
            about = resolve('/about')
            about.description = 'Changed description!'
            d.checkpoint()
        self.assertEqual(resolve('/about').description, 'Changed description!')

    def test_remove(self):
        with delta('Remove article-2') as d:
            remove(resolve('/news/article-2'))
            d.checkpoint()

        self.assertEqual(self._structure(resolve('/news')),
                         ('news', ['article-1', 'article-3']))

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
        self._check('/news/*',
                    (Page, 'article-1'), (Page, 'article-2'), (Page, 'article-3'))

    def test_axis(self):
        self._check('//.',
                    (Site, 'test'), (Page, 'about'), (Folder, 'news'),
                    (Page, 'article-1'), (Page, 'article-2'), (Page, 'article-3'))
        self._check('/news/article-1/parent::*', (Folder, 'news'))
        self._check('/news/article-2/sibling::*', (Page, 'article-1'), (Page, 'article-3'))
        self._check('/news/article-2/preceding-sibling::*', (Page, 'article-1'))
        self._check('/news/article-2/following-sibling::*', (Page, 'article-3'))

    def test_kind(self):
        self._check('/Folder', (Folder, 'news'))
        self._check('//Page',
                    (Page, 'about'), (Page, 'article-1'),
                    (Page, 'article-2'), (Page, 'article-3'))

    def _check(self, path, *result):
        self.assertEqual(tuple((type(r), r.name) for r in query(path)), result)



