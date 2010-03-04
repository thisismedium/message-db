## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""tests -- unit tests"""

from __future__ import absolute_import
import unittest
from . import *
from ..prelude import *

class TestBackingStore(object):

    def setUp(self):
        self.back = self.makeStore().open()
        self.populate({
            'a': '1',
            'b': '2'
        })

    def tearDown(self):
        self.back.destroy()

    def populate(self, data):
        self.back.mset(data.iteritems())

    def test_get(self):
        self.assertEqual(self.back.get('a'), '1')
        self.assertEqual(dict(self.back.mget(['a', 'b'])), dict(a='1', b='2'))

    def test_gets(self):
        self.assertEqual(self.back.gets('a'), self.back.gets('a'))

    def test_set(self):
        self.back.set('a', '3')
        self.assertEqual(self.back.get('a'), '3')

    def test_add(self):
        self.back.add('c', '3')
        self.assertEqual(self.back.get('c'), '3')
        self.assertRaises(NotStored, lambda: self.back.add('a', '4'))

    def test_replace(self):
        self.back.replace('a', '3')
        self.assertEqual(self.back.get('a'), '3')
        self.assertRaises(NotStored, lambda: self.back.replace('c', '3'))

    def test_cas(self):
        (_, t1) = self.back.gets('a')
        self.back.cas('a', '3', t1)
        (value, t2) = self.back.gets('a')
        self.assertEqual(value, '3')
        self.assertNotEqual(t1, t2)
        self.assertRaises(NotStored, lambda: self.back.cas('a', '4', t1))

    def test_delete(self):
        self.back.delete('a')
        self.assertEqual(self.back.get('a'), Undefined)
        self.assertRaises(NotFound, lambda: self.back.delete('c'))

class TestFSDir(TestBackingStore, unittest.TestCase):

    def makeStore(self):
        from .. import os
        return back.fsdir(os.mkdtemp())

class TestPrefixed(TestBackingStore, unittest.TestCase):

    def makeStore(self):
        return back.prefixed(back.memory(), '#')

class TestMemory(TestBackingStore, unittest.TestCase):

    def makeStore(self):
        return back.memory()

class TestStatic(unittest.TestCase):

    def setUp(self):
        from .. import yaml
        self.back = back.static(back.memory(), yaml, prefix='#')
        self.back.open()

    def tearDown(self):
        self.back.destroy()

    def test_get(self):
        (key, _) = self.back.put(1)
        self.assertEqual(self.back.get(key), 1)

    def test_put(self):
        [(k1, v1), (k2, v2)] = self.back.mput([1, 2])
        self.assertNotEqual(k1, k2)
        self.assertEqual(v1, 1)
        self.assertEqual(v2, 2)

        (k3, v3) = self.back.put(1)
        self.assertEqual(k1, k3)
        self.assertEqual(v1, v3)

    def test_data(self):
        from ..collections import tree, omap

        t1 = tree(a=1, z=2, m=3, b=4)
        (k1, _) = self.back.put(t1)
        (k2, v2) = self.back.put(tree(a=1, z=2, m=3, b=4))
        self.assertEqual(k1, k2)
        self.assertEqual(t1, v2)

        m1 = omap([('a', 1), ('b', 2), ('c', 3)])
        (k1, _) = self.back.put(m1)
        (k2, v2) = self.back.put(omap([('a', 1), ('b', 2), ('c', 3)]))
        self.assertEqual(k1, k2)
        self.assertEqual(m1, v2)
