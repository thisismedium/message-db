## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""tests -- unit tests"""

from __future__ import absolute_import
import unittest, cStringIO, weakref
from md.prelude import *
from . import *

# Schema can be declared externally in a JSON format.  Fixed data have
# exact lengths.  A record has a name and a list of fields.  The Avro
# specification has more details about how to write schema.

SCHEMA = """
{ "type": "fixed", "name": "Test.uuid", "size": 16 }

{ "type": "string", "name": "text" }

{
    "type": "record",
    "name": "Test.Pointer",
    "fields": [
      { "name": "address", "type": "string" }
    ]
}

{
    "type": "record",
    "name": "Test.Link",
    "doc": "A linked list.",
    "fields": [
        { "name": "value", "type": "text" },
        { "name": "next", "type": "Test.Pointer" }
    ]
}

{
    "type": "record",
    "name": "Test.Box",
    "fields": [
        { "name": "value", "type": [ "null", "Test.uuid", "Test.Pointer" ]}
    ]
}
"""

class TestExternalSchema(unittest.TestCase):

    # Clear the loaded schema before each test to prevent "already
    # defined" errors.

    def setUp(self):
        schema.clear()

    def test_load(self):
        loaded = schema.load(SCHEMA)
        self.assertEqual([s.fullname for s in loaded],
                         ['Test.uuid', 'text', 'Test.Pointer', 'Test.Link', 'Test.Box'])

    def test_get(self):
        schema.load(SCHEMA)
        self.assertEqual(str(types.get_schema('Test.Pointer')), '{"type": "record", "namespace": "Test", "name": "Pointer", "fields": [{"type": "string", "name": "address"}]}')

    def test_declare(self):
        item = schema.declare(dict(
            name='Test.Item',
            type='record',
            fields=[
                dict(name="title", type="string"),
                dict(name="content", type="string")
            ]
        ))

        self.assertEqual(item, types.get_schema('Test.Item'))
        self.assertEqual(str(item), '{"type": "record", "namespace": "Test", "name": "Item", "fields": [{"type": "string", "name": "title"}, {"type": "string", "name": "content"}]}')

## A structure is a simple Python type for an Avro record.  Make sure
## they are well-behaved Python types and marshall correctly.  See
## record.py

class TestStructure(unittest.TestCase):

    def setUp(self):
        schema.clear()
        schema.load(SCHEMA)

        class text(primitive('text', string)):
            pass

        class Pointer(structure('Test.Pointer', weak=True)):
            pass

        class Link(structure('Test.Link')):
            pass

        self.text = text
        self.Pointer = Pointer
        self.Link = Link

    def test_type_json(self):
        self.assertEqual(dumps(self.Pointer), '{"fields": [{"name": "address", "type": "string"}], "name": "Pointer", "namespace": "Test", "type": "record"}',)

    def test_instance_json(self):
        thing = self.Link("a", self.Pointer(""))
        self.assertEqual(dumps(thing), '{"next": {"address": ""}, "value": "a"}')
        self.assertEqual(thing, loads(dumps(thing), self.Link))

    def test_destructure(self):
        thing = self.Link("a", self.Pointer(""))
        (val, (addr, )) = thing
        self.assertEqual(val, 'a')
        self.assertEqual(addr, '')

    def test_copy(self):
        thing = self.Link("a", self.Pointer(""))
        other = thing.replace(value='b')
        self.assert_(other is not thing)
        self.assertNotEqual(other, thing)
        self.assertEqual(thing.value, 'a')
        self.assertEqual(other.value, 'b')

    def test_dump_binary(self):
        thing = self.Link("a", self.Pointer(""))

        self.assertEqual(dumps_binary(thing),
                         '\x02\x00\x12Test.Link\x02a\x00')

    def test_load_binary(self):
        thing = self.Link("a", self.Pointer(""))
        self.assertEqual(thing, loads_binary('\x02\x00\x12Test.Link\x02a\x00'))

    def test_weak(self):
        obj = self.Pointer("example")
        self.assertEqual(weakref.ref(obj)(), obj)

## Primitive types are numbers, strings, boolean, etc.  Complex,
## non-named types are maps, arrays, unions, etc.

class TestTypes(unittest.TestCase):

    def setUp(self):
        schema.clear()
        schema.load(SCHEMA)

        class uuid(fixed('Test.uuid')):
            pass

        self.uuid = uuid

        class Pointer(structure('Test.Pointer')):
            pass

        self.Pointer = Pointer

        class Box(structure('Test.Box')):
            pass

        self.Box = Box
        self.ValueUnion = union(null, uuid, Pointer)

        ## Mappings and Sequences over primitive and named types.
        self.ITree = map(int)
        self.OIMap = omap(int)
        self.PTree = map(Pointer)
        self.IArray = array(int)
        self.SSet = set(string)

        ## A 3-level deep non-named Python type.
        self.PArray = array(self.PTree)

    def test_uuid(self):
        value = '\xddm\x92\xa1\xcfwE>\xa4\x18\x01\x18uw\xf2\xaf'
        return self.expect(self.uuid(value),
                           '\x02\x00\x12Test.uuid' + value)

    def test_box(self):
        self.expect(self.Box(None),
                    '\x02\x00\x10Test.Box\x00',
                    '{"value": null}')

        self.expect(self.Box(self.uuid('1234567890123456')),
                    '\x02\x00\x10Test.Box\x021234567890123456',
                    '{"value": "1234567890123456"}')

        self.expect(self.Box(self.Pointer("foo")),
                    '\x02\x00\x10Test.Box\x04\x06foo',
                    '{"value": {"address": "foo"}}')

    def test_itree(self):
        return self.expect(self.ITree(a=1, b=2),
                           '\x02\x00\x10map<int>\x04\x02a\x02\x02b\x04\x00',
                           '{"a": 1, "b": 2}')

    def test_oitree(self):
        return self.expect(self.OIMap([('c', 1), ('a', 2), ('b', 3)]),
                           '\x02\x00\x12omap<int>\x06\x02c\x02\x02a\x04\x02b\x06\x00',
                           '[["c", 1], ["a", 2], ["b", 3]]')

    def test_ptree(self):
        return self.expect(self.PTree(a=self.Pointer('alpha'), b=self.Pointer('beta')),
                           '\x02\x00\x22map<Test.Pointer>\x04\x02a\nalpha\x02b\x08beta\x00',
                           '{"a": {"address": "alpha"}, "b": {"address": "beta"}}')

    def test_iarray(self):
        return self.expect(self.IArray([1, 2]),
                           '\x02\x00\x14array<int>\x04\x02\x04\x00',
                           '[1, 2]')

    def test_sset(self):
        return self.expect(self.SSet(["foo", "bar", "mumble", "foo"]),
                           '\x02\x00\x16set<string>\x06\x06bar\x06foo\x0cmumble\x00',
                           '["bar", "foo", "mumble"]')

    def test_parray(self):
        t1 = self.PTree(a=self.Pointer('alpha'))
        t2 = self.PTree(b=self.Pointer('beta'))
        self.expect(self.PArray([t1, t2]),
                    '\x02\x000array<map<Test.Pointer>>\x04\x02\x02a\nalpha\x00\x02\x02b\x08beta\x00\x00',
                    '[{"a": {"address": "alpha"}}, {"b": {"address": "beta"}}]')

    def expect(self, val, data, json=None):
        dumped = dumps_binary(val)
        self.assertEqual(dumped, data)

        obj = loads_binary(dumped)
        self.assertEqual(type(obj), type(val))
        self.assertEqual(obj, val)

        if json is not None:
            dumped = dumps(val)
            self.assertEqual(dumped, json)
            obj = loads(dumped, type(val))
            self.assertEqual(type(obj), type(val))
            self.assertEqual(obj, val)


INHERIT = """
{ "type": "string", "name": "M.text" }

{ "type": "string", "name": "M.key" }

{
    "type": "record",
    "name": "M.Item",
    "fields": [
        { "name": "name", "type": "string" },
        { "name": "title", "type": "string" },
        { "name": "folder", "type": ["M.key", "null"] },
        { "name": "description", "type": "M.text" }
    ]
}

{
    "type": "record",
    "name": "M.Folder",
    "base": "M.Item",
    "fields": [
        { "name": "default_name", "type": "string" },
        { "name": "description", "type": "M.text" },
        { "name": "contents", "type": { "type": "omap", "values": "M.key" } }
    ]
}

{
    "type": "record",
    "name": "M.Site",
    "base": "M.Folder"
}
"""

class TestInherit(unittest.TestCase):

    def setUp(self):
        schema.clear()
        schema.load(INHERIT)

        class Item(structure('M.Item')):
            pass

        class Folder(structure('M.Folder')):
            pass

        class Site(structure('M.Site')):
            pass

        self.Item = Item
        self.Folder = Folder
        self.Site = Site

    def test_hierarchy(self):
        self.assert_(issubclass(self.Folder, self.Item))
        self.assert_(issubclass(self.Site, self.Folder))

    def test_fields(self):
        self.assertEqual([f.name for f in types.to_schema(self.Item).fields],
                         ['name', 'title', 'folder', 'description'])

        self.assertEqual([f.name for f in types.to_schema(self.Folder).fields],
                         ['name', 'title', 'folder', 'default_name', 'description', 'contents'])

        self.assertEqual([f.name for f in types.to_schema(self.Site).fields],
                         ['name', 'title', 'folder', 'default_name', 'description', 'contents'])

    def test_json_schema(self):
        self.assertEqual(dumps(self.Folder),
                         '{"base": "Item", "fields": [{"name": "name", "type": "string"}, {"name": "title", "type": "string"}, {"name": "folder", "type": ["key", "null"]}, {"name": "default_name", "type": "string"}, {"name": "description", "type": "text"}, {"name": "contents", "type": {"type": "omap", "values": "key"}}], "name": "Folder", "namespace": "M", "type": "record"}')


