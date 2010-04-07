from __future__ import absolute_import
import unittest, cStringIO, weakref
from md.prelude import *
from . import *

# Schema can be declared externally in a JSON format.  This example
# shows two "record" schemas.  A record has a name and a list of
# fields.  The Avro specification has more details about how to write
# schema.

SCHEMA = """
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
        { "name": "value", "type": "string" },
        { "name": "next", "type": "Test.Pointer" }
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
                         ['Test.Pointer', 'Test.Link'])

    def test_get(self):
        schema.load(SCHEMA)
        self.assertEqual(str(schema.get('Test.Pointer')), '{"type": "record", "namespace": "Test", "name": "Pointer", "fields": [{"type": "string", "name": "address"}]}')

    def test_declare(self):
        item = schema.declare(dict(
            name='Test.Item',
            type='record',
            fields=[
                dict(name="title", type="string"),
                dict(name="content", type="string")
            ]
        ))

        self.assertEqual(item, schema.get('Test.Item'))
        self.assertEqual(str(item), '{"type": "record", "namespace": "Test", "name": "Item", "fields": [{"type": "string", "name": "title"}, {"type": "string", "name": "content"}]}')

    def test_name(self):
        schema.load(SCHEMA)
        self.assertEqual(types.name(schema.get('Test.Link')), 'Test.Link')

class TestStructure(unittest.TestCase):

    def setUp(self):
        schema.clear()
        schema.load(SCHEMA)

        class Pointer(structure('Test.Pointer', weak=True)):
            pass

        class Link(structure('Test.Link')):
            pass

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

class TestTypes(unittest.TestCase):

    def setUp(self):
        schema.clear()
        schema.load(SCHEMA)

        class Pointer(structure('Test.Pointer')):
            pass

        self.Pointer = Pointer
        self.ITree = mapping(int)
        self.PTree = mapping(Pointer)
        self.IArray = array(int)
        self.PArray = array(self.PTree)

    def test_itree(self):
        return self.expect(self.ITree(a=1, b=2),
                           '\x02\x00\x10map<int>\x04\x02a\x02\x02b\x04\x00')

    def test_ptree(self):
        return self.expect(self.PTree(a=self.Pointer('alpha'), b=self.Pointer('beta')),
                           '\x02\x00\x22map<Test.Pointer>\x04\x02a\nalpha\x02b\x08beta\x00')

    def test_iarray(self):
        return self.expect(self.IArray([1, 2]),
                           '\x02\x00\x14array<int>\x04\x02\x04\x00')

    def test_parray(self):
        t1 = self.PTree(a=self.Pointer('alpha'))
        t2 = self.PTree(b=self.Pointer('beta'))
        self.expect(self.PArray([t1, t2]),
                    '\x02\x000array<map<Test.Pointer>>\x04\x02\x02a\nalpha\x00\x02\x02b\x08beta\x00\x00')

    def expect(self, val, data):
        dumped = dumps_binary(val)
        self.assertEqual(dumped, data)

        obj = loads_binary(dumped)
        self.assertEqual(type(obj), type(val))
        self.assertEqual(obj, val)


