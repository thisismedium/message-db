## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""yaml -- YAML marshalling"""

from __future__ import absolute_import
import yaml
from .prelude import *
from . import collections as coll

__all__ = ('load', 'loads', 'dump', 'dumps', 'pretty')


### Public interface

def load(stream):
    """Load YAML from a file object."""

    return yaml.load(stream, Loader)

def loads(data):
    """Load YAML from a string.

    >>> obj = loads('''
    ... foo: "bar"
    ... mumble: "quux"
    ... baz: "gorp"
    ... ''')
    >>> obj
    tree([('baz', 'gorp'), ('foo', 'bar'), ('mumble', 'quux')])

    >>> loads('''!!omap [{c: 1}, {b: 2}, {m: 3}]\\n''')
    """
    return yaml.load(data, Loader)

def dump(data, stream):
    """Serialize data to YAML; write it to stream."""

    return yaml.dump(data, stream, Dumper, default_flow_style=True)

def dumps(data):
    """Serialize data to YAML; return a string.

    >>> dumps(coll.tree(c=1, b=2, m=3))
    '{b: 2, c: 1, m: 3}\\n'
    >>> dumps(coll.omap([('c', 1), ('b', 2), ('m', 3)]))
    """

    return dump(data, None)

def pretty(data, stream=None):
    """Serialize data to un-flowed YAML.  Write the YAML to stream;
    otherwise return a string.

    >>> pretty(coll.tree(c=1, b=2, m=3))
    'b: 2\\nc: 1\\nm: 3\\n'
    >>> pretty(coll.omap([('c', 1), ('b', 2), ('m', 3)]))
    """
    return yaml.dump(data, stream, Dumper, default_flow_style=False)


### Construct

def add_constructor(cls, construct):
    Constructor.add_constructor(cls, construct)

class Constructor(yaml.constructor.SafeConstructor):
    """Override the default behavior of SafeConstructor to make maps
    and pairs into tree objects."""

    MapType = coll.tree
    OMapType = coll.omap
    AllowedPairsNodes = (yaml.SequenceNode, yaml.MappingNode)

    def construct_yaml_map(self, node):
        """Identical to the default implementation, but substitute a
        tree for a dict."""

        data = self.MapType()
        yield data
        value = self.construct_mapping(node)
        data.update(value)

    def construct_yaml_omap(self, node):
        omap = self.OMapType()
        yield omap
        if not isinstance(node, yaml.SequenceNode):
            raise yaml.constructor.ConstructorError(
                "while constructing an ordered map",
                node.start_mark,
                "expected a sequence, but found %s" % node.id, node.start_mark
            )
        for subnode in node.value:
            if not isinstance(subnode, yaml.MappingNode):
                raise yaml.constructor.ConstructorError(
                    "while constructing an ordered map", node.start_mark,
                    "expected a mapping of length 1, but found %s" % subnode.id,
                    subnode.start_mark
                )
            if len(subnode.value) != 1:
                raise yaml.constructor.ConstructorError(
                    "while constructing an ordered map", node.start_mark,
                    "expected a single mapping item, but found %d items" % len(subnode.value),
                    subnode.start_mark
                )
            key_node, value_node = subnode.value[0]
            key = self.construct_object(key_node)
            value = self.construct_object(value_node)
            omap.append((key, value))

add_constructor(u'tag:yaml.org,2002:omap', Constructor.construct_yaml_omap)
add_constructor(u'tag:yaml.org,2002:map', Constructor.construct_yaml_map)


### Load

if yaml.__with_libyaml__:
    from yaml import CParser

    class Loader(CParser, Constructor, yaml.resolver.Resolver):

        def __init__(self, stream):
            CParser.__init__(self, stream)
            Constructor.__init__(self)
            yaml.resolver.Resolver.__init__(self)

else:
    class Loader(yaml.reader.Reader, yaml.scanner.Scanner, yaml.parser.Parser,
                 yaml.composer.Composer, Constructor, yaml.resolver.Resolver):

        def __init__(self, stream):
            yaml.reader.Reader.__init__(self, stream)
            yaml.scanner.Scanner.__init__(self)
            yaml.parser.Parser.__init__(self)
            yaml.composer.Composer.__init__(self)
            Constructor.__init__(self)
            yaml.resolver.Resolver.__init__(self)


### Represent

def add_representer(cls, represent):
    Representer.add_representer(cls, represent)

Representer = yaml.representer.SafeRepresenter

@partial(add_representer, coll.tree)
def repr_tree(dumper, data):
    return dumper.represent_mapping(u'tag:yaml.org,2002:map', data.iteritems())

@partial(add_representer, coll.omap)
def repr_odict(dumper, data):
    return repr_pairs(dumper, u'tag:yaml.org,2002:omap', data.iteritems())

def repr_pairs(dump, tag, sequence, flow_style=None):
    """This is the same code as BaseRepresenter.represent_sequence(),
    but the value passed to dump.represent_data() in the loop is a
    dictionary instead of a tuple."""

    value = []
    node = yaml.SequenceNode(tag, value, flow_style=flow_style)
    if dump.alias_key is not None:
        dump.represented_objects[dump.alias_key] = node
    best_style = True
    for (key, val) in sequence:
        item = dump.represent_data({key: val})
        if not (isinstance(item, yaml.ScalarNode) and not item.style):
            best_style = False
        value.append(item)
    if flow_style is None:
        if dump.default_flow_style is not None:
            node.flow_style = dump.default_flow_style
        else:
            node.flow_style = best_style
    return node


### Dump

Dumper = yaml.SafeDumper

