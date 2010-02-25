#!/usr/bin/env python

"""demo-server.py -- serve up a demo content tree

This example answers path queries sent over XMPP.  A content tree is
built from the YAML data in the "demo" directory.  When a path query
is received the query is evaluated against the root of the content
tree and matching items are returned in a JSON format.
"""

import os, sys, json, xmpp, base64, collections as coll
from xmpp import xml
from mdb import db

def main(data):
    top = db.init(data, os.path.basename(data))
    server = xmpp.Server({
        'plugins': [(QueryServer, { 'root': top })],
        'users': { 'user': 'secret' },
        'host': 'localhost'
    })
    print 'Waiting for clients...'
    xmpp.start([xmpp.TCPServer(server).bind('127.0.0.1', 5222)])

class QueryServer(xmpp.Plugin):

    def __init__(self, root):
        self.root = root

    @xmpp.iq('{urn:message}query')
    def message_query(self, iq):
        assert iq.get('type') == 'get'
        expr = base64.b64decode(xml.child(iq, '{urn:message}query/text()'))
        try:
            result = dumps(db.query(expr)(self.root))
            self.iq('result', iq, self.E.query({ 'xmlns': 'urn:message'}, result))
        except SyntaxError as exc:
            self.error(iq, 'modify', 'undefined-condition', str(exc))

    @xmpp.stanza('presence')
    def presence(self, elem):
        pass

def dumps(obj):
    return base64.b64encode(json.dumps(dumps_value(obj)))

def dumps_value(obj):
    if isinstance(obj, db.Item):
        return dumps_model(obj)
    elif isinstance(obj, (coll.Sequence, coll.Iterator)):
        return map(dumps_value, obj)
    return obj

def dumps_model(obj):
    return dict(
        ((n, dumps_property(getattr(obj, n))) for n in property_names(obj)),
        kind=obj.kind(),
        key=str(obj.key()),
    )

def dumps_property(value):
    if isinstance(value, db.Item):
        value = value.key()
    if isinstance(value, db.Key):
        return str(value)
    elif isinstance(value, list):
        return map(dumps_property, value)
    return value

def property_names(obj):
    for key in obj.properties().iterkeys():
        yield key
    for key in obj.dynamic_properties():
        yield key

if __name__ == '__main__':
    main(os.path.join(os.path.dirname(__file__), 'demo'))
