#!/usr/bin/env python

"""demo-server.py -- serve up a demo content tree"""

import os, sys, mdb, json, xmpp, collections as coll
from xmpp import xml
from mdb import datastore as ds

def main(data):
    top = ds.init(data, os.path.basename(data))
    server = xmpp.Server({
        'plugins': [(QueryServer, { 'root': top })],
        'users': { 'user': 'secret' },
    })
    print 'Waiting for clients...'
    xmpp.start([xmpp.TCPServer(server).bind('127.0.0.1', 5222)])

class QueryServer(xmpp.Plugin):

    def __init__(self, root):
        self.root = root

    @xmpp.iq('{urn:message}query')
    def message_query(self, iq):
        assert iq.get('type') == 'get'
        expr = xml.child(iq, '{urn:message}query/text()')
        return self.iq('result', iq, self.E.query(
            { 'xmlns': 'urn:message'},
            dumps(mdb.query(expr)(self.root))
        ))

def dumps(obj):
    return json.dumps(dumps_value(obj))

def dumps_value(obj):
    if isinstance(obj, ds.Item):
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
    if isinstance(value, ds.Item):
        value = value.key()
    if isinstance(value, ds.Key):
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
