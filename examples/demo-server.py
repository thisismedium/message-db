#!/usr/bin/env python

"""demo-server.py -- serve up a demo content tree

This example answers path queries sent over XMPP.  A content tree is
built from the YAML data in the "demo" directory.  When a path query
is received the query is evaluated against the root of the content
tree and matching items are returned in a JSON format.
"""

import os, sys, json, xmpp, base64, hashlib
from md import collections as coll
from xmpp import xml
from mdb import db

def main(data):
    top = db.setup(data, os.path.basename(data))
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
        return self._get(iq, self._query)

    def _query(self, expr):
        return db.query(expr)(self.root)

    @xmpp.iq('{urn:message}schema')
    def message_schema(self, iq):
        return self._get(iq, db.model)

    def _get(self, iq, method):
        try:
            if iq.get('type') != 'get':
                raise ValueError("Expected type 'get', not %r." % iq.get('type'))
            expr = base64.b64decode(iq[0].text)
        except (ValueError, AttributeError) as exc:
            return self.error(iq, 'modify', 'bad-request', str(exc))

        try:
            result = dumps(method(expr))
            match = hashlib.md5(result).hexdigest()
            self.iq('result', iq, self.E.query({
                'xmlns': 'urn:message',
                'match': match
            }, result))
        except SyntaxError as exc:
            self.error(iq, 'modify', 'undefined-condition', str(exc))

    @xmpp.stanza('presence')
    def presence(self, elem):
        pass

def dumps(obj):
    return base64.b64encode(json.dumps(dumps_value(obj)))

def dumps_value(obj, rec=None):
    if isinstance(obj, (type(None), bool, int, float, basestring)):
        return obj
    elif isinstance(obj, type):
        if issubclass(obj, db.Model):
            return dumps_model(obj)
        else:
            return obj.__name__
    elif isinstance(obj, db.Item):
        return dumps_item(obj)
    elif isinstance(obj, db.Key):
        return str(obj)
    elif isinstance(obj, (coll.Sequence, coll.Iterator)):
        return map(rec or dumps_value, obj)
    elif isinstance(obj, (coll.Tree, coll.OrderedMap)):
        return [map(rec or dumps_value, i) for i in obj.iteritems()]
    return obj

def dumps_item(obj):
    return dict(
        ((n, dumps_property(v)) for (n, v) in db.describe(obj)),
        kind=obj.kind,
        key=str(obj.key),
        _path=db.path(obj)
    )

def dumps_property(value):
    if isinstance(value, db.Item):
        value = value.key
    return dumps_value(value, dumps_property)

def dumps_model(value):
    return {
        'type': 'record',
        'name': value.kind,
        'fields': list(
            dict(
                (dumps_value(k), (dumps_value(v)))
                for (k, v) in db.describe(p)
                if v
            )
            for p in db.properties(value).itervalues()
        )
    }

if __name__ == '__main__':
    main(os.path.join(os.path.dirname(__file__), 'demo'))
