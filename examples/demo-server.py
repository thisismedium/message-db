#!/usr/bin/env python

"""demo-server.py -- serve up a demo content tree

This example answers path queries sent over XMPP.  A content tree is
built from the YAML data in the "demo" directory.  When a path query
is received the query is evaluated against the root of the content
tree and matching items are returned in a JSON format.
"""

import os, sys, xmpp, base64, hashlib, logging
from md import collections as coll
from xmpp import xml
from mdb import db, avro

def main(data):
    top = db.load.fsdir(data, os.path.basename(data))
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

    @xmpp.stanza('presence')
    def presence(self, elem):
        """No-op on presence so strophe doesn't fail."""

    @xmpp.iq('{urn:M}item')
    def M_item(self, iq):
        return self._dispatch(iq)

    def get_item(self, iq, query):
        return self._dumps(iq, db.query(query, self.root))

    def set_item(self, iq, data):
        result = coll.omap()
        with db.delta(iq[0].get('message', 'set items')) as delta:
            for action in loads(data):
                method = getattr(self, 'set_item_%s' % action['method'], None)
                if not method:
                    raise ValueError('Bad action: %r.' % action)
                for affected in method(action['data']):
                    result.setdefault(affected.key, affected)
            delta.checkpoint()
        return self._dumps(iq, result.values())

    def set_item_create(self, data):
        path = data['_path']
        kind = db.get_type(data['_kind'])

        attr = dict(without_underscores(data))
        attr.setdefault('name', os.path.basename(path))

        folder = db.resolve(os.path.dirname(path))
        yield db.make(kind, folder=folder, **attr)
        yield folder

    def set_item_save(self, data):
        yield db.save(db.resolve(data['_path']), without_underscores(data))

    def set_item_remove(self, data):
        yield db.remove(db.resolve(data['_path']))

    @xmpp.iq('{urn:M}query')
    def M_query(self, iq):
        return self._dispatch(iq)

    @xmpp.iq('{urn:M}schema')
    def message_schema(self, iq):
        return self._dispatch(iq)

    def get_schema(self, iq, name):
        result = dumps(db.get_type(name))
        match = hashlib.md5(result).hexdigest()
        return self._result(iq, result, match=match)

    def _dispatch(self, iq):
        """Something resembling an HTTP method dispatcher."""

        try:
            name = iq[0].get('method', iq.get('type')).lower()
            data = base64.b64decode(iq[0].text)
        except (IndexError, AttributeError) as exc:
            return self.error(iq, 'modify', 'bad-request', str(exc))

        suffix = iq[0].tag.rsplit('}', 1)[1]
        name = '_'.join((name, suffix))
        method = getattr(self, name, None)
        if method is None:
            return self.error(iq, 'cancel', 'feature-not-implemented', name)

        try:
            return method(iq, data)
        except Exception as exc:
            logging.exception('Exception while processing IQ.')
            return self.error(iq, 'modify', 'undefined-condition', str(exc))

    def _result(self, iq, data, **attr):
        """Create a result for _dispatch."""

        attr.setdefault('xmlns', 'urn:M')
        return self.iq('result', iq, self.E(
            iq[0].tag,
            attr,
            base64.b64encode(data)
        ))

    def _dumps(self, iq, value):
        """Dump a value to JSON and return it in a _response()."""

        return self._result(iq, dumps(value))

def without_underscores(value):
    return ((str(k), v) for (k, v) in value.iteritems() if not k.startswith('_'))

def loads(data):
    return avro.json.loads(data)

def dumps(obj):
    return avro.dumps(state(obj))

def state(obj, rec=None):
    if isinstance(obj, coll.Iterator):
        return map(rec or state, obj)
    return obj

if __name__ == '__main__':
    main(os.path.join(os.path.dirname(__file__), 'demo'))
