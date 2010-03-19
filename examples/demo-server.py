#!/usr/bin/env python

"""demo-server.py -- serve up a demo content tree

This example answers path queries sent over XMPP.  A content tree is
built from the YAML data in the "demo" directory.  When a path query
is received the query is evaluated against the root of the content
tree and matching items are returned in a JSON format.
"""

import os, sys, json, xmpp, base64, hashlib, logging
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

    @xmpp.stanza('presence')
    def presence(self, elem):
        """No-op on presence so strophe doesn't fail."""

    @xmpp.iq('{urn:M}item')
    def M_item(self, iq):
        return self._dispatch(iq)

    def get_item(self, iq, query):
        return self._dumps(iq, db.query(query)(self.root))

    def set_item(self, iq, data):
        result = coll.omap()
        with db.transaction(iq[0].get('message', 'set items')):
            for action in loads(data):
                method = getattr(self, 'set_item_%s' % action['method'], None)
                if not method:
                    raise ValueError('Bad action: %r.' % action)
                res = method(action['data'])
                result.setdefault(res.key, res)
        return self._dumps(iq, result.values())

    def set_item_create(self, item):
        path = item['_path']; kind = db.model(item['_kind'])
        attr = dict(without_underscores(item))
        attr.setdefault('name', os.path.basename(path))
        return db.add_child(
            db.resolve(os.path.dirname(path)),
            kind(**attr)
        )

    def set_item_save(self, item):
        return db.resolve(item['_path']).update(without_underscores(item))

    def set_item_remove(self, item):
        return db.remove(db.resolve(item['_path']))

    @xmpp.iq('{urn:M}query')
    def M_query(self, iq):
        return self._dispatch(iq)

    @xmpp.iq('{urn:M}schema')
    def message_schema(self, iq):
        return self._dispatch(iq)

    def get_schema(self, iq, name):
        result = dumps(db.model(name))
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
    return json.loads(data)

def dumps(obj):
    return json.dumps(dumps_value(obj))

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
        _kind=obj.kind,
        _key=str(obj.key),
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
