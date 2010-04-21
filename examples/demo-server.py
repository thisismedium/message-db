#!/usr/bin/env python

"""demo-server.py -- serve up a demo content tree

This example answers path queries sent over XMPP.  A content tree is
built from the YAML data in the "demo" directory.  When the server
processes a request, the result is returned in a JSON format.

Usage: demo-server.py [options]

   -h  Show this message.
   -v  Show extra output.
"""

import os, sys, xmpp, base64, hashlib, logging, getopt
from md import collections as coll
from xmpp import xml
from mdb import db, avro

VERBOSE = False

def main(data):
    opts = dict(getopt.getopt(sys.argv[1:], 'vh')[0])

    if '-v' in opts:
        verbose()
    elif '-h' in opts:
        return help()

    start('fsdir:%s' % data, 'yaml:%s' % os.path.dirname(data))

def help():
    print __doc__
    sys.exit()

def verbose():
    global VERBOSE

    VERBOSE = True
    xmpp.log.setLevel(logging.DEBUG)

def start(data, load):
    db.init('demo', data, load=load, create=setup, host='localhost')

    auth = db.authenticator()
    server = xmpp.Server({
        'plugins': [QueryServer],
        'auth': auth,
        'jid': auth.host()
    })

    print 'Waiting for clients...'
    xmpp.start([xmpp.TCPServer(server).bind('127.0.0.1', 5222)])

def setup():
    with db.user_transaction('Create users'):
        db.make_user(name='user', password='secret', email='user@localhost')

class QueryServer(xmpp.Plugin):

    def __init__(self):
        self.root = db.root()

    @xmpp.stanza('presence')
    def presence(self, elem):
        """No-op on presence so strophe doesn't fail."""

    ## Content

    @xmpp.iq('{urn:M}item')
    def M_item(self, iq):
        return self._dispatch(iq)

    def get_item(self, iq, query):
        return self._dumps(iq, db.query(query, self.root))

    def set_item(self, iq, data):
        return self._change('set_item', iq, data, db.delta, 'update items')

    def set_item_create(self, data):
        path = data['_path']
        kind = db.get_type(data['_kind'])

        attr = dict(without_underscores(data))
        attr.setdefault('name', os.path.basename(path))

        folder = db.resolve(os.path.dirname(path))
        yield db.make(kind, folder=folder, **attr)
        yield folder

    def set_item_save(self, data):
        yield db.save(self._item(data), without_underscores(data))

    def set_item_remove(self, data):
        yield db.remove(self._item(data))

    def _item(self, data):
        return db.resolve(data['_path'])

    ## Schema

    @xmpp.iq('{urn:M}schema')
    def message_schema(self, iq):
        return self._dispatch(iq)

    def get_schema(self, iq, name):
        result = dumps(db.get_type(name))
        match = hashlib.md5(result).hexdigest()
        return self._result(iq, result, match=match)

    ## Users

    @xmpp.iq('{urn:M}user')
    def message_user(self, iq):
        return self._dispatch(iq)

    def get_user(self, iq, data):
        return self._dumps(iq, db.get_user(data) if data else db.list_users())

    def set_user(self, iq, data):
        return self._change('set_user', iq, data, db.user_transaction, 'update users')

    def set_user_create(self, data):
        kind = data.get('_kind')
        attr = dict(without_underscores(data))
        yield db.make_user(kind and db.get_type(kind), **attr)

    def set_user_save(self, data):
        yield db.save_user(self._user(data), without_underscores(data))

    def set_user_remove(self, data):
        db.remove_user(self._user(data))
        return ()

    def _user(self, data):
        return db.get(data['_key'])

    ## Branches

    @xmpp.iq('{urn:M}branch')
    def message_branch(self, iq):
        return self._dispatch(iq)

    def get_branch(self, iq, data):
        return self._dumps(iq, db.get_branch(data) if data else db.branches())

    def set_branch(self, iq, data):
        return self._change('set_branch', iq, data, db.repository_transaction, 'update branches')

    def set_branch_create(self, data):
        name = data.pop('_name')
        attr = dict(without_underscores(data))
        yield db.make_branch(name, **attr)

    def set_branch_save(self, data):
        yield db.save_branch(self._branch(data), without_underscores(data))

    def set_branch_remove(self, data):
        db.remove_branch(self._branch(data))
        return ()

    def _branch(self, data):
        key = data.get('_key')
        return db.get(key) if key else db.get_branch(data['_name'])

    ## Private

    def _dispatch(self, iq):
        """Something resembling an HTTP method dispatcher."""

        try:
            name = iq[0].get('method', iq.get('type')).lower()
            text = iq[0].text
            data = text and base64.b64decode(text)
            if VERBOSE:
                print 'REQUEST (%s/%s):' % (iq.get('type'), iq.get('id')), repr(data)
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

    def _change(self, prefix, iq, data, delta, message):
        result = coll.omap()
        with delta(iq.get('message', message)) as d:
            for action in loads(data):
                method = getattr(self, '%s_%s' % (prefix, action['method']), None)
                if not method:
                    raise ValueError('Bad action: %r.' % action)
                for affected in method(action['data']):
                    result.setdefault(affected.key, affected)
            d and d.checkpoint()
        return self._dumps(iq, result.values())

    def _result(self, iq, data, **attr):
        """Create a result for _dispatch."""

        attr.setdefault('xmlns', 'urn:M')
        if VERBOSE:
            print 'RESULT (%s/%s):' % (iq.get('type'), iq.get('id')), repr(data)
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
    main(os.path.join(os.path.dirname(__file__), 'demo', 'demo.data'))
