#!/usr/bin/env python

"""demo-client.py -- query the demo server

Example:

## In one terminal:
> python demo-server.py

## In another terminal:
> python demo-client.py get-item '*'
> python demo-client.py set-item '[{ "method": "create", "data": { "_kind": "Page", "_path": "/example" } }]'
> python demo-client.py set-item '[{ "method": "save", "data": { "_path": "/example", "title": "Hello, world!" } }]'
> python demo-client.py get-item '*'
> python demo-client.py set-item '[{ "method": "remove", "data": { "_path": "/example" } }]'
> python demo-client.py get-item '*'
"""

import os, sys, xmpp, socket, base64
from xmpp import xml

def usage():
    print __doc__
    print 'usage: %s expression' % sys.argv[0]
    sys.exit(1)

def main(method, query):
    client = xmpp.Client({
        'plugins': [(QueryClient, { 'method': method, 'query': query })],
        'username': 'user',
        'password': 'secret',
        'host': 'localhost'
    })
    xmpp.start([xmpp.TCPClient(client).connect('127.0.0.1', 5222)])

class QueryClient(xmpp.Plugin):

    def __init__(self, method, query):
        self.send(method, query)

    def send(self, name, query):
        type = 'get'
        if '-' in name:
            (type, name) = name.split('-')
        self.iq(type, self.on_reply, self.E(
            name,
            { 'xmlns': 'urn:M' },
            base64.b64encode(query)
        ))

    def on_reply(self, iq):
        assert iq.get('type') == 'result'
        print 'Got reply:', base64.b64decode(iq[0].text)
        xmpp.loop().stop()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        usage()
    main(*sys.argv[1:])
