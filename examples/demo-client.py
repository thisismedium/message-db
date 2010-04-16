#!/usr/bin/env python

"""demo-client.py -- query the demo server

Example:

## In one terminal:
./demo-server.py

## In another terminal:
./demo-client.py get-item '*'
./demo-client.py set-item '[{ "method": "create", "data": { "_kind": "Page", "_path": "/example" } }]'
./demo-client.py set-item '[{ "method": "save", "data": { "_path": "/example", "title": "Hello, world!" } }]'
./demo-client.py get-item '*'
./demo-client.py set-item '[{ "method": "remove", "data": { "_path": "/example" } }]'
./demo-client.py get-item '*'

./demo-client.py get-user ''
./demo-client.py get-user 'user'
./demo-client.py set-user '[{ "method": "create", "data": { "name": "new", "email": "new@localhost", "password": "hello" }}]'
./demo-client.py get-user ''
./demo-client.py set-user '[{ "method": "save", "data": { "_key": "DE0uVXNlcgIGbmV3", "admin": true }}]'
./demo-client.py set-user '[{ "method": "remove", "data": { "_key": "DE0uVXNlcgIGbmV3" }}]'
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
            query and base64.b64encode(query)
        ))

    def on_reply(self, iq):
        assert iq.get('type') == 'result'
        print 'Got reply:', base64.b64decode(iq[0].text)
        xmpp.loop().stop()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        usage()
    main(*sys.argv[1:])
