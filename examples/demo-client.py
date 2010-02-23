#!/usr/bin/env python

"""demo-client.py -- query the demo server"""

import os, sys, xmpp, socket
from xmpp import xml

def usage():
    print __doc__
    print 'usage: %s expression' % sys.argv[0]
    sys.exit(1)

def main(query):
    client = xmpp.Client({
        'plugins': [(QueryClient, { 'query': query })],
        'username': 'user',
        'password': 'secret',
        'host': socket.gethostname()
    })
    xmpp.start([xmpp.TCPClient(client).connect('127.0.0.1', 5222)])

class QueryClient(xmpp.Plugin):

    def __init__(self, query):
        self.send(query)

    def send(self, query):
        self.iq('get', self.on_reply, self.E.query(
            { 'xmlns': 'urn:message' },
            query
        ))

    def on_reply(self, iq):
        assert iq.get('type') == 'result'
        data = xml.child(iq, '{urn:message}query/text()')
        print 'Got reply:', data
        xmpp.loop().stop()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        usage()
    main(sys.argv[1])
