#!/usr/bin/env python

"""demo-query.py -- run path queries against a demo content tree

The is like demo-server/demo-client, but is not client-server.  It
builts a content tree from the YAML in "demo" and evaluates a path
query against the root of the tree.

> python demo-query.py '//Page'
"""

import os, sys, mdb
from mdb import db

def usage():
    print __doc__
    print 'usage: %s expression' % sys.argv[0]
    sys.exit(1)

def main(data, expr):
    db.load.fsdir(data, os.path.basename(data))
    print 'Result of %r:' % expr
    result = db.query(expr)
    if isinstance(result, tuple):
        for seq in result:
            for item in seq:
                print '    %r' % (item,)
    else:
        for item in result:
            print '    %r' % (item,)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        usage()
    main(os.path.join(os.path.dirname(__file__), 'demo'), sys.argv[1])
