#!/usr/bin/env python

"""exec-path.py -- execute a path query

Example: exec-path.py ???
"""

import sys
from mdb import compiler

def usage():
    print __doc__
    print 'usage: %s expression' % sys.argv[0]
    sys.exit(1)

def main(path):
    print 'result: %r' % (compiler.evaluate(path),)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        usage()
    main(sys.argv[1])
