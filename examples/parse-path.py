#!/usr/bin/env python

"""parse-path.py -- generate an abstract syntax tree for a path query

Example: parse-path.py 'child::para[position()>1]'
"""

import sys
from mdb import parse

def usage():
    print __doc__
    print 'usage: %s expression' % sys.argv[0]
    sys.exit(1)

def main(path):
    print parse.path(path)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        usage()
    main(sys.argv[1])
