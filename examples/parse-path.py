#!/usr/bin/env python

"""parse-path.py -- generate an abstract syntax tree for a path query

Example: parse-path.py 'child::para[position()>1]'
"""

import sys, optparse
from mdb.query import parse

def usage():
    print __doc__
    print 'usage: %s expression' % sys.argv[0]
    sys.exit(1)

def run(path, debug=False):
    print parse.path(path, debug=debug)

def main():
    opt = optparse.OptionParser()
    opt.add_option('-d', dest='debug', action='store_true', default=False)
    (options, args) = opt.parse_args()
    if len(args) != 1:
        usage()
    run(*args, **options.__dict__)

if __name__ == '__main__':
    main()
