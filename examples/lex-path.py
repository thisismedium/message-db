#!/usr/bin/env python

"""lex-path.py -- tokenize a path query

Example: lex-path.py 'child::para[position()>1]'
"""

import sys
from mdb.query import parse

def usage():
    print __doc__
    print 'usage: %s expression' % sys.argv[0]
    sys.exit(1)

def main(path):
    (_, lex) = parse.Lexer()
    lex.input(path)
    while True:
        tok = lex.token()
        if not tok:
            break
        print tok

if __name__ == '__main__':
    if len(sys.argv) != 2:
        usage()
    main(sys.argv[1])
