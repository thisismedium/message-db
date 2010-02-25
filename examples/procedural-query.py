#!/usr/bin/env python

"""procedural-query.py -- use a procedural API to construct a query."""

import os, sys, mdb
from mdb import db

def main(data):
    run(db.init(data, os.path.basename(data)))

def run(root):
    query = db.query(root).children('news').children('Page')
    for item in query:
        print item

if __name__ == '__main__':
    main(os.path.join(os.path.dirname(__file__), 'demo'))
