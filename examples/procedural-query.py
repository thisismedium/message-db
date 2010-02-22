#!/usr/bin/env python

"""procedural-query.py -- use procedures to query a tree"""

import os, sys, mdb
from mdb import datastore as ds

def main(data):
    run(ds.init(data, os.path.basename(data)))

# from pykk.lib import profile
# @profile.profiled(restrict=20)
def run(root):
    query = mdb.Query(root).children('news').children('Page')
    for item in query:
        print item

if __name__ == '__main__':
    main(os.path.join(os.path.dirname(__file__), 'demo'))
