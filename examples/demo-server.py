#!/usr/bin/env python

"""demo-server.py -- serve up a demo content tree"""

import os, sys
from mdb import datastore as ds

def main(data):
    top = ds.init(data, os.path.basename(data))
    for item in ds.walk(top):
        print item, ds.path(item)

if __name__ == '__main__':
    main(os.path.join(os.path.dirname(__file__), 'demo'))
