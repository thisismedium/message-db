## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""ops -- default operations available in a path query"""

## Note: __all__ is not defined.  Follow the convention of prefixing
## private bindings with an underscore; this module is imported
## entirely into the global path evaluation context.

from __future__ import absolute_import
from . import tree as _tree

some = any
every = all

def root(seq):
    seq = list(seq)
    return unique(last(_tree.ascend(i), i) for i in seq)

def last(seq, default=None):
    item = default
    for item in seq:
        pass
    return item

def unique(seq):
    seen = set()
    for item in seq:
        if item not in seen:
            seen.add(item)
            yield item
