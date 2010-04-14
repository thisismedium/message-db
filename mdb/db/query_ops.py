## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""ops -- additional path query operations"""

## Note: __all__ is not defined.  Follow the convention of prefixing
## private bindings with an underscore; this module is imported
## entirely into the global path evaluation context.

from __future__ import absolute_import
from . import _tree, api as _api

## Support query_ast.NamedTest
def kind(name):
    return _tree.get_type('M.%s' % name)

def get(*keys):
    """Abandon the current context; produce a sequence of items
    associated with the given keys."""

    return _api.get([k for f in keys for k in f])
