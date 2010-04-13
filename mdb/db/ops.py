## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""ops -- additional path query operations"""

## Note: __all__ is not defined.  Follow the convention of prefixing
## private bindings with an underscore; this module is imported
## entirely into the global path evaluation context.

from __future__ import absolute_import
from .. import avro as _a

## Support query_ast.NamedTest
kind = _a.get_type

def get(*keys):
    """Abandon the current context; produce a sequence of items
    associated with the given keys."""

    ## FIXME: this looks broken.  Make a unit test!
    return _ds.get([k for f in keys for k in f])
