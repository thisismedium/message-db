## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""interfaces -- storage interfaces"""

from __future__ import absolute_import

__all__ = ('StoreError', 'NotStored')

class StoreError(Exception):
    """A generic catch-all for storage errors."""

class NotFound(StoreError):
    """Raised when an operation is performed on a non-existant
    item."""

class NotStored(StoreError):
    """Raised when an item cannot be stored."""
