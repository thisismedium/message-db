## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""interfaces -- storage interfaces"""

from __future__ import absolute_import
from md import abc

__all__ = ('StoreError', 'NotStored', 'NotFound', 'Logical')

class StoreError(Exception):
    """A generic catch-all for storage errors."""

class NotFound(StoreError):
    """Raised when an operation is performed on a non-existant
    item."""

class NotStored(StoreError):
    """Raised when an item cannot be stored."""

class Logical(object):
    __metaclass__ = abc.ABCMeta

    @property
    def _prefix(self):
        """A string prefix applied to all keys."""

    @property
    def _back(self):
        """The real backing store."""
