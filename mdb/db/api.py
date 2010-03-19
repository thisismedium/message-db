## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""api -- high-level data operations"""

from __future__ import absolute_import
from . import stm, models

__all__ = ('get', )

def get(key):
    if key is None or isinstance(key, models.Model):
        return key
    elif isinstance(key, (basestring, models.Key)):
        return stm.current_journal().get(_key(key))
    else:
        return stm.current_journal().mget(_key(k) for k in key)

def _delete(key):
    if isinstance(key, models.Model):
        key = key.key
    if isinstance(key, (basestring, models.Key)):
        stm.delete(key)
    else:
        for k in key: stm.delete(k)

def _key(key):
    if isinstance(key, models.Key):
        return key
    return Key(key)
