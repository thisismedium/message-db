from __future__ import absolute_import
from .interface import *

def back(name, *args, **kw):
    """Load a backing-store driver by name.

    >>> back('fsdir', '/tmp')
    fsdir('/tmp')
    """

    from importlib import import_module
    return getattr(import_module('.' + name, __name__), name)(*args, **kw)
