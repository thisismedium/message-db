from __future__ import absolute_import
from .interface import *

class _Back(object):
    """Auto-import a backing-store driver by name.

    >>> back.fsdir('/tmp')
    fsdir('/tmp')
    """

    __slots__ = ()

    def __getattr__(self, name):
        from importlib import import_module
        try:
            ## There should be a module called name in this package
            ## with a class called name defined in it.
            mod = import_module('.' + name, __name__)
            return getattr(mod, name)
        except ImportError:
            raise AttributeError(name)

back = _Back()
