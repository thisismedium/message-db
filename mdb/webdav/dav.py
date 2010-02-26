## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""dav -- wsgidav integration"""

from __future__ import absolute_import
import httplib
from wsgidav import wsgidav_app
from . import wsgi

__all__ = ('DAVHandler', 'App', 'Filesystem')

def DAVHandler(**settings):
    return wsgi.WSGIContainer(App(**settings))

def App(**settings):
    """A WSGI app that supports WebDAV."""

    config = dict(wsgidav_app.DEFAULT_CONFIG, **settings)
    return wsgidav_app.WsgiDAVApp(config)

def Filesystem(root):
    """Create a filesystem provider at a given path."""

    from wsgidav.fs_dav_provider import FilesystemProvider
    return FilesystemProvider(root)
