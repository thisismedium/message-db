## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""auth -- users and authentication"""

from __future__ import absolute_import
import sasl
from md.prelude import *
from md import fluid
from .. import avro
from . import _tree, api

__all__ = (
    'User', 'email', 'password',
    'list_users', 'get_user', 'valid_user', 'is_admin',
    'user', 'set_user', 'authenticator', 'init_auth',
    'user_delta', 'make_user', 'save_user', 'remove_user',
    'LocalAuth'
)


### Users

avro.require('auth.json')

class User(_tree.content('User')):

    def __init__(self, **kw):
        super(User, self).__init__(**kw)
        self.password = make_password(self.email, self.password)

    def __repr__(self):
        return '%s <%s>' % (self.name, self.email)

    def update(self, seq=(), **kw):
        pwd = update(kw, seq).pop('password', '')
        if pwd:
            kw['password'] = make_password(kw.get('email', self.email), pwd)
        return super(User, self).update(kw)

class email(avro.primitive('M.email', avro.string)):
    """An email address."""

class password(avro.primitive('M.password', avro.string)):
    """A password."""

    def __repr__(self):
        return '******'

    def __json__(self):
        return ''

_roles = avro.set(avro.string)


### User Traversal

def list_users():
    return api.find(User)

def get_user(email, require=False):
    ## FIXME: Change this to get from the repository once repositories
    ## and branches are implemented.
    user = api.get(_tree.Key.make(User, name=email))
    if require and not user:
        raise ValueError('User does not exist: %r.' % email)
    return user

def valid_user(obj):
    if isinstance(obj, User):
        return obj
    return get_user(obj, True)

def is_admin(obj):
    return valid_user(obj).admin


### Dynamic Environment

CURRENT_USER = fluid.cell(fluid.UNDEFINED, validate=valid_user, type=fluid.acquired)
user = fluid.accessor(CURRENT_USER)

def set_user(user):
    CURRENT_USER.set(user)
    return user

CURRENT_AUTH = fluid.cell(None, type=fluid.acquired)
authenticator = fluid.accessor(CURRENT_AUTH)

def init_auth(auth):
    CURRENT_AUTH.set(auth)
    return auth


### User Manipulation

def user_delta(message):
    return api.delta(message)

def make_user(**kw):
    return api.new(User, update(kw, key_name=kw.get('email')))

def save_user(user, *args, **kw):
    return api.update(user.update(*args, **kw))

def remove_user(user):
    api.delete(user)


### Authentication

PASSWORD_TYPE = sasl.DigestMD5Password

def make_password(email, passwd):
    auth = authenticator()
    if not auth:
        raise ValueError('No active authenticator.')
    return PASSWORD_TYPE.make(auth, email, passwd)

class LocalAuth(sasl.Authenticator):
    def __init__(self, service=None, host=None):
        self._service = service or 'message'
        self._host = host or get_host()

    def service_type(self):
        return self._service

    def host(self):
        return self._host

    def realm(self):
        return self._host

    def username(self):
        raise NotImplemented

    def password(self):
        raise NotImplemented

    def get_password(self, email):
        return valid_user(email).password

    def _compare_passwords(self, email, attempt, stored):
        try:
            return PASSWORD_TYPE.make(self, email, attempt) == stored
        except sasl.PasswordError:
            return False

def get_host():
    import socket
    return socket.gethostname()
