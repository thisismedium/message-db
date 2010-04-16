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
    'User', 'identifier', 'email', 'password',
    'list_users', 'get_user', 'valid_user', 'is_admin',
    'user', 'set_user', 'authenticator', 'author',
    'user_delta', 'make_user', 'save_user', 'remove_user',
    'LocalAuth'
)


### Users

avro.require('auth.json')

class User(_tree.content('User')):

    def __init__(self, **kw):
        super(User, self).__init__(**kw)
        self.password = make_password(self.name, self.password)

    def __repr__(self):
        return '%s <%s>' % (self.display_name(), self.email)

    def display_name(self):
        return self.full_name or self.name

    def update(self, seq=(), **kw):
        pwd = update(kw, seq).pop('password', '')
        if pwd:
            kw['password'] = make_password(kw.get('name', self.name), pwd)
        return super(User, self).update(kw)

class identifier(avro.primitive('M.identifier', avro.string)):
    """A name that must be lowercase and not have spaces or odd
    characters."""

    def __new__(cls, value):
        return super(identifier, cls).__new__(cls, cls.normalize(value))

    @staticmethod
    def normalize(value):
        return value.lower()

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

def get_user(name, require=False):
    ## FIXME: Change this to get from the repository once repositories
    ## and branches are implemented.
    user = api.get(_tree.Key.make(User, name=identifier.normalize(name)))
    if require and not user:
        raise ValueError('User does not exist: %r.' % name)
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

def init_auth(auth=None, service=None, host=None):
    """Initialize the authentication service.  See load.init()."""

    auth = auth or LocalAuth(service, host)
    CURRENT_AUTH.set(auth)
    return auth

def author():
    """When transactions are committed to a zipper, this name is
    recorded with the changes."""

    user = CURRENT_USER.value
    if user is not fluid.UNDEFINED:
        return user.display_name()
    return 'Anonymous <nobody@example.net>'


### User Manipulation

def user_delta(message):
    return api.delta(message)

def make_user(_kind=None, **kw):
    name = kw.get('name')
    if not name:
        raise TypeError("Missing required 'name' parameter.")
    if get_user(name):
        raise NameError('User already exists: %r.' % name)
    return api.new(_kind or User, update(kw, key_name=name))

def save_user(user, *args, **kw):
    if not user:
        raise NameError('User does not exist: %r.' % user)
    return api.update(user.update(*args, **kw))

def remove_user(user):
    if not (user and get_user(user.name)):
        raise NameError('User does not exist: %r.' % user)
    api.delete(user)


### Authentication

PASSWORD_TYPE = sasl.DigestMD5Password

def make_password(name, passwd):
    auth = authenticator()
    if not auth:
        raise ValueError('No active authenticator.')
    return password(PASSWORD_TYPE.make(auth, name, passwd))

class LocalAuth(sasl.Authenticator):
    def __init__(self, service=None, host=None):
        self._service = service or 'xmpp'
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

    def get_password(self, name):
        user = get_user(name)
        return user and user.password

    def _compare_passwords(self, name, attempt, stored):
        try:
            return PASSWORD_TYPE.make(self, name, attempt) == stored
        except sasl.PasswordError:
            return False

def get_host():
    import socket
    return socket.gethostname()
