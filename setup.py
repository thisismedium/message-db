from __future__ import absolute_import
from setuptools import setup, find_packages

setup(
    name = 'mdb',
    version = '0.1',
    description = 'The Message Database, a versioned object datastore.',
    author = 'Medium',
    author_email = 'labs@thisismedium.com',
    license = 'BSD',
    keywords = 'xmpp jabber version object datastore datastore',

    packages = list(find_packages(exclude=('examples', )))
)
