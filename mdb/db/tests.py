## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""tests -- unit tests"""

from __future__ import absolute_import
import os, unittest, sasl
from md.prelude import *
from .. import avro
from . import *

def load_test_data():
    data = os.path.join(os.path.dirname(__file__), 'test')
    init('test', 'memory:', load='yaml:%s' % data)
    return root()

class TestTree(unittest.TestCase):

    def setUp(self):
        self.root = load_test_data()

    def test_key(self):
        key = self.root.key
        data = unicode(self.root.key)
        self.assertEqual(avro.cast(data, Key), key)
        self.assertEqual(avro.cast(data, tree._folder), key)


    def test_root(self):
        self.assertEqual(self.root.name, 'test')
        self.assertEqual(type(self.root.contents), tree._content)
        self.assertEqual(self.root.contents.keys(), ['about', 'news'])
        self.assertEqual(str(self.root.key), str(self.root.child('about').folder))

    def test_structure(self):
        self.assertEqual(self._structure(self.root),
                         ('test',
                          ['about',
                           ('news', ['article-1', 'article-2', 'article-3'])]))

    def test_path(self):
        self.assertEqual(path(resolve('/news/article-2')), '/news/article-2')

    def test_add(self):
        with delta('Add Page') as d:
            add(self.root, make(Page, name='hello'))
            d.checkpoint()
        self.assertEqual([c.name for c in self.root], ['about', 'news', 'hello'])

    def test_update(self):
        with delta('Update Page') as d:
            about = resolve('/about')
            about.description = 'Changed description!'
            d.checkpoint()
        self.assertEqual(resolve('/about').description, 'Changed description!')

    def test_remove(self):
        with delta('Remove article-2') as d:
            remove(resolve('/news/article-2'))
            d.checkpoint()

        self.assertEqual(self._structure(resolve('/news')),
                         ('news', ['article-1', 'article-3']))

    def _structure(self, top):
        if not isinstance(top, Folder):
            return top.name
        return (top.name, [self._structure(c) for c in top])

class TestQuery(unittest.TestCase):

    def setUp(self):
        self.root = load_test_data()

    def test_root(self):
        self._check('/', (Site, 'test'))
        self._check('.', (Site, 'test'))
        self._check('*', (Page, 'about'), (Folder, 'news'))

    def test_simple(self):
        self._check('/news', (Folder, 'news'))
        self._check('/news/*',
                    (Page, 'article-1'), (Page, 'article-2'), (Page, 'article-3'))

    def test_axis(self):
        self._check('//.',
                    (Site, 'test'), (Page, 'about'), (Folder, 'news'),
                    (Page, 'article-1'), (Page, 'article-2'), (Page, 'article-3'))
        self._check('/news/article-1/parent::*', (Folder, 'news'))
        self._check('/news/article-2/sibling::*', (Page, 'article-1'), (Page, 'article-3'))
        self._check('/news/article-2/preceding-sibling::*', (Page, 'article-1'))
        self._check('/news/article-2/following-sibling::*', (Page, 'article-3'))

    def test_kind(self):
        self._check('/Folder', (Folder, 'news'))
        self._check('//Page',
                    (Page, 'about'), (Page, 'article-1'),
                    (Page, 'article-2'), (Page, 'article-3'))

    def test_ops(self):
        self._check('get(%r)' % str(self.root.key), (Site, 'test'))

    def _check(self, path, *result):
        self.assertEqual(tuple((type(r), r.name) for r in query(path)), result)

class TestAuth(unittest.TestCase):

    def setUp(self):
        import logging

        sasl.log.setLevel(logging.CRITICAL)
        self.zs = init('test', 'memory:', create=self._create)

    def _create(self):
        with user_transaction('Add users'):
            self.foo = make_user(name='foo', email='foo@example.net', password='secret')
            self.bar = make_user(name='bar', email='bar@example.net', password='terces', admin=True)

    def test_props(self):
        self.assertEqual(repr(self.foo), 'foo <foo@example.net>')
        self.assertEqual(str(self.foo.password), '{DIGEST-MD5}bfeff2d37e161fad556dd382f313dc00')
        self.assertEqual(is_admin(self.foo), False)
        self.assertEqual(is_admin(self.bar), True)

    def test_create(self):
        with user_transaction('Create baz'):
            baz = make_user(name='baz', email='baz@example.net', password='hidden')

        self.assertEqual(avro.dumps(baz),
                         '{"_key": "DE0uVXNlcgIGYmF6", "_kind": "User", "admin": false, "branch": "baz", "email": "baz@example.net", "full_name": "", "name": "baz", "password": "", "roles": []}')

        with user_transaction('Create baz'):
            self.assertRaises(NameError, lambda: make_user(name='Baz', email='baz@example.net', password='hidden'))

    def test_get(self):
        self.assertEqual(get_user('foo'), self.foo)
        self.assertEqual(get_user('Foo'), self.foo)
        self.assertEqual(get_user('baz'), Undefined)

    def test_list(self):
        self.assertEqual(sorted(u.name for u in list_users()),
                         ['bar', 'foo'])

    def test_update(self):
        with user_transaction('Update foo'):
            save_user(self.foo, admin=True, password='new-password')

        self.assertEqual(avro.dumps(self.foo),
                         '{"_key": "DE0uVXNlcgIGZm9v", "_kind": "User", "admin": true, "branch": "foo", "email": "foo@example.net", "full_name": "", "name": "foo", "password": "", "roles": []}')

        foo = get_user('foo')
        self.assertEqual(is_admin(foo), True)
        self.assertEqual(str(foo.password), '{DIGEST-MD5}236856eb15d36cadb24338d244d2fa26')

    def test_remove(self):
        with user_transaction('Delete bar'):
            remove_user(self.bar)
            self.assertRaises(NameError, lambda: remove_user(self.bar))
        self.assertEqual(get_user('bar'), Undefined)

    def test_login(self):
        self.assert_(self._login('foo', 'secret'))
        self.assert_(not self._login('foo', 'bad-password'))

    def _login(self, user, pwd, mech=None):
        client = self._client(user, pwd)
        mech = mech or sasl.DigestMD5
        return self._negotiate(mech(authenticator()), mech(client))

    def _negotiate(self, server, client):
        sk = server.challenge()
        ck = client.respond(sk.data)

        while not (sk.finished() and ck.finished()):
            if not sk.finished():
                sk = sk(ck.data)

            if not ck.finished():
                ck = ck(sk.data)

        return (ck.success() or ck.confirm()) and sk.success()

    def _client(self, user, pwd):
        return sasl.SimpleAuth(
            sasl.DigestMD5Password,
            {},
            lambda: user,
            lambda: pwd,
            lambda: authenticator().service_type(),
            lambda: authenticator().host()
        )

class TestBranch(unittest.TestCase):

    def setUp(self):
        self.zs = init('test', 'memory:')

    def _branches(self, *names):
        self.assertEqual(list(names), sorted(b.name for b in branches()))

    def _json(self, name, data):
        self.assertEqual(avro.dumps(get_branch(name)), data)

    def test_list(self):
        self._branches('live', 'staging')

    def test_get(self):
        self._json('staging', '{"_key": "EE0uYnJhbmNoAg5zdGFnaW5n", "_kind": "branch", "_name": "staging", "config": {}, "owner": "anonymous", "publish": "live"}')

    def test_save(self):
        with repository_transaction('Update "staging".'):
            save_branch(get_branch('staging'), owner='foo')
        self._json('staging', '{"_key": "EE0uYnJhbmNoAg5zdGFnaW5n", "_kind": "branch", "_name": "staging", "config": {}, "owner": "foo", "publish": "live"}')

    def test_remove(self):
        with repository_transaction('Remove "live".'):
            remove_branch(get_branch('staging'))

        self._branches('live')
        with repository_transaction('Make "staging".'):
            api.make_branch('staging')
        self._branches('live', 'staging')

    def test_use(self):
        self.assertEqual('staging', source().name)
        self.assertEqual('live', source().publish)

        use('live')
        self.assertEqual('live', source().name)
        self.assertEqual('', source().publish)

        self.assertRaises(RepoError, lambda: use('foo'))

    def test_user(self):

        ## Check that creating a user creates a branch as well.
        with user_transaction('Make "foo" user.'):
            foo = make_user(name='foo', email='foo@example.net', password='secret')
        self._branches('foo', 'live', 'staging')

        ## Make sure the branch and user have references to each
        ## other.
        self.assertEqual('foo', foo.branch)
        fb = open_branch(foo.branch)
        self.assertEqual('foo', fb.owner)
        self.assertEqual('staging', fb.publish)

        ## Add some data to the new branch.
        use('foo')
        with delta('Add "foo" data.') as d:
            make(Item, name='a')
            d.checkpoint()
        self.assertEqual(['a'], list(i.name for i in source().find(Item)))

        ## Make another user; force it to share the "foo" branch.
        with user_transaction('Make "bar" user.'):
            bar = make_user(name='bar', email='bar@example.net', password='secret', branch='foo')
        self._branches('foo', 'live', 'staging')

        ## Verify that that the original branch wasn't destroyed.
        use('foo')
        self.assertEqual(['a'], list(i.name for i in source().find(Item)))
