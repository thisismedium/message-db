## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""tests -- unit tests"""

from __future__ import absolute_import
import os, unittest, sasl
from md.prelude import *
from .. import avro
from . import *; from . import _tree

def load():
    from . import load

    data = os.path.join(os.path.dirname(__file__), 'test')
    return load.memory('test', data)

class TestTree(unittest.TestCase):

    def setUp(self):
        self.root = load()

    def test_key(self):
        key = self.root.key
        data = unicode(self.root.key)
        self.assertEqual(avro.cast(data, Key), key)
        self.assertEqual(avro.cast(data, _tree._folder), key)


    def test_root(self):
        self.assertEqual(self.root.name, 'test')
        self.assertEqual(type(self.root.contents), _tree._content)
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
        self.root = load()

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
        from . import load

        self.zs = load.memory('test')
        init_auth(LocalAuth())
        self._create()

        import logging
        sasl.log.setLevel(logging.CRITICAL)

    def _create(self):
        with user_delta('Add users') as d:
            self.foo = make_user(name='Foo', email='foo@example.net', password='secret')
            self.bar = make_user(name='Bar', email='bar@example.net', password='terces', admin=True)
            d.checkpoint()

    def test_props(self):
        self.assertEqual(repr(self.foo), 'Foo <foo@example.net>')
        self.assertEqual(self.foo.password, '{DIGEST-MD5}ccd954df41d8f2e1d600954840d3f34c')
        self.assertEqual(is_admin(self.foo), False)
        self.assertEqual(is_admin(self.bar), True)

    def test_get(self):
        self.assertEqual(get_user('foo@example.net'), self.foo)
        self.assertEqual(get_user('baz@nowhere.com'), Undefined)

    def test_list(self):
        self.assertEqual(sorted(u.email for u in list_users()),
                         ['bar@example.net', 'foo@example.net'])

    def test_update(self):
        with user_delta('Update foo') as d:
            save_user(self.foo, admin=True, password='new-password')
            d.checkpoint()

        foo = get_user('foo@example.net')
        self.assertEqual(is_admin(foo), True)
        self.assertEqual(foo.password, '{DIGEST-MD5}238eda908da86e205cb03b34bc02869c')

    def test_remove(self):
        with user_delta('Delete bar') as d:
            remove_user(self.bar)
            d.checkpoint()

        self.assertEqual(get_user('bar@example.net'), Undefined)

    def test_login(self):
        self.assert_(self._login('foo@example.net', 'secret'))
        self.assert_(not self._login('foo@example.net', 'bad-password'))

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
