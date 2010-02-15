#!/usr/bin/env python

"""demo-server.py -- serve up a demo content tree"""

import os, sys, glob, yaml, contextlib, re
from google.appengine.ext import db

def main(data):
    created = init(data, os.path.basename(data))
    top = build_tree(data) if created else root()
    for item in walk(top):
        print item, path(item)
    print resolve('/news/article-1')


### Content Tree

def build_tree(path):
    root = Folder(title="Demo", key_name='root')
    for name in glob.iglob('%s/*.yaml' % path):
        with contextlib.closing(open(name, 'r')) as port:
            build_item(root, os.path.basename(name), yaml.load(port))
    return put(root)

def build_item(root, name, data):
    model = db.class_for_kind(data.pop('kind', 'Item'))
    slugs = os.path.splitext(name)[0].split('--')
    (_, item) = put(add_child(
        build_folders(root, slugs[0:-1]),
        model(**updated(data, slug=slugs[-1]))
    ))
    return item

def build_folders(top, slugs):
    for slug in slugs:
        probe = top.get_child(slug)
        if not probe:
            (_, probe) = put(add_child(
                top,
                Folder(slug=slug, title=make_title(slug))
            ))
        top = probe
    return top

def path(item):
    return '/%s' % '/'.join(reversed(list(i.slug for i in parents(item))))

def resolve(expr, top=None):
    top = top or root()
    for slug in expr.strip('/').split('/'):
        probe = top.get_child(slug)
        if not probe:
            raise ValueError('Bad expr: %r (%r has no child %r).' % (
                expr, path(top), slug
            ))
        top = probe
    return top

def root():
    return Folder.get_by_key_name('root')

def add_child(folder, child):
    if not child.is_saved():
        child.put()
    return (folder.add_child(child), child)

def walk(item):
    yield item
    if isinstance(item, Folder):
        for child in item:
            for item in walk(child):
                yield item

def parents(item):
    while item.folder:
        yield item
        item = item.folder


### Models

class Item(db.Expando):

    def __init__(self, *args, **kw):
        slug = make_slug(kw.get('title', ''))
        if not slug:
            raise ValueError('An Item requires a title.')
        kw.setdefault('slug', slug)
        super(Item, self).__init__(*args, **kw)

    def __repr__(self):
        return '<%s %s>' % (type(self).__name__, self.title)

    slug = db.StringProperty()
    title = db.StringProperty()
    folder = db.ReferenceProperty(db.Model)

class Folder(Item):
    contents = db.ListProperty(db.Key)

    def __contains__(self, name):
        return any(name == c.slug for c in self)

    def __iter__(self):
        for key in self.contents:
            yield db.get(key)

    def add_child(self, item):
        if item.slug in self:
            raise ValueError('Child alread exists: %r.' % item.slug)
        elif item.folder:
            raise ValueError('Child alread in folder: %r' % item.folder)
        self.contents.append(item.key())
        item.folder = self
        return self

    def get_child(self, name):
        return next((c for c in self if c.slug == name), None)

class Page(Item):
    description = db.StringProperty()
    content = db.TextProperty()


### Utilities

SLUG = re.compile(r'[^a-z0-9]+')
def make_slug(name):
    return SLUG.sub('-', name.lower()).strip('-')

def make_title(slug):
    return slug.replace('-', ' ').title()

def put(items):
    db.put(items)
    return items

def updated(data, *args, **kw):
    data.update(*args, **kw)
    return data

def init(path, app_id):
    from google.appengine.api import datastore_file_stub
    from google.appengine.api import apiproxy_stub_map

    os.environ['APPLICATION_ID'] = app_id
    path = os.path.join(path, '%s.data' % app_id)
    created = not os.path.exists(path)
    apiproxy_stub_map.apiproxy.RegisterStub(
        'datastore_v3',
        datastore_file_stub.DatastoreFileStub(app_id, path)
    )

    return created

if __name__ == '__main__':
    main(os.path.join(os.path.dirname(__file__), 'demo'))
