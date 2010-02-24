==================
 Message Database
==================

Introduction
------------

The Message Database, mdb, is a versioned object datastore.

Installation
------------

To install, you need to install lxml, PLY, tornado, python-sasl, and
python-xmpp-server::

  sudo easy_install setuptools pycurl==7.16.2.1 simplejson ply lxml
  sudo easy_install -f http://www.tornadoweb.org/ tornado

  git clone git://github.com/thisismedium/python-sasl.git
  cd python-sasl
  python setup.py build
  sudo python setup.py develop

  git clone git://github.com/thisismedium/python-xmpp-server.git
  cd python-xmpp-server
  python setup.py build
  sudo python setup.py develop

  git clone git://git.hosts.coptix.com/git/mdb.git
  cd mdb
  python setup.py build
  sudo python setup.py develop

BOSH
----

Install punjab, a twisted BOSH server::

  sudo easy_install twisted
  sudo easy_instll -f http://code.stanziq.com/punjab/releases/ punjab


