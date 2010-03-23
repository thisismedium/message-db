==================
 Message Database
==================

The Message Database is a versioned datastore for structured content.

Installation
------------

The easiest way to install this project is to download `Message`_ and
use its install scripts.  To install Message DB manually, follow these
steps.

.. _`Message`: http://github.com/thisismedium/message

First, install some dependencies::

  sudo easy_install setuptools pycurl==7.16.2.1 simplejson ply lxml pyyaml
  sudo easy_install -f http://www.tornadoweb.org/ tornado

Next, set up a devlopment area to track several repositories related
to this project::

  mkdir ~/projects; cd ~/projects

  git clone git://github.com/thisismedium/md.git
  cd md
  python setup.py build
  sudo python setup.py develop

  cd ..
  git clone git://github.com/thisismedium/python-sasl.git
  cd python-sasl
  python setup.py build
  sudo python setup.py develop

  cd ..
  git clone git://github.com/thisismedium/python-xmpp-server.git
  cd python-xmpp-server
  python setup.py build
  sudo python setup.py develop

  cd ..
  git clone git.hosts.coptix.com:/git/mdb.git
  cd mdb
  git checkout origin/prototype -b prototype
  python setup.py build
  sudo python setup.py develop

Verify everything worked by running a test query::

  python examples/demo-query.py '*'

BOSH
----

To run the example BOSH server, install punjab::

  sudo easy_install twisted
  sudo easy_install -f http://code.stanziq.com/punjab/releases/ punjab

In one terminal::

  cd ~/projects/mdb/examples
  twistd -ny repl.tac

In another terminal::

  cd ~/projects/mdb/examples
  python demo-server.py

Visit http://localhost:5280/repl.html in a browser.  You may edit the
YAML data in the ``examples/demo`` folder to change the content tree.
If you do this, the datastore needs to rebuilt::

  rm ~/projects/mdb/examples/demo/demo.data
  ## restart demo-server.py
