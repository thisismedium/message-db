"""repl.tac -- run the punjab BOSH server for a REPL

## In one terminal:
> twistd -ny repl.tac

## In another terminal:
> python demo-server.py

## In a browser: http://localhost:5080/repl.html
"""

from twisted.web import server, resource, static
from twisted.application import service, internet
from twisted.scripts.twistd import run
from punjab.httpb  import Httpb, HttpbService

root = static.File("./html") # a static html directory

b = resource.IResource(HttpbService(1))
root.putChild('bosh', b) # url for BOSH

site  = server.Site(root)

application = service.Application("punjab")
internet.TCPServer(5280, site).setServiceParent(application)
