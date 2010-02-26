import tempfile
from tornado import ioloop, httpserver
from mdb import webdav

def main():
    root = tempfile.gettempdir()
    server = httpserver.HTTPServer(webdav.DAVHandler(
        provider_mapping = { '/': webdav.Filesystem(root) },
        user_mapping = {},
        verbose = 1,
        enable_loggers = [],
        propsmanager = True,
        locksmanager = True,
        domaincontroller = None
    ))

    print 'Starting server; root=%r.' % root
    server.listen(8888)
    ioloop.IOLoop.instance().start()

if __name__ == '__main__':
    main()
