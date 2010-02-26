## Adapted from <http://github.com/facebook/tornado/>, which is
## licensed under the Apache Licence, Version 2.0
## <http://www.apache.org/licenses/LICENSE-2.0.html>.

"""wsgi -- augment Tornado WSGI support"""

from __future__ import absolute_import
from tornado import wsgi, escape

__all__ = ('WSGIContainer', )

class WSGIContainer(wsgi.WSGIContainer):
    """Identical to the original __call__() method, but add the
    exc_info parameter to start_response()."""

    def __call__(self, request):
        data = {}

        def start_response(status, response_headers, exc_info=None):
            data["status"] = status
            data["headers"] = wsgi.HTTPHeaders(response_headers)

        body = "".join(self.wsgi_application(
            WSGIContainer.environ(request), start_response))
        if not data: raise Exception("WSGI app did not call start_response")

        status_code = int(data["status"].split()[0])
        headers = data["headers"]
        body = escape.utf8(body)
        headers["Content-Length"] = str(len(body))
        headers.setdefault("Content-Type", "text/html; charset=UTF-8")
        headers.setdefault("Server", "TornadoServer/0.1")

        parts = ["HTTP/1.1 " + data["status"] + "\r\n"]
        for key, value in headers.iteritems():
            parts.append(escape.utf8(key) + ": " + escape.utf8(value) + "\r\n")
        parts.append("\r\n")
        parts.append(body)
        request.write("".join(parts))
        request.finish()
        self._log(status_code, request)
