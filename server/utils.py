

import logging
from google.appengine.ext import webapp
import google.appengine.ext.webapp.template
import google.appengine.ext.webapp.util
import cgitb
import sys
import os

ROOT = os.path.dirname(__file__)


class ErrorMessage(Exception):
    """Raise this exception to show an error message to the user."""
    def __init__(self, status, message, type='html'):
        self.status = status
        self.message = message
        self.type = type

class Redirect(Exception):
    """Raise this exception to redirect to another page."""
    def __init__(self, url):
        self.url = url


class Struct:
    pass


class Handler(webapp.RequestHandler):
    auto_params = {
    }

    def render(self, path, **params):
        """Renders the template at the given path with the given parameters."""
        self.write(webapp.template.render(os.path.join(ROOT, path), params))

    def write(self, text):
        self.response.out.write(text)

    def initialize(self, request, response):
        webapp.RequestHandler.initialize(self, request, response)
        for name in request.headers.keys():
            if name.lower().startswith('x-appengine'):
                logging.debug('%s: %s' % (name, request.headers[name]))
        self.params = Struct()
        for param in self.auto_params:
            validator = self.auto_params[param]
            setattr(self.params, param, validator(request.get(param, '')))

    def handle_exception(self, exception, debug_mode):
        if isinstance(exception, Redirect):
            self.redirect(exception.url)
        elif isinstance(exception, ErrorMessage):
            self.error(exception.status)
            logging.info('ErrorMessage raised: %s'%exception.message)
            self.response.clear()
            if exception.type == 'json':
                self.write(json.encode({'rc': 1,
                                        'msg': exception.message}))
            else:
                self.render('templates/error.html',
                            params=self.params,
                            message=exception.message)
        else:
            self.error(500)
            logging.exception(exception)
            if debug_mode:
                self.response.clear()
                self.write(cgitb.html(sys.exc_info()))


def run(*args, **kwargs):
  webapp.util.run_wsgi_app(webapp.WSGIApplication(*args, **kwargs))