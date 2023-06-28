#!/usr/bin/env python

import requests

from authentication import AccessToken
from authentication import EmptyToken


#
# Making requests
#


class Requester:
    """
    `Requester` provides a thin layer around the vanilla requests library.

    This provides three pieces of convenient functionality:

    1. Remembers the base URL so you don't need to keep passing it around.
    2. Authenticates using an `AccessToken`, so requests will all use
       constantly refreshing credentials.
    3. Remembers miscellaneous extra headers (example: headers which disable
       CSRF) that are used in all the requests.

    Instead of providing separate methods for GET/POST/etc. , it provides a
    single `request` method which accepts the HTTP method name as a string.

    Using a single `request` method reduces duplication in client code because
    differences between calls to HTTP methods are generally small, and often
    the method name is the only difference.  (For example, PUT vs POST vs PATCH
    are generally interchangeable except for the method name.)  Therefore
    parametrizing the method with a string encourages code reuse without
    needing a bunch of `if` statements or separate methods in the client code
    to decide which method to call here.
    """

    def __init__(
        self,
        url,
        token: AccessToken = EmptyToken(),
        common_extra_headers=None,
    ):
        """Initialize the instance."""
        self.url = url
        self.token = token
        self.common_extra_headers = common_extra_headers or {}

    def _construct_url(self, path):
        return self.url.rstrip("/") + "/" + path.lstrip("/")

    def request(self, method, path="/", extra_headers=None, **kwargs):
        """
        Perform an HTTP request.

        This accepts the `method` name (case-insensitive), the endpoint `path`,
        and `extra_headers` as a dictionary.  Additional keyword arguments are
        forwarded directly to the corresponding `requests` function.

        The `path` should be relative to the `url` property of this `Requester`
        object.  Leading slashes in `path` and trailing slashes in the `url`
        attribute are stripped so exactly one slash will always be used to join
        the base URL to this path.
        """
        func = getattr(requests, method.lower(), None)
        if func is None:
            raise LookupError(
                "No function for performing method '{method.lower()}' in requests."
            )
        extra_headers = extra_headers or {}
        # auth_headers is a property that is computed dynamically to account
        # for possible token expiry.  Therefore we can't just save the token
        # headers in a class member, we need to call `auth_headers` each time
        # we make a request!
        normal_headers = {**self.token.auth_headers}
        return func(
            self._construct_url(path),
            headers={
                **normal_headers,
                **self.common_extra_headers,
                **extra_headers,
            },
            **kwargs,
        )


#
# Handling responses
#


class ResponseHandler:
    """
    We collect common means of dealing with HTTP responses in these
    `ResponseHandler` classes.

    By collecting standard behaviors for HTTP responses in one place we gain a
    few advantages:

    1. We reduce boilerplate in client code.
    2. We facilitate parametrization of response behaviors.

    This is implemented in an OOP way with objects that have a single method so
    we can swap behavior by directly swapping the instance (and not modifying
    any invocations).  In most cases the handling requires no configuration --
    or there are obvious defaults -- so those cases are collected in a single
    convenient interface: `ResponseHandlerBroker`.

    If you add a new kind of `ResponseHandler` and it has no configuration or
    reasonable default configuration, consider adding a convenience method to
    `ResponseHandlerBroker`.

    An example of a `ResponseHandler` which WOULD require configuration might
    be one which logs the responses to a file and needs to be told where the
    file should go.

    Another example is `TracingResponseHandler`, implemented in this module.
    """

    def handle(self, response):
        raise NotImplementedError("Override me")


class DefaultHandlers:
    """
    Dispatch to the appropriate "default" `ResponseHandler` instances.

    For convenience, useful handlers which don't require configuration at
    instantiation time are collected here as class methods (which demand no
    instantiation by the client code.)
    """

    @classmethod
    def return_response(cls, response):
        return IdentityResponseHandler().handle(response)

    @classmethod
    def raise_or_return_json(cls, response):
        return RaiseOrJsonHandler().handle(response)

    @classmethod
    def raise_or_return_text(cls, response):
        return RaiseOrTextHandler().handle(response)

    @classmethod
    def raise_unless_ok(cls, response):
        return RaiseUnlessOkHandler().handle(response)

    @classmethod
    def print_return_response(cls, response):
        return TracingResponseHandler(trace_func=print).handle(response)


#
# Specific response handlers
#


class IdentityResponseHandler(ResponseHandler):

    def handle(self, response):
        """Return the reponse object verbatim."""
        return response


class RaiseOrJsonHandler(ResponseHandler):

    def handle(self, response):
        """Return JSON from the response if it's ok, or raise otherwise."""
        response.raise_for_status()
        return response.json()


class RaiseOrTextHandler(ResponseHandler):

    def handle(self, response):
        """Return text from the response if it's ok, or raise otherwise."""
        response.raise_for_status()
        return response.text


class RaiseUnlessOkHandler(ResponseHandler):

    def handle(self, response):
        """Raise if the response is not ok; return nothing otherwise."""
        response.raise_for_status()


class TracingResponseHandler(ResponseHandler):

    def __init__(self, trace_func=None):
        """Initialize the instance."""
        self.trace = trace_func or print

    def handle(self, response):
        """Trace out the response, then return it verbatim."""
        self.trace(f">>>> REQUEST\n{response.request.__dict__}")
        self.trace(f"<<<< RESPONSE\n{response.__dict__}")
        return response
