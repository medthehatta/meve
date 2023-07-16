from dataclasses import dataclass
from dataclasses import field
import base64
import time
from typing import Any

import requests

import re
import http.server


_FOUND_PATH = [""]

class GetPathHandler(http.server.BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path:
            _FOUND_PATH[0] = self.path
            self.wfile.write(b'You have been logged in')
        else:
            self.wfile.write(b'Error logging in')


def get_code_http(port):

    def _get_code_http(self):
        print(self.idp_login_url())
        http.server.HTTPServer(("0.0.0.0", port), GetPathHandler).handle_request()
        found = _FOUND_PATH[0]
        _FOUND_PATH[0] = ""
        match = re.search(r"(?<=code=).*?(?=&)", found)
        if match:
            return match.group(0)
        else:
            raise ValueError(found)

    return _get_code_http


class AccessToken:
    """Base class for an access token."""

    def get(self):
        """Get the token."""
        raise NotImplementedError

    @property
    def auth_headers(self):
        """Get a dict of authentication headers."""
        raise NotImplementedError


class EmptyToken(AccessToken):
    """Empty token which carries no data."""

    def get(self):
        """Get the token."""
        return None

    @property
    def auth_headers(self):
        """Get a dict of authentication headers."""
        return {}


class ApiKey(AccessToken):
    """A constant API key."""

    def __init__(self, header_name, key):
        """Initialize the instance."""
        self.key = key
        self.header_name = header_name

    def get(self):
        """Get the token."""
        return self.key

    @property
    def auth_headers(self):
        """Get a dict of authentication headers."""
        return {self.header_name: self.get()}


class BasicAuth(AccessToken):
    """Basic HTTP Authentication."""

    def __init__(self, username, password):
        """Initialize the instance."""
        client_id = ":".join([username, password])
        encoded = base64.b64encode(client_id.encode("utf-8"))
        self.key = f"Basic {encoded.decode('utf-8')}"

    def get(self):
        """Get the token."""
        return self.key

    @property
    def auth_headers(self):
        """Get a dict of authentication headers."""
        return {"Authorization": self.get()}


@dataclass
class OidcFlow(AccessToken):

    url: str
    client_id: str
    client_secret: str
    # Token initialization is lazy: no guarantees you have a valid token
    # until you try calling `get()`
    tokens: Any = None

    def login_grant(self):
        raise NotImplementedError()

    def fetch_response(self, grant_data):
        # Default implementation
        return requests.post(
            url=self.url,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
                "User-Agent": "Apache-HttpClient",
            },
            data=grant_data,
        )

    def _request_tokens_with_grant(self, grant_data):
        """Get the access token given the `grant_data`."""
        response = self.fetch_response(grant_data)
        response.raise_for_status()
        self.tokens = response.json()
        # Refresh the token `leeway` seconds earlier than we "really need to"
        # to be careful.
        leeway = 60
        self.expire_time = (
            time.time() + int(self.tokens["expires_in"]) - leeway
        )
        self.refresh_expire_time = (
            time.time() + int(self.tokens.get("refresh_expires_in", 0)) - leeway
        )
        return self.tokens

    def _login(self):
        """Get the access token with the login flow."""
        return self._request_tokens_with_grant(self.login_grant())

    def _refresh(self):
        """Refresh the access token with a refresh flow."""
        refresh_token = self.tokens["refresh_token"]
        return self._request_tokens_with_grant(
            {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": refresh_token,
                "grant_type": "refresh_token",
            },
        )

    def get(self):
        """Retrieve the token, refreshing if necessary."""
        current_time = time.time()
        if self.tokens is None:
            self.tokens = self._login()
        elif current_time < self.expire_time:
            pass
        elif current_time >= self.expire_time:
            self.tokens = self._refresh()
        else:
            self.tokens = self._login()
        return self.tokens["access_token"]

    @property
    def auth_headers(self):
        return {"Authorization": f"Bearer {self.get()}"}


@dataclass
class EveOnlineFlow(OidcFlow):

    scopes: list[str] = field(default_factory=lambda: [])
    redirect_url: str = "http://localhost:8080"
    code_fetcher: callable = None
    code: str = ""

    def idp_login_url(self):
        req = requests.Request(
            method="GET",
            url="https://login.eveonline.com/v2/oauth/authorize",
            params={
                "response_type": "code",
                "redirect_uri": self.redirect_url,
                "client_id": self.client_id,
                "scope": " ".join(self.scopes),
                "secret": self.client_secret,
                "state": "abcdef",
            },
        )
        return req.prepare().url

    def get_code(self):
        if self.code_fetcher:
            return self.code_fetcher(self)
        else:
            raise TypeError("Did not provide a code_fetcher")

    def login_grant(self):
        self.code = self.get_code()
        return {
            "grant_type": "authorization_code",
            "code": self.code,
        }

    def fetch_response(self, grant_data):
        return requests.post(
            url=self.url,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
                "User-Agent": "Apache-HttpClient",
            },
            data=grant_data,
            auth=(self.client_id, self.client_secret),
        )

    def _refresh(self):
        return self._login()
