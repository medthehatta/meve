from dataclasses import dataclass
import base64
from typing import Any


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
class OidcBearerTokenFactory:

    url: str
    client_id: str
    client_secret: str

    def login(self, username, password):
        return OidcBearerToken(
            self.url,
            self.client_id,
            self.client_secret,
            self._login_grant(username, password),
        )

    def _login_grant(self, username, password):
        return lambda: {
            "username": username,
            "password": password,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "password",
        }


@dataclass
class OidcBearerToken(AccessToken):

    url: str
    client_id: str
    client_secret: str
    login_grant_method: callable
    # Token initialization is lazy: no guarantees you have a valid token
    # until you try calling `get()`
    tokens: Any = None

    def _request_tokens_with_grant(self, grant_data):
        """Get the access token given the `grant_data`."""
        response = requests.post(
            url=self.url,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
                "User-Agent": "Apache-HttpClient",
            },
            data=grant_data,
        )
        response.raise_for_status()
        self.tokens = response.json()
        # Refresh the token `leeway` seconds earlier than we "really need to"
        # to be careful.
        leeway = 60
        self.expire_time = (
            time.time() + int(self.tokens["expires_in"]) - leeway
        )
        self.refresh_expire_time = (
            time.time() + int(self.tokens["refresh_expires_in"]) - leeway
        )
        return self.tokens

    def _login(self):
        """Get the access token with the login flow."""
        return self._request_tokens_with_grant(self.login_grant())

    def login_grant_method(self, client_id, client_secret):
        """Compute the login grant."""

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
        elif current_time > self.refresh_expire_time:
            self.tokens = self._login()
        elif current_time > self.expire_time:
            self.tokens = self._refresh()
        return self.tokens["access_token"]

    @property
    def auth_headers(self):
        return {"Authorization": f"Bearer {self.get()}"}
