import json
import time
import uuid
import threading
import webbrowser
import http.server
import socketserver
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse, parse_qs
import requests


class FAFAuthClient:
    """Handles OAuth authentication against FAF Hydra."""

    def __init__(
        self,
        client_id: str,
        oauth_base_url: str,
        redirect_uri: str,
        scopes: str,
        token_file: str | Path,
    ) -> None:
        self.client_id = client_id
        self.oauth_base_url = oauth_base_url.rstrip("/")
        self.redirect_uri = redirect_uri
        self.scopes = scopes
        self.token_path = Path(token_file)

        self._state: str | None = None
        self._auth_code: str | None = None

    # ---------------------------------------------------------
    # Public API
    # ---------------------------------------------------------

    def get_token(self) -> Dict[str, Any]:
        """Returns a valid access token, refreshing or re-logging in as needed."""
        token_data = self._load_token()

        if token_data is None:
            return self._login()

        if not self._is_token_expired(token_data):
            return token_data

        # Try to refresh only if we actually have a refresh token.
        refresh_token = token_data.get("refresh_token")
        if refresh_token:
            try:
                return self._refresh_token(refresh_token)
            except Exception:
                # Refresh failed (revoked, expired, server error) — fall through
                # to a full re-login so the user isn't stuck.
                pass

        # No refresh token, or refresh failed: delete the stale token file
        # so _login() starts from a completely clean state.
        self._delete_token()
        return self._login()

    def force_relogin(self) -> Dict[str, Any]:
        """Delete any cached token and run a fresh login flow."""
        self._delete_token()
        return self._login()

    # ---------------------------------------------------------
    # OAuth Flow
    # ---------------------------------------------------------

    def _login(self) -> Dict[str, Any]:
        """Runs the full OAuth authorization code flow."""
        self._state = uuid.uuid4().hex
        self._auth_code = None

        # Always request 'offline' so the server includes a refresh_token.
        scopes = self.scopes

        auth_url = (
            f"{self.oauth_base_url}/oauth2/auth"
            f"?response_type=code"
            f"&client_id={self.client_id}"
            f"&redirect_uri={self.redirect_uri}"
            f"&scope={scopes}"
            f"&state={self._state}"
        )

        port = self._extract_port_from_redirect()

        with socketserver.TCPServer(
            ("localhost", port),
            self._build_handler(),
        ) as httpd:
            thread = threading.Thread(target=httpd.serve_forever)
            thread.daemon = True
            thread.start()

            webbrowser.open(auth_url)
            self._wait_for_code()
            httpd.shutdown()

        if self._auth_code is None:
            raise RuntimeError("OAuth login failed: no authorization code received.")

        return self._exchange_code_for_token(self._auth_code)

    def _refresh_token(self, refresh_token: str) -> Dict[str, Any]:
        """Exchanges a refresh token for a new access token."""
        response = requests.post(
            f"{self.oauth_base_url}/oauth2/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": self.client_id,
            },
            timeout=10,
        )
        response.raise_for_status()

        token_data = response.json()
        token_data["expires_at"] = time.time() + token_data["expires_in"]

        # Preserve the old refresh token if the server didn't issue a new one
        # (some servers rotate, some don't).
        if "refresh_token" not in token_data:
            token_data["refresh_token"] = refresh_token

        self._save_token(token_data)
        return token_data

    def _exchange_code_for_token(self, code: str) -> Dict[str, Any]:
        """Exchanges an authorization code for an access + refresh token."""
        response = requests.post(
            f"{self.oauth_base_url}/oauth2/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": self.client_id,
                "redirect_uri": self.redirect_uri,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=10,
        )
        response.raise_for_status()

        token_data = response.json()
        token_data["expires_at"] = time.time() + token_data["expires_in"]

        self._save_token(token_data)
        return token_data

    # ---------------------------------------------------------
    # Local Callback Server
    # ---------------------------------------------------------

    def _build_handler(self):
        parent = self

        class OAuthCallbackHandler(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                parsed = urlparse(self.path)
                params = parse_qs(parsed.query)

                code = params.get("code", [None])[0]
                received_state = params.get("state", [None])[0]

                if received_state != parent._state:
                    self.send_response(400)
                    self.end_headers()
                    self.wfile.write(b"State mismatch.")
                    return

                parent._auth_code = code

                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(b"Login successful. You may close this window.")

            def log_message(self, format: str, *args: Any) -> None:
                return  # suppress console noise

        return OAuthCallbackHandler

    def _wait_for_code(self, timeout: int = 120) -> None:
        start = time.time()
        while self._auth_code is None:
            if time.time() - start > timeout:
                raise TimeoutError("OAuth login timed out.")
            time.sleep(0.5)

    def _extract_port_from_redirect(self) -> int:
        parsed = urlparse(self.redirect_uri)
        if parsed.port is None:
            raise ValueError(
                "Redirect URI must include an explicit port, "
                "e.g. http://localhost:8765/callback"
            )
        return parsed.port

    # ---------------------------------------------------------
    # Token Storage
    # ---------------------------------------------------------

    def _load_token(self) -> Optional[Dict[str, Any]]:
        if not self.token_path.exists():
            return None
        try:
            with self.token_path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return None

    def _save_token(self, token_data: Dict[str, Any]) -> None:
        with self.token_path.open("w", encoding="utf-8") as f:
            json.dump(token_data, f, indent=2)

    def _delete_token(self) -> None:
        if self.token_path.exists():
            self.token_path.unlink()

    @staticmethod
    def _is_token_expired(token_data: Dict[str, Any]) -> bool:
        # Treat as expired 60 seconds early to avoid edge-case races.
        return time.time() >= token_data.get("expires_at", 0) - 60