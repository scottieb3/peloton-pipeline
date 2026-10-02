#!/usr/bin/env python3
"""
Peloton First-Time Login
=========================

Mints an initial ``peloton_tokens.json`` (access + refresh token) from your
Peloton username and password, so the pipeline has something to refresh.

Peloton's public OAuth client has the device-code and password grants disabled,
and only allows the app's own ``members.onepeloton.com/callback`` redirect. So
this performs the same Authorization Code + PKCE flow the Peloton web app uses,
driven headlessly through Auth0's embedded login endpoint
(``/usernamepassword/login``). No browser required.

Credentials are read from the environment (or a local ``.env``); they are never
stored anywhere except the resulting tokens file.

Usage:
    # set PELOTON_USERNAME and PELOTON_PASSWORD in your .env first
    python peloton_login.py
"""

import base64
import hashlib
import json
import logging
import os
import secrets
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple
from urllib.parse import urlencode, urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# --- Non-secret constants (public values used by the Peloton web client) ---
AUTH0_DOMAIN = "auth.onepeloton.com"
AUTH0_BASE = f"https://{AUTH0_DOMAIN}"
AUTHORIZE_ENDPOINT = f"{AUTH0_BASE}/authorize"
LOGIN_ENDPOINT = f"{AUTH0_BASE}/usernamepassword/login"
LOGIN_CALLBACK_ENDPOINT = f"{AUTH0_BASE}/login/callback"
TOKEN_ENDPOINT = f"{AUTH0_BASE}/oauth/token"
API_BASE = "https://api.onepeloton.com"

# Public Peloton SPA client ID (visible in the web app; not a secret).
DEFAULT_CLIENT_ID = "WVoJxVDdPoFx4RNewvvg6ch2mZ7bwnsM"
AUDIENCE = "https://api.onepeloton.com/"
SCOPE = "offline_access openid peloton-api.members:default"
REDIRECT_URI = "https://members.onepeloton.com/callback"
MEMBERS_ORIGIN = "https://members.onepeloton.com"
CONNECTION = "pelo-user-password"
TENANT = "peloton-prod"
# auth0-spa-js client hint sent by the web app on /authorize.
AUTH0_CLIENT_PAYLOAD = "eyJuYW1lIjoiYXV0aDAtc3BhLWpzIiwidmVyc2lvbiI6IjIuMS4zIn0="

BROWSER_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120 Safari/537.36"
)

TOKENS_FILE = os.environ.get("PELOTON_TOKENS_FILE", "peloton_tokens.json")


def _b64url_no_pad(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _sha256_b64url(s: str) -> str:
    return _b64url_no_pad(hashlib.sha256(s.encode("utf-8")).digest())


@dataclass
class OAuthConfig:
    code_verifier: str
    code_challenge: str
    state: str
    nonce: str


def generate_oauth_config() -> OAuthConfig:
    code_verifier = _b64url_no_pad(os.urandom(64))
    return OAuthConfig(
        code_verifier=code_verifier,
        code_challenge=_sha256_b64url(code_verifier),
        state=secrets.token_urlsafe(32),
        nonce=secrets.token_urlsafe(32),
    )


def build_authorize_url(client_id: str, cfg: OAuthConfig) -> str:
    params = {
        "client_id": client_id,
        "audience": AUDIENCE,
        "scope": SCOPE,
        "response_type": "code",
        "response_mode": "query",
        "redirect_uri": REDIRECT_URI,
        "state": cfg.state,
        "nonce": cfg.nonce,
        "code_challenge": cfg.code_challenge,
        "code_challenge_method": "S256",
        "auth0Client": AUTH0_CLIENT_PAYLOAD,
    }
    return f"{AUTHORIZE_ENDPOINT}?{urlencode(params)}"


def get_login_page(session: requests.Session, authorize_url: str) -> Tuple[str, Optional[str]]:
    """Follow /authorize to the hosted login page and grab the CSRF cookie."""
    resp = session.get(authorize_url, allow_redirects=True, timeout=30)
    if resp.status_code >= 400:
        logger.error(f"/authorize failed ({resp.status_code}): {resp.text[:500]}")
        resp.raise_for_status()
    csrf = session.cookies.get("_csrf", domain=AUTH0_DOMAIN)
    return resp.url, csrf


def _build_login_payload(login_page_url: str, username: str, password: str, csrf: str) -> Dict[str, str]:
    q = parse_qs(urlparse(login_page_url).query)
    flat = {k: v[0] for k, v in q.items() if v}

    client = flat.get("client") or flat.get("client_id")
    if client:
        flat["client_id"] = client

    flat.update({
        "username": username,
        "password": password,
        "_csrf": csrf or "",
        "tenant": TENANT,
        "connection": CONNECTION,
    })
    return flat


def submit_credentials(
    session: requests.Session,
    login_page_url: str,
    username: str,
    password: str,
    csrf: Optional[str],
) -> requests.Response:
    """POST credentials to the embedded login endpoint; returns the WS-Fed form."""
    payload = _build_login_payload(login_page_url, username, password, csrf or "")
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": MEMBERS_ORIGIN,
        "Referer": login_page_url,
        "User-Agent": BROWSER_UA,
    }
    resp = session.post(LOGIN_ENDPOINT, data=payload, headers=headers,
                        allow_redirects=False, timeout=30)
    if resp.status_code != 200:
        logger.error(f"Login failed ({resp.status_code}): {resp.text[:500]}")
        if resp.status_code in (401, 403):
            logger.error("Check PELOTON_USERNAME / PELOTON_PASSWORD. "
                         "Note: this flow does not support 2FA-protected accounts.")
        resp.raise_for_status()
    return resp


def parse_hidden_form(html: str) -> Tuple[str, Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    form = soup.find("form")
    if not form or not form.get("action"):
        raise RuntimeError(
            "Expected a hidden login form but none was found. This usually means "
            "the login was rejected (bad credentials) or the account requires 2FA."
        )
    data = {}
    for inp in form.find_all("input"):
        name = inp.get("name")
        if name:
            data[name] = inp.get("value", "")
    return form["action"], data


def submit_hidden_form(session: requests.Session, base_url: str, action: str,
                       data: Dict[str, str]) -> requests.Response:
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": AUTH0_BASE,
        "Referer": AUTH0_BASE + "/",
        "User-Agent": BROWSER_UA,
    }
    return session.post(urljoin(base_url, action), data=data, headers=headers,
                        allow_redirects=False, timeout=30)


def _extract_code(url: str) -> Optional[str]:
    return parse_qs(urlparse(url).query).get("code", [None])[0]


def follow_until_code(session: requests.Session, start_url: str, max_hops: int = 15) -> str:
    """Follow redirects (and any intermediate hidden forms) until the callback code."""
    url = start_url
    for hop in range(max_hops):
        if _extract_code(url):
            return url

        resp = session.get(url, allow_redirects=False, timeout=30)
        if resp.is_redirect or resp.status_code in (301, 302, 303, 307, 308):
            nxt = resp.headers.get("Location")
            if not nxt:
                raise RuntimeError(f"Redirect without Location at hop {hop}")
            url = urljoin(url, nxt)
            continue

        ct = (resp.headers.get("Content-Type") or "").lower()
        if "text/html" in ct and "<form" in resp.text.lower():
            action, data = parse_hidden_form(resp.text)
            post_resp = submit_hidden_form(session, url, action, data)
            if "Location" in post_resp.headers:
                url = urljoin(url, post_resp.headers["Location"])
                continue
            url = post_resp.url
            continue

        raise RuntimeError(
            f"Stuck at hop {hop}: status={resp.status_code}, content-type={ct}, url={url}"
        )
    raise RuntimeError(f"Exceeded {max_hops} hops without finding an authorization code.")


def exchange_code_for_tokens(session: requests.Session, client_id: str,
                             cfg: OAuthConfig, code: str) -> Dict:
    payload = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "code_verifier": cfg.code_verifier,
        "code": code,
        "redirect_uri": REDIRECT_URI,
    }
    resp = session.post(TOKEN_ENDPOINT, json=payload, timeout=30)
    if resp.status_code >= 400:
        logger.error(f"Token exchange failed ({resp.status_code}): {resp.text[:500]}")
        resp.raise_for_status()
    return resp.json()


def build_tokens_record(token_response: Dict) -> Dict:
    """Shape the response into the file format peloton_token_exchange expects."""
    now = int(time.time())
    expires_in = int(token_response.get("expires_in", 0))
    return {
        "access_token": token_response["access_token"],
        "refresh_token": token_response["refresh_token"],
        "token_type": token_response.get("token_type", "Bearer"),
        "scope": token_response.get("scope", SCOPE),
        "expires_at": now + expires_in if expires_in else now,
    }


def save_tokens(file_path: Path, tokens: Dict) -> None:
    with open(file_path, "w") as f:
        json.dump(tokens, f, indent=2)
    logger.info(f"Saved tokens to {file_path}")


def validate(tokens: Dict) -> None:
    headers = {"Authorization": f'{tokens.get("token_type", "Bearer")} {tokens["access_token"]}'}
    resp = requests.get(f"{API_BASE}/api/me", headers=headers, timeout=30)
    resp.raise_for_status()
    me = resp.json()
    print(f"\n{'='*50}")
    print("SUCCESS: Logged in and minted tokens!")
    print(f"User ID:  {me.get('id', 'Unknown')}")
    print(f"Username: {me.get('username', 'Unknown')}")
    print(f"{'='*50}\n")


def login() -> Dict:
    username = os.environ.get("PELOTON_USERNAME", "")
    password = os.environ.get("PELOTON_PASSWORD", "")
    if not username or not password:
        logger.error("PELOTON_USERNAME and PELOTON_PASSWORD must be set "
                     "(in your environment or .env file).")
        raise RuntimeError("Missing Peloton credentials")

    client_id = os.environ.get("PELOTON_CLIENT_ID", DEFAULT_CLIENT_ID)

    session = requests.Session()
    session.headers.update({"User-Agent": BROWSER_UA})

    cfg = generate_oauth_config()
    authorize_url = build_authorize_url(client_id, cfg)

    logger.info("Requesting login page...")
    login_page_url, csrf = get_login_page(session, authorize_url)

    logger.info("Submitting credentials...")
    login_resp = submit_credentials(session, login_page_url, username, password, csrf)

    # The embedded login returns a self-submitting WS-Fed form; post it onward.
    if "Location" in login_resp.headers:
        next_url = urljoin(LOGIN_ENDPOINT, login_resp.headers["Location"])
    else:
        action, data = parse_hidden_form(login_resp.text)
        form_resp = submit_hidden_form(session, LOGIN_CALLBACK_ENDPOINT, action, data)
        next_url = (urljoin(form_resp.url, form_resp.headers["Location"])
                    if "Location" in form_resp.headers else form_resp.url)

    logger.info("Following redirects to the authorization code...")
    final_url = follow_until_code(session, next_url)
    code = _extract_code(final_url)
    if not code:
        raise RuntimeError("No authorization code found after login.")

    logger.info("Exchanging code for tokens...")
    token_response = exchange_code_for_tokens(session, client_id, cfg, code)
    return build_tokens_record(token_response)


def main():
    try:
        tokens = login()
    except Exception as e:
        logger.error(f"Login failed: {e}")
        sys.exit(1)

    save_tokens(Path(TOKENS_FILE).resolve(), tokens)
    try:
        validate(tokens)
    except Exception as e:
        logger.warning(f"Tokens saved, but validation call failed: {e}")


if __name__ == "__main__":
    main()
