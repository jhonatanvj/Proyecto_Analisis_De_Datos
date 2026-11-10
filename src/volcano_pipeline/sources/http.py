"""HTTP session with timeouts, retries with exponential backoff and a polite User-Agent."""
from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_TIMEOUT = (10, 120)  # connect, read (seconds)


def make_session(user_agent: str, retries: int = 4, backoff: float = 2.0) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff,                     # 2, 4, 8, 16 s
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.headers.update({"User-Agent": user_agent})
    return session


def get(session: requests.Session, url: str, params: dict | None = None,
        etag: str | None = None, last_modified: str | None = None) -> requests.Response:
    headers = {}
    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified
    resp = session.get(url, params=params, headers=headers, timeout=DEFAULT_TIMEOUT)
    if resp.status_code != 304:
        resp.raise_for_status()
    return resp
