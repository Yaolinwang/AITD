from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import ProxyHandler, Request, build_opener

from .utils import DATA_DIR, now_iso, read_json, sha1_hex, write_json


CACHE_DIR = DATA_DIR / "cache" / "http"


class HttpRequestError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retry_after: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after


def _cache_path(namespace: str, url: str) -> Path:
    return CACHE_DIR / namespace / f"{sha1_hex(url)}.json"


def _cache_payload(path: Path, payload: Any, ttl_seconds: int, max_stale_seconds: int) -> None:
    now_ms = int(__import__("time").time() * 1000)
    write_json(
        path,
        {
            "fetchedAt": now_iso(),
            "fetchedAtMs": now_ms,
            "expiresAtMs": now_ms + max(1, ttl_seconds) * 1000,
            "staleUntilMs": now_ms + max(ttl_seconds, max_stale_seconds) * 1000,
            "payload": payload,
        },
    )


def _cache_is_fresh(cache: dict[str, Any] | None) -> bool:
    if not cache:
        return False
    return int(cache.get("expiresAtMs") or 0) > int(__import__("time").time() * 1000)


def _cache_is_usable(cache: dict[str, Any] | None) -> bool:
    if not cache:
        return False
    return int(cache.get("staleUntilMs") or 0) > int(__import__("time").time() * 1000)


def _should_bypass_proxy(hostname: str, network_settings: dict[str, Any]) -> bool:
    no_proxy = [item.lower() for item in network_settings.get("noProxy", [])]
    host = (hostname or "").lower()
    return any(host == item or host.endswith(f".{item}") for item in no_proxy)


def _build_opener(url: str, network_settings: dict[str, Any] | None):
    if not network_settings:
        return build_opener()
    parsed = urlparse(url)
    if (
        not network_settings.get("proxyEnabled")
        or not network_settings.get("proxyUrl")
        or _should_bypass_proxy(parsed.hostname or "", network_settings)
    ):
        # Explicitly install an empty ProxyHandler so urllib does not silently
        # fall back to HTTP(S)_PROXY environment variables when this request
        # is configured to bypass the local proxy.
        return build_opener(ProxyHandler({}))
    proxy_url = str(network_settings.get("proxyUrl") or "").strip()
    scheme = parsed.scheme.lower()
    proxies = {
        "http": proxy_url,
        "https": proxy_url,
    }
    if scheme in {"http", "https"}:
        return build_opener(ProxyHandler(proxies))
    return build_opener()


def request_text(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    payload: Any = None,
    timeout_seconds: int = 45,
    network_settings: dict[str, Any] | None = None,
) -> str:
    body: bytes | None
    if payload is None:
        body = None
    elif isinstance(payload, (bytes, bytearray)):
        body = bytes(payload)
    elif isinstance(payload, str):
        body = payload.encode("utf-8")
    else:
        body = json.dumps(payload).encode("utf-8")
    request = Request(url=url, method=method.upper(), data=body)
    merged_headers = {
        "accept": "application/json",
        "user-agent": "python-trading-agent/1.0",
    }
    if body is not None and "content-type" not in {key.lower() for key in (headers or {})}:
        merged_headers["content-type"] = "application/json"
    merged_headers.update(headers or {})
    for key, value in merged_headers.items():
        request.add_header(key, value)
    opener = _build_opener(url, network_settings or {})
    try:
        with opener.open(request, timeout=timeout_seconds) as response:
            return response.read().decode("utf-8")
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        retry_after = error.headers.get("Retry-After") if error.headers else None
        raise HttpRequestError(
            f"{error.code} {error.reason}: {detail}",
            status_code=error.code,
            retry_after=retry_after,
        ) from error
    except URLError as error:
        raise HttpRequestError(str(error.reason)) from error


def request_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    payload: Any = None,
    timeout_seconds: int = 45,
    network_settings: dict[str, Any] | None = None,
) -> Any:
    text = request_text(
        method,
        url,
        headers=headers,
        payload=payload,
        timeout_seconds=timeout_seconds,
        network_settings=network_settings,
    )
    try:
        return json.loads(text)
    except json.JSONDecodeError as error:
        snippet = text[:220].replace("\n", " ").strip()
        if len(text) > 220:
            snippet += "..."
        message = f"invalid JSON response from {url}: {error}"
        if snippet:
            message += f" | response starts with: {snippet}"
        raise HttpRequestError(message) from error


def cached_get_json(
    url: str,
    *,
    namespace: str = "generic",
    ttl_seconds: int = 60,
    max_stale_seconds: int = 3600,
    timeout_seconds: int = 45,
    headers: dict[str, str] | None = None,
    network_settings: dict[str, Any] | None = None,
    prefer_cache: bool = False,
    allow_network: bool = True,
) -> Any:
    path = _cache_path(namespace, url)
    cache = read_json(path, {})
    if _cache_is_fresh(cache):
        return cache.get("payload")
    if prefer_cache and _cache_is_usable(cache):
        return cache.get("payload")
    if not allow_network:
        raise HttpRequestError("cached response unavailable while exchange API cooldown is active")
    try:
        payload = request_json(
            "GET",
            url,
            headers=headers,
            timeout_seconds=timeout_seconds,
            network_settings=network_settings,
        )
        _cache_payload(path, payload, ttl_seconds, max_stale_seconds)
        return payload
    except Exception:
        if _cache_is_usable(cache):
            return cache.get("payload")
        raise
