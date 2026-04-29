from __future__ import annotations

import email.utils
import re
import time
from datetime import datetime, timezone
from typing import Any

from .utils import DATA_DIR, now_iso, one_line, read_json, write_json


COOLDOWN_PATH = DATA_DIR / "exchange_cooldowns.json"
DEFAULT_RATE_LIMIT_SECONDS = 5 * 60
DEFAULT_BAN_SECONDS = 60 * 60


def _now_ms() -> int:
    return int(time.time() * 1000)


def _iso_from_ms(value: int | float | None) -> str | None:
    if not value:
        return None
    return datetime.fromtimestamp(float(value) / 1000, timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_payload() -> dict[str, Any]:
    payload = read_json(COOLDOWN_PATH, {})
    return payload if isinstance(payload, dict) else {}


def _write_payload(payload: dict[str, Any]) -> None:
    write_json(COOLDOWN_PATH, payload)


def _parse_retry_after(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    now = time.time()
    if re.fullmatch(r"\d+", text):
        return int((now + max(0, int(text))) * 1000)
    try:
        parsed = email.utils.parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def _parse_until_from_message(message: str) -> int | None:
    now_ms = _now_ms()
    for match in re.finditer(r"\b(\d{13})\b", message):
        candidate = int(match.group(1))
        if candidate > now_ms:
            return candidate
    for match in re.finditer(r"\b(\d{10})\b", message):
        candidate = int(match.group(1)) * 1000
        if candidate > now_ms:
            return candidate
    return None


def is_exchange_rate_limit_error(error: Any) -> bool:
    text = str(error or "").lower()
    status_code = getattr(error, "status_code", None)
    return (
        status_code in {418, 429}
        or "-1003" in text
        or "too many requests" in text
        or "rate limit" in text
        or "ip banned" in text
        or "banned" in text
    )


def _is_ban_error(error: Any) -> bool:
    text = str(error or "").lower()
    status_code = getattr(error, "status_code", None)
    return status_code == 418 or "ip banned" in text or "banned" in text


def cooldown_status(exchange_id: str = "binance") -> dict[str, Any]:
    exchange = str(exchange_id or "binance").strip().lower()
    payload = _read_payload().get(exchange)
    if not isinstance(payload, dict):
        return {
            "exchange": exchange,
            "active": False,
            "remainingSeconds": 0,
        }
    now_ms = _now_ms()
    until_ms = int(payload.get("untilMs") or 0)
    remaining = max(0, int((until_ms - now_ms + 999) / 1000))
    active = remaining > 0
    status = {
        **payload,
        "exchange": exchange,
        "active": active,
        "untilAt": _iso_from_ms(until_ms),
        "remainingSeconds": remaining,
    }
    if active:
        reason = payload.get("reason") or "exchange API cooldown"
        status["message"] = f"{exchange.upper()} API cooldown active until {status['untilAt']} ({remaining}s left): {reason}"
    return status


def record_exchange_cooldown(
    exchange_id: str,
    error: Any,
    *,
    retry_after: Any = None,
    endpoint: str | None = None,
) -> dict[str, Any]:
    exchange = str(exchange_id or "binance").strip().lower()
    message = one_line(error, 500)
    now_ms = _now_ms()
    retry_until_ms = _parse_retry_after(retry_after)
    message_until_ms = _parse_until_from_message(message)
    fallback_seconds = DEFAULT_BAN_SECONDS if _is_ban_error(error) else DEFAULT_RATE_LIMIT_SECONDS
    explicit_untils = [item for item in [retry_until_ms, message_until_ms] if item]
    until_ms = max(explicit_untils) if explicit_untils else now_ms + fallback_seconds * 1000
    existing = cooldown_status(exchange)
    if not explicit_untils and existing.get("active") and int(existing.get("untilMs") or 0) > until_ms:
        until_ms = int(existing.get("untilMs") or until_ms)
    reason = "IP banned by exchange" if _is_ban_error(error) else "rate limit / too many requests"
    payload = _read_payload()
    payload[exchange] = {
        "exchange": exchange,
        "active": True,
        "untilMs": int(until_ms),
        "untilAt": _iso_from_ms(until_ms),
        "reason": reason,
        "endpoint": endpoint,
        "lastError": message,
        "updatedAt": now_iso(),
    }
    _write_payload(payload)
    return cooldown_status(exchange)


def raise_if_exchange_cooldown_active(exchange_id: str = "binance") -> None:
    status = cooldown_status(exchange_id)
    if status.get("active"):
        raise RuntimeError(status.get("message") or f"{str(exchange_id).upper()} API cooldown is active.")
