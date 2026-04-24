from __future__ import annotations

import json
import math
import re
from hashlib import sha1
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from datetime import datetime, timedelta, timezone as datetime_timezone, tzinfo


ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT / "config"
DATA_DIR = ROOT / "data"
DASHBOARD_DIR = ROOT / "dashboard"

FIXED_TIMEZONE_OFFSETS = {
    "Asia/Shanghai": 8 * 60 * 60,
    "UTC": 0,
    "Etc/UTC": 0,
}


def now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _timezone_for_name(timezone_name: str) -> tzinfo:
    normalized = str(timezone_name or "").strip() or "UTC"
    try:
        return ZoneInfo(normalized)
    except ZoneInfoNotFoundError:
        offset_seconds = FIXED_TIMEZONE_OFFSETS.get(normalized)
        if offset_seconds is None:
            return datetime_timezone.utc
        return datetime_timezone(timedelta(seconds=offset_seconds), normalized)


def current_run_date(timezone: str = "Asia/Shanghai") -> str:
    return datetime.now(_timezone_for_name(timezone)).strftime("%Y-%m-%d")


def read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def num(value: Any) -> float | None:
    if value in (None, "", False):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed) or math.isinf(parsed):
        return None
    return parsed


def clamp(value: Any, minimum: float, maximum: float) -> float:
    parsed = num(value)
    if parsed is None:
        return minimum
    return max(minimum, min(maximum, parsed))


def clean_bool(value: Any, fallback: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    return fallback


def one_line(value: Any, limit: int = 220) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def safe_last(items: list[Any] | None) -> Any:
    if not items:
        return None
    return items[-1]


def sha1_hex(value: str) -> str:
    return sha1(value.encode("utf-8")).hexdigest()


def parse_json_loose(raw_text: str) -> Any:
    text = str(raw_text or "").strip()
    if not text:
      raise ValueError("empty JSON payload")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    fence_match = re.search(r"```(?:json)?\s*(\{.*\}|\[.*\])\s*```", text, re.S)
    if fence_match:
        return json.loads(fence_match.group(1))
    brace_match = re.search(r"(\{.*\}|\[.*\])", text, re.S)
    if brace_match:
        return json.loads(brace_match.group(1))
    raise ValueError("could not find JSON object in response")
