from __future__ import annotations

import contextlib
import io
import math
import time
from pathlib import Path
from typing import Any

from .config import PROMPT_KLINE_FEED_OPTIONS, read_candidate_source_code, read_fixed_universe, read_network_settings, read_trading_settings
from .instances import instance_paths
from .exchanges.catalog import DEFAULT_EXCHANGE_ID, normalize_exchange_id
from .exchanges import get_active_exchange_gateway
from .utils import CONFIG_DIR, DATA_DIR, ROOT, clamp, current_run_date, now_iso, num, read_json, sha1_hex, write_json


LEGACY_LATEST_SCAN_PATH = DATA_DIR / "scans" / "latest.json"
SHARED_SCAN_ARCHIVE_ROOT = DATA_DIR / "scan-archive"
SCAN_BUCKET_LIMITS = {
    "research": 70,
    "watch": 55,
    "liquidityFloorUsd": 5_000_000,
}


def _latest_scan_path(exchange_id: str | None = None, instance_id: str | None = None) -> Path:
    if exchange_id is None:
        settings = read_trading_settings(instance_id)
        exchange_id = settings.get("activeExchange")
    normalized = normalize_exchange_id(exchange_id, capability="market")
    if instance_id:
        return instance_paths(instance_id)["scans_dir"] / normalized / "latest.json"
    return DATA_DIR / "scans" / normalized / "latest.json"


def _scan_archive_dir(exchange_id: str | None = None, instance_id: str | None = None, run_date: str | None = None) -> Path:
    if exchange_id is None:
        settings = read_trading_settings(instance_id)
        exchange_id = settings.get("activeExchange")
    normalized = normalize_exchange_id(exchange_id, capability="market")
    bucket = str(run_date or current_run_date()).strip() or current_run_date()
    if instance_id:
        return instance_paths(instance_id)["scans_dir"] / normalized / "archive" / bucket
    return DATA_DIR / "scans" / normalized / "archive" / bucket


def _scan_archive_refs_dir(exchange_id: str | None = None, instance_id: str | None = None, run_date: str | None = None) -> Path:
    if exchange_id is None:
        settings = read_trading_settings(instance_id)
        exchange_id = settings.get("activeExchange")
    normalized = normalize_exchange_id(exchange_id, capability="market")
    bucket = str(run_date or current_run_date()).strip() or current_run_date()
    if instance_id:
        return instance_paths(instance_id)["scans_dir"] / normalized / "archive_refs" / bucket
    return DATA_DIR / "scans" / normalized / "archive_refs" / bucket


def _scan_archive_path(payload: dict[str, Any], exchange_id: str | None = None, instance_id: str | None = None) -> Path:
    run_date = str(payload.get("runDate") or current_run_date()).strip() or current_run_date()
    fetched_at = str(payload.get("fetchedAt") or now_iso()).strip() or now_iso()
    safe_timestamp = (
        fetched_at.replace("-", "")
        .replace(":", "")
        .replace(".", "")
        .replace("+", "")
        .replace("Z", "Z")
    )
    scan_id = str(payload.get("scanId") or f"scan-{int(time.time() * 1000)}").strip()
    return _scan_archive_dir(exchange_id, instance_id, run_date) / f"{safe_timestamp}-{scan_id}.json"


def _shared_scan_archive_dir(exchange_id: str | None = None, run_date: str | None = None) -> Path:
    normalized = normalize_exchange_id(exchange_id, capability="market")
    bucket = str(run_date or current_run_date()).strip() or current_run_date()
    return SHARED_SCAN_ARCHIVE_ROOT / normalized / bucket


def _scan_archive_fingerprint(payload: dict[str, Any], exchange_id: str | None = None) -> str:
    normalized_payload = {
        "version": int(payload.get("version") or 1),
        "runDate": payload.get("runDate"),
        "fetchedAt": payload.get("fetchedAt"),
        "source": payload.get("source"),
        "exchange": normalize_exchange_id(exchange_id or payload.get("exchange"), capability="market"),
        "universeSize": int(payload.get("universeSize") or 0),
        "skippedSymbols": payload.get("skippedSymbols") if isinstance(payload.get("skippedSymbols"), list) else [],
        "candidateSource": payload.get("candidateSource") if isinstance(payload.get("candidateSource"), dict) else {},
        "opportunities": payload.get("opportunities") if isinstance(payload.get("opportunities"), list) else [],
    }
    return sha1_hex(__import__("json").dumps(normalized_payload, ensure_ascii=False, sort_keys=True))


def _shared_scan_archive_path(payload: dict[str, Any], exchange_id: str | None = None) -> Path:
    run_date = str(payload.get("runDate") or current_run_date()).strip() or current_run_date()
    fetched_at = str(payload.get("fetchedAt") or now_iso()).strip() or now_iso()
    safe_timestamp = (
        fetched_at.replace("-", "")
        .replace(":", "")
        .replace(".", "")
        .replace("+", "")
        .replace("Z", "Z")
    )
    fingerprint = _scan_archive_fingerprint(payload, exchange_id)
    return _shared_scan_archive_dir(exchange_id or payload.get("exchange"), run_date) / f"{safe_timestamp}-{fingerprint[:12]}.json"


def archive_scan(payload: dict[str, Any], exchange_id: str | None = None, instance_id: str | None = None) -> dict[str, Any]:
    shared_path = _shared_scan_archive_path(payload, exchange_id)
    shared_created = not shared_path.exists()
    if shared_created:
        shared_payload = {
            **payload,
            "archiveFingerprint": _scan_archive_fingerprint(payload, exchange_id),
        }
        write_json(shared_path, shared_payload)
    if not instance_id:
        return {
            "sharedPath": shared_path,
            "refPath": None,
            "sharedCreated": shared_created,
        }
    ref_payload = {
        "version": 1,
        "instanceId": instance_id,
        "exchange": normalize_exchange_id(exchange_id or payload.get("exchange"), capability="market"),
        "runDate": payload.get("runDate"),
        "fetchedAt": payload.get("fetchedAt"),
        "scanId": payload.get("scanId"),
        "sharedPath": str(shared_path.relative_to(ROOT)),
        "sharedFingerprint": _scan_archive_fingerprint(payload, exchange_id),
        "source": payload.get("source"),
        "universeSize": int(payload.get("universeSize") or 0),
        "opportunityCount": len(payload.get("opportunities") or []),
    }
    ref_path = _scan_archive_refs_dir(exchange_id, instance_id, payload.get("runDate")) / f"{payload.get('scanId')}.json"
    write_json(ref_path, ref_payload)
    return {
        "sharedPath": shared_path,
        "refPath": ref_path,
        "sharedCreated": shared_created,
    }


def read_latest_scan(exchange_id: str | None = None, instance_id: str | None = None) -> dict[str, Any]:
    if exchange_id is None:
        settings = read_trading_settings(instance_id)
        exchange_id = settings.get("activeExchange")
    normalized = normalize_exchange_id(exchange_id, capability="market")
    payload = read_json(_latest_scan_path(normalized, instance_id), None)
    if not instance_id and not isinstance(payload, dict) and normalized == DEFAULT_EXCHANGE_ID:
        payload = read_json(LEGACY_LATEST_SCAN_PATH, {})
    if isinstance(payload, dict):
        return {
            "version": int(payload.get("version") or 1),
            "runDate": payload.get("runDate"),
            "fetchedAt": payload.get("fetchedAt"),
            "source": payload.get("source") or "fixed_universe",
            "exchange": str(payload.get("exchange") or normalized),
            "universeSize": int(payload.get("universeSize") or 0),
            "skippedSymbols": payload.get("skippedSymbols") if isinstance(payload.get("skippedSymbols"), list) else [],
            "candidateSource": payload.get("candidateSource") if isinstance(payload.get("candidateSource"), dict) else {},
            "opportunities": payload.get("opportunities") if isinstance(payload.get("opportunities"), list) else [],
        }
    return {
        "version": 1,
        "runDate": None,
        "fetchedAt": None,
        "source": "fixed_universe",
        "exchange": normalized,
        "universeSize": 0,
        "skippedSymbols": [],
        "candidateSource": {},
        "opportunities": [],
    }


def _normalize_candidate_symbols(raw_symbols: Any) -> list[str]:
    if isinstance(raw_symbols, str):
        candidate_values = [item.strip().upper() for item in raw_symbols.replace(",", "\n").splitlines()]
    elif isinstance(raw_symbols, (list, tuple, set)):
        candidate_values = [str(item).strip().upper() for item in raw_symbols]
    else:
        candidate_values = []
    symbols: list[str] = []
    for item in candidate_values:
        if item and item not in symbols:
            symbols.append(item)
    return symbols


def _split_valid_candidate_symbols(raw_symbols: Any, exchange_id: str | None = None) -> tuple[list[str], list[str]]:
    gateway = get_active_exchange_gateway(exchange_id)
    normalized = _normalize_candidate_symbols(raw_symbols)
    valid: list[str] = []
    invalid: list[str] = []
    for symbol in normalized:
        if gateway.validate_symbol(symbol):
            resolved_symbol = gateway.normalize_symbol(symbol)
            if resolved_symbol not in valid:
                valid.append(resolved_symbol)
        else:
            invalid.append(symbol)
    return valid, invalid


def dynamic_candidate_source_enabled(universe: dict[str, Any] | None = None, instance_id: str | None = None) -> bool:
    universe = universe or read_fixed_universe(instance_id)
    return universe.get("dynamicSource", {}).get("enabled") is True


def resolve_candidate_symbols(
    universe: dict[str, Any] | None = None,
    code_override: str | None = None,
    exchange_id: str | None = None,
    instance_id: str | None = None,
) -> dict[str, Any]:
    universe = universe or read_fixed_universe(instance_id)
    gateway = get_active_exchange_gateway(exchange_id)
    dynamic_source = universe.get("dynamicSource", {}) if isinstance(universe.get("dynamicSource"), dict) else {}
    if dynamic_source.get("enabled") is not True:
        manual_symbols = list(universe.get("symbols", []))
        valid_symbols, invalid_symbols = _split_valid_candidate_symbols(manual_symbols, exchange_id)
        if not valid_symbols:
            raise ValueError("Manual candidate pool is empty.")
        return {
            "mode": "manual_symbols",
            "enabled": False,
            "symbols": valid_symbols,
            "invalidSymbols": invalid_symbols,
            "stdout": "",
            "note": "Using manually configured symbols.",
            "durationMs": 0,
            "functionName": dynamic_source.get("functionName") or "load_candidate_symbols",
        }

    source_code = str(code_override if code_override is not None else read_candidate_source_code(instance_id))
    function_name = str(dynamic_source.get("functionName") or "load_candidate_symbols").strip() or "load_candidate_symbols"
    latest_scan_path = str(_latest_scan_path(gateway.exchange_id, instance_id))
    scope: dict[str, Any] = {
        "__builtins__": __builtins__,
        "scan_path": latest_scan_path,
        "latest_scan_path": latest_scan_path,
    }
    context = {
        "project_root": str(ROOT),
        "config_dir": str(CONFIG_DIR),
        "data_dir": str(DATA_DIR),
        "manual_symbols": list(universe.get("symbols", [])),
        "network_settings": read_network_settings(instance_id),
        "run_date": current_run_date(),
        "now_iso": now_iso(),
        "active_exchange": gateway.exchange_id,
        "scan_path": latest_scan_path,
        "latest_scan_path": latest_scan_path,
    }
    stdout_buffer = io.StringIO()
    started = time.perf_counter()
    with contextlib.redirect_stdout(stdout_buffer):
        exec(source_code, scope)
        func = scope.get(function_name)
        if not callable(func):
            raise ValueError(f"Dynamic candidate source must define `{function_name}(context)`.")
        result = func(context)
    duration_ms = int((time.perf_counter() - started) * 1000)
    note = None
    if isinstance(result, dict):
        note = str(result.get("note") or result.get("message") or "").strip() or None
        raw_symbols = result.get("symbols")
    else:
        raw_symbols = result
    symbols, invalid_symbols = _split_valid_candidate_symbols(raw_symbols, exchange_id)
    if not symbols:
        raise ValueError(f"Dynamic candidate source returned no valid {gateway.candidate_symbol_hint()}.")
    return {
        "mode": "python_function",
        "enabled": True,
        "symbols": symbols,
        "invalidSymbols": invalid_symbols,
        "stdout": stdout_buffer.getvalue().strip(),
        "note": note,
        "durationMs": duration_ms,
        "functionName": function_name,
    }


def test_candidate_source(
    universe: dict[str, Any] | None = None,
    code_override: str | None = None,
    exchange_id: str | None = None,
    instance_id: str | None = None,
) -> dict[str, Any]:
    resolved = resolve_candidate_symbols(universe=universe, code_override=code_override, exchange_id=exchange_id, instance_id=instance_id)
    return {
        "mode": resolved["mode"],
        "enabled": resolved["enabled"],
        "functionName": resolved["functionName"],
        "durationMs": resolved["durationMs"],
        "count": len(resolved["symbols"]),
        "symbols": resolved["symbols"],
        "invalidSymbols": resolved.get("invalidSymbols", []),
        "stdout": resolved["stdout"],
        "note": resolved["note"],
    }


def score_symbol(*, price_change_pct: float, quote_volume: float, funding_pct: float, range_pct: float) -> float:
    impulse = clamp(abs(price_change_pct) * 3.5, 0, 38)
    volume_score = clamp((math.log10(quote_volume + 1) - 6.2) * 11, 0, 24) if quote_volume > 0 else 0
    funding_skew = clamp(abs(funding_pct) * 260, 0, 14) if (
        (price_change_pct > 0 and funding_pct < 0) or (price_change_pct < 0 and funding_pct > 0)
    ) else 0
    range_score = clamp((range_pct - 3) * 1.3, 0, 20)
    return clamp(28 + impulse + volume_score + funding_skew + range_score, 0, 100)


def directional_bias(price_change_pct: float, funding_pct: float) -> str:
    if price_change_pct >= 1:
        return "Bullish"
    if price_change_pct <= -1:
        return "Bearish"
    if funding_pct < -0.02:
        return "Bullish"
    if funding_pct > 0.02:
        return "Bearish"
    return "Neutral"


def matched_strategies(price_change_pct: float, funding_pct: float, score: float) -> list[dict[str, Any]]:
    if price_change_pct >= 4 and funding_pct < 0:
        return [
            {
                "id": "mm_short_squeeze_fuel",
                "name": "Short squeeze continuation",
                "reason": "Positive impulse is rising while funding stays negative, so shorts may still be trapped.",
            },
            {
                "id": "attention_event_speedrun",
                "name": "Attention speedrun",
                "reason": "The contract is already moving hard enough to warrant breakout-style review.",
            },
        ]
    if price_change_pct <= -4 and funding_pct > 0:
        return [
            {
                "id": "mm_long_squeeze_fuel",
                "name": "Long squeeze flush",
                "reason": "Negative impulse is accelerating while funding stays positive, so crowded longs may keep unwinding.",
            },
            {
                "id": "attention_event_flush",
                "name": "Attention flush",
                "reason": "The move is weak enough to keep short-side failure scenarios active.",
            },
        ]
    if score >= 70 and price_change_pct >= 0:
        return [
            {
                "id": "alpha_contract_path",
                "name": "Trend continuation path",
                "reason": "Liquid contract with enough motion to keep trend-following entries on the table.",
            }
        ]
    if score >= 70 and price_change_pct < 0:
        return [
            {
                "id": "attention_event_flush",
                "name": "Weak trend continuation",
                "reason": "Negative trend pressure is strong enough to keep pullback shorts interesting.",
            }
        ]
    return [
        {
            "id": "alpha_contract_path",
            "name": "Wait for cleaner structure",
            "reason": "Keep the name in review, but wait for better intraday structure before forcing entries.",
        }
    ]


def detectors(*, price_change_pct: float, quote_volume: float, funding_pct: float, range_pct: float, min_quote_volume_usd: float) -> list[dict[str, Any]]:
    volume_passed = quote_volume >= min_quote_volume_usd
    impulse_passed = abs(price_change_pct) >= 4
    skew_passed = (price_change_pct > 0 and funding_pct < 0.01) or (price_change_pct < 0 and funding_pct > -0.01)
    range_passed = range_pct >= 6
    needle_risk = range_pct >= 18 and abs(price_change_pct) < 3
    return [
        {
            "id": "quote_volume_liquidity",
            "label": "Liquidity",
            "passed": volume_passed,
            "effect": 10 if volume_passed else 0,
            "reason": "Quote volume is large enough to support entries and exits." if volume_passed else "Liquidity is still below the preferred review floor.",
        },
        {
            "id": "price_impulse",
            "label": "Impulse",
            "passed": impulse_passed,
            "effect": 12 if impulse_passed else 0,
            "reason": "24h price change is large enough to keep the name on active review." if impulse_passed else "Momentum is still too small to force priority review.",
        },
        {
            "id": "funding_skew",
            "label": "Funding skew",
            "passed": skew_passed,
            "effect": 8 if skew_passed else 0,
            "reason": "Funding skew is not fighting the current move too aggressively." if skew_passed else "Funding does not add much directional confirmation right now.",
        },
        {
            "id": "range_expansion",
            "label": "Range expansion",
            "passed": range_passed,
            "effect": 8 if range_passed else 0,
            "reason": "The daily range is open enough to justify active monitoring." if range_passed else "Range is still compressed.",
        },
        {
            "id": "needle_trap_risk",
            "label": "Needle trap risk",
            "passed": needle_risk,
            "effect": -10 if needle_risk else 0,
            "reason": "Range is wide without directional follow-through, so wick risk is elevated." if needle_risk else "No obvious wick-trap penalty.",
        },
    ]


def bucket_for_score(score: float, price_change_pct: float, quote_volume: float, thresholds: dict[str, Any] | None = None) -> str:
    thresholds = thresholds or SCAN_BUCKET_LIMITS
    if score >= thresholds["research"] and quote_volume >= thresholds["liquidityFloorUsd"]:
        return "Research"
    if score >= thresholds["watch"] or abs(price_change_pct) >= 3:
        return "Watch"
    return "No trade"


def refresh_candidate_pool(exchange_id: str | None = None, instance_id: str | None = None) -> dict[str, Any]:
    gateway = get_active_exchange_gateway(exchange_id)
    universe = read_fixed_universe(instance_id)
    thresholds = SCAN_BUCKET_LIMITS
    resolved_source = resolve_candidate_symbols(universe=universe, exchange_id=gateway.exchange_id, instance_id=instance_id)
    symbols = resolved_source["symbols"]
    if not symbols:
        raise RuntimeError("config/fixed_universe.json does not contain any symbols.")

    tickers = gateway.fetch_all_tickers_24h()
    premium = gateway.fetch_all_premium_index()
    ticker_by_symbol = {str(item.get("symbol") or "").upper(): item for item in (tickers or [])}
    premium_by_symbol = {str(item.get("symbol") or "").upper(): item for item in (premium or [])}

    opportunities: list[dict[str, Any]] = []
    skipped_symbols: list[str] = []
    for symbol in symbols:
        resolved_symbol = gateway.normalize_symbol(symbol)
        ticker = ticker_by_symbol.get(resolved_symbol, {})
        premium_row = premium_by_symbol.get(resolved_symbol, {})
        if not ticker and not premium_row:
            skipped_symbols.append(resolved_symbol)
            continue
        last_price = num(ticker.get("lastPrice")) or num(premium_row.get("markPrice")) or 0
        high_price = num(ticker.get("highPrice")) or last_price
        low_price = num(ticker.get("lowPrice")) or last_price
        price_change_pct = num(ticker.get("priceChangePercent")) or 0
        quote_volume = num(ticker.get("quoteVolume")) or 0
        funding_pct = (num(premium_row.get("lastFundingRate")) or num(premium_row.get("fundingRate")) or 0) * 100
        range_pct = ((high_price - low_price) / last_price) * 100 if last_price > 0 and high_price > 0 and low_price > 0 else 0
        score = score_symbol(
            price_change_pct=price_change_pct,
            quote_volume=quote_volume,
            funding_pct=funding_pct,
            range_pct=range_pct,
        )
        action_bucket = bucket_for_score(score, price_change_pct, quote_volume, thresholds)
        bias = directional_bias(price_change_pct, funding_pct)
        strategies = matched_strategies(price_change_pct, funding_pct, score)
        signal_detectors = detectors(
            price_change_pct=price_change_pct,
            quote_volume=quote_volume,
            funding_pct=funding_pct,
            range_pct=range_pct,
            min_quote_volume_usd=thresholds["liquidityFloorUsd"],
        )
        flags = []
        if range_pct >= 18:
            flags.append("wide_range")
        if abs(price_change_pct) >= 10:
            flags.append("fast_move")
        if quote_volume < thresholds["liquidityFloorUsd"]:
            flags.append("light_liquidity")
        opportunities.append(
            {
                "symbol": resolved_symbol,
                "baseAsset": gateway.base_asset_from_symbol(resolved_symbol),
                "score": round(score, 2),
                "actionBucket": action_bucket,
                "directionalBias": bias,
                "summary": strategies[0]["reason"] if strategies else "Fixed-universe review candidate.",
                "matchedStrategies": strategies,
                "detectors": signal_detectors,
                "flags": flags,
                "market": {
                    "lastPrice": last_price,
                    "priceChangePct": price_change_pct,
                    "quoteVolume": quote_volume,
                    "fundingPct": funding_pct,
                    "rangePct": range_pct,
                    "openInterestNotional": None,
                },
            }
        )

    opportunities.sort(key=lambda item: float(item.get("score") or 0), reverse=True)
    payload = {
        "version": 1,
        "scanId": f"scan-{int(time.time() * 1000)}",
        "runDate": current_run_date(),
        "fetchedAt": __import__("datetime").datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "source": resolved_source["mode"],
        "exchange": gateway.exchange_id,
        "universeSize": len(symbols),
        "skippedSymbols": skipped_symbols,
        "candidateSource": {
            "mode": resolved_source["mode"],
            "enabled": resolved_source["enabled"],
            "functionName": resolved_source["functionName"],
            "durationMs": resolved_source["durationMs"],
            "invalidSymbols": resolved_source.get("invalidSymbols", []),
            "stdout": resolved_source["stdout"],
            "note": resolved_source["note"],
        },
        "opportunities": opportunities,
    }
    write_json(_latest_scan_path(gateway.exchange_id, instance_id), payload)
    archive_scan(payload, gateway.exchange_id, instance_id)
    if not instance_id and gateway.exchange_id == DEFAULT_EXCHANGE_ID:
        write_json(LEGACY_LATEST_SCAN_PATH, payload)
    return payload


def parse_klines(rows: list[list[Any]] | None) -> list[dict[str, Any]]:
    parsed = []
    for row in rows or []:
        close_value = num(row[4]) if len(row) > 4 else None
        if close_value is None:
            continue
        parsed.append(
            {
                "openTime": row[0],
                "open": num(row[1]),
                "high": num(row[2]),
                "low": num(row[3]),
                "close": close_value,
                "volume": num(row[5]),
                "closeTime": row[6],
                "quoteVolume": num(row[7]),
            }
        )
    return parsed


def average(values: list[float | None]) -> float | None:
    filtered = [value for value in values if value is not None]
    if not filtered:
        return None
    return sum(filtered) / len(filtered)


def ema(values: list[float | None], period: int) -> float | None:
    filtered = [value for value in values if value is not None]
    if not filtered:
        return None
    factor = 2 / (max(1, period) + 1)
    current = filtered[0]
    for value in filtered[1:]:
        current = (value * factor) + (current * (1 - factor))
    return current


def atr(candles: list[dict[str, Any]], period: int = 14) -> float | None:
    if len(candles) < 2:
        return None
    true_ranges = []
    for index in range(1, len(candles)):
        current = candles[index]
        previous = candles[index - 1]
        high = num(current.get("high"))
        low = num(current.get("low"))
        previous_close = num(previous.get("close"))
        if high is None or low is None or previous_close is None:
            continue
        true_ranges.append(max(high - low, abs(high - previous_close), abs(low - previous_close)))
    return average(true_ranges[-max(1, period):])


def highest(candles: list[dict[str, Any]], field: str, count: int) -> float | None:
    values = [num(item.get(field)) for item in candles[-max(1, count):]]
    filtered = [value for value in values if value is not None]
    return max(filtered) if filtered else None


def lowest(candles: list[dict[str, Any]], field: str, count: int) -> float | None:
    values = [num(item.get(field)) for item in candles[-max(1, count):]]
    filtered = [value for value in values if value is not None]
    return min(filtered) if filtered else None


def pct_distance(left: float | None, right: float | None) -> float | None:
    if left is None or right in (None, 0):
        return None
    return ((left - right) / right) * 100


PROMPT_KLINE_LIMIT = 20
PROMPT_KLINE_FETCH_SPECS = {
    "1m": {"limit": 120},
    "5m": {"limit": 96},
    "15m": {"limit": 64},
}


def normalize_prompt_kline_feeds(raw_feeds: Any) -> dict[str, dict[str, Any]]:
    normalized: dict[str, dict[str, Any]] = {}
    source = raw_feeds if isinstance(raw_feeds, dict) else {}
    for interval in PROMPT_KLINE_FEED_OPTIONS:
        current = source.get(interval) if isinstance(source.get(interval), dict) else {}
        normalized[interval] = {
            "enabled": current.get("enabled") is True,
            "limit": int(clamp(current.get("limit"), 1, 300)),
        }
        if not current:
            normalized[interval]["limit"] = PROMPT_KLINE_FETCH_SPECS[interval]["limit"]
    if not any(item["enabled"] for item in normalized.values()):
        normalized["15m"]["enabled"] = True
    return normalized


def enabled_prompt_kline_feeds(raw_feeds: Any) -> dict[str, dict[str, Any]]:
    feeds = normalize_prompt_kline_feeds(raw_feeds)
    return {
        interval: feeds[interval]
        for interval in PROMPT_KLINE_FEED_OPTIONS
        if feeds[interval]["enabled"]
    }


def compact_klines(candles: list[dict[str, Any]], limit: int = PROMPT_KLINE_LIMIT) -> list[dict[str, Any]]:
    rows = candles[-max(1, limit):]
    return [
        {
            "openTime": item.get("openTime"),
            "open": item.get("open"),
            "high": item.get("high"),
            "low": item.get("low"),
            "close": item.get("close"),
            "volume": item.get("volume"),
            "closeTime": item.get("closeTime"),
            "quoteVolume": item.get("quoteVolume"),
        }
        for item in rows
    ]


def fetch_klines(symbol: str, interval: str, limit: int, exchange_id: str | None = None) -> list[dict[str, Any]]:
    gateway = get_active_exchange_gateway(exchange_id)
    return gateway.fetch_klines(symbol, interval, limit)


def fetch_ticker_24h(symbol: str, exchange_id: str | None = None) -> dict[str, Any]:
    gateway = get_active_exchange_gateway(exchange_id)
    return gateway.fetch_ticker_24h(symbol)


def fetch_premium(symbol: str, exchange_id: str | None = None) -> dict[str, Any]:
    gateway = get_active_exchange_gateway(exchange_id)
    return gateway.fetch_premium(symbol)


def _fetch_prompt_kline_map(
    symbol: str,
    prompt_kline_feeds: Any,
    exchange_id: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    feeds = enabled_prompt_kline_feeds(prompt_kline_feeds)
    return {
        interval: compact_klines(fetch_klines(symbol, interval, feed["limit"], exchange_id), feed["limit"])
        for interval, feed in feeds.items()
    }


def _default_entry_side(opportunity: dict[str, Any], candles: list[dict[str, Any]], allow_shorts: bool) -> str | None:
    directional_bias = str(opportunity.get("directionalBias") or "").strip().lower()
    if directional_bias == "bullish":
        return "long"
    if allow_shorts and directional_bias == "bearish":
        return "short"
    first_close = num(candles[0].get("close")) if candles else None
    last_close = num(candles[-1].get("close")) if candles else None
    if first_close is None or last_close is None or first_close == last_close:
        return None
    if last_close > first_close:
        return "long"
    if allow_shorts:
        return "short"
    return None


def _default_stop_and_target(side: str | None, price: float | None, candles: list[dict[str, Any]]) -> tuple[float | None, float | None]:
    entry = num(price)
    atr_value = atr(candles, 14)
    if entry is None or side not in {"long", "short"}:
        return None, None
    raw_stop_pct = ((atr_value / entry) * 100 * 1.45) if atr_value else 3
    stop_pct = clamp(raw_stop_pct, 3, 12)
    reward_risk = 2.5
    if side == "short":
        stop_loss = entry * (1 + stop_pct / 100)
        take_profit = entry - ((stop_loss - entry) * reward_risk)
        return stop_loss, take_profit
    stop_loss = entry * (1 - stop_pct / 100)
    take_profit = entry + ((entry - stop_loss) * reward_risk)
    return stop_loss, take_profit


def _default_entry_confidence(opportunity: dict[str, Any], candles: list[dict[str, Any]], default_side: str | None) -> float:
    score = num(opportunity.get("score")) or 60
    first_close = num(candles[0].get("close")) if candles else None
    last_close = num(candles[-1].get("close")) if candles else None
    trend_bonus = 0
    if first_close is not None and last_close is not None and default_side in {"long", "short"}:
        if default_side == "long" and last_close > first_close:
            trend_bonus = 8
        elif default_side == "short" and last_close < first_close:
            trend_bonus = 8
        else:
            trend_bonus = -6
    return clamp(score + trend_bonus, 1, 100)


def fetch_candidate_live_context(
    symbol: str,
    prompt_kline_feeds: dict[str, Any] | None = None,
    exchange_id: str | None = None,
) -> dict[str, Any]:
    ticker_24h = fetch_ticker_24h(symbol, exchange_id)
    premium = fetch_premium(symbol, exchange_id)
    feeds = normalize_prompt_kline_feeds(prompt_kline_feeds)
    klines_by_interval = _fetch_prompt_kline_map(symbol, feeds, exchange_id)
    return {
        "symbol": symbol.upper(),
        "ticker24h": ticker_24h,
        "premium": premium,
        "promptKlineFeeds": feeds,
        "klinesByInterval": klines_by_interval,
    }


def fetch_market_backdrop(
    prompt_kline_feeds: dict[str, Any] | None = None,
    exchange_id: str | None = None,
) -> dict[str, Any]:
    gateway = get_active_exchange_gateway(exchange_id)
    live = fetch_candidate_live_context(gateway.default_backdrop_symbol, prompt_kline_feeds, gateway.exchange_id)
    price = num(live["premium"].get("markPrice")) or num(live["ticker24h"].get("lastPrice"))
    return {
        "symbol": gateway.default_backdrop_symbol,
        "price": price,
        "priceChangePct": live["ticker24h"].get("priceChangePct"),
        "quoteVolume": live["ticker24h"].get("quoteVolume"),
        "fundingPct": live["premium"].get("fundingPct"),
        "klineFeeds": live["promptKlineFeeds"],
        "klinesByInterval": live["klinesByInterval"],
    }


def candidate_universe_from_scan(scan: dict[str, Any]) -> list[dict[str, Any]]:
    ranked = sorted(
        scan.get("opportunities", []),
        key=lambda item: float(item.get("score") or 0),
        reverse=True,
    )
    return ranked


def build_candidate_snapshot(
    opportunity: dict[str, Any],
    live: dict[str, Any],
    settings: dict[str, Any],
    exchange_id: str | None = None,
) -> dict[str, Any]:
    gateway = get_active_exchange_gateway(exchange_id)
    price = num(live["premium"].get("markPrice")) or num(live["ticker24h"].get("lastPrice")) or num(opportunity.get("market", {}).get("lastPrice"))
    prompt_feeds = normalize_prompt_kline_feeds(live.get("promptKlineFeeds"))
    if prompt_feeds["15m"]["enabled"]:
        primary_interval = "15m"
    elif prompt_feeds["5m"]["enabled"]:
        primary_interval = "5m"
    else:
        primary_interval = "1m"
    primary_klines = list((live.get("klinesByInterval") or {}).get(primary_interval) or [])
    default_side = _default_entry_side(opportunity, primary_klines, settings["allowShorts"])
    default_stop_loss, default_take_profit = _default_stop_and_target(default_side, price, primary_klines)
    top_strategy = None
    strategies = opportunity.get("matchedStrategies") or []
    if strategies:
        top_strategy = strategies[0].get("name") or strategies[0].get("reason")
    return {
        "symbol": opportunity["symbol"],
        "baseAsset": opportunity.get("baseAsset") or gateway.base_asset_from_symbol(opportunity["symbol"]),
        "price": price,
        "priceChangePct": live["ticker24h"].get("priceChangePct"),
        "quoteVolume": live["ticker24h"].get("quoteVolume"),
        "fundingPct": live["premium"].get("fundingPct"),
        "klineFeeds": prompt_feeds,
        "klinesByInterval": live["klinesByInterval"],
        "defaultSide": default_side,
        "defaultStopLoss": default_stop_loss,
        "defaultTakeProfit": default_take_profit,
        "confidenceScore": _default_entry_confidence(opportunity, primary_klines, default_side),
        "topStrategy": top_strategy or opportunity.get("summary") or "",
    }
