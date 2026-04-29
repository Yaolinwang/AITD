from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any

from .instances import instance_paths
from .utils import DATA_DIR, num, read_json


LEGACY_DECISIONS_DIR = DATA_DIR / "trading-agent" / "decisions"
CURRENT_CONTEXT_MARKER = "# Current Trading Context"


def _decisions_dir(instance_id: str | None = None) -> Path:
    if instance_id:
        return instance_paths(instance_id)["decisions_dir"]
    return LEGACY_DECISIONS_DIR


def _parse_ts(value: Any) -> float:
    if not value:
        return 0.0
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _read_decision_payloads(instance_id: str | None, limit: int) -> list[dict[str, Any]]:
    root = _decisions_dir(instance_id)
    if not root.exists():
        return []
    paths: list[Path] = []
    for day_dir in sorted(root.iterdir()):
        if day_dir.is_dir():
            paths.extend(sorted(day_dir.glob("*.json")))
    rows: list[dict[str, Any]] = []
    for path in paths[-max(limit * 2, limit):]:
        payload = read_json(path, {})
        if not isinstance(payload, dict):
            continue
        ts = _parse_ts(payload.get("finishedAt") or payload.get("startedAt"))
        if ts <= 0:
            continue
        rows.append({"path": str(path), "ts": ts, "payload": payload})
    rows.sort(key=lambda item: item["ts"])
    return rows[-limit:]


def _parse_prompt_context(prompt: Any) -> dict[str, Any] | None:
    text = str(prompt or "")
    marker_index = text.find(CURRENT_CONTEXT_MARKER)
    if marker_index < 0:
        return None
    context_text = text[marker_index + len(CURRENT_CONTEXT_MARKER):].strip()
    if not context_text:
        return None
    try:
        payload = json.loads(context_text)
    except Exception:
        try:
            payload, _ = json.JSONDecoder().raw_decode(context_text)
        except Exception:
            return None
    return payload if isinstance(payload, dict) else None


def _candidate_rank_map(candidates: list[dict[str, Any]]) -> dict[str, int]:
    return {
        str(candidate.get("symbol") or "").upper(): index + 1
        for index, candidate in enumerate(candidates)
        if isinstance(candidate, dict) and str(candidate.get("symbol") or "").strip()
    }


def _price_map(payload: dict[str, Any], context: dict[str, Any] | None) -> dict[str, float]:
    prices: dict[str, float] = {}
    for source in (
        (context or {}).get("candidates") if isinstance((context or {}).get("candidates"), list) else [],
        payload.get("candidateUniverse") if isinstance(payload.get("candidateUniverse"), list) else [],
    ):
        for item in source:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol") or "").upper()
            price = num(item.get("price"))
            if symbol and price and price > 0:
                prices[symbol] = price
    return prices


def _enabled_klines(candidate: dict[str, Any]) -> list[dict[str, Any]]:
    by_interval = candidate.get("klinesByInterval") if isinstance(candidate.get("klinesByInterval"), dict) else {}
    for interval in ("15m", "5m", "1m"):
        rows = by_interval.get(interval)
        if isinstance(rows, list) and rows:
            return [item for item in rows if isinstance(item, dict)]
    return []


def _bucket(value: float | None, bands: list[tuple[float, str]], fallback: str = "unknown") -> str:
    if value is None:
        return fallback
    for limit, label in bands:
        if value <= limit:
            return label
    return bands[-1][1] if bands else fallback


def _scene_for_candidate(candidate: dict[str, Any], rank: int | None = None, side: str | None = None) -> dict[str, Any]:
    candles = _enabled_klines(candidate)
    first_close = num(candles[0].get("close")) if candles else None
    last_close = num(candles[-1].get("close")) if candles else None
    highs = [num(item.get("high")) for item in candles]
    lows = [num(item.get("low")) for item in candles]
    volumes = [num(item.get("quoteVolume")) or num(item.get("volume")) for item in candles]
    highs = [item for item in highs if item is not None and item > 0]
    lows = [item for item in lows if item is not None and item > 0]
    volumes = [item for item in volumes if item is not None and item > 0]
    kline_change = None
    if first_close and last_close:
        kline_change = (last_close / first_close - 1) * 100
    range_pct = None
    if highs and lows and last_close:
        range_pct = (max(highs) - min(lows)) / last_close * 100
    pullback_pct = None
    if highs and last_close:
        pullback_pct = (max(highs) - last_close) / max(highs) * 100
    recent_volume_ratio = None
    if len(volumes) >= 10:
        recent = sum(volumes[-5:]) / 5
        prior = sum(volumes[-10:-5]) / 5
        if prior > 0:
            recent_volume_ratio = recent / prior
    price_change = num(candidate.get("priceChangePct"))
    funding = num(candidate.get("fundingPct"))
    quote_volume = num(candidate.get("quoteVolume"))
    return {
        "side": "short" if str(side or candidate.get("defaultSide") or "").lower() == "short" else "long",
        "rankBucket": "top3" if rank and rank <= 3 else "top10" if rank and rank <= 10 else "deep" if rank else "unknown",
        "trendBucket": _bucket(kline_change, [(-4, "down_strong"), (-1, "down"), (1, "flat"), (4, "up"), (999, "up_strong")]),
        "impulseBucket": _bucket(price_change, [(-10, "selloff"), (-3, "down"), (3, "flat"), (10, "up"), (999, "hot")]),
        "pullbackBucket": _bucket(pullback_pct, [(1.5, "near_high"), (5, "shallow"), (12, "normal"), (999, "deep")]),
        "volatilityBucket": _bucket(range_pct, [(4, "quiet"), (10, "normal"), (20, "wide"), (999, "chaotic")]),
        "volumeBucket": _bucket(recent_volume_ratio, [(0.75, "fading"), (1.25, "steady"), (999, "expanding")]),
        "liquidityBucket": _bucket(math.log10(quote_volume + 1) if quote_volume and quote_volume > 0 else None, [(6, "thin"), (7.5, "normal"), (999, "deep")]),
        "fundingBucket": _bucket(funding, [(-0.05, "negative"), (0.05, "neutral"), (999, "positive")]),
    }


def _scene_similarity(a: dict[str, Any], b: dict[str, Any]) -> float:
    weights = {
        "side": 2.5,
        "trendBucket": 3.0,
        "impulseBucket": 2.0,
        "pullbackBucket": 2.0,
        "volatilityBucket": 2.0,
        "volumeBucket": 1.5,
        "liquidityBucket": 1.0,
        "fundingBucket": 1.0,
        "rankBucket": 1.0,
    }
    return sum(weight for key, weight in weights.items() if a.get(key) == b.get(key))


def _future_outcome(
    *,
    rows: list[dict[str, Any]],
    symbol: str,
    side: str,
    entry_price: float,
    entry_ts: float,
    horizon_hours: int,
) -> dict[str, Any] | None:
    cutoff = entry_ts + horizon_hours * 3600
    observed = [
        row["prices"][symbol]
        for row in rows
        if entry_ts < row["ts"] <= cutoff and symbol in row.get("prices", {})
    ]
    if not observed:
        return None
    if side == "short":
        terminal_pct = (entry_price / observed[-1] - 1) * 100
        favorable_pct = (entry_price / min(observed) - 1) * 100
        adverse_pct = (entry_price / max(observed) - 1) * 100
    else:
        terminal_pct = (observed[-1] / entry_price - 1) * 100
        favorable_pct = (max(observed) / entry_price - 1) * 100
        adverse_pct = (min(observed) / entry_price - 1) * 100
    return {
        "terminalPct": round(terminal_pct, 2),
        "maxFavorablePct": round(favorable_pct, 2),
        "maxAdversePct": round(adverse_pct, 2),
        "observations": len(observed),
    }


def _lesson_text(symbol: str, side: str, scene: dict[str, Any], outcome: dict[str, Any], reason: str, horizon_hours: int) -> str:
    result = "worked" if outcome["terminalPct"] > 0 else "failed"
    reason_text = f" Model reason: {reason[:160]}" if reason else ""
    return (
        f"{side.upper()} scene {result} over {horizon_hours}h: "
        f"trend={scene['trendBucket']}, impulse={scene['impulseBucket']}, "
        f"pullback={scene['pullbackBucket']}, volatility={scene['volatilityBucket']}, "
        f"terminal={outcome['terminalPct']}%, best={outcome['maxFavorablePct']}%, worst={outcome['maxAdversePct']}%."
        f"{reason_text}"
    )


def historical_lessons_for_prompt(
    *,
    instance_id: str | None,
    settings: dict[str, Any],
    candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    learning = settings.get("selfLearning") if isinstance(settings.get("selfLearning"), dict) else {}
    if learning.get("enabled") is not True:
        return []
    max_lessons = int(learning.get("maxLessons") or 5)
    lookback = int(learning.get("lookbackDecisions") or 300)
    horizon_hours = int(learning.get("reviewHorizonHours") or 4)
    current_ranks = _candidate_rank_map(candidates)
    current_scenes = [
        {
            "symbol": str(candidate.get("symbol") or "").upper(),
            "scene": _scene_for_candidate(candidate, current_ranks.get(str(candidate.get("symbol") or "").upper())),
        }
        for candidate in candidates
        if isinstance(candidate, dict) and str(candidate.get("symbol") or "").strip()
    ]
    if not current_scenes:
        return []

    raw_rows = _read_decision_payloads(instance_id, lookback)
    rows: list[dict[str, Any]] = []
    for row in raw_rows:
        payload = row["payload"]
        context = _parse_prompt_context(payload.get("prompt"))
        rows.append(
            {
                **row,
                "context": context,
                "prices": _price_map(payload, context),
            }
        )

    lessons: list[dict[str, Any]] = []
    for row in rows:
        context = row.get("context")
        if not isinstance(context, dict):
            continue
        candidates_at_decision = context.get("candidates") if isinstance(context.get("candidates"), list) else []
        rank_map = _candidate_rank_map(candidates_at_decision)
        candidates_by_symbol = {
            str(candidate.get("symbol") or "").upper(): candidate
            for candidate in candidates_at_decision
            if isinstance(candidate, dict)
        }
        output = row["payload"].get("output") if isinstance(row["payload"].get("output"), dict) else {}
        entry_actions = output.get("entryActions") if isinstance(output.get("entryActions"), list) else []
        for action in entry_actions:
            if not isinstance(action, dict):
                continue
            symbol = str(action.get("symbol") or "").upper()
            if not symbol:
                continue
            side = "short" if str(action.get("side") or "").lower() == "short" else "long"
            candidate = candidates_by_symbol.get(symbol)
            if not candidate:
                continue
            entry_price = num(candidate.get("price"))
            if not entry_price or entry_price <= 0:
                continue
            outcome = _future_outcome(
                rows=rows,
                symbol=symbol,
                side=side,
                entry_price=entry_price,
                entry_ts=row["ts"],
                horizon_hours=horizon_hours,
            )
            if not outcome:
                continue
            scene = _scene_for_candidate(candidate, rank_map.get(symbol), side)
            matches = [
                {
                    "symbol": current["symbol"],
                    "score": _scene_similarity(scene, current["scene"]),
                }
                for current in current_scenes
            ]
            best_match = max(matches, key=lambda item: item["score"], default={"symbol": "", "score": 0})
            if best_match["score"] <= 0:
                continue
            lessons.append(
                {
                    "sourceDecisionId": row["payload"].get("id"),
                    "sourceSymbol": symbol,
                    "matchedCurrentSymbol": best_match["symbol"],
                    "similarityScore": round(best_match["score"], 2),
                    "side": side,
                    "outcome": "winner" if outcome["terminalPct"] > 0 else "loser",
                    "horizonHours": horizon_hours,
                    "terminalPct": outcome["terminalPct"],
                    "maxFavorablePct": outcome["maxFavorablePct"],
                    "maxAdversePct": outcome["maxAdversePct"],
                    "scene": scene,
                    "lesson": _lesson_text(symbol, side, scene, outcome, str(action.get("reason") or ""), horizon_hours),
                }
            )

    lessons.sort(
        key=lambda item: (
            item["similarityScore"],
            abs(float(item.get("terminalPct") or 0)),
            str(item.get("sourceDecisionId") or ""),
        ),
        reverse=True,
    )
    return lessons[:max_lessons]
