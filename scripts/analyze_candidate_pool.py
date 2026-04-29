#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data" / "instances"
DEFAULT_HORIZONS = [1, 4, 12]


def read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def parse_ts(value: Any) -> float:
    if not value:
        return 0.0
    text = str(value).replace("Z", "+00:00")
    try:
        return __import__("datetime").datetime.fromisoformat(text).timestamp()
    except Exception:
        return 0.0


@dataclass
class Snapshot:
    ts: float
    timestamp: str
    prices: dict[str, float]
    selected: set[str]
    source: str


def _extract_price(item: dict[str, Any]) -> float | None:
    if not isinstance(item, dict):
        return None
    direct = item.get("price")
    if direct not in (None, ""):
        try:
            return float(direct)
        except Exception:
            return None
    market = item.get("market")
    if isinstance(market, dict):
        value = market.get("lastPrice")
        if value not in (None, ""):
            try:
                return float(value)
            except Exception:
                return None
    return None


def load_scan_archive_snapshots(instance_id: str) -> list[Snapshot]:
    base = DATA_DIR / instance_id / "scans"
    snapshots: list[Snapshot] = []
    if not base.exists():
        return snapshots
    ref_seen: set[tuple[str, str]] = set()
    for exchange_dir in sorted(base.iterdir()):
        refs_root = exchange_dir / "archive_refs"
        if refs_root.exists():
            for day_dir in sorted(refs_root.iterdir()):
                if not day_dir.is_dir():
                    continue
                for path in sorted(day_dir.glob("*.json")):
                    ref = read_json(path, {})
                    if not isinstance(ref, dict):
                        continue
                    shared_value = str(ref.get("sharedPath") or "").strip()
                    timestamp = str(ref.get("fetchedAt") or "")
                    if not shared_value or not timestamp:
                        continue
                    shared_path = (ROOT / shared_value).resolve()
                    payload = read_json(shared_path, {})
                    if not isinstance(payload, dict):
                        continue
                    key = (timestamp, str(shared_path))
                    if key in ref_seen:
                        continue
                    ref_seen.add(key)
                    prices: dict[str, float] = {}
                    for item in payload.get("opportunities") or []:
                        if not isinstance(item, dict):
                            continue
                        symbol = str(item.get("symbol") or "").upper()
                        price = _extract_price(item)
                        if symbol and price and price > 0:
                            prices[symbol] = price
                    if prices:
                        snapshots.append(
                            Snapshot(
                                ts=parse_ts(timestamp),
                                timestamp=timestamp,
                                prices=prices,
                                selected=set(),
                                source="scan_archive",
                            )
                        )
        archive_root = exchange_dir / "archive"
        if not archive_root.exists():
            continue
        for day_dir in sorted(archive_root.iterdir()):
            if not day_dir.is_dir():
                continue
            for path in sorted(day_dir.glob("*.json")):
                payload = read_json(path, {})
                if not isinstance(payload, dict):
                    continue
                timestamp = str(payload.get("fetchedAt") or "")
                key = (timestamp, str(path.resolve()))
                if key in ref_seen:
                    continue
                prices: dict[str, float] = {}
                for item in payload.get("opportunities") or []:
                    if not isinstance(item, dict):
                        continue
                    symbol = str(item.get("symbol") or "").upper()
                    price = _extract_price(item)
                    if symbol and price and price > 0:
                        prices[symbol] = price
                if prices and timestamp:
                    ref_seen.add(key)
                    snapshots.append(
                        Snapshot(
                            ts=parse_ts(timestamp),
                            timestamp=timestamp,
                            prices=prices,
                            selected=set(),
                            source="scan_archive",
                        )
                    )
    snapshots.sort(key=lambda row: row.ts)
    return snapshots


def load_decision_snapshots(instance_id: str) -> list[Snapshot]:
    base = DATA_DIR / instance_id / "decisions"
    snapshots: list[Snapshot] = []
    if not base.exists():
        return snapshots
    for day_dir in sorted(base.iterdir()):
        if not day_dir.is_dir():
            continue
        for path in sorted(day_dir.glob("*.json")):
            payload = read_json(path, {})
            if not isinstance(payload, dict):
                continue
            timestamp = str(payload.get("finishedAt") or payload.get("startedAt") or "")
            prices: dict[str, float] = {}
            for item in payload.get("candidateUniverse") or []:
                if not isinstance(item, dict):
                    continue
                symbol = str(item.get("symbol") or "").upper()
                price = _extract_price(item)
                if symbol and price and price > 0:
                    prices[symbol] = price
            selected = set()
            output = payload.get("output") if isinstance(payload.get("output"), dict) else {}
            for action in output.get("entryActions") or []:
                if not isinstance(action, dict):
                    continue
                symbol = str(action.get("symbol") or "").upper()
                if symbol:
                    selected.add(symbol)
            if prices and timestamp:
                snapshots.append(
                    Snapshot(
                        ts=parse_ts(timestamp),
                        timestamp=timestamp,
                        prices=prices,
                        selected=selected,
                        source="decision_history",
                    )
                )
    snapshots.sort(key=lambda row: row.ts)
    return snapshots


def choose_candidate_snapshots(instance_id: str) -> tuple[str, list[Snapshot]]:
    archived = load_scan_archive_snapshots(instance_id)
    if archived:
        return "scan_archive", archived
    return "decision_history", load_decision_snapshots(instance_id)


def evaluate_symbol_window(symbol: str, entry_price: float, entry_ts: float, snapshots: list[Snapshot], hours: int) -> dict[str, Any] | None:
    cutoff = entry_ts + hours * 3600
    observed = [snap.prices[symbol] for snap in snapshots if snap.ts > entry_ts and snap.ts <= cutoff and symbol in snap.prices]
    if not observed:
        return None
    last_price = observed[-1]
    max_price = max(observed)
    min_price = min(observed)
    terminal = (last_price / entry_price - 1) * 100
    upside = (max_price / entry_price - 1) * 100
    downside = (min_price / entry_price - 1) * 100
    abs_move = max(abs(upside), abs(downside))
    return {
        "terminalPct": terminal,
        "upsidePct": upside,
        "downsidePct": downside,
        "absMovePct": abs_move,
        "observations": len(observed),
    }


def build_candidate_entry_events(snapshots: list[Snapshot]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    previous: set[str] = set()
    for snap in snapshots:
        current = set(snap.prices.keys())
        for symbol in sorted(current - previous):
            events.append(
                {
                    "symbol": symbol,
                    "timestamp": snap.timestamp,
                    "ts": snap.ts,
                    "entryPrice": snap.prices[symbol],
                }
            )
        previous = current
    return events


def summarize_window(rows: list[dict[str, Any]], key_prefix: str = "") -> dict[str, Any]:
    observed = [row for row in rows if row.get("metrics") is not None]
    values = [row["metrics"] for row in observed]
    if not values:
        return {
            "count": len(rows),
            "observed": 0,
        }
    terminal = [item["terminalPct"] for item in values]
    upside = [item["upsidePct"] for item in values]
    abs_move = [item["absMovePct"] for item in values]
    return {
        "count": len(rows),
        "observed": len(observed),
        "avgTerminalPct": round(sum(terminal) / len(terminal), 2),
        "medianTerminalPct": round(statistics.median(terminal), 2),
        "avgUpsidePct": round(sum(upside) / len(upside), 2),
        "avgAbsMovePct": round(sum(abs_move) / len(abs_move), 2),
        "hitAbs10Pct": round(sum(1 for item in abs_move if item >= 10) / len(abs_move) * 100, 1),
        "hitAbs20Pct": round(sum(1 for item in abs_move if item >= 20) / len(abs_move) * 100, 1),
        "hitUpside10Pct": round(sum(1 for item in upside if item >= 10) / len(upside) * 100, 1),
        "hitUpside20Pct": round(sum(1 for item in upside if item >= 20) / len(upside) * 100, 1),
    }


def analyze_candidate_entries(instance_id: str, snapshots: list[Snapshot], horizons: list[int]) -> dict[str, Any]:
    events = build_candidate_entry_events(snapshots)
    results: dict[str, Any] = {
        "events": len(events),
        "windows": {},
    }
    for hours in horizons:
        rows = []
        for event in events:
            rows.append(
                {
                    **event,
                    "metrics": evaluate_symbol_window(event["symbol"], event["entryPrice"], event["ts"], snapshots, hours),
                }
            )
        results["windows"][f"{hours}h"] = summarize_window(rows)
        ranked = sorted(
            [row for row in rows if row.get("metrics")],
            key=lambda row: row["metrics"]["absMovePct"],
            reverse=True,
        )[:8]
        results["windows"][f"{hours}h"]["topMoves"] = [
            {
                "symbol": row["symbol"],
                "timestamp": row["timestamp"],
                "entryPrice": row["entryPrice"],
                "upsidePct": round(row["metrics"]["upsidePct"], 2),
                "downsidePct": round(row["metrics"]["downsidePct"], 2),
                "absMovePct": round(row["metrics"]["absMovePct"], 2),
            }
            for row in ranked
        ]
    return results


def analyze_model_selection(instance_id: str, decision_snapshots: list[Snapshot], horizons: list[int]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for snap in decision_snapshots:
        if not snap.selected:
            continue
        for symbol, entry_price in snap.prices.items():
            rows.append(
                {
                    "symbol": symbol,
                    "timestamp": snap.timestamp,
                    "ts": snap.ts,
                    "entryPrice": entry_price,
                    "selected": symbol in snap.selected,
                }
            )
    results: dict[str, Any] = {"candidateRows": len(rows), "windows": {}}
    for hours in horizons:
        selected_rows = []
        unselected_rows = []
        for row in rows:
            payload = {
                **row,
                "metrics": evaluate_symbol_window(row["symbol"], row["entryPrice"], row["ts"], decision_snapshots, hours),
            }
            (selected_rows if row["selected"] else unselected_rows).append(payload)
        results["windows"][f"{hours}h"] = {
            "selected": summarize_window(selected_rows),
            "unselected": summarize_window(unselected_rows),
        }
    return results


def print_summary(report: dict[str, Any]) -> None:
    print(f"Instance: {report['instanceId']}")
    print(f"Candidate source for analysis: {report['candidateSource']}")
    print(f"Candidate snapshots: {report['candidateSnapshots']}")
    print(f"Decision snapshots: {report['decisionSnapshots']}")
    print()
    print("Candidate Entry Study")
    print("---------------------")
    print(f"Entry events: {report['candidateEntries']['events']}")
    for window, stats in report["candidateEntries"]["windows"].items():
        print(
            f"{window}: observed {stats.get('observed', 0)}/{stats.get('count', 0)} | "
            f"avg terminal {stats.get('avgTerminalPct', 'n/a')}% | "
            f"avg upside {stats.get('avgUpsidePct', 'n/a')}% | "
            f"avg abs move {stats.get('avgAbsMovePct', 'n/a')}% | "
            f"hit abs>=10 {stats.get('hitAbs10Pct', 'n/a')}% | "
            f"hit abs>=20 {stats.get('hitAbs20Pct', 'n/a')}%"
        )
    print()
    top_4h = report["candidateEntries"]["windows"].get("4h", {}).get("topMoves", [])
    if top_4h:
        print("Top 4H candidate-entry moves")
        print("----------------------------")
        for row in top_4h:
            print(
                f"{row['timestamp']} {row['symbol']} | entry {row['entryPrice']:.6f} | "
                f"upside {row['upsidePct']:+.2f}% | downside {row['downsidePct']:+.2f}% | abs {row['absMovePct']:.2f}%"
            )
        print()
    print("Model Selection Study")
    print("---------------------")
    for window, stats in report["selectionStudy"]["windows"].items():
        selected = stats["selected"]
        unselected = stats["unselected"]
        print(
            f"{window}: selected avg abs {selected.get('avgAbsMovePct', 'n/a')}% "
            f"(obs {selected.get('observed', 0)}) vs "
            f"unselected avg abs {unselected.get('avgAbsMovePct', 'n/a')}% "
            f"(obs {unselected.get('observed', 0)})"
        )


def build_report(instance_id: str, horizons: list[int]) -> dict[str, Any]:
    candidate_source, candidate_snapshots = choose_candidate_snapshots(instance_id)
    decision_snapshots = load_decision_snapshots(instance_id)
    return {
        "instanceId": instance_id,
        "candidateSource": candidate_source,
        "candidateSnapshots": len(candidate_snapshots),
        "decisionSnapshots": len(decision_snapshots),
        "candidateEntries": analyze_candidate_entries(instance_id, candidate_snapshots, horizons),
        "selectionStudy": analyze_model_selection(instance_id, decision_snapshots, horizons),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze candidate-pool quality and model selection quality.")
    parser.add_argument("--instance", required=True, help="Instance id, e.g. paper-default")
    parser.add_argument("--horizons", nargs="*", type=int, default=DEFAULT_HORIZONS, help="Future windows in hours")
    parser.add_argument("--json", action="store_true", help="Print JSON instead of text")
    args = parser.parse_args()

    report = build_report(args.instance, [item for item in args.horizons if item > 0] or DEFAULT_HORIZONS)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return
    print_summary(report)


if __name__ == "__main__":
    main()
