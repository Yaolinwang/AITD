from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from pathlib import Path
from typing import Any

from .config import (
    read_fixed_universe,
    read_live_trading_config,
    read_llm_provider,
    read_prompt_settings,
    read_trading_settings,
)
from .exchanges import base_asset_for_symbol
from .live_trading import (
    apply_symbol_settings,
    cancel_all_open_orders,
    fetch_account_snapshot,
    live_execution_status,
    normalize_quantity,
    place_market_order,
    place_protection_orders,
)
from .llm import generate_trading_decision, provider_status
from .market import (
    build_candidate_snapshot,
    candidate_universe_from_scan,
    fetch_candidate_live_context,
    fetch_market_backdrop,
    read_latest_scan,
    refresh_candidate_pool,
)
from .utils import DATA_DIR, clamp, current_run_date, now_iso, num, one_line, read_json, safe_last, write_json


STATE_PATH = DATA_DIR / "trading_agent_state.json"
DECISIONS_DIR = DATA_DIR / "trading-agent" / "decisions"


def clean_mode(value: Any) -> str:
    return "live" if str(value or "paper").strip().lower() == "live" else "paper"


def account_key_for_mode(value: Any) -> str:
    return "live" if clean_mode(value) == "live" else "paper"


def enabled_modes(settings: dict[str, Any]) -> list[str]:
    modes: list[str] = []
    if settings.get("paperTrading", {}).get("enabled"):
        modes.append("paper")
    if settings.get("liveTrading", {}).get("enabled"):
        modes.append("live")
    return modes


def empty_trading_account(initial_capital_usd: float, source: str) -> dict[str, Any]:
    return {
        "initialCapitalUsd": initial_capital_usd,
        "accountSource": source,
        "highWatermarkEquity": initial_capital_usd,
        "sessionStartedAt": None,
        "lastDecisionAt": None,
        "circuitBreakerTripped": False,
        "circuitBreakerReason": None,
        "exchangeWalletBalanceUsd": None,
        "exchangeEquityUsd": None,
        "exchangeAvailableBalanceUsd": None,
        "exchangeUnrealizedPnlUsd": None,
        "exchangeNetCashflowUsd": None,
        "exchangeIncomeRealizedPnlUsd": None,
        "exchangeFundingFeeUsd": None,
        "exchangeCommissionUsd": None,
        "exchangeOtherIncomeUsd": None,
        "exchangeAccountingUpdatedAt": None,
        "exchangeAccountingNote": None,
        "openPositions": [],
        "openOrders": [],
        "exchangeClosedTrades": [],
        "closedTrades": [],
        "decisions": [],
    }


def default_state(settings: dict[str, Any] | None = None) -> dict[str, Any]:
    settings = settings or read_trading_settings()
    return {
        "version": 2,
        "updatedAt": now_iso(),
        "paper": empty_trading_account(settings["initialCapitalUsd"], "paper"),
        "live": empty_trading_account(settings["initialCapitalUsd"], "exchange"),
        "adaptive": {
            "updatedAt": None,
            "notes": [
                "The Python build keeps execution logic local and uses the editable trade-logic fields only for trade judgment.",
                "Paper mode and live mode can be started independently from the dashboard.",
            ],
        },
    }


def normalize_position(position: dict[str, Any]) -> dict[str, Any]:
    side = "short" if str(position.get("side") or "long").lower() == "short" else "long"
    symbol = str(position.get("symbol") or "").upper()
    exchange_id = str(position.get("source") or "binance").strip().lower() or "binance"
    quantity = num(position.get("quantity")) or 0
    entry_price = num(position.get("entryPrice")) or 0
    notional = num(position.get("notionalUsd")) or quantity * entry_price
    return {
        "id": str(position.get("id") or f"{symbol}-{int(__import__('time').time() * 1000)}"),
        "symbol": symbol,
        "baseAsset": str(position.get("baseAsset") or base_asset_for_symbol(symbol, exchange_id)),
        "side": side,
        "quantity": quantity,
        "initialQuantity": num(position.get("initialQuantity")) or quantity,
        "entryPrice": entry_price,
        "notionalUsd": notional,
        "initialNotionalUsd": num(position.get("initialNotionalUsd")) or notional,
        "stopLoss": num(position.get("stopLoss")),
        "takeProfit": num(position.get("takeProfit")),
        "lastMarkPrice": num(position.get("lastMarkPrice")) or entry_price,
        "lastMarkTime": position.get("lastMarkTime") or now_iso(),
        "leverage": num(position.get("leverage")) or 1,
        "status": "open",
        "openedAt": position.get("openedAt"),
        "updatedAt": position.get("updatedAt") or now_iso(),
        "source": position.get("source") or "trading_agent",
        "entryReason": position.get("entryReason") or "",
        "decisionId": position.get("decisionId"),
        "confidenceScore": num(position.get("confidenceScore")),
    }


def normalize_trade(trade: dict[str, Any]) -> dict[str, Any]:
    symbol = str(trade.get("symbol") or "").upper()
    exchange_id = str(trade.get("source") or "binance").strip().lower() or "binance"
    return {
        "id": str(trade.get("id") or f"trade-{int(__import__('time').time() * 1000)}"),
        "positionId": trade.get("positionId"),
        "symbol": symbol,
        "baseAsset": str(trade.get("baseAsset") or base_asset_for_symbol(symbol, exchange_id)),
        "side": "short" if str(trade.get("side") or "long").lower() == "short" else "long",
        "quantity": num(trade.get("quantity")) or 0,
        "entryPrice": num(trade.get("entryPrice")) or 0,
        "exitPrice": num(trade.get("exitPrice")) or 0,
        "notionalUsd": num(trade.get("notionalUsd")) or 0,
        "realizedPnl": num(trade.get("realizedPnl")) or 0,
        "openedAt": trade.get("openedAt"),
        "closedAt": trade.get("closedAt") or now_iso(),
        "exitReason": trade.get("exitReason") or "manual",
        "decisionId": trade.get("decisionId"),
    }


def normalize_exchange_closed_trade(trade: dict[str, Any]) -> dict[str, Any]:
    symbol = str(trade.get("symbol") or "").upper()
    exchange_id = str(trade.get("source") or "binance").strip().lower() or "binance"
    return {
        "id": str(trade.get("id") or f"exchange-close-{int(__import__('time').time() * 1000)}"),
        "symbol": symbol,
        "baseAsset": str(trade.get("baseAsset") or base_asset_for_symbol(symbol, exchange_id)),
        "realizedPnl": num(trade.get("realizedPnl")) or 0,
        "asset": str(trade.get("asset") or "USDT").strip().upper() or "USDT",
        "closedAt": trade.get("closedAt") or now_iso(),
        "info": str(trade.get("info") or "").strip(),
        "source": str(trade.get("source") or exchange_id),
    }


def normalize_order(order: dict[str, Any]) -> dict[str, Any]:
    symbol = str(order.get("symbol") or "").upper()
    exchange_id = str(order.get("source") or "binance").strip().lower() or "binance"
    return {
        "id": str(order.get("id") or f"order-{int(__import__('time').time() * 1000)}"),
        "symbol": symbol,
        "baseAsset": str(order.get("baseAsset") or base_asset_for_symbol(symbol, exchange_id)),
        "side": str(order.get("side") or "").upper(),
        "positionSide": str(order.get("positionSide") or "").upper(),
        "type": str(order.get("type") or "").upper(),
        "status": str(order.get("status") or "").upper(),
        "price": num(order.get("price")),
        "triggerPrice": num(order.get("triggerPrice")),
        "quantity": num(order.get("quantity")),
        "reduceOnly": order.get("reduceOnly") is True,
        "closePosition": order.get("closePosition") is True,
        "workingType": str(order.get("workingType") or "").upper(),
        "source": str(order.get("source") or exchange_id),
        "updatedAt": order.get("updatedAt") or now_iso(),
    }


def derive_session_started_at(book: dict[str, Any]) -> str | None:
    candidates: list[str] = []
    if book.get("sessionStartedAt"):
        candidates.append(str(book.get("sessionStartedAt")))
    if book.get("lastDecisionAt"):
        candidates.append(str(book.get("lastDecisionAt")))
    for decision in book.get("decisions", []):
        if isinstance(decision, dict):
            if decision.get("startedAt"):
                candidates.append(str(decision.get("startedAt")))
            if decision.get("finishedAt"):
                candidates.append(str(decision.get("finishedAt")))
    for trade in book.get("closedTrades", []):
        if isinstance(trade, dict):
            if trade.get("openedAt"):
                candidates.append(str(trade.get("openedAt")))
            if trade.get("closedAt"):
                candidates.append(str(trade.get("closedAt")))
    for position in book.get("openPositions", []):
        if isinstance(position, dict) and position.get("openedAt"):
            candidates.append(str(position.get("openedAt")))

    parsed: list[tuple[float, str]] = []
    for value in candidates:
        try:
            dt = __import__("datetime").datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            continue
        parsed.append((dt.timestamp(), value))
    if not parsed:
        return None
    parsed.sort(key=lambda item: item[0])
    return parsed[0][1]


def normalize_decision(decision: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(decision.get("id") or f"decision-{int(__import__('time').time() * 1000)}"),
        "startedAt": decision.get("startedAt") or now_iso(),
        "finishedAt": decision.get("finishedAt") or now_iso(),
        "runnerReason": decision.get("runnerReason") or "manual",
        "mode": clean_mode(decision.get("mode")),
        "prompt": str(decision.get("prompt") or ""),
        "promptSummary": str(decision.get("promptSummary") or ""),
        "output": decision.get("output") if isinstance(decision.get("output"), dict) else {},
        "rawModelResponse": decision.get("rawModelResponse") if isinstance(decision.get("rawModelResponse"), dict) else {},
        "actions": decision.get("actions") if isinstance(decision.get("actions"), list) else [],
        "warnings": decision.get("warnings") if isinstance(decision.get("warnings"), list) else [],
        "candidateUniverse": decision.get("candidateUniverse") if isinstance(decision.get("candidateUniverse"), list) else [],
        "accountBefore": decision.get("accountBefore") if isinstance(decision.get("accountBefore"), dict) else {},
        "accountAfter": decision.get("accountAfter") if isinstance(decision.get("accountAfter"), dict) else {},
    }


def read_trading_state(settings: dict[str, Any] | None = None) -> dict[str, Any]:
    settings = settings or read_trading_settings()
    saved = read_json(STATE_PATH, {})
    state = default_state(settings)
    for key in ("paper", "live"):
        source = "exchange" if key == "live" else "paper"
        seed = saved.get(key) if isinstance(saved.get(key), dict) else {}
        normalized = {
            **empty_trading_account(settings["initialCapitalUsd"], source),
            **seed,
            "initialCapitalUsd": num(seed.get("initialCapitalUsd")) or settings["initialCapitalUsd"],
            "accountSource": seed.get("accountSource") or source,
            "highWatermarkEquity": num(seed.get("highWatermarkEquity")) or settings["initialCapitalUsd"],
            "exchangeWalletBalanceUsd": num(seed.get("exchangeWalletBalanceUsd")),
            "exchangeEquityUsd": num(seed.get("exchangeEquityUsd")),
            "exchangeAvailableBalanceUsd": num(seed.get("exchangeAvailableBalanceUsd")),
            "exchangeUnrealizedPnlUsd": num(seed.get("exchangeUnrealizedPnlUsd")),
            "exchangeNetCashflowUsd": num(seed.get("exchangeNetCashflowUsd")),
            "exchangeIncomeRealizedPnlUsd": num(seed.get("exchangeIncomeRealizedPnlUsd")),
            "exchangeFundingFeeUsd": num(seed.get("exchangeFundingFeeUsd")),
            "exchangeCommissionUsd": num(seed.get("exchangeCommissionUsd")),
            "exchangeOtherIncomeUsd": num(seed.get("exchangeOtherIncomeUsd")),
            "exchangeAccountingUpdatedAt": seed.get("exchangeAccountingUpdatedAt"),
            "exchangeAccountingNote": seed.get("exchangeAccountingNote"),
            "openPositions": [normalize_position(item) for item in seed.get("openPositions", [])],
            "openOrders": [normalize_order(item) for item in seed.get("openOrders", [])],
            "exchangeClosedTrades": [normalize_exchange_closed_trade(item) for item in seed.get("exchangeClosedTrades", [])],
            "closedTrades": [normalize_trade(item) for item in seed.get("closedTrades", [])],
            "decisions": [normalize_decision(item) for item in seed.get("decisions", [])],
        }
        state[key] = normalized
    adaptive = saved.get("adaptive") if isinstance(saved.get("adaptive"), dict) else {}
    state["adaptive"] = {
        "updatedAt": adaptive.get("updatedAt"),
        "notes": adaptive.get("notes") if isinstance(adaptive.get("notes"), list) else state["adaptive"]["notes"],
    }
    state["updatedAt"] = saved.get("updatedAt") or state["updatedAt"]
    return state


def write_trading_state(state: dict[str, Any]) -> dict[str, Any]:
    payload = deepcopy(state)
    for key in ("paper", "live"):
        payload[key]["openPositions"] = [normalize_position(item) for item in payload[key].get("openPositions", [])]
        payload[key]["openOrders"] = [normalize_order(item) for item in payload[key].get("openOrders", [])][-80:]
        payload[key]["exchangeClosedTrades"] = [normalize_exchange_closed_trade(item) for item in payload[key].get("exchangeClosedTrades", [])][-400:]
        payload[key]["closedTrades"] = [normalize_trade(item) for item in payload[key].get("closedTrades", [])][-400:]
        payload[key]["decisions"] = [normalize_decision(item) for item in payload[key].get("decisions", [])][-40:]
    payload["updatedAt"] = now_iso()
    write_json(STATE_PATH, payload)
    return payload


def archive_decision(decision: dict[str, Any]) -> None:
    run_date = current_run_date()
    path = DECISIONS_DIR / run_date / f"{decision['id']}.json"
    write_json(path, decision)


def position_pnl(position: dict[str, Any], mark_price: float | None) -> float | None:
    entry_price = num(position.get("entryPrice"))
    quantity = num(position.get("quantity"))
    mark = num(mark_price)
    if entry_price is None or quantity is None or mark is None:
        return None
    multiplier = -1 if position.get("side") == "short" else 1
    return (mark - entry_price) * quantity * multiplier


def enrich_position(position: dict[str, Any]) -> dict[str, Any]:
    mark_price = num(position.get("lastMarkPrice")) or num(position.get("entryPrice")) or 0
    unrealized_pnl = position_pnl(position, mark_price) or 0
    notional_usd = num(position.get("notionalUsd")) or (mark_price * (num(position.get("quantity")) or 0))
    pnl_pct = (unrealized_pnl / notional_usd) * 100 if notional_usd else None
    enriched = dict(position)
    enriched["markPrice"] = mark_price
    enriched["unrealizedPnl"] = unrealized_pnl
    enriched["pnlPct"] = pnl_pct
    return enriched


def summarize_account(book: dict[str, Any], settings: dict[str, Any]) -> dict[str, Any]:
    open_positions = [enrich_position(item) for item in book.get("openPositions", [])]
    open_orders = [normalize_order(item) for item in book.get("openOrders", [])]
    exchange_closed_trades = [normalize_exchange_closed_trade(item) for item in book.get("exchangeClosedTrades", [])]
    local_estimated_realized_pnl = sum(num(item.get("realizedPnl")) or 0 for item in book.get("closedTrades", []))
    unrealized_pnl = sum(num(item.get("unrealizedPnl")) or 0 for item in open_positions)
    initial_capital = num(book.get("initialCapitalUsd")) or settings["initialCapitalUsd"]
    account_source = book.get("accountSource") or "paper"
    equity_usd = (num(book.get("exchangeEquityUsd")) if account_source == "exchange" else None)
    if equity_usd is None:
        equity_usd = initial_capital + local_estimated_realized_pnl + unrealized_pnl
    has_local_history = bool(book.get("decisions") or book.get("closedTrades"))
    if account_source == "exchange" and not has_local_history and equity_usd is not None:
        initial_capital = equity_usd
    exchange_wallet_balance = num(book.get("exchangeWalletBalanceUsd"))
    exchange_unrealized_pnl = num(book.get("exchangeUnrealizedPnlUsd"))
    exchange_net_cashflow_usd = num(book.get("exchangeNetCashflowUsd"))
    exchange_realized_pnl_usd = None
    if account_source == "exchange":
        exchange_realized_pnl_usd = sum(num(item.get("realizedPnl")) or 0 for item in exchange_closed_trades)
    realized_pnl_usd = exchange_realized_pnl_usd if account_source == "exchange" and exchange_realized_pnl_usd is not None else local_estimated_realized_pnl
    gross_exposure = sum(abs((num(item.get("markPrice")) or 0) * (num(item.get("quantity")) or 0)) for item in open_positions)
    max_gross_exposure = equity_usd * (settings["maxGrossExposurePct"] / 100)
    available_exposure = max(0.0, max_gross_exposure - gross_exposure)
    if account_source == "exchange" and not has_local_history and equity_usd is not None:
        high_watermark = equity_usd
    else:
        high_watermark = max(num(book.get("highWatermarkEquity")) or initial_capital, equity_usd)
    drawdown_pct = ((high_watermark - equity_usd) / high_watermark) * 100 if high_watermark else 0
    return {
        "baselineCapitalUsd": initial_capital,
        "initialCapitalUsd": initial_capital,
        "equityUsd": equity_usd,
        "realizedPnlUsd": realized_pnl_usd,
        "localEstimatedRealizedPnlUsd": local_estimated_realized_pnl,
        "exchangeRealizedPnlUsd": exchange_realized_pnl_usd,
        "exchangeNetCashflowUsd": exchange_net_cashflow_usd,
        "exchangeIncomeRealizedPnlUsd": num(book.get("exchangeIncomeRealizedPnlUsd")),
        "exchangeFundingFeeUsd": num(book.get("exchangeFundingFeeUsd")),
        "exchangeCommissionUsd": num(book.get("exchangeCommissionUsd")),
        "exchangeOtherIncomeUsd": num(book.get("exchangeOtherIncomeUsd")),
        "exchangeAccountingUpdatedAt": book.get("exchangeAccountingUpdatedAt"),
        "exchangeAccountingNote": book.get("exchangeAccountingNote"),
        "unrealizedPnlUsd": unrealized_pnl,
        "highWatermarkEquity": high_watermark,
        "drawdownPct": drawdown_pct,
        "grossExposureUsd": gross_exposure,
        "maxGrossExposureUsd": max_gross_exposure,
        "availableExposureUsd": available_exposure,
        "exchangeWalletBalanceUsd": exchange_wallet_balance,
        "exchangeAvailableBalanceUsd": num(book.get("exchangeAvailableBalanceUsd")),
        "exchangeUnrealizedPnlUsd": exchange_unrealized_pnl,
        "exchangeClosedTradesCount": len(exchange_closed_trades),
        "openPositions": open_positions,
        "openOrdersCount": len(open_orders),
        "closedTradesCount": len(book.get("closedTrades", [])),
        "decisionsCount": len(book.get("decisions", [])),
        "circuitBreakerTripped": book.get("circuitBreakerTripped") is True,
        "circuitBreakerReason": book.get("circuitBreakerReason"),
        "accountSource": account_source,
    }


def action_label(action_type: str, symbol: str | None = None, side: str | None = None) -> str:
    symbol = symbol or "MARKET"
    if action_type == "open":
        return f"{(side or '').upper()} {symbol}".strip()
    if action_type == "close":
        return f"Close {symbol}"
    if action_type == "reduce":
        return f"Reduce {symbol}"
    if action_type == "update":
        return f"Update risk {symbol}"
    if action_type == "circuit_breaker":
        return "Circuit breaker"
    return action_type


def serialize_candidate_for_history(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": candidate.get("symbol"),
        "baseAsset": candidate.get("baseAsset"),
        "price": candidate.get("price"),
    }


def serialize_candidate_for_prompt(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        **serialize_candidate_for_history(candidate),
        "priceChangePct": candidate.get("priceChangePct"),
        "quoteVolume": candidate.get("quoteVolume"),
        "fundingPct": candidate.get("fundingPct"),
        "klineFeeds": candidate.get("klineFeeds"),
        "klinesByInterval": candidate.get("klinesByInterval"),
    }


def close_position(book: dict[str, Any], position: dict[str, Any], exit_price: float, decision_id: str, reason: str) -> tuple[dict[str, Any], dict[str, Any]]:
    trade = normalize_trade(
        {
            "id": f"{position['id']}-close-{int(__import__('time').time() * 1000)}",
            "positionId": position["id"],
            "symbol": position["symbol"],
            "baseAsset": position["baseAsset"],
            "side": position["side"],
            "quantity": position["quantity"],
            "entryPrice": position["entryPrice"],
            "exitPrice": exit_price,
            "notionalUsd": position.get("notionalUsd"),
            "realizedPnl": position_pnl(position, exit_price) or 0,
            "openedAt": position.get("openedAt"),
            "closedAt": now_iso(),
            "exitReason": reason,
            "decisionId": decision_id,
        }
    )
    book["openPositions"] = [item for item in book.get("openPositions", []) if item["id"] != position["id"]]
    book.setdefault("closedTrades", []).append(trade)
    action = {
        "type": "close",
        "symbol": position["symbol"],
        "side": position["side"],
        "realizedPnlUsd": trade["realizedPnl"],
        "reason": reason,
        "label": action_label("close", position["symbol"]),
    }
    return book, action


def reduce_position(book: dict[str, Any], position: dict[str, Any], exit_price: float, reduce_fraction: float, decision_id: str, reason: str) -> tuple[dict[str, Any], dict[str, Any] | None]:
    total_qty = num(position.get("quantity")) or 0
    fraction = clamp(reduce_fraction, 0.05, 0.95)
    close_qty = total_qty * fraction
    remaining_qty = total_qty - close_qty
    if remaining_qty <= 1e-9:
        return close_position(book, position, exit_price, decision_id, reason)
    partial_position = dict(position)
    partial_position["quantity"] = close_qty
    trade = normalize_trade(
        {
            "id": f"{position['id']}-reduce-{int(__import__('time').time() * 1000)}",
            "positionId": position["id"],
            "symbol": position["symbol"],
            "baseAsset": position["baseAsset"],
            "side": position["side"],
            "quantity": close_qty,
            "entryPrice": position["entryPrice"],
            "exitPrice": exit_price,
            "notionalUsd": (num(position.get("notionalUsd")) or 0) * fraction,
            "realizedPnl": position_pnl(partial_position, exit_price) or 0,
            "openedAt": position.get("openedAt"),
            "closedAt": now_iso(),
            "exitReason": reason,
            "decisionId": decision_id,
        }
    )
    for index, current in enumerate(book.get("openPositions", [])):
        if current["id"] != position["id"]:
            continue
        updated = dict(current)
        updated["quantity"] = remaining_qty
        updated["notionalUsd"] = (num(current.get("notionalUsd")) or 0) * (remaining_qty / total_qty)
        updated["updatedAt"] = now_iso()
        book["openPositions"][index] = normalize_position(updated)
        break
    book.setdefault("closedTrades", []).append(trade)
    action = {
        "type": "reduce",
        "symbol": position["symbol"],
        "side": position["side"],
        "reduceFraction": fraction,
        "realizedPnlUsd": trade["realizedPnl"],
        "reason": reason,
        "label": action_label("reduce", position["symbol"]),
    }
    return book, action


def build_prompt(
    *,
    settings: dict[str, Any],
    prompt_settings: dict[str, Any],
    provider: dict[str, Any],
    market_backdrop: dict[str, Any],
    account_summary: dict[str, Any],
    open_positions: list[dict[str, Any]],
    open_orders: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
) -> str:
    response_contract = {
        "summary": "short plain-language summary",
        "position_actions": [
            {
                "symbol": "POSITION_SYMBOL",
                "decision": "hold | close | reduce | update",
                "reason": "short reason",
                "reduceFraction": 0.25,
                "stopLoss": 0.0,
                "takeProfit": 0.0,
            }
        ],
        "entry_actions": [
            {
                "symbol": "CANDIDATE_SYMBOL",
                "action": "open",
                "side": "long | short",
                "confidence": 72,
                "reason": "short reason",
                "stopLoss": 0.0,
                "takeProfit": 0.0,
            }
        ],
        "watchlist": [
            {
                "symbol": "WATCHLIST_SYMBOL",
                "reason": "why it is worth watching",
            }
        ],
    }
    context = {
        "timestamp": now_iso(),
        "mode": settings["mode"],
        "provider": {
            "preset": provider["preset"],
            "apiStyle": provider["apiStyle"],
            "model": provider["model"],
        },
        "hardRiskLimits": {
            "maxNewPositionsPerCycle": settings["maxNewPositionsPerCycle"],
            "maxOpenPositions": settings["maxOpenPositions"],
            "maxPositionNotionalUsd": settings["maxPositionNotionalUsd"],
            "maxGrossExposurePct": settings["maxGrossExposurePct"],
            "maxAccountDrawdownPct": settings["maxAccountDrawdownPct"],
            "riskPerTradePct": settings["riskPerTradePct"],
            "minConfidence": settings["minConfidence"],
            "allowShorts": settings["allowShorts"],
        },
        "account": account_summary,
        "openPositions": open_positions,
        "openOrders": open_orders,
        "candidates": [serialize_candidate_for_prompt(item) for item in candidates],
    }
    if market_backdrop:
        context["marketBackdrop"] = market_backdrop
    rules = [
        "Manage every existing position first. Existing positions should appear in position_actions.",
        "Respect existing exchange open orders and avoid duplicating protection logic that is already active.",
        f"You may propose at most {settings['maxNewPositionsPerCycle']} new entries.",
        "Do not propose entries for symbols that are not in candidates.",
        "Summary and watchlist should only mention symbols that already exist in openPositions or candidates.",
        "Respect the hard risk limits from the system context even if the user logic asks for more.",
        "If there is no clear edge, return empty entry_actions.",
        "Return strict JSON only. No markdown, no prose outside the JSON object.",
    ]
    return "\n".join(
        [
            "# Editable Trading Logic JSON",
            json.dumps(prompt_settings["decision_logic"], ensure_ascii=False, indent=2),
            "",
            "# System Rules",
            *[f"- {rule}" for rule in rules],
            "",
            "# Required JSON Contract",
            json.dumps(response_contract, ensure_ascii=False, indent=2),
            "",
            "# Current Trading Context",
            json.dumps(context, ensure_ascii=False, indent=2),
        ]
    )


def default_model_decision(open_positions: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "summary": "Fallback decision because model output was unavailable.",
        "position_actions": [
            {
                "symbol": position["symbol"],
                "decision": "hold",
                "reason": "Fallback hold because model output was unavailable.",
            }
            for position in open_positions
        ],
        "entry_actions": [],
        "watchlist": [],
    }


def normalize_model_decision(
    parsed: dict[str, Any],
    *,
    open_positions: list[dict[str, Any]],
    candidates_by_symbol: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(parsed, dict):
        raise ValueError("Model response must be a JSON object.")
    position_actions_raw = parsed.get("position_actions") if isinstance(parsed.get("position_actions"), list) else []
    entry_actions_raw = parsed.get("entry_actions") if isinstance(parsed.get("entry_actions"), list) else []
    watchlist_raw = parsed.get("watchlist") if isinstance(parsed.get("watchlist"), list) else []
    positions_by_symbol = {item["symbol"]: item for item in open_positions}
    normalized_positions: list[dict[str, Any]] = []
    seen_symbols: set[str] = set()
    for item in position_actions_raw:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "").upper()
        if symbol not in positions_by_symbol or symbol in seen_symbols:
            continue
        decision = str(item.get("decision") or "hold").strip().lower()
        if decision not in {"hold", "close", "reduce", "update"}:
            decision = "hold"
        normalized_positions.append(
            {
                "symbol": symbol,
                "decision": decision,
                "reason": str(item.get("reason") or ""),
                "reduceFraction": clamp(item.get("reduceFraction"), 0.05, 0.95),
                "stopLoss": num(item.get("stopLoss")),
                "takeProfit": num(item.get("takeProfit")),
            }
        )
        seen_symbols.add(symbol)
    for symbol in positions_by_symbol:
        if symbol not in seen_symbols:
            normalized_positions.append(
                {
                    "symbol": symbol,
                    "decision": "hold",
                    "reason": "No explicit model instruction; defaulting to hold.",
                    "reduceFraction": 0.25,
                    "stopLoss": None,
                    "takeProfit": None,
                }
            )
    normalized_entries: list[dict[str, Any]] = []
    for item in entry_actions_raw:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "").upper()
        candidate = candidates_by_symbol.get(symbol)
        if not candidate:
            continue
        action = str(item.get("action") or "open").strip().lower()
        if action != "open":
            continue
        side = str(item.get("side") or candidate.get("defaultSide") or "").strip().lower()
        if side not in {"long", "short"}:
            continue
        normalized_entries.append(
            {
                "symbol": symbol,
                "action": "open",
                "side": side,
                "confidence": clamp(item.get("confidence") or candidate.get("confidenceScore"), 1, 100),
                "reason": str(item.get("reason") or candidate.get("topStrategy") or ""),
                "stopLoss": num(item.get("stopLoss")) or num(candidate.get("defaultStopLoss")),
                "takeProfit": num(item.get("takeProfit")) or num(candidate.get("defaultTakeProfit")),
            }
        )
    normalized_watchlist = []
    for item in watchlist_raw:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "").upper()
        if not symbol:
            continue
        normalized_watchlist.append(
            {
                "symbol": symbol,
                "reason": str(item.get("reason") or ""),
            }
        )
    return {
        "summary": str(parsed.get("summary") or ""),
        "position_actions": normalized_positions,
        "entry_actions": normalized_entries,
        "watchlist": normalized_watchlist,
    }


def mark_to_market(book: dict[str, Any], live_by_symbol: dict[str, dict[str, Any]]) -> None:
    for position in book.get("openPositions", []):
        live = live_by_symbol.get(position["symbol"])
        if not live:
            continue
        mark_price = num(live["premium"].get("markPrice")) or num(live["ticker24h"].get("lastPrice")) or num(position.get("entryPrice")) or 0
        position["lastMarkPrice"] = mark_price
        position["lastMarkTime"] = now_iso()
        position["updatedAt"] = now_iso()


def _risk_valid_for_side(side: str, mark_price: float, stop_loss: float | None, take_profit: float | None) -> bool:
    if side == "long":
        if stop_loss is not None and stop_loss >= mark_price:
            return False
        if take_profit is not None and take_profit <= mark_price:
            return False
    else:
        if stop_loss is not None and stop_loss <= mark_price:
            return False
        if take_profit is not None and take_profit >= mark_price:
            return False
    return True


def apply_protection_hits(book: dict[str, Any], decision_id: str) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for position in list(book.get("openPositions", [])):
        mark_price = num(position.get("lastMarkPrice"))
        if mark_price is None:
            continue
        stop_loss = num(position.get("stopLoss"))
        take_profit = num(position.get("takeProfit"))
        if position["side"] == "long":
            if stop_loss is not None and mark_price <= stop_loss:
                book, action = close_position(book, position, mark_price, decision_id, "stop_loss_hit")
                actions.append(action)
                continue
            if take_profit is not None and mark_price >= take_profit:
                book, action = close_position(book, position, mark_price, decision_id, "take_profit_hit")
                actions.append(action)
                continue
        else:
            if stop_loss is not None and mark_price >= stop_loss:
                book, action = close_position(book, position, mark_price, decision_id, "stop_loss_hit")
                actions.append(action)
                continue
            if take_profit is not None and mark_price <= take_profit:
                book, action = close_position(book, position, mark_price, decision_id, "take_profit_hit")
                actions.append(action)
                continue
    return actions


def position_notional_from_risk(
    account_summary: dict[str, Any],
    *,
    entry_price: float,
    stop_loss: float,
    settings: dict[str, Any],
) -> float:
    stop_pct = abs(((entry_price - stop_loss) / entry_price))
    if stop_pct <= 0:
        return 0
    risk_budget = account_summary["equityUsd"] * (settings["riskPerTradePct"] / 100)
    risk_sized_notional = risk_budget / stop_pct
    return min(
        settings["maxPositionNotionalUsd"],
        account_summary["availableExposureUsd"],
        risk_sized_notional,
    )


def cap_live_notional_by_margin(
    requested_notional_usd: float,
    *,
    account_summary: dict[str, Any],
    live_config: dict[str, Any],
) -> float:
    available_balance = num(account_summary.get("exchangeAvailableBalanceUsd"))
    leverage = int(clamp(live_config.get("defaultLeverage"), 1, 125))
    if available_balance is None or available_balance <= 0:
        return requested_notional_usd
    max_margin_notional = max(0.0, available_balance * leverage * 0.92)
    return min(requested_notional_usd, max_margin_notional)


def open_paper_position(
    book: dict[str, Any],
    *,
    candidate: dict[str, Any],
    side: str,
    stop_loss: float,
    take_profit: float | None,
    confidence: float,
    notional_usd: float,
    reason: str,
    decision_id: str,
) -> dict[str, Any]:
    entry_price = num(candidate.get("price")) or 0
    quantity = notional_usd / entry_price if entry_price else 0
    position = normalize_position(
        {
            "id": f"{candidate['symbol']}-{int(__import__('time').time() * 1000)}",
            "symbol": candidate["symbol"],
            "baseAsset": candidate["baseAsset"],
            "side": side,
            "quantity": quantity,
            "initialQuantity": quantity,
            "entryPrice": entry_price,
            "notionalUsd": notional_usd,
            "initialNotionalUsd": notional_usd,
            "stopLoss": stop_loss,
            "takeProfit": take_profit,
            "lastMarkPrice": entry_price,
            "lastMarkTime": now_iso(),
            "leverage": 1,
            "openedAt": now_iso(),
            "updatedAt": now_iso(),
            "source": "paper",
            "entryReason": reason,
            "decisionId": decision_id,
            "confidenceScore": confidence,
        }
    )
    book.setdefault("openPositions", []).append(position)
    return position


def sync_live_book(
    book: dict[str, Any],
    settings: dict[str, Any],
) -> tuple[dict[str, Any], list[str], dict[str, Any], dict[str, Any] | None]:
    live_config = read_live_trading_config()
    status = live_execution_status(live_config, settings)
    warnings: list[str] = []
    if not status["canSync"]:
        warnings.extend(status["issues"])
        return book, warnings, status, live_config
    session_started_at = book.get("sessionStartedAt") or derive_session_started_at(book)
    if session_started_at:
        book["sessionStartedAt"] = session_started_at
    snapshot = fetch_account_snapshot(live_config, session_started_at=session_started_at)
    accounting_note = str(snapshot.get("accountingNote") or "").strip()
    if accounting_note:
        warnings.append(accounting_note)
    prior_positions = {item["symbol"]: item for item in book.get("openPositions", [])}
    merged_positions = []
    for position in snapshot["openPositions"]:
        prior = prior_positions.get(position["symbol"], {})
        merged = normalize_position(
            {
                **position,
                "stopLoss": prior.get("stopLoss"),
                "takeProfit": prior.get("takeProfit"),
                "openedAt": prior.get("openedAt") or now_iso(),
                "entryReason": prior.get("entryReason") or "synced_from_exchange",
                "decisionId": prior.get("decisionId"),
            }
        )
        merged_positions.append(merged)
    merged_orders = [normalize_order(item) for item in snapshot.get("openOrders", [])]
    exchange_closed_trades = [normalize_exchange_closed_trade(item) for item in snapshot.get("exchangeClosedTrades", [])]
    should_seed_equity_baseline = not book.get("decisions") and not book.get("closedTrades")
    snapshot_equity = num(snapshot.get("equityUsd"))
    book.update(
        {
            "accountSource": "exchange",
            "exchangeWalletBalanceUsd": snapshot["walletBalanceUsd"],
            "exchangeEquityUsd": snapshot["equityUsd"],
            "exchangeAvailableBalanceUsd": snapshot["availableBalanceUsd"],
            "exchangeUnrealizedPnlUsd": snapshot["unrealizedPnlUsd"],
            "exchangeNetCashflowUsd": num(snapshot.get("netCashflowUsd")),
            "exchangeIncomeRealizedPnlUsd": num(snapshot.get("incomeRealizedPnlUsd")),
            "exchangeFundingFeeUsd": num(snapshot.get("fundingFeeUsd")),
            "exchangeCommissionUsd": num(snapshot.get("commissionUsd")),
            "exchangeOtherIncomeUsd": num(snapshot.get("otherIncomeUsd")),
            "exchangeAccountingUpdatedAt": snapshot.get("accountingUpdatedAt"),
            "exchangeAccountingNote": snapshot.get("accountingNote"),
            "openPositions": merged_positions,
            "openOrders": merged_orders,
            "exchangeClosedTrades": exchange_closed_trades,
        }
    )
    if snapshot_equity is not None:
        current_initial = num(book.get("initialCapitalUsd"))
        current_high_watermark = num(book.get("highWatermarkEquity"))
        if current_initial is None or current_initial <= 0:
            book["initialCapitalUsd"] = snapshot_equity
        if should_seed_equity_baseline or current_high_watermark is None or current_high_watermark <= 0:
            book["highWatermarkEquity"] = snapshot_equity
    return book, warnings, status, live_config


def refresh_account_state_after_settings_save(*, reset_live_session: bool = False) -> dict[str, Any]:
    settings = read_trading_settings()
    state = read_trading_state(settings)

    paper_has_history = bool(state["paper"].get("decisions") or state["paper"].get("closedTrades"))
    if not paper_has_history:
        state["paper"]["initialCapitalUsd"] = settings["initialCapitalUsd"]
        if not state["paper"].get("openPositions"):
            state["paper"]["highWatermarkEquity"] = settings["initialCapitalUsd"]

    live_has_history = bool(state["live"].get("decisions") or state["live"].get("closedTrades"))
    if not live_has_history:
        state["live"]["initialCapitalUsd"] = settings["initialCapitalUsd"]
        if not state["live"].get("openPositions"):
            state["live"]["highWatermarkEquity"] = settings["initialCapitalUsd"]
    if reset_live_session:
        state["live"]["sessionStartedAt"] = now_iso()
        state["live"]["exchangeClosedTrades"] = []
    elif settings.get("liveTrading", {}).get("enabled") and not state["live"].get("sessionStartedAt"):
        state["live"]["sessionStartedAt"] = now_iso()

    live_sync_warnings: list[str] = []
    live_status_payload: dict[str, Any] | None = None
    live_config: dict[str, Any] | None = None
    try:
        state["live"], live_sync_warnings, live_status_payload, live_config = sync_live_book(state["live"], settings)
    except Exception as error:
        live_sync_warnings = [f"Live account sync after settings save failed: {error}"]

    write_trading_state(state)
    return {
        "state": state,
        "liveSyncWarnings": live_sync_warnings,
        "liveStatus": live_status_payload,
        "liveConfig": live_config,
    }


def apply_live_position_action(
    book: dict[str, Any],
    position: dict[str, Any],
    action: dict[str, Any],
    decision_id: str,
    status: dict[str, Any],
    live_config: dict[str, Any],
    settings: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[str]]:
    actions: list[dict[str, Any]] = []
    warnings: list[str] = []
    decision = action["decision"]
    mark_price = num(position.get("lastMarkPrice")) or num(position.get("entryPrice")) or 0
    if decision in {"close", "reduce"} and not status["canExecute"]:
        warnings.append(f"Live execution skipped for {position['symbol']}: real execution is not enabled.")
        return book, actions, warnings
    if decision == "close":
        cancel_all_open_orders(live_config, position["symbol"])
        order_side = "SELL" if position["side"] == "long" else "BUY"
        place_market_order(live_config, symbol=position["symbol"], side=order_side, quantity=position["quantity"], reduce_only=True)
        book, recorded = close_position(book, position, mark_price, decision_id, action["reason"] or "model_close")
        recorded["exchange"] = True
        actions.append(recorded)
        return book, actions, warnings
    if decision == "reduce":
        close_qty = (num(position.get("quantity")) or 0) * action["reduceFraction"]
        normalized_qty = normalize_quantity(live_config, position["symbol"], quantity=close_qty, reference_price=mark_price)
        order_side = "SELL" if position["side"] == "long" else "BUY"
        place_market_order(live_config, symbol=position["symbol"], side=order_side, quantity=normalized_qty, reduce_only=True)
        book, recorded = reduce_position(book, position, mark_price, action["reduceFraction"], decision_id, action["reason"] or "model_reduce")
        if recorded:
            recorded["exchange"] = True
            actions.append(recorded)
        return book, actions, warnings
    if decision in {"hold", "update"}:
        stop_loss = action.get("stopLoss")
        take_profit = action.get("takeProfit")
        if stop_loss is None and take_profit is None:
            return book, actions, warnings
        if not _risk_valid_for_side(position["side"], mark_price, stop_loss, take_profit):
            warnings.append(f"Ignored invalid live protection update for {position['symbol']}.")
            return book, actions, warnings
        for current in book.get("openPositions", []):
            if current["id"] != position["id"]:
                continue
            current["stopLoss"] = stop_loss
            current["takeProfit"] = take_profit
            current["updatedAt"] = now_iso()
            break
        if status["canExecute"] and settings["liveExecution"]["useExchangeProtectionOrders"]:
            try:
                cancel_all_open_orders(live_config, position["symbol"])
                place_protection_orders(
                    live_config,
                    symbol=position["symbol"],
                    position_side=position["side"],
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                )
            except Exception as error:
                warnings.append(f"Exchange protection order update failed for {position['symbol']}: {error}")
        actions.append(
            {
                "type": "update",
                "symbol": position["symbol"],
                "side": position["side"],
                "stopLoss": stop_loss,
                "takeProfit": take_profit,
                "reason": action["reason"] or "model_update",
                "label": action_label("update", position["symbol"]),
            }
        )
    return book, actions, warnings


def apply_paper_position_action(
    book: dict[str, Any],
    position: dict[str, Any],
    action: dict[str, Any],
    decision_id: str,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[str]]:
    actions: list[dict[str, Any]] = []
    warnings: list[str] = []
    mark_price = num(position.get("lastMarkPrice")) or num(position.get("entryPrice")) or 0
    decision = action["decision"]
    if decision == "close":
        book, recorded = close_position(book, position, mark_price, decision_id, action["reason"] or "model_close")
        actions.append(recorded)
        return book, actions, warnings
    if decision == "reduce":
        book, recorded = reduce_position(book, position, mark_price, action["reduceFraction"], decision_id, action["reason"] or "model_reduce")
        if recorded:
            actions.append(recorded)
        return book, actions, warnings
    stop_loss = action.get("stopLoss")
    take_profit = action.get("takeProfit")
    if decision in {"hold", "update"} and (stop_loss is not None or take_profit is not None):
        if not _risk_valid_for_side(position["side"], mark_price, stop_loss, take_profit):
            warnings.append(f"Ignored invalid risk update for {position['symbol']}.")
            return book, actions, warnings
        for current in book.get("openPositions", []):
            if current["id"] != position["id"]:
                continue
            current["stopLoss"] = stop_loss
            current["takeProfit"] = take_profit
            current["updatedAt"] = now_iso()
            break
        actions.append(
            {
                "type": "update",
                "symbol": position["symbol"],
                "side": position["side"],
                "stopLoss": stop_loss,
                "takeProfit": take_profit,
                "reason": action["reason"] or "model_update",
                "label": action_label("update", position["symbol"]),
            }
        )
    return book, actions, warnings


def apply_account_circuit_breaker(
    book: dict[str, Any],
    settings: dict[str, Any],
    decision_id: str,
    *,
    live_mode: bool,
    live_status_payload: dict[str, Any] | None = None,
    live_config: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[str]]:
    account = summarize_account(book, settings)
    if account["drawdownPct"] < settings["maxAccountDrawdownPct"]:
        book["circuitBreakerTripped"] = False
        book["circuitBreakerReason"] = None
        return book, [], []
    book["circuitBreakerTripped"] = True
    book["circuitBreakerReason"] = f"Drawdown {account['drawdownPct']:.2f}% breached max {settings['maxAccountDrawdownPct']:.2f}%."
    actions: list[dict[str, Any]] = []
    warnings: list[str] = []
    for position in list(book.get("openPositions", [])):
        if live_mode:
            if not live_status_payload or not live_status_payload.get("canExecute"):
                warnings.append(f"Circuit breaker could not close live {position['symbol']} because real execution is not enabled.")
                continue
            cancel_all_open_orders(live_config, position["symbol"])
            order_side = "SELL" if position["side"] == "long" else "BUY"
            place_market_order(live_config, symbol=position["symbol"], side=order_side, quantity=position["quantity"], reduce_only=True)
        book, recorded = close_position(
            book,
            position,
            num(position.get("lastMarkPrice")) or num(position.get("entryPrice")) or 0,
            decision_id,
            "circuit_breaker",
        )
        recorded["type"] = "circuit_breaker"
        recorded["label"] = action_label("circuit_breaker")
        actions.append(recorded)
    return book, actions, warnings


def _fetch_live_contexts(symbols: list[str], prompt_kline_feeds: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], list[str]]:
    return _fetch_live_contexts_for_exchange(symbols, prompt_kline_feeds)


def _fetch_live_contexts_for_exchange(
    symbols: list[str],
    prompt_kline_feeds: dict[str, Any],
    exchange_id: str | None = None,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    live_by_symbol: dict[str, dict[str, Any]] = {}
    warnings: list[str] = []
    with ThreadPoolExecutor(max_workers=min(4, max(1, len(symbols)))) as executor:
        futures = {
            executor.submit(fetch_candidate_live_context, symbol, prompt_kline_feeds, exchange_id): symbol
            for symbol in symbols
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                live_by_symbol[symbol] = future.result()
            except Exception as error:
                warnings.append(f"{symbol}: {error}")
    return live_by_symbol, warnings


def run_trading_cycle(reason: str = "manual", mode_override: str | None = None) -> dict[str, Any]:
    settings = read_trading_settings()
    settings["mode"] = clean_mode(mode_override or settings["mode"])
    universe = read_fixed_universe()
    account_key = account_key_for_mode(settings["mode"])
    cycle_exchange_id = str(settings.get("activeExchange") or "binance").strip().lower() or "binance"
    live_config = None
    if account_key == "live":
        live_config = read_live_trading_config()
        cycle_exchange_id = str(live_config.get("exchange") or cycle_exchange_id).strip().lower() or cycle_exchange_id
    scan = read_latest_scan(cycle_exchange_id)
    if universe.get("dynamicSource", {}).get("enabled") or not scan["opportunities"] or str(scan.get("exchange") or "").strip().lower() != cycle_exchange_id:
        scan = refresh_candidate_pool(cycle_exchange_id)
    state = read_trading_state(settings)
    book = state[account_key]
    if account_key != "live":
        book["initialCapitalUsd"] = settings["initialCapitalUsd"]
    book.setdefault("sessionStartedAt", book.get("sessionStartedAt") or now_iso())
    decision_id = f"trade-cycle-{int(__import__('time').time() * 1000)}"
    warnings: list[str] = []
    live_status_payload = None
    if account_key == "live":
        book, live_warnings, live_status_payload, live_config = sync_live_book(book, settings)
        warnings.extend(live_warnings)
        state["live"] = book
    prompt_settings = read_prompt_settings()
    prompt_kline_feeds = prompt_settings.get("klineFeeds") if isinstance(prompt_settings.get("klineFeeds"), dict) else {}
    raw_candidates = candidate_universe_from_scan(scan)
    symbols = []
    for item in raw_candidates:
        symbol = str(item.get("symbol") or "").upper()
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    for position in book.get("openPositions", []):
        if position["symbol"] not in symbols:
            symbols.append(position["symbol"])
    live_by_symbol, live_context_warnings = _fetch_live_contexts_for_exchange(symbols, prompt_kline_feeds, cycle_exchange_id)
    warnings.extend(live_context_warnings)
    mark_to_market(book, live_by_symbol)
    protection_actions = apply_protection_hits(book, decision_id)
    gateway = get_active_exchange_gateway(cycle_exchange_id)
    market_backdrop = fetch_market_backdrop(prompt_kline_feeds, cycle_exchange_id) if gateway.default_backdrop_symbol in symbols else {}
    candidate_snapshots = []
    for opportunity in raw_candidates:
        symbol = str(opportunity.get("symbol") or "").upper()
        live = live_by_symbol.get(symbol)
        if not live:
            continue
        candidate_snapshots.append(build_candidate_snapshot(opportunity, live, settings, cycle_exchange_id))
    candidates_by_symbol = {item["symbol"]: item for item in candidate_snapshots}
    account_before = summarize_account(book, settings)
    provider = read_llm_provider()
    prompt = build_prompt(
        settings=settings,
        prompt_settings=prompt_settings,
        provider=provider,
        market_backdrop=market_backdrop,
        account_summary=account_before,
        open_positions=account_before["openPositions"],
        open_orders=[normalize_order(item) for item in book.get("openOrders", [])],
        candidates=candidate_snapshots,
    )
    model_result: dict[str, Any] | None = None
    try:
        model_result = generate_trading_decision(prompt, provider)
        parsed_model = normalize_model_decision(
            model_result["parsed"],
            open_positions=account_before["openPositions"],
            candidates_by_symbol=candidates_by_symbol,
        )
    except Exception as error:
        warnings.append(f"Model decision failed: {error}")
        parsed_model = default_model_decision(account_before["openPositions"])
    management_actions = list(protection_actions)
    for instruction in parsed_model["position_actions"]:
        position = next((item for item in list(book.get("openPositions", [])) if item["symbol"] == instruction["symbol"]), None)
        if not position:
            continue
        if account_key == "live":
            book, applied_actions, applied_warnings = apply_live_position_action(
                book,
                position,
                instruction,
                decision_id,
                live_status_payload or {"canExecute": False},
                live_config or read_live_trading_config(),
                settings,
            )
        else:
            book, applied_actions, applied_warnings = apply_paper_position_action(
                book,
                position,
                instruction,
                decision_id,
            )
        management_actions.extend(applied_actions)
        warnings.extend(applied_warnings)
    book, breaker_actions, breaker_warnings = apply_account_circuit_breaker(
        book,
        settings,
        decision_id,
        live_mode=account_key == "live",
        live_status_payload=live_status_payload,
        live_config=live_config,
    )
    warnings.extend(breaker_warnings)
    entry_actions: list[dict[str, Any]] = []
    if not book.get("circuitBreakerTripped"):
        account_after_management = summarize_account(book, settings)
        open_symbols = {item["symbol"] for item in book.get("openPositions", [])}
        opened = 0
        for entry in parsed_model["entry_actions"]:
            if opened >= settings["maxNewPositionsPerCycle"]:
                break
            if entry["symbol"] in open_symbols:
                continue
            if entry["confidence"] < settings["minConfidence"]:
                continue
            candidate = candidates_by_symbol.get(entry["symbol"])
            if not candidate:
                continue
            side = entry["side"]
            if side == "short" and not settings["allowShorts"]:
                continue
            entry_price = num(candidate.get("price")) or 0
            stop_loss = num(entry.get("stopLoss"))
            take_profit = num(entry.get("takeProfit"))
            if entry_price <= 0 or stop_loss is None:
                continue
            if not _risk_valid_for_side(side, entry_price, stop_loss, take_profit):
                warnings.append(f"Ignored invalid entry risk for {entry['symbol']}.")
                continue
            notional_usd = position_notional_from_risk(
                account_after_management,
                entry_price=entry_price,
                stop_loss=stop_loss,
                settings=settings,
            )
            if notional_usd < 20:
                continue
            if account_key == "live":
                live_config = live_config or read_live_trading_config()
                live_status_payload = live_status_payload or live_execution_status(live_config, settings)
                if not live_status_payload["canExecute"]:
                    warnings.append(f"Skipped live entry {entry['symbol']}: real execution is not enabled.")
                    continue
                notional_usd = cap_live_notional_by_margin(
                    notional_usd,
                    account_summary=account_after_management,
                    live_config=live_config,
                )
                if notional_usd < 20:
                    warnings.append(f"Skipped live entry {entry['symbol']}: available margin is too small after leverage cap.")
                    continue
                try:
                    apply_symbol_settings(live_config, entry["symbol"])
                except Exception as error:
                    warnings.append(f"Live symbol settings update skipped for {entry['symbol']}: {error}")
                quantity = normalize_quantity(live_config, entry["symbol"], notional_usd=notional_usd, reference_price=entry_price)
                order_side = "BUY" if side == "long" else "SELL"
                place_market_order(live_config, symbol=entry["symbol"], side=order_side, quantity=quantity)
                if settings["liveExecution"]["useExchangeProtectionOrders"]:
                    try:
                        cancel_all_open_orders(live_config, entry["symbol"])
                        place_protection_orders(
                            live_config,
                            symbol=entry["symbol"],
                            position_side=side,
                            stop_loss=stop_loss,
                            take_profit=take_profit,
                        )
                    except Exception as error:
                        warnings.append(f"Exchange protection order placement failed for {entry['symbol']}: {error}")
            else:
                open_paper_position(
                    book,
                    candidate=candidate,
                    side=side,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    confidence=entry["confidence"],
                    notional_usd=notional_usd,
                    reason=entry["reason"],
                    decision_id=decision_id,
                )
            open_symbols.add(entry["symbol"])
            opened += 1
            entry_actions.append(
                {
                    "type": "open",
                    "symbol": entry["symbol"],
                    "side": side,
                    "confidence": entry["confidence"],
                    "notionalUsd": notional_usd,
                    "stopLoss": stop_loss,
                    "takeProfit": take_profit,
                    "reason": entry["reason"],
                    "label": action_label("open", entry["symbol"], side),
                }
            )
            account_after_management = summarize_account(book, settings)
    if account_key == "live":
        book, live_warnings, live_status_payload, live_config = sync_live_book(book, settings)
        warnings.extend(live_warnings)
        state["live"] = book
    else:
        state["paper"] = book
    account_after = summarize_account(book, settings)
    if account_after["equityUsd"] > (num(book.get("highWatermarkEquity")) or book["initialCapitalUsd"]):
        book["highWatermarkEquity"] = account_after["equityUsd"]
    book["lastDecisionAt"] = now_iso()
    decision = normalize_decision(
        {
            "id": decision_id,
            "startedAt": now_iso(),
            "finishedAt": now_iso(),
            "runnerReason": reason,
            "mode": settings["mode"],
            "prompt": prompt,
            "promptSummary": one_line(parsed_model.get("summary") or f"Managed {len(account_before['openPositions'])} positions and reviewed {len(candidate_snapshots)} candidates."),
            "output": {
                "summary": parsed_model.get("summary"),
                "positionActions": parsed_model["position_actions"],
                "entryActions": parsed_model["entry_actions"],
                "watchlist": parsed_model["watchlist"],
                "providerStatus": provider_status(provider),
                "liveExecutionStatus": live_status_payload,
            },
            "rawModelResponse": model_result or {},
            "actions": management_actions + breaker_actions + entry_actions,
            "warnings": warnings,
            "candidateUniverse": [serialize_candidate_for_history(item) for item in candidate_snapshots],
            "accountBefore": account_before,
            "accountAfter": account_after,
        }
    )
    book.setdefault("decisions", []).append(decision)
    state["adaptive"] = {
        "updatedAt": now_iso(),
        "notes": [
            f"Latest cycle used {provider['preset']} / {provider['model']} in {settings['mode']} mode.",
            f"Current account drawdown is {account_after['drawdownPct']:.2f}% and gross exposure is ${account_after['grossExposureUsd']:.2f}.",
            "The editable trade-logic fields affect judgment only. Market data, positions, and risk limits are always injected by the system.",
        ],
    }
    write_trading_state(state)
    archive_decision(decision)
    return {
        "settings": settings,
        "state": state,
        "decision": decision,
        "marketBackdrop": market_backdrop,
        "liveExecutionStatus": live_status_payload,
    }


def run_trading_cycle_batch(reason: str = "manual", modes: list[str] | None = None) -> dict[str, Any]:
    settings = read_trading_settings()
    requested_modes = [clean_mode(item) for item in (modes or enabled_modes(settings))]
    unique_modes: list[str] = []
    for mode in requested_modes:
        if mode not in unique_modes:
            unique_modes.append(mode)
    results = []
    for mode in unique_modes:
        result = run_trading_cycle(reason=reason, mode_override=mode)
        results.append(
            {
                "ok": True,
                "mode": mode,
                "result": result,
            }
        )
    return {
        "settings": settings,
        "modes": unique_modes,
        "activeMode": unique_modes[0] if unique_modes else "paper",
        "results": results,
        "primaryResult": results[0]["result"] if results else None,
    }


def preview_trading_prompt_decision(mode_override: str | None = None, prompt_override: dict[str, Any] | None = None) -> dict[str, Any]:
    settings = read_trading_settings()
    settings["mode"] = clean_mode(mode_override or settings["mode"])
    universe = read_fixed_universe()
    account_key = account_key_for_mode(settings["mode"])
    cycle_exchange_id = str(settings.get("activeExchange") or "binance").strip().lower() or "binance"
    if account_key == "live":
        live_config = read_live_trading_config()
        cycle_exchange_id = str(live_config.get("exchange") or cycle_exchange_id).strip().lower() or cycle_exchange_id
    scan = read_latest_scan(cycle_exchange_id)
    if universe.get("dynamicSource", {}).get("enabled") or not scan["opportunities"] or str(scan.get("exchange") or "").strip().lower() != cycle_exchange_id:
        scan = refresh_candidate_pool(cycle_exchange_id)
    state = read_trading_state(settings)
    book = deepcopy(state[account_key])
    warnings: list[str] = []
    if account_key == "live":
        book, live_warnings, _, _ = sync_live_book(book, settings)
        warnings.extend(live_warnings)
    prompt_settings = prompt_override or read_prompt_settings()
    prompt_kline_feeds = prompt_settings.get("klineFeeds") if isinstance(prompt_settings.get("klineFeeds"), dict) else {}
    raw_candidates = candidate_universe_from_scan(scan)
    symbols = []
    for item in raw_candidates:
        symbol = str(item.get("symbol") or "").upper()
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    for position in book.get("openPositions", []):
        if position["symbol"] not in symbols:
            symbols.append(position["symbol"])
    live_by_symbol, live_context_warnings = _fetch_live_contexts_for_exchange(symbols, prompt_kline_feeds, cycle_exchange_id)
    warnings.extend(live_context_warnings)
    mark_to_market(book, live_by_symbol)
    gateway = get_active_exchange_gateway(cycle_exchange_id)
    market_backdrop = fetch_market_backdrop(prompt_kline_feeds, cycle_exchange_id) if gateway.default_backdrop_symbol in symbols else {}
    candidate_snapshots = []
    for opportunity in raw_candidates:
        symbol = str(opportunity.get("symbol") or "").upper()
        live = live_by_symbol.get(symbol)
        if not live:
            continue
        candidate_snapshots.append(build_candidate_snapshot(opportunity, live, settings, cycle_exchange_id))
    candidates_by_symbol = {item["symbol"]: item for item in candidate_snapshots}
    account_summary = summarize_account(book, settings)
    provider = read_llm_provider()
    prompt = build_prompt(
        settings=settings,
        prompt_settings=prompt_settings,
        provider=provider,
        market_backdrop=market_backdrop,
        account_summary=account_summary,
        open_positions=account_summary["openPositions"],
        open_orders=[normalize_order(item) for item in book.get("openOrders", [])],
        candidates=candidate_snapshots,
    )
    model_result = generate_trading_decision(prompt, provider)
    parsed_model = normalize_model_decision(
        model_result["parsed"],
        open_positions=account_summary["openPositions"],
        candidates_by_symbol=candidates_by_symbol,
    )
    return {
        "mode": settings["mode"],
        "promptName": prompt_settings.get("name") or "default_trading_logic",
        "candidateCount": len(candidate_snapshots),
        "account": account_summary,
        "warnings": warnings,
        "prompt": prompt,
        "rawText": model_result["rawText"],
        "parsed": parsed_model,
        "provider": model_result["provider"],
    }


def summarize_book_history(book: dict[str, Any]) -> dict[str, Any]:
    recent_decisions = list(book.get("decisions", []))[-8:]
    decision_timeline = [
        {
            "id": item["id"],
            "startedAt": item["startedAt"],
            "finishedAt": item["finishedAt"],
            "actions": item["actions"],
        }
        for item in book.get("decisions", [])[-240:]
    ]
    return {
        "sessionStartedAt": book.get("sessionStartedAt"),
        "lastDecisionAt": book.get("lastDecisionAt"),
        "decisions": recent_decisions,
        "decisionTimeline": decision_timeline,
        "exchangeClosedTrades": list(book.get("exchangeClosedTrades", [])),
        "closedTrades": list(book.get("closedTrades", [])),
    }


def compact_latest_decision(decision: dict[str, Any] | None) -> dict[str, Any] | None:
    if not decision:
        return None
    return {
        "id": decision["id"],
        "startedAt": decision["startedAt"],
        "finishedAt": decision["finishedAt"],
        "runnerReason": decision["runnerReason"],
        "mode": decision["mode"],
        "promptSummary": decision["promptSummary"],
        "actionsCount": len(decision.get("actions", [])),
    }


def summarize_trading_state() -> dict[str, Any]:
    settings = read_trading_settings()
    state = read_trading_state(settings)
    live_status_payload = live_execution_status(read_live_trading_config(), settings)
    scan = read_latest_scan(settings.get("activeExchange"))
    active_mode = settings["mode"]
    active_key = account_key_for_mode(active_mode)
    active_book = state[active_key]
    paper_account = summarize_account(state["paper"], {**settings, "mode": "paper"})
    live_account = summarize_account(state["live"], {**settings, "mode": "live"})
    active_account = summarize_account(active_book, settings)
    return {
        "settings": settings,
        "activeMode": active_mode,
        "paperTradingEnabled": settings.get("paperTrading", {}).get("enabled") is True,
        "liveTradingEnabled": settings.get("liveTrading", {}).get("enabled") is True,
        "scan": {
            "runDate": scan.get("runDate"),
            "fetchedAt": scan.get("fetchedAt"),
            "candidateUniverseSize": len(scan.get("opportunities", [])),
        },
        "account": active_account,
        "paperAccount": paper_account,
        "liveAccount": live_account,
        "adaptive": state.get("adaptive"),
        "latestDecision": compact_latest_decision(safe_last(active_book.get("decisions", []))),
        "latestPaperDecision": compact_latest_decision(safe_last(state["paper"].get("decisions", []))),
        "latestLiveDecision": compact_latest_decision(safe_last(state["live"].get("decisions", []))),
        "paperBook": state["paper"],
        "liveBook": state["live"],
        "activeBook": active_book,
        "paperHistory": summarize_book_history(state["paper"]),
        "liveHistory": summarize_book_history(state["live"]),
        "liveExecutionStatus": live_status_payload,
        "providerStatus": provider_status(),
    }


def flatten_active_account(reason: str = "manual_flatten", mode_override: str | None = None) -> dict[str, Any]:
    settings = read_trading_settings()
    target_mode = clean_mode(mode_override or settings["mode"])
    state = read_trading_state(settings)
    account_key = account_key_for_mode(target_mode)
    book = state[account_key]
    decision_id = f"flatten-{int(__import__('time').time() * 1000)}"
    actions = []
    warnings: list[str] = []
    if account_key == "live":
        book, live_warnings, live_status_payload, live_config = sync_live_book(book, settings)
        warnings.extend(live_warnings)
        if not live_status_payload["canExecute"]:
            raise RuntimeError("Live flatten requires real execution to be enabled.")
        for position in list(book.get("openPositions", [])):
            cancel_all_open_orders(live_config, position["symbol"])
            side = "SELL" if position["side"] == "long" else "BUY"
            place_market_order(live_config, symbol=position["symbol"], side=side, quantity=position["quantity"], reduce_only=True)
            book, action = close_position(book, position, num(position.get("lastMarkPrice")) or num(position.get("entryPrice")) or 0, decision_id, reason)
            actions.append(action)
        book, live_warnings, _, _ = sync_live_book(book, settings)
        warnings.extend(live_warnings)
    else:
        for position in list(book.get("openPositions", [])):
            book, action = close_position(book, position, num(position.get("lastMarkPrice")) or num(position.get("entryPrice")) or 0, decision_id, reason)
            actions.append(action)
    decision = normalize_decision(
        {
            "id": decision_id,
            "startedAt": now_iso(),
            "finishedAt": now_iso(),
            "runnerReason": "manual",
            "mode": target_mode,
            "prompt": f"Flatten all open {target_mode} positions because: {reason}",
            "promptSummary": f"Flattened {len(actions)} open {target_mode} positions.",
            "actions": actions,
            "warnings": warnings,
            "output": {"actions": actions},
            "candidateUniverse": [],
            "accountBefore": {},
            "accountAfter": summarize_account(book, {**settings, "mode": target_mode}),
        }
    )
    book.setdefault("decisions", []).append(decision)
    book["lastDecisionAt"] = now_iso()
    write_trading_state(state)
    archive_decision(decision)
    return state


def reset_paper_account(mode: str = "full") -> dict[str, Any]:
    settings = read_trading_settings()
    state = read_trading_state(settings)
    if str(mode) == "equity_only":
        state["paper"]["initialCapitalUsd"] = settings["initialCapitalUsd"]
        state["paper"]["highWatermarkEquity"] = settings["initialCapitalUsd"]
        state["paper"]["openPositions"] = []
        state["paper"]["circuitBreakerTripped"] = False
        state["paper"]["circuitBreakerReason"] = None
    else:
        state["paper"] = empty_trading_account(settings["initialCapitalUsd"], "paper")
    state["adaptive"] = {
        "updatedAt": now_iso(),
        "notes": [
            "Paper account was reset in the Python build.",
            "The trade-logic fields, provider config, and proxy config were preserved.",
        ],
    }
    return write_trading_state(state)


def reset_trading_account(mode: str = "paper") -> dict[str, Any]:
    reset_mode = str(mode or "paper").strip().lower()
    if reset_mode in {"paper", "full", "equity_only"}:
        return reset_paper_account(reset_mode if reset_mode == "equity_only" else "full")

    if reset_mode != "live":
        raise ValueError(f"Unsupported reset mode: {mode}")

    settings = read_trading_settings()
    state = read_trading_state(settings)
    book = state["live"]
    book, warnings, live_status_payload, live_config = sync_live_book(book, settings)
    state["live"] = book
    if not live_status_payload["canSync"]:
        state["live"] = empty_trading_account(settings["initialCapitalUsd"], "exchange")
        state["adaptive"] = {
            "updatedAt": now_iso(),
            "notes": [
                "Live account local state was reset without exchange sync.",
                "No valid live API configuration was available, so only local live decisions, positions, and drawdown baseline were cleared.",
            ],
        }
        return write_trading_state(state)
    if book.get("openPositions"):
        if not live_status_payload["canExecute"]:
            raise RuntimeError("实盘重置发现当前仍有持仓。请先启用实盘并关闭模拟下单，或先手动全部平仓。")
        for position in list(book.get("openPositions", [])):
            cancel_all_open_orders(live_config, position["symbol"])
            side = "SELL" if position["side"] == "long" else "BUY"
            place_market_order(
                live_config,
                symbol=position["symbol"],
                side=side,
                quantity=position["quantity"],
                reduce_only=True,
            )
    fresh_book = empty_trading_account(num(book.get("exchangeEquityUsd")) or settings["initialCapitalUsd"], "exchange")
    fresh_book, sync_warnings, _, _ = sync_live_book(fresh_book, settings)
    warnings.extend(sync_warnings)
    state["live"] = fresh_book
    state["adaptive"] = {
        "updatedAt": now_iso(),
        "notes": [
            "Live account was reset in the Python build.",
            "Live decisions, local estimated realized PnL, drawdown baseline, and synced positions were cleared.",
        ] + ([f"Reset warnings: {'; '.join(warnings[:3])}"] if warnings else []),
    }
    return write_trading_state(state)
