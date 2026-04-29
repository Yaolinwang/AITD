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
    read_network_settings,
    read_prompt_settings,
    read_trading_settings,
)
from .exchange_cooldown import cooldown_status
from .exchanges import base_asset_for_symbol, get_active_exchange_gateway
from .evolution import historical_lessons_for_prompt
from .instances import instance_paths, read_instance
from .live_trading import (
    apply_symbol_settings,
    cancel_all_open_orders,
    fetch_account_snapshot,
    live_execution_status,
    normalize_quantity,
    place_market_order,
    place_protection_orders,
)
from .llm import ModelDecisionParseError, generate_trading_decision, provider_status
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


def _state_path(instance_id: str | None = None) -> Path:
    if instance_id:
        return instance_paths(instance_id)["state"]
    return STATE_PATH


def _decisions_dir(instance_id: str | None = None) -> Path:
    if instance_id:
        return instance_paths(instance_id)["decisions_dir"]
    return DECISIONS_DIR


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


def dedupe_messages(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in values:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


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
        "executionEvents": [],
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
        "takeProfitFraction": num(position.get("takeProfitFraction")),
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
        "side": "short" if str(trade.get("side") or "long").lower() == "short" else "long",
        "quantity": num(trade.get("quantity")),
        "exitPrice": num(trade.get("exitPrice")),
        "notionalUsd": num(trade.get("notionalUsd")),
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


def _json_safe(value: Any) -> Any:
    try:
        return json.loads(json.dumps(value, ensure_ascii=False, default=str))
    except Exception:
        return str(value)


def normalize_execution_event(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(event.get("id") or f"exec-{int(__import__('time').time() * 1000)}"),
        "at": event.get("at") or now_iso(),
        "decisionId": event.get("decisionId"),
        "mode": clean_mode(event.get("mode") or "live"),
        "exchange": str(event.get("exchange") or "").strip().lower(),
        "stage": str(event.get("stage") or "").strip(),
        "actionType": str(event.get("actionType") or "").strip(),
        "symbol": str(event.get("symbol") or "").upper(),
        "side": str(event.get("side") or "").strip().lower(),
        "success": event.get("success") is not False,
        "requested": _json_safe(event.get("requested") if isinstance(event.get("requested"), dict) else {}),
        "exchangeResult": _json_safe(event.get("exchangeResult")),
        "error": str(event.get("error")).strip() if event.get("error") is not None else None,
    }


def append_execution_event(
    events: list[dict[str, Any]] | None,
    *,
    decision_id: str,
    stage: str,
    action_type: str,
    symbol: str | None = None,
    side: str | None = None,
    exchange: str | None = None,
    requested: dict[str, Any] | None = None,
    exchange_result: Any = None,
    success: bool = True,
    error: Any = None,
) -> dict[str, Any] | None:
    if events is None:
        return None
    event = normalize_execution_event(
        {
            "id": f"{decision_id}-exec-{len(events) + 1:03d}",
            "decisionId": decision_id,
            "mode": "live",
            "exchange": exchange,
            "stage": stage,
            "actionType": action_type,
            "symbol": symbol,
            "side": side,
            "success": success,
            "requested": requested or {},
            "exchangeResult": exchange_result,
            "error": error,
        }
    )
    events.append(event)
    return event


def _order_matches_position_side(order: dict[str, Any], position: dict[str, Any]) -> bool:
    side = str(position.get("side") or "").strip().lower()
    order_side = str(order.get("side") or "").strip().upper()
    position_side = str(order.get("positionSide") or "").strip().upper()
    if side == "long":
        return order_side == "SELL" and position_side in {"", "BOTH", "LONG"}
    if side == "short":
        return order_side == "BUY" and position_side in {"", "BOTH", "SHORT"}
    return False


def _infer_exchange_protection_from_orders(position: dict[str, Any], orders: list[dict[str, Any]]) -> dict[str, Any]:
    symbol = str(position.get("symbol") or "").upper()
    side = str(position.get("side") or "").strip().lower()
    quantity = num(position.get("quantity")) or 0
    reference_price = num(position.get("lastMarkPrice")) or num(position.get("markPrice")) or num(position.get("entryPrice"))
    if not symbol or side not in {"long", "short"} or reference_price is None:
        return {}
    stop_candidates: list[float] = []
    take_profit_candidates: list[tuple[float, float | None]] = []
    for raw_order in orders:
        order = normalize_order(raw_order)
        if order.get("symbol") != symbol or not _order_matches_position_side(order, position):
            continue
        if not _is_protection_order(order):
            continue
        trigger_price = num(order.get("triggerPrice"))
        if trigger_price is None:
            continue
        if side == "long":
            if trigger_price <= reference_price:
                stop_candidates.append(trigger_price)
            else:
                take_profit_candidates.append((trigger_price, num(order.get("quantity"))))
        else:
            if trigger_price >= reference_price:
                stop_candidates.append(trigger_price)
            else:
                take_profit_candidates.append((trigger_price, num(order.get("quantity"))))
    result: dict[str, Any] = {}
    if stop_candidates:
        result["stopLoss"] = max(stop_candidates) if side == "long" else min(stop_candidates)
    if take_profit_candidates:
        price, order_quantity = (
            min(take_profit_candidates, key=lambda item: item[0])
            if side == "long"
            else max(take_profit_candidates, key=lambda item: item[0])
        )
        result["takeProfit"] = price
        if order_quantity is not None and quantity > 0:
            result["takeProfitFraction"] = max(0.05, min(1.0, order_quantity / quantity))
    return result


def _is_protection_order(order: dict[str, Any]) -> bool:
    source = str(order.get("source") or "").strip().lower()
    order_type = str(order.get("type") or "").strip().upper()
    if order.get("closePosition") is True or order.get("reduceOnly") is True:
        return True
    if source == "binance_algo_order":
        return True
    if "TAKE_PROFIT" in order_type or "STOP" in order_type:
        return True
    return order_type in {"CONDITIONAL", "STOP_MARKET", "TAKE_PROFIT_MARKET"}


def _cleanup_orphan_live_protection_orders(
    snapshot: dict[str, Any],
    settings: dict[str, Any],
    live_config: dict[str, Any] | None,
    status: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    if not live_config or not status.get("canExecute"):
        return snapshot, []
    if not settings.get("liveExecution", {}).get("useExchangeProtectionOrders", True):
        return snapshot, []

    open_positions = [normalize_position(item) for item in snapshot.get("openPositions", [])]
    open_orders = [normalize_order(item) for item in snapshot.get("openOrders", [])]
    position_symbols = {item["symbol"] for item in open_positions}
    orders_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for order in open_orders:
        symbol = order.get("symbol")
        if not symbol:
            continue
        orders_by_symbol.setdefault(symbol, []).append(order)

    orphan_symbols = [
        symbol
        for symbol, orders in orders_by_symbol.items()
        if symbol not in position_symbols and orders and all(_is_protection_order(order) for order in orders)
    ]
    if not orphan_symbols:
        return snapshot, []

    warnings: list[str] = []
    for symbol in orphan_symbols:
        cancel_all_open_orders(live_config, symbol)
        warnings.append(f"Canceled orphaned exchange protection orders for {symbol}.")
    refreshed = fetch_account_snapshot(live_config, session_started_at=snapshot.get("sessionStartedAt"))
    return refreshed, warnings


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
        "executionEvents": [normalize_execution_event(item) for item in decision.get("executionEvents", []) if isinstance(item, dict)],
        "warnings": decision.get("warnings") if isinstance(decision.get("warnings"), list) else [],
        "candidateUniverse": decision.get("candidateUniverse") if isinstance(decision.get("candidateUniverse"), list) else [],
        "accountBefore": decision.get("accountBefore") if isinstance(decision.get("accountBefore"), dict) else {},
        "accountAfter": decision.get("accountAfter") if isinstance(decision.get("accountAfter"), dict) else {},
    }


def read_trading_state(settings: dict[str, Any] | None = None, instance_id: str | None = None) -> dict[str, Any]:
    settings = settings or read_trading_settings(instance_id)
    saved = read_json(_state_path(instance_id), {})
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
            "executionEvents": [normalize_execution_event(item) for item in seed.get("executionEvents", []) if isinstance(item, dict)],
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


def write_trading_state(state: dict[str, Any], instance_id: str | None = None) -> dict[str, Any]:
    payload = deepcopy(state)
    for key in ("paper", "live"):
        payload[key]["openPositions"] = [normalize_position(item) for item in payload[key].get("openPositions", [])]
        payload[key]["openOrders"] = [normalize_order(item) for item in payload[key].get("openOrders", [])][-80:]
        payload[key]["exchangeClosedTrades"] = [normalize_exchange_closed_trade(item) for item in payload[key].get("exchangeClosedTrades", [])][-400:]
        payload[key]["executionEvents"] = [normalize_execution_event(item) for item in payload[key].get("executionEvents", []) if isinstance(item, dict)][-1000:]
        payload[key]["closedTrades"] = [normalize_trade(item) for item in payload[key].get("closedTrades", [])][-400:]
        payload[key]["decisions"] = [normalize_decision(item) for item in payload[key].get("decisions", [])][-40:]
    payload["updatedAt"] = now_iso()
    write_json(_state_path(instance_id), payload)
    return payload


def archive_decision(decision: dict[str, Any], instance_id: str | None = None) -> None:
    run_date = current_run_date()
    path = _decisions_dir(instance_id) / run_date / f"{decision['id']}.json"
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
    if (book.get("accountSource") or "") == "exchange" and open_orders:
        open_positions = [
            {
                **position,
                **_infer_exchange_protection_from_orders(position, open_orders),
            }
            for position in open_positions
        ]
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
    historical_lessons: list[dict[str, Any]] | None = None,
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
                "takeProfitFraction": 0.25,
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
                "takeProfitFraction": 0.25,
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
        "Use takeProfitFraction below 1.0 when a take-profit should only close part of the position and leave a runner.",
        "Return strict JSON only. No markdown, no prose outside the JSON object.",
    ]
    if historical_lessons:
        rules.append(
            "Historical scene lessons are advisory memory only. Use them to adjust judgment, never to override hard risk limits."
        )
        rules.append(
            "Historical source symbols are audit labels, not transferable edge; match on scene, trend, volatility, pullback, volume, funding, and rank."
        )
    sections = [
        "# Editable Trading Logic JSON",
        json.dumps(prompt_settings["decision_logic"], ensure_ascii=False, indent=2),
        "",
        "# System Rules",
        *[f"- {rule}" for rule in rules],
        "",
        "# Required JSON Contract",
        json.dumps(response_contract, ensure_ascii=False, indent=2),
        "",
    ]
    if historical_lessons:
        sections.extend(
            [
                "# Historical Scene Lessons (Self-Learning Enabled)",
                json.dumps(historical_lessons, ensure_ascii=False, indent=2),
                "",
            ]
        )
    sections.extend(
        [
            "# Current Trading Context",
            json.dumps(context, ensure_ascii=False, indent=2),
        ]
    )
    return "\n".join(sections)


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
        reduce_fraction = None
        if decision == "reduce":
            reduce_fraction = clamp(item.get("reduceFraction"), 0.05, 0.95)
        take_profit_fraction = num(item.get("takeProfitFraction"))
        if take_profit_fraction is not None:
            take_profit_fraction = max(0.05, min(1.0, take_profit_fraction))
        normalized_positions.append(
            {
                "symbol": symbol,
                "decision": decision,
                "reason": str(item.get("reason") or ""),
                "reduceFraction": reduce_fraction,
                "stopLoss": num(item.get("stopLoss")),
                "takeProfit": num(item.get("takeProfit")),
                "takeProfitFraction": take_profit_fraction,
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
                    "reduceFraction": None,
                    "stopLoss": None,
                    "takeProfit": None,
                    "takeProfitFraction": None,
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
                "takeProfitFraction": max(0.05, min(1.0, num(item.get("takeProfitFraction")) or 1.0)),
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
    return _stop_valid_for_side(side, mark_price, stop_loss) and _take_profit_valid_for_side(side, mark_price, take_profit)


def _reward_r_multiple(side: str, reference_price: float, stop_loss: float | None, take_profit: float | None) -> float | None:
    if reference_price <= 0 or stop_loss is None or take_profit is None:
        return None
    risk = abs(reference_price - stop_loss)
    reward = abs(take_profit - reference_price)
    if risk <= 0:
        return None
    return reward / risk


def _execution_price_from_order_result(result: Any, fallback_price: float) -> float:
    if not isinstance(result, dict):
        return fallback_price
    for key in ("avgPrice", "averagePrice", "price", "fillPx", "px"):
        value = num(result.get(key))
        if value is not None and value > 0:
            return value
    fills = result.get("fills")
    if isinstance(fills, list):
        total_qty = 0.0
        total_quote = 0.0
        for fill in fills:
            if not isinstance(fill, dict):
                continue
            price = num(fill.get("price") or fill.get("px"))
            qty = num(fill.get("qty") or fill.get("quantity") or fill.get("sz"))
            if price is None or qty is None or price <= 0 or qty <= 0:
                continue
            total_qty += qty
            total_quote += price * qty
        if total_qty > 0:
            return total_quote / total_qty
    return fallback_price


def _validate_live_protection_inputs(
    *,
    symbol: str,
    side: str,
    reference_price: float,
    stop_loss: float | None,
    take_profit: float | None,
    take_profit_fraction: float | None,
    min_reward_r: float = 1.0,
) -> tuple[float | None, float | None, float | None, bool, list[str]]:
    warnings: list[str] = []
    if reference_price <= 0:
        return stop_loss, take_profit, take_profit_fraction, True, warnings
    if not _stop_valid_for_side(side, reference_price, stop_loss):
        warnings.append(
            f"Live protection rejected for {symbol}: stopLoss {stop_loss} is invalid after actual reference price {reference_price}."
        )
        return None, None, None, False, warnings
    safe_take_profit = take_profit
    safe_take_profit_fraction = take_profit_fraction
    if not _take_profit_valid_for_side(side, reference_price, safe_take_profit):
        warnings.append(
            f"Live take-profit skipped for {symbol}: takeProfit {safe_take_profit} would immediately trigger after actual reference price {reference_price}."
        )
        safe_take_profit = None
        safe_take_profit_fraction = None
    reward_r = _reward_r_multiple(side, reference_price, stop_loss, safe_take_profit)
    if reward_r is not None and reward_r < min_reward_r:
        warnings.append(
            f"Live take-profit skipped for {symbol}: first target is only {reward_r:.2f}R after actual reference price {reference_price}."
        )
        safe_take_profit = None
        safe_take_profit_fraction = None
    return stop_loss, safe_take_profit, safe_take_profit_fraction, True, warnings


def _place_live_protection_orders_with_fallback(
    live_config: dict[str, Any],
    *,
    symbol: str,
    position_side: str,
    quantity: float | None,
    stop_loss: float | None,
    take_profit: float | None,
    take_profit_fraction: float | None,
    warning_prefix: str,
    decision_id: str | None = None,
    action_type: str = "protection_update",
    execution_events: list[dict[str, Any]] | None = None,
) -> list[str]:
    warnings: list[str] = []
    exchange_id = str(live_config.get("exchange") or "").strip().lower()
    requested = {
        "quantity": quantity,
        "stopLoss": stop_loss,
        "takeProfit": take_profit,
        "takeProfitFraction": take_profit_fraction,
        "positionSide": position_side,
    }
    try:
        result = place_protection_orders(
            live_config,
            symbol=symbol,
            position_side=position_side,
            quantity=quantity,
            stop_loss=stop_loss,
            take_profit=take_profit,
            take_profit_fraction=take_profit_fraction,
        )
        if decision_id:
            append_execution_event(
                execution_events,
                decision_id=decision_id,
                stage="protection_orders",
                action_type=action_type,
                symbol=symbol,
                side=position_side,
                exchange=exchange_id,
                requested=requested,
                exchange_result=result,
            )
        return warnings
    except Exception as error:
        if decision_id:
            append_execution_event(
                execution_events,
                decision_id=decision_id,
                stage="protection_orders",
                action_type=action_type,
                symbol=symbol,
                side=position_side,
                exchange=exchange_id,
                requested=requested,
                success=False,
                error=error,
            )
        warnings.append(f"{warning_prefix} for {symbol}: {error}")
        if stop_loss is None or take_profit is None:
            return warnings
    try:
        cancel_result = cancel_all_open_orders(live_config, symbol)
        if decision_id:
            append_execution_event(
                execution_events,
                decision_id=decision_id,
                stage="cancel_open_orders",
                action_type=f"{action_type}_fallback",
                symbol=symbol,
                side=position_side,
                exchange=exchange_id,
                requested={"reason": "retry_stop_only_after_take_profit_failed"},
                exchange_result=cancel_result,
            )
        result = place_protection_orders(
            live_config,
            symbol=symbol,
            position_side=position_side,
            quantity=quantity,
            stop_loss=stop_loss,
            take_profit=None,
            take_profit_fraction=None,
        )
        if decision_id:
            append_execution_event(
                execution_events,
                decision_id=decision_id,
                stage="protection_orders_stop_only",
                action_type=f"{action_type}_fallback",
                symbol=symbol,
                side=position_side,
                exchange=exchange_id,
                requested={**requested, "takeProfit": None, "takeProfitFraction": None},
                exchange_result=result,
            )
        warnings.append(f"Placed stop-only live protection for {symbol} after take-profit placement failed.")
    except Exception as stop_error:
        if decision_id:
            append_execution_event(
                execution_events,
                decision_id=decision_id,
                stage="protection_orders_stop_only",
                action_type=f"{action_type}_fallback",
                symbol=symbol,
                side=position_side,
                exchange=exchange_id,
                requested={**requested, "takeProfit": None, "takeProfitFraction": None},
                success=False,
                error=stop_error,
            )
        warnings.append(f"Stop-only live protection also failed for {symbol}: {stop_error}")
    return warnings


def _stop_valid_for_side(side: str, mark_price: float, stop_loss: float | None) -> bool:
    if side == "long":
        if stop_loss is not None and stop_loss >= mark_price:
            return False
    else:
        if stop_loss is not None and stop_loss <= mark_price:
            return False
    return True


def _take_profit_valid_for_side(side: str, mark_price: float, take_profit: float | None) -> bool:
    if side == "long":
        if take_profit is not None and take_profit <= mark_price:
            return False
    else:
        if take_profit is not None and take_profit >= mark_price:
            return False
    return True


def _take_profit_reached_for_side(side: str, mark_price: float, take_profit: float | None) -> bool:
    if take_profit is None:
        return False
    if side == "long":
        return take_profit <= mark_price
    return take_profit >= mark_price


def _trailing_profit_stop(position: dict[str, Any]) -> float | None:
    entry = num(position.get("entryPrice"))
    mark = num(position.get("lastMarkPrice"))
    if entry is None or mark is None or entry <= 0 or mark <= 0:
        return None
    side = str(position.get("side") or "").lower()
    if side == "long":
        profit_pct = ((mark - entry) / entry) * 100
    else:
        profit_pct = ((entry - mark) / entry) * 100
    if profit_pct < 8:
        return None
    if profit_pct >= 20:
        lock_fraction = 0.50
    elif profit_pct >= 12:
        lock_fraction = 0.35
    else:
        lock_fraction = 0.20
    locked_profit = abs(mark - entry) * lock_fraction
    if side == "long":
        return entry + locked_profit
    if side == "short":
        return entry - locked_profit
    return None


def apply_trailing_profit_stops(
    book: dict[str, Any],
    *,
    live_mode: bool,
    decision_id: str,
    status: dict[str, Any] | None = None,
    live_config: dict[str, Any] | None = None,
    settings: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], list[str], list[dict[str, Any]]]:
    actions: list[dict[str, Any]] = []
    warnings: list[str] = []
    execution_events: list[dict[str, Any]] = []
    use_exchange_orders = bool(settings and settings.get("liveExecution", {}).get("useExchangeProtectionOrders", True))
    for position in list(book.get("openPositions", [])):
        mark_price = num(position.get("lastMarkPrice")) or num(position.get("entryPrice"))
        proposed_stop = _trailing_profit_stop(position)
        if proposed_stop is None or mark_price is None:
            continue
        current_stop = num(position.get("stopLoss"))
        side = str(position.get("side") or "").lower()
        if side == "long" and current_stop is not None and current_stop >= proposed_stop:
            continue
        if side == "short" and current_stop is not None and current_stop <= proposed_stop:
            continue
        if not _stop_valid_for_side(side, mark_price, proposed_stop):
            continue
        for current in book.get("openPositions", []):
            if current["id"] != position["id"]:
                continue
            current["stopLoss"] = proposed_stop
            current["updatedAt"] = now_iso()
            position = current
            break
        if live_mode and status and status.get("canExecute") and live_config and use_exchange_orders:
            exchange_id = str(live_config.get("exchange") or "").strip().lower()
            try:
                cancel_result = cancel_all_open_orders(live_config, position["symbol"])
                append_execution_event(
                    execution_events,
                    decision_id=decision_id,
                    stage="cancel_open_orders",
                    action_type="auto_trailing_stop",
                    symbol=position["symbol"],
                    side=position["side"],
                    exchange=exchange_id,
                    requested={"reason": "replace_protection_for_trailing_stop"},
                    exchange_result=cancel_result,
                )
            except Exception as error:
                append_execution_event(
                    execution_events,
                    decision_id=decision_id,
                    stage="cancel_open_orders",
                    action_type="auto_trailing_stop",
                    symbol=position["symbol"],
                    side=position["side"],
                    exchange=exchange_id,
                    requested={"reason": "replace_protection_for_trailing_stop"},
                    success=False,
                    error=error,
                )
                warnings.append(f"Trailing stop update failed for {position['symbol']}: {error}")
            else:
                warnings.extend(
                    _place_live_protection_orders_with_fallback(
                        live_config,
                        symbol=position["symbol"],
                        position_side=position["side"],
                        quantity=num(position.get("quantity")) if position.get("takeProfit") is not None else None,
                        stop_loss=proposed_stop,
                        take_profit=num(position.get("takeProfit")),
                        take_profit_fraction=num(position.get("takeProfitFraction")),
                        warning_prefix="Trailing stop update failed",
                        decision_id=decision_id,
                        action_type="auto_trailing_stop",
                        execution_events=execution_events,
                    )
                )
        actions.append(
            {
                "type": "update",
                "symbol": position["symbol"],
                "side": position["side"],
                "stopLoss": proposed_stop,
                "takeProfit": position.get("takeProfit"),
                "takeProfitFraction": position.get("takeProfitFraction"),
                "reason": "auto_trailing_profit_stop",
                "label": action_label("update", position["symbol"]),
            }
        )
    return actions, warnings, execution_events


def apply_protection_hits(book: dict[str, Any], decision_id: str) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for position in list(book.get("openPositions", [])):
        mark_price = num(position.get("lastMarkPrice"))
        if mark_price is None:
            continue
        stop_loss = num(position.get("stopLoss"))
        take_profit = num(position.get("takeProfit"))
        take_profit_fraction = num(position.get("takeProfitFraction"))
        if position["side"] == "long":
            if stop_loss is not None and mark_price <= stop_loss:
                book, action = close_position(book, position, mark_price, decision_id, "stop_loss_hit")
                actions.append(action)
                continue
            if take_profit is not None and mark_price >= take_profit:
                if take_profit_fraction is not None and take_profit_fraction < 0.999:
                    book, action = reduce_position(book, position, mark_price, take_profit_fraction, decision_id, "take_profit_hit")
                    for current in book.get("openPositions", []):
                        if current["id"] != position["id"]:
                            continue
                        current["takeProfit"] = None
                        current["takeProfitFraction"] = None
                        current["updatedAt"] = now_iso()
                        break
                else:
                    book, action = close_position(book, position, mark_price, decision_id, "take_profit_hit")
                actions.append(action)
                continue
        else:
            if stop_loss is not None and mark_price >= stop_loss:
                book, action = close_position(book, position, mark_price, decision_id, "stop_loss_hit")
                actions.append(action)
                continue
            if take_profit is not None and mark_price <= take_profit:
                if take_profit_fraction is not None and take_profit_fraction < 0.999:
                    book, action = reduce_position(book, position, mark_price, take_profit_fraction, decision_id, "take_profit_hit")
                    for current in book.get("openPositions", []):
                        if current["id"] != position["id"]:
                            continue
                        current["takeProfit"] = None
                        current["takeProfitFraction"] = None
                        current["updatedAt"] = now_iso()
                        break
                else:
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
    take_profit_fraction: float | None,
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
            "takeProfitFraction": take_profit_fraction,
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
    instance_id: str | None = None,
) -> tuple[dict[str, Any], list[str], dict[str, Any], dict[str, Any] | None]:
    live_config = read_live_trading_config(instance_id)
    status = live_execution_status(live_config, settings)
    warnings: list[str] = []
    if not status["canSync"]:
        warnings.extend(status["issues"])
        return book, warnings, status, live_config
    session_started_at = book.get("sessionStartedAt") or derive_session_started_at(book)
    if session_started_at:
        book["sessionStartedAt"] = session_started_at
    snapshot = fetch_account_snapshot(live_config, session_started_at=session_started_at)
    snapshot["sessionStartedAt"] = session_started_at
    accounting_note = str(snapshot.get("accountingNote") or "").strip()
    if accounting_note:
        warnings.append(accounting_note)
    snapshot, cleanup_warnings = _cleanup_orphan_live_protection_orders(snapshot, settings, live_config, status)
    warnings.extend(cleanup_warnings)
    prior_positions = {item["symbol"]: item for item in book.get("openPositions", [])}
    merged_orders = [normalize_order(item) for item in snapshot.get("openOrders", [])]
    use_exchange_protection = settings.get("liveExecution", {}).get("useExchangeProtectionOrders", True)
    merged_positions = []
    for position in snapshot["openPositions"]:
        prior = prior_positions.get(position["symbol"], {})
        protection = _infer_exchange_protection_from_orders(position, merged_orders) if use_exchange_protection else {}
        merged = normalize_position(
            {
                **position,
                "stopLoss": protection.get("stopLoss") if use_exchange_protection else prior.get("stopLoss"),
                "takeProfit": protection.get("takeProfit") if use_exchange_protection else prior.get("takeProfit"),
                "takeProfitFraction": protection.get("takeProfitFraction") if use_exchange_protection else prior.get("takeProfitFraction"),
                "openedAt": prior.get("openedAt") or now_iso(),
                "entryReason": prior.get("entryReason") or "synced_from_exchange",
                "decisionId": prior.get("decisionId"),
            }
        )
        merged_positions.append(merged)
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


def refresh_account_state_after_settings_save(*, reset_live_session: bool = False, instance_id: str | None = None) -> dict[str, Any]:
    settings = read_trading_settings(instance_id)
    state = read_trading_state(settings, instance_id)

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
    live_config: dict[str, Any] | None = read_live_trading_config(instance_id)
    live_sync_attempted = False
    instance_type = None
    if instance_id:
        try:
            instance_type = read_instance(instance_id)["type"]
        except Exception:
            instance_type = None

    live_has_local_state = bool(
        state["live"].get("sessionStartedAt")
        or state["live"].get("openPositions")
        or state["live"].get("decisions")
        or state["live"].get("closedTrades")
    )
    if instance_type == "paper":
        should_sync_live = False
    else:
        live_status_payload = live_execution_status(live_config, settings)
        can_sync_live = bool(live_status_payload.get("canSync"))
        if instance_type == "live":
            should_sync_live = can_sync_live
        else:
            should_sync_live = can_sync_live and bool(
                reset_live_session
                or settings.get("liveTrading", {}).get("enabled")
                or live_has_local_state
            )

    if should_sync_live:
        live_sync_attempted = True
        try:
            state["live"], live_sync_warnings, live_status_payload, live_config = sync_live_book(state["live"], settings, instance_id)
        except Exception as error:
            live_sync_warnings = [f"Live account sync after settings save failed: {error}"]

    write_trading_state(state, instance_id)
    return {
        "state": state,
        "liveSyncWarnings": live_sync_warnings,
        "liveStatus": live_status_payload,
        "liveConfig": live_config,
        "liveSyncAttempted": live_sync_attempted,
    }


def apply_live_position_action(
    book: dict[str, Any],
    position: dict[str, Any],
    action: dict[str, Any],
    decision_id: str,
    status: dict[str, Any],
    live_config: dict[str, Any],
    settings: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[str], list[dict[str, Any]]]:
    actions: list[dict[str, Any]] = []
    warnings: list[str] = []
    execution_events: list[dict[str, Any]] = []
    exchange_id = str(live_config.get("exchange") or "").strip().lower()
    decision = action["decision"]
    mark_price = num(position.get("lastMarkPrice")) or num(position.get("entryPrice")) or 0
    if decision in {"close", "reduce"} and not status["canExecute"]:
        warnings.append(f"Live execution skipped for {position['symbol']}: real execution is not enabled.")
        return book, actions, warnings, execution_events
    if decision == "close":
        cancel_result = cancel_all_open_orders(live_config, position["symbol"])
        append_execution_event(
            execution_events,
            decision_id=decision_id,
            stage="cancel_open_orders",
            action_type="close",
            symbol=position["symbol"],
            side=position["side"],
            exchange=exchange_id,
            requested={"reason": "close_before_market_order"},
            exchange_result=cancel_result,
        )
        order_side = "SELL" if position["side"] == "long" else "BUY"
        order_result = place_market_order(live_config, symbol=position["symbol"], side=order_side, quantity=position["quantity"], reduce_only=True)
        append_execution_event(
            execution_events,
            decision_id=decision_id,
            stage="market_order",
            action_type="close",
            symbol=position["symbol"],
            side=position["side"],
            exchange=exchange_id,
            requested={"orderSide": order_side, "quantity": position["quantity"], "reduceOnly": True},
            exchange_result=order_result,
        )
        book, recorded = close_position(book, position, mark_price, decision_id, action["reason"] or "model_close")
        recorded["exchange"] = True
        actions.append(recorded)
        return book, actions, warnings, execution_events
    if decision == "reduce":
        position_qty = num(position.get("quantity")) or 0
        close_qty = position_qty * action["reduceFraction"]
        order_side = "SELL" if position["side"] == "long" else "BUY"
        existing_stop_loss = num(position.get("stopLoss"))
        existing_take_profit = num(position.get("takeProfit"))
        existing_take_profit_fraction = num(position.get("takeProfitFraction"))
        try:
            normalized_qty = normalize_quantity(live_config, position["symbol"], quantity=close_qty, reference_price=mark_price)
        except Exception as error:
            warnings.append(f"Live reduce skipped for {position['symbol']}: {error}")
            append_execution_event(
                execution_events,
                decision_id=decision_id,
                stage="normalize_quantity",
                action_type="reduce",
                symbol=position["symbol"],
                side=position["side"],
                exchange=exchange_id,
                requested={"requestedQuantity": close_qty, "reduceFraction": action["reduceFraction"], "referencePrice": mark_price},
                success=False,
                error=error,
            )
            return book, actions, warnings, execution_events
        try:
            cancel_result = cancel_all_open_orders(live_config, position["symbol"])
            append_execution_event(
                execution_events,
                decision_id=decision_id,
                stage="cancel_open_orders",
                action_type="reduce",
                symbol=position["symbol"],
                side=position["side"],
                exchange=exchange_id,
                requested={"reason": "reduce_before_market_order"},
                exchange_result=cancel_result,
            )
            order_result = place_market_order(live_config, symbol=position["symbol"], side=order_side, quantity=normalized_qty, reduce_only=True)
            append_execution_event(
                execution_events,
                decision_id=decision_id,
                stage="market_order",
                action_type="reduce",
                symbol=position["symbol"],
                side=position["side"],
                exchange=exchange_id,
                requested={
                    "orderSide": order_side,
                    "quantity": normalized_qty,
                    "reduceOnly": True,
                    "requestedQuantity": close_qty,
                    "requestedReduceFraction": action["reduceFraction"],
                },
                exchange_result=order_result,
            )
        except Exception as error:
            warnings.append(f"Live reduce failed for {position['symbol']}: {error}")
            append_execution_event(
                execution_events,
                decision_id=decision_id,
                stage="market_order",
                action_type="reduce",
                symbol=position["symbol"],
                side=position["side"],
                exchange=exchange_id,
                requested={"orderSide": order_side, "quantity": normalized_qty, "reduceOnly": True},
                success=False,
                error=error,
            )
            if settings["liveExecution"]["useExchangeProtectionOrders"] and (existing_stop_loss is not None or existing_take_profit is not None):
                try:
                    restore_result = place_protection_orders(
                        live_config,
                        symbol=position["symbol"],
                        position_side=position["side"],
                        quantity=num(position.get("quantity")),
                        stop_loss=existing_stop_loss,
                        take_profit=existing_take_profit,
                        take_profit_fraction=existing_take_profit_fraction,
                    )
                    append_execution_event(
                        execution_events,
                        decision_id=decision_id,
                        stage="protection_orders_restore",
                        action_type="reduce_restore_after_failure",
                        symbol=position["symbol"],
                        side=position["side"],
                        exchange=exchange_id,
                        requested={
                            "quantity": num(position.get("quantity")),
                            "stopLoss": existing_stop_loss,
                            "takeProfit": existing_take_profit,
                            "takeProfitFraction": existing_take_profit_fraction,
                        },
                        exchange_result=restore_result,
                    )
                except Exception as restore_error:
                    warnings.append(f"Exchange protection restore failed for {position['symbol']}: {restore_error}")
                    append_execution_event(
                        execution_events,
                        decision_id=decision_id,
                        stage="protection_orders_restore",
                        action_type="reduce_restore_after_failure",
                        symbol=position["symbol"],
                        side=position["side"],
                        exchange=exchange_id,
                        requested={
                            "quantity": num(position.get("quantity")),
                            "stopLoss": existing_stop_loss,
                            "takeProfit": existing_take_profit,
                            "takeProfitFraction": existing_take_profit_fraction,
                        },
                        success=False,
                        error=restore_error,
                    )
            return book, actions, warnings, execution_events
        actual_reduce_fraction = normalized_qty / position_qty if position_qty > 0 else action["reduceFraction"]
        book, recorded = reduce_position(book, position, mark_price, actual_reduce_fraction, decision_id, action["reason"] or "model_reduce")
        if recorded:
            recorded["exchange"] = True
            actions.append(recorded)
        remaining_position = next((item for item in book.get("openPositions", []) if item["id"] == position["id"]), None)
        if remaining_position:
            next_stop_loss = num(action.get("stopLoss")) if action.get("stopLoss") is not None else existing_stop_loss
            next_take_profit = num(action.get("takeProfit"))
            next_take_profit_fraction = num(action.get("takeProfitFraction"))
            protection_valid = True
            if settings["liveExecution"]["useExchangeProtectionOrders"] and (next_stop_loss is not None or next_take_profit is not None):
                reference_price = num(remaining_position.get("lastMarkPrice")) or mark_price
                (
                    next_stop_loss,
                    next_take_profit,
                    next_take_profit_fraction,
                    protection_valid,
                    protection_warnings,
                ) = _validate_live_protection_inputs(
                    symbol=position["symbol"],
                    side=position["side"],
                    reference_price=reference_price,
                    stop_loss=next_stop_loss,
                    take_profit=next_take_profit,
                    take_profit_fraction=next_take_profit_fraction,
                )
                warnings.extend(protection_warnings)
            for current in book.get("openPositions", []):
                if current["id"] != position["id"]:
                    continue
                current["stopLoss"] = next_stop_loss if protection_valid else existing_stop_loss
                current["takeProfit"] = next_take_profit if protection_valid else existing_take_profit
                current["takeProfitFraction"] = next_take_profit_fraction if protection_valid else existing_take_profit_fraction
                current["updatedAt"] = now_iso()
                remaining_position = current
                break
            if (
                protection_valid
                and settings["liveExecution"]["useExchangeProtectionOrders"]
                and (next_stop_loss is not None or next_take_profit is not None)
            ):
                warnings.extend(
                    _place_live_protection_orders_with_fallback(
                        live_config,
                        symbol=position["symbol"],
                        position_side=position["side"],
                        quantity=num(remaining_position.get("quantity")),
                        stop_loss=next_stop_loss,
                        take_profit=next_take_profit,
                        take_profit_fraction=next_take_profit_fraction,
                        warning_prefix="Exchange protection order update failed",
                        decision_id=decision_id,
                        action_type="reduce_protection_update",
                        execution_events=execution_events,
                    )
                )
        return book, actions, warnings, execution_events
    if decision in {"hold", "update"}:
        stop_loss = action.get("stopLoss")
        take_profit = action.get("takeProfit")
        take_profit_fraction = num(action.get("takeProfitFraction"))
        if take_profit_fraction is not None:
            take_profit_fraction = max(0.05, min(1.0, take_profit_fraction))
        if stop_loss is None and take_profit is None and take_profit_fraction is None:
            return book, actions, warnings, execution_events
        if (
            take_profit is not None
            and take_profit_fraction is not None
            and take_profit_fraction < 1.0
            and _take_profit_reached_for_side(position["side"], mark_price, take_profit)
        ):
            if not status["canExecute"]:
                warnings.append(f"Live partial take-profit skipped for {position['symbol']}: real execution is not enabled.")
                return book, actions, warnings, execution_events
            existing_stop_loss = num(position.get("stopLoss"))
            existing_take_profit = num(position.get("takeProfit"))
            existing_take_profit_fraction = num(position.get("takeProfitFraction"))
            next_stop_loss = stop_loss if _stop_valid_for_side(position["side"], mark_price, stop_loss) else existing_stop_loss
            position_qty = num(position.get("quantity")) or 0
            close_qty = position_qty * take_profit_fraction
            try:
                normalized_qty = normalize_quantity(live_config, position["symbol"], quantity=close_qty, reference_price=mark_price)
            except Exception as error:
                warnings.append(f"Live partial take-profit skipped for {position['symbol']}: {error}")
                append_execution_event(
                    execution_events,
                    decision_id=decision_id,
                    stage="normalize_quantity",
                    action_type="partial_take_profit",
                    symbol=position["symbol"],
                    side=position["side"],
                    exchange=exchange_id,
                    requested={"requestedQuantity": close_qty, "takeProfitFraction": take_profit_fraction, "referencePrice": mark_price},
                    success=False,
                    error=error,
                )
                return book, actions, warnings, execution_events
            try:
                order_side = "SELL" if position["side"] == "long" else "BUY"
                cancel_result = cancel_all_open_orders(live_config, position["symbol"])
                append_execution_event(
                    execution_events,
                    decision_id=decision_id,
                    stage="cancel_open_orders",
                    action_type="partial_take_profit",
                    symbol=position["symbol"],
                    side=position["side"],
                    exchange=exchange_id,
                    requested={"reason": "partial_take_profit_before_market_order"},
                    exchange_result=cancel_result,
                )
                order_result = place_market_order(live_config, symbol=position["symbol"], side=order_side, quantity=normalized_qty, reduce_only=True)
                append_execution_event(
                    execution_events,
                    decision_id=decision_id,
                    stage="market_order",
                    action_type="partial_take_profit",
                    symbol=position["symbol"],
                    side=position["side"],
                    exchange=exchange_id,
                    requested={
                        "orderSide": order_side,
                        "quantity": normalized_qty,
                        "reduceOnly": True,
                        "requestedQuantity": close_qty,
                        "takeProfit": take_profit,
                        "takeProfitFraction": take_profit_fraction,
                    },
                    exchange_result=order_result,
                )
            except Exception as error:
                warnings.append(f"Live partial take-profit failed for {position['symbol']}: {error}")
                append_execution_event(
                    execution_events,
                    decision_id=decision_id,
                    stage="market_order",
                    action_type="partial_take_profit",
                    symbol=position["symbol"],
                    side=position["side"],
                    exchange=exchange_id,
                    requested={"orderSide": order_side, "quantity": normalized_qty, "reduceOnly": True},
                    success=False,
                    error=error,
                )
                if settings["liveExecution"]["useExchangeProtectionOrders"] and (existing_stop_loss is not None or existing_take_profit is not None):
                    try:
                        restore_result = place_protection_orders(
                            live_config,
                            symbol=position["symbol"],
                            position_side=position["side"],
                            quantity=num(position.get("quantity")),
                            stop_loss=existing_stop_loss,
                            take_profit=existing_take_profit,
                            take_profit_fraction=existing_take_profit_fraction,
                        )
                        append_execution_event(
                            execution_events,
                            decision_id=decision_id,
                            stage="protection_orders_restore",
                            action_type="partial_take_profit_restore_after_failure",
                            symbol=position["symbol"],
                            side=position["side"],
                            exchange=exchange_id,
                            requested={
                                "quantity": num(position.get("quantity")),
                                "stopLoss": existing_stop_loss,
                                "takeProfit": existing_take_profit,
                                "takeProfitFraction": existing_take_profit_fraction,
                            },
                            exchange_result=restore_result,
                        )
                    except Exception as restore_error:
                        warnings.append(f"Exchange protection restore failed for {position['symbol']}: {restore_error}")
                        append_execution_event(
                            execution_events,
                            decision_id=decision_id,
                            stage="protection_orders_restore",
                            action_type="partial_take_profit_restore_after_failure",
                            symbol=position["symbol"],
                            side=position["side"],
                            exchange=exchange_id,
                            requested={
                                "quantity": num(position.get("quantity")),
                                "stopLoss": existing_stop_loss,
                                "takeProfit": existing_take_profit,
                                "takeProfitFraction": existing_take_profit_fraction,
                            },
                            success=False,
                            error=restore_error,
                        )
                return book, actions, warnings, execution_events
            actual_take_profit_fraction = normalized_qty / position_qty if position_qty > 0 else take_profit_fraction
            book, recorded = reduce_position(book, position, mark_price, actual_take_profit_fraction, decision_id, action["reason"] or "model_partial_take_profit")
            if recorded:
                recorded["exchange"] = True
                actions.append(recorded)
            remaining_position = next((item for item in book.get("openPositions", []) if item["id"] == position["id"]), None)
            if remaining_position:
                for current in book.get("openPositions", []):
                    if current["id"] != position["id"]:
                        continue
                    current["stopLoss"] = next_stop_loss
                    current["takeProfit"] = None
                    current["takeProfitFraction"] = None
                    current["updatedAt"] = now_iso()
                    remaining_position = current
                    break
                if settings["liveExecution"]["useExchangeProtectionOrders"] and next_stop_loss is not None:
                    (
                        next_stop_loss,
                        _,
                        _,
                        protection_valid,
                        protection_warnings,
                    ) = _validate_live_protection_inputs(
                        symbol=position["symbol"],
                        side=position["side"],
                        reference_price=num(remaining_position.get("lastMarkPrice")) or mark_price,
                        stop_loss=next_stop_loss,
                        take_profit=None,
                        take_profit_fraction=None,
                    )
                    warnings.extend(protection_warnings)
                    if protection_valid:
                        warnings.extend(
                            _place_live_protection_orders_with_fallback(
                                live_config,
                                symbol=position["symbol"],
                                position_side=position["side"],
                                quantity=num(remaining_position.get("quantity")),
                                stop_loss=next_stop_loss,
                                take_profit=None,
                                take_profit_fraction=None,
                                warning_prefix="Exchange protection order update failed",
                                decision_id=decision_id,
                                action_type="partial_take_profit_protection_update",
                                execution_events=execution_events,
                            )
                        )
            return book, actions, warnings, execution_events
        (
            stop_loss,
            take_profit,
            take_profit_fraction,
            protection_valid,
            protection_warnings,
        ) = _validate_live_protection_inputs(
            symbol=position["symbol"],
            side=position["side"],
            reference_price=mark_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            take_profit_fraction=take_profit_fraction,
        )
        warnings.extend(protection_warnings)
        if not protection_valid:
            warnings.append(f"Ignored invalid live protection update for {position['symbol']}.")
            append_execution_event(
                execution_events,
                decision_id=decision_id,
                stage="protection_validation",
                action_type="protection_update",
                symbol=position["symbol"],
                side=position["side"],
                exchange=exchange_id,
                requested={"stopLoss": stop_loss, "takeProfit": take_profit, "takeProfitFraction": take_profit_fraction},
                success=False,
                error="invalid live protection update",
            )
            return book, actions, warnings, execution_events
        for current in book.get("openPositions", []):
            if current["id"] != position["id"]:
                continue
            current["stopLoss"] = stop_loss
            current["takeProfit"] = take_profit
            current["takeProfitFraction"] = take_profit_fraction
            current["updatedAt"] = now_iso()
            break
        if status["canExecute"] and settings["liveExecution"]["useExchangeProtectionOrders"]:
            cancel_result = cancel_all_open_orders(live_config, position["symbol"])
            append_execution_event(
                execution_events,
                decision_id=decision_id,
                stage="cancel_open_orders",
                action_type="protection_update",
                symbol=position["symbol"],
                side=position["side"],
                exchange=exchange_id,
                requested={"reason": "replace_protection_for_model_update"},
                exchange_result=cancel_result,
            )
            warnings.extend(
                _place_live_protection_orders_with_fallback(
                    live_config,
                    symbol=position["symbol"],
                    position_side=position["side"],
                    quantity=num(position.get("quantity")),
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    take_profit_fraction=take_profit_fraction,
                    warning_prefix="Exchange protection order update failed",
                    decision_id=decision_id,
                    action_type="protection_update",
                    execution_events=execution_events,
                )
            )
        actions.append(
            {
                "type": "update",
                "symbol": position["symbol"],
                "side": position["side"],
                "stopLoss": stop_loss,
                "takeProfit": take_profit,
                "takeProfitFraction": take_profit_fraction,
                "reason": action["reason"] or "model_update",
                "label": action_label("update", position["symbol"]),
            }
        )
    return book, actions, warnings, execution_events


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
    take_profit_fraction = num(action.get("takeProfitFraction"))
    if take_profit_fraction is not None:
        take_profit_fraction = max(0.05, min(1.0, take_profit_fraction))
    if decision in {"hold", "update"} and (stop_loss is not None or take_profit is not None or take_profit_fraction is not None):
        if (
            take_profit is not None
            and take_profit_fraction is not None
            and take_profit_fraction < 1.0
            and _take_profit_reached_for_side(position["side"], mark_price, take_profit)
        ):
            book, recorded = reduce_position(book, position, mark_price, take_profit_fraction, decision_id, action["reason"] or "model_partial_take_profit")
            if recorded:
                actions.append(recorded)
            remaining_position = next((item for item in book.get("openPositions", []) if item["id"] == position["id"]), None)
            if remaining_position:
                next_stop_loss = stop_loss if _stop_valid_for_side(position["side"], mark_price, stop_loss) else num(position.get("stopLoss"))
                for current in book.get("openPositions", []):
                    if current["id"] != position["id"]:
                        continue
                    current["stopLoss"] = next_stop_loss
                    current["takeProfit"] = None
                    current["takeProfitFraction"] = None
                    current["updatedAt"] = now_iso()
                    break
            return book, actions, warnings
        if not _risk_valid_for_side(position["side"], mark_price, stop_loss, take_profit):
            warnings.append(f"Ignored invalid risk update for {position['symbol']}.")
            return book, actions, warnings
        for current in book.get("openPositions", []):
            if current["id"] != position["id"]:
                continue
            current["stopLoss"] = stop_loss
            current["takeProfit"] = take_profit
            current["takeProfitFraction"] = take_profit_fraction
            current["updatedAt"] = now_iso()
            break
        actions.append(
            {
                "type": "update",
                "symbol": position["symbol"],
                "side": position["side"],
                "stopLoss": stop_loss,
                "takeProfit": take_profit,
                "takeProfitFraction": take_profit_fraction,
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
) -> tuple[dict[str, Any], list[dict[str, Any]], list[str], list[dict[str, Any]]]:
    account = summarize_account(book, settings)
    if account["drawdownPct"] < settings["maxAccountDrawdownPct"]:
        book["circuitBreakerTripped"] = False
        book["circuitBreakerReason"] = None
        return book, [], [], []
    book["circuitBreakerTripped"] = True
    book["circuitBreakerReason"] = f"Drawdown {account['drawdownPct']:.2f}% breached max {settings['maxAccountDrawdownPct']:.2f}%."
    actions: list[dict[str, Any]] = []
    warnings: list[str] = []
    execution_events: list[dict[str, Any]] = []
    exchange_id = str((live_config or {}).get("exchange") or "").strip().lower()
    for position in list(book.get("openPositions", [])):
        if live_mode:
            if not live_status_payload or not live_status_payload.get("canExecute"):
                warnings.append(f"Circuit breaker could not close live {position['symbol']} because real execution is not enabled.")
                continue
            cancel_result = cancel_all_open_orders(live_config, position["symbol"])
            append_execution_event(
                execution_events,
                decision_id=decision_id,
                stage="cancel_open_orders",
                action_type="circuit_breaker",
                symbol=position["symbol"],
                side=position["side"],
                exchange=exchange_id,
                requested={"reason": "circuit_breaker"},
                exchange_result=cancel_result,
            )
            order_side = "SELL" if position["side"] == "long" else "BUY"
            order_result = place_market_order(live_config, symbol=position["symbol"], side=order_side, quantity=position["quantity"], reduce_only=True)
            append_execution_event(
                execution_events,
                decision_id=decision_id,
                stage="market_order",
                action_type="circuit_breaker",
                symbol=position["symbol"],
                side=position["side"],
                exchange=exchange_id,
                requested={"orderSide": order_side, "quantity": position["quantity"], "reduceOnly": True},
                exchange_result=order_result,
            )
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
    return book, actions, warnings, execution_events


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


def run_trading_cycle(reason: str = "manual", mode_override: str | None = None, instance_id: str | None = None) -> dict[str, Any]:
    settings = read_trading_settings(instance_id)
    settings["mode"] = clean_mode(mode_override or settings["mode"])
    universe = read_fixed_universe(instance_id)
    account_key = account_key_for_mode(settings["mode"])
    cycle_exchange_id = str(settings.get("activeExchange") or "binance").strip().lower() or "binance"
    live_config = None
    if account_key == "live":
        live_config = read_live_trading_config(instance_id)
        cycle_exchange_id = str(live_config.get("exchange") or cycle_exchange_id).strip().lower() or cycle_exchange_id
    scan = read_latest_scan(cycle_exchange_id, instance_id)
    if universe.get("dynamicSource", {}).get("enabled") or not scan["opportunities"] or str(scan.get("exchange") or "").strip().lower() != cycle_exchange_id:
        scan = refresh_candidate_pool(cycle_exchange_id, instance_id)
    state = read_trading_state(settings, instance_id)
    book = state[account_key]
    if account_key != "live":
        book["initialCapitalUsd"] = settings["initialCapitalUsd"]
    book.setdefault("sessionStartedAt", book.get("sessionStartedAt") or now_iso())
    decision_id = f"trade-cycle-{int(__import__('time').time() * 1000)}"
    warnings: list[str] = []
    execution_events: list[dict[str, Any]] = []
    live_status_payload = None
    if account_key == "live":
        book, live_warnings, live_status_payload, live_config = sync_live_book(book, settings, instance_id)
        warnings.extend(live_warnings)
        state["live"] = book
        active_cooldown = (live_status_payload or {}).get("cooldown") or cooldown_status(cycle_exchange_id)
        if active_cooldown.get("active"):
            account_snapshot = summarize_account(book, settings)
            provider = read_llm_provider(instance_id)
            summary = f"Skipped live cycle because {str(cycle_exchange_id).upper()} API cooldown is active."
            book["lastDecisionAt"] = now_iso()
            warnings = dedupe_messages(warnings + [active_cooldown.get("message") or summary])
            decision = normalize_decision(
                {
                    "id": decision_id,
                    "startedAt": now_iso(),
                    "finishedAt": now_iso(),
                    "runnerReason": reason,
                    "mode": settings["mode"],
                    "prompt": "",
                    "promptSummary": summary,
                    "output": {
                        "summary": summary,
                        "positionActions": [],
                        "entryActions": [],
                        "watchlist": [],
                        "providerStatus": provider_status(provider),
                        "liveExecutionStatus": live_status_payload,
                    },
                    "rawModelResponse": {},
                    "actions": [],
                    "executionEvents": [],
                    "warnings": warnings,
                    "candidateUniverse": [],
                    "accountBefore": account_snapshot,
                    "accountAfter": account_snapshot,
                }
            )
            book.setdefault("decisions", []).append(decision)
            state["adaptive"] = {
                "updatedAt": now_iso(),
                "notes": [
                    summary,
                    "Live exchange requests are paused during cooldown to avoid extending the ban window.",
                    "Paper instances can keep using cached/public data while live signed requests wait.",
                ],
            }
            write_trading_state(state, instance_id)
            archive_decision(decision, instance_id)
            return {
                "settings": settings,
                "state": state,
                "decision": decision,
                "marketBackdrop": {},
                "liveExecutionStatus": live_status_payload,
                "instanceId": instance_id,
            }
    prompt_settings = read_prompt_settings(instance_id)
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
    trailing_actions, trailing_warnings, trailing_execution_events = apply_trailing_profit_stops(
        book,
        live_mode=account_key == "live",
        decision_id=decision_id,
        status=live_status_payload,
        live_config=live_config,
        settings=settings,
    )
    warnings.extend(trailing_warnings)
    execution_events.extend(trailing_execution_events)
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
    provider = read_llm_provider(instance_id)
    historical_lessons = historical_lessons_for_prompt(
        instance_id=instance_id,
        settings=settings,
        candidates=candidate_snapshots,
    )
    prompt = build_prompt(
        settings=settings,
        prompt_settings=prompt_settings,
        provider=provider,
        market_backdrop=market_backdrop,
        account_summary=account_before,
        open_positions=account_before["openPositions"],
        open_orders=[normalize_order(item) for item in book.get("openOrders", [])],
        candidates=candidate_snapshots,
        historical_lessons=historical_lessons,
    )
    model_result: dict[str, Any] | None = None
    try:
        model_result = generate_trading_decision(prompt, provider, read_network_settings(instance_id))
        parsed_model = normalize_model_decision(
            model_result["parsed"],
            open_positions=account_before["openPositions"],
            candidates_by_symbol=candidates_by_symbol,
        )
    except ModelDecisionParseError as error:
        warnings.append(f"Model decision failed: {error}")
        model_result = {
            "provider": error.provider_result,
            "rawText": error.raw_text,
            "rawResponse": error.raw_response,
            "parsed": {},
        }
        parsed_model = default_model_decision(account_before["openPositions"])
    except Exception as error:
        warnings.append(f"Model decision failed: {error}")
        parsed_model = default_model_decision(account_before["openPositions"])
    management_actions = list(protection_actions) + trailing_actions
    for instruction in parsed_model["position_actions"]:
        position = next((item for item in list(book.get("openPositions", [])) if item["symbol"] == instruction["symbol"]), None)
        if not position:
            continue
        if account_key == "live":
            book, applied_actions, applied_warnings, applied_execution_events = apply_live_position_action(
                book,
                position,
                instruction,
                decision_id,
                live_status_payload or {"canExecute": False},
                live_config or read_live_trading_config(instance_id),
                settings,
            )
            execution_events.extend(applied_execution_events)
        else:
            book, applied_actions, applied_warnings = apply_paper_position_action(
                book,
                position,
                instruction,
                decision_id,
            )
        management_actions.extend(applied_actions)
        warnings.extend(applied_warnings)
    book, breaker_actions, breaker_warnings, breaker_execution_events = apply_account_circuit_breaker(
        book,
        settings,
        decision_id,
        live_mode=account_key == "live",
        live_status_payload=live_status_payload,
        live_config=live_config,
    )
    warnings.extend(breaker_warnings)
    execution_events.extend(breaker_execution_events)
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
            take_profit_fraction = num(entry.get("takeProfitFraction"))
            if take_profit_fraction is not None:
                take_profit_fraction = max(0.05, min(1.0, take_profit_fraction))
            if entry_price <= 0 or stop_loss is None:
                continue
            if not _risk_valid_for_side(side, entry_price, stop_loss, take_profit):
                warnings.append(f"Ignored invalid entry risk for {entry['symbol']}.")
                continue
            reward_r = _reward_r_multiple(side, entry_price, stop_loss, take_profit)
            if reward_r is not None and reward_r < 1.0:
                warnings.append(
                    f"Skipped entry {entry['symbol']}: first take-profit is only {reward_r:.2f}R versus stop risk."
                )
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
                live_config = live_config or read_live_trading_config(instance_id)
                exchange_id = str(live_config.get("exchange") or "").strip().lower()
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
                    append_execution_event(
                        execution_events,
                        decision_id=decision_id,
                        stage="symbol_settings",
                        action_type="open",
                        symbol=entry["symbol"],
                        side=side,
                        exchange=exchange_id,
                        requested={
                            "leverage": live_config.get("defaultLeverage"),
                            "marginType": live_config.get("marginType"),
                        },
                    )
                except Exception as error:
                    warnings.append(f"Live symbol settings update skipped for {entry['symbol']}: {error}")
                    append_execution_event(
                        execution_events,
                        decision_id=decision_id,
                        stage="symbol_settings",
                        action_type="open",
                        symbol=entry["symbol"],
                        side=side,
                        exchange=exchange_id,
                        requested={
                            "leverage": live_config.get("defaultLeverage"),
                            "marginType": live_config.get("marginType"),
                        },
                        success=False,
                        error=error,
                    )
                quantity = normalize_quantity(live_config, entry["symbol"], notional_usd=notional_usd, reference_price=entry_price)
                order_side = "BUY" if side == "long" else "SELL"
                order_result = place_market_order(live_config, symbol=entry["symbol"], side=order_side, quantity=quantity)
                append_execution_event(
                    execution_events,
                    decision_id=decision_id,
                    stage="market_order",
                    action_type="open",
                    symbol=entry["symbol"],
                    side=side,
                    exchange=exchange_id,
                    requested={
                        "orderSide": order_side,
                        "quantity": quantity,
                        "reduceOnly": False,
                        "candidatePrice": entry_price,
                        "notionalUsd": notional_usd,
                    },
                    exchange_result=order_result,
                )
                live_entry_price = _execution_price_from_order_result(order_result, entry_price)
                (
                    stop_loss,
                    take_profit,
                    take_profit_fraction,
                    protection_valid,
                    protection_warnings,
                ) = _validate_live_protection_inputs(
                    symbol=entry["symbol"],
                    side=side,
                    reference_price=live_entry_price,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    take_profit_fraction=take_profit_fraction,
                )
                warnings.extend(protection_warnings)
                if not protection_valid:
                    try:
                        cancel_all_open_orders(live_config, entry["symbol"])
                        close_side = "SELL" if side == "long" else "BUY"
                        guard_result = place_market_order(live_config, symbol=entry["symbol"], side=close_side, quantity=quantity, reduce_only=True)
                        append_execution_event(
                            execution_events,
                            decision_id=decision_id,
                            stage="market_order",
                            action_type="entry_guard_close",
                            symbol=entry["symbol"],
                            side=side,
                            exchange=exchange_id,
                            requested={"orderSide": close_side, "quantity": quantity, "reduceOnly": True},
                            exchange_result=guard_result,
                        )
                        warnings.append(
                            f"Live entry guard closed {entry['symbol']}: fill price invalidated the requested protection."
                        )
                    except Exception as guard_error:
                        warnings.append(f"Live entry guard failed for {entry['symbol']}: {guard_error}")
                        append_execution_event(
                            execution_events,
                            decision_id=decision_id,
                            stage="market_order",
                            action_type="entry_guard_close",
                            symbol=entry["symbol"],
                            side=side,
                            exchange=exchange_id,
                            requested={"quantity": quantity, "reduceOnly": True},
                            success=False,
                            error=guard_error,
                        )
                    continue
                if settings["liveExecution"]["useExchangeProtectionOrders"]:
                    cancel_result = cancel_all_open_orders(live_config, entry["symbol"])
                    append_execution_event(
                        execution_events,
                        decision_id=decision_id,
                        stage="cancel_open_orders",
                        action_type="open_protection_setup",
                        symbol=entry["symbol"],
                        side=side,
                        exchange=exchange_id,
                        requested={"reason": "replace_protection_after_open"},
                        exchange_result=cancel_result,
                    )
                    warnings.extend(
                        _place_live_protection_orders_with_fallback(
                            live_config,
                            symbol=entry["symbol"],
                            position_side=side,
                            quantity=quantity,
                            stop_loss=stop_loss,
                            take_profit=take_profit,
                            take_profit_fraction=take_profit_fraction,
                            warning_prefix="Exchange protection order placement failed",
                            decision_id=decision_id,
                            action_type="open_protection_setup",
                            execution_events=execution_events,
                        )
                    )
            else:
                open_paper_position(
                    book,
                    candidate=candidate,
                    side=side,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    take_profit_fraction=take_profit_fraction,
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
                    "takeProfitFraction": take_profit_fraction,
                    "reason": entry["reason"],
                    "label": action_label("open", entry["symbol"], side),
                }
            )
            account_after_management = summarize_account(book, settings)
    if account_key == "live":
        book, live_warnings, live_status_payload, live_config = sync_live_book(book, settings, instance_id)
        warnings.extend(live_warnings)
        state["live"] = book
    else:
        state["paper"] = book
    account_after = summarize_account(book, settings)
    if account_after["equityUsd"] > (num(book.get("highWatermarkEquity")) or book["initialCapitalUsd"]):
        book["highWatermarkEquity"] = account_after["equityUsd"]
    book["lastDecisionAt"] = now_iso()
    warnings = dedupe_messages(warnings)
    execution_events = [normalize_execution_event(item) for item in execution_events if isinstance(item, dict)]
    if account_key == "live" and execution_events:
        book.setdefault("executionEvents", []).extend(execution_events)
    output_payload = {
        "summary": parsed_model.get("summary"),
        "positionActions": parsed_model["position_actions"],
        "entryActions": parsed_model["entry_actions"],
        "watchlist": parsed_model["watchlist"],
        "providerStatus": provider_status(provider),
        "liveExecutionStatus": live_status_payload,
    }
    if historical_lessons:
        output_payload["historicalLessons"] = historical_lessons
    decision = normalize_decision(
        {
            "id": decision_id,
            "startedAt": now_iso(),
            "finishedAt": now_iso(),
            "runnerReason": reason,
            "mode": settings["mode"],
            "prompt": prompt,
            "promptSummary": one_line(parsed_model.get("summary") or f"Managed {len(account_before['openPositions'])} positions and reviewed {len(candidate_snapshots)} candidates."),
            "output": output_payload,
            "rawModelResponse": model_result or {},
            "actions": management_actions + breaker_actions + entry_actions,
            "executionEvents": execution_events,
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
    write_trading_state(state, instance_id)
    archive_decision(decision, instance_id)
    return {
        "settings": settings,
        "state": state,
        "decision": decision,
        "marketBackdrop": market_backdrop,
        "liveExecutionStatus": live_status_payload,
        "instanceId": instance_id,
    }


def run_trading_cycle_batch(reason: str = "manual", modes: list[str] | None = None, instance_id: str | None = None) -> dict[str, Any]:
    settings = read_trading_settings(instance_id)
    default_modes = modes or [clean_mode(settings["mode"])]
    requested_modes = [clean_mode(item) for item in default_modes]
    unique_modes: list[str] = []
    for mode in requested_modes:
        if mode not in unique_modes:
            unique_modes.append(mode)
    results = []
    for mode in unique_modes:
        result = run_trading_cycle(reason=reason, mode_override=mode, instance_id=instance_id)
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
        "instanceId": instance_id,
    }


def preview_trading_prompt_decision(mode_override: str | None = None, prompt_override: dict[str, Any] | None = None, instance_id: str | None = None) -> dict[str, Any]:
    settings = read_trading_settings(instance_id)
    settings["mode"] = clean_mode(mode_override or settings["mode"])
    universe = read_fixed_universe(instance_id)
    account_key = account_key_for_mode(settings["mode"])
    cycle_exchange_id = str(settings.get("activeExchange") or "binance").strip().lower() or "binance"
    if account_key == "live":
        live_config = read_live_trading_config(instance_id)
        cycle_exchange_id = str(live_config.get("exchange") or cycle_exchange_id).strip().lower() or cycle_exchange_id
    scan = read_latest_scan(cycle_exchange_id, instance_id)
    if universe.get("dynamicSource", {}).get("enabled") or not scan["opportunities"] or str(scan.get("exchange") or "").strip().lower() != cycle_exchange_id:
        scan = refresh_candidate_pool(cycle_exchange_id, instance_id)
    state = read_trading_state(settings, instance_id)
    book = deepcopy(state[account_key])
    warnings: list[str] = []
    if account_key == "live":
        book, live_warnings, _, _ = sync_live_book(book, settings, instance_id)
        warnings.extend(live_warnings)
    prompt_settings = prompt_override or read_prompt_settings(instance_id)
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
    provider = read_llm_provider(instance_id)
    historical_lessons = historical_lessons_for_prompt(
        instance_id=instance_id,
        settings=settings,
        candidates=candidate_snapshots,
    )
    prompt = build_prompt(
        settings=settings,
        prompt_settings=prompt_settings,
        provider=provider,
        market_backdrop=market_backdrop,
        account_summary=account_summary,
        open_positions=account_summary["openPositions"],
        open_orders=[normalize_order(item) for item in book.get("openOrders", [])],
        candidates=candidate_snapshots,
        historical_lessons=historical_lessons,
    )
    model_result = generate_trading_decision(prompt, provider, read_network_settings(instance_id))
    parsed_model = normalize_model_decision(
        model_result["parsed"],
        open_positions=account_summary["openPositions"],
        candidates_by_symbol=candidates_by_symbol,
    )
    warnings = dedupe_messages(warnings)
    result = {
        "mode": settings["mode"],
        "promptName": prompt_settings.get("name") or "default_trading_logic",
        "candidateCount": len(candidate_snapshots),
        "account": account_summary,
        "warnings": warnings,
        "prompt": prompt,
        "rawText": model_result["rawText"],
        "parsed": parsed_model,
        "provider": model_result["provider"],
        "instanceId": instance_id,
    }
    if historical_lessons:
        result["historicalLessons"] = historical_lessons
    return result
def _parse_iso_timestamp(value: Any) -> float | None:
    if not value:
        return None
    try:
        return __import__("datetime").datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def archived_decision_timeline(mode: str, session_started_at: str | None, limit: int = 1000, instance_id: str | None = None) -> list[dict[str, Any]]:
    decisions_dir = _decisions_dir(instance_id)
    if not decisions_dir.exists():
        return []
    started_ts = _parse_iso_timestamp(session_started_at)
    started_date = None
    if session_started_at:
        try:
            started_date = str(session_started_at)[:10]
        except Exception:
            started_date = None
    rows: list[dict[str, Any]] = []
    for day_dir in sorted(decisions_dir.iterdir()):
        if not day_dir.is_dir():
            continue
        if started_date and day_dir.name < started_date:
            continue
        for path in sorted(day_dir.glob("*.json")):
            payload = read_json(path, {})
            if not isinstance(payload, dict):
                continue
            if clean_mode(payload.get("mode")) != clean_mode(mode):
                continue
            finished_at = payload.get("finishedAt") or payload.get("startedAt")
            finished_ts = _parse_iso_timestamp(finished_at)
            if started_ts is not None and finished_ts is not None and finished_ts < started_ts:
                continue
            rows.append(
                {
                    "id": str(payload.get("id") or path.stem),
                    "startedAt": payload.get("startedAt"),
                    "finishedAt": payload.get("finishedAt"),
                    "actions": payload.get("actions") if isinstance(payload.get("actions"), list) else [],
                    "equityUsd": num(payload.get("accountAfter", {}).get("equityUsd")),
                }
            )
    rows.sort(
        key=lambda item: (
            _parse_iso_timestamp(item.get("finishedAt") or item.get("startedAt")) or 0,
            str(item.get("id") or ""),
        )
    )
    return rows[-limit:]


def summarize_book_history(book: dict[str, Any], mode: str, instance_id: str | None = None) -> dict[str, Any]:
    recent_decisions = list(book.get("decisions", []))[-8:]
    archived_timeline = archived_decision_timeline(mode, book.get("sessionStartedAt"), instance_id=instance_id)
    decision_timeline = archived_timeline or [
        {
            "id": item["id"],
            "startedAt": item["startedAt"],
            "finishedAt": item["finishedAt"],
            "actions": item["actions"],
            "equityUsd": num(item.get("accountAfter", {}).get("equityUsd")),
        }
        for item in book.get("decisions", [])[-240:]
    ]
    return {
        "sessionStartedAt": book.get("sessionStartedAt"),
        "lastDecisionAt": book.get("lastDecisionAt"),
        "decisions": recent_decisions,
        "decisionTimeline": decision_timeline,
        "exchangeClosedTrades": list(book.get("exchangeClosedTrades", [])),
        "executionEvents": list(book.get("executionEvents", []))[-80:],
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


def summarize_trading_state(instance_id: str | None = None, *, include_live_status: bool = True) -> dict[str, Any]:
    settings = read_trading_settings(instance_id)
    state = read_trading_state(settings, instance_id)
    live_status_payload = live_execution_status(read_live_trading_config(instance_id), settings) if include_live_status else None
    scan = read_latest_scan(settings.get("activeExchange"), instance_id)
    exchange_cooldown = cooldown_status(settings.get("activeExchange") or "binance")
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
        "paperHistory": summarize_book_history(state["paper"], "paper", instance_id),
        "liveHistory": summarize_book_history(state["live"], "live", instance_id),
        "liveExecutionStatus": live_status_payload,
        "exchangeCooldown": exchange_cooldown,
        "providerStatus": provider_status(read_llm_provider(instance_id)),
        "instance": read_instance(instance_id) if instance_id else None,
    }


def flatten_active_account(reason: str = "manual_flatten", mode_override: str | None = None, instance_id: str | None = None) -> dict[str, Any]:
    settings = read_trading_settings(instance_id)
    target_mode = clean_mode(mode_override or settings["mode"])
    state = read_trading_state(settings, instance_id)
    account_key = account_key_for_mode(target_mode)
    book = state[account_key]
    decision_id = f"flatten-{int(__import__('time').time() * 1000)}"
    actions = []
    warnings: list[str] = []
    execution_events: list[dict[str, Any]] = []
    if account_key == "live":
        book, live_warnings, live_status_payload, live_config = sync_live_book(book, settings, instance_id)
        warnings.extend(live_warnings)
        if not live_status_payload["canExecute"]:
            raise RuntimeError("Live flatten requires real execution to be enabled.")
        exchange_id = str(live_config.get("exchange") or "").strip().lower()
        for position in list(book.get("openPositions", [])):
            cancel_result = cancel_all_open_orders(live_config, position["symbol"])
            append_execution_event(
                execution_events,
                decision_id=decision_id,
                stage="cancel_open_orders",
                action_type="manual_flatten",
                symbol=position["symbol"],
                side=position["side"],
                exchange=exchange_id,
                requested={"reason": reason},
                exchange_result=cancel_result,
            )
            side = "SELL" if position["side"] == "long" else "BUY"
            order_result = place_market_order(live_config, symbol=position["symbol"], side=side, quantity=position["quantity"], reduce_only=True)
            append_execution_event(
                execution_events,
                decision_id=decision_id,
                stage="market_order",
                action_type="manual_flatten",
                symbol=position["symbol"],
                side=position["side"],
                exchange=exchange_id,
                requested={"orderSide": side, "quantity": position["quantity"], "reduceOnly": True},
                exchange_result=order_result,
            )
            book, action = close_position(book, position, num(position.get("lastMarkPrice")) or num(position.get("entryPrice")) or 0, decision_id, reason)
            actions.append(action)
        book, live_warnings, _, _ = sync_live_book(book, settings, instance_id)
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
            "executionEvents": execution_events,
            "warnings": warnings,
            "output": {"actions": actions},
            "candidateUniverse": [],
            "accountBefore": {},
            "accountAfter": summarize_account(book, {**settings, "mode": target_mode}),
        }
    )
    book.setdefault("decisions", []).append(decision)
    if account_key == "live" and execution_events:
        book.setdefault("executionEvents", []).extend(execution_events)
    book["lastDecisionAt"] = now_iso()
    write_trading_state(state, instance_id)
    archive_decision(decision, instance_id)
    return state


def reset_paper_account(mode: str = "full", instance_id: str | None = None) -> dict[str, Any]:
    settings = read_trading_settings(instance_id)
    state = read_trading_state(settings, instance_id)
    if str(mode) == "equity_only":
        state["paper"]["initialCapitalUsd"] = settings["initialCapitalUsd"]
        state["paper"]["highWatermarkEquity"] = settings["initialCapitalUsd"]
        state["paper"]["openPositions"] = []
        state["paper"]["openOrders"] = []
        state["paper"]["closedTrades"] = []
        state["paper"]["decisions"] = []
        state["paper"]["executionEvents"] = []
        state["paper"]["lastDecisionAt"] = None
        state["paper"]["sessionStartedAt"] = now_iso()
        state["paper"]["circuitBreakerTripped"] = False
        state["paper"]["circuitBreakerReason"] = None
    else:
        state["paper"] = empty_trading_account(settings["initialCapitalUsd"], "paper")
        state["paper"]["sessionStartedAt"] = now_iso()
    state["adaptive"] = {
        "updatedAt": now_iso(),
        "notes": [
            "Paper account was reset in the Python build.",
            "The trade-logic fields, provider config, and proxy config were preserved.",
        ],
    }
    return write_trading_state(state, instance_id)


def reset_trading_account(mode: str = "paper", instance_id: str | None = None) -> dict[str, Any]:
    reset_mode = str(mode or "paper").strip().lower()
    if reset_mode in {"paper", "full", "equity_only"}:
        return reset_paper_account(reset_mode if reset_mode == "equity_only" else "full", instance_id)

    if reset_mode != "live":
        raise ValueError(f"Unsupported reset mode: {mode}")

    settings = read_trading_settings(instance_id)
    state = read_trading_state(settings, instance_id)
    book = state["live"]
    book, warnings, live_status_payload, live_config = sync_live_book(book, settings, instance_id)
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
        return write_trading_state(state, instance_id)
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
    fresh_book, sync_warnings, _, _ = sync_live_book(fresh_book, settings, instance_id)
    warnings.extend(sync_warnings)
    state["live"] = fresh_book
    state["adaptive"] = {
        "updatedAt": now_iso(),
        "notes": [
            "Live account was reset in the Python build.",
            "Live decisions, local estimated realized PnL, drawdown baseline, and synced positions were cleared.",
        ] + ([f"Reset warnings: {'; '.join(warnings[:3])}"] if warnings else []),
    }
    return write_trading_state(state, instance_id)
