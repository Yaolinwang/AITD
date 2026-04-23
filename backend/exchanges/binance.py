from __future__ import annotations

import hashlib
import hmac
import re
from typing import Any
from urllib.parse import urlencode

from ..config import read_live_trading_config, read_network_settings
from ..http_client import HttpRequestError, cached_get_json, request_json
from ..utils import clamp, now_iso, num
from .base import ExchangeGateway
from .catalog import exchange_config


class BinanceGateway(ExchangeGateway):
    exchange_id = "binance"
    display_name = "Binance"
    market_label = "USDT futures"
    default_backdrop_symbol = "BTCUSDT"
    public_base_url = exchange_config("binance")["defaultBaseUrl"]
    symbol_pattern = re.compile(r"^[A-Z0-9]+USDT$")
    transfer_income_types = {
        "TRANSFER",
        "INTERNAL_TRANSFER",
        "CROSS_COLLATERAL_TRANSFER",
        "COIN_SWAP_DEPOSIT",
        "COIN_SWAP_WITHDRAW",
        "STRATEGY_UMFUTURES_TRANSFER",
    }

    def __init__(self) -> None:
        self._server_time_offset_ms = 0

    @staticmethod
    def _exchange_margin_type(value: Any) -> str:
        normalized = str(value or "cross").strip().lower()
        if normalized in {"isolated", "isolate"}:
            return "ISOLATED"
        return "CROSSED"

    @staticmethod
    def _exchange_position_side(side: str) -> str:
        return "SHORT" if str(side or "").strip().lower() == "short" else "LONG"

    def _current_position_mode(self, config: dict[str, Any]) -> str:
        payload = self._signed_request_json(config, "GET", "/fapi/v1/positionSide/dual")
        dual_side = payload.get("dualSidePosition") if isinstance(payload, dict) else False
        if str(dual_side).strip().lower() == "true" or dual_side is True:
            return "hedge"
        return "oneway"

    def _resolved_position_mode(self, config: dict[str, Any]) -> str:
        cached = str(config.get("_resolvedPositionMode") or "").strip().lower()
        if cached in {"hedge", "oneway"}:
            return cached
        fallback = "hedge" if str(config.get("positionMode") or "").strip().lower() == "hedge" else "oneway"
        config["_resolvedPositionMode"] = fallback
        return fallback

    def validate_symbol(self, symbol: str) -> bool:
        normalized = self.normalize_symbol(symbol)
        if not self.symbol_pattern.fullmatch(normalized):
            return False
        try:
            payload = self._public_get_json(
                "/fapi/v1/exchangeInfo",
                namespace="exchange_info",
                ttl_seconds=6 * 60 * 60,
                max_stale_seconds=7 * 24 * 60 * 60,
            )
            symbols = payload.get("symbols") if isinstance(payload, dict) else []
            if isinstance(symbols, list) and symbols:
                return any(self.normalize_symbol(item.get("symbol")) == normalized for item in symbols if isinstance(item, dict))
        except Exception:
            pass
        return True

    def base_asset_from_symbol(self, symbol: str) -> str:
        normalized = self.normalize_symbol(symbol)
        return normalized[:-4] if normalized.endswith("USDT") else normalized

    def _cache_policy_for_kline_interval(self, interval: str) -> tuple[int, int]:
        interval = str(interval or "").lower()
        if interval == "1m":
            return 20, 60 * 60
        if interval == "5m":
            return 30, 2 * 60 * 60
        if interval == "15m":
            return 60, 3 * 60 * 60
        if interval == "1h":
            return 5 * 60, 12 * 60 * 60
        if interval == "4h":
            return 15 * 60, 48 * 60 * 60
        return 60, 6 * 60 * 60

    def _query(self, base_url: str, endpoint: str, params: dict[str, Any] | None = None) -> str:
        params = params or {}
        filtered = {key: value for key, value in params.items() if value not in (None, "", [])}
        encoded = urlencode(filtered)
        if not encoded:
            return f"{base_url.rstrip('/')}{endpoint}"
        return f"{base_url.rstrip('/')}{endpoint}?{encoded}"

    def _public_get_json(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        *,
        namespace: str,
        ttl_seconds: int,
        max_stale_seconds: int,
    ) -> Any:
        network_settings = read_network_settings()
        url = self._query(self.public_base_url, endpoint, params)
        return cached_get_json(
            url,
            namespace=f"binance_{namespace}",
            ttl_seconds=ttl_seconds,
            max_stale_seconds=max_stale_seconds,
            timeout_seconds=45,
            network_settings=network_settings,
        )

    def _parse_klines(self, rows: list[list[Any]] | None) -> list[dict[str, Any]]:
        parsed: list[dict[str, Any]] = []
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

    def fetch_all_tickers_24h(self) -> list[dict[str, Any]]:
        payload = self._public_get_json(
            "/fapi/v1/ticker/24hr",
            namespace="ticker24h_all",
            ttl_seconds=60,
            max_stale_seconds=45 * 60,
        )
        return payload if isinstance(payload, list) else []

    def fetch_all_premium_index(self) -> list[dict[str, Any]]:
        payload = self._public_get_json(
            "/fapi/v1/premiumIndex",
            namespace="premium_all",
            ttl_seconds=60,
            max_stale_seconds=45 * 60,
        )
        return payload if isinstance(payload, list) else []

    def fetch_ticker_24h(self, symbol: str) -> dict[str, Any]:
        payload = self._public_get_json(
            "/fapi/v1/ticker/24hr",
            params={"symbol": self.normalize_symbol(symbol)},
            namespace="ticker24h_symbol",
            ttl_seconds=60,
            max_stale_seconds=45 * 60,
        )
        return {
            "symbol": self.normalize_symbol(symbol),
            "lastPrice": num(payload.get("lastPrice")),
            "priceChangePct": num(payload.get("priceChangePercent")),
            "quoteVolume": num(payload.get("quoteVolume")),
            "highPrice": num(payload.get("highPrice")),
            "lowPrice": num(payload.get("lowPrice")),
            "count": num(payload.get("count")),
        }

    def fetch_premium(self, symbol: str) -> dict[str, Any]:
        payload = self._public_get_json(
            "/fapi/v1/premiumIndex",
            params={"symbol": self.normalize_symbol(symbol)},
            namespace="premium_symbol",
            ttl_seconds=60,
            max_stale_seconds=45 * 60,
        )
        return {
            "symbol": self.normalize_symbol(symbol),
            "markPrice": num(payload.get("markPrice")),
            "indexPrice": num(payload.get("indexPrice")),
            "fundingPct": (num(payload.get("lastFundingRate")) or 0) * 100,
            "nextFundingTime": payload.get("nextFundingTime"),
        }

    def fetch_klines(self, symbol: str, interval: str, limit: int) -> list[dict[str, Any]]:
        ttl_seconds, max_stale_seconds = self._cache_policy_for_kline_interval(interval)
        rows = self._public_get_json(
            "/fapi/v1/klines",
            params={"symbol": self.normalize_symbol(symbol), "interval": interval, "limit": limit},
            namespace=f"klines_{interval}",
            ttl_seconds=ttl_seconds,
            max_stale_seconds=max_stale_seconds,
        )
        return self._parse_klines(rows if isinstance(rows, list) else [])

    def resolved_base_url(self, config: dict[str, Any]) -> str:
        return str(config.get("baseUrl") or self.public_base_url)

    def live_execution_status(
        self,
        live_config: dict[str, Any] | None = None,
        trading_settings: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        live_config = live_config or read_live_trading_config()
        issues = []
        if not live_config.get("apiKey"):
            issues.append("Live trading API key is missing.")
        if not live_config.get("apiSecret"):
            issues.append("Live trading API secret is missing.")
        resolved_position_mode = "hedge" if str(live_config.get("positionMode") or "").strip().lower() == "hedge" else "oneway"
        if not issues:
            try:
                resolved_position_mode = self._current_position_mode(live_config)
                live_config["_resolvedPositionMode"] = resolved_position_mode
            except Exception:
                live_config["_resolvedPositionMode"] = resolved_position_mode
        can_sync = not issues
        can_execute = can_sync and live_config.get("enabled") and not live_config.get("dryRun")
        return {
            "configEnabled": live_config.get("enabled") is True,
            "dryRun": live_config.get("dryRun") is True,
            "armed": can_execute,
            "canSync": can_sync,
            "canExecute": can_execute,
            "issues": issues,
            "baseUrl": self.resolved_base_url(live_config),
            "exchange": self.exchange_id,
            "positionMode": resolved_position_mode,
        }

    def _signed_params(self, config: dict[str, Any], params: dict[str, Any] | None = None) -> str:
        payload = dict(params or {})
        payload["timestamp"] = str(int(__import__("time").time() * 1000) + int(self._server_time_offset_ms or 0))
        payload["recvWindow"] = str(int(config.get("recvWindow") or 5000))
        query_string = urlencode({key: value for key, value in payload.items() if value not in (None, "")})
        signature = hmac.new(
            str(config["apiSecret"]).encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return f"{query_string}&signature={signature}"

    def _sync_server_time_offset(self, config: dict[str, Any]) -> None:
        network_settings = read_network_settings()
        base_url = self.resolved_base_url(config).rstrip("/")
        payload = request_json(
            "GET",
            f"{base_url}/fapi/v1/time",
            timeout_seconds=10,
            network_settings=network_settings,
        )
        server_time = num(payload.get("serverTime")) if isinstance(payload, dict) else None
        local_time = int(__import__("time").time() * 1000)
        if server_time is not None:
            self._server_time_offset_ms = int(server_time) - local_time

    def _signed_request_json(
        self,
        config: dict[str, Any],
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        network_settings = read_network_settings()
        base_url = self.resolved_base_url(config).rstrip("/")
        headers_base = {"X-MBX-APIKEY": config["apiKey"]}

        def send_once() -> Any:
            query = self._signed_params(config, params)
            url = f"{base_url}{endpoint}"
            if method.upper() in {"GET", "DELETE"}:
                return request_json(
                    method,
                    f"{url}?{query}",
                    headers=headers_base,
                    timeout_seconds=45,
                    network_settings=network_settings,
                )
            return request_json(
                method,
                url,
                headers={
                    **headers_base,
                    "content-type": "application/x-www-form-urlencoded",
                },
                payload=query,
                timeout_seconds=45,
                network_settings=network_settings,
            )

        try:
            return send_once()
        except HttpRequestError as error:
            if "-1021" in str(error):
                self._sync_server_time_offset(config)
                return send_once()
            raise HttpRequestError(f"{error} [endpoint {method.upper()} {endpoint}]") from error

    def _exchange_info(self, config: dict[str, Any]) -> dict[str, Any]:
        network_settings = read_network_settings()
        url = f"{self.resolved_base_url(config).rstrip('/')}/fapi/v1/exchangeInfo"
        payload = cached_get_json(
            url,
            namespace="binance_live_exchange_info",
            ttl_seconds=6 * 60 * 60,
            max_stale_seconds=7 * 24 * 60 * 60,
            timeout_seconds=45,
            network_settings=network_settings,
        )
        return payload if isinstance(payload, dict) else {}

    def _symbol_info(self, config: dict[str, Any], symbol: str) -> dict[str, Any]:
        normalized = self.normalize_symbol(symbol)
        info = self._exchange_info(config)
        for item in info.get("symbols", []):
            if self.normalize_symbol(item.get("symbol")) == normalized:
                return item
        raise ValueError(f"{self.display_name} symbol not found: {normalized}")

    def _step_precision(self, step_size: str | float | int | None) -> int:
        text = str(step_size or "1")
        if "." not in text:
            return 0
        return len(text.rstrip("0").split(".")[1])

    def _round_down_to_step(self, value: float, step_size: float, precision: int) -> float:
        units = int(value / step_size)
        return round(units * step_size, precision)

    def _round_to_step(self, value: float, step_size: float, precision: int) -> float:
        units = round(value / step_size)
        return round(units * step_size, precision)

    def normalize_quantity(
        self,
        config: dict[str, Any],
        symbol: str,
        *,
        reference_price: float | None = None,
        quantity: float | None = None,
        notional_usd: float | None = None,
    ) -> float:
        info = self._symbol_info(config, symbol)
        filters = info.get("filters") or []
        filter_row = next((item for item in filters if item.get("filterType") in {"MARKET_LOT_SIZE", "LOT_SIZE"}), None)
        if not filter_row:
            raise ValueError(f"Could not determine lot size for {self.normalize_symbol(symbol)}")
        step_size = num(filter_row.get("stepSize")) or 1
        min_qty = num(filter_row.get("minQty")) or 0
        precision = self._step_precision(filter_row.get("stepSize"))
        raw_quantity = quantity if quantity is not None else ((notional_usd or 0) / (reference_price or 0) if reference_price else 0)
        normalized = self._round_down_to_step(max(0.0, float(raw_quantity or 0)), step_size, precision)
        if normalized < min_qty:
            raise ValueError(f"Quantity for {self.normalize_symbol(symbol)} is below exchange minimum.")
        return normalized

    def normalize_price(self, config: dict[str, Any], symbol: str, price: float) -> float:
        info = self._symbol_info(config, symbol)
        filters = info.get("filters") or []
        filter_row = next((item for item in filters if item.get("filterType") == "PRICE_FILTER"), None)
        if not filter_row:
            return float(price)
        tick_size = num(filter_row.get("tickSize")) or 0
        if tick_size <= 0:
            return float(price)
        precision = self._step_precision(filter_row.get("tickSize"))
        return self._round_to_step(float(price), tick_size, precision)

    def _safe_signed_call(
        self,
        config: dict[str, Any],
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        try:
            return self._signed_request_json(config, method, endpoint, params)
        except Exception as error:
            message = str(error)
            if (
                "No need to change margin type" in message
                or "-4046" in message
                or "-4067" in message
                or "-4068" in message
            ):
                return {"ignored": True}
            raise

    def _income_summary(self, config: dict[str, Any]) -> dict[str, Any]:
        limit = 1000
        now_ms = int(__import__("time").time() * 1000)
        ninety_days_ms = 90 * 24 * 60 * 60 * 1000
        rows = self._signed_request_json(
            config,
            "GET",
            "/fapi/v1/income",
            {
                "startTime": now_ms - ninety_days_ms,
                "endTime": now_ms,
                "limit": limit,
            },
        )
        entries = rows if isinstance(rows, list) else []
        net_cashflow = 0.0
        realized_pnl = 0.0
        funding_fee = 0.0
        commission = 0.0
        other_income = 0.0
        for row in entries:
            if not isinstance(row, dict):
                continue
            income_type = str(row.get("incomeType") or "").strip().upper()
            income_value = num(row.get("income")) or 0
            if income_type in self.transfer_income_types:
                net_cashflow += income_value
            elif income_type == "REALIZED_PNL":
                realized_pnl += income_value
            elif income_type == "FUNDING_FEE":
                funding_fee += income_value
            elif income_type == "COMMISSION":
                commission += income_value
            else:
                other_income += income_value
        note = None
        if len(entries) >= limit:
            note = "Binance income history hit the 1000-row sync cap; very active accounts may need a deeper backfill."
        return {
            "netCashflowUsd": net_cashflow,
            "incomeRealizedPnlUsd": realized_pnl,
            "fundingFeeUsd": funding_fee,
            "commissionUsd": commission,
            "otherIncomeUsd": other_income,
            "accountingUpdatedAt": now_iso(),
            "accountingNote": note,
        }

    @staticmethod
    def _session_start_ms(session_started_at: str | None) -> int | None:
        if not session_started_at:
            return None
        try:
            value = __import__("datetime").datetime.fromisoformat(str(session_started_at).replace("Z", "+00:00"))
        except Exception:
            return None
        return int(value.timestamp() * 1000)

    def _exchange_closed_trades(self, config: dict[str, Any], session_started_at: str | None) -> list[dict[str, Any]]:
        session_start_ms = self._session_start_ms(session_started_at)
        if session_start_ms is None:
            return []
        rows = self._signed_request_json(
            config,
            "GET",
            "/fapi/v1/income",
            {
                "incomeType": "REALIZED_PNL",
                "startTime": session_start_ms,
                "endTime": int(__import__("time").time() * 1000),
                "limit": 1000,
            },
        )
        entries = rows if isinstance(rows, list) else []
        closed_trades: list[dict[str, Any]] = []
        for row in entries:
            if not isinstance(row, dict):
                continue
            realized_pnl = num(row.get("income"))
            if realized_pnl is None or abs(realized_pnl) <= 1e-12:
                continue
            symbol = self.normalize_symbol(row.get("symbol"))
            closed_at_ms = num(row.get("time"))
            closed_at = (
                __import__("datetime").datetime.utcfromtimestamp(closed_at_ms / 1000).replace(microsecond=0).isoformat() + "Z"
                if closed_at_ms is not None
                else now_iso()
            )
            closed_trades.append(
                {
                    "id": f"binance-realized-{row.get('tranId') or row.get('tradeId') or symbol}-{int(closed_at_ms or 0)}",
                    "symbol": symbol,
                    "baseAsset": self.base_asset_from_symbol(symbol),
                    "realizedPnl": realized_pnl,
                    "asset": str(row.get("asset") or "USDT").strip().upper() or "USDT",
                    "closedAt": closed_at,
                    "info": str(row.get("info") or "").strip(),
                    "source": "binance_realized_pnl",
                }
            )
        closed_trades.sort(key=lambda item: str(item.get("closedAt") or ""))
        return closed_trades

    def apply_symbol_settings(self, config: dict[str, Any], symbol: str) -> None:
        leverage = int(clamp(config.get("defaultLeverage"), 1, 125))
        normalized = self.normalize_symbol(symbol)
        self._safe_signed_call(config, "POST", "/fapi/v1/leverage", {"symbol": normalized, "leverage": leverage})
        self._safe_signed_call(
            config,
            "POST",
            "/fapi/v1/marginType",
            {"symbol": normalized, "marginType": self._exchange_margin_type(config.get("marginType"))},
        )

    def fetch_account_snapshot(self, config: dict[str, Any], session_started_at: str | None = None) -> dict[str, Any]:
        account = self._signed_request_json(config, "GET", "/fapi/v2/account")
        positions = self._signed_request_json(config, "GET", "/fapi/v2/positionRisk")
        open_orders_payload = self._signed_request_json(config, "GET", "/fapi/v1/openOrders")
        open_algo_orders_payload = self._signed_request_json(config, "GET", "/fapi/v1/openAlgoOrders")
        try:
            config["_resolvedPositionMode"] = self._current_position_mode(config)
        except Exception:
            config["_resolvedPositionMode"] = self._resolved_position_mode(config)
        accounting_summary = {
            "netCashflowUsd": None,
            "incomeRealizedPnlUsd": None,
            "fundingFeeUsd": None,
            "commissionUsd": None,
            "otherIncomeUsd": None,
            "accountingUpdatedAt": None,
            "accountingNote": None,
        }
        try:
            accounting_summary.update(self._income_summary(config))
        except Exception as error:
            accounting_summary["accountingNote"] = f"Binance income sync failed: {error}"
        exchange_closed_trades: list[dict[str, Any]] = []
        try:
            exchange_closed_trades = self._exchange_closed_trades(config, session_started_at)
        except Exception as error:
            note = f"Binance closed-trade sync failed: {error}"
            if accounting_summary.get("accountingNote"):
                accounting_summary["accountingNote"] = f"{accounting_summary['accountingNote']} | {note}"
            else:
                accounting_summary["accountingNote"] = note
        open_positions = []
        for row in positions or []:
            raw_amount = num(row.get("positionAmt"))
            if raw_amount is None or abs(raw_amount) <= 1e-9:
                continue
            symbol = self.normalize_symbol(row.get("symbol"))
            raw_position_side = str(row.get("positionSide") or "").strip().upper()
            if raw_position_side == "LONG":
                side = "long"
            elif raw_position_side == "SHORT":
                side = "short"
            else:
                side = "long" if raw_amount > 0 else "short"
            quantity = abs(raw_amount)
            entry_price = num(row.get("entryPrice")) or 0
            mark_price = num(row.get("markPrice")) or entry_price
            notional_usd = abs(num(row.get("notional")) or (mark_price * quantity))
            open_positions.append(
                {
                    "id": f"live-{symbol}-{side}",
                    "symbol": symbol,
                    "baseAsset": self.base_asset_from_symbol(symbol),
                    "side": side,
                    "quantity": quantity,
                    "initialQuantity": quantity,
                    "entryPrice": entry_price,
                    "notionalUsd": notional_usd,
                    "initialNotionalUsd": notional_usd,
                    "stopLoss": None,
                    "takeProfit": None,
                    "lastMarkPrice": mark_price,
                    "lastMarkTime": now_iso(),
                    "leverage": num(row.get("leverage")) or 1,
                    "status": "open",
                    "openedAt": None,
                    "updatedAt": now_iso(),
                    "source": self.exchange_id,
                    "entryReason": "synced_from_exchange",
                    "decisionId": None,
                    "exchangePositionSide": raw_position_side or ("BOTH" if self._resolved_position_mode(config) == "oneway" else self._exchange_position_side(side)),
                }
            )
        open_orders = []
        for row in open_orders_payload or []:
            if not isinstance(row, dict):
                continue
            symbol = self.normalize_symbol(row.get("symbol"))
            open_orders.append(
                {
                    "id": f"std-{row.get('orderId') or row.get('clientOrderId') or symbol}",
                    "symbol": symbol,
                    "baseAsset": self.base_asset_from_symbol(symbol),
                    "side": str(row.get("side") or "").upper(),
                    "positionSide": str(row.get("positionSide") or "").upper(),
                    "type": str(row.get("type") or row.get("origType") or "").upper(),
                    "status": str(row.get("status") or "").upper(),
                    "price": num(row.get("price")),
                    "triggerPrice": num(row.get("stopPrice")),
                    "quantity": num(row.get("origQty")) or num(row.get("quantity")),
                    "reduceOnly": str(row.get("reduceOnly") or "").lower() == "true" or row.get("reduceOnly") is True,
                    "closePosition": str(row.get("closePosition") or "").lower() == "true" or row.get("closePosition") is True,
                    "workingType": str(row.get("workingType") or "").upper(),
                    "source": "binance_standard_order",
                    "updatedAt": now_iso(),
                }
            )
        for row in open_algo_orders_payload or []:
            if not isinstance(row, dict):
                continue
            symbol = self.normalize_symbol(row.get("symbol"))
            open_orders.append(
                {
                    "id": f"algo-{row.get('algoId') or row.get('clientAlgoId') or symbol}",
                    "symbol": symbol,
                    "baseAsset": self.base_asset_from_symbol(symbol),
                    "side": str(row.get("side") or "").upper(),
                    "positionSide": str(row.get("positionSide") or "").upper(),
                    "type": str(row.get("type") or row.get("algoType") or "").upper(),
                    "status": str(row.get("status") or "OPEN").upper(),
                    "price": num(row.get("price")),
                    "triggerPrice": num(row.get("triggerPrice")) or num(row.get("stopPrice")),
                    "quantity": num(row.get("quantity")) or num(row.get("origQty")),
                    "reduceOnly": str(row.get("reduceOnly") or "").lower() == "true" or row.get("reduceOnly") is True,
                    "closePosition": str(row.get("closePosition") or "").lower() == "true" or row.get("closePosition") is True,
                    "workingType": str(row.get("workingType") or "").upper(),
                    "source": "binance_algo_order",
                    "updatedAt": now_iso(),
                }
            )
        return {
            "walletBalanceUsd": num(account.get("totalWalletBalance")) or num(account.get("walletBalance")) or 0,
            "equityUsd": num(account.get("totalMarginBalance")) or num(account.get("totalWalletBalance")) or 0,
            "availableBalanceUsd": num(account.get("availableBalance")) or 0,
            "unrealizedPnlUsd": num(account.get("totalUnrealizedProfit")) or 0,
            "positionMode": self._resolved_position_mode(config),
            **accounting_summary,
            "exchangeClosedTrades": exchange_closed_trades,
            "openPositions": open_positions,
            "openOrders": open_orders,
            "raw": {
                "account": account,
                "positions": positions,
                "openOrders": open_orders_payload,
                "openAlgoOrders": open_algo_orders_payload,
            },
        }

    def cancel_all_open_orders(self, config: dict[str, Any], symbol: str) -> Any:
        normalized = self.normalize_symbol(symbol)
        standard_result = self._signed_request_json(config, "DELETE", "/fapi/v1/allOpenOrders", {"symbol": normalized})
        algo_result = self._signed_request_json(config, "DELETE", "/fapi/v1/algoOpenOrders", {"symbol": normalized})
        return {
            "standard": standard_result,
            "algo": algo_result,
        }

    def place_market_order(
        self,
        config: dict[str, Any],
        *,
        symbol: str,
        side: str,
        quantity: float,
        reduce_only: bool = False,
    ) -> dict[str, Any]:
        params = {
            "symbol": self.normalize_symbol(symbol),
            "side": str(side or "").upper(),
            "type": "MARKET",
            "quantity": quantity,
        }
        if self._resolved_position_mode(config) == "hedge":
            if params["side"] == "BUY":
                params["positionSide"] = "SHORT" if reduce_only else "LONG"
            else:
                params["positionSide"] = "LONG" if reduce_only else "SHORT"
        elif reduce_only:
            params["reduceOnly"] = "true"
        return self._signed_request_json(config, "POST", "/fapi/v1/order", params)

    def place_protection_orders(
        self,
        config: dict[str, Any],
        *,
        symbol: str,
        position_side: str,
        stop_loss: float | None,
        take_profit: float | None,
    ) -> list[dict[str, Any]]:
        created: list[dict[str, Any]] = []
        if stop_loss is None and take_profit is None:
            return created
        normalized = self.normalize_symbol(symbol)
        close_side = "SELL" if position_side == "long" else "BUY"
        hedge_mode = self._resolved_position_mode(config) == "hedge"
        exchange_position_side = self._exchange_position_side(position_side)
        if stop_loss is not None:
            stop_order = {
                "algoType": "CONDITIONAL",
                "symbol": normalized,
                "side": close_side,
                "type": "STOP_MARKET",
                "triggerPrice": self.normalize_price(config, normalized, stop_loss),
                "closePosition": "true",
                "workingType": "MARK_PRICE",
            }
            if hedge_mode:
                stop_order["positionSide"] = exchange_position_side
            created.append(
                self._signed_request_json(
                    config,
                    "POST",
                    "/fapi/v1/algoOrder",
                    stop_order,
                )
            )
        if take_profit is not None:
            take_profit_order = {
                "algoType": "CONDITIONAL",
                "symbol": normalized,
                "side": close_side,
                "type": "TAKE_PROFIT_MARKET",
                "triggerPrice": self.normalize_price(config, normalized, take_profit),
                "closePosition": "true",
                "workingType": "MARK_PRICE",
            }
            if hedge_mode:
                take_profit_order["positionSide"] = exchange_position_side
            created.append(
                self._signed_request_json(
                    config,
                    "POST",
                    "/fapi/v1/algoOrder",
                    take_profit_order,
                )
            )
        return created
