from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

from ..config import read_live_trading_config, read_network_settings
from ..http_client import cached_get_json, request_json
from ..utils import clamp, now_iso, num
from .base import ExchangeGateway
from .catalog import exchange_config


class OkxGateway(ExchangeGateway):
    exchange_id = "okx"
    display_name = "OKX"
    market_label = "USDT perpetual swap"
    default_backdrop_symbol = "BTC-USDT-SWAP"
    public_base_url = exchange_config("okx")["defaultBaseUrl"]
    symbol_pattern = re.compile(r"^[A-Z0-9]+-USDT-SWAP$")
    interval_map = {
        "1m": "1m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1H",
        "2h": "2H",
        "4h": "4H",
        "12h": "12H",
    }

    def candidate_symbol_hint(self) -> str:
        return "OKX USDT perpetual swap symbols"

    def normalize_symbol(self, symbol: str) -> str:
        normalized = str(symbol or "").strip().upper().replace("_", "-").replace("/", "-")
        normalized = re.sub(r"-{2,}", "-", normalized)
        if normalized.endswith("-USDT-SWAP"):
            return normalized
        if normalized.endswith("-USDT"):
            return f"{normalized}-SWAP"
        if normalized.endswith("USDT") and "-" not in normalized and len(normalized) > 4:
            base = normalized[:-4]
            if base:
                return f"{base}-USDT-SWAP"
        return normalized

    def validate_symbol(self, symbol: str) -> bool:
        normalized = self.normalize_symbol(symbol)
        if not self.symbol_pattern.fullmatch(normalized):
            return False
        try:
            instruments = self._instruments()
            if instruments:
                return any(
                    self.normalize_symbol(item.get("instId")) == normalized
                    for item in instruments
                    if isinstance(item, dict)
                )
        except Exception:
            pass
        return True

    def base_asset_from_symbol(self, symbol: str) -> str:
        normalized = self.normalize_symbol(symbol)
        return normalized.split("-", 1)[0] if "-" in normalized else normalized

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

    def resolved_base_url(self, config: dict[str, Any]) -> str:
        base_url = str(config.get("baseUrl") or self.public_base_url).strip() or self.public_base_url
        normalized = base_url.rstrip("/")
        if normalized.endswith("/api/v5"):
            normalized = normalized[:-7]
        return normalized.rstrip("/")

    def _query_string(self, params: dict[str, Any] | None = None) -> str:
        filtered = {
            key: value
            for key, value in (params or {}).items()
            if value not in (None, "", [], {})
        }
        return urlencode(filtered)

    def _query(self, base_url: str, endpoint: str, params: dict[str, Any] | None = None) -> str:
        query_string = self._query_string(params)
        if not query_string:
            return f"{base_url.rstrip('/')}{endpoint}"
        return f"{base_url.rstrip('/')}{endpoint}?{query_string}"

    def _okx_data(self, payload: Any, *, endpoint: str) -> Any:
        if not isinstance(payload, dict):
            raise ValueError(f"Unexpected OKX response for {endpoint}.")
        code = str(payload.get("code") or "")
        if code not in {"", "0"}:
            data_rows = payload.get("data") if isinstance(payload.get("data"), list) else []
            row_message = ""
            row_code = ""
            if data_rows and isinstance(data_rows[0], dict):
                row_code = str(data_rows[0].get("sCode") or "").strip()
                row_message = str(data_rows[0].get("sMsg") or "").strip()
            message = row_message or str(payload.get("msg") or code).strip() or code
            if code == "50105":
                raise ValueError("OKX API passphrase is incorrect for this API key.")
            if code == "50111":
                raise ValueError("OKX API key is invalid.")
            if code == "50113":
                raise ValueError("OKX API signature is invalid. Please re-check API secret and request signing.")
            if row_code:
                raise ValueError(f"OKX {endpoint} failed: {row_code} {message}".strip())
            raise ValueError(f"OKX {endpoint} failed: {message}")
        return payload.get("data")

    def _credential(self, config: dict[str, Any], key: str) -> str:
        return str(config.get(key) or "").strip()

    def _public_get_data(
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
        payload = cached_get_json(
            url,
            namespace=f"okx_{namespace}",
            ttl_seconds=ttl_seconds,
            max_stale_seconds=max_stale_seconds,
            timeout_seconds=45,
            network_settings=network_settings,
        )
        return self._okx_data(payload, endpoint=endpoint)

    def _timestamp(self) -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def _signed_request_json(
        self,
        config: dict[str, Any],
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | list[dict[str, Any]] | None = None,
    ) -> Any:
        network_settings = read_network_settings()
        method_upper = method.upper()
        query_string = self._query_string(params)
        request_path = endpoint if not query_string else f"{endpoint}?{query_string}"
        url = f"{self.resolved_base_url(config)}{request_path}"
        body_text = "" if body is None else json.dumps(body, separators=(",", ":"), ensure_ascii=False)
        timestamp = self._timestamp()
        prehash = f"{timestamp}{method_upper}{request_path}{body_text}"
        signature = base64.b64encode(
            hmac.new(
                self._credential(config, "apiSecret").encode("utf-8"),
                prehash.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        ).decode("utf-8")
        headers = {
            "OK-ACCESS-KEY": self._credential(config, "apiKey"),
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self._credential(config, "apiPassphrase"),
            "Content-Type": "application/json",
        }
        payload = request_json(
            method_upper,
            url,
            headers=headers,
            payload=body_text if body is not None else None,
            timeout_seconds=45,
            network_settings=network_settings,
        )
        return self._okx_data(payload, endpoint=endpoint)

    def _instruments(self) -> list[dict[str, Any]]:
        payload = self._public_get_data(
            "/api/v5/public/instruments",
            {"instType": "SWAP"},
            namespace="public_instruments_swap",
            ttl_seconds=6 * 60 * 60,
            max_stale_seconds=7 * 24 * 60 * 60,
        )
        return payload if isinstance(payload, list) else []

    def _symbol_info(self, symbol: str) -> dict[str, Any]:
        normalized = self.normalize_symbol(symbol)
        for item in self._instruments():
            if self.normalize_symbol(item.get("instId")) == normalized:
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

    def _contract_notional_usd(self, info: dict[str, Any], reference_price: float | None) -> float:
        ct_val = num(info.get("ctVal")) or 0
        if ct_val <= 0:
            raise ValueError(f"Could not determine contract value for {info.get('instId')}.")
        ct_val_ccy = str(info.get("ctValCcy") or "").upper()
        base_ccy = str(info.get("baseCcy") or "").upper()
        quote_ccy = str(info.get("quoteCcy") or info.get("settleCcy") or "").upper()
        if ct_val_ccy and quote_ccy and ct_val_ccy == quote_ccy:
            return ct_val
        if ct_val_ccy and base_ccy and ct_val_ccy == base_ccy:
            if not reference_price or reference_price <= 0:
                raise ValueError(f"Reference price is required to size {info.get('instId')}.")
            return ct_val * float(reference_price)
        if str(info.get("ctType") or "").lower() == "linear":
            if not reference_price or reference_price <= 0:
                raise ValueError(f"Reference price is required to size {info.get('instId')}.")
            return ct_val * float(reference_price)
        return ct_val

    def _format_number(self, value: float, precision: int = 12) -> str:
        text = f"{float(value):.{precision}f}".rstrip("0").rstrip(".")
        return text or "0"

    def _map_ticker_row(self, row: dict[str, Any]) -> dict[str, Any]:
        symbol = self.normalize_symbol(row.get("instId"))
        last_price = num(row.get("last")) or 0
        open_24h = num(row.get("open24h")) or num(row.get("sodUtc8")) or num(row.get("sodUtc0")) or 0
        quote_volume = 0.0
        base_volume = num(row.get("volCcy24h"))
        if base_volume is not None and last_price > 0:
            quote_volume = base_volume * last_price
        else:
            quote_volume = num(row.get("vol24h")) or 0
        price_change_pct = ((last_price - open_24h) / open_24h) * 100 if last_price > 0 and open_24h > 0 else 0
        return {
            "symbol": symbol,
            "lastPrice": last_price,
            "highPrice": num(row.get("high24h")) or last_price,
            "lowPrice": num(row.get("low24h")) or last_price,
            "priceChangePercent": price_change_pct,
            "quoteVolume": quote_volume,
            "baseVolume": base_volume,
            "ts": row.get("ts"),
            "raw": row,
        }

    def fetch_all_tickers_24h(self) -> list[dict[str, Any]]:
        rows = self._public_get_data(
            "/api/v5/market/tickers",
            {"instType": "SWAP"},
            namespace="market_tickers_swap",
            ttl_seconds=60,
            max_stale_seconds=45 * 60,
        )
        if not isinstance(rows, list):
            return []
        return [self._map_ticker_row(row) for row in rows if isinstance(row, dict)]

    def fetch_all_premium_index(self) -> list[dict[str, Any]]:
        try:
            rows = self._public_get_data(
                "/api/v5/public/mark-price",
                {"instType": "SWAP"},
                namespace="public_mark_price_swap",
                ttl_seconds=60,
                max_stale_seconds=45 * 60,
            )
        except Exception:
            return []
        if not isinstance(rows, list):
            return []
        return [
            {
                "symbol": self.normalize_symbol(row.get("instId")),
                "markPrice": num(row.get("markPx")),
                "lastFundingRate": None,
                "fundingRate": None,
                "raw": row,
            }
            for row in rows
            if isinstance(row, dict) and row.get("instId")
        ]

    def fetch_ticker_24h(self, symbol: str) -> dict[str, Any]:
        normalized = self.normalize_symbol(symbol)
        rows = self._public_get_data(
            "/api/v5/market/ticker",
            {"instId": normalized},
            namespace=f"market_ticker_{normalized}",
            ttl_seconds=20,
            max_stale_seconds=30 * 60,
        )
        row = rows[0] if isinstance(rows, list) and rows else {}
        return self._map_ticker_row(row if isinstance(row, dict) else {})

    def fetch_premium(self, symbol: str) -> dict[str, Any]:
        normalized = self.normalize_symbol(symbol)
        mark_row: dict[str, Any] = {}
        funding_row: dict[str, Any] = {}
        try:
            rows = self._public_get_data(
                "/api/v5/public/mark-price",
                {"instType": "SWAP", "instId": normalized},
                namespace=f"public_mark_price_{normalized}",
                ttl_seconds=20,
                max_stale_seconds=30 * 60,
            )
            if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                mark_row = rows[0]
        except Exception:
            mark_row = {}
        try:
            rows = self._public_get_data(
                "/api/v5/public/funding-rate",
                {"instId": normalized},
                namespace=f"public_funding_rate_{normalized}",
                ttl_seconds=60,
                max_stale_seconds=12 * 60 * 60,
            )
            if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                funding_row = rows[0]
        except Exception:
            funding_row = {}
        funding_rate = num(funding_row.get("fundingRate"))
        return {
            "symbol": normalized,
            "markPrice": num(mark_row.get("markPx")) or num(funding_row.get("markPx")),
            "lastFundingRate": funding_rate,
            "fundingRate": funding_rate,
            "fundingPct": (funding_rate or 0) * 100,
            "nextFundingTime": funding_row.get("nextFundingTime"),
            "raw": {
                "mark": mark_row,
                "funding": funding_row,
            },
        }

    def fetch_klines(self, symbol: str, interval: str, limit: int) -> list[dict[str, Any]]:
        normalized = self.normalize_symbol(symbol)
        bar = self.interval_map.get(str(interval or "").lower())
        if not bar:
            raise ValueError(f"Unsupported OKX kline interval: {interval}")
        ttl_seconds, max_stale_seconds = self._cache_policy_for_kline_interval(interval)
        rows = self._public_get_data(
            "/api/v5/market/history-candles",
            {"instId": normalized, "bar": bar, "limit": int(clamp(limit, 1, 300))},
            namespace=f"market_history_candles_{normalized}_{bar}",
            ttl_seconds=ttl_seconds,
            max_stale_seconds=max_stale_seconds,
        )
        parsed: list[dict[str, Any]] = []
        for row in reversed(rows if isinstance(rows, list) else []):
            if not isinstance(row, list) or len(row) < 8:
                continue
            close_value = num(row[4])
            if close_value is None:
                continue
            parsed.append(
                {
                    "openTime": int(num(row[0]) or 0),
                    "open": num(row[1]),
                    "high": num(row[2]),
                    "low": num(row[3]),
                    "close": close_value,
                    "volume": num(row[5]),
                    "closeTime": int(num(row[0]) or 0),
                    "quoteVolume": num(row[7]) or num(row[6]),
                }
            )
        return parsed

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
        if not live_config.get("apiPassphrase"):
            issues.append("OKX API passphrase is missing.")
        if live_config.get("positionMode") != "oneway":
            issues.append("Only OKX net mode / one-way mode is supported in the Python build.")
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
        }

    def normalize_quantity(
        self,
        config: dict[str, Any],
        symbol: str,
        *,
        reference_price: float | None = None,
        quantity: float | None = None,
        notional_usd: float | None = None,
    ) -> float:
        info = self._symbol_info(symbol)
        lot_size = num(info.get("lotSz")) or 1
        min_size = num(info.get("minSz")) or lot_size
        precision = self._step_precision(info.get("lotSz"))
        raw_quantity = quantity
        if raw_quantity is None:
            contract_notional = self._contract_notional_usd(info, reference_price)
            raw_quantity = (notional_usd or 0) / contract_notional if contract_notional > 0 else 0
        normalized = self._round_down_to_step(max(0.0, float(raw_quantity or 0)), lot_size, precision)
        if normalized < min_size:
            raise ValueError(f"Quantity for {self.normalize_symbol(symbol)} is below exchange minimum.")
        return normalized

    def normalize_price(self, config: dict[str, Any], symbol: str, price: float) -> float:
        info = self._symbol_info(symbol)
        tick_size = num(info.get("tickSz")) or 0
        if tick_size <= 0:
            return float(price)
        precision = self._step_precision(info.get("tickSz"))
        return self._round_to_step(float(price), tick_size, precision)

    def apply_symbol_settings(self, config: dict[str, Any], symbol: str) -> None:
        body = {
            "instId": self.normalize_symbol(symbol),
            "lever": self._format_number(int(clamp(config.get("defaultLeverage"), 1, 125)), 0),
            "mgnMode": "isolated" if str(config.get("marginType") or "cross").lower() == "isolated" else "cross",
            "posSide": "net",
        }
        self._signed_request_json(config, "POST", "/api/v5/account/set-leverage", body=body)

    def fetch_account_snapshot(self, config: dict[str, Any], session_started_at: str | None = None) -> dict[str, Any]:
        balances = self._signed_request_json(config, "GET", "/api/v5/account/balance")
        positions = self._signed_request_json(config, "GET", "/api/v5/account/positions", params={"instType": "SWAP"})
        account_row = balances[0] if isinstance(balances, list) and balances else {}
        balance_details = account_row.get("details") if isinstance(account_row, dict) else []
        usdt_detail = next(
            (item for item in balance_details or [] if str(item.get("ccy") or "").upper() == "USDT"),
            {},
        )
        open_positions: list[dict[str, Any]] = []
        unrealized_pnl = 0.0
        for row in positions if isinstance(positions, list) else []:
            contracts = num(row.get("pos"))
            if contracts is None or abs(contracts) <= 1e-9:
                continue
            symbol = self.normalize_symbol(row.get("instId"))
            side_hint = str(row.get("posSide") or "").strip().lower()
            side = "long" if side_hint == "long" or contracts > 0 else "short"
            quantity = abs(contracts)
            entry_price = num(row.get("avgPx")) or num(row.get("openAvgPx")) or 0
            mark_price = num(row.get("markPx")) or entry_price
            try:
                contract_value_usd = self._contract_notional_usd(self._symbol_info(symbol), mark_price)
                notional_usd = abs(quantity * contract_value_usd)
            except Exception:
                notional_usd = abs(num(row.get("notionalUsd")) or (mark_price * quantity))
            position_upl = num(row.get("upl")) or 0
            unrealized_pnl += position_upl
            open_positions.append(
                {
                    "id": f"live-{symbol}",
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
                    "leverage": num(row.get("lever")) or 1,
                    "status": "open",
                    "openedAt": None,
                    "updatedAt": now_iso(),
                    "source": self.exchange_id,
                    "entryReason": "synced_from_exchange",
                    "decisionId": None,
                }
            )
        wallet_balance = num(account_row.get("totalEq")) or num(usdt_detail.get("eqUsd")) or num(usdt_detail.get("eq")) or 0
        available_balance = num(usdt_detail.get("availEq")) or num(usdt_detail.get("availBal")) or 0
        return {
            "walletBalanceUsd": wallet_balance,
            "equityUsd": num(account_row.get("totalEq")) or wallet_balance,
            "availableBalanceUsd": available_balance,
            "unrealizedPnlUsd": unrealized_pnl,
            "openPositions": open_positions,
            "raw": {
                "balance": balances,
                "positions": positions,
            },
        }

    def cancel_all_open_orders(self, config: dict[str, Any], symbol: str) -> Any:
        normalized = self.normalize_symbol(symbol)
        result: dict[str, Any] = {"ordersCancelled": [], "algosCancelled": []}
        pending_orders = self._signed_request_json(
            config,
            "GET",
            "/api/v5/trade/orders-pending",
            params={"instType": "SWAP", "instId": normalized},
        )
        orders_to_cancel = []
        for row in pending_orders if isinstance(pending_orders, list) else []:
            ord_id = str(row.get("ordId") or "").strip()
            if ord_id:
                orders_to_cancel.append({"instId": normalized, "ordId": ord_id})
        if orders_to_cancel:
            result["ordersCancelled"] = self._signed_request_json(
                config,
                "POST",
                "/api/v5/trade/cancel-batch-orders",
                body=orders_to_cancel[:20],
            )
        pending_algos = self._signed_request_json(
            config,
            "GET",
            "/api/v5/trade/orders-algo-pending",
            params={"ordType": "conditional"},
        )
        algos_to_cancel = []
        for row in pending_algos if isinstance(pending_algos, list) else []:
            if self.normalize_symbol(row.get("instId")) != normalized:
                continue
            algo_id = str(row.get("algoId") or "").strip()
            if algo_id:
                algos_to_cancel.append({"instId": normalized, "algoId": algo_id})
        if algos_to_cancel:
            result["algosCancelled"] = self._signed_request_json(
                config,
                "POST",
                "/api/v5/trade/cancel-algos",
                body=algos_to_cancel[:20],
            )
        return result

    def place_market_order(
        self,
        config: dict[str, Any],
        *,
        symbol: str,
        side: str,
        quantity: float,
        reduce_only: bool = False,
    ) -> dict[str, Any]:
        body = {
            "instId": self.normalize_symbol(symbol),
            "tdMode": "isolated" if str(config.get("marginType") or "cross").lower() == "isolated" else "cross",
            "side": str(side or "").strip().lower(),
            "ordType": "market",
            "sz": self._format_number(quantity),
            "posSide": "net",
        }
        if reduce_only:
            body["reduceOnly"] = "true"
        rows = self._signed_request_json(config, "POST", "/api/v5/trade/order", body=body)
        return rows[0] if isinstance(rows, list) and rows else {}

    def place_protection_orders(
        self,
        config: dict[str, Any],
        *,
        symbol: str,
        position_side: str,
        quantity: float | None = None,
        stop_loss: float | None,
        take_profit: float | None,
        take_profit_fraction: float | None = None,
    ) -> list[dict[str, Any]]:
        created: list[dict[str, Any]] = []
        if stop_loss is None and take_profit is None:
            return created
        normalized = self.normalize_symbol(symbol)
        close_side = "sell" if position_side == "long" else "buy"
        base_payload = {
            "instId": normalized,
            "tdMode": "isolated" if str(config.get("marginType") or "cross").lower() == "isolated" else "cross",
            "side": close_side,
            "ordType": "conditional",
            "posSide": "net",
        }
        if stop_loss is not None:
            payload = {
                **base_payload,
                "closeFraction": "1",
                "slTriggerPx": self._format_number(self.normalize_price(config, normalized, stop_loss)),
                "slTriggerPxType": "mark",
                "slOrdPx": "-1",
            }
            rows = self._signed_request_json(config, "POST", "/api/v5/trade/order-algo", body=payload)
            if isinstance(rows, list):
                created.extend(rows)
        if take_profit is not None:
            tp_fraction = max(0.05, min(1.0, num(take_profit_fraction) or 1.0))
            payload = {
                **base_payload,
                "tpTriggerPx": self._format_number(self.normalize_price(config, normalized, take_profit)),
                "tpTriggerPxType": "mark",
                "tpOrdPx": "-1",
            }
            if tp_fraction < 0.999 and quantity is not None:
                tp_quantity = self.normalize_quantity(
                    config,
                    normalized,
                    quantity=max(0.0, float(quantity) * tp_fraction),
                )
                payload["sz"] = self._format_number(tp_quantity)
            else:
                payload["closeFraction"] = "1"
            rows = self._signed_request_json(config, "POST", "/api/v5/trade/order-algo", body=payload)
            if isinstance(rows, list):
                created.extend(rows)
        return created
