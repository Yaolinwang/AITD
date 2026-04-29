from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlencode

from ..config import read_live_trading_config, read_network_settings
from ..http_client import cached_get_json
from ..utils import num
from .base import ExchangeGateway
from .catalog import exchange_config


class BybitGateway(ExchangeGateway):
    exchange_id = "bybit"
    display_name = "Bybit"
    market_label = "USDT linear perpetual"
    default_backdrop_symbol = "BTCUSDT"
    public_base_url = exchange_config("bybit")["defaultBaseUrl"]
    symbol_pattern = re.compile(r"^[A-Z0-9]{2,}USDT$")
    interval_map = {
        "1m": "1",
        "5m": "5",
        "15m": "15",
        "30m": "30",
        "1h": "60",
        "2h": "120",
        "4h": "240",
        "12h": "720",
    }

    def validate_symbol(self, symbol: str) -> bool:
        normalized = self.normalize_symbol(symbol)
        return bool(self.symbol_pattern.fullmatch(normalized))

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

    def resolved_base_url(self, config: dict[str, Any]) -> str:
        return str(config.get("baseUrl") or self.public_base_url).strip() or self.public_base_url

    def _query(self, base_url: str, endpoint: str, params: dict[str, Any] | None = None) -> str:
        filtered = {key: value for key, value in (params or {}).items() if value not in (None, "", [], {})}
        query = urlencode(filtered)
        if not query:
            return f"{base_url.rstrip('/')}{endpoint}"
        return f"{base_url.rstrip('/')}{endpoint}?{query}"

    def _bybit_result(self, payload: Any, *, endpoint: str) -> Any:
        if not isinstance(payload, dict):
            raise ValueError(f"Unexpected Bybit response for {endpoint}.")
        ret_code = int(payload.get("retCode") or 0)
        if ret_code != 0:
            raise ValueError(f"Bybit {endpoint} failed: {payload.get('retMsg') or ret_code}")
        result = payload.get("result")
        return result if isinstance(result, dict) else {}

    def _public_get_result(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        *,
        namespace: str,
        ttl_seconds: int,
        max_stale_seconds: int,
    ) -> dict[str, Any]:
        network_settings = read_network_settings()
        url = self._query(self.public_base_url, endpoint, params)
        payload = cached_get_json(
            url,
            namespace=f"bybit_{namespace}",
            ttl_seconds=ttl_seconds,
            max_stale_seconds=max_stale_seconds,
            timeout_seconds=45,
            network_settings=network_settings,
        )
        return self._bybit_result(payload, endpoint=endpoint)

    def _map_ticker_row(self, row: dict[str, Any]) -> dict[str, Any]:
        symbol = self.normalize_symbol(row.get("symbol"))
        price_change_ratio = num(row.get("price24hPcnt")) or 0
        return {
            "symbol": symbol,
            "lastPrice": num(row.get("lastPrice")),
            "priceChangePct": price_change_ratio * 100,
            "priceChangePercent": price_change_ratio * 100,
            "quoteVolume": num(row.get("turnover24h")),
            "highPrice": num(row.get("highPrice24h")),
            "lowPrice": num(row.get("lowPrice24h")),
            "openInterest": num(row.get("openInterest")),
            "openInterestValue": num(row.get("openInterestValue")),
            "fundingRate": num(row.get("fundingRate")),
            "nextFundingTime": row.get("nextFundingTime"),
            "indexPrice": num(row.get("indexPrice")),
            "markPrice": num(row.get("markPrice")),
            "raw": row,
        }

    def fetch_all_tickers_24h(self) -> list[dict[str, Any]]:
        result = self._public_get_result(
            "/v5/market/tickers",
            {"category": "linear"},
            namespace="tickers_linear_all",
            ttl_seconds=60,
            max_stale_seconds=45 * 60,
        )
        rows = result.get("list")
        if not isinstance(rows, list):
            return []
        return [self._map_ticker_row(row) for row in rows if isinstance(row, dict)]

    def fetch_all_premium_index(self) -> list[dict[str, Any]]:
        return [
            {
                "symbol": row["symbol"],
                "markPrice": row.get("markPrice"),
                "indexPrice": row.get("indexPrice"),
                "lastFundingRate": row.get("fundingRate"),
                "fundingRate": row.get("fundingRate"),
                "nextFundingTime": row.get("nextFundingTime"),
                "raw": row.get("raw"),
            }
            for row in self.fetch_all_tickers_24h()
        ]

    def fetch_ticker_24h(self, symbol: str) -> dict[str, Any]:
        normalized = self.normalize_symbol(symbol)
        result = self._public_get_result(
            "/v5/market/tickers",
            {"category": "linear", "symbol": normalized},
            namespace=f"ticker_linear_{normalized}",
            ttl_seconds=20,
            max_stale_seconds=30 * 60,
        )
        rows = result.get("list")
        row = rows[0] if isinstance(rows, list) and rows and isinstance(rows[0], dict) else {"symbol": normalized}
        return self._map_ticker_row(row)

    def fetch_premium(self, symbol: str) -> dict[str, Any]:
        ticker = self.fetch_ticker_24h(symbol)
        return {
            "symbol": ticker["symbol"],
            "markPrice": ticker.get("markPrice"),
            "indexPrice": ticker.get("indexPrice"),
            "lastFundingRate": ticker.get("fundingRate"),
            "fundingRate": ticker.get("fundingRate"),
            "fundingPct": (num(ticker.get("fundingRate")) or 0) * 100,
            "nextFundingTime": ticker.get("nextFundingTime"),
            "raw": ticker.get("raw"),
        }

    def fetch_klines(self, symbol: str, interval: str, limit: int) -> list[dict[str, Any]]:
        normalized = self.normalize_symbol(symbol)
        resolved_interval = self.interval_map.get(str(interval or "").lower())
        if not resolved_interval:
            raise ValueError(f"Unsupported Bybit kline interval: {interval}")
        ttl_seconds, max_stale_seconds = self._cache_policy_for_kline_interval(interval)
        result = self._public_get_result(
            "/v5/market/kline",
            {"category": "linear", "symbol": normalized, "interval": resolved_interval, "limit": int(limit)},
            namespace=f"klines_linear_{normalized}_{resolved_interval}",
            ttl_seconds=ttl_seconds,
            max_stale_seconds=max_stale_seconds,
        )
        rows = result.get("list")
        parsed: list[dict[str, Any]] = []
        for row in reversed(rows if isinstance(rows, list) else []):
            if not isinstance(row, list) or len(row) < 7:
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
                    "quoteVolume": num(row[6]),
                }
            )
        return parsed

    def live_execution_status(
        self,
        live_config: dict[str, Any] | None = None,
        trading_settings: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        live_config = live_config or read_live_trading_config()
        issues = ["Bybit live trading is not wired in yet in this Python build."]
        return {
            "configEnabled": live_config.get("enabled") is True,
            "dryRun": True,
            "armed": False,
            "canSync": False,
            "canExecute": False,
            "issues": issues,
            "baseUrl": self.resolved_base_url(live_config),
            "exchange": self.exchange_id,
        }

    def fetch_account_snapshot(self, config: dict[str, Any], session_started_at: str | None = None) -> dict[str, Any]:
        raise NotImplementedError("Bybit live trading is not wired in yet.")

    def normalize_quantity(
        self,
        config: dict[str, Any],
        symbol: str,
        *,
        reference_price: float | None = None,
        quantity: float | None = None,
        notional_usd: float | None = None,
    ) -> float:
        raise NotImplementedError("Bybit live trading is not wired in yet.")

    def normalize_price(self, config: dict[str, Any], symbol: str, price: float) -> float:
        raise NotImplementedError("Bybit live trading is not wired in yet.")

    def apply_symbol_settings(self, config: dict[str, Any], symbol: str) -> None:
        raise NotImplementedError("Bybit live trading is not wired in yet.")

    def cancel_all_open_orders(self, config: dict[str, Any], symbol: str) -> Any:
        raise NotImplementedError("Bybit live trading is not wired in yet.")

    def place_market_order(
        self,
        config: dict[str, Any],
        *,
        symbol: str,
        side: str,
        quantity: float,
        reduce_only: bool = False,
    ) -> dict[str, Any]:
        raise NotImplementedError("Bybit live trading is not wired in yet.")

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
        raise NotImplementedError("Bybit live trading is not wired in yet.")
