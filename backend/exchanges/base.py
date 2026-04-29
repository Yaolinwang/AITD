from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class ExchangeGateway(ABC):
    exchange_id: str = ""
    display_name: str = ""
    market_label: str = ""
    default_backdrop_symbol: str = ""

    def candidate_symbol_hint(self) -> str:
        return f"{self.display_name} {self.market_label} symbols".strip()

    def normalize_symbol(self, symbol: str) -> str:
        return str(symbol or "").strip().upper()

    @abstractmethod
    def validate_symbol(self, symbol: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def base_asset_from_symbol(self, symbol: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def fetch_all_tickers_24h(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def fetch_all_premium_index(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def fetch_ticker_24h(self, symbol: str) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def fetch_premium(self, symbol: str) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def fetch_klines(self, symbol: str, interval: str, limit: int) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def resolved_base_url(self, config: dict[str, Any]) -> str:
        raise NotImplementedError

    @abstractmethod
    def live_execution_status(
        self,
        live_config: dict[str, Any] | None = None,
        trading_settings: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def fetch_account_snapshot(self, config: dict[str, Any], session_started_at: str | None = None) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def normalize_quantity(
        self,
        config: dict[str, Any],
        symbol: str,
        *,
        reference_price: float | None = None,
        quantity: float | None = None,
        notional_usd: float | None = None,
    ) -> float:
        raise NotImplementedError

    @abstractmethod
    def normalize_price(self, config: dict[str, Any], symbol: str, price: float) -> float:
        raise NotImplementedError

    @abstractmethod
    def apply_symbol_settings(self, config: dict[str, Any], symbol: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def cancel_all_open_orders(self, config: dict[str, Any], symbol: str) -> Any:
        raise NotImplementedError

    @abstractmethod
    def place_market_order(
        self,
        config: dict[str, Any],
        *,
        symbol: str,
        side: str,
        quantity: float,
        reduce_only: bool = False,
    ) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError
