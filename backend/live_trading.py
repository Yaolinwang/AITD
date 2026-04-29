from __future__ import annotations

from typing import Any

from .config import read_live_trading_config
from .exchanges import get_live_exchange_gateway


def live_execution_status(
    live_config: dict[str, Any] | None = None,
    trading_settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    live_config = live_config or read_live_trading_config()
    gateway = get_live_exchange_gateway(live_config)
    return gateway.live_execution_status(live_config, trading_settings)


def resolved_base_url(config: dict[str, Any]) -> str:
    gateway = get_live_exchange_gateway(config)
    return gateway.resolved_base_url(config)


def normalize_quantity(config: dict[str, Any], symbol: str, *, reference_price: float | None = None, quantity: float | None = None, notional_usd: float | None = None) -> float:
    gateway = get_live_exchange_gateway(config)
    return gateway.normalize_quantity(
        config,
        symbol,
        reference_price=reference_price,
        quantity=quantity,
        notional_usd=notional_usd,
    )


def normalize_price(config: dict[str, Any], symbol: str, price: float) -> float:
    gateway = get_live_exchange_gateway(config)
    return gateway.normalize_price(config, symbol, price)


def apply_symbol_settings(config: dict[str, Any], symbol: str) -> None:
    gateway = get_live_exchange_gateway(config)
    gateway.apply_symbol_settings(config, symbol)


def fetch_account_snapshot(config: dict[str, Any], session_started_at: str | None = None) -> dict[str, Any]:
    gateway = get_live_exchange_gateway(config)
    return gateway.fetch_account_snapshot(config, session_started_at=session_started_at)


def cancel_all_open_orders(config: dict[str, Any], symbol: str) -> Any:
    gateway = get_live_exchange_gateway(config)
    return gateway.cancel_all_open_orders(config, symbol)


def place_market_order(config: dict[str, Any], *, symbol: str, side: str, quantity: float, reduce_only: bool = False) -> dict[str, Any]:
    gateway = get_live_exchange_gateway(config)
    return gateway.place_market_order(
        config,
        symbol=symbol,
        side=side,
        quantity=quantity,
        reduce_only=reduce_only,
    )


def place_protection_orders(
    config: dict[str, Any],
    *,
    symbol: str,
    position_side: str,
    quantity: float | None = None,
    stop_loss: float | None,
    take_profit: float | None,
    take_profit_fraction: float | None = None,
) -> list[dict[str, Any]]:
    gateway = get_live_exchange_gateway(config)
    return gateway.place_protection_orders(
        config,
        symbol=symbol,
        position_side=position_side,
        quantity=quantity,
        stop_loss=stop_loss,
        take_profit=take_profit,
        take_profit_fraction=take_profit_fraction,
    )
