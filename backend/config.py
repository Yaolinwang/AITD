from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import re
from typing import Any

from .exchanges.catalog import DEFAULT_EXCHANGE_ID, exchange_catalog_payload, exchange_config, normalize_exchange_id
from .utils import CONFIG_DIR, clamp, clean_bool, current_run_date, now_iso, read_json, write_json


TRADING_SETTINGS_PATH = CONFIG_DIR / "trading_agent.json"
DASHBOARD_SETTINGS_PATH = CONFIG_DIR / "dashboard_settings.json"
FIXED_UNIVERSE_PATH = CONFIG_DIR / "fixed_universe.json"
CANDIDATE_SOURCE_PATH = CONFIG_DIR / "candidate_source.py"
LIVE_TRADING_PATH = CONFIG_DIR / "live_trading.json"
LLM_PROVIDER_PATH = CONFIG_DIR / "llm_provider.json"
NETWORK_SETTINGS_PATH = CONFIG_DIR / "network.json"
PROMPT_SETTINGS_PATH = CONFIG_DIR / "trading_prompt.json"
PROMPT_LIBRARY_PATH = CONFIG_DIR / "trading_prompt_library.json"


def _instance_path(instance_id: str | None, key: str, legacy_path: Path) -> Path:
    if not instance_id:
        return legacy_path
    from .instances import ensure_instances_migrated, instance_paths

    ensure_instances_migrated()
    return instance_paths(instance_id)[key]


DEFAULT_TRADING_SETTINGS = {
    "version": 2,
    "updated": "2026-04-20",
    "description": "Python-only local trading agent settings.",
    "mode": "paper",
    "activeExchange": DEFAULT_EXCHANGE_ID,
    "server": {
        "host": "127.0.0.1",
        "port": 8788,
    },
    "decisionIntervalMinutes": 15,
    "initialCapitalUsd": 1000,
    "maxNewPositionsPerCycle": 1,
    "maxOpenPositions": 4,
    "maxPositionNotionalUsd": 150,
    "maxGrossExposurePct": 100,
    "maxAccountDrawdownPct": 20,
    "riskPerTradePct": 2.5,
    "minConfidence": 60,
    "paperFeesBps": 4,
    "allowShorts": True,
    "paperTrading": {
        "enabled": False,
    },
    "liveTrading": {
        "enabled": False,
    },
    "selfLearning": {
        "enabled": False,
        "maxLessons": 5,
        "lookbackDecisions": 300,
        "reviewHorizonHours": 4,
    },
    "liveExecution": {
        "configPath": "config/live_trading.json",
        "useExchangeProtectionOrders": True,
        "note": "Only enable live routing after paper verification, exchange API setup, and tiny-size dry-run validation."
    }
}


DEFAULT_DASHBOARD_SETTINGS = {
    "version": 1,
    "updated": "2026-04-20",
    "timezone": "Asia/Shanghai",
    "pageAutoRefreshSeconds": 30,
    "marketAutoScanEnabled": True,
    "marketScanIntervalMinutes": 60,
    "marketScanOffsetMinute": 7,
}


DEFAULT_LIVE_TRADING_SETTINGS = {
    "version": 1,
    "updated": "2026-04-20",
    "exchange": DEFAULT_EXCHANGE_ID,
    "market": exchange_config(DEFAULT_EXCHANGE_ID)["market"],
    "enabled": False,
    "baseUrl": exchange_config(DEFAULT_EXCHANGE_ID)["defaultBaseUrl"],
    "apiKey": "",
    "apiSecret": "",
    "apiPassphrase": "",
    "recvWindow": 5000,
    "dryRun": True,
    "positionMode": "oneway",
    "marginType": "cross",
    "defaultLeverage": 3,
    "note": "Fill the API credentials, keep dryRun on first, and only disable dryRun after you have verified the full flow."
}


DEFAULT_PROVIDER_SETTINGS = {
    "version": 1,
    "updated": "2026-04-20",
    "preset": "gpt",
    "apiStyle": "openai",
    "model": "gpt-5.4-mini",
    "baseUrl": "https://api.openai.com/v1",
    "apiKey": "",
    "timeoutSeconds": 45,
    "temperature": 0.2,
    "maxOutputTokens": 1200,
    "bypassProxy": False,
    "anthropicVersion": "2023-06-01",
    "customHeaders": {},
}


DEFAULT_NETWORK_SETTINGS = {
    "version": 1,
    "updated": "2026-04-20",
    "proxyEnabled": False,
    "proxyUrl": "",
    "noProxy": ["127.0.0.1", "localhost"],
}


DEFAULT_FIXED_UNIVERSE_SETTINGS = {
    "version": 1,
    "updated": "2026-04-20",
    "description": "Fixed futures symbol universe for the open-source trading-agent build. Edit this list to change what the agent reviews.",
    "symbols": [
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
        "DOGEUSDT",
        "1000PEPEUSDT",
    ],
    "dynamicSource": {
        "enabled": False,
        "functionName": "load_candidate_symbols",
        "functionFile": "config/candidate_source.py",
    },
}


DEFAULT_CANDIDATE_SOURCE_CODE = """def load_candidate_symbols(context):
    return context["manual_symbols"]
"""


PROMPT_KLINE_FEED_OPTIONS = ["1m", "5m", "15m"]


DEFAULT_PROMPT_SETTINGS = {
    "version": 1,
    "updated": "2026-04-20",
    "name": "default_trading_logic",
    "presetId": None,
    "klineFeeds": {
        "1m": {"enabled": False, "limit": 120},
        "5m": {"enabled": False, "limit": 96},
        "15m": {"enabled": True, "limit": 64},
    },
    "decision_logic": {
        "role": "You are a careful crypto futures trader.",
        "core_principles": [
            "Protect capital first."
        ],
        "entry_preferences": [
            "Only trade clear setups."
        ],
        "position_management": [
            "Cut losers quickly and take profit simply."
        ],
        "response_style": [
            "Return strict JSON only.",
            "Every action must include a short reason tied to current market structure."
        ]
    }
}


DEFAULT_PROMPT_LIBRARY_SETTINGS = {
    "version": 1,
    "updated": "2026-04-21",
    "prompts": [],
}


PROVIDER_PRESET_MAP = {
    "gpt": {
        "apiStyle": "openai",
        "baseUrl": "https://api.openai.com/v1",
    },
    "kimi": {
        "apiStyle": "openai",
        "baseUrl": "https://api.moonshot.ai/v1",
    },
    "deepseek": {
        "apiStyle": "openai",
        "baseUrl": "https://api.deepseek.com/v1",
    },
    "qwen": {
        "apiStyle": "openai",
        "baseUrl": "https://dashscope.aliyuncs.com/compatible-mode/v1",
    },
    "claude": {
        "apiStyle": "anthropic",
        "baseUrl": "https://api.anthropic.com/v1",
    },
    "custom": {
        "apiStyle": "openai",
        "baseUrl": "",
    },
}


def _with_default_file(path: Path, default_payload: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        write_json(path, default_payload)
    payload = read_json(path, {})
    if not isinstance(payload, dict):
        payload = {}
    return payload


def _with_default_text_file(path: Path, default_text: str) -> str:
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(default_text.rstrip() + "\n", encoding="utf-8")
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return default_text.rstrip() + "\n"


def read_trading_settings(instance_id: str | None = None) -> dict[str, Any]:
    payload = _with_default_file(_instance_path(instance_id, "trading_settings", TRADING_SETTINGS_PATH), DEFAULT_TRADING_SETTINGS)
    normalized_payload = {key: value for key, value in payload.items() if key in DEFAULT_TRADING_SETTINGS}
    live_settings = payload.get("liveExecution") if isinstance(payload.get("liveExecution"), dict) else {}
    server_settings = payload.get("server") if isinstance(payload.get("server"), dict) else {}
    paper_trading = payload.get("paperTrading") if isinstance(payload.get("paperTrading"), dict) else {}
    live_trading = payload.get("liveTrading") if isinstance(payload.get("liveTrading"), dict) else {}
    self_learning = payload.get("selfLearning") if isinstance(payload.get("selfLearning"), dict) else {}
    return {
        **deepcopy(DEFAULT_TRADING_SETTINGS),
        **normalized_payload,
        "updated": payload.get("updated") or current_run_date(),
        "mode": "live" if str(payload.get("mode", "paper")).strip().lower() == "live" else "paper",
        "activeExchange": normalize_exchange_id(payload.get("activeExchange"), capability="market"),
        "decisionIntervalMinutes": int(clamp(payload.get("decisionIntervalMinutes"), 5, 1440)),
        "initialCapitalUsd": clamp(payload.get("initialCapitalUsd"), 1, 1_000_000),
        "maxNewPositionsPerCycle": int(clamp(payload.get("maxNewPositionsPerCycle"), 1, 100)),
        "maxOpenPositions": int(clamp(payload.get("maxOpenPositions"), 1, 1000)),
        "maxPositionNotionalUsd": clamp(payload.get("maxPositionNotionalUsd"), 20, 10_000_000_000),
        "maxGrossExposurePct": clamp(payload.get("maxGrossExposurePct"), 10, 10_000),
        "maxAccountDrawdownPct": clamp(payload.get("maxAccountDrawdownPct"), 5, 90),
        "riskPerTradePct": clamp(payload.get("riskPerTradePct"), 0.25, 100),
        "minConfidence": clamp(payload.get("minConfidence"), 1, 100),
        "paperFeesBps": clamp(payload.get("paperFeesBps"), 0, 50),
        "allowShorts": clean_bool(payload.get("allowShorts"), True),
        "paperTrading": {
            **deepcopy(DEFAULT_TRADING_SETTINGS["paperTrading"]),
            **paper_trading,
            "enabled": clean_bool(paper_trading.get("enabled"), DEFAULT_TRADING_SETTINGS["paperTrading"]["enabled"]),
        },
        "liveTrading": {
            **deepcopy(DEFAULT_TRADING_SETTINGS["liveTrading"]),
            **live_trading,
            "enabled": clean_bool(live_trading.get("enabled"), DEFAULT_TRADING_SETTINGS["liveTrading"]["enabled"]),
        },
        "selfLearning": {
            **deepcopy(DEFAULT_TRADING_SETTINGS["selfLearning"]),
            **self_learning,
            "enabled": clean_bool(self_learning.get("enabled"), DEFAULT_TRADING_SETTINGS["selfLearning"]["enabled"]),
            "maxLessons": int(clamp(self_learning.get("maxLessons", DEFAULT_TRADING_SETTINGS["selfLearning"]["maxLessons"]), 1, 10)),
            "lookbackDecisions": int(clamp(self_learning.get("lookbackDecisions", DEFAULT_TRADING_SETTINGS["selfLearning"]["lookbackDecisions"]), 20, 2000)),
            "reviewHorizonHours": int(clamp(self_learning.get("reviewHorizonHours", DEFAULT_TRADING_SETTINGS["selfLearning"]["reviewHorizonHours"]), 1, 72)),
        },
        "server": {
            "host": str(server_settings.get("host") or DEFAULT_TRADING_SETTINGS["server"]["host"]),
            "port": int(clamp(server_settings.get("port"), 1024, 65535)),
        },
        "liveExecution": {
            **deepcopy(DEFAULT_TRADING_SETTINGS["liveExecution"]),
            "configPath": str(live_settings.get("configPath") or DEFAULT_TRADING_SETTINGS["liveExecution"]["configPath"]),
            "useExchangeProtectionOrders": clean_bool(
                live_settings.get("useExchangeProtectionOrders"),
                DEFAULT_TRADING_SETTINGS["liveExecution"]["useExchangeProtectionOrders"],
            ),
        },
    }


def write_trading_settings(patch: dict[str, Any], instance_id: str | None = None) -> dict[str, Any]:
    current = read_trading_settings(instance_id)
    normalized_patch = {key: value for key, value in patch.items() if key in current}
    live_patch = patch.get("liveExecution") if isinstance(patch.get("liveExecution"), dict) else {}
    server_patch = patch.get("server") if isinstance(patch.get("server"), dict) else {}
    paper_trading_patch = patch.get("paperTrading") if isinstance(patch.get("paperTrading"), dict) else {}
    live_trading_patch = patch.get("liveTrading") if isinstance(patch.get("liveTrading"), dict) else {}
    self_learning_patch = patch.get("selfLearning") if isinstance(patch.get("selfLearning"), dict) else {}
    next_payload = {
        **current,
        **normalized_patch,
        "updated": current_run_date(),
        "mode": "live" if str(patch.get("mode", current["mode"])).strip().lower() == "live" else "paper",
        "activeExchange": normalize_exchange_id(patch.get("activeExchange", current["activeExchange"]), capability="market"),
        "decisionIntervalMinutes": int(clamp(patch.get("decisionIntervalMinutes", current["decisionIntervalMinutes"]), 5, 1440)),
        "initialCapitalUsd": clamp(patch.get("initialCapitalUsd", current["initialCapitalUsd"]), 1, 1_000_000),
        "maxNewPositionsPerCycle": int(clamp(patch.get("maxNewPositionsPerCycle", current["maxNewPositionsPerCycle"]), 1, 100)),
        "maxOpenPositions": int(clamp(patch.get("maxOpenPositions", current["maxOpenPositions"]), 1, 1000)),
        "maxPositionNotionalUsd": clamp(patch.get("maxPositionNotionalUsd", current["maxPositionNotionalUsd"]), 20, 10_000_000_000),
        "maxGrossExposurePct": clamp(patch.get("maxGrossExposurePct", current["maxGrossExposurePct"]), 10, 10_000),
        "maxAccountDrawdownPct": clamp(patch.get("maxAccountDrawdownPct", current["maxAccountDrawdownPct"]), 5, 90),
        "riskPerTradePct": clamp(patch.get("riskPerTradePct", current["riskPerTradePct"]), 0.25, 100),
        "minConfidence": clamp(patch.get("minConfidence", current["minConfidence"]), 1, 100),
        "paperFeesBps": clamp(patch.get("paperFeesBps", current["paperFeesBps"]), 0, 50),
        "allowShorts": patch.get("allowShorts", current["allowShorts"]) is not False,
        "paperTrading": {
            **current["paperTrading"],
            **paper_trading_patch,
            "enabled": clean_bool(paper_trading_patch.get("enabled"), current["paperTrading"]["enabled"]),
        },
        "liveTrading": {
            **current["liveTrading"],
            **live_trading_patch,
            "enabled": clean_bool(live_trading_patch.get("enabled"), current["liveTrading"]["enabled"]),
        },
        "selfLearning": {
            **current["selfLearning"],
            **self_learning_patch,
            "enabled": clean_bool(self_learning_patch.get("enabled"), current["selfLearning"]["enabled"]),
            "maxLessons": int(clamp(self_learning_patch.get("maxLessons", current["selfLearning"]["maxLessons"]), 1, 10)),
            "lookbackDecisions": int(clamp(self_learning_patch.get("lookbackDecisions", current["selfLearning"]["lookbackDecisions"]), 20, 2000)),
            "reviewHorizonHours": int(clamp(self_learning_patch.get("reviewHorizonHours", current["selfLearning"]["reviewHorizonHours"]), 1, 72)),
        },
        "server": {
            "host": str(server_patch.get("host") or current["server"]["host"]),
            "port": int(clamp(server_patch.get("port", current["server"]["port"]), 1024, 65535)),
        },
        "liveExecution": {
            **current["liveExecution"],
            "configPath": str(live_patch.get("configPath") or current["liveExecution"]["configPath"]),
            "useExchangeProtectionOrders": clean_bool(
                live_patch.get("useExchangeProtectionOrders"),
                current["liveExecution"]["useExchangeProtectionOrders"],
            ),
        },
    }
    write_json(_instance_path(instance_id, "trading_settings", TRADING_SETTINGS_PATH), next_payload)
    return next_payload


def read_dashboard_settings() -> dict[str, Any]:
    payload = _with_default_file(DASHBOARD_SETTINGS_PATH, DEFAULT_DASHBOARD_SETTINGS)
    return {
        **deepcopy(DEFAULT_DASHBOARD_SETTINGS),
        **payload,
        "updated": payload.get("updated") or current_run_date(),
        "pageAutoRefreshSeconds": int(clamp(payload.get("pageAutoRefreshSeconds"), 10, 600)),
        "marketAutoScanEnabled": clean_bool(payload.get("marketAutoScanEnabled"), True),
        "marketScanIntervalMinutes": int(clamp(payload.get("marketScanIntervalMinutes"), 15, 1440)),
        "marketScanOffsetMinute": int(clamp(payload.get("marketScanOffsetMinute"), 0, 59)),
    }


def write_dashboard_settings(patch: dict[str, Any]) -> dict[str, Any]:
    current = read_dashboard_settings()
    next_payload = {
        **current,
        **patch,
        "updated": current_run_date(),
        "pageAutoRefreshSeconds": int(clamp(patch.get("pageAutoRefreshSeconds", current["pageAutoRefreshSeconds"]), 10, 600)),
        "marketAutoScanEnabled": clean_bool(patch.get("marketAutoScanEnabled"), current["marketAutoScanEnabled"]),
        "marketScanIntervalMinutes": int(clamp(patch.get("marketScanIntervalMinutes", current["marketScanIntervalMinutes"]), 15, 1440)),
        "marketScanOffsetMinute": int(clamp(patch.get("marketScanOffsetMinute", current["marketScanOffsetMinute"]), 0, 59)),
    }
    write_json(DASHBOARD_SETTINGS_PATH, next_payload)
    return next_payload


def read_candidate_source_code(instance_id: str | None = None) -> str:
    return _with_default_text_file(_instance_path(instance_id, "candidate_source", CANDIDATE_SOURCE_PATH), DEFAULT_CANDIDATE_SOURCE_CODE)


def write_candidate_source_code(code: str, instance_id: str | None = None) -> str:
    text = str(code or "").rstrip() + "\n"
    path = _instance_path(instance_id, "candidate_source", CANDIDATE_SOURCE_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return text


def _normalized_symbol_values(raw_symbols: Any, fallback: list[str]) -> list[str]:
    if isinstance(raw_symbols, str):
        candidate_values = [item.strip().upper() for item in raw_symbols.replace(",", "\n").splitlines()]
    elif isinstance(raw_symbols, list):
        candidate_values = [str(item).strip().upper() for item in raw_symbols]
    else:
        candidate_values = fallback
    symbols: list[str] = []
    for item in candidate_values:
        if item and item not in symbols:
            symbols.append(item)
    return symbols


def _merged_fixed_universe_payload(current: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    raw_symbols = patch.get("symbols", current["symbols"])
    dynamic_patch = patch.get("dynamicSource") if isinstance(patch.get("dynamicSource"), dict) else {}
    symbols = _normalized_symbol_values(raw_symbols, current["symbols"])
    return {
        **current,
        "updated": current_run_date(),
        "symbols": symbols,
        "dynamicSource": {
            **current["dynamicSource"],
            **dynamic_patch,
            "enabled": clean_bool(dynamic_patch.get("enabled"), current["dynamicSource"]["enabled"]),
            "functionName": str(dynamic_patch.get("functionName") or current["dynamicSource"]["functionName"]).strip() or DEFAULT_FIXED_UNIVERSE_SETTINGS["dynamicSource"]["functionName"],
            "functionFile": str(dynamic_patch.get("functionFile") or current["dynamicSource"]["functionFile"]).strip() or DEFAULT_FIXED_UNIVERSE_SETTINGS["dynamicSource"]["functionFile"],
        },
    }


def read_fixed_universe(instance_id: str | None = None) -> dict[str, Any]:
    payload = _with_default_file(_instance_path(instance_id, "fixed_universe", FIXED_UNIVERSE_PATH), DEFAULT_FIXED_UNIVERSE_SETTINGS)
    _ = read_candidate_source_code(instance_id)
    payload_without_scoring = {key: value for key, value in payload.items() if key != "scoring"}
    symbols = _normalized_symbol_values(payload.get("symbols", []), DEFAULT_FIXED_UNIVERSE_SETTINGS["symbols"])
    dynamic_source = payload.get("dynamicSource") if isinstance(payload.get("dynamicSource"), dict) else {}
    return {
        **deepcopy(DEFAULT_FIXED_UNIVERSE_SETTINGS),
        **payload_without_scoring,
        "version": int(payload.get("version") or DEFAULT_FIXED_UNIVERSE_SETTINGS["version"]),
        "updated": payload.get("updated") or current_run_date(),
        "description": payload.get("description") or DEFAULT_FIXED_UNIVERSE_SETTINGS["description"],
        "symbols": symbols,
        "dynamicSource": {
            **deepcopy(DEFAULT_FIXED_UNIVERSE_SETTINGS["dynamicSource"]),
            **dynamic_source,
            "enabled": clean_bool(dynamic_source.get("enabled"), DEFAULT_FIXED_UNIVERSE_SETTINGS["dynamicSource"]["enabled"]),
            "functionName": str(dynamic_source.get("functionName") or DEFAULT_FIXED_UNIVERSE_SETTINGS["dynamicSource"]["functionName"]).strip(),
            "functionFile": str(dynamic_source.get("functionFile") or DEFAULT_FIXED_UNIVERSE_SETTINGS["dynamicSource"]["functionFile"]).strip(),
        },
    }


def preview_fixed_universe(patch: dict[str, Any], instance_id: str | None = None) -> dict[str, Any]:
    current = read_fixed_universe(instance_id)
    return _merged_fixed_universe_payload(current, patch)


def write_fixed_universe(patch: dict[str, Any], instance_id: str | None = None) -> dict[str, Any]:
    next_payload = preview_fixed_universe(patch, instance_id)
    if "candidateSourceCode" in patch:
        write_candidate_source_code(str(patch.get("candidateSourceCode") or ""), instance_id)
    write_json(_instance_path(instance_id, "fixed_universe", FIXED_UNIVERSE_PATH), next_payload)
    return next_payload


def _normalized_provider_defaults(preset: str) -> dict[str, Any]:
    return PROVIDER_PRESET_MAP.get(preset, PROVIDER_PRESET_MAP["custom"])


def _normalized_api_key(value: Any) -> str:
    text = str(value or "").strip().strip('"').strip("'")
    if text.lower().startswith("bearer "):
        text = text[7:].strip()
    return text


def read_llm_provider(instance_id: str | None = None) -> dict[str, Any]:
    payload = _with_default_file(_instance_path(instance_id, "llm_provider", LLM_PROVIDER_PATH), DEFAULT_PROVIDER_SETTINGS)
    preset = str(payload.get("preset") or DEFAULT_PROVIDER_SETTINGS["preset"]).strip().lower()
    preset_defaults = _normalized_provider_defaults(preset)
    base_url = str(payload.get("baseUrl") or preset_defaults["baseUrl"] or "").strip()
    api_style = str(payload.get("apiStyle") or preset_defaults["apiStyle"]).strip().lower()
    if api_style not in {"openai", "anthropic"}:
        api_style = preset_defaults["apiStyle"]
    return {
        **deepcopy(DEFAULT_PROVIDER_SETTINGS),
        **payload,
        "updated": payload.get("updated") or current_run_date(),
        "preset": preset if preset in PROVIDER_PRESET_MAP else "custom",
        "apiStyle": api_style,
        "model": str(payload.get("model") or DEFAULT_PROVIDER_SETTINGS["model"]).strip(),
        "baseUrl": base_url,
        "apiKey": _normalized_api_key(payload.get("apiKey")),
        "timeoutSeconds": int(clamp(payload.get("timeoutSeconds"), 10, 180)),
        "temperature": clamp(payload.get("temperature"), 0, 1.5),
        "maxOutputTokens": int(clamp(payload.get("maxOutputTokens"), 256, 4096)),
        "bypassProxy": clean_bool(payload.get("bypassProxy"), False),
        "anthropicVersion": str(payload.get("anthropicVersion") or DEFAULT_PROVIDER_SETTINGS["anthropicVersion"]),
        "customHeaders": payload.get("customHeaders") if isinstance(payload.get("customHeaders"), dict) else {},
    }


def write_llm_provider(patch: dict[str, Any], instance_id: str | None = None) -> dict[str, Any]:
    current = read_llm_provider(instance_id)
    preset = str(patch.get("preset", current["preset"])).strip().lower()
    if preset not in PROVIDER_PRESET_MAP:
        preset = "custom"
    preset_defaults = _normalized_provider_defaults(preset)
    api_style = str(patch.get("apiStyle", current["apiStyle"] or preset_defaults["apiStyle"])).strip().lower()
    if api_style not in {"openai", "anthropic"}:
        api_style = preset_defaults["apiStyle"]
    next_payload = {
        **current,
        **patch,
        "updated": current_run_date(),
        "preset": preset,
        "apiStyle": api_style,
        "baseUrl": str(patch.get("baseUrl") or current.get("baseUrl") or preset_defaults["baseUrl"]).strip(),
        "model": str(patch.get("model") or current["model"]).strip(),
        "apiKey": _normalized_api_key(patch.get("apiKey", current["apiKey"])),
        "timeoutSeconds": int(clamp(patch.get("timeoutSeconds", current["timeoutSeconds"]), 10, 180)),
        "temperature": clamp(patch.get("temperature", current["temperature"]), 0, 1.5),
        "maxOutputTokens": int(clamp(patch.get("maxOutputTokens", current["maxOutputTokens"]), 256, 4096)),
        "bypassProxy": clean_bool(patch.get("bypassProxy", current["bypassProxy"]), False),
        "anthropicVersion": str(patch.get("anthropicVersion", current["anthropicVersion"])),
        "customHeaders": patch.get("customHeaders") if isinstance(patch.get("customHeaders"), dict) else current["customHeaders"],
    }
    write_json(_instance_path(instance_id, "llm_provider", LLM_PROVIDER_PATH), next_payload)
    return next_payload


def read_network_settings(instance_id: str | None = None) -> dict[str, Any]:
    payload = _with_default_file(_instance_path(instance_id, "network", NETWORK_SETTINGS_PATH), DEFAULT_NETWORK_SETTINGS)
    no_proxy = payload.get("noProxy")
    if isinstance(no_proxy, str):
        normalized_no_proxy = [item.strip() for item in no_proxy.split(",") if item.strip()]
    elif isinstance(no_proxy, list):
        normalized_no_proxy = [str(item).strip() for item in no_proxy if str(item).strip()]
    else:
        normalized_no_proxy = deepcopy(DEFAULT_NETWORK_SETTINGS["noProxy"])
    return {
        **deepcopy(DEFAULT_NETWORK_SETTINGS),
        **payload,
        "updated": payload.get("updated") or current_run_date(),
        "proxyEnabled": clean_bool(payload.get("proxyEnabled"), False),
        "proxyUrl": str(payload.get("proxyUrl") or "").strip(),
        "noProxy": normalized_no_proxy,
    }


def write_network_settings(patch: dict[str, Any], instance_id: str | None = None) -> dict[str, Any]:
    current = read_network_settings(instance_id)
    no_proxy = patch.get("noProxy", current["noProxy"])
    if isinstance(no_proxy, str):
        normalized_no_proxy = [item.strip() for item in no_proxy.split(",") if item.strip()]
    elif isinstance(no_proxy, list):
        normalized_no_proxy = [str(item).strip() for item in no_proxy if str(item).strip()]
    else:
        normalized_no_proxy = current["noProxy"]
    next_payload = {
        **current,
        **patch,
        "updated": current_run_date(),
        "proxyEnabled": clean_bool(patch.get("proxyEnabled"), current["proxyEnabled"]),
        "proxyUrl": str(patch.get("proxyUrl", current["proxyUrl"])).strip(),
        "noProxy": normalized_no_proxy,
    }
    write_json(_instance_path(instance_id, "network", NETWORK_SETTINGS_PATH), next_payload)
    return next_payload


def read_live_exchange_catalog() -> list[dict[str, Any]]:
    return exchange_catalog_payload()


def read_live_trading_config(instance_id: str | None = None) -> dict[str, Any]:
    payload = _with_default_file(_instance_path(instance_id, "live_trading", LIVE_TRADING_PATH), DEFAULT_LIVE_TRADING_SETTINGS)
    normalized_payload = {key: value for key, value in payload.items() if key in DEFAULT_LIVE_TRADING_SETTINGS}
    exchange_id = normalize_exchange_id(payload.get("exchange"), capability="trade")
    exchange_meta = exchange_config(exchange_id)
    enabled = clean_bool(payload.get("enabled"), False)
    dry_run = False if enabled else clean_bool(payload.get("dryRun"), True)
    return {
        **deepcopy(DEFAULT_LIVE_TRADING_SETTINGS),
        **normalized_payload,
        "updated": payload.get("updated") or current_run_date(),
        "exchange": exchange_id,
        "market": exchange_meta["market"],
        "enabled": enabled,
        "dryRun": dry_run,
        "recvWindow": int(clamp(payload.get("recvWindow"), 1000, 60000)),
        "positionMode": "hedge" if str(payload.get("positionMode")).strip().lower() == "hedge" else "oneway",
        "marginType": "isolated" if str(payload.get("marginType")).strip().lower() == "isolated" else "cross",
        "defaultLeverage": int(clamp(payload.get("defaultLeverage"), 1, 125)),
        "apiKey": str(payload.get("apiKey") or ""),
        "apiSecret": str(payload.get("apiSecret") or ""),
        "apiPassphrase": str(payload.get("apiPassphrase") or ""),
        "baseUrl": str(payload.get("baseUrl") or exchange_meta["defaultBaseUrl"]),
        "note": str(payload.get("note") or DEFAULT_LIVE_TRADING_SETTINGS["note"]),
    }


def write_live_trading_config(patch: dict[str, Any], instance_id: str | None = None) -> dict[str, Any]:
    current = read_live_trading_config(instance_id)
    normalized_patch = {key: value for key, value in patch.items() if key in current}
    exchange_id = normalize_exchange_id(patch.get("exchange", current["exchange"]), capability="trade")
    exchange_meta = exchange_config(exchange_id)
    enabled = clean_bool(patch.get("enabled"), current["enabled"])
    dry_run = False if enabled else clean_bool(patch.get("dryRun"), current["dryRun"])
    requested_base_url = str(patch.get("baseUrl", current["baseUrl"])).strip()
    if not requested_base_url:
        requested_base_url = exchange_meta["defaultBaseUrl"]
    next_payload = {
        **current,
        **normalized_patch,
        "updated": current_run_date(),
        "exchange": exchange_id,
        "market": exchange_meta["market"],
        "enabled": enabled,
        "dryRun": dry_run,
        "recvWindow": int(clamp(patch.get("recvWindow", current["recvWindow"]), 1000, 60000)),
        "positionMode": "hedge" if str(patch.get("positionMode", current["positionMode"])).strip().lower() == "hedge" else "oneway",
        "marginType": "isolated" if str(patch.get("marginType", current["marginType"])).strip().lower() == "isolated" else "cross",
        "defaultLeverage": int(clamp(patch.get("defaultLeverage", current["defaultLeverage"]), 1, 125)),
        "apiKey": str(patch.get("apiKey", current["apiKey"])),
        "apiSecret": str(patch.get("apiSecret", current["apiSecret"])),
        "apiPassphrase": str(patch.get("apiPassphrase", current["apiPassphrase"])),
        "baseUrl": requested_base_url,
        "note": str(patch.get("note", current["note"])),
    }
    write_json(_instance_path(instance_id, "live_trading", LIVE_TRADING_PATH), next_payload)
    return next_payload


def _normalized_prompt_kline_feeds(raw_feeds: Any, legacy_intervals: Any = None) -> dict[str, dict[str, Any]]:
    defaults = deepcopy(DEFAULT_PROMPT_SETTINGS["klineFeeds"])
    if isinstance(raw_feeds, dict):
        normalized = {}
        for interval in PROMPT_KLINE_FEED_OPTIONS:
            current = raw_feeds.get(interval) if isinstance(raw_feeds.get(interval), dict) else {}
            normalized[interval] = {
                "enabled": clean_bool(current.get("enabled"), defaults[interval]["enabled"]),
                "limit": int(clamp(current.get("limit"), 1, 300)),
            }
        if any(item["enabled"] for item in normalized.values()):
            return normalized
        normalized["15m"]["enabled"] = True
        return normalized
    enabled_from_legacy: set[str] = set()
    if isinstance(legacy_intervals, str):
        legacy_values = [item.strip().lower() for item in legacy_intervals.replace(",", "\n").splitlines()]
    elif isinstance(legacy_intervals, list):
        legacy_values = [str(item).strip().lower() for item in legacy_intervals]
    else:
        legacy_values = []
    for item in legacy_values:
        if item in PROMPT_KLINE_FEED_OPTIONS:
            enabled_from_legacy.add(item)
    normalized = deepcopy(defaults)
    if enabled_from_legacy:
        for interval in PROMPT_KLINE_FEED_OPTIONS:
            normalized[interval]["enabled"] = interval in enabled_from_legacy
    return normalized


def read_prompt_settings(instance_id: str | None = None) -> dict[str, Any]:
    payload = _with_default_file(_instance_path(instance_id, "prompt", PROMPT_SETTINGS_PATH), DEFAULT_PROMPT_SETTINGS)
    decision_logic = payload.get("decision_logic")
    if not isinstance(decision_logic, dict):
        decision_logic = deepcopy(DEFAULT_PROMPT_SETTINGS["decision_logic"])
    return {
        **deepcopy(DEFAULT_PROMPT_SETTINGS),
        **payload,
        "updated": payload.get("updated") or current_run_date(),
        "name": str(payload.get("name") or DEFAULT_PROMPT_SETTINGS["name"]),
        "presetId": str(payload.get("presetId")).strip() if str(payload.get("presetId") or "").strip() else None,
        "klineFeeds": _normalized_prompt_kline_feeds(payload.get("klineFeeds"), payload.get("klineIntervals")),
        "decision_logic": decision_logic,
    }


def write_prompt_settings(patch: dict[str, Any], instance_id: str | None = None) -> dict[str, Any]:
    current = read_prompt_settings(instance_id)
    next_payload = {
        **current,
        **patch,
        "updated": current_run_date(),
        "name": str(patch.get("name", current["name"])),
        "presetId": str(patch.get("presetId", current.get("presetId"))).strip() if str(patch.get("presetId", current.get("presetId")) or "").strip() else None,
        "klineFeeds": _normalized_prompt_kline_feeds(patch.get("klineFeeds", current["klineFeeds"])),
        "decision_logic": patch.get("decision_logic") if isinstance(patch.get("decision_logic"), dict) else current["decision_logic"],
    }
    write_json(_instance_path(instance_id, "prompt", PROMPT_SETTINGS_PATH), next_payload)
    return next_payload


def _prompt_preset_name(value: Any, fallback: str = "untitled_prompt") -> str:
    text = str(value or "").strip()
    return text or fallback


def _prompt_preset_slug(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")
    return cleaned or "prompt"


def _normalized_prompt_preset(payload: dict[str, Any], *, preset_id: str | None = None) -> dict[str, Any]:
    decision_logic = payload.get("decision_logic")
    if not isinstance(decision_logic, dict):
        decision_logic = deepcopy(DEFAULT_PROMPT_SETTINGS["decision_logic"])
    return {
        "id": str(preset_id or payload.get("id") or "").strip(),
        "name": _prompt_preset_name(payload.get("name"), DEFAULT_PROMPT_SETTINGS["name"]),
        "updatedAt": str(payload.get("updatedAt") or now_iso()),
        "klineFeeds": _normalized_prompt_kline_feeds(payload.get("klineFeeds"), payload.get("klineIntervals")),
        "decision_logic": {
            **deepcopy(DEFAULT_PROMPT_SETTINGS["decision_logic"]),
            **decision_logic,
            "response_style": list(DEFAULT_PROMPT_SETTINGS["decision_logic"]["response_style"]),
        },
    }


def _prompt_preset_signature(payload: dict[str, Any]) -> str:
    normalized = _normalized_prompt_preset(payload)
    comparable = {
        "name": normalized.get("name"),
        "klineFeeds": normalized.get("klineFeeds"),
        "decision_logic": normalized.get("decision_logic"),
    }
    import json

    return json.dumps(comparable, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def read_prompt_library(instance_id: str | None = None) -> dict[str, Any]:
    payload = _with_default_file(PROMPT_LIBRARY_PATH, DEFAULT_PROMPT_LIBRARY_SETTINGS)
    raw_prompts: list[Any] = list(payload.get("prompts") if isinstance(payload.get("prompts"), list) else [])
    try:
        from .instances import instance_paths, list_instances

        for instance in list_instances():
            extra_payload = read_json(instance_paths(instance["id"])["prompt_library"], None)
            if not isinstance(extra_payload, dict):
                continue
            extra_prompts = extra_payload.get("prompts") if isinstance(extra_payload.get("prompts"), list) else []
            raw_prompts.extend(extra_prompts)
    except Exception:
        pass
    prompts: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for index, item in enumerate(raw_prompts):
        if not isinstance(item, dict):
            continue
        normalized = _normalized_prompt_preset(item)
        preset_id = normalized["id"] or f"{_prompt_preset_slug(normalized['name'])}-{index + 1}"
        while preset_id in seen_ids:
            preset_id = f"{preset_id}-{len(seen_ids) + 1}"
        normalized["id"] = preset_id
        seen_ids.add(preset_id)
        prompts.append(normalized)
    prompts.sort(key=lambda item: str(item.get("updatedAt") or ""), reverse=True)
    return {
        **deepcopy(DEFAULT_PROMPT_LIBRARY_SETTINGS),
        **payload,
        "updated": payload.get("updated") or current_run_date(),
        "prompts": prompts,
    }


def _write_prompt_library_payload(prompts: list[dict[str, Any]], instance_id: str | None = None) -> dict[str, Any]:
    payload = {
        "version": DEFAULT_PROMPT_LIBRARY_SETTINGS["version"],
        "updated": current_run_date(),
        "prompts": prompts,
    }
    write_json(PROMPT_LIBRARY_PATH, payload)
    return read_prompt_library()


def save_prompt_preset(payload: dict[str, Any], instance_id: str | None = None) -> dict[str, Any]:
    library = read_prompt_library()
    prompts = list(library["prompts"])
    requested_id = str(payload.get("id") or payload.get("presetId") or "").strip()
    requested_name = _prompt_preset_name(payload.get("name"), DEFAULT_PROMPT_SETTINGS["name"])
    incoming_signature = _prompt_preset_signature(payload)
    if requested_id:
        existing = next((item for item in prompts if item["id"] == requested_id), None)
        if existing:
            preset = _normalized_prompt_preset(payload, preset_id=requested_id)
            for index, item in enumerate(prompts):
                if item["id"] == requested_id:
                    prompts[index] = preset
                    break
        else:
            requested_id = ""
    if not requested_id:
        same_name_prompts = [item for item in prompts if _prompt_preset_name(item.get("name")) == requested_name]
        if same_name_prompts:
            # Same-name save should behave like updating the existing prompt instead of
            # creating another duplicate card every time the user clicks save.
            target = max(same_name_prompts, key=lambda item: str(item.get("updatedAt") or ""))
            requested_id = str(target.get("id") or "").strip()
            preset = _normalized_prompt_preset(payload, preset_id=requested_id)
            for index, item in enumerate(prompts):
                if item["id"] == requested_id:
                    prompts[index] = preset
                    break
        else:
            duplicate = next(
                (
                    item for item in prompts
                    if _prompt_preset_signature(item) == incoming_signature
                ),
                None,
            )
            if duplicate:
                requested_id = str(duplicate.get("id") or "").strip()
                preset = {
                    **duplicate,
                    "updatedAt": now_iso(),
                }
                for index, item in enumerate(prompts):
                    if item["id"] == requested_id:
                        prompts[index] = preset
                        break
    if not requested_id:
        base_slug = _prompt_preset_slug(_prompt_preset_name(payload.get("name"), DEFAULT_PROMPT_SETTINGS["name"]))
        candidate_id = base_slug
        suffix = 2
        existing_ids = {item["id"] for item in prompts}
        while candidate_id in existing_ids:
            candidate_id = f"{base_slug}-{suffix}"
            suffix += 1
        preset = _normalized_prompt_preset(payload, preset_id=candidate_id)
        prompts.append(preset)
    library = _write_prompt_library_payload(prompts)
    saved = next((item for item in library["prompts"] if item["id"] == preset["id"]), preset)
    return {
        "preset": saved,
        "prompts": library["prompts"],
    }


def read_prompt_preset(preset_id: str, instance_id: str | None = None) -> dict[str, Any]:
    target = str(preset_id or "").strip()
    if not target:
        raise ValueError("Prompt preset id is required.")
    library = read_prompt_library()
    preset = next((item for item in library["prompts"] if item["id"] == target), None)
    if not preset:
        raise ValueError(f"Prompt preset not found: {target}")
    return preset


def rename_prompt_preset(preset_id: str, name: str, instance_id: str | None = None) -> dict[str, Any]:
    library = read_prompt_library()
    prompts = list(library["prompts"])
    target = str(preset_id or "").strip()
    if not target:
        raise ValueError("Prompt preset id is required.")
    updated_name = _prompt_preset_name(name, DEFAULT_PROMPT_SETTINGS["name"])
    updated_preset = None
    for index, item in enumerate(prompts):
        if item["id"] != target:
            continue
        updated_preset = {
            **item,
            "name": updated_name,
            "updatedAt": now_iso(),
        }
        prompts[index] = updated_preset
        break
    if updated_preset is None:
        raise ValueError(f"Prompt preset not found: {target}")
    library = _write_prompt_library_payload(prompts)
    saved = next((item for item in library["prompts"] if item["id"] == target), updated_preset)
    return {
        "preset": saved,
        "prompts": library["prompts"],
    }


def delete_prompt_preset(preset_id: str, instance_id: str | None = None) -> dict[str, Any]:
    library = read_prompt_library()
    prompts = list(library["prompts"])
    target = str(preset_id or "").strip()
    if not target:
        raise ValueError("Prompt preset id is required.")
    remaining = [item for item in prompts if item["id"] != target]
    if len(remaining) == len(prompts):
        raise ValueError(f"Prompt preset not found: {target}")
    library = _write_prompt_library_payload(remaining)
    return {
        "deletedId": target,
        "prompts": library["prompts"],
    }
