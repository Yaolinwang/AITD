from __future__ import annotations

import ipaddress
import json
import socket
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .config import (
    DEFAULT_PROMPT_SETTINGS,
    preview_fixed_universe,
    read_candidate_source_code,
    read_dashboard_settings,
    read_fixed_universe,
    read_live_exchange_catalog,
    read_live_trading_config,
    read_llm_provider,
    read_network_settings,
    delete_prompt_preset,
    read_prompt_library,
    read_prompt_preset,
    read_prompt_settings,
    read_trading_settings,
    rename_prompt_preset,
    save_prompt_preset,
    write_dashboard_settings,
    write_fixed_universe,
    write_live_trading_config,
    write_llm_provider,
    write_network_settings,
    write_prompt_settings,
    write_trading_settings,
)
from .http_client import HttpRequestError, request_text
from .engine import (
    flatten_active_account,
    preview_trading_prompt_decision,
    read_trading_state,
    refresh_account_state_after_settings_save,
    reset_trading_account,
    run_trading_cycle_batch,
    summarize_trading_state,
)
from .market import read_latest_scan, refresh_candidate_pool
from .market import test_candidate_source
from .utils import DASHBOARD_DIR, now_iso


SCHEDULE_TRIGGER_WINDOW_SECONDS = 20

PUBLIC_IP_PROBES = (
    ("ipify", "https://api.ipify.org"),
    ("ifconfig.me", "https://ifconfig.me/ip"),
    ("icanhazip", "https://icanhazip.com"),
)

CLIENT_DISCONNECT_WINERRORS = {10053, 10054}
CLIENT_DISCONNECT_ERRNOS = {32, 54, 104}
CLIENT_DISCONNECT_EXCEPTIONS = (BrokenPipeError, ConnectionAbortedError, ConnectionResetError)


def _is_client_disconnect(error: BaseException) -> bool:
    if isinstance(error, CLIENT_DISCONNECT_EXCEPTIONS):
        return True
    if not isinstance(error, OSError):
        return False
    winerror = getattr(error, "winerror", None)
    if winerror in CLIENT_DISCONNECT_WINERRORS:
        return True
    return getattr(error, "errno", None) in CLIENT_DISCONNECT_ERRNOS


def _prompt_lines(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [item.strip() for item in value.splitlines() if item.strip()]
    return []


def _friendly_ip_error(error: Any) -> str:
    text = str(error or "").strip()
    lowered = text.lower()
    if not text or text == "None":
        return "无法连接公网 IP 查询服务。"
    if "expected pattern" in lowered:
        return "代理地址格式可能不正确，或当前网络环境无法完成公网 IP 查询。"
    if "nodename nor servname provided" in lowered or "name or service not known" in lowered:
        return "无法解析公网 IP 查询服务域名。"
    return text


def _detect_local_ip() -> str | None:
    for target in ("1.1.1.1", "8.8.8.8"):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.connect((target, 80))
                candidate = str(sock.getsockname()[0] or "").strip()
                if candidate and not candidate.startswith("127."):
                    ipaddress.ip_address(candidate)
                    return candidate
        except OSError:
            continue
    try:
        hostname = socket.gethostname()
        for family, _, _, _, sockaddr in socket.getaddrinfo(hostname, None):
            if family not in {socket.AF_INET, socket.AF_INET6}:
                continue
            candidate = str(sockaddr[0]).split("%", 1)[0].strip()
            if not candidate:
                continue
            parsed = ipaddress.ip_address(candidate)
            if parsed.is_loopback or parsed.is_unspecified:
                continue
            return candidate
    except OSError:
        return None
    return None


def _network_ip_payload() -> dict[str, Any]:
    network_settings = read_network_settings()
    last_error = None
    for source, url in PUBLIC_IP_PROBES:
        try:
            raw = request_text(
                "GET",
                url,
                timeout_seconds=5,
                network_settings=network_settings,
            ).strip()
            ip_text = raw.splitlines()[0].strip()
            ipaddress.ip_address(ip_text)
            return {
                "ip": ip_text,
                "source": source,
                "scope": "public",
                "proxyEnabled": network_settings.get("proxyEnabled") is True,
                "error": None,
            }
        except (HttpRequestError, OSError, ValueError) as error:
            last_error = _friendly_ip_error(error)
    local_ip = _detect_local_ip()
    if local_ip:
        return {
            "ip": local_ip,
            "source": "local",
            "scope": "local",
            "proxyEnabled": network_settings.get("proxyEnabled") is True,
            "error": last_error,
        }
    return {
        "ip": None,
        "source": None,
        "scope": "unknown",
        "proxyEnabled": network_settings.get("proxyEnabled") is True,
        "error": last_error or "无法获取本机 IP 地址。",
    }


def _prompt_form_payload(prompt: dict[str, Any]) -> dict[str, Any]:
    logic = prompt.get("decision_logic") if isinstance(prompt.get("decision_logic"), dict) else {}
    core_principles = _prompt_lines(logic.get("core_principles"))
    entry_preferences = _prompt_lines(logic.get("entry_preferences"))
    position_management = _prompt_lines(logic.get("position_management"))
    return {
        **prompt,
        "role": str(logic.get("role") or ""),
        "corePrinciplesText": "\n".join(core_principles),
        "entryPreferencesText": "\n".join(entry_preferences),
        "positionManagementText": "\n".join(position_management),
        "klineFeeds": prompt.get("klineFeeds") if isinstance(prompt.get("klineFeeds"), dict) else dict(DEFAULT_PROMPT_SETTINGS.get("klineFeeds", {})),
    }


def _prompt_logic_from_payload(payload: dict[str, Any], fallback_prompt: dict[str, Any] | None = None) -> dict[str, Any]:
    fallback_prompt = fallback_prompt or read_prompt_settings()
    fallback_logic = fallback_prompt.get("decision_logic") if isinstance(fallback_prompt.get("decision_logic"), dict) else {}
    if "rawJson" in payload:
        parsed = json.loads(payload["rawJson"])
        if not isinstance(parsed, dict):
            raise ValueError("decision_logic payload must be an object.")
        return parsed
    response_style = DEFAULT_PROMPT_SETTINGS["decision_logic"]["response_style"]
    return {
        "role": str(payload.get("role") or fallback_logic.get("role") or "").strip(),
        "core_principles": _prompt_lines(payload.get("corePrinciplesText", fallback_logic.get("core_principles", []))),
        "entry_preferences": _prompt_lines(payload.get("entryPreferencesText", fallback_logic.get("entry_preferences", []))),
        "position_management": _prompt_lines(payload.get("positionManagementText", fallback_logic.get("position_management", []))),
        "response_style": list(response_style),
    }


class AppRuntime:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.trade_lock = threading.Lock()
        self.session_started_at = now_iso()
        self.log_entries: deque[dict[str, Any]] = deque(maxlen=400)
        self.scan_runner = {
            "running": False,
            "lastStartedAt": None,
            "lastFinishedAt": None,
            "lastError": None,
            "lastReason": None,
        }
        self.trade_runners = {
            "paper": {
                "running": False,
                "lastStartedAt": None,
                "lastFinishedAt": None,
                "lastError": None,
                "lastReason": None,
            },
            "live": {
                "running": False,
                "lastStartedAt": None,
                "lastFinishedAt": None,
                "lastError": None,
                "lastReason": None,
            },
        }
        self._scheduler_started = False

    def record_log(self, level: str, message: str) -> None:
        level_text = (level or "INFO").upper()
        line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] [{level_text}] {message}"
        with self.lock:
            self.log_entries.append(
                {
                    "at": now_iso(),
                    "level": level_text,
                    "message": message,
                    "line": line,
                }
            )
        print(line, flush=True)

    def api_logs(self) -> dict[str, Any]:
        with self.lock:
            return {
                "sessionStartedAt": self.session_started_at,
                "entries": list(self.log_entries),
                "paperRunner": dict(self.trade_runners["paper"]),
                "liveRunner": dict(self.trade_runners["live"]),
                "scanRunner": dict(self.scan_runner),
            }

    def api_latest(self) -> dict[str, Any]:
        scan = read_latest_scan()
        opportunities = scan.get("opportunities", [])
        return {
            "updatedAt": now_iso(),
            "scan": {
                "runDate": scan.get("runDate"),
                "fetchedAt": scan.get("fetchedAt"),
                "opportunities": len(opportunities),
                "research": len([item for item in opportunities if item.get("actionBucket") == "Research"]),
                "watch": len([item for item in opportunities if item.get("actionBucket") == "Watch"]),
                "noTrade": len([item for item in opportunities if item.get("actionBucket") == "No trade"]),
                "scanRunner": self.scan_runner,
            },
        }

    def _run_scan_job(self, reason: str) -> None:
        with self.lock:
            self.scan_runner["running"] = True
            self.scan_runner["lastStartedAt"] = now_iso()
            self.scan_runner["lastFinishedAt"] = None
            self.scan_runner["lastError"] = None
            self.scan_runner["lastReason"] = reason
        self.record_log("INFO", f"开始刷新候选池，触发原因：{reason}")
        try:
            refresh_candidate_pool()
            latest = read_latest_scan()
            self.record_log("INFO", f"候选池刷新完成，当前候选数：{len(latest.get('opportunities', []))}")
        except Exception as error:
            with self.lock:
                self.scan_runner["lastError"] = str(error)
            self.record_log("ERROR", f"候选池刷新失败：{error}")
        finally:
            with self.lock:
                self.scan_runner["running"] = False
                self.scan_runner["lastFinishedAt"] = now_iso()

    def _run_trade_job(self, mode: str, reason: str) -> None:
        with self.lock:
            runner = self.trade_runners[mode]
            runner["running"] = True
            runner["lastStartedAt"] = now_iso()
            runner["lastFinishedAt"] = None
            runner["lastError"] = None
            runner["lastReason"] = reason
        self.record_log("INFO", f"开始执行{mode.upper()}交易决策循环，触发原因：{reason}")
        try:
            with self.trade_lock:
                run_trading_cycle_batch(reason=reason, modes=[mode])
            summary = summarize_trading_state()
            latest_decision = summary.get("latestLiveDecision" if mode == "live" else "latestPaperDecision") or {}
            self.record_log(
                "INFO",
                f"{mode.upper()}交易决策循环完成，latestDecision={latest_decision.get('id', 'n/a')}",
            )
        except Exception as error:
            with self.lock:
                self.trade_runners[mode]["lastError"] = str(error)
            self.record_log("ERROR", f"{mode.upper()}交易决策循环失败：{error}")
        finally:
            with self.lock:
                self.trade_runners[mode]["running"] = False
                self.trade_runners[mode]["lastFinishedAt"] = now_iso()

    def start_scan(self, reason: str = "manual") -> bool:
        with self.lock:
            if self.scan_runner["running"]:
                return False
        thread = threading.Thread(target=self._run_scan_job, args=(reason,), daemon=True)
        thread.start()
        return True

    def start_trade(self, mode: str, reason: str = "manual") -> bool:
        mode = "live" if str(mode).strip().lower() == "live" else "paper"
        with self.lock:
            if self.trade_runners[mode]["running"]:
                return False
        thread = threading.Thread(target=self._run_trade_job, args=(mode, reason), daemon=True)
        thread.start()
        return True

    @staticmethod
    def _aligned_slot(reference_ts: float, interval_minutes: int, offset_minutes: int = 0) -> tuple[float, float]:
        timezone_offset = 8 * 60 * 60
        interval_seconds = max(5, interval_minutes) * 60
        adjusted = reference_ts + timezone_offset + (offset_minutes * 60)
        slot_start_local = int(adjusted // interval_seconds) * interval_seconds
        start_ts = slot_start_local - timezone_offset - (offset_minutes * 60)
        end_ts = start_ts + interval_seconds
        return start_ts, end_ts

    @staticmethod
    def _parse_timestamp(value: str | None) -> float | None:
        if not value:
            return None
        try:
            return __import__("datetime").datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except Exception:
            return None

    def _latest_scheduled_trade_ts(self, mode: str) -> float | None:
        settings = read_trading_settings()
        state = read_trading_state(settings)
        for decision in reversed(state.get(mode, {}).get("decisions", [])):
            if decision.get("runnerReason") != "scheduled":
                continue
            return self._parse_timestamp(decision.get("startedAt"))
        return None

    def next_trade_due_at(self, mode: str) -> str:
        mode = "live" if str(mode).strip().lower() == "live" else "paper"
        settings = read_trading_settings()
        now_ts = time.time()
        start_ts, end_ts = self._aligned_slot(now_ts, settings["decisionIntervalMinutes"])
        latest_ts = self._latest_scheduled_trade_ts(mode)
        if latest_ts is not None and start_ts <= latest_ts < end_ts:
            due_ts = end_ts
        elif now_ts < start_ts + SCHEDULE_TRIGGER_WINDOW_SECONDS:
            due_ts = start_ts
        else:
            due_ts = end_ts
        return __import__("datetime").datetime.utcfromtimestamp(due_ts).replace(microsecond=0).isoformat() + "Z"

    def _maybe_start_scheduled_scan(self) -> None:
        settings = read_dashboard_settings()
        if not settings["marketAutoScanEnabled"]:
            return
        if self.scan_runner["running"]:
            return
        now_ts = time.time()
        scan = read_latest_scan()
        fetched_at = scan.get("fetchedAt")
        fetched_ts = None
        if fetched_at:
            try:
                fetched_ts = __import__("datetime").datetime.fromisoformat(fetched_at.replace("Z", "+00:00")).timestamp()
            except Exception:
                fetched_ts = None
        slot_start, slot_end = self._aligned_slot(now_ts, settings["marketScanIntervalMinutes"], settings["marketScanOffsetMinute"])
        if fetched_ts is not None and slot_start <= fetched_ts < slot_end:
            return
        if slot_start <= now_ts < slot_start + SCHEDULE_TRIGGER_WINDOW_SECONDS:
            self.start_scan("scheduled")

    def _maybe_start_scheduled_trade(self) -> None:
        settings = read_trading_settings()
        now_ts = time.time()
        start_ts, end_ts = self._aligned_slot(now_ts, settings["decisionIntervalMinutes"])
        if not (start_ts <= now_ts < start_ts + SCHEDULE_TRIGGER_WINDOW_SECONDS):
            return
        for mode in ("paper", "live"):
            if not settings.get(f"{mode}Trading", {}).get("enabled"):
                continue
            if self.trade_runners[mode]["running"]:
                continue
            latest_ts = self._latest_scheduled_trade_ts(mode)
            if latest_ts is not None and start_ts <= latest_ts < end_ts:
                continue
            self.start_trade(mode, "scheduled")

    def start_scheduler(self) -> None:
        if self._scheduler_started:
            return
        self._scheduler_started = True

        def loop() -> None:
            while True:
                try:
                    self._maybe_start_scheduled_scan()
                    self._maybe_start_scheduled_trade()
                except Exception as error:
                    self.record_log("ERROR", f"调度器异常：{error}")
                time.sleep(10)

        thread = threading.Thread(target=loop, daemon=True)
        thread.start()
        self.record_log("INFO", "自动调度器已启动。")


def _read_json_body(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    length = int(handler.headers.get("content-length") or 0)
    if length <= 0:
        return {}
    raw = handler.rfile.read(length).decode("utf-8")
    if not raw.strip():
        return {}
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("JSON body must be an object.")
    return payload


def _json_response(handler: BaseHTTPRequestHandler, payload: Any, status: int = 200) -> None:
    data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def _text_response(handler: BaseHTTPRequestHandler, payload: str, status: int = 200, content_type: str = "text/plain; charset=utf-8") -> None:
    data = payload.encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def _static_content_type(path: Path) -> str:
    if path.suffix == ".html":
        return "text/html; charset=utf-8"
    if path.suffix == ".js":
        return "text/javascript; charset=utf-8"
    if path.suffix == ".css":
        return "text/css; charset=utf-8"
    if path.suffix == ".json":
        return "application/json; charset=utf-8"
    return "application/octet-stream"


class TradingAgentHandler(BaseHTTPRequestHandler):
    runtime: AppRuntime

    def do_GET(self) -> None:  # noqa: N802
        self._handle("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._handle("POST")

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return

    def _handle(self, method: str) -> None:
        parsed = urlparse(self.path)
        try:
            if method == "GET" and parsed.path == "/api/latest":
                return _json_response(self, self.runtime.api_latest())
            if method == "GET" and parsed.path == "/api/opportunities":
                return _json_response(self, read_latest_scan())
            if method == "GET" and parsed.path == "/api/logs":
                return _json_response(self, self.runtime.api_logs())
            if method == "GET" and parsed.path == "/api/settings":
                return _json_response(self, read_dashboard_settings())
            if method == "POST" and parsed.path == "/api/settings":
                result = write_dashboard_settings(_read_json_body(self))
                self.runtime.record_log(
                    "INFO",
                    f"Dashboard 设置已保存，pageAutoRefreshSeconds={result.get('pageAutoRefreshSeconds')}",
                )
                return _json_response(self, result)
            if method == "GET" and parsed.path == "/api/trading/settings":
                return _json_response(self, {**read_trading_settings(), "exchangeCatalog": read_live_exchange_catalog()})
            if method == "POST" and parsed.path == "/api/trading/settings":
                result = write_trading_settings(_read_json_body(self))
                refresh_result = refresh_account_state_after_settings_save()
                live_sync_warnings = refresh_result.get("liveSyncWarnings") if isinstance(refresh_result, dict) else []
                self.runtime.record_log(
                    "INFO",
                    "运行设置已保存，"
                    f"decisionIntervalMinutes={result.get('decisionIntervalMinutes')}，"
                    f"activeExchange={result.get('activeExchange', 'binance')}，"
                    f"paper={result.get('paperTrading', {}).get('enabled', False)}，"
                    f"live={result.get('liveTrading', {}).get('enabled', False)}",
                )
                if live_sync_warnings:
                    self.runtime.record_log("INFO", f"保存运行设置后已刷新实盘账户：{'; '.join(str(item) for item in live_sync_warnings[:2])}")
                else:
                    self.runtime.record_log("INFO", "保存运行设置后已刷新账户状态。")
                return _json_response(self, {**result, "exchangeCatalog": read_live_exchange_catalog()})
            if method == "GET" and parsed.path == "/api/trading/provider":
                return _json_response(self, read_llm_provider())
            if method == "POST" and parsed.path == "/api/trading/provider":
                result = write_llm_provider(_read_json_body(self))
                self.runtime.record_log(
                    "INFO",
                    f"模型配置已保存，provider={result.get('preset', 'custom')}，model={result.get('model', 'n/a')}",
                )
                return _json_response(self, result)
            if method == "GET" and parsed.path == "/api/trading/universe":
                universe = read_fixed_universe()
                universe["rawSymbols"] = "\n".join(universe.get("symbols", []))
                universe["candidateSourceCode"] = read_candidate_source_code()
                return _json_response(self, universe)
            if method == "POST" and parsed.path == "/api/trading/universe":
                result = write_fixed_universe(_read_json_body(self))
                result["rawSymbols"] = "\n".join(result.get("symbols", []))
                result["candidateSourceCode"] = read_candidate_source_code()
                self.runtime.record_log(
                    "INFO",
                    f"候选池配置已保存，symbols={len(result.get('symbols', []))}，dynamic={result.get('dynamicSource', {}).get('enabled', False)}",
                )
                return _json_response(self, result)
            if method == "POST" and parsed.path == "/api/trading/universe/test":
                payload = _read_json_body(self)
                universe = preview_fixed_universe(payload)
                code_override = payload.get("candidateSourceCode") if "candidateSourceCode" in payload else None
                result = test_candidate_source(universe=universe, code_override=code_override)
                self.runtime.record_log(
                    "INFO",
                    f"候选池测试已完成，mode={result.get('mode')}，symbols={result.get('count', 0)}",
                )
                return _json_response(self, result)
            if method == "GET" and parsed.path == "/api/trading/prompt":
                prompt = read_prompt_settings()
                return _json_response(self, _prompt_form_payload(prompt))
            if method == "GET" and parsed.path == "/api/trading/prompt-library":
                library = read_prompt_library()
                return _json_response(
                    self,
                    {
                        "updated": library.get("updated"),
                        "prompts": [_prompt_form_payload(item) for item in library.get("prompts", [])],
                    },
                )
            if method == "POST" and parsed.path == "/api/trading/prompt":
                payload = _read_json_body(self)
                payload["decision_logic"] = _prompt_logic_from_payload(payload)
                result = write_prompt_settings(payload)
                self.runtime.record_log("INFO", f"交易逻辑已保存，name={result.get('name', 'default_trading_logic')}")
                return _json_response(self, _prompt_form_payload(result))
            if method == "POST" and parsed.path == "/api/trading/prompt-library/save":
                payload = _read_json_body(self)
                payload["decision_logic"] = _prompt_logic_from_payload(payload)
                result = save_prompt_preset(payload)
                preset = result.get("preset") if isinstance(result.get("preset"), dict) else {}
                self.runtime.record_log("INFO", f"Prompt 模板已保存，name={preset.get('name', 'untitled')}，id={preset.get('id', 'n/a')}")
                return _json_response(
                    self,
                    {
                        "preset": _prompt_form_payload(preset),
                        "prompts": [_prompt_form_payload(item) for item in result.get("prompts", [])],
                    },
                )
            if method == "POST" and parsed.path == "/api/trading/prompt-library/use":
                payload = _read_json_body(self)
                preset = read_prompt_preset(str(payload.get("id") or ""))
                result = write_prompt_settings(
                    {
                        "name": preset.get("name"),
                        "presetId": preset.get("id"),
                        "klineFeeds": preset.get("klineFeeds"),
                        "decision_logic": preset.get("decision_logic"),
                    }
                )
                self.runtime.record_log("INFO", f"Prompt 模板已启用，name={preset.get('name', 'untitled')}，id={preset.get('id', 'n/a')}")
                return _json_response(self, _prompt_form_payload(result))
            if method == "POST" and parsed.path == "/api/trading/prompt-library/rename":
                payload = _read_json_body(self)
                result = rename_prompt_preset(str(payload.get("id") or ""), str(payload.get("name") or ""))
                preset = result.get("preset") if isinstance(result.get("preset"), dict) else {}
                self.runtime.record_log("INFO", f"Prompt 模板已重命名，name={preset.get('name', 'untitled')}，id={preset.get('id', 'n/a')}")
                return _json_response(
                    self,
                    {
                        "preset": _prompt_form_payload(preset),
                        "prompts": [_prompt_form_payload(item) for item in result.get("prompts", [])],
                    },
                )
            if method == "POST" and parsed.path == "/api/trading/prompt-library/delete":
                payload = _read_json_body(self)
                result = delete_prompt_preset(str(payload.get("id") or ""))
                self.runtime.record_log("WARN", f"Prompt 模板已删除，id={result.get('deletedId', 'n/a')}")
                return _json_response(
                    self,
                    {
                        "deletedId": result.get("deletedId"),
                        "prompts": [_prompt_form_payload(item) for item in result.get("prompts", [])],
                    },
                )
            if method == "POST" and parsed.path == "/api/trading/prompt/test":
                payload = _read_json_body(self)
                prompt_override = None
                if {"role", "corePrinciplesText", "entryPreferencesText", "positionManagementText"} & set(payload.keys()) or "rawJson" in payload:
                    prompt_override = {
                        "name": payload.get("name") or "default_trading_logic",
                        "klineFeeds": payload.get("klineFeeds"),
                        "decision_logic": _prompt_logic_from_payload(payload),
                    }
                mode = "live" if str(payload.get("mode") or "paper").strip().lower() == "live" else "paper"
                result = preview_trading_prompt_decision(mode_override=mode, prompt_override=prompt_override)
                provider_info = result.get("provider") if isinstance(result.get("provider"), dict) else {}
                if provider_info.get("autoConfiguredSaved"):
                    self.runtime.record_log(
                        "INFO",
                        f"模型网关已自动识别并保存，preset={provider_info.get('preset', 'n/a')}，apiStyle={provider_info.get('resolvedApiStyle', 'n/a')}",
                    )
                self.runtime.record_log("INFO", f"Prompt 测试已完成，mode={mode}，candidates={result.get('candidateCount', 0)}")
                return _json_response(self, result)
            if method == "GET" and parsed.path == "/api/trading/live-config":
                return _json_response(self, {**read_live_trading_config(), "exchangeCatalog": read_live_exchange_catalog()})
            if method == "POST" and parsed.path == "/api/trading/live-config":
                result = write_live_trading_config(_read_json_body(self))
                self.runtime.record_log("INFO", f"实盘账号配置已保存，exchange={result.get('exchange', 'binance')}")
                return _json_response(self, {**result, "exchangeCatalog": read_live_exchange_catalog()})
            if method == "GET" and parsed.path == "/api/network":
                return _json_response(self, read_network_settings())
            if method == "GET" and parsed.path == "/api/network/ip":
                return _json_response(self, _network_ip_payload())
            if method == "POST" and parsed.path == "/api/network":
                result = write_network_settings(_read_json_body(self))
                self.runtime.record_log("INFO", f"代理配置已保存，enabled={result.get('proxyEnabled', False)}")
                return _json_response(self, result)
            if method == "GET" and parsed.path == "/api/trading/state":
                payload = summarize_trading_state()
                payload["paperRunner"] = self.runtime.trade_runners["paper"]
                payload["liveRunner"] = self.runtime.trade_runners["live"]
                payload["scanRunner"] = self.runtime.scan_runner
                payload["paperNextDecisionDueAt"] = self.runtime.next_trade_due_at("paper")
                payload["liveNextDecisionDueAt"] = self.runtime.next_trade_due_at("live")
                return _json_response(self, payload)
            if method == "POST" and parsed.path == "/api/trading/run":
                payload = _read_json_body(self)
                mode = "live" if str(payload.get("mode") or "paper").strip().lower() == "live" else "paper"
                started = self.runtime.start_trade(mode, "manual")
                if not started:
                    self.runtime.record_log("WARN", f"收到手动{mode.upper()}交易请求，但上一轮仍在执行。")
                return _json_response(self, {"started": started, "mode": mode, "runner": self.runtime.trade_runners[mode], "nextDecisionDueAt": self.runtime.next_trade_due_at(mode)})
            if method == "POST" and parsed.path == "/api/trading/reset":
                payload = _read_json_body(self)
                reset_mode = payload.get("mode") or "paper"
                result = reset_trading_account(str(reset_mode))
                target_label = "实盘" if str(reset_mode).strip().lower() == "live" else "模拟盘"
                self.runtime.record_log("WARN", f"{target_label}账户已重置，mode={reset_mode}")
                return _json_response(self, result)
            if method == "POST" and parsed.path == "/api/trading/flatten":
                payload = _read_json_body(self)
                mode = "live" if str(payload.get("mode") or "paper").strip().lower() == "live" else "paper"
                result = flatten_active_account("manual_flatten", mode_override=mode)
                self.runtime.record_log("WARN", f"已对{mode.upper()}执行全部平仓。")
                return _json_response(self, result)
            if method == "POST" and parsed.path == "/api/scan/run":
                started = self.runtime.start_scan("manual")
                if not started:
                    self.runtime.record_log("WARN", "收到手动候选池刷新请求，但上一轮刷新仍在执行。")
                return _json_response(self, {"started": started, "scanRunner": self.runtime.scan_runner})
            if method not in {"GET", "HEAD"}:
                return _text_response(self, "Method not allowed", status=405)
            return self._serve_static(parsed.path)
        except Exception as error:
            if _is_client_disconnect(error):
                return
            self.runtime.record_log("ERROR", f"{method} {parsed.path} 失败：{error}")
            try:
                return _json_response(self, {"error": str(error)}, status=500)
            except Exception as response_error:
                if _is_client_disconnect(response_error):
                    return
                raise

    def _serve_static(self, request_path: str) -> None:
        relative = "index.html" if request_path == "/" else request_path.lstrip("/")
        file_path = (DASHBOARD_DIR / relative).resolve()
        dashboard_root = DASHBOARD_DIR.resolve()
        if dashboard_root not in file_path.parents and file_path != dashboard_root:
            return _text_response(self, "Forbidden", status=403)
        if not file_path.exists() or not file_path.is_file():
            return _text_response(self, "Not found", status=404)
        payload = file_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", _static_content_type(file_path))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def _next_available_port(host: str, preferred_port: int, max_checks: int = 20) -> int:
    for port in range(preferred_port, preferred_port + max_checks + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if sock.connect_ex((host, port)) != 0:
                return port
    raise RuntimeError(f"Could not find a free port near {preferred_port}.")


def start_server(port_override: int | None = None) -> None:
    settings = read_trading_settings()
    host = settings["server"]["host"]
    preferred_port = int(port_override if port_override is not None else settings["server"]["port"])
    if preferred_port < 1024 or preferred_port > 65535:
        raise ValueError("Port must be between 1024 and 65535.")
    port = _next_available_port(host, preferred_port)
    runtime = AppRuntime()
    TradingAgentHandler.runtime = runtime
    server = ThreadingHTTPServer((host, port), TradingAgentHandler)
    runtime.start_scheduler()
    runtime.record_log("INFO", f"Trading Agent dashboard running at http://{host}:{port}/trader.html")
    server.serve_forever()
