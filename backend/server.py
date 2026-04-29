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
from .exchange_cooldown import cooldown_status
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
from .instances import clone_instance, create_instance, delete_instance, list_instances, read_instance, rename_instance
from .utils import DASHBOARD_DIR, now_iso


SCHEDULE_TRIGGER_WINDOW_SECONDS = 20

PUBLIC_IP_PROBES = (
    ("ipify", "https://api.ipify.org"),
    ("ifconfig.me", "https://ifconfig.me/ip"),
    ("icanhazip", "https://icanhazip.com"),
)


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


def _network_ip_payload(instance_id: str | None = None) -> dict[str, Any]:
    network_settings = read_network_settings(instance_id)
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


def _sample_equity_curve(points: list[dict[str, Any]], max_points: int = 120) -> list[dict[str, Any]]:
    if len(points) <= max_points:
        return points
    if max_points <= 1:
        return [points[-1]]
    indexes = sorted({
        round(index * (len(points) - 1) / (max_points - 1))
        for index in range(max_points)
    })
    return [points[index] for index in indexes]


class AppRuntime:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.session_started_at = now_iso()
        self.log_entries: deque[dict[str, Any]] = deque(maxlen=800)
        self.scan_runners: dict[str, dict[str, Any]] = {}
        self.trade_runners: dict[str, dict[str, Any]] = {}
        self.trade_locks: dict[str, threading.Lock] = {}
        self._scheduler_started = False

    @staticmethod
    def _runner_template() -> dict[str, Any]:
        return {
            "running": False,
            "lastStartedAt": None,
            "lastFinishedAt": None,
            "lastError": None,
            "lastReason": None,
        }

    def _scan_runner(self, instance_id: str) -> dict[str, Any]:
        return self.scan_runners.setdefault(instance_id, self._runner_template())

    def _trade_runner(self, instance_id: str) -> dict[str, Any]:
        return self.trade_runners.setdefault(instance_id, self._runner_template())

    def _trade_lock(self, instance_id: str) -> threading.Lock:
        lock = self.trade_locks.get(instance_id)
        if lock is None:
            lock = threading.Lock()
            self.trade_locks[instance_id] = lock
        return lock

    def record_log(self, level: str, message: str, instance_id: str | None = None) -> None:
        level_text = (level or "INFO").upper()
        instance_name = None
        if instance_id:
            try:
                instance_name = read_instance(instance_id)["name"]
            except Exception:
                instance_name = instance_id
        line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] [{level_text}] {message}"
        with self.lock:
            self.log_entries.append(
                {
                    "at": now_iso(),
                    "level": level_text,
                    "message": message,
                    "line": line,
                    "instanceId": instance_id,
                    "instanceName": instance_name,
                }
            )
        print(line, flush=True)

    def api_logs(self, instance_id: str | None = None) -> dict[str, Any]:
        with self.lock:
            if instance_id:
                entries = [item for item in self.log_entries if item.get("instanceId") in {None, instance_id}]
            else:
                entries = list(self.log_entries)
            return {
                "sessionStartedAt": self.session_started_at,
                "entries": entries,
                "tradeRunner": dict(self._trade_runner(instance_id)) if instance_id else None,
                "scanRunner": dict(self._scan_runner(instance_id)) if instance_id else None,
            }

    def instance_card_payload(self, instance_id: str) -> dict[str, Any]:
        instance = read_instance(instance_id)
        state = summarize_trading_state(instance_id, include_live_status=False)
        active_mode = instance["type"]
        account = state["liveAccount"] if active_mode == "live" else state["paperAccount"]
        history = state.get("liveHistory") if active_mode == "live" else state.get("paperHistory")
        scan = state.get("scan", {})
        settings = state.get("settings", {})
        live_config = read_live_trading_config(instance_id)
        exchange_id = str(settings.get("activeExchange") or live_config.get("exchange") or "binance").strip().lower()
        exchange_cooldown = cooldown_status(exchange_id)
        warnings: list[str] = []
        if instance["type"] == "live":
            live_instances = [item for item in list_instances() if item["type"] == "live" and item["id"] != instance_id]
            key_marker = str(live_config.get("apiKey") or "").strip()
            base_marker = str(live_config.get("baseUrl") or "").strip()
            exchange_marker = str(live_config.get("exchange") or "").strip()
            if key_marker:
                for peer in live_instances:
                    peer_live = read_live_trading_config(peer["id"])
                    if (
                        str(peer_live.get("apiKey") or "").strip() == key_marker
                        and str(peer_live.get("baseUrl") or "").strip() == base_marker
                        and str(peer_live.get("exchange") or "").strip() == exchange_marker
                    ):
                        warnings.append("检测到多个实盘实例正在使用同一套交易所账户配置。")
                        break
        equity_curve = []
        for item in (history or {}).get("decisionTimeline", []):
            if not isinstance(item, dict):
                continue
            equity_value = item.get("equityUsd")
            if equity_value in (None, ""):
                continue
            equity_curve.append(
                {
                    "at": item.get("finishedAt") or item.get("startedAt"),
                    "equityUsd": equity_value,
                }
            )
        equity_curve = _sample_equity_curve(equity_curve)
        if not equity_curve and account.get("equityUsd") is not None:
            equity_curve = [{"at": history.get("sessionStartedAt") or now_iso(), "equityUsd": account.get("equityUsd")}]
        return {
            "id": instance["id"],
            "name": instance["name"],
            "type": instance["type"],
            "createdAt": instance["createdAt"],
            "updatedAt": instance["updatedAt"],
            "exchange": settings.get("activeExchange"),
            "exchangeCooldown": exchange_cooldown,
            "running": bool(settings.get(f"{active_mode}Trading", {}).get("enabled")),
            "equityUsd": account.get("equityUsd"),
            "openPositions": len(account.get("openPositions", [])),
            "lastDecisionAt": (state.get("liveHistory") if active_mode == "live" else state.get("paperHistory")).get("lastDecisionAt"),
            "nextDecisionDueAt": self.next_trade_due_at(instance_id),
            "candidateUniverseSize": scan.get("candidateUniverseSize", 0),
            "equityCurve": equity_curve,
            "tradeRunner": dict(self._trade_runner(instance_id)),
            "scanRunner": dict(self._scan_runner(instance_id)),
            "warnings": warnings,
        }

    def instances_payload(self) -> dict[str, Any]:
        cards = [self.instance_card_payload(item["id"]) for item in list_instances()]
        exchange_ids = sorted({str(item.get("exchange") or "binance").strip().lower() for item in cards} | {"binance"})
        return {
            "sessionStartedAt": self.session_started_at,
            "instances": cards,
            "exchangeCooldowns": {exchange_id: cooldown_status(exchange_id) for exchange_id in exchange_ids},
            "dashboardSettings": read_dashboard_settings(),
        }

    def _run_scan_job(self, instance_id: str, reason: str) -> None:
        runner = self._scan_runner(instance_id)
        with self.lock:
            runner["running"] = True
            runner["lastStartedAt"] = now_iso()
            runner["lastFinishedAt"] = None
            runner["lastError"] = None
            runner["lastReason"] = reason
        instance = read_instance(instance_id)
        self.record_log("INFO", f"开始刷新候选池，触发原因：{reason}", instance_id)
        try:
            refresh_candidate_pool(instance_id=instance_id)
            latest = read_latest_scan(instance_id=instance_id)
            self.record_log("INFO", f"候选池刷新完成，当前候选数：{len(latest.get('opportunities', []))}", instance_id)
        except Exception as error:
            with self.lock:
                runner["lastError"] = str(error)
            self.record_log("ERROR", f"{instance['name']} 候选池刷新失败：{error}", instance_id)
        finally:
            with self.lock:
                runner["running"] = False
                runner["lastFinishedAt"] = now_iso()

    def _run_trade_job(self, instance_id: str, reason: str) -> None:
        instance = read_instance(instance_id)
        mode = instance["type"]
        runner = self._trade_runner(instance_id)
        with self.lock:
            runner["running"] = True
            runner["lastStartedAt"] = now_iso()
            runner["lastFinishedAt"] = None
            runner["lastError"] = None
            runner["lastReason"] = reason
        self.record_log("INFO", f"开始执行{instance['name']} {mode.upper()}交易决策循环，触发原因：{reason}", instance_id)
        try:
            with self._trade_lock(instance_id):
                run_trading_cycle_batch(reason=reason, modes=[mode], instance_id=instance_id)
            summary = summarize_trading_state(instance_id)
            latest_decision = summary.get("latestLiveDecision" if mode == "live" else "latestPaperDecision") or {}
            self.record_log("INFO", f"{instance['name']} 交易决策循环完成，latestDecision={latest_decision.get('id', 'n/a')}", instance_id)
        except Exception as error:
            with self.lock:
                runner["lastError"] = str(error)
            self.record_log("ERROR", f"{instance['name']} 交易决策循环失败：{error}", instance_id)
        finally:
            with self.lock:
                runner["running"] = False
                runner["lastFinishedAt"] = now_iso()

    def start_scan(self, instance_id: str, reason: str = "manual") -> bool:
        runner = self._scan_runner(instance_id)
        with self.lock:
            if runner["running"]:
                return False
        thread = threading.Thread(target=self._run_scan_job, args=(instance_id, reason), daemon=True)
        thread.start()
        return True

    def start_trade(self, instance_id: str, reason: str = "manual") -> bool:
        runner = self._trade_runner(instance_id)
        with self.lock:
            if runner["running"]:
                return False
        thread = threading.Thread(target=self._run_trade_job, args=(instance_id, reason), daemon=True)
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

    def _latest_scheduled_trade_ts(self, instance_id: str) -> float | None:
        instance = read_instance(instance_id)
        settings = read_trading_settings(instance_id)
        state = read_trading_state(settings, instance_id)
        for decision in reversed(state.get(instance["type"], {}).get("decisions", [])):
            if decision.get("runnerReason") != "scheduled":
                continue
            return self._parse_timestamp(decision.get("startedAt"))
        return None

    def next_trade_due_at(self, instance_id: str) -> str | None:
        try:
            settings = read_trading_settings(instance_id)
        except Exception:
            return None
        now_ts = time.time()
        start_ts, end_ts = self._aligned_slot(now_ts, settings["decisionIntervalMinutes"])
        latest_ts = self._latest_scheduled_trade_ts(instance_id)
        if latest_ts is not None and start_ts <= latest_ts < end_ts:
            due_ts = end_ts
        elif now_ts < start_ts + SCHEDULE_TRIGGER_WINDOW_SECONDS:
            due_ts = start_ts
        else:
            due_ts = end_ts
        return __import__("datetime").datetime.utcfromtimestamp(due_ts).replace(microsecond=0).isoformat() + "Z"

    def _maybe_start_scheduled_scan(self) -> None:
        dashboard_settings = read_dashboard_settings()
        if not dashboard_settings["marketAutoScanEnabled"]:
            return
        now_ts = time.time()
        slot_start, slot_end = self._aligned_slot(now_ts, dashboard_settings["marketScanIntervalMinutes"], dashboard_settings["marketScanOffsetMinute"])
        if not (slot_start <= now_ts < slot_start + SCHEDULE_TRIGGER_WINDOW_SECONDS):
            return
        for instance in list_instances():
            settings = read_trading_settings(instance["id"])
            mode = instance["type"]
            if not settings.get(f"{mode}Trading", {}).get("enabled"):
                continue
            runner = self._scan_runner(instance["id"])
            if runner["running"]:
                continue
            scan = read_latest_scan(instance_id=instance["id"])
            fetched_at = scan.get("fetchedAt")
            fetched_ts = self._parse_timestamp(fetched_at)
            if fetched_ts is not None and slot_start <= fetched_ts < slot_end:
                continue
            self.start_scan(instance["id"], "scheduled")

    def _maybe_start_scheduled_trade(self) -> None:
        now_ts = time.time()
        for instance in list_instances():
            settings = read_trading_settings(instance["id"])
            mode = instance["type"]
            if not settings.get(f"{mode}Trading", {}).get("enabled"):
                continue
            start_ts, end_ts = self._aligned_slot(now_ts, settings["decisionIntervalMinutes"])
            if not (start_ts <= now_ts < start_ts + SCHEDULE_TRIGGER_WINDOW_SECONDS):
                continue
            runner = self._trade_runner(instance["id"])
            if runner["running"]:
                continue
            latest_ts = self._latest_scheduled_trade_ts(instance["id"])
            if latest_ts is not None and start_ts <= latest_ts < end_ts:
                continue
            self.start_trade(instance["id"], "scheduled")

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


CLIENT_DISCONNECT_ERRORS = (BrokenPipeError, ConnectionResetError)


def _json_response(handler: BaseHTTPRequestHandler, payload: Any, status: int = 200) -> bool:
    data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    try:
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json; charset=utf-8")
        handler.send_header("Cache-Control", "no-store")
        handler.send_header("Content-Length", str(len(data)))
        handler.end_headers()
        handler.wfile.write(data)
        return True
    except CLIENT_DISCONNECT_ERRORS:
        return False


def _text_response(handler: BaseHTTPRequestHandler, payload: str, status: int = 200, content_type: str = "text/plain; charset=utf-8") -> bool:
    data = payload.encode("utf-8")
    try:
        handler.send_response(status)
        handler.send_header("Content-Type", content_type)
        handler.send_header("Cache-Control", "no-store")
        handler.send_header("Content-Length", str(len(data)))
        handler.end_headers()
        handler.wfile.write(data)
        return True
    except CLIENT_DISCONNECT_ERRORS:
        return False


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

    def _instance_route_parts(self, path: str) -> tuple[str | None, str | None]:
        parts = [item for item in path.split("/") if item]
        if len(parts) < 2 or parts[0] != "api" or parts[1] != "instances":
            return None, None
        if len(parts) == 2:
            return None, ""
        instance_id = parts[2]
        subpath = "/" + "/".join(parts[3:]) if len(parts) > 3 else "/"
        return instance_id, subpath

    def _handle_instance_api(self, method: str, instance_id: str | None, subpath: str) -> bool:
        if instance_id is None and subpath == "":
            if method == "GET":
                return _json_response(self, self.runtime.instances_payload()) or True
            if method == "POST":
                payload = _read_json_body(self)
                instance = create_instance(str(payload.get("name") or ""), str(payload.get("type") or "paper"))
                self.runtime.record_log("INFO", f"实例已创建，name={instance['name']}，type={instance['type']}", instance["id"])
                return _json_response(self, {"instance": instance, "instances": self.runtime.instances_payload()["instances"]}) or True
            return _text_response(self, "Method not allowed", status=405) or True

        if not instance_id:
            return False

        if method == "POST" and subpath == "/rename":
            payload = _read_json_body(self)
            instance = rename_instance(instance_id, str(payload.get("name") or ""))
            self.runtime.record_log("INFO", f"实例已重命名，name={instance['name']}", instance_id)
            return _json_response(self, {"instance": instance, "instances": self.runtime.instances_payload()["instances"]}) or True
        if method == "POST" and subpath == "/clone-live":
            source = read_instance(instance_id)
            if source["type"] != "paper":
                raise ValueError("只有模拟盘实例可以一键克隆为实盘。")
            payload = _read_json_body(self)
            instance = clone_instance(instance_id, "live", str(payload.get("name") or ""))
            self.runtime.record_log("INFO", f"已从模拟盘克隆实盘实例，source={instance_id}，target={instance['id']}，name={instance['name']}")
            self.runtime.record_log("INFO", "当前实盘克隆已创建，默认保持暂停，请检查实盘配置后再启动。", instance["id"])
            return _json_response(self, {"instance": instance, "instances": self.runtime.instances_payload()["instances"]}) or True
        if method == "POST" and subpath == "/delete":
            runner = self.runtime._trade_runner(instance_id)
            scan_runner = self.runtime._scan_runner(instance_id)
            if runner["running"] or scan_runner["running"]:
                raise RuntimeError("正在运行的实例不能删除，请先暂停。")
            delete_instance(instance_id)
            self.runtime.record_log("WARN", f"实例已删除，id={instance_id}")
            return _json_response(self, {"deletedId": instance_id, "instances": self.runtime.instances_payload()["instances"]}) or True
        if method == "GET" and subpath == "/state":
            payload = summarize_trading_state(instance_id)
            payload["tradeRunner"] = self.runtime._trade_runner(instance_id)
            payload["scanRunner"] = self.runtime._scan_runner(instance_id)
            payload["nextDecisionDueAt"] = self.runtime.next_trade_due_at(instance_id)
            return _json_response(self, payload) or True
        if method == "GET" and subpath == "/logs":
            return _json_response(self, self.runtime.api_logs(instance_id)) or True
        if method == "GET" and subpath == "/latest":
            scan = read_latest_scan(instance_id=instance_id)
            opportunities = scan.get("opportunities", [])
            return _json_response(
                self,
                {
                    "updatedAt": now_iso(),
                    "scan": {
                        "runDate": scan.get("runDate"),
                        "fetchedAt": scan.get("fetchedAt"),
                        "opportunities": len(opportunities),
                        "scanRunner": self.runtime._scan_runner(instance_id),
                    },
                },
            ) or True
        if method == "GET" and subpath == "/opportunities":
            return _json_response(self, read_latest_scan(instance_id=instance_id)) or True
        if method == "GET" and subpath == "/trading/settings":
            return _json_response(self, {**read_trading_settings(instance_id), "exchangeCatalog": read_live_exchange_catalog(), "instance": read_instance(instance_id)}) or True
        if method == "POST" and subpath == "/trading/settings":
            result = write_trading_settings(_read_json_body(self), instance_id)
            refresh_result = refresh_account_state_after_settings_save(instance_id=instance_id)
            live_sync_warnings = refresh_result.get("liveSyncWarnings") if isinstance(refresh_result, dict) else []
            live_sync_attempted = bool(refresh_result.get("liveSyncAttempted")) if isinstance(refresh_result, dict) else False
            self.runtime.record_log(
                "INFO",
                "运行设置已保存，"
                f"decisionIntervalMinutes={result.get('decisionIntervalMinutes')}，"
                f"activeExchange={result.get('activeExchange', 'binance')}，"
                f"paper={result.get('paperTrading', {}).get('enabled', False)}，"
                f"live={result.get('liveTrading', {}).get('enabled', False)}",
                instance_id,
            )
            if live_sync_warnings:
                self.runtime.record_log("INFO", f"保存运行设置后已刷新实盘账户：{'; '.join(str(item) for item in live_sync_warnings[:2])}", instance_id)
            elif live_sync_attempted:
                self.runtime.record_log("INFO", "保存运行设置后已刷新实盘账户状态。", instance_id)
            return _json_response(self, {**result, "exchangeCatalog": read_live_exchange_catalog(), "instance": read_instance(instance_id)}) or True
        if method == "GET" and subpath == "/provider":
            return _json_response(self, read_llm_provider(instance_id)) or True
        if method == "POST" and subpath == "/provider":
            result = write_llm_provider(_read_json_body(self), instance_id)
            self.runtime.record_log("INFO", f"模型配置已保存，provider={result.get('preset', 'custom')}，model={result.get('model', 'n/a')}", instance_id)
            return _json_response(self, result) or True
        if method == "GET" and subpath == "/universe":
            universe = read_fixed_universe(instance_id)
            universe["rawSymbols"] = "\n".join(universe.get("symbols", []))
            universe["candidateSourceCode"] = read_candidate_source_code(instance_id)
            return _json_response(self, universe) or True
        if method == "POST" and subpath == "/universe":
            result = write_fixed_universe(_read_json_body(self), instance_id)
            result["rawSymbols"] = "\n".join(result.get("symbols", []))
            result["candidateSourceCode"] = read_candidate_source_code(instance_id)
            self.runtime.record_log("INFO", f"候选池配置已保存，symbols={len(result.get('symbols', []))}，dynamic={result.get('dynamicSource', {}).get('enabled', False)}", instance_id)
            return _json_response(self, result) or True
        if method == "POST" and subpath == "/universe/test":
            payload = _read_json_body(self)
            universe = preview_fixed_universe(payload, instance_id)
            code_override = payload.get("candidateSourceCode") if "candidateSourceCode" in payload else None
            result = test_candidate_source(universe=universe, code_override=code_override, instance_id=instance_id)
            self.runtime.record_log("INFO", f"候选池测试已完成，mode={result.get('mode')}，symbols={result.get('count', 0)}", instance_id)
            return _json_response(self, result) or True
        if method == "GET" and subpath == "/prompt":
            return _json_response(self, _prompt_form_payload(read_prompt_settings(instance_id))) or True
        if method == "GET" and subpath == "/prompt-library":
            library = read_prompt_library(instance_id)
            return _json_response(self, {"updated": library.get("updated"), "prompts": [_prompt_form_payload(item) for item in library.get("prompts", [])]}) or True
        if method == "POST" and subpath == "/prompt":
            payload = _read_json_body(self)
            payload["decision_logic"] = _prompt_logic_from_payload(payload, read_prompt_settings(instance_id))
            result = write_prompt_settings(payload, instance_id)
            self.runtime.record_log("INFO", f"交易逻辑已保存，name={result.get('name', 'default_trading_logic')}", instance_id)
            return _json_response(self, _prompt_form_payload(result)) or True
        if method == "POST" and subpath == "/prompt-library/save":
            payload = _read_json_body(self)
            payload["decision_logic"] = _prompt_logic_from_payload(payload, read_prompt_settings(instance_id))
            result = save_prompt_preset(payload, instance_id)
            preset = result.get("preset") if isinstance(result.get("preset"), dict) else {}
            self.runtime.record_log("INFO", f"Prompt 模板已保存，name={preset.get('name', 'untitled')}，id={preset.get('id', 'n/a')}", instance_id)
            return _json_response(self, {"preset": _prompt_form_payload(preset), "prompts": [_prompt_form_payload(item) for item in result.get("prompts", [])]}) or True
        if method == "POST" and subpath == "/prompt-library/use":
            payload = _read_json_body(self)
            preset = read_prompt_preset(str(payload.get("id") or ""), instance_id)
            result = write_prompt_settings({"name": preset.get("name"), "presetId": preset.get("id"), "klineFeeds": preset.get("klineFeeds"), "decision_logic": preset.get("decision_logic")}, instance_id)
            self.runtime.record_log("INFO", f"Prompt 模板已启用，name={preset.get('name', 'untitled')}，id={preset.get('id', 'n/a')}", instance_id)
            return _json_response(self, _prompt_form_payload(result)) or True
        if method == "POST" and subpath == "/prompt-library/rename":
            payload = _read_json_body(self)
            result = rename_prompt_preset(str(payload.get("id") or ""), str(payload.get("name") or ""), instance_id)
            preset = result.get("preset") if isinstance(result.get("preset"), dict) else {}
            self.runtime.record_log("INFO", f"Prompt 模板已重命名，name={preset.get('name', 'untitled')}，id={preset.get('id', 'n/a')}", instance_id)
            return _json_response(self, {"preset": _prompt_form_payload(preset), "prompts": [_prompt_form_payload(item) for item in result.get("prompts", [])]}) or True
        if method == "POST" and subpath == "/prompt-library/delete":
            payload = _read_json_body(self)
            result = delete_prompt_preset(str(payload.get("id") or ""), instance_id)
            self.runtime.record_log("WARN", f"Prompt 模板已删除，id={result.get('deletedId', 'n/a')}", instance_id)
            return _json_response(self, {"deletedId": result.get("deletedId"), "prompts": [_prompt_form_payload(item) for item in result.get("prompts", [])]}) or True
        if method == "POST" and subpath == "/prompt/test":
            payload = _read_json_body(self)
            prompt_override = None
            if {"role", "corePrinciplesText", "entryPreferencesText", "positionManagementText"} & set(payload.keys()) or "rawJson" in payload:
                prompt_override = {
                    "name": payload.get("name") or "default_trading_logic",
                    "klineFeeds": payload.get("klineFeeds"),
                    "decision_logic": _prompt_logic_from_payload(payload, read_prompt_settings(instance_id)),
                }
            mode = read_instance(instance_id)["type"]
            result = preview_trading_prompt_decision(mode_override=mode, prompt_override=prompt_override, instance_id=instance_id)
            provider_info = result.get("provider") if isinstance(result.get("provider"), dict) else {}
            if provider_info.get("autoConfiguredSaved"):
                self.runtime.record_log("INFO", f"模型网关已自动识别并保存，preset={provider_info.get('preset', 'n/a')}，apiStyle={provider_info.get('resolvedApiStyle', 'n/a')}", instance_id)
            self.runtime.record_log("INFO", f"Prompt 测试已完成，mode={mode}，candidates={result.get('candidateCount', 0)}", instance_id)
            return _json_response(self, result) or True
        if method == "GET" and subpath == "/live-config":
            return _json_response(self, {**read_live_trading_config(instance_id), "exchangeCatalog": read_live_exchange_catalog()}) or True
        if method == "POST" and subpath == "/live-config":
            result = write_live_trading_config(_read_json_body(self), instance_id)
            self.runtime.record_log("INFO", f"实盘账号配置已保存，exchange={result.get('exchange', 'binance')}", instance_id)
            return _json_response(self, {**result, "exchangeCatalog": read_live_exchange_catalog()}) or True
        if method == "GET" and subpath == "/network":
            return _json_response(self, read_network_settings(instance_id)) or True
        if method == "GET" and subpath == "/network/ip":
            return _json_response(self, _network_ip_payload(instance_id)) or True
        if method == "POST" and subpath == "/network":
            result = write_network_settings(_read_json_body(self), instance_id)
            self.runtime.record_log("INFO", f"代理配置已保存，enabled={result.get('proxyEnabled', False)}", instance_id)
            return _json_response(self, result) or True
        if method == "POST" and subpath == "/run":
            started = self.runtime.start_trade(instance_id, "manual")
            if not started:
                self.runtime.record_log("WARN", "收到手动交易请求，但上一轮仍在执行。", instance_id)
            return _json_response(self, {"started": started, "tradeRunner": self.runtime._trade_runner(instance_id), "nextDecisionDueAt": self.runtime.next_trade_due_at(instance_id)}) or True
        if method == "POST" and subpath == "/reset":
            mode = read_instance(instance_id)["type"]
            result = reset_trading_account(mode, instance_id)
            self.runtime.record_log("WARN", f"{'实盘' if mode == 'live' else '模拟盘'}账户已重置。", instance_id)
            return _json_response(self, result) or True
        if method == "POST" and subpath == "/flatten":
            mode = read_instance(instance_id)["type"]
            result = flatten_active_account("manual_flatten", mode_override=mode, instance_id=instance_id)
            self.runtime.record_log("WARN", "已执行全部平仓。", instance_id)
            return _json_response(self, result) or True
        if method == "POST" and subpath == "/scan/run":
            started = self.runtime.start_scan(instance_id, "manual")
            if not started:
                self.runtime.record_log("WARN", "收到手动候选池刷新请求，但上一轮刷新仍在执行。", instance_id)
            return _json_response(self, {"started": started, "scanRunner": self.runtime._scan_runner(instance_id)}) or True
        return False

    def do_GET(self) -> None:  # noqa: N802
        self._handle("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._handle("POST")

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return

    def _handle(self, method: str) -> None:
        parsed = urlparse(self.path)
        try:
            instance_id, subpath = self._instance_route_parts(parsed.path)
            if instance_id is not None or subpath == "":
                handled = self._handle_instance_api(method, instance_id, subpath)
                if handled:
                    return
            if method == "GET" and parsed.path == "/api/latest":
                payload = self.runtime.instances_payload()
                return _json_response(self, payload)
            if method == "GET" and parsed.path == "/api/opportunities":
                instances = list_instances()
                default_id = instances[0]["id"] if instances else None
                return _json_response(self, read_latest_scan(instance_id=default_id))
            if method == "GET" and parsed.path == "/api/logs":
                query = {}
                if parsed.query:
                    for pair in parsed.query.split("&"):
                        key, _, value = pair.partition("=")
                        query[key] = value
                return _json_response(self, self.runtime.api_logs(query.get("instance") or None))
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
                live_sync_attempted = bool(refresh_result.get("liveSyncAttempted")) if isinstance(refresh_result, dict) else False
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
                elif live_sync_attempted:
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
        except CLIENT_DISCONNECT_ERRORS:
            return
        except Exception as error:
            self.runtime.record_log("ERROR", f"{method} {parsed.path} 失败：{error}")
            return _json_response(self, {"error": str(error)}, status=500)

    def _serve_static(self, request_path: str) -> None:
        relative = "index.html" if request_path == "/" else request_path.lstrip("/")
        file_path = (DASHBOARD_DIR / relative).resolve()
        dashboard_root = DASHBOARD_DIR.resolve()
        if dashboard_root not in file_path.parents and file_path != dashboard_root:
            return _text_response(self, "Forbidden", status=403)
        if not file_path.exists() or not file_path.is_file():
            return _text_response(self, "Not found", status=404)
        payload = file_path.read_bytes()
        try:
            self.send_response(200)
            self.send_header("Content-Type", _static_content_type(file_path))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except CLIENT_DISCONNECT_ERRORS:
            return


def _next_available_port(host: str, preferred_port: int, max_checks: int = 20) -> int:
    for port in range(preferred_port, preferred_port + max_checks + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if sock.connect_ex((host, port)) != 0:
                return port
    raise RuntimeError(f"Could not find a free port near {preferred_port}.")


def start_server(port_override: int | None = None) -> None:
    settings = read_trading_settings()
    _ = list_instances()
    host = settings["server"]["host"]
    preferred_port = int(port_override if port_override is not None else settings["server"]["port"])
    if preferred_port < 1024 or preferred_port > 65535:
        raise ValueError("Port must be between 1024 and 65535.")
    port = _next_available_port(host, preferred_port)
    runtime = AppRuntime()
    TradingAgentHandler.runtime = runtime
    server = ThreadingHTTPServer((host, port), TradingAgentHandler)
    runtime.start_scheduler()
    runtime.record_log("INFO", f"Trading Agent dashboard running at http://{host}:{port}/")
    server.serve_forever()
