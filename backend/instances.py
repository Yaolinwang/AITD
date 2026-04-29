from __future__ import annotations

import json
import shutil
import uuid
from pathlib import Path
from typing import Any

from .utils import DATA_DIR, ROOT, current_run_date, now_iso, read_json, write_json


INSTANCE_ROOT = DATA_DIR / "instances"
INSTANCE_INDEX_PATH = INSTANCE_ROOT / "index.json"
LEGACY_STATE_PATH = DATA_DIR / "trading_agent_state.json"
LEGACY_DECISIONS_DIR = DATA_DIR / "trading-agent" / "decisions"
LEGACY_SCANS_DIR = DATA_DIR / "scans"


def clean_instance_type(value: Any) -> str:
    return "live" if str(value or "paper").strip().lower() == "live" else "paper"


def instance_paths(instance_id: str) -> dict[str, Path]:
    root = INSTANCE_ROOT / str(instance_id).strip()
    return {
        "root": root,
        "trading_settings": root / "trading_settings.json",
        "live_trading": root / "live_trading.json",
        "llm_provider": root / "llm_provider.json",
        "network": root / "network.json",
        "prompt": root / "trading_prompt.json",
        "prompt_library": root / "trading_prompt_library.json",
        "fixed_universe": root / "fixed_universe.json",
        "candidate_source": root / "candidate_source.py",
        "state": root / "trading_state.json",
        "decisions_dir": root / "decisions",
        "scans_dir": root / "scans",
    }


def _normalize_instance_meta(item: dict[str, Any]) -> dict[str, Any]:
    instance_type = clean_instance_type(item.get("type"))
    instance_id = str(item.get("id") or "").strip()
    if not instance_id:
        raise ValueError("Instance id is required.")
    name = str(item.get("name") or "").strip() or ("Live Default" if instance_type == "live" else "Paper Default")
    created_at = str(item.get("createdAt") or now_iso())
    updated_at = str(item.get("updatedAt") or created_at)
    return {
        "id": instance_id,
        "name": name,
        "type": instance_type,
        "createdAt": created_at,
        "updatedAt": updated_at,
    }


def _default_index_payload() -> dict[str, Any]:
    return {
        "version": 1,
        "updatedAt": now_iso(),
        "instances": [],
    }


def read_instance_index() -> dict[str, Any]:
    payload = read_json(INSTANCE_INDEX_PATH, None)
    if not isinstance(payload, dict):
        payload = _default_index_payload()
    raw_instances = payload.get("instances") if isinstance(payload.get("instances"), list) else []
    instances: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for item in raw_instances:
        if not isinstance(item, dict):
            continue
        try:
            normalized = _normalize_instance_meta(item)
        except Exception:
            continue
        if normalized["id"] in seen_ids:
            continue
        seen_ids.add(normalized["id"])
        instances.append(normalized)
    return {
        "version": int(payload.get("version") or 1),
        "updatedAt": str(payload.get("updatedAt") or now_iso()),
        "instances": instances,
    }


def write_instance_index(instances: list[dict[str, Any]]) -> dict[str, Any]:
    normalized = [_normalize_instance_meta(item) for item in instances]
    payload = {
        "version": 1,
        "updatedAt": now_iso(),
        "instances": normalized,
    }
    write_json(INSTANCE_INDEX_PATH, payload)
    return payload


def list_instances() -> list[dict[str, Any]]:
    ensure_instances_migrated()
    return read_instance_index()["instances"]


def read_instance(instance_id: str) -> dict[str, Any]:
    target = str(instance_id or "").strip()
    if not target:
        raise ValueError("Instance id is required.")
    ensure_instances_migrated()
    instance = next((item for item in read_instance_index()["instances"] if item["id"] == target), None)
    if not instance:
        raise ValueError(f"Instance not found: {target}")
    return instance


def _copy_if_exists(source: Path, target: Path) -> None:
    if not source.exists():
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    if source.is_dir():
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(source, target)
        return
    target.write_bytes(source.read_bytes())


def _legacy_instance_payloads() -> list[dict[str, Any]]:
    return [
        {
            "id": "paper-default",
            "name": "Paper Default",
            "type": "paper",
            "createdAt": now_iso(),
            "updatedAt": now_iso(),
        },
        {
            "id": "live-default",
            "name": "Live Default",
            "type": "live",
            "createdAt": now_iso(),
            "updatedAt": now_iso(),
        },
    ]


def _seed_instance_files(instance_meta: dict[str, Any], *, migrate_from_legacy: bool) -> None:
    from .config import (
        DEFAULT_CANDIDATE_SOURCE_CODE,
        DEFAULT_FIXED_UNIVERSE_SETTINGS,
        DEFAULT_LIVE_TRADING_SETTINGS,
        DEFAULT_NETWORK_SETTINGS,
        DEFAULT_PROMPT_LIBRARY_SETTINGS,
        DEFAULT_PROMPT_SETTINGS,
        DEFAULT_PROVIDER_SETTINGS,
        DEFAULT_TRADING_SETTINGS,
    )
    from .engine import default_state

    paths = instance_paths(instance_meta["id"])
    paths["root"].mkdir(parents=True, exist_ok=True)

    if migrate_from_legacy:
        legacy_config_dir = ROOT / "config"
        _copy_if_exists(legacy_config_dir / "live_trading.json", paths["live_trading"])
        _copy_if_exists(legacy_config_dir / "llm_provider.json", paths["llm_provider"])
        _copy_if_exists(legacy_config_dir / "network.json", paths["network"])
        _copy_if_exists(legacy_config_dir / "trading_prompt.json", paths["prompt"])
        _copy_if_exists(legacy_config_dir / "trading_prompt_library.json", paths["prompt_library"])
        _copy_if_exists(legacy_config_dir / "fixed_universe.json", paths["fixed_universe"])
        _copy_if_exists(legacy_config_dir / "candidate_source.py", paths["candidate_source"])
    else:
        write_json(paths["live_trading"], DEFAULT_LIVE_TRADING_SETTINGS)
        write_json(paths["llm_provider"], DEFAULT_PROVIDER_SETTINGS)
        write_json(paths["network"], DEFAULT_NETWORK_SETTINGS)
        write_json(paths["prompt"], DEFAULT_PROMPT_SETTINGS)
        write_json(paths["prompt_library"], DEFAULT_PROMPT_LIBRARY_SETTINGS)
        write_json(paths["fixed_universe"], DEFAULT_FIXED_UNIVERSE_SETTINGS)
        paths["candidate_source"].write_text(DEFAULT_CANDIDATE_SOURCE_CODE.rstrip() + "\n", encoding="utf-8")

    legacy_trading_settings = read_json(ROOT / "config" / "trading_agent.json", {})
    trading_settings = {
        **DEFAULT_TRADING_SETTINGS,
        **(legacy_trading_settings if isinstance(legacy_trading_settings, dict) else {}),
        "mode": instance_meta["type"],
        "paperTrading": {
            "enabled": bool(
                (legacy_trading_settings.get("paperTrading", {}) if isinstance(legacy_trading_settings, dict) else {}).get("enabled")
            )
            if instance_meta["type"] == "paper"
            else False,
        },
        "liveTrading": {
            "enabled": bool(
                (legacy_trading_settings.get("liveTrading", {}) if isinstance(legacy_trading_settings, dict) else {}).get("enabled")
            )
            if instance_meta["type"] == "live"
            else False,
        },
        "server": deepcopy_dict(DEFAULT_TRADING_SETTINGS["server"]),
    }
    write_json(paths["trading_settings"], trading_settings)

    saved_state = read_json(LEGACY_STATE_PATH, {}) if migrate_from_legacy else {}
    instance_state = default_state(trading_settings)
    if instance_meta["type"] == "paper":
        instance_state["paper"] = (saved_state.get("paper") if isinstance(saved_state.get("paper"), dict) else instance_state["paper"])
    else:
        instance_state["live"] = (saved_state.get("live") if isinstance(saved_state.get("live"), dict) else instance_state["live"])
    adaptive = saved_state.get("adaptive") if isinstance(saved_state.get("adaptive"), dict) else None
    if adaptive:
        instance_state["adaptive"] = adaptive
    write_json(paths["state"], instance_state)

    if migrate_from_legacy and LEGACY_DECISIONS_DIR.exists():
        for day_dir in sorted(LEGACY_DECISIONS_DIR.iterdir()):
            if not day_dir.is_dir():
                continue
            for path in sorted(day_dir.glob("*.json")):
                payload = read_json(path, {})
                if not isinstance(payload, dict):
                    continue
                if clean_instance_type(payload.get("mode")) != instance_meta["type"]:
                    continue
                target = paths["decisions_dir"] / day_dir.name / path.name
                target.parent.mkdir(parents=True, exist_ok=True)
                write_json(target, payload)

    if migrate_from_legacy and LEGACY_SCANS_DIR.exists():
        for source in LEGACY_SCANS_DIR.iterdir():
            target = paths["scans_dir"] / source.name
            _copy_if_exists(source, target)


def deepcopy_dict(value: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(value))


def ensure_instances_migrated() -> dict[str, Any]:
    if INSTANCE_INDEX_PATH.exists():
        return read_instance_index()

    instances = _legacy_instance_payloads()
    for item in instances:
        _seed_instance_files(item, migrate_from_legacy=True)
    return write_instance_index(instances)


def create_instance(name: str, instance_type: str) -> dict[str, Any]:
    ensure_instances_migrated()
    instance_type = clean_instance_type(instance_type)
    instance_name = str(name or "").strip() or ("Live Instance" if instance_type == "live" else "Paper Instance")
    instance_meta = {
        "id": f"{instance_type}-{uuid.uuid4().hex[:8]}",
        "name": instance_name,
        "type": instance_type,
        "createdAt": now_iso(),
        "updatedAt": now_iso(),
    }
    _seed_instance_files(instance_meta, migrate_from_legacy=False)
    index = read_instance_index()
    index["instances"].append(instance_meta)
    write_instance_index(index["instances"])
    return instance_meta


def clone_instance(source_instance_id: str, target_type: str, name: str | None = None) -> dict[str, Any]:
    ensure_instances_migrated()
    source = read_instance(source_instance_id)
    target_type = clean_instance_type(target_type)
    target_name = str(name or "").strip() or (
        f"{source['name']} · {'LIVE（实盘）' if target_type == 'live' else 'PAPER（模拟）'}"
    )

    from .engine import default_state

    source_paths = instance_paths(source["id"])
    instance_meta = {
        "id": f"{target_type}-{uuid.uuid4().hex[:8]}",
        "name": target_name,
        "type": target_type,
        "createdAt": now_iso(),
        "updatedAt": now_iso(),
    }
    _seed_instance_files(instance_meta, migrate_from_legacy=False)
    target_paths = instance_paths(instance_meta["id"])

    for key in ("live_trading", "llm_provider", "network", "prompt", "prompt_library", "fixed_universe", "candidate_source"):
        _copy_if_exists(source_paths[key], target_paths[key])

    source_trading = read_json(source_paths["trading_settings"], {})
    if not isinstance(source_trading, dict):
        source_trading = {}
    target_trading = {
        **source_trading,
        "mode": target_type,
        "updated": current_run_date(),
        "paperTrading": {
            "enabled": False,
        },
        "liveTrading": {
            "enabled": False,
        },
    }
    write_json(target_paths["trading_settings"], target_trading)

    source_state = read_json(source_paths["state"], {})
    adaptive = source_state.get("adaptive") if isinstance(source_state.get("adaptive"), dict) else None
    target_state = default_state(target_trading)
    if adaptive:
        target_state["adaptive"] = adaptive
    write_json(target_paths["state"], target_state)

    index = read_instance_index()
    index["instances"].append(instance_meta)
    write_instance_index(index["instances"])
    return instance_meta


def rename_instance(instance_id: str, name: str) -> dict[str, Any]:
    target = str(instance_id or "").strip()
    new_name = str(name or "").strip()
    if not target or not new_name:
        raise ValueError("Instance id and name are required.")
    index = read_instance_index()
    updated: dict[str, Any] | None = None
    next_instances: list[dict[str, Any]] = []
    for item in index["instances"]:
        if item["id"] == target:
            updated = {
                **item,
                "name": new_name,
                "updatedAt": now_iso(),
            }
            next_instances.append(updated)
        else:
            next_instances.append(item)
    if updated is None:
        raise ValueError(f"Instance not found: {target}")
    write_instance_index(next_instances)
    return updated


def delete_instance(instance_id: str) -> None:
    target = str(instance_id or "").strip()
    if not target:
        raise ValueError("Instance id is required.")
    index = read_instance_index()
    remaining = [item for item in index["instances"] if item["id"] != target]
    if len(remaining) == len(index["instances"]):
        raise ValueError(f"Instance not found: {target}")
    paths = instance_paths(target)
    if paths["root"].exists():
        shutil.rmtree(paths["root"])
    write_instance_index(remaining)
