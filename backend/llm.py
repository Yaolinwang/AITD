from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from .config import read_llm_provider, read_network_settings, write_llm_provider
from .http_client import HttpRequestError, request_json
from .utils import parse_json_loose


class ModelDecisionParseError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        raw_text: str,
        raw_response: dict[str, Any] | None,
        provider_result: dict[str, Any],
    ) -> None:
        super().__init__(message)
        self.raw_text = raw_text
        self.raw_response = raw_response or {}
        self.provider_result = provider_result


def provider_issues(provider: dict[str, Any] | None = None) -> list[str]:
    provider = provider or read_llm_provider()
    issues = []
    if not provider.get("baseUrl"):
        issues.append("LLM base URL is missing.")
    if not provider.get("apiKey"):
        issues.append("LLM API key is missing.")
    if not provider.get("model"):
        issues.append("LLM model is missing.")
    if provider.get("apiStyle") not in {"openai", "anthropic"}:
        issues.append("LLM apiStyle must be openai or anthropic.")
    return issues


def provider_status(provider: dict[str, Any] | None = None) -> dict[str, Any]:
    provider = provider or read_llm_provider()
    issues = provider_issues(provider)
    return {
        "preset": provider.get("preset"),
        "apiStyle": provider.get("apiStyle"),
        "model": provider.get("model"),
        "baseUrl": provider.get("baseUrl"),
        "bypassProxy": provider.get("bypassProxy") is True,
        "configured": not issues,
        "issues": issues,
    }


def _join_endpoint(base_url: str, suffix: str) -> str:
    base = str(base_url or "").rstrip("/")
    if base.endswith(suffix):
        return base
    return f"{base}{suffix}"


def _normalized_api_base_url(base_url: str, api_style: str) -> str:
    raw = str(base_url or "").strip().rstrip("/")
    if not raw:
        return raw
    parsed = urlparse(raw)
    path = str(parsed.path or "").rstrip("/")
    if api_style in {"openai", "anthropic"} and not path:
        return f"{raw}/v1"
    return raw


def _gateway_hint(provider: dict[str, Any], request_url: str) -> str | None:
    base_url = str(provider.get("baseUrl") or "").strip()
    parsed = urlparse(base_url)
    host = str(parsed.hostname or "").lower()
    api_style = str(provider.get("apiStyle") or "").strip().lower()
    if api_style == "anthropic" and host and host not in {"api.anthropic.com", "console.anthropic.com"}:
        return (
            "当前 Base URL 不是 Anthropic 官方域名。很多第三方网关虽然支持 Claude 模型名，"
            "但接口实际上是 OpenAI compatible。系统会继续自动尝试 OpenAI compatible。"
            f" 当前请求地址：{request_url}"
        )
    return None


def _transport_error_hint(request_url: str, error: Exception, network_settings: dict[str, Any]) -> str | None:
    text = str(error)
    if "UNEXPECTED_EOF_WHILE_READING" not in text and "EOF occurred in violation of protocol" not in text:
        return None
    host = str(urlparse(request_url).hostname or "").lower()
    if host == "api.moonshot.ai":
        if network_settings.get("proxyEnabled") and network_settings.get("proxyUrl"):
            return (
                "这更像是 TLS/代理握手失败，不是 API key 错误。当前项目代理已开启，"
                "请优先尝试关闭代理后重试，或把 api.moonshot.ai 加到“不走代理的地址”。"
            )
        return "这更像是 TLS 握手失败，不是 API key 错误。请检查当前网络、VPN 或本机代理。"
    if network_settings.get("proxyEnabled") and network_settings.get("proxyUrl"):
        return "这更像是 TLS/代理握手失败。请优先尝试关闭代理后重试，或把当前域名加入“不走代理的地址”。"
    return "这更像是 TLS 握手失败。请检查当前网络、VPN 或本机代理。"


def _provider_transport_candidates(provider: dict[str, Any]) -> list[str]:
    configured = str(provider.get("apiStyle") or "").strip().lower()
    candidates: list[str] = []
    if configured in {"openai", "anthropic"}:
        candidates.append(configured)
    preset = str(provider.get("preset") or "").strip().lower()
    host = str(urlparse(str(provider.get("baseUrl") or "")).hostname or "").lower()
    if preset == "claude" and host and host not in {"api.anthropic.com", "console.anthropic.com"}:
        fallback = "openai" if configured != "openai" else "anthropic"
        if fallback not in candidates:
            candidates.append(fallback)
    if not candidates:
        candidates.append("openai")
    return candidates


def _is_deepseek_openai_provider(provider: dict[str, Any], request_api_style: str) -> bool:
    if str(request_api_style or "").strip().lower() != "openai":
        return False
    preset = str(provider.get("preset") or "").strip().lower()
    host = str(urlparse(str(provider.get("baseUrl") or "")).hostname or "").lower()
    return preset == "deepseek" or host == "api.deepseek.com"


def _persist_provider_api_style(provider: dict[str, Any], resolved_api_style: str) -> dict[str, Any] | None:
    current_style = str(provider.get("apiStyle") or "").strip().lower()
    if resolved_api_style == current_style:
        return None
    return write_llm_provider(
        {
            "preset": provider.get("preset"),
            "apiStyle": resolved_api_style,
            "model": provider.get("model"),
            "baseUrl": provider.get("baseUrl"),
            "apiKey": provider.get("apiKey"),
            "timeoutSeconds": provider.get("timeoutSeconds"),
            "temperature": provider.get("temperature"),
            "maxOutputTokens": provider.get("maxOutputTokens"),
            "bypassProxy": provider.get("bypassProxy") is True,
            "anthropicVersion": provider.get("anthropicVersion"),
            "customHeaders": provider.get("customHeaders") if isinstance(provider.get("customHeaders"), dict) else {},
        }
    )


def _provider_network_settings(provider: dict[str, Any], network_settings: dict[str, Any] | None = None) -> dict[str, Any]:
    resolved = dict(network_settings or {})
    if provider.get("bypassProxy") is True:
        resolved["proxyEnabled"] = False
    return resolved


def _openai_messages(prompt_text: str) -> list[dict[str, str]]:
    return [
        {
            "role": "system",
            "content": (
                "You are a disciplined crypto futures trading assistant. "
                "Return strict JSON only. Do not include markdown fences."
            ),
        },
        {
            "role": "user",
            "content": prompt_text,
        },
    ]


def _anthropic_message(prompt_text: str) -> tuple[str, list[dict[str, Any]]]:
    system = (
        "You are a disciplined crypto futures trading assistant. "
        "Return strict JSON only. Do not include markdown fences."
    )
    messages = [
        {
            "role": "user",
            "content": prompt_text,
        }
    ]
    return system, messages


def _extract_openai_text(response: dict[str, Any]) -> str:
    choices = response.get("choices") if isinstance(response.get("choices"), list) else []
    if not choices:
        raise ValueError("OpenAI-compatible response did not include choices.")
    message = choices[0].get("message") if isinstance(choices[0], dict) else {}
    content = message.get("content") if isinstance(message, dict) else None
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        text_parts = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                text_parts.append(str(item.get("text") or ""))
        if text_parts:
            return "\n".join(text_parts)
    raise ValueError("OpenAI-compatible response did not include message content.")


def _extract_anthropic_text(response: dict[str, Any]) -> str:
    content = response.get("content") if isinstance(response.get("content"), list) else []
    text_parts = []
    for block in content:
        if isinstance(block, dict) and block.get("type") == "text":
            text_parts.append(str(block.get("text") or ""))
    if not text_parts:
        raise ValueError("Anthropic response did not include text content.")
    return "\n".join(text_parts)


def _request_provider_text(
    prompt_text: str,
    provider: dict[str, Any],
    request_api_style: str,
    network_settings: dict[str, Any],
) -> dict[str, Any]:
    headers = dict(provider.get("customHeaders") or {})
    timeout_seconds = int(provider.get("timeoutSeconds") or 45)
    api_base_url = _normalized_api_base_url(provider.get("baseUrl") or "", request_api_style)
    if request_api_style == "anthropic":
        system, messages = _anthropic_message(prompt_text)
        url = _join_endpoint(api_base_url, "/messages")
        headers.update(
            {
                "x-api-key": provider["apiKey"],
                "anthropic-version": provider.get("anthropicVersion") or "2023-06-01",
                "content-type": "application/json",
            }
        )
        payload = {
            "model": provider["model"],
            "system": system,
            "messages": messages,
            "temperature": provider["temperature"],
            "max_tokens": provider["maxOutputTokens"],
        }
        try:
            raw_response = request_json(
                "POST",
                url,
                headers=headers,
                payload=payload,
                timeout_seconds=timeout_seconds,
                network_settings=network_settings,
            )
        except HttpRequestError as error:
            hint = _gateway_hint({**provider, "apiStyle": request_api_style}, url)
            if hint:
                raise HttpRequestError(f"{error} | {hint}") from error
            transport_hint = _transport_error_hint(url, error, network_settings)
            if transport_hint:
                raise HttpRequestError(f"{error} | {transport_hint} | 当前请求地址：{url}") from error
            raise
        raw_text = _extract_anthropic_text(raw_response)
    else:
        url = _join_endpoint(api_base_url, "/chat/completions")
        headers.update(
            {
                "Authorization": f"Bearer {provider['apiKey']}",
                "content-type": "application/json",
            }
        )
        payload = {
            "model": provider["model"],
            "messages": _openai_messages(prompt_text),
            "temperature": provider["temperature"],
            "max_tokens": provider["maxOutputTokens"],
        }
        if _is_deepseek_openai_provider(provider, request_api_style):
            payload["response_format"] = {"type": "json_object"}
        try:
            raw_response = request_json(
                "POST",
                url,
                headers=headers,
                payload=payload,
                timeout_seconds=timeout_seconds,
                network_settings=network_settings,
            )
        except HttpRequestError as error:
            transport_hint = _transport_error_hint(url, error, network_settings)
            if transport_hint:
                raise HttpRequestError(f"{error} | {transport_hint} | 当前请求地址：{url}") from error
            raise HttpRequestError(f"{error} | 当前请求地址：{url}") from error
        raw_text = _extract_openai_text(raw_response)
    return {
        "rawResponse": raw_response,
        "rawText": raw_text,
        "requestUrl": url,
        "resolvedBaseUrl": api_base_url,
        "resolvedApiStyle": request_api_style,
    }


def generate_trading_decision(
    prompt_text: str,
    provider: dict[str, Any] | None = None,
    network_settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    provider = provider or read_llm_provider()
    issues = provider_issues(provider)
    if issues:
        raise ValueError(" ".join(issues))
    configured_api_style = str(provider.get("apiStyle") or "").strip().lower()
    effective_network_settings = _provider_network_settings(provider, network_settings or read_network_settings())
    attempts: list[str] = []
    last_error: Exception | None = None
    selected_result: dict[str, Any] | None = None
    for request_api_style in _provider_transport_candidates(provider):
        try:
            selected_result = _request_provider_text(prompt_text, provider, request_api_style, effective_network_settings)
            break
        except (HttpRequestError, ValueError) as error:
            last_error = error
            attempts.append(f"{request_api_style}: {error}")
            continue
    if selected_result is None:
        if len(attempts) > 1:
            raise HttpRequestError(f"模型网关自动探测失败。已尝试 { ' | '.join(attempts) }") from last_error
        if last_error:
            raise last_error
        raise HttpRequestError("模型请求失败，未获得可用响应。")
    resolved_api_style = selected_result["resolvedApiStyle"]
    parse_attempt = 0
    while True:
        parse_attempt += 1
        try:
            parsed = parse_json_loose(selected_result["rawText"])
            break
        except Exception as error:
            should_retry = parse_attempt == 1 and _is_deepseek_openai_provider(provider, resolved_api_style)
            if should_retry:
                selected_result = _request_provider_text(prompt_text, provider, resolved_api_style, effective_network_settings)
                continue
            saved_provider = _persist_provider_api_style(provider, resolved_api_style)
            raise ModelDecisionParseError(
                str(error),
                raw_text=selected_result["rawText"],
                raw_response=selected_result["rawResponse"] if isinstance(selected_result.get("rawResponse"), dict) else {},
                provider_result={
                    "preset": provider.get("preset"),
                    "apiStyle": configured_api_style,
                    "resolvedApiStyle": resolved_api_style,
                    "model": provider.get("model"),
                    "baseUrl": provider.get("baseUrl"),
                    "bypassProxy": provider.get("bypassProxy") is True,
                    "resolvedBaseUrl": selected_result["resolvedBaseUrl"],
                    "requestUrl": selected_result["requestUrl"],
                    "autoConfigured": resolved_api_style != configured_api_style,
                    "autoConfiguredSaved": saved_provider is not None,
                },
            ) from error
    saved_provider = _persist_provider_api_style(provider, resolved_api_style)
    return {
        "provider": {
            "preset": provider.get("preset"),
            "apiStyle": configured_api_style,
            "resolvedApiStyle": resolved_api_style,
            "model": provider.get("model"),
            "baseUrl": provider.get("baseUrl"),
            "bypassProxy": provider.get("bypassProxy") is True,
            "resolvedBaseUrl": selected_result["resolvedBaseUrl"],
            "requestUrl": selected_result["requestUrl"],
            "autoConfigured": resolved_api_style != configured_api_style,
            "autoConfiguredSaved": saved_provider is not None,
        },
        "rawResponse": selected_result["rawResponse"],
        "rawText": selected_result["rawText"],
        "parsed": parsed,
    }
