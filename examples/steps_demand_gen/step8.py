# -*- coding: utf-8 -*-
"""
Шаг 8 (Demand Gen):
- раскрывает панель «Campaign URL options»;
- с помощью LLM подбирает tracking template, final URL suffix и custom parameters;
- валидирует и при необходимости использует безопасный фолбэк;
- проставляет значения и возвращает итоговую конфигурацию.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

from selenium.webdriver.remote.webdriver import WebDriver

from examples.steps import step4 as base_step4  # type: ignore

try:
    from ads_ai.llm.gemini import GeminiClient  # type: ignore
except Exception:  # pragma: no cover
    GeminiClient = None  # type: ignore

logger = logging.getLogger("ads_ai.gads.step8.demand_gen")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

_emit = base_step4._emit  # type: ignore
_maybe_handle_confirm_its_you = base_step4._maybe_handle_confirm_its_you  # type: ignore
_dismiss_soft_dialogs = base_step4._dismiss_soft_dialogs  # type: ignore
_ensure_panel_open = base_step4._ensure_panel_open  # type: ignore

LLM_MODEL = os.getenv("GEMINI_MODEL", "models/gemini-2.0-flash")
PANEL_SYNS: List[str] = [
    "campaign url options",
    "url options",
    "опции url",
    "tracking template",
    "final url suffix",
]

ALLOWED_PARAM_VALUE_CHARS = re.compile(r"^[A-Za-z0-9{}\-_.,:/=%]+$")
CUSTOM_NAME_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,31}$")
ALLOWED_MACROS = {
    "{lpurl}",
    "{lpurl_path}",
    "{unescapedlpurl}",
    "{campaignid}",
    "{adgroupid}",
    "{creative}",
    "{placement}",
    "{targetid}",
    "{feeditemid}",
    "{device}",
    "{campaignname}",
    "{adgroupname}",
    "{matchtype}",
    "{network}",
    "{keyword}",
    "{ifmobile:}",
    "{ifnotmobile:}",
}


def _maybe_handle_confirm_async(
    driver: WebDriver,
    *,
    emit: Optional[Callable[[str], None]] = None,
    timeout: float = 6.0,
    interval: float = 0.35,
) -> bool:
    """
    Повторно пингует Confirm it's you, пока не исчезнет.
    Возвращает True, если диалог был обработан.
    """
    timeout = max(0.0, float(timeout))
    interval = max(0.1, min(interval, 1.0))
    if timeout <= 0:
        return bool(_maybe_handle_confirm_its_you(driver, emit=emit))

    handled = False
    hard_deadline = time.time() + timeout
    settle_window = min(timeout, max(0.35, interval * 2.5))
    quiet_deadline = time.time() + settle_window

    while time.time() < hard_deadline:
        if _maybe_handle_confirm_its_you(driver, emit=emit):
            handled = True
            quiet_deadline = min(hard_deadline, time.time() + max(0.6, interval * 2.0))
        elif time.time() >= quiet_deadline:
            break
        time.sleep(interval)

    return handled


def _slugify(value: Optional[str]) -> str:
    if not value:
        return ""
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug[:40]


def _domain_from_url(url: Optional[str]) -> str:
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        host = parsed.hostname or ""
        return host.lower()
    except Exception:
        return ""


def _set_input_value(driver: WebDriver, selector: str, value: str) -> bool:
    """Устанавливает значение input + триггерит события."""
    try:
        _maybe_handle_confirm_its_you(driver, emit=None)
        return bool(
            driver.execute_script(
                """
                const selector = arguments[0];
                const value = arguments[1];
                const node = document.querySelector(selector);
                if (!node) return false;
                const input = node.tagName === 'INPUT' ? node : node.querySelector('input');
                if (!input) return false;
                input.focus();
                input.value = value || '';
                input.setAttribute('value', value || '');
                input.dispatchEvent(new Event('input', {bubbles: true}));
                input.dispatchEvent(new Event('change', {bubbles: true}));
                input.blur();
                return true;
                """,
                selector,
                value or "",
            )
        )
    except Exception as exc:  # pragma: no cover
        logger.debug("Set input failed (%s): %s", selector, exc)
        return False


def _get_custom_param_count(driver: WebDriver) -> int:
    try:
        count = driver.execute_script(
            """
            const container = document.querySelector('tracking-url-options .custom-params-editor');
            if (!container) return 0;
            const keys = container.querySelectorAll('material-input.parameter-key input').length;
            const values = container.querySelectorAll('material-input.parameter-value input').length;
            return Math.min(keys, values);
            """
        )
        return int(count or 0)
    except Exception:
        return 0


def _ensure_custom_param_rows(driver: WebDriver, target: int) -> bool:
    target = max(0, min(target, 8))
    for _ in range(10):
        current = _get_custom_param_count(driver)
        if current >= target or target <= 0:
            return True
        _maybe_handle_confirm_its_you(driver, emit=None)
        clicked = driver.execute_script(
            """
            const container = document.querySelector('tracking-url-options .custom-params-editor');
            if (!container) return false;
            const addBtn = container.querySelector('material-fab, button[aria-label*="Add"]');
            if (!addBtn) return false;
            try { addBtn.click(); return true; } catch (err) {
                addBtn.dispatchEvent(new MouseEvent('click', {bubbles:true,cancelable:true,view:window}));
                return true;
            }
            """
        )
        if not clicked:
            break
        time.sleep(0.3)
    return _get_custom_param_count(driver) >= target


def _set_custom_param(driver: WebDriver, index: int, name: str, value: str) -> bool:
    try:
        _maybe_handle_confirm_its_you(driver, emit=None)
        return bool(
            driver.execute_script(
                """
                const index = arguments[0];
                const name = arguments[1];
                const value = arguments[2];
                const container = document.querySelector('tracking-url-options .custom-params-editor');
                if (!container) return false;
                const keys = [...container.querySelectorAll('material-input.parameter-key input')];
                const values = [...container.querySelectorAll('material-input.parameter-value input')];
                if (index >= keys.length || index >= values.length) return false;
                const keyInput = keys[index];
                keyInput.focus();
                keyInput.value = name || '';
                keyInput.setAttribute('value', name || '');
                keyInput.dispatchEvent(new Event('input', {bubbles: true}));
                keyInput.dispatchEvent(new Event('change', {bubbles: true}));
                keyInput.blur();
                const valInput = values[index];
                valInput.focus();
                valInput.value = value || '';
                valInput.setAttribute('value', value || '');
                valInput.dispatchEvent(new Event('input', {bubbles: true}));
                valInput.dispatchEvent(new Event('change', {bubbles: true}));
                valInput.blur();
                return true;
                """,
                index,
                name or "",
                value or "",
            )
        )
    except Exception as exc:  # pragma: no cover
        logger.debug("Set custom param failed (%d): %s", index, exc)
        return False


def _derive_campaign_slug(
    *,
    business_name: Optional[str],
    site_url: Optional[str],
    usp: Optional[str],
) -> str:
    candidates = [
        business_name or "",
        usp or "",
        _domain_from_url(site_url or ""),
    ]
    raw = "-".join(filter(None, candidates))
    slug = _slugify(raw)
    return slug or "campaign"


def _build_fallback_options(
    *,
    business_name: Optional[str],
    site_url: Optional[str],
    usp: Optional[str],
) -> Dict[str, Any]:
    site_domain = _domain_from_url(site_url)
    slug = _derive_campaign_slug(business_name=business_name, site_url=site_url, usp=usp)
    tracking_template = ""
    if site_domain:
        tracking_template = f"https://{site_domain}/?lpurl={{lpurl}}"
    final_url_suffix = (
        f"utm_source=google&utm_medium=demand_gen&utm_campaign={slug}_{{campaignid}}"
        "&utm_content={adgroupid}&utm_term={targetid}&utm_id={creative}&utm_device={device}"
    )
    custom_parameters = [
        {"name": "campaign_id", "value": "{campaignid}"},
        {"name": "asset_group", "value": "{adgroupid}"},
        {"name": "creative_id", "value": "{creative}"},
        {"name": "audience", "value": "{targetid}"},
    ]
    return {
        "source": "fallback",
        "reason": "LLM unavailable — applied default Demand Gen tracking structure.",
        "tracking_template": tracking_template,
        "final_url_suffix": final_url_suffix,
        "custom_parameters": custom_parameters,
    }


PLACEHOLDER_HOSTS = {
    "example.com",
    "example.org",
    "example.net",
    "trackingtemplate.foo",
    "trackingtemplate.com",
}

PLACEHOLDER_KEYS = {"param1", "param2", "value1", "value2"}


def _sanitize_tracking_template(raw: str, *, site_domain: str) -> str:
    value = (raw or "").strip()
    if not value:
        return ""
    lower = value.lower()
    if not (lower.startswith("http://") or lower.startswith("https://")):
        return ""
    if "{lpurl" not in lower:
        return ""
    if " " in value:
        return ""
    try:
        parsed = urlparse(value)
        host = (parsed.hostname or "").lower()
    except Exception:
        return ""
    if not host:
        return ""
    if host in PLACEHOLDER_HOSTS or "example" in host:
        return ""
    if site_domain and site_domain not in host:
        # allow third-party trackers but reject obvious placeholders
        if host.endswith(".example.com") or host.endswith(".example.org"):
            return ""
    return value


def _sanitize_final_url_suffix(raw: str) -> str:
    value = (raw or "").strip().lstrip("?").strip("&")
    if not value:
        return ""
    parts = [p for p in value.split("&") if p]
    if not parts:
        return ""
    sanitized_parts: List[str] = []
    for part in parts:
        if "=" not in part:
            continue
        key, val = part.split("=", 1)
        key = key.strip()
        val = val.strip()
        if not key or not val:
            continue
        if " " in key or " " in val:
            continue
        if not ALLOWED_PARAM_VALUE_CHARS.match(key):
            continue
        if not ALLOWED_PARAM_VALUE_CHARS.match(val):
            continue
        if key.lower() in PLACEHOLDER_KEYS:
            continue
        if "example" in val.lower():
            continue
        sanitized_parts.append(f"{key}={val}")
        if len(sanitized_parts) >= 12:
            break
    return "&".join(sanitized_parts)


def _ensure_suffix_basics(suffix: str, campaign_slug: str) -> str:
    parts: List[tuple[str, str]] = []
    seen: set[str] = set()
    for part in (suffix.split("&") if suffix else []):
        if "=" not in part:
            continue
        key, val = part.split("=", 1)
        key = key.strip()
        val = val.strip()
        if not key or not val:
            continue
        lower = key.lower()
        if lower in seen:
            continue
        parts.append((key, val))
        seen.add(lower)

    def append_if_missing(key: str, value: str) -> None:
        lower = key.lower()
        if lower not in seen:
            parts.append((key, value))
            seen.add(lower)

    append_if_missing("utm_source", "google")
    append_if_missing("utm_medium", "demand_gen")
    append_if_missing("utm_campaign", f"{campaign_slug}_{{campaignid}}")

    return "&".join(f"{key}={value}" for key, value in parts)


def _sanitize_custom_parameters(raw: Iterable[Dict[str, Any]]) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    seen: set[str] = set()
    for item in raw or []:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        value = str(item.get("value") or "").strip()
        if not name or not value:
            continue
        if not CUSTOM_NAME_PATTERN.match(name):
            continue
        if " " in value:
            continue
        if not ALLOWED_PARAM_VALUE_CHARS.match(value):
            continue
        lower_value = value.lower()
        macros = re.findall(r"\{[^}]+\}", lower_value)
        if macros and not all(m in ALLOWED_MACROS for m in macros):
            continue
        if name.lower() in seen:
            continue
        seen.add(name.lower())
        if "example" in value.lower():
            continue
        results.append({"name": name, "value": value})
        if len(results) >= 5:
            break
    return results


def _decide_url_options(
    *,
    business_name: Optional[str],
    usp: Optional[str],
    site_url: Optional[str],
    languages: Optional[Iterable[str]],
) -> Dict[str, Any]:
    site_domain = _domain_from_url(site_url)
    campaign_slug = _derive_campaign_slug(business_name=business_name, site_url=site_url, usp=usp)
    campaign_keywords = [token for token in campaign_slug.split("-") if token]
    fallback = _build_fallback_options(business_name=business_name, site_url=site_url, usp=usp)

    if GeminiClient is None:
        return fallback

    payload = {
        "task": (
            "Design campaign URL options for a Google Ads Demand Gen campaign. "
            "Return ONLY JSON with keys: tracking_template (string or null), final_url_suffix (string or null), "
            "custom_parameters (list of objects with name and value), reason (<=160 chars). "
            "Tracking template must be an https URL containing {lpurl}. "
            "Final URL suffix must be a query string without leading '?' using & delimited key=value pairs. "
            "Custom parameter names must start with a letter and use only letters, numbers, or underscores. "
            "Values may include Google Ads macros such as {campaignid}, {adgroupid}, {creative}, {device}, {targetid}, {placement}. "
            "Limit custom parameters to at most 5 entries."
        ),
        "context": {
            "business_name": (business_name or "").strip(),
            "unique_selling_point": (usp or "").strip(),
            "site_url": (site_url or "").strip(),
            "site_domain": site_domain,
            "campaign_slug": campaign_slug,
            "campaign_keywords": campaign_keywords,
            "fallback_example": {
                "tracking_template": fallback.get("tracking_template"),
                "final_url_suffix": fallback.get("final_url_suffix"),
                "custom_parameters": fallback.get("custom_parameters"),
            },
            "languages": list(languages or []),
            "tracking_requirements": [
                "Include UTM parameters to track traffic source, medium, campaign, content, and term.",
                "Prefer macros for IDs to avoid hardcoding numeric values.",
                "Keep names lowercase with underscores.",
                "If no dedicated tracking domain exists, default to the advertiser domain when building the tracking template.",
                "Suffix parameters should highlight the offer (product line, service category, or audience segment).",
                "Never use placeholder domains like example.com or generic pairs such as param1=value1.",
                "Incorporate campaign_keywords or campaign_slug into utm_campaign and related parameters.",
            ],
        },
        "output_schema": {
            "tracking_template": "string|null",
            "final_url_suffix": "string|null",
            "custom_parameters": [{"name": "string", "value": "string"}],
            "reason": "string",
        },
    }

    try:
        client = GeminiClient(model=LLM_MODEL, temperature=0.2, retries=1, fallback_model=None)  # type: ignore
        raw = client.generate_json(json.dumps(payload, ensure_ascii=False))
    except Exception as exc:  # pragma: no cover
        logger.warning("LLM URL options failed: %s — fallback applied.", exc)
        return fallback

    if not isinstance(raw, dict):
        return fallback

    sanitized_template = _sanitize_tracking_template(raw.get("tracking_template") or "", site_domain=site_domain)
    sanitized_suffix_raw = _sanitize_final_url_suffix(raw.get("final_url_suffix") or "")
    sanitized_suffix = (
        _ensure_suffix_basics(sanitized_suffix_raw, campaign_slug) if sanitized_suffix_raw else ""
    )
    sanitized_custom = _sanitize_custom_parameters(raw.get("custom_parameters") or [])

    result = dict(fallback)
    if sanitized_template:
        result["tracking_template"] = sanitized_template
        result["source"] = "llm"
    if sanitized_suffix:
        result["final_url_suffix"] = sanitized_suffix
        result["source"] = "llm"
    if sanitized_custom:
        result["custom_parameters"] = sanitized_custom
        result["source"] = "llm"
    reason = str(raw.get("reason") or "").strip()
    if reason:
        result["reason"] = reason
    return result


def run_step8(
    driver: WebDriver,
    *,
    business_name: Optional[str] = None,
    site_url: Optional[str] = None,
    usp: Optional[str] = None,
    languages: Optional[Iterable[str]] = None,
    timeout_total: float = 90.0,
    emit: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    t0 = time.time()
    stage_ts = t0
    stage_log: List[Tuple[str, float]] = []

    def _mark_stage(label: str) -> None:
        nonlocal stage_ts
        now = time.time()
        elapsed_stage = (now - stage_ts) * 1000.0
        elapsed_total = (now - t0) * 1000.0
        logger.info(
            "DemandGen Step8 stage %s | %.1f ms (total %.1f ms) stages=%d",
            label,
            elapsed_stage,
            elapsed_total,
            len(stage_log),
        )
        stage_log.append((label, elapsed_stage))
        stage_ts = now

    _maybe_handle_confirm_its_you(driver, emit=emit)
    _maybe_handle_confirm_async(driver, emit=emit, timeout=8.0)
    _mark_stage("initial_confirm")

    _emit(emit, "Открываю Campaign URL options")
    _ensure_panel_open(driver, PANEL_SYNS)
    time.sleep(0.2)
    _maybe_handle_confirm_async(driver, emit=emit, timeout=4.0)
    _mark_stage("open_panel")

    decision = _decide_url_options(
        business_name=business_name,
        usp=usp,
        site_url=site_url,
        languages=languages,
    )
    _mark_stage("url_decision")
    _emit(emit, f"Настраиваю отслеживание: {decision.get('reason', decision.get('source'))}")

    tracking_template = decision.get("tracking_template") or ""
    final_url_suffix = decision.get("final_url_suffix") or ""
    custom_parameters: List[Dict[str, str]] = decision.get("custom_parameters") or []

    if tracking_template:
        ok = _set_input_value(
            driver,
            'material-input[debugid="tracking-template-input"] input',
            tracking_template,
        )
        if not ok:
            logger.warning("Failed to set tracking template via selector.")
    else:
        _set_input_value(
            driver,
            'material-input[debugid="tracking-template-input"] input',
            "",
        )
    _mark_stage("apply_tracking_template")

    if final_url_suffix:
        ok = _set_input_value(
            driver,
            'material-input[debugid="final-url-suffix-input"] input',
            final_url_suffix,
        )
        if not ok:
            logger.warning("Failed to set final URL suffix via selector.")
    else:
        _set_input_value(
            driver,
            'material-input[debugid="final-url-suffix-input"] input',
            "",
        )
    _mark_stage("apply_final_suffix")

    applied_params: List[Dict[str, str]] = []
    if custom_parameters:
        target_rows = len(custom_parameters)
        if _ensure_custom_param_rows(driver, target_rows):
            for idx, item in enumerate(custom_parameters):
                name = item.get("name") or ""
                value = item.get("value") or ""
                if not name or not value:
                    continue
                _maybe_handle_confirm_async(driver, emit=emit, timeout=1.0)
                if _set_custom_param(driver, idx, name, value):
                    applied_params.append({"name": name, "value": value})
                else:
                    logger.warning("Failed to apply custom parameter #%d (%s).", idx, name)
        else:
            logger.warning("Unable to allocate rows for custom parameters.")
    _mark_stage("apply_custom_params")

    summary_text = driver.execute_script(
        """
        const summary = document.querySelector('material-expansionpanel .summary, tracking-url-options .summary');
        if (!summary) return '';
        return (summary.textContent || summary.innerText || '').replace(/\\s+/g, ' ').trim();
        """
    )
    _mark_stage("summary_read")

    _emit(emit, "Готово — параметры отслеживания заданы")

    go_clicked = driver.execute_script(
        """
        const buttonTexts = [
            'go to ad group 1',
            'go to ad group',
            'go to asset group',
            'продолжить к группе объявлений',
        ];
        const buttons = [...document.querySelectorAll('material-button, button')];
        for (const btn of buttons) {
            const text = (btn.textContent || '').trim().toLowerCase();
            if (!text) continue;
            if (!buttonTexts.some(pattern => text.includes(pattern))) continue;
            if (btn.getAttribute('aria-disabled') === 'true' || btn.hasAttribute('disabled')) continue;
            try {
                btn.click();
            } catch (err) {
                btn.dispatchEvent(new MouseEvent('click', {bubbles:true,cancelable:true,view:window}));
            }
            return true;
        }
        return false;
        """
    )
    if go_clicked:
        _emit(emit, "Перехожу к Ad group")
    else:
        logger.info("Go to Ad group button not found or not clickable.")
    _mark_stage("go_to_ad_group")

    elapsed = int((time.time() - t0) * 1000)
    if stage_log:
        breakdown = ", ".join(f"{name}={dur:.0f}ms" for name, dur in stage_log)
        logger.info("step8 DemandGen breakdown: %s", breakdown)
    logger.info(
        "step8 DemandGen completed (%d ms). tracking_template=%s | suffix=%s | custom_params=%d",
        elapsed,
        bool(tracking_template),
        bool(final_url_suffix),
        len(applied_params),
    )

    return {
        "tracking_template": tracking_template,
        "final_url_suffix": final_url_suffix,
        "custom_parameters": applied_params,
        "decision_source": decision.get("source"),
        "decision_reason": decision.get("reason"),
        "duration_ms": elapsed,
        "panel_summary": str(summary_text or "").strip(),
        "go_to_ad_group_clicked": bool(go_clicked),
        "timing_breakdown": [{"stage": name, "duration_ms": dur} for name, dur in stage_log],
    }
