# -*- coding: utf-8 -*-
"""
Шаг 6 (Demand Gen):
- раскрывает секцию устройств и включает режим «Set specific targeting for devices»;
- запрашивает у LLM, какие категории устройств оставить активными, и применяет выбор;
- открывает диалог операционных систем, повторяет процедуру выбора через LLM и жмёт Done;
- возвращает управление без перехода на следующий экран.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from selenium.webdriver.remote.webdriver import WebDriver

from examples.steps import step4 as base_step4  # type: ignore

try:
    from ads_ai.llm.gemini import GeminiClient  # type: ignore
except Exception:  # pragma: no cover
    GeminiClient = None  # type: ignore

logger = logging.getLogger("ads_ai.gads.step6.demand_gen")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

_emit = base_step4._emit  # type: ignore
_maybe_handle_confirm_its_you = base_step4._maybe_handle_confirm_its_you  # type: ignore
_dismiss_soft_dialogs = base_step4._dismiss_soft_dialogs  # type: ignore

LLM_DEFAULT_MODEL = os.getenv("GEMINI_MODEL", "models/gemini-2.0-flash")

DEVICES_KEYWORDS: List[str] = [
    "device",
    "devices",
    "устройства",
    "dispositivos",
    "aparelhos",
    "geräte",
    "dispositivi",
    "urządzenia",
]

OS_DIALOG_MARKERS: List[str] = [
    "choose operating systems",
    "operating systems",
    "операционн",
]


def _normalize_token(value: Optional[str]) -> str:
    if not value:
        return ""
    token = "".join(ch.lower() if ch.isalnum() else "_" for ch in value)
    return token.strip("_")


def _ensure_list(value: Any) -> List[str]:
    if isinstance(value, (list, tuple, set)):
        return [str(v) for v in value if v is not None]
    if isinstance(value, str):
        return [value]
    return []


def _open_devices_panel(driver: WebDriver) -> bool:
    _maybe_handle_confirm_its_you(driver, emit=None)
    try:
        return bool(
            driver.execute_script(
                """
                const keywords = new Set((arguments[0] || []).map(v => String(v || '').toLowerCase()));
                const headers = [...document.querySelectorAll('[role="button"][aria-controls]')];
                for (const header of headers) {
                    const text = ((header.getAttribute('aria-label') || '') + ' ' + (header.textContent || '')).toLowerCase();
                    const matches = [...keywords].some(k => text.includes(k));
                    if (!matches) continue;
                    const expanded = (header.getAttribute('aria-expanded') || '').toLowerCase() === 'true';
                    if (!expanded) {
                        try { header.click(); } catch (e) {
                            const btn = header.querySelector('.expand-button, .icon-container');
                            if (btn) btn.click();
                        }
                    }
                    return true;
                }
                return false;
                """,
                DEVICES_KEYWORDS,
            )
        )
    except Exception as exc:  # pragma: no cover
        logger.debug("Devices panel open failed: %s", exc)
        return False


def _select_specific_device_mode(driver: WebDriver) -> bool:
    _maybe_handle_confirm_its_you(driver, emit=None)
    try:
        return bool(
            driver.execute_script(
                """
                const radio = document.querySelector('material-radio[debugid="target-selection-radio"]');
                if (!radio) return false;
                const checked = (radio.getAttribute('aria-checked') || '').toLowerCase() === 'true';
                if (checked) return true;
                const target = radio.querySelector('.icon-container, .content, label') || radio;
                try { target.click(); return true; } catch (e) {
                    radio.dispatchEvent(new MouseEvent('click', {bubbles:true,cancelable:true,view:window}));
                    return true;
                }
                """,
            )
        )
    except Exception as exc:  # pragma: no cover
        logger.debug("Specific device mode toggle failed: %s", exc)
        return False


def _collect_device_options(driver: WebDriver) -> List[Dict[str, Any]]:
    try:
        data = driver.execute_script(
            """
            const clean = (value) => (value || '').replace(/\\s+/g, ' ').trim();
            const nodes = [...document.querySelectorAll('material-checkbox[debugid$="-bid-modifier"]')];
            return nodes.map(node => {
                const debugid = node.getAttribute('debugid') || '';
                const checked = (node.getAttribute('aria-checked') || '').toLowerCase() === 'true';
                const labelNode = node.querySelector('.description, .bid-modifier-item, .render-cell, label');
                const label = clean(labelNode ? labelNode.textContent : node.textContent);
                return {debugid, label, checked};
            });
            """
        )
    except Exception as exc:  # pragma: no cover
        logger.debug("Device option collection failed: %s", exc)
        data = None

    options: List[Dict[str, Any]] = []
    if isinstance(data, list):
        for row in data:
            if not isinstance(row, dict):
                continue
            debugid = str(row.get("debugid") or "").strip()
            label = str(row.get("label") or "").strip()
            if not debugid or not label:
                continue
            options.append(
                {
                    "debugid": debugid,
                    "label": label,
                    "slug": _normalize_token(label),
                    "checked": bool(row.get("checked")),
                }
            )
    return options


def _decide_devices(
    options: Sequence[Dict[str, Any]],
    *,
    business_name: Optional[str],
    usp: Optional[str],
    site_url: Optional[str],
    locations: Optional[Iterable[str]],
    languages: Optional[Iterable[str]],
) -> Dict[str, Any]:
    if not options:
        return {
            "source": "empty",
            "include": [],
            "reason": "No device checkboxes detected.",
        }

    fallback_include = [_normalize_token(opt["label"]) for opt in options]
    fallback = {
        "source": "fallback",
        "include": fallback_include,
        "reason": "LLM unavailable — keeping all device categories.",
    }

    if GeminiClient is None:
        return fallback

    payload = {
        "task": (
            "Select which device categories should remain enabled for a Google Ads Demand Gen campaign. "
            "Return ONLY JSON with keys: include_devices (list of slugs from options) and reason (<=120 chars). "
            "If all devices should stay enabled, include every slug."
        ),
        "options": [{"slug": opt["slug"], "label": opt["label"]} for opt in options],
        "context": {
            "business_name": (business_name or "").strip(),
            "usp": (usp or "").strip(),
            "site_url": (site_url or "").strip(),
            "locations": list(locations or []) if locations else [],
            "languages": list(languages or []) if languages else [],
        },
        "guidelines": [
            "Use only slugs from options.",
            "Demand Gen campaigns usually remain mobile-first unless product is desktop-only B2B.",
            "Keep TV screens unless they clearly conflict with the brief.",
        ],
        "output_schema": {
            "include_devices": ["slug"],
            "reason": "string",
        },
    }

    try:
        client = GeminiClient(model=LLM_DEFAULT_MODEL, temperature=0.2, retries=1, fallback_model=None)  # type: ignore
        raw = client.generate_json(json.dumps(payload, ensure_ascii=False))
    except Exception as exc:  # pragma: no cover
        logger.warning("LLM device decision failed: %s — using fallback.", exc)
        return fallback

    if not isinstance(raw, dict):
        return fallback

    allowed_slugs: Set[str] = {opt["slug"] for opt in options}
    include_slugs = [
        slug for slug in (_normalize_token(s) for s in _ensure_list(raw.get("include_devices")))
        if slug in allowed_slugs
    ]
    if not include_slugs:
        include_slugs = list(allowed_slugs)

    reason = str(raw.get("reason") or "").strip() or "LLM device decision."
    return {
        "source": "llm",
        "include": sorted(set(include_slugs)),
        "reason": reason,
    }


def _set_device_checkbox(driver: WebDriver, debugid: str, value: bool) -> bool:
    try:
        return bool(
            driver.execute_script(
                """
                const debugid = arguments[0];
                const desired = !!arguments[1];
                const node = document.querySelector(`material-checkbox[debugid="${debugid}"]`);
                if (!node) return false;
                const current = (node.getAttribute('aria-checked') || '').toLowerCase() === 'true';
                if (current === desired) return true;
                const target = node.querySelector('.icon-container, .content, label') || node;
                try { target.click(); return true; } catch (e) {
                    node.dispatchEvent(new MouseEvent('click', {bubbles:true,cancelable:true,view:window}));
                    return true;
                }
                """,
                debugid,
                value,
            )
        )
    except Exception as exc:  # pragma: no cover
        logger.debug("Device checkbox toggle failed (%s): %s", debugid, exc)
        return False


def _apply_device_decision(
    driver: WebDriver,
    options: Sequence[Dict[str, Any]],
    decision: Dict[str, Any],
) -> Dict[str, Any]:
    desired_slugs = set(decision.get("include") or [])
    if not desired_slugs:
        desired_slugs = {opt["slug"] for opt in options}

    selected_labels: List[str] = []
    failed_labels: List[str] = []

    for opt in options:
        slug = opt["slug"]
        should_enable = slug in desired_slugs
        current = bool(opt.get("checked"))
        if current == should_enable:
            if should_enable:
                selected_labels.append(opt["label"])
            continue
        _maybe_handle_confirm_its_you(driver, emit=None)
        if _set_device_checkbox(driver, opt["debugid"], should_enable):
            if should_enable:
                selected_labels.append(opt["label"])
        else:
            failed_labels.append(opt["label"])
        time.sleep(0.12)

    return {
        "selected_labels": selected_labels,
        "failed_labels": failed_labels,
        "included_slugs": sorted(desired_slugs),
    }


def _open_os_dialog(driver: WebDriver) -> bool:
    _maybe_handle_confirm_its_you(driver, emit=None)
    try:
        return bool(
            driver.execute_script(
                """
                const normalize = value => (value || '').toLowerCase();
                const containers = [...document.querySelectorAll('operating-system-version, targeting-summary, [role="region"]')];
                for (const node of containers) {
                    const text = normalize((node.getAttribute('aria-label') || '') + ' ' + (node.textContent || ''));
                    if (!/operating\\s*systems?/.test(text)) continue;

                    const button =
                        node.querySelector('[role="button"]') ||
                        node.closest('[role="button"]') ||
                        node.querySelector('.targeting-panel, .targeting-header');

                    if (!button) continue;

                    try { button.click(); return true; } catch (e) {
                        button.dispatchEvent(new MouseEvent('click', {bubbles:true,cancelable:true,view:window}));
                        return true;
                    }
                }
                return false;
                """
            )
        )
    except Exception as exc:  # pragma: no cover
        logger.debug("Open OS dialog failed: %s", exc)
        return False


def _find_os_dialog(driver: WebDriver) -> bool:
    try:
        return bool(
            driver.execute_script(
                """
                const markers = (arguments[0] || []).map(v => String(v || '').toLowerCase());
                const dialogs = [...document.querySelectorAll('material-dialog')];
                for (const dlg of dialogs) {
                    const text = (dlg.innerText || dlg.textContent || '').toLowerCase();
                    if (!markers.some(marker => text.includes(marker))) continue;
                    const styles = window.getComputedStyle(dlg);
                    if (styles.visibility === 'hidden' || styles.display === 'none') continue;
                    return true;
                }
                return false;
                """,
                OS_DIALOG_MARKERS,
            )
        )
    except Exception:  # pragma: no cover
        return False


def _wait_os_dialog(driver: WebDriver, visible: bool, timeout: float = 8.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        _maybe_handle_confirm_its_you(driver, emit=None)
        if _find_os_dialog(driver) == visible:
            return True
        time.sleep(0.2)
    return _find_os_dialog(driver) == visible


def _collect_os_options(driver: WebDriver) -> List[Dict[str, Any]]:
    try:
        data = driver.execute_script(
            """
            const dialogs = [...document.querySelectorAll('material-dialog')];
            const dlg = dialogs.find(d => /operating systems/i.test(d.innerText || d.textContent || ''));
            if (!dlg) return [];
            const clean = value => (value || '').replace(/\\s+/g, ' ').trim();
            const boxes = [...dlg.querySelectorAll('material-checkbox.check')];
            return boxes.map(box => {
                const row = box.closest('.row');
                const labelNode = row?.querySelector('.string-cell, .render-cell') || row || box;
                const label = clean(labelNode ? labelNode.textContent : '');
                const checked = (box.getAttribute('aria-checked') || '').toLowerCase() === 'true';
                return {label, checked};
            });
            """
        )
    except Exception as exc:  # pragma: no cover
        logger.debug("OS option collection failed: %s", exc)
        data = None

    options: List[Dict[str, Any]] = []
    if isinstance(data, list):
        for row in data:
            if not isinstance(row, dict):
                continue
            label = str(row.get("label") or "").strip()
            if not label:
                continue
            options.append(
                {
                    "label": label,
                    "slug": _normalize_token(label),
                    "checked": bool(row.get("checked")),
                }
            )
    return options


def _decide_os(
    options: Sequence[Dict[str, Any]],
    *,
    business_name: Optional[str],
    usp: Optional[str],
    device_decision: Dict[str, Any],
) -> Dict[str, Any]:
    if not options:
        return {
            "source": "empty",
            "selected": [],
            "reason": "No operating-system options detected.",
        }

    fallback = {
        "source": "fallback",
        "selected": [],
        "reason": "LLM unavailable — keeping all operating systems.",
    }

    if GeminiClient is None:
        return fallback

    payload = {
        "task": (
            "Choose which operating systems to target. "
            "Return ONLY JSON with keys: operating_systems (list of slugs from options) and reason (<=120 chars). "
            "Empty list keeps defaults."
        ),
        "options": [{"slug": opt["slug"], "label": opt["label"]} for opt in options],
        "device_choice": {
            "include": device_decision.get("include", []),
            "reason": device_decision.get("reason"),
        },
        "context": {
            "business_name": (business_name or "").strip(),
            "usp": (usp or "").strip(),
        },
        "guidelines": [
            "Return only slugs from options.",
            "Prefer mainstream mobile OS unless the brief requires otherwise.",
        ],
        "output_schema": {
            "operating_systems": ["slug"],
            "reason": "string",
        },
    }

    try:
        client = GeminiClient(model=LLM_DEFAULT_MODEL, temperature=0.2, retries=1, fallback_model=None)  # type: ignore
        raw = client.generate_json(json.dumps(payload, ensure_ascii=False))
    except Exception as exc:  # pragma: no cover
        logger.warning("LLM OS decision failed: %s — using fallback.", exc)
        return fallback

    if not isinstance(raw, dict):
        return fallback

    allowed_slugs: Set[str] = {opt["slug"] for opt in options}
    selected = [
        slug for slug in (_normalize_token(s) for s in _ensure_list(raw.get("operating_systems")))
        if slug in allowed_slugs
    ]
    reason = str(raw.get("reason") or "").strip() or "LLM operating-system decision."

    return {
        "source": "llm",
        "selected": sorted(set(selected)),
        "reason": reason,
    }


def _set_os_option(driver: WebDriver, slug: str, value: bool) -> bool:
    try:
        return bool(
            driver.execute_script(
                """
                const slug = arguments[0];
                const desired = !!arguments[1];
                const normalize = text => (text || '').toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
                const dlg = [...document.querySelectorAll('material-dialog')]
                    .find(d => /operating systems/i.test(d.innerText || d.textContent || ''));
                if (!dlg) return false;
                const boxes = [...dlg.querySelectorAll('material-checkbox.check')];
                for (const box of boxes) {
                    const row = box.closest('.row');
                    const labelNode = row?.querySelector('.string-cell, .render-cell') || row || box;
                    const label = normalize(labelNode ? labelNode.textContent : '');
                    if (!label || label !== slug) continue;
                    const current = (box.getAttribute('aria-checked') || '').toLowerCase() === 'true';
                    if (current === desired) return true;
                    const target = box.querySelector('.icon-container, .content, label') || box;
                    try { target.click(); return true; } catch (e) {
                        box.dispatchEvent(new MouseEvent('click', {bubbles:true,cancelable:true,view:window}));
                        return true;
                    }
                }
                return false;
                """,
                slug,
                value,
            )
        )
    except Exception as exc:  # pragma: no cover
        logger.debug("OS option toggle failed (%s): %s", slug, exc)
        return False


def _apply_os_decision(
    driver: WebDriver,
    options: Sequence[Dict[str, Any]],
    decision: Dict[str, Any],
) -> Dict[str, Any]:
    desired_slugs = set(decision.get("selected") or [])
    selected_labels: List[str] = []
    failed_labels: List[str] = []

    if not desired_slugs:
        return {
            "selected_labels": selected_labels,
            "failed_labels": failed_labels,
            "selected_slugs": [],
        }

    for opt in options:
        slug = opt["slug"]
        should_select = slug in desired_slugs
        current = bool(opt.get("checked"))
        if current == should_select:
            if should_select:
                selected_labels.append(opt["label"])
            continue
        _maybe_handle_confirm_its_you(driver, emit=None)
        if _set_os_option(driver, slug, should_select):
            if should_select:
                selected_labels.append(opt["label"])
        else:
            failed_labels.append(opt["label"])
        time.sleep(0.12)

    return {
        "selected_labels": selected_labels,
        "failed_labels": failed_labels,
        "selected_slugs": sorted(desired_slugs),
    }


def _close_os_dialog(driver: WebDriver) -> bool:
    try:
        return bool(
            driver.execute_script(
                """
                const dlg = [...document.querySelectorAll('material-dialog')]
                    .find(d => /operating systems/i.test(d.innerText || d.textContent || ''));
                if (!dlg) return false;
                const buttons = [...dlg.querySelectorAll('material-button, button')];
                const target = buttons.find(btn => /done/i.test(btn.textContent || ''));
                if (!target) return false;
                try { target.click(); return true; } catch (e) {
                    target.dispatchEvent(new MouseEvent('click', {bubbles:true,cancelable:true,view:window}));
                    return true;
                }
                """,
            )
        )
    except Exception as exc:  # pragma: no cover
        logger.debug("Close OS dialog failed: %s", exc)
        return False


def _read_os_summary(driver: WebDriver) -> str:
    try:
        summary = driver.execute_script(
            """
            const host = document.querySelector('operating-system-version targeting-summary, operating-system-version');
            if (!host) return '';
            const text = host.querySelector('.targeting-summary, .all-targeting-summary');
            const raw = text ? text.innerText || text.textContent : host.innerText || host.textContent;
            return (raw || '').replace(/\\s+/g, ' ').trim();
            """
        )
    except Exception:  # pragma: no cover
        summary = ""
    return str(summary or "").strip()


def run_step6(
    driver: WebDriver,
    *,
    business_name: Optional[str] = None,
    site_url: Optional[str] = None,
    usp: Optional[str] = None,
    locations: Optional[Iterable[str]] = None,
    languages: Optional[Iterable[str]] = None,
    logo_prompt: Optional[str] = None,
    timeout_total: float = 90.0,
    emit: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    del logo_prompt

    t0 = time.time()
    stage_ts = t0
    stage_log: List[Tuple[str, float]] = []

    def _mark_stage(label: str) -> None:
        nonlocal stage_ts
        now = time.time()
        elapsed_stage = (now - stage_ts) * 1000.0
        elapsed_total = (now - t0) * 1000.0
        logger.debug("DemandGen Step6: %s took %.1f ms (total %.1f ms)", label, elapsed_stage, elapsed_total)
        stage_log.append((label, elapsed_stage))
        stage_ts = now

    _maybe_handle_confirm_its_you(driver, emit=emit)
    _mark_stage("initial_confirm")

    _emit(emit, "Открываю раздел устройств")
    if not _open_devices_panel(driver):
        _emit(emit, "Не удалось открыть панель устройств")
    _mark_stage("open_devices_panel")

    if not _select_specific_device_mode(driver):
        _emit(emit, "Не могу переключить режим устройств — оставляю по умолчанию")
    _mark_stage("select_device_mode")

    device_options = _collect_device_options(driver)
    _mark_stage("collect_device_options")
    if not device_options:
        _emit(emit, "Не нашёл чекбоксы устройств — пропускаю настройку")
        elapsed = int((time.time() - t0) * 1000)
        return {
            "devices_selected": [],
            "device_decision_reason": "Device controls not detected.",
            "device_decision_source": "empty",
            "devices_included_slugs": [],
            "operating_systems_selected": [],
            "operating_systems_reason": "Skipped (no devices).",
            "operating_systems_summary": _read_os_summary(driver),
            "duration_ms": elapsed,
            "timing_breakdown": [{"stage": name, "duration_ms": dur} for name, dur in stage_log],
        }

    device_decision = _decide_devices(
        device_options,
        business_name=business_name,
        usp=usp,
        site_url=site_url,
        locations=locations,
        languages=languages,
    )
    _mark_stage("decide_devices")
    _emit(emit, f"Подбираю устройства: {device_decision.get('reason')}")
    device_apply = _apply_device_decision(driver, device_options, device_decision)
    if device_apply["failed_labels"]:
        logger.warning("Device toggles failed: %s", device_apply["failed_labels"])
    _mark_stage("apply_devices")

    os_selected_labels: List[str] = []
    os_reason = "OS dialog not opened."

    _emit(emit, "Настраиваю операционные системы")
    if _open_os_dialog(driver):
        _mark_stage("open_os_dialog")
        if _wait_os_dialog(driver, visible=True, timeout=min(timeout_total, 8.0)):
            os_options = _collect_os_options(driver)
            if os_options:
                os_decision = _decide_os(
                    os_options,
                    business_name=business_name,
                    usp=usp,
                    device_decision=device_decision,
                )
                os_reason = os_decision.get("reason", "LLM operating-system decision.")
                _emit(emit, os_reason)
                os_apply = _apply_os_decision(driver, os_options, os_decision)
                os_selected_labels = os_apply["selected_labels"]
                if os_apply["failed_labels"]:
                    logger.warning("OS toggles failed: %s", os_apply["failed_labels"])
                _mark_stage("apply_os")
            else:
                os_reason = "OS dialog opened without selectable options."
            _close_os_dialog(driver)
            _wait_os_dialog(driver, visible=False, timeout=6.0)
        else:
            os_reason = "OS dialog did not appear."
            _emit(emit, os_reason)
    else:
        _emit(emit, "Не удалось открыть диалог ОС")

    os_summary = _read_os_summary(driver)
    _mark_stage("os_summary")

    elapsed = int((time.time() - t0) * 1000)
    if stage_log:
        breakdown = ", ".join(f"{name}={dur:.0f}ms" for name, dur in stage_log)
        logger.info("step6 DemandGen breakdown: %s", breakdown)
    logger.info(
        "step6 DemandGen completed (%d ms). Devices=%s | OS=%s | Summary=%s",
        elapsed,
        device_apply["selected_labels"],
        os_selected_labels,
        os_summary,
    )
    _emit(emit, "Шаг 6 завершён")

    return {
        "devices_selected": device_apply["selected_labels"],
        "device_decision_reason": device_decision.get("reason"),
        "device_decision_source": device_decision.get("source"),
        "devices_included_slugs": device_apply["included_slugs"],
        "operating_systems_selected": os_selected_labels,
        "operating_systems_reason": os_reason,
        "operating_systems_summary": os_summary,
        "duration_ms": elapsed,
        "timing_breakdown": [{"stage": name, "duration_ms": dur} for name, dur in stage_log],
    }
