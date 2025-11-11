# -*- coding: utf-8 -*-
"""
Шаг 9 (Demand Gen):
- генерирует имя рекламной группы;
- открывает «Channels», переключается на режим «Let me choose»;
- с помощью LLM выбирает, где показывать рекламу;
- завершает конфигурацию (интерфейс сам переносит на аудитории).
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webdriver import WebDriver, WebElement

from examples.steps import step4 as base_step4  # type: ignore

try:
    from ads_ai.llm.gemini import GeminiClient  # type: ignore
except Exception:  # pragma: no cover
    GeminiClient = None  # type: ignore

logger = logging.getLogger("ads_ai.gads.step9.demand_gen")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

_emit = base_step4._emit  # type: ignore
_maybe_handle_confirm_its_you = base_step4._maybe_handle_confirm_its_you  # type: ignore
_dismiss_soft_dialogs = base_step4._dismiss_soft_dialogs  # type: ignore
_ensure_panel_open = base_step4._ensure_panel_open  # type: ignore
_is_interactable = base_step4._is_interactable  # type: ignore

AD_GROUP_NAME_INPUT_SELECTOR = 'input[aria-label="Ad group name"], material-input input[aria-label="Ad group name"]'
CHANNEL_PANEL_SYNS: List[str] = [
    "channels",
    "каналы",
    "рекламные каналы",
    "ad channels",
    "inventory",
]

LLM_MODEL = os.getenv("GEMINI_MODEL", "models/gemini-2.0-flash")


class _ConfirmItsYouWatcher:
    """Параллельно «пробивает» confirm-диалог, пока идёт шаг."""

    def __init__(self, driver: WebDriver, emit: Optional[Callable[[str], None]], interval: float = 0.8) -> None:
        self._driver = driver
        self._emit = emit
        self._interval = max(0.3, float(interval))
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                _maybe_handle_confirm_its_you(self._driver, self._emit)
            except Exception:
                pass
            self._stop.wait(self._interval)

    def __enter__(self) -> "_ConfirmItsYouWatcher":
        _maybe_handle_confirm_its_you(self._driver, self._emit)
        if self._thread is None:
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._loop,
                name="step9-confirm-watcher",
                daemon=True,
            )
            self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.2)
            self._thread = None
        return False


def _maybe_handle_confirm_async(
    driver: WebDriver,
    *,
    emit: Optional[Callable[[str], None]] = None,
    timeout: float = 6.0,
    interval: float = 0.35,
) -> bool:
    handled = False
    if timeout <= 0:
        return bool(_maybe_handle_confirm_its_you(driver, emit))
    deadline = time.time() + timeout
    interval = max(0.1, min(interval, 1.0))
    while time.time() < deadline:
        if _maybe_handle_confirm_its_you(driver, emit):
            handled = True
        time.sleep(interval)
    return handled


def _set_input_value(driver: WebDriver, selector: str, value: str) -> bool:
    try:
        _maybe_handle_confirm_its_you(driver, emit=None)
        return bool(
            driver.execute_script(
                """
                const selector = arguments[0];
                const value = arguments[1];
                const node = document.querySelector(selector);
                if (!node) return false;
                const input = node.matches('input') ? node : node.querySelector('input');
                if (!input) return false;
                input.focus();
                input.value = '';
                input.dispatchEvent(new Event('input', {bubbles: true}));
                input.value = value || '';
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
        logger.debug("Ad-group input set failed (%s): %s", selector, exc)
        return False


def _derive_slug(business_name: Optional[str], usp: Optional[str], site_url: Optional[str]) -> str:
    raw = " ".join(filter(None, [business_name or "", usp or "", site_url or ""]))
    slug = re.sub(r"[^a-z0-9]+", "-", raw.lower()).strip("-")
    return slug or "adgroup"


def _fallback_ad_group_name(
    *,
    business_name: Optional[str],
    usp: Optional[str],
    site_url: Optional[str],
) -> str:
    slug = _derive_slug(business_name, usp, site_url)
    digits = datetime.utcnow().strftime("%y%m%d")
    suffix = f"{random.randint(10, 99)}"
    base = f"{slug[:18]}-{digits}-{suffix}"
    return base[:60]


def _decide_ad_group_name_via_llm(
    *,
    business_name: Optional[str],
    usp: Optional[str],
    site_url: Optional[str],
    campaign_context: Optional[str] = None,
) -> Dict[str, str]:
    fallback = {
        "name": _fallback_ad_group_name(business_name=business_name, usp=usp, site_url=site_url),
        "reason": "Fallback ad-group name.",
        "source": "fallback",
    }

    if GeminiClient is None:
        return fallback

    payload = {
        "task": (
            "Create a concise Google Ads Demand Gen ad group name. "
            "Return ONLY JSON with keys: name (<=50 chars, must contain letters and digits), reason (<=160 chars). "
            "Avoid generic placeholders; make it descriptive for the campaign context."
        ),
        "context": {
            "business_name": (business_name or "").strip(),
            "usp": (usp or "").strip(),
            "site_url": (site_url or "").strip(),
            "campaign_context": (campaign_context or "").strip(),
        },
        "guidelines": [
            "Name must include digits (e.g. year, offer code, or channel hint).",
            "Use Latin characters only.",
            "Avoid special characters except '-' or '_'.",
        ],
        "output_schema": {
            "name": "string",
            "reason": "string",
        },
    }

    try:
        client = GeminiClient(model=LLM_MODEL, temperature=0.2, retries=1, fallback_model=None)  # type: ignore
        raw = client.generate_json(json.dumps(payload, ensure_ascii=False))
    except Exception as exc:  # pragma: no cover
        logger.warning("LLM ad-group naming failed: %s — fallback used.", exc)
        return fallback

    if not isinstance(raw, dict):
        return fallback

    name = str(raw.get("name") or "").strip()
    reason = str(raw.get("reason") or "").strip() or "LLM generated name."
    if not name:
        return fallback
    if len(name) > 60 or not re.search(r"[0-9]", name) or re.search(r"[^a-zA-Z0-9 _\\-]", name):
        return fallback

    return {
        "name": name,
        "reason": reason,
        "source": "llm",
    }


def _find_ad_group_name_input(driver: WebDriver) -> Optional[WebElement]:
    try:
        el = driver.execute_script(
            """
            return document.querySelector('material-input[aria-label*="Ad group name" i] input') ||
                   document.querySelector('material-input input[aria-label*="ad group name" i]') ||
                   document.querySelector('input[aria-label="Ad group name"]');
            """
        )
        return el  # type: ignore
    except Exception:
        return None


def _select_let_me_choose(driver: WebDriver) -> bool:
    try:
        _maybe_handle_confirm_its_you(driver, emit=None)
        return bool(
            driver.execute_script(
                """
                const PATTERNS = [
                  'let me choose',
                  'пусть я выберу',
                  'выбрать самостоятельно',
                  'выбрать вручную',
                  'seleccionar manualmente',
                  'choisir manuellement'
                ];
                const radios = [...document.querySelectorAll('material-radio, material-radio button, material-radio .content')];
                for (const radio of radios) {
                    const root = radio.closest('material-radio') || radio;
                    if (!root) continue;
                    const txt = ((root.innerText || root.textContent || '') + ' ' + (root.getAttribute('aria-label') || '')).toLowerCase();
                    if (!txt.trim()) continue;
                    if (!PATTERNS.some(p => txt.includes(p))) continue;
                    const isChecked = (root.getAttribute('aria-checked') || '').toLowerCase() === 'true';
                    if (isChecked) return true;
                    try { root.click(); return true; } catch (e) {}
                    const target = root.querySelector('.content, label, button');
                    if (target) {
                        try { target.click(); return true; } catch (e2) {}
                    }
                }
                return false;
                """
            )
        )
    except Exception as exc:  # pragma: no cover
        logger.debug("Selecting «Let me choose» failed: %s", exc)
        return False


def _collect_channel_options(driver: WebDriver) -> List[Dict[str, Any]]:
    try:
        raw = driver.execute_script(
            """
            const result = [];
            const container = document.querySelector('tree-picker');
            if (!container) return result;
            const rows = [...container.querySelectorAll('.item-container[role="treeitem"]')];
            let seq = 0;
            for (const row of rows) {
                const checkbox = row.querySelector('material-checkbox');
                if (!checkbox) continue;
                const hasChildren = !!row.getAttribute('aria-owns');
                if (hasChildren) continue;
                const labelEl = row.querySelector('inventory-controls-picker-item .label');
                const descEl = row.querySelector('inventory-controls-picker-item .description');
                const label = (labelEl && labelEl.textContent || '').trim();
                if (!label) continue;
                let id = checkbox.getAttribute('data-step9-channel-id');
                if (!id) {
                    seq += 1;
                    id = `step9-ch-${seq}`;
                    checkbox.setAttribute('data-step9-channel-id', id);
                    row.setAttribute('data-step9-channel-id', id);
                } else {
                    row.setAttribute('data-step9-channel-id', id);
                }
                const description = (descEl && descEl.textContent || '').trim();
                const checked = (checkbox.getAttribute('aria-checked') || '').toLowerCase() === 'true';
                const disabled = (checkbox.getAttribute('aria-disabled') || '').toLowerCase() === 'true';
                const pathParts = [];
                let current = row;
                while (current) {
                    const labelNode = current.querySelector('inventory-controls-picker-item .label');
                    const txt = (labelNode && labelNode.textContent || '').trim();
                    if (txt) pathParts.unshift(txt);
                    const parentSubitems = current.parentElement ? current.parentElement.closest('.children, .subitems') : null;
                    if (!parentSubitems) break;
                    const parentRow = parentSubitems.previousElementSibling;
                    if (!parentRow || !parentRow.classList || !parentRow.classList.contains('item-container')) break;
                    current = parentRow;
                }
                result.push({
                    id,
                    label,
                    description,
                    path: pathParts,
                    checked,
                    disabled,
                });
            }
            return result;
            """
        )
    except Exception as exc:  # pragma: no cover
        logger.debug("Failed to collect channel options: %s", exc)
        return []

    options: List[Dict[str, Any]] = []
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            try:
                channel_id = str(item.get("id") or "").strip()
                label = str(item.get("label") or "").strip()
                description = str(item.get("description") or "").strip()
                path_raw = item.get("path") or []
                path = [str(p or "").strip() for p in path_raw if str(p or "").strip()]
                checked = bool(item.get("checked"))
                disabled = bool(item.get("disabled"))
            except Exception:
                continue
            if not channel_id or not label:
                continue
            options.append(
                {
                    "id": channel_id,
                    "label": label,
                    "description": description,
                    "path": path or [label],
                    "checked": checked,
                    "disabled": disabled,
                }
            )
    return options


def _normalize_reason_text(text: Optional[str], *, default: str) -> str:
    value = str(text or "").strip()
    if not value:
        return default
    if not re.search(r"[А-Яа-яЁё]", value):
        return default
    return value


def _decide_channels_via_llm(
    *,
    options: Sequence[Dict[str, Any]],
    business_name: Optional[str],
    usp: Optional[str],
    site_url: Optional[str],
    campaign_context: Optional[str],
) -> Dict[str, Any]:
    valid_ids = {opt["id"] for opt in options}
    label_to_id = {opt["label"].lower(): opt["id"] for opt in options}
    path_to_id = {" > ".join(opt["path"]).lower(): opt["id"] for opt in options}
    id_to_label = {opt["id"]: opt["label"] for opt in options}
    fallback_ids = [opt["id"] for opt in options if opt["checked"]]
    if not fallback_ids and options:
        fallback_ids = [options[0]["id"]]

    fallback = {
        "selected_ids": fallback_ids,
        "selected": [
            {"id": cid, "reason": "Стандартный канал Google — оставляем включенным."}
            for cid in fallback_ids
        ],
        "reason": "Сохраняю стандартный набор каналов Google.",
        "source": "fallback",
    }

    if not options or GeminiClient is None:
        return fallback

    payload = {
        "task": (
            "Choose only the Demand Gen inventory options that are truly relevant for the campaign. "
            "Return ONLY JSON with keys: select (array of objects {id, reason}) and reason (<=180 chars summary). "
            "Use only ids from the provided list; choose at least one. "
            "Write every reason in Russian."
        ),
        "context": {
            "business_name": (business_name or "").strip(),
            "usp": (usp or "").strip(),
            "site_url": (site_url or "").strip(),
            "campaign_context": (campaign_context or "").strip(),
        },
        "options": [
            {
                "id": opt["id"],
                "path": " > ".join(opt["path"]),
                "label": opt["label"],
                "description": opt["description"],
                "default_selected": bool(opt["checked"]),
            }
            for opt in options
        ],
        "guidelines": [
            "Prioritise channels that match the creative offer and audience.",
            "Exclude channels that conflict with the context or seem redundant.",
            "Provide a short reason (<=120 chars) in Russian for every selected channel.",
        ],
        "output_schema": {
            "select": "array",
            "reason": "string",
        },
    }

    try:
        client = GeminiClient(model=LLM_MODEL, temperature=0.2, retries=1, fallback_model=None)  # type: ignore
        raw = client.generate_json(json.dumps(payload, ensure_ascii=False))
    except Exception as exc:  # pragma: no cover
        logger.warning("LLM channel decision failed: %s — fallback used.", exc)
        return fallback

    if not isinstance(raw, dict):
        return fallback

    raw_select = raw.get("select") or raw.get("selected") or raw.get("choices") or []
    chosen: List[Dict[str, str]] = []
    if isinstance(raw_select, list):
        for item in raw_select:
            candidate_id: Optional[str] = None
            reason_val = ""
            if isinstance(item, dict):
                candidate_id = str(item.get("id") or item.get("channel") or "").strip()
                reason_val = str(item.get("reason") or item.get("why") or "").strip()
            elif isinstance(item, str):
                candidate_id = item.strip()
            else:
                continue
            if not candidate_id:
                continue
            lower = candidate_id.lower()
            resolved_id = None
            if candidate_id in valid_ids:
                resolved_id = candidate_id
            elif lower in label_to_id:
                resolved_id = label_to_id[lower]
            elif lower in path_to_id:
                resolved_id = path_to_id[lower]
            if not resolved_id:
                continue
            chosen.append(
                {
                    "id": resolved_id,
                    "reason": reason_val,
                }
            )

    chosen_ids = []
    filtered: List[Dict[str, str]] = []
    for choice in chosen:
        cid = choice.get("id")
        if not cid or cid not in valid_ids:
            continue
        if cid in chosen_ids:
            continue
        chosen_ids.append(cid)
        label = id_to_label.get(cid, cid)
        filtered.append(
            {
                "id": cid,
                "reason": _normalize_reason_text(
                    choice.get("reason"),
                    default=f"Канал «{label}» соответствует целевой аудитории кампании.",
                ),
            }
        )

    if not chosen_ids:
        return fallback

    reason = _normalize_reason_text(
        raw.get("reason") or raw.get("comment"),
        default="Каналы подобраны под цель кампании.",
    )

    return {
        "selected_ids": list(dict.fromkeys(chosen_ids)),
        "selected": filtered or [{"id": cid, "reason": ""} for cid in chosen_ids],
        "reason": reason,
        "source": "llm",
    }


def _apply_channel_selection(driver: WebDriver, desired_ids: Sequence[str]) -> Dict[str, List[str]]:
    desired = [str(cid) for cid in desired_ids if str(cid).strip()]
    try:
        _maybe_handle_confirm_its_you(driver, emit=None)
        result = driver.execute_script(
            """
            const desired = new Set(arguments[0] || []);
            const getChecked = (box) => ((box.getAttribute('aria-checked') || '').toLowerCase() === 'true');
            const isDisabled = (box) => ((box.getAttribute('aria-disabled') || '').toLowerCase() === 'true');
            const tryClick = (node) => {
                if (!node) return false;
                if (typeof node.scrollIntoView === 'function') {
                    try { node.scrollIntoView({block: 'center', inline: 'nearest', behavior: 'instant'}); } catch (err) {}
                }
                try { node.click(); return true; } catch (err) {}
                try {
                    node.dispatchEvent(new MouseEvent('click', {bubbles: true, cancelable: true, view: window}));
                    return true;
                } catch (err2) {}
                return false;
            };
            const ensureState = (box, wantChecked) => {
                const disabled = isDisabled(box);
                if (disabled) {
                    return getChecked(box) === wantChecked;
                }
                let attempt = 0;
                while (attempt < 3) {
                    const current = getChecked(box);
                    if (current === wantChecked) return true;
                    const row = box.closest('.item-container[role="treeitem"]');
                    const targets = [box, box.querySelector('input'), row?.querySelector('.content'), row];
                    let clicked = false;
                    for (const target of targets) {
                        if (!target) continue;
                        if (tryClick(target)) {
                            clicked = true;
                            break;
                        }
                    }
                    if (!clicked) break;
                    attempt += 1;
                }
                return getChecked(box) === wantChecked;
            };

            const boxes = [...document.querySelectorAll('material-checkbox[data-step9-channel-id]')];
            const missing = [];
            const extra = [];
            const disabledMissing = [];

            for (const box of boxes) {
                const id = box.getAttribute('data-step9-channel-id');
                if (!id) continue;
                const wantChecked = desired.has(id);
                if (!ensureState(box, wantChecked)) {
                    const disabled = isDisabled(box);
                    const actual = getChecked(box);
                    if (wantChecked && !actual) {
                        (disabled ? disabledMissing : missing).push(id);
                    } else if (!wantChecked && actual && !disabled) {
                        extra.push(id);
                    }
                }
            }

            return {missing, extra, disabledMissing};
            """,
            desired,
        )
        if not isinstance(result, dict):
            return {"missing": [], "extra": [], "disabledMissing": []}
        normalized = {
            "missing": [str(v) for v in result.get("missing", []) if v],
            "extra": [str(v) for v in result.get("extra", []) if v],
            "disabledMissing": [str(v) for v in result.get("disabledMissing", []) if v],
        }
        return normalized
    except Exception as exc:  # pragma: no cover
        logger.debug("Applying channel selection failed: %s", exc)
        return {"missing": [], "extra": [], "disabledMissing": []}


def run_step9(
    driver: WebDriver,
    *,
    business_name: Optional[str] = None,
    site_url: Optional[str] = None,
    usp: Optional[str] = None,
    locations: Optional[Iterable[str]] = None,  # noqa: ARG002 (поддерживаем сигнатуру)
    languages: Optional[Iterable[str]] = None,  # noqa: ARG002
    campaign_context: Optional[str] = None,
    timeout_total: float = 120.0,
    emit: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    started = time.time()
    _dismiss_soft_dialogs(driver, budget_ms=800)
    logger.info(
        "step9 DemandGen: start (business=%s, site=%s, usp=%s)",
        (business_name or "").strip() or "-",
        (site_url or "").strip() or "-",
        (usp or "").strip() or "-",
    )

    channel_reason_common = "Каналы подобраны под цель кампании."

    with _ConfirmItsYouWatcher(driver, emit=emit):
        _maybe_handle_confirm_async(driver, emit=emit, timeout=min(6.0, timeout_total * 0.15))

        # --- Ad group name ---
        name_decision = _decide_ad_group_name_via_llm(
            business_name=business_name,
            usp=usp,
            site_url=site_url,
            campaign_context=campaign_context,
        )
        ad_group_name = name_decision["name"]
        _emit(emit, f"Имя рекламной группы: {ad_group_name}")

        name_input = _find_ad_group_name_input(driver)
        if not name_input or not _is_interactable(driver, name_input):
            _maybe_handle_confirm_async(driver, emit=emit, timeout=2.5)
            name_input = _find_ad_group_name_input(driver)
        if not name_input or not _is_interactable(driver, name_input):
            raise RuntimeError("Поле «Ad group name» не найдено или недоступно.")
        if not _set_input_value(driver, AD_GROUP_NAME_INPUT_SELECTOR, ad_group_name):
            try:
                name_input.clear()
            except Exception:
                pass
            try:
                name_input.send_keys(Keys.CONTROL, "a")  # type: ignore[arg-type]
                name_input.send_keys(Keys.DELETE)
            except Exception:
                pass
            name_input.send_keys(ad_group_name)
        time.sleep(0.2)

        _maybe_handle_confirm_async(driver, emit=emit, timeout=2.0)

        # --- Channels ---
        _emit(emit, "Открываю панель «Channels»")
        if not _ensure_panel_open(driver, CHANNEL_PANEL_SYNS):
            raise RuntimeError("Раздел «Channels» не найден.")
        _maybe_handle_confirm_async(driver, emit=emit, timeout=1.5)

        if not _select_let_me_choose(driver):
            raise RuntimeError("Не удалось переключиться в режим «Let me choose» для каналов.")
        time.sleep(0.3)
        _maybe_handle_confirm_async(driver, emit=emit, timeout=1.5)

        options = _collect_channel_options(driver)
        if not options:
            raise RuntimeError("Не удалось получить список каналов для выбора.")
        logger.info("step9 DemandGen: обнаружено %d каналов для выбора", len(options))

        _emit(emit, "Выбираю каналы показов через нейросеть")
        channel_decision = _decide_channels_via_llm(
            options=options,
            business_name=business_name,
            usp=usp,
            site_url=site_url,
            campaign_context=campaign_context,
        )
        selected_specs = channel_decision.get("selected") or []
        desired_ids = [cid for cid in channel_decision.get("selected_ids", []) if cid]
        if not desired_ids:
            raise RuntimeError("Нейросеть не предложила ни одного канала показа.")
        logger.info(
            "step9 DemandGen: модель выбрала %d каналов (source=%s)",
            len(desired_ids),
            channel_decision.get("source"),
        )
        channel_reason_common = _normalize_reason_text(
            channel_decision.get("reason"),
            default="Каналы подобраны под цель кампании.",
        )

        id_to_label_map = {opt["id"]: opt["label"] for opt in options}
        reason_map: Dict[str, str] = {}
        for spec in selected_specs:
            if not isinstance(spec, dict) or not spec.get("id"):
                continue
            cid = str(spec.get("id") or "").strip()
            if not cid:
                continue
            label_display = id_to_label_map.get(cid, cid)
            reason_map[cid] = _normalize_reason_text(
                spec.get("reason"),
                default=f"Канал «{label_display}» подходит под кампанию.",
            )
        desired_set = set(desired_ids)

        selected_channels: List[Dict[str, Any]] = []
        final_options: List[Dict[str, Any]] = []
        selection_mismatch = {"missing": [], "extra": [], "disabled_extra": [], "disabled_missing": []}
        apply_outcome: Dict[str, List[str]] = {}
        for attempt in range(3):
            apply_outcome = _apply_channel_selection(driver, desired_ids)
            time.sleep(0.4)
            _maybe_handle_confirm_async(driver, emit=emit, timeout=1.5)
            final_options = _collect_channel_options(driver)
            selected_channels = [opt for opt in final_options if opt["checked"]]
            selected_ids_now = {opt["id"] for opt in selected_channels}
            controllable_ids = {
                opt["id"] for opt in selected_channels if not opt.get("disabled")
            }
            missing = desired_set - selected_ids_now
            controllable_extra = controllable_ids - desired_set
            if not missing and not controllable_extra:
                selection_mismatch = {"missing": [], "extra": [], "disabled_extra": [], "disabled_missing": []}
                break
            extra_all = selected_ids_now - desired_set
            logger.debug(
                "Channel selection mismatch (attempt %d): missing=%s extra=%s apply_outcome=%s",
                attempt + 1,
                list(missing),
                list(extra_all),
                apply_outcome,
            )
            if attempt < 2:
                _emit(emit, "Повторяю выбор каналов — не все совпали после первого клика")
                continue
        else:
            selected_ids_now = {opt["id"] for opt in selected_channels}
            controllable_ids = {
                opt["id"] for opt in selected_channels if not opt.get("disabled")
            }
            missing = sorted(desired_set - selected_ids_now)
            controllable_extra = sorted(controllable_ids - desired_set)
            disabled_extra = sorted(selected_ids_now - desired_set - set(controllable_extra))
            # Disabled каналы, которые не удалось включить, фиксируем отдельно.
            disabled_missing = sorted(set(apply_outcome.get("disabledMissing", [])))
            selection_mismatch = {
                "missing": missing,
                "extra": controllable_extra,
                "disabled_extra": disabled_extra,
                "disabled_missing": disabled_missing,
            }
            logger.warning(
                "step9 DemandGen: не удалось добиться точного совпадения каналов. missing=%s extra=%s disabled_extra=%s disabled_missing=%s",
                missing,
                controllable_extra,
                disabled_extra,
                disabled_missing,
            )
            if missing:
                _emit(
                    emit,
                    "Не удалось включить каналы: " + ", ".join(missing),
                )
            if controllable_extra:
                _emit(
                    emit,
                    "Некоторые каналы остались включёнными: " + ", ".join(controllable_extra),
                )
            if disabled_extra:
                _emit(
                    emit,
                    "Часть каналов заблокирована интерфейсом и не может быть отключена: "
                    + ", ".join(disabled_extra),
                )
            if apply_outcome.get("disabledMissing"):
                _emit(
                    emit,
                    "Интерфейс не даёт включить каналы: " + ", ".join(apply_outcome["disabledMissing"]),
                )
            # продолжаем с фактическим набором выбранных каналов

        if not selected_channels:
            raise RuntimeError("После выбора не осталось доступных каналов показа.")

        selected_ids_now = {opt["id"] for opt in selected_channels}
        disabled_extras = [
            opt for opt in selected_channels if opt.get("disabled") and opt["id"] not in desired_set
        ]
        if disabled_extras:
            labels = [
                " > ".join(opt["path"]) if len(opt["path"]) > 1 else opt["label"] for opt in disabled_extras
            ]
            _emit(emit, f"Обязательные каналы (нельзя отключить): {', '.join(labels)}")

        selected_paths = [
            " > ".join(opt["path"]) if len(opt["path"]) > 1 else opt["label"] for opt in selected_channels
        ]
        _emit(emit, f"Выбранные каналы: {', '.join(selected_paths)}")
        for opt in selected_channels:
            display = " > ".join(opt["path"]) if len(opt["path"]) > 1 else opt["label"]
            reason = _normalize_reason_text(
                reason_map.get(opt["id"]) or channel_reason_common,
                default=f"Канал «{display}» подходит под кампанию.",
            )
            if reason:
                _emit(emit, f"- {display}: {reason}")
        if channel_reason_common:
            _emit(emit, f"Общее обоснование: {channel_reason_common}")

        _maybe_handle_confirm_async(driver, emit=emit, timeout=2.5)
        _dismiss_soft_dialogs(driver, budget_ms=600)

        # На Demand Gen экране после выбора каналов нет отдельной кнопки Next — интерфейс автоматически
        # переключает нас к настройке аудитории (шаг 10). Просто выходим, сохранив состояние.
        _emit(emit, "Шаг 9 завершён — продолжу на экране аудитории.")

    logger.info("step9 DemandGen: переход к шагу 10")
    elapsed = int((time.time() - started) * 1000)
    final_options = _collect_channel_options(driver)
    final_selected: List[Dict[str, Any]] = []
    for opt in final_options:
        if not opt["checked"]:
            continue
        path_display = " > ".join(opt["path"]) if opt["path"] else opt["label"]
        if opt["id"] not in desired_set and opt.get("disabled"):
            reason = "Канал обязателен для Demand Gen (нельзя отключить)."
        else:
            reason = _normalize_reason_text(
                reason_map.get(opt["id"]) or channel_reason_common,
                default=f"Канал «{path_display}» подходит под кампанию.",
            )
        final_selected.append(
            {
                "id": opt["id"],
                "label": opt["label"],
                "path": path_display,
                "description": opt["description"],
                "reason": reason,
            }
        )

    logger.info(
        "step9 DemandGen OK (%d ms). AdGroup='%s' (source=%s) | channels=%s | mismatch=%s",
        elapsed,
        ad_group_name,
        name_decision.get("source"),
        [opt["path"] for opt in final_selected],
        selection_mismatch,
    )
    _emit(emit, "Настройки группы объявлений сохранены")

    return {
        "ad_group_name": ad_group_name,
        "ad_group_name_source": name_decision.get("source"),
        "ad_group_name_reason": name_decision.get("reason"),
        "channels_selected": final_selected,
        "channels_decision_source": channel_decision.get("source"),
        "channels_decision_reason": channel_reason_common,
        "channels_selection_mismatch": selection_mismatch,
        "duration_ms": elapsed,
    }
