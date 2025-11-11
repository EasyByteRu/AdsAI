# -*- coding: utf-8 -*-
"""
Шаг 5 (Demand Gen, Campaign settings):
- раскрывает блок EU political ads и выбирает вариант «Нет»;
- включает переключатель «Campaign level location and language targeting»;
- добавляет локации: для каждой строки из CLI собирает подсказки, передаёт их в LLM и
  кликает Include по наиболее подходящей записи (fallback — эвристика);
- настраивает языки (переиспользуем базовую реализацию step4);
- завершает настройку на текущем экране (без поиска кнопки Continue).
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Callable, Dict, Iterable, List, Optional, Tuple

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webdriver import WebDriver, WebElement

from examples.steps import step4 as base_step4  # type: ignore

try:
    from ads_ai.llm.gemini import GeminiClient  # type: ignore
except Exception:  # pragma: no cover
    GeminiClient = None  # type: ignore

logger = logging.getLogger("ads_ai.gads.step5.demand_gen")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

# --- Шорткаты на утилиты step4 (не тащим туда-сюда код) ---
_emit = base_step4._emit  # type: ignore
_maybe_handle_confirm_its_you = base_step4._maybe_handle_confirm_its_you  # type: ignore
_is_interactable = base_step4._is_interactable  # type: ignore
_ensure_panel_open = base_step4._ensure_panel_open  # type: ignore
_select_enter_another_location = base_step4._select_enter_another_location  # type: ignore
_find_location_input = base_step4._find_location_input  # type: ignore
_add_languages = base_step4._add_languages  # type: ignore
_choose_eu_political_ads_no = base_step4._choose_eu_political_ads_no  # type: ignore

PANEL_LOCATIONS = base_step4.PANEL_LOCATIONS  # type: ignore
_is_confirm_dialog_visible = base_step4._is_confirm_dialog_visible  # type: ignore

_LAST_CONFIRM_PROBE_TS = 0.0


def _maybe_confirm_quick(
    driver: WebDriver,
    emit: Optional[Callable[[str], None]] = None,
    *,
    force: bool = False,
    min_interval: float = 1.1,
) -> bool:
    global _LAST_CONFIRM_PROBE_TS
    now = time.time()
    if not force and (now - _LAST_CONFIRM_PROBE_TS) < min_interval:
        return False
    _LAST_CONFIRM_PROBE_TS = now
    try:
        visible = _is_confirm_dialog_visible(driver)
    except Exception:
        visible = False
    if not visible:
        return False
    handled = _maybe_handle_confirm_its_you(driver, emit=emit)  # type: ignore
    if handled:
        _LAST_CONFIRM_PROBE_TS = time.time()
    return handled

# --- Быстрая зачистка мягких диалогов ---
# --- Дополнительные словари/синонимы именно для demand-gen UI ---
PANEL_LOCATION_LANGUAGE_SYNS: List[str] = [
    "location and language",
    "расположение и язык",
    "местоположения и язык",
    "ubicación y idioma",
    "localização e idioma",
    "lieu et langue",
    "standort und sprache",
    "località e lingua",
    "lokalizacja i język",
    "konum ve dil",
    "розташування та мова",
]

CAMPAIGN_TOGGLE_SYNS: List[str] = [
    "campaign level location",
    "location and language targeting",
    "campagne niveau emplacement",
    "standortausrichtung auf kampagnenebene",
    "на уровне кампании",
    "уровня кампании",
    "настройка на уровне кампании",
    "orientación de ubicación e idioma",
    "segmentação de local e idioma",
]

LLM_DEFAULT_MODEL = os.getenv("GEMINI_MODEL", "models/gemini-2.0-flash")


def _read_campaign_toggle_state(driver: WebDriver) -> Dict[str, bool]:
    try:
        data = driver.execute_script(
            """
            const syns = new Set(arguments[0].map(s => String(s || '').toLowerCase()));
            const nodes = [...document.querySelectorAll('material-toggle, material-toggle[label], [role="switch"], .mdc-switch__native-control')];
            const getState = (el) => {
                if (!el) return false;
                const aria = String(el.getAttribute('aria-checked') || '').toLowerCase();
                if (aria === 'true') return true;
                if (aria === 'false') return false;
                if (typeof el.checked === 'boolean') return !!el.checked;
                return false;
            };
            for (const node of nodes) {
                const root = node.matches('material-toggle, material-toggle[label]')
                    ? node
                    : (node.closest('material-toggle') || node.closest('[role="switch"]') || node);
                if (!root) continue;
                const text = ((root.getAttribute('label') || '') + ' ' + (root.innerText || root.textContent || '')).toLowerCase();
                if (!text.trim()) continue;
                const ok = [...syns].some(key => key && text.includes(key));
                if (!ok) continue;
                const input = root.querySelector('input[type="checkbox"], input[role="switch"], .mdc-switch__native-control, [role="switch"]');
                return {found: true, checked: getState(input)};
            }
            return {found: false, checked: false};
            """,
            CAMPAIGN_TOGGLE_SYNS,
        )
        if isinstance(data, dict):
            return {"found": bool(data.get("found")), "checked": bool(data.get("checked"))}
    except Exception as e:
        logger.debug("Toggle state read failed: %s", e)
    return {"found": False, "checked": False}


def _click_campaign_toggle(driver: WebDriver) -> bool:
    try:
        return bool(
            driver.execute_script(
                """
                const syns = new Set(arguments[0].map(s => String(s || '').toLowerCase()));
                const nodes = [...document.querySelectorAll('material-toggle, material-toggle[label], [role="switch"], .mdc-switch__native-control')];
                for (const node of nodes) {
                    const root = node.matches('material-toggle, material-toggle[label]')
                        ? node
                        : (node.closest('material-toggle') || node.closest('[role="switch"]') || node);
                    if (!root) continue;
                    const text = ((root.getAttribute('label') || '') + ' ' + (root.innerText || root.textContent || '')).toLowerCase();
                    if (!text.trim()) continue;
                    const ok = [...syns].some(key => key && text.includes(key));
                    if (!ok) continue;
                    const input = root.querySelector('input[type="checkbox"], input[role="switch"], .mdc-switch__native-control, [role="switch"]');
                    try {
                        if (input && typeof input.click === 'function') {
                            input.click();
                        } else if (root && typeof root.click === 'function') {
                            root.click();
                        } else {
                            const parts = root.querySelectorAll('.mdc-switch__thumb-underlay, .mdc-switch__thumb, .mdc-switch__track, label');
                            for (const p of parts) {
                                try { p.click(); return true; } catch (e) {}
                            }
                        }
                        return true;
                    } catch (e) {
                        try { root.querySelector('label')?.click(); return true; } catch (e2) {}
                    }
                    return false;
                }
                return false;
                """,
                CAMPAIGN_TOGGLE_SYNS,
            )
        )
    except Exception as e:
        logger.debug("Toggle click failed: %s", e)
        return False


def _enable_campaign_level_targeting(driver: WebDriver, emit: Optional[Callable[[str], None]] = None) -> bool:
    _ensure_panel_open(driver, PANEL_LOCATION_LANGUAGE_SYNS)
    state = _read_campaign_toggle_state(driver)
    if not state["found"]:
        logger.warning("Campaign-level toggle not found")
        return False
    if state["checked"]:
        return True
    _emit(emit, "Включаю таргетинг на уровне кампании")
    attempts = 0
    while attempts < 4:
        _maybe_confirm_quick(driver, emit=emit, force=(attempts == 0))
        clicked = _click_campaign_toggle(driver)
        poll_deadline = time.time() + 0.65
        while time.time() < poll_deadline:
            state = _read_campaign_toggle_state(driver)
            if state["checked"]:
                return True
            time.sleep(0.08)
        if not clicked:
            time.sleep(0.05)
        attempts += 1
    logger.warning("Не удалось включить campaign-level toggle — остаётся режим по умолчанию")
    return False


def _is_location_popup_visible(driver: WebDriver) -> bool:
    try:
        return bool(
            driver.execute_script(
                """
                const pop = document.querySelector('.location-suggest-popup, material-popup[arialabel="Location suggestions"]');
                if (!pop) return false;
                const cs = getComputedStyle(pop);
                if (cs.visibility === 'hidden' || cs.display === 'none' || parseFloat(cs.opacity || '1') < 0.2) return false;
                const r = pop.getBoundingClientRect();
                return r.width > 12 && r.height > 12 && r.bottom > 0 && r.right > 0;
                """
            )
        )
    except Exception:
        return False


def _wait_popup_state(driver: WebDriver, *, visible: bool, timeout: float = 6.0) -> bool:
    deadline = time.time() + timeout
    fast_until = time.time() + min(0.9, timeout * 0.4)
    while time.time() < deadline:
        _maybe_confirm_quick(driver, emit=None)
        if _is_location_popup_visible(driver) == visible:
            return True
        if time.time() < fast_until:
            time.sleep(0.05)
        else:
            time.sleep(0.1)
    return _is_location_popup_visible(driver) == visible


def _wait_location_suggestions(driver: WebDriver, timeout: float = 2.2) -> List[Dict[str, str]]:
    deadline = time.time() + timeout
    fast_until = time.time() + min(0.9, timeout * 0.5)
    while time.time() < deadline:
        _maybe_confirm_quick(driver, emit=None)
        suggestions = _collect_location_suggestions(driver)
        if suggestions:
            return suggestions
        if time.time() < fast_until:
            time.sleep(0.05)
        else:
            time.sleep(0.1)
    return _collect_location_suggestions(driver)


def _collect_location_suggestions(driver: WebDriver) -> List[Dict[str, str]]:
    try:
        data = driver.execute_script(
            """
            const pop = document.querySelector('.location-suggest-popup, material-popup[arialabel="Location suggestions"]');
            if (!pop) return [];
            const items = [...pop.querySelectorAll('.list-dynamic-item')];
            const clean = (value) => (value || '').replace(/\\s+/g, ' ').trim();
            return items.map((item, index) => {
                item.setAttribute('data-demandgen-location-index', index);
                const includeBtn = item.querySelector('.entry-button .material-button.add, .entry-button button.add, .entry-button [aria-label*="Include"]');
                const excludeBtn = item.querySelector('.entry-button .material-button.exclude, .entry-button button.exclude, .entry-button [aria-label*="Exclude"]');
                if (includeBtn) includeBtn.setAttribute('data-demandgen-location-include', index);
                if (excludeBtn) excludeBtn.setAttribute('data-demandgen-location-exclude', index);
                const name = clean(item.querySelector('.name')?.textContent || item.querySelector('.description')?.textContent);
                const type = clean(item.querySelector('.type')?.textContent);
                const reach = clean(item.querySelector('.reach')?.textContent);
                return {
                    index,
                    name,
                    type,
                    reach,
                    include_available: !!includeBtn,
                    exclude_available: !!excludeBtn,
                };
            });
            """
        )
        if isinstance(data, list):
            cleaned: List[Dict[str, str]] = []
            for item in data:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name") or "").strip()
                if not name:
                    continue
                cleaned.append(
                    {
                        "index": item.get("index", 0),
                        "name": name,
                        "type": str(item.get("type") or "").strip(),
                        "reach": str(item.get("reach") or "").strip(),
                        "include_available": item.get("include_available", False),
                        "exclude_available": item.get("exclude_available", False),
                    }
                )
            return cleaned
    except Exception as e:
        logger.debug("Collect suggestions failed: %s", e)
    return []


def _detect_exclude_action(query: str) -> bool:
    q = (query or "").strip().lower()
    if not q:
        return False
    hints = ["exclude", "исключ", "除外", "排除", "exclude:", "без ", "not ", "minus", "- "]
    return any(token in q for token in hints)


def _heuristic_location_choice(query: str, suggestions: List[Dict[str, str]]) -> Dict[str, str]:
    q = (query or "").strip().lower()
    if not suggestions:
        return {"index": "0", "action": "include", "reason": "no_suggestions"}
    if not q:
        return {"index": str(suggestions[0]["index"]), "action": "include", "reason": "empty_query"}

    def score_one(item: Dict[str, str]) -> int:
        name = str(item.get("name") or "").lower()
        type_name = str(item.get("type") or "").lower()
        score = 0
        if name == q:
            score += 120
        if name.startswith(q):
            score += 90
        if q in name:
            score += 80
        if len(q) <= 3 and type_name == "country":
            score += 40
        if len(q) <= 3 and name.startswith(q[:2]):
            score += 25
        if q.replace(" ", "") in name.replace(" ", ""):
            score += 20
        return score

    best = max(suggestions, key=score_one)
    return {
        "index": str(best["index"]),
        "action": "include",
        "reason": "heuristic",
    }


def _normalize_location_text(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def _snapshot_selected_locations(driver: WebDriver) -> List[str]:
    try:
        data = driver.execute_script(
            """
            const clean = (value) => (value || '').replace(/\\s+/g, ' ').trim();
            const root = document.querySelector('custom-location-input');
            if (!root) return [];
            const out = [];
            const push = (text) => {
              const val = clean(text);
              if (val && !out.includes(val)) out.push(val);
            };
            const chips = [...root.querySelectorAll('.selected-location-chip, .location-chip, material-chip, material-chip-row material-chip, .chip')];
            for (const chip of chips) {
              push(chip.innerText || chip.textContent || '');
            }
            const srList = root.querySelector('.selected-locations');
            if (srList) push(srList.innerText || srList.textContent || '');
            const summary = root.closest('.panel')?.querySelector('.summary-host .summary, .summary');
            if (summary) push(summary.innerText || summary.textContent || '');
            return out;
            """
        )
    except Exception:
        data = None

    if not isinstance(data, list):
        return []

    snapshot: List[str] = []
    for raw in data:
        if not isinstance(raw, str):
            continue
        parts = [part.strip() for part in re.split(r"[\n,;]", raw) if part and part.strip()]
        if not parts:
            continue
        joined = " ".join(parts)
        if joined and joined not in snapshot:
            snapshot.append(joined)
    return snapshot


def _decide_location_choice(query: str, suggestions: List[Dict[str, str]]) -> Dict[str, str]:
    if not suggestions:
        return {"index": "0", "action": "include", "reason": "no_suggestions"}
    choice = _heuristic_location_choice(query, suggestions)
    choice.setdefault("source", "heuristic")
    exclude = _detect_exclude_action(query)
    if exclude:
        choice["action"] = "exclude"

    normalized_query = _normalize_location_text(query)
    exact_match = next(
        (
            item
            for item in suggestions
            if _normalize_location_text(item.get("name")) == normalized_query
        ),
        None,
    )
    if exact_match:
        choice.update(
            {
                "index": str(exact_match.get("index", choice["index"])),
                "reason": "exact_match",
            }
        )
        choice["source"] = "heuristic"
        return choice

    use_llm = bool(GeminiClient) and len(suggestions) > 1 and len(normalized_query) > 3

    if GeminiClient is None:
        choice["source"] = "heuristic"
        return choice
    if not use_llm:
        choice["source"] = "heuristic"
        return choice

    payload = {
        "task": "Return ONLY JSON describing which location suggestion to pick for Google Ads.",
        "query": (query or "").strip(),
        "suggestions": [
            {
                "index": int(item.get("index", 0)),
                "name": str(item.get("name") or ""),
                "type": str(item.get("type") or ""),
                "reach": str(item.get("reach") or ""),
            }
            for item in suggestions
        ],
        "rules": [
            "Prefer exact or synonymous matches for country/state/city names.",
            "Interpret short codes (RU, UK, US, etc.) as country abbreviations when appropriate.",
            "Default action is include unless the query explicitly requests exclusion.",
            "Answer format must be JSON with fields index (int), action ('include'|'exclude'), reason (string).",
        ],
        "default_action": "exclude" if exclude else "include",
        "output_schema": {"index": "int", "action": "include|exclude", "reason": "string"},
    }
    try:
        client = GeminiClient(model=LLM_DEFAULT_MODEL, temperature=0.1, retries=1, fallback_model=None)  # type: ignore
        raw = client.generate_json(json.dumps(payload, ensure_ascii=False))
        if isinstance(raw, dict):
            idx = raw.get("index", choice["index"])
            action = str(raw.get("action") or choice["action"]).lower()
            reason = str(raw.get("reason") or "").strip() or "llm"
            try:
                idx_int = int(idx)
            except Exception:
                idx_int = int(choice["index"])
            indices = [int(item["index"]) for item in suggestions]
            if idx_int not in indices:
                idx_int = int(choice["index"])
            if action not in {"include", "exclude"}:
                action = choice["action"]
            return {
                "index": str(idx_int),
                "action": "exclude" if action == "exclude" else "include",
                "reason": reason,
                "source": "llm",
            }
    except Exception as e:
        logger.warning("LLM location decision failed (%s) — fallback to heuristics", e)
    choice["source"] = "heuristic"
    return choice


def _apply_location_choice(driver: WebDriver, index: int, action: str) -> bool:
    try:
        return bool(
            driver.execute_script(
                """
                const idx = Number(arguments[0]);
                const action = String(arguments[1] || 'include').toLowerCase();
                const pop = document.querySelector('.location-suggest-popup, material-popup[arialabel="Location suggestions"]');
                if (!pop) return false;
                const attr = action === 'exclude' ? 'data-demandgen-location-exclude' : 'data-demandgen-location-include';
                let btn = pop.querySelector(`[${attr}="${idx}"]`);
                if (!btn) {
                    const container = pop.querySelector(`[data-demandgen-location-index="${idx}"]`);
                    if (!container) return false;
                    btn = action === 'exclude'
                        ? (container.querySelector('.entry-button .material-button.exclude, .entry-button button.exclude') || container.querySelector('.entry-button [aria-label*="Exclude"]'))
                        : (container.querySelector('.entry-button .material-button.add, .entry-button button.add') || container.querySelector('.entry-button [aria-label*="Include"]'));
                }
                if (!btn) return false;
                try { btn.click(); return true; } catch (e) {}
                try { btn.dispatchEvent(new MouseEvent('click', {bubbles: true, cancelable: true})); return true; } catch (e2) {}
                return false;
                """,
                index,
                action,
            )
        )
    except Exception as e:
        logger.debug("Apply location choice failed: %s", e)
        return False


def _reset_location_input(driver: WebDriver, inp: WebElement) -> None:
    try:
        inp.clear()
    except Exception:
        pass
    try:
        driver.execute_script(
            """
            arguments[0].value = '';
            arguments[0].dispatchEvent(new Event('input', {bubbles: true}));
            """,
            inp,
        )
    except Exception:
        pass


def _add_single_location(driver: WebDriver, query: str, emit: Optional[Callable[[str], None]]) -> Dict[str, object]:
    result: Dict[str, object] = {
        "query": query,
        "success": False,
        "action": "include",
        "reason": "",
        "source": "heuristic",
        "chosen": "",
        "type": "",
        "reach": "",
    }
    q = (query or "").strip()
    if not q:
        result["reason"] = "empty_query"
        return result

    before_snapshot_values = _snapshot_selected_locations(driver)
    before_snapshot_lookup = {_normalize_location_text(v): v for v in before_snapshot_values if _normalize_location_text(v)}

    def _finalize(res: Dict[str, object]) -> Dict[str, object]:
        after_values = _snapshot_selected_locations(driver)
        after_lookup = {_normalize_location_text(v): v for v in after_values if _normalize_location_text(v)}
        res["snapshot_before"] = before_snapshot_values
        res["snapshot_after"] = after_values
        if not res.get("success"):
            new_norms = [norm for norm in after_lookup if norm and norm not in before_snapshot_lookup]
            if new_norms:
                guess_label = after_lookup[new_norms[0]]
                res.update(
                    {
                        "success": True,
                        "reason": res.get("reason") or "Локация отображается в списке выбранных — считаю добавленной.",
                        "source": res.get("source") or "ui_snapshot",
                        "chosen": res.get("chosen") or guess_label,
                    }
                )
        return res

    _maybe_confirm_quick(driver, emit=emit, force=True)
    inp = _find_location_input(driver)
    if not inp or not _is_interactable(driver, inp):
        result["reason"] = "input_not_found"
        return _finalize(result)

    try:
        driver.execute_script("arguments[0].focus();", inp)
    except Exception:
        pass
    _reset_location_input(driver, inp)
    _maybe_confirm_quick(driver, emit=emit)
    try:
        inp.send_keys(q)
    except Exception as e:
        result["reason"] = f"send_keys_failed:{e}"
        return _finalize(result)

    suggestions = _wait_location_suggestions(driver, timeout=2.2)
    if not suggestions:
        try:
            inp.send_keys(Keys.ENTER)
        except Exception:
            pass
        result["reason"] = "no_suggestions"
        return _finalize(result)

    decision = _decide_location_choice(q, suggestions)
    action = decision.get("action", "include")
    idx = int(decision.get("index", suggestions[0]["index"]))
    reason = decision.get("reason", "")
    source = decision.get("source", "heuristic")
    chosen = next((item for item in suggestions if int(item["index"]) == idx), suggestions[0])

    applied = _apply_location_choice(driver, idx, action)
    if not applied:
        logger.debug("Primary apply failed — fallback arrow navigation")
        try:
            inp.send_keys(Keys.ARROW_DOWN)
            time.sleep(0.05)
            inp.send_keys(Keys.ENTER)
            applied = True
        except Exception:
            applied = False

    if not applied:
        result["reason"] = "apply_failed"
        return _finalize(result)

    _wait_popup_state(driver, visible=False, timeout=1.6)
    result.update(
        {
            "success": True,
            "action": action,
            "reason": reason,
            "source": source,
            "chosen": chosen.get("name", ""),
            "type": chosen.get("type", ""),
            "reach": chosen.get("reach", ""),
        }
    )
    return _finalize(result)


def _add_locations(
    driver: WebDriver,
    locations: Iterable[str],
    emit: Optional[Callable[[str], None]] = None,
) -> Dict[str, object]:
    queries = [loc for loc in locations if loc and str(loc).strip()]
    if not queries:
        return {"added": [], "details": []}

    _emit(emit, "Перехожу к настройке локаций")
    _ensure_panel_open(driver, PANEL_LOCATION_LANGUAGE_SYNS)
    _ensure_panel_open(driver, PANEL_LOCATIONS)
    _select_enter_another_location(driver)

    added: List[str] = []
    details: List[Dict[str, object]] = []
    for raw in queries:
        _maybe_confirm_quick(driver, emit=emit)
        _emit(emit, f"Добавляю локацию: {raw}")
        res = _add_single_location(driver, raw, emit)
        details.append(res)
        if res.get("success"):
            chosen = str(res.get("chosen") or "").strip() or raw
            added.append(chosen)
            logger.info(
                "Location '%s' -> '%s' (%s), action=%s, source=%s",
                raw,
                chosen,
                res.get("type", ""),
                res.get("action", ""),
                res.get("source", ""),
            )
        else:
            logger.warning("Location '%s' failed: %s", raw, res.get("reason"))
        time.sleep(0.03)

    return {"added": added, "details": details}


def run_step5(
    driver: WebDriver,
    *,
    locations: Optional[Iterable[str]] = None,
    languages: Optional[Iterable[str]] = None,
    eu_political_ads_no: bool = True,
    timeout_total: float = 90.0,
    emit: Optional[Callable[[str], None]] = None,
) -> Dict[str, object]:
    t0 = time.time()
    stage_ts = t0
    stage_log: List[Tuple[str, float]] = []

    def _mark_stage(label: str) -> None:
        nonlocal stage_ts
        now = time.time()
        elapsed_stage = (now - stage_ts) * 1000.0
        elapsed_total = (now - t0) * 1000.0
        logger.debug("DemandGen Step5: %s took %.1f ms (total %.1f ms)", label, elapsed_stage, elapsed_total)
        stage_log.append((label, elapsed_stage))
        stage_ts = now

    _maybe_confirm_quick(driver, emit=emit, force=True)
    _mark_stage("confirm_probe_initial")

    eu_status = "skip"
    if eu_political_ads_no:
        _emit(emit, "Отвечаю, что кампания не касается политической рекламы ЕС")
        eu_status = "no" if _choose_eu_political_ads_no(driver) else "skip"
    _mark_stage("eu_political_ads")
    _maybe_confirm_quick(driver, emit=emit)

    _emit(emit, "Проверяю переключатель для таргетинга уровня кампании")
    _enable_campaign_level_targeting(driver, emit=emit)
    _mark_stage("campaign_toggle")
    _maybe_confirm_quick(driver, emit=emit)

    loc_info = {"added": [], "details": []}
    if locations:
        loc_info = _add_locations(driver, locations, emit=emit)
    else:
        _emit(emit, "Локации не переданы — оставляю настройку по умолчанию")
    _mark_stage("locations")

    _maybe_confirm_quick(driver, emit=emit)

    if languages is not None:
        _emit(emit, "Настраиваю языки кампании")
        lang_selected = _add_languages(driver, list(languages), clear_before=True, emit=emit)  # type: ignore
    else:
        _emit(emit, "Языки не трогаю")
        lang_selected = _add_languages(driver, [], clear_before=False, emit=emit)  # type: ignore
    _mark_stage("languages")

    _maybe_confirm_quick(driver, emit=emit)

    new_url = driver.current_url or ""
    elapsed = int((time.time() - t0) * 1000)
    if stage_log:
        human = ", ".join(f"{name}={dur:.0f}ms" for name, dur in stage_log)
        logger.info("DemandGen Step5 breakdown: %s", human)
    logger.info(
        "step5 DemandGen OK (%d ms). URL=%s | locations=%s | languages=%s | eu=%s",
        elapsed,
        new_url,
        loc_info.get("added"),
        lang_selected,
        eu_status,
    )
    _emit(emit, "Шаг завершён — настройки сохранены")

    return {
        "locations_added": loc_info.get("added", []),
        "location_decisions": loc_info.get("details", []),
        "languages_selected": lang_selected,
        "eu_political_ads": eu_status,
        "new_url": new_url,
        "duration_ms": elapsed,
        "timing_breakdown": [{"stage": name, "duration_ms": dur} for name, dur in stage_log],
    }
