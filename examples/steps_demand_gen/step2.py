# -*- coding: utf-8 -*-
"""
Шаг 2 для Demand Gen:
1) Выбираем маркетинговую цель (Sales / Leads / Traffic / Awareness / …) — решение принимает LLM.
2) Жмём Continue.
3) Выбираем тип кампании Demand Gen (OWNED_AND_OPERATED) и жмём Continue.
4) Ждём перехода на экран настройки (construction-layout либо форма имени кампании).

Возвращаем словарь с выбранной целью, причиной и типом кампании.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Callable, Dict, Optional, Tuple

from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver

from examples.steps.step2 import (  # type: ignore
    _click_continue_button,
    _dismiss_hover_popups,
    _dismiss_soft_dialogs,
    _emit,
    _find_continue_button_any_language,
    _is_construction_layout_visible,
    _is_interactable,
    _is_significant_url_change,
    _mouse_jiggle,
    _select_tab_by_datavalue_or_text,
    _short_type_label_by_code,
)

logger = logging.getLogger("ads_ai.gads.step2.demand_gen")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

try:
    from ads_ai.llm.gemini import GeminiClient  # type: ignore
except Exception as e:  # pragma: no cover
    GeminiClient = None  # type: ignore
    logger.warning("GeminiClient not available for DemandGen step2: %s", e)


DEFAULT_DEMAND_GEN_CODE = "OWNED_AND_OPERATED"

OBJECTIVE_OPTIONS: Dict[str, Tuple[str, str]] = {
    "sales": ("SALES", "Sales"),
    "leads": ("LEADS", "Leads"),
    "website_traffic": ("WEBSITE_TRAFFIC", "Website traffic"),
    "app_promotion": ("APP_DOWNLOADS", "App promotion"),
    "awareness": ("AWARENESS_AND_CONSIDERATION", "Awareness and consideration"),
    "local_store": ("LOCAL_STORE_VISITS", "Local store visits and promotions"),
    "no_guidance": ("No objective", "Create a campaign without guidance"),
}

OBJECTIVE_SYNONYMS: Dict[str, list[str]] = {
    "SALES": ["sales", "продажи", "продаж", "sell", "sale"],
    "LEADS": ["leads", "лиды", "заявки", "lead"],
    "WEBSITE_TRAFFIC": ["traffic", "трафик", "website traffic", "web traffic", "посетители"],
    "APP_DOWNLOADS": ["app", "приложение", "установки", "installs", "app promotion"],
    "AWARENESS_AND_CONSIDERATION": ["awareness", "brand", "узнаваемость", "consideration"],
    "LOCAL_STORE_VISITS": ["local", "store", "магазин", "визиты", "продажи офлайн"],
    "No objective": ["without guidance", "no objective", "без подсказок", "без цели"],
}

DEFAULT_OBJECTIVE_KEY = "website_traffic"
ObjectiveDecision = Tuple[str, str, str]  # data_value, label, reason


def _probe_saved_draft_dialog(
    driver: WebDriver,
    *,
    attempt_click: bool = True,
) -> Dict[str, bool]:
    """
    Возвращает состояние диалога сохранённого черновика.
    present   — диалог найден и видим.
    actionable — нашли целевую кнопку (она может быть временно disabled).
    clicked   — удалось нажать кнопку (только при attempt_click=True).
    """
    try:
        state = driver.execute_script(
            """
            const doClick = Boolean(arguments[0]);
            const result = {present: false, actionable: false, clicked: false};
            const lookFor = [
                'saved draft',
                'finish a saved draft',
                'черновик',
                'сохраненный черновик',
                'черновика',
                'finish draft',
                'продолжить черновик',
            ];
            const affirmative = [
                'start new',
                'start a new',
                'create new',
                'new campaign',
                'начать новую',
                'создать новую',
                'создайте новую',
            ];
            const dialogs = [...document.querySelectorAll(
                'material-dialog, material-dialog.simple-dialog, .simple-dialog, [role=\"dialog\"]'
            )];
            for (const dlg of dialogs) {
                const style = window.getComputedStyle(dlg);
                if (style && (style.display === 'none' || style.visibility === 'hidden')) continue;
                const text = (dlg.innerText || dlg.textContent || '').toLowerCase();
                if (!text) continue;
                if (!lookFor.some(p => text.includes(p))) continue;
                result.present = true;
                const buttons = [...dlg.querySelectorAll('material-button, button, [role=\"button\"]')];
                for (const btn of buttons) {
                    const raw = (btn.innerText || btn.textContent || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                    if (!raw) continue;
                    if (!affirmative.some(p => raw.includes(p))) continue;
                    result.actionable = true;
                    const disabled = (btn.getAttribute('aria-disabled') || '').toLowerCase() === 'true'
                        || btn.hasAttribute('disabled');
                    if (disabled) continue;
                    if (!doClick) return result;
                    try {
                        btn.click();
                        result.clicked = true;
                        return result;
                    } catch (err) {}
                    try {
                        btn.dispatchEvent(new MouseEvent('click', {bubbles:true,cancelable:true,view:window}));
                        result.clicked = true;
                        return result;
                    } catch (err2) {}
                }
                return result;
            }
            return result;
            """,
            attempt_click,
        )
    except Exception:
        state = {}
    if not isinstance(state, dict):
        state = {}
    return {
        "present": bool(state.get("present")),
        "actionable": bool(state.get("actionable")),
        "clicked": bool(state.get("clicked")),
    }


def _wait_for_campaign_type_panel(driver: WebDriver, timeout: float = 20.0) -> bool:
    poll = min(0.18, max(0.07, timeout / 110 if timeout else 0.12))
    end = time.time() + timeout
    while time.time() < end:
        _handle_saved_draft_dialog(driver, timeout=0.1)
        try:
            nodes = driver.find_elements(By.CSS_SELECTOR, '[role="tab"][data-value]')
            for node in nodes:
                val = (node.get_attribute("data-value") or "").strip().upper()
                if val == DEFAULT_DEMAND_GEN_CODE:
                    return True
        except Exception:
            pass
        time.sleep(poll)
    return False


def _wait_continue_button_ready(driver: WebDriver, timeout: float = 12.0) -> bool:
    poll = min(0.16, max(0.06, timeout / 140 if timeout else 0.1))
    end = time.time() + timeout
    while time.time() < end:
        _handle_saved_draft_dialog(driver, timeout=0.1)
        try:
            btn = _find_continue_button_any_language(driver)
        except Exception:
            btn = None
        if btn and _is_interactable(driver, btn):
            aria = (btn.get_attribute("aria-disabled") or "").strip().lower()
            disabled_attr = btn.get_attribute("disabled")
            classes = (btn.get_attribute("class") or "").lower()
            blocked = aria == "true" or (disabled_attr is not None)
            if not blocked and "disabled" not in classes:
                return True
        time.sleep(poll)
    return False


def _is_campaign_name_input_visible(driver: WebDriver) -> bool:
    try:
        el = driver.find_element(By.CSS_SELECTOR, 'input[aria-label="Campaign name"], material-input input[type="text"][aria-required="true"]')
        return el.is_displayed()
    except Exception:
        return False


def _wait_for_construction_layout(driver: WebDriver, old_url: str, timeout: float = 35.0) -> bool:
    poll = min(0.18, max(0.07, timeout / 160 if timeout else 0.12))
    end = time.time() + timeout
    if _is_construction_layout_visible(driver) or _is_campaign_name_input_visible(driver):
        return True
    while time.time() < end:
        _handle_saved_draft_dialog(driver, timeout=0.1)
        cur = driver.current_url or ""
        changed, _ = _is_significant_url_change(old_url, cur)
        if changed and (_is_construction_layout_visible(driver) or _is_campaign_name_input_visible(driver)):
            return True
        if _is_construction_layout_visible(driver) or _is_campaign_name_input_visible(driver):
            return True
        time.sleep(poll)
    return False


def _decide_campaign_objective_via_llm(
    *,
    business_name: Optional[str],
    usp: Optional[str],
    site_url: Optional[str],
    budget_per_day: Optional[str],
    campaign_type_label: str,
) -> ObjectiveDecision:
    default_code, default_label = OBJECTIVE_OPTIONS[DEFAULT_OBJECTIVE_KEY]
    default_reason = "Выбираю цель «Website traffic» по умолчанию: нужно привести людей на сайт."
    if GeminiClient is None:
        return default_code, default_label, default_reason

    prompt = {
        "task": (
            "Return ONLY compact JSON with fields 'goal' and 'reason'. "
            "'goal' must be one of: sales, leads, website_traffic, app_promotion, awareness, local_store, no_guidance."
        ),
        "constraints": [
            "Проанализируй описание бизнеса и выбери наиболее уместную цель.",
            "Если бизнесу критичны заявки/продажи — выбирай sales или leads.",
            "Если важен трафик/awareness — выбирай соответствующие цели.",
            "reason — короткое объяснение на русском (<= 140 символов).",
        ],
        "inputs": {
            "business_name": (business_name or "").strip(),
            "usp": (usp or "").strip(),
            "site_url": (site_url or "").strip(),
            "budget_per_day": (budget_per_day or "").strip(),
            "campaign_type": campaign_type_label,
        },
        "output_schema": {
            "goal": "sales|leads|website_traffic|app_promotion|awareness|local_store|no_guidance",
            "reason": "string",
        },
        "format": "json_only_no_explanations",
    }

    model = os.getenv("GEMINI_MODEL", "models/gemini-2.0-flash")
    try:
        client = GeminiClient(model=model, temperature=0.1, retries=1, fallback_model=None)
        raw = client.generate_json(json.dumps(prompt, ensure_ascii=False))
        goal_key = DEFAULT_OBJECTIVE_KEY
        reason = ""
        if isinstance(raw, dict):
            goal_key = str(raw.get("goal", "")).strip().lower() or DEFAULT_OBJECTIVE_KEY
            reason = str(raw.get("reason", "")).strip()
        elif isinstance(raw, str):
            goal_key = raw.strip().lower() or DEFAULT_OBJECTIVE_KEY
        if goal_key not in OBJECTIVE_OPTIONS:
            logger.warning("LLM returned unexpected objective key: %r", raw)
            goal_key = DEFAULT_OBJECTIVE_KEY
        code, label = OBJECTIVE_OPTIONS[goal_key]
        if not reason:
            reason = f"LLM: выбрал цель «{label}» исходя из описания бизнеса."
        return code, label, reason
    except Exception as e:
        logger.warning("LLM objective decision failed: %s — fallback to default.", e)
        fail_reason = f"LLM недоступен ({e}), выбираю «{default_label}»."
        return default_code, default_label, fail_reason


def _select_campaign_objective(driver: WebDriver, objective_code: str, timeout: float = 12.0) -> bool:
    synonyms = OBJECTIVE_SYNONYMS.get(objective_code, [])
    return _select_tab_by_datavalue_or_text(
        driver,
        data_value=objective_code,
        text_synonyms=[s.lower() for s in synonyms],
        scope_css='div.panel.panel--construction-selection-cards',
        timeout=timeout,
    )


def _handle_saved_draft_dialog(
    driver: WebDriver,
    *,
    emit: Optional[Callable[[str], None]] = None,
    timeout: float = 6.0,
) -> bool:
    """
    Обрабатывает всплывающий диалог «Create a new campaign or finish a saved draft?»
    — выбираем «Start new», если диалог появился.
    """
    timeout = max(0.0, float(timeout))
    if timeout <= 0:
        return False

    sleep_delay = min(0.2, max(0.05, timeout / 20 if timeout else 0.1))
    hard_deadline = time.time() + timeout
    quiet_deadline = time.time() + min(timeout, max(0.7, sleep_delay * 4))
    saw_dialog = False

    while time.time() < hard_deadline:
        state = _probe_saved_draft_dialog(driver, attempt_click=True)
        if state["clicked"]:
            logger.info("DemandGen Step2: detected draft dialog and pressed 'Start new'.")
            _emit(emit, "Выбираю «Start new» вместо сохранённого черновика")
            time.sleep(0.35)
            return True
        if state["present"] or state["actionable"]:
            saw_dialog = True
            quiet_deadline = min(hard_deadline, time.time() + max(0.6, sleep_delay * 3))
        elif not saw_dialog and time.time() >= quiet_deadline:
            break
        time.sleep(sleep_delay)
    return False


def _handle_saved_draft_dialog_async(
    driver: WebDriver,
    *,
    emit: Optional[Callable[[str], None]] = None,
    timeout: float = 10.0,
) -> bool:
    """
    Быстрый неблокирующий пинг: если диалог виден — обработать коротко;
    если нет — мгновенно выйти без ожиданий.
    """
    try:
        state = _probe_saved_draft_dialog(driver, attempt_click=False)
    except Exception:
        return False
    if not state:
        return False
    if not (state.get("present") or state.get("actionable")):
        return False

    base_timeout = max(0.0, float(timeout))
    sub_timeout = base_timeout * 0.2 if base_timeout else 0.0
    sub_timeout = max(0.25, min(0.5, sub_timeout or 0.3))
    try:
        return _handle_saved_draft_dialog(driver, emit=emit, timeout=sub_timeout)
    except Exception:
        return False


def run_step2(
    driver: WebDriver,
    *,
    choose_type: str = DEFAULT_DEMAND_GEN_CODE,
    business_name: Optional[str] = None,
    usp: Optional[str] = None,
    site_url: Optional[str] = None,
    budget_per_day: Optional[str] = None,
    budget_clean: Optional[str] = None,
    timeout_total: float = 60.0,
    emit: Optional[Callable[[str], None]] = None,
) -> Dict[str, str]:
    """
    Demand Gen Step2: цель кампании + тип кампании.
    """
    _dismiss_soft_dialogs(driver)
    _handle_saved_draft_dialog(driver, emit=emit, timeout=5.0)

    # --- 1) Определяем и выбираем маркетинговую цель ---
    _emit(emit, "Подбираю цель кампании…")
    objective_code, objective_label, objective_reason = _decide_campaign_objective_via_llm(
        business_name=business_name,
        usp=usp,
        site_url=site_url,
        budget_per_day=budget_per_day or budget_clean,
        campaign_type_label="Demand Gen",
    )
    logger.info("DemandGen Step2: objective=%s (%s)", objective_code, objective_label)

    ok = _select_campaign_objective(driver, objective_code, timeout=14.0)
    if not ok:
        # fallback — попробуем без data_value, только по тексту
        synonyms = OBJECTIVE_SYNONYMS.get(objective_code, [])
        ok = _select_tab_by_datavalue_or_text(
            driver,
            data_value=None,
            text_synonyms=[s.lower() for s in synonyms],
            scope_css='div.panel.panel--construction-selection-cards',
            timeout=6.0,
        )
    if not ok:
        _emit(emit, f"Не удалось выбрать цель «{objective_label}» — стоп")
        raise RuntimeError(f"Не удалось выбрать маркетинговую цель '{objective_code}'.")

    _emit(emit, f"Цель кампании: «{objective_label}». {objective_reason}")
    _handle_saved_draft_dialog_async(driver, emit=emit, timeout=0.0)
    if not _click_continue_button(driver, skip_preflight=True):
        _dismiss_hover_popups(driver)
        _mouse_jiggle(driver, amplitude=12, repeats=1)
        _handle_saved_draft_dialog_async(driver, emit=emit, timeout=0.0)
        if not _click_continue_button(driver):
            _emit(emit, "Кнопка Continue после выбора цели недоступна — стоп")
            raise RuntimeError("После выбора цели кнопка Continue не нажалась.")

    if not _wait_for_campaign_type_panel(driver, timeout=timeout_total / 2):
        _emit(emit, "Не дождался экрана выбора типа кампании — стоп")
        raise RuntimeError("Экран выбора типа кампании не появился.")

    # --- 2) Выбор типа кампании ---
    target_code = (choose_type or DEFAULT_DEMAND_GEN_CODE).strip().upper() or DEFAULT_DEMAND_GEN_CODE
    human_label = _short_type_label_by_code(target_code)
    _emit(emit, f"Выбираю тип кампании: {human_label}")
    logger.info("DemandGen Step2: выбираю тип кампании %s (%s)", human_label, target_code)

    synonyms = [
        "demand gen",
        "demand generation",
        "создание спроса",
        target_code.lower(),
        human_label.lower(),
    ]
    ok = _select_tab_by_datavalue_or_text(
        driver,
        data_value=target_code,
        text_synonyms=synonyms,
        scope_css="div.cards",
        timeout=18.0,
    )
    if not ok:
        ok = _select_tab_by_datavalue_or_text(driver, target_code, synonyms, None, 8.0)
    if not ok:
        _emit(emit, f"Тип {human_label} не нашёлся — стоп")
        raise RuntimeError(f"Не удалось выбрать тип кампании '{target_code}'.")
    _emit(emit, "Тип выбран — продолжаю")

    _handle_saved_draft_dialog_async(driver, emit=emit, timeout=0.0)

    old_url = driver.current_url or ""
    _emit(emit, "Жму «Продолжить»")
    logger.info("DemandGen Step2: нажимаю Continue после выбора типа…")
    if not _click_continue_button(driver, skip_preflight=True):
        _wait_continue_button_ready(driver, timeout=1.2)
        if not _click_continue_button(driver, skip_preflight=True):
            _dismiss_hover_popups(driver)
            _mouse_jiggle(driver, amplitude=12, repeats=1)
            _handle_saved_draft_dialog_async(driver, emit=emit, timeout=0.0)
            if not _click_continue_button(driver):
                _emit(emit, "Кнопка Continue не нажалась — стоп")
                raise RuntimeError("Кнопка Continue не нажалась после выбора типа кампании.")

    if not _wait_for_construction_layout(driver, old_url, timeout=timeout_total):
        _emit(emit, "Похоже, экран не переключился — повторяю «Продолжить»")
        logger.info("DemandGen Step2: повторный Continue (экран не сменился).")
        _wait_continue_button_ready(driver, timeout=1.5)
        if not _click_continue_button(driver, skip_preflight=True):
            _dismiss_hover_popups(driver)
            _mouse_jiggle(driver, amplitude=12, repeats=1)
            _handle_saved_draft_dialog_async(driver, emit=emit, timeout=0.0)
            if not _click_continue_button(driver):
                _emit(emit, "Повторное «Продолжить» не сработало — стоп")
                raise RuntimeError("Повторное Continue не нажалось.")
        if not _wait_for_construction_layout(driver, old_url, timeout=max(20.0, timeout_total / 2)):
            _emit(emit, "Не дождался экрана настройки кампании — стоп")
            raise RuntimeError("После Continue не появился экран настройки кампании.")

    _emit(emit, "Готово: перешёл к настройке кампании")
    logger.info(
        "DemandGen Step2 завершён. Objective=%s (%s). Тип=%s",
        objective_code,
        objective_label,
        target_code,
    )
    return {
        "campaign_type": target_code,
        "campaign_objective_code": objective_code,
        "campaign_objective_label": objective_label,
        "campaign_objective_reason": objective_reason,
    }
