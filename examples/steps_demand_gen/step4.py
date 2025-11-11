# -*- coding: utf-8 -*-
"""
Шаг 4 (Demand Gen):
- использует LLM для выбора дневного бюджета внутри заданного диапазона и, при необходимости, целевого CPA;
- конвертирует сумму с учётом валюты аккаунта и применяет настройки на экране;
- после чего жмёт Continue, не затрагивая прочие блоки.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Callable, Dict, List, Optional, Tuple

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webdriver import WebDriver, WebElement

# Reuse отдельных утилит из базового шага 4
from examples.steps.step4 import _maybe_handle_confirm_its_you, _click_next, _wait_url_change_or_button_stale, _emit, _is_interactable  # type: ignore

try:
    from ads_ai.llm.gemini import GeminiClient  # type: ignore
except Exception as e:  # pragma: no cover
    GeminiClient = None  # type: ignore

logger = logging.getLogger("ads_ai.gads.step4.demand_gen")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

def _normalize_number(value: Optional[str | float | int]) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except Exception:
            return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(" ", "").replace(",", ".")
    try:
        return float(text)
    except Exception:
        return None


def _open_budget_panel(driver: WebDriver, timeout: float = 4.0) -> None:
    selectors = (
        ("css", 'div[aria-label="Budget and dates"][role="button"]'),
        ("xpath", "//div[@role='button' and contains(@aria-label,'Budget')]"),
    )
    deadline = time.time() + max(0.5, timeout)
    last_click = 0.0

    while time.time() < deadline:
        for mode, sel in selectors:
            try:
                header = (
                    driver.find_element(By.CSS_SELECTOR, sel)
                    if mode == "css"
                    else driver.find_element(By.XPATH, sel)
                )
            except Exception:
                continue

            try:
                expanded = (header.get_attribute("aria-expanded") or "").lower() == "true"
            except Exception:
                expanded = False
            if expanded:
                return

            now = time.time()
            if now - last_click < 0.15:
                continue

            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center',inline:'nearest'});", header)
            except Exception:
                pass
            if _is_interactable(driver, header):
                try:
                    header.click()
                except Exception:
                    try:
                        driver.execute_script("arguments[0].click();", header)
                    except Exception:
                        continue
                last_click = time.time()
            else:
                try:
                    driver.execute_script("arguments[0].click();", header)
                    last_click = time.time()
                except Exception:
                    continue
        time.sleep(0.06)


def _clear_and_type(driver: WebDriver, element: WebElement, value: str) -> None:
    try:
        element.clear()
    except Exception:
        pass
    try:
        element.send_keys(Keys.CONTROL, "a")
        element.send_keys(Keys.DELETE)
    except Exception:
        try:
            element.send_keys(Keys.COMMAND, "a")
            element.send_keys(Keys.DELETE)
        except Exception:
            driver.execute_script("arguments[0].value='';", element)
    driver.execute_script(
        """
        arguments[0].value='';
        arguments[0].dispatchEvent(new Event('input',{bubbles:true}));
        """,
        element,
    )
    element.send_keys(value)
    driver.execute_script(
        "arguments[0].dispatchEvent(new Event('change',{bubbles:true}));",
        element,
    )
    time.sleep(0.08)


def _find_budget_input(driver: WebDriver) -> Optional[WebElement]:
    selectors = [
        'input[aria-label*="Budget amount"]',
        'input[aria-label*="Budget"]',
        'money-input input',
    ]
    for sel in selectors:
        try:
            for inp in driver.find_elements(By.CSS_SELECTOR, sel):
                if _is_interactable(driver, inp):
                    return inp
        except Exception:
            continue
    return None


def _detect_currency_symbol_and_code(driver: WebDriver, input_el: Optional[WebElement]) -> Tuple[str, str]:
    symbol = ""
    code = ""
    target = input_el
    if target is not None:
        try:
            root = target.find_element(By.XPATH, "./ancestor::material-input[1]")
        except Exception:
            root = None
        if root is not None:
            try:
                leading = root.find_element(By.CSS_SELECTOR, ".leading-text")
                raw = leading.text.strip()
                symbol = raw
            except Exception:
                pass
    if not symbol:
        try:
            leading_generic = driver.find_element(By.CSS_SELECTOR, ".leading-text")
            symbol = leading_generic.text.strip()
        except Exception:
            symbol = ""

    symbol_map = {
        "₽": "RUB",
        "руб.": "RUB",
        "руб": "RUB",
        "RUB": "RUB",
        "€": "EUR",
        "EUR": "EUR",
        "$": "USD",
        "US$": "USD",
        "USD": "USD",
        "£": "GBP",
        "GBP": "GBP",
        "¥": "JPY",
        "JPY": "JPY",
        "₴": "UAH",
        "UAH": "UAH",
        "₸": "KZT",
        "KZT": "KZT",
        "₹": "INR",
        "INR": "INR",
        "₱": "PHP",
        "PHP": "PHP",
        "CHF": "CHF",
        "C$": "CAD",
        "CAD": "CAD",
        "A$": "AUD",
        "AUD": "AUD",
        "MX$": "MXN",
        "MXN": "MXN",
        "kr": "SEK",
        "SEK": "SEK",
    }

    raw_upper = symbol.upper()
    if symbol in symbol_map:
        code = symbol_map[symbol]
    elif raw_upper in symbol_map:
        code = symbol_map[raw_upper]
        symbol = raw_upper if len(raw_upper) != 3 else symbol
    elif len(raw_upper) == 3 and raw_upper.isalpha():
        code = raw_upper
        symbol = raw_upper
    else:
        code = "RUB"
        if not symbol:
            symbol = "₽"

    if not symbol:
        symbol = code if len(code) != 3 else ""
    if not symbol:
        symbol = "₽" if code == "RUB" else code
    return symbol, code


def _format_currency_amount(symbol: str, code: str, amount: float) -> str:
    try:
        amt = float(amount)
    except Exception:
        return str(amount)
    use_prefix = symbol and len(symbol) == 1 and symbol not in (code or "")
    if use_prefix:
        return f"{symbol}{amt:.2f}"
    if symbol and symbol.upper() == code.upper():
        return f"{amt:.2f} {code.upper()}"
    if symbol and len(symbol) > 1 and symbol.upper() != code.upper():
        return f"{amt:.2f} {symbol}"
    return f"{amt:.2f} {code.upper() if code else ''}".strip()


def _find_target_cpa_checkbox(driver: WebDriver) -> Optional[WebElement]:
    selectors = [
        "//material-checkbox[contains(., 'target cost per action')]",
        "//material-checkbox[contains(., 'Target CPA')]",
    ]
    for sel in selectors:
        try:
            el = driver.find_element(By.XPATH, sel)
            if _is_interactable(driver, el):
                return el
        except Exception:
            continue
    return None


def _find_target_cpa_input(driver: WebDriver, timeout: float = 4.0) -> Optional[WebElement]:
    selectors = [
        'input[aria-label*="Target cost"]',
        'input[aria-label*="Target CPA"]',
        'input[aria-label*="цена за конверсию"]',
        'money-input input',
    ]
    deadline = time.time() + max(0.5, timeout)
    fast_phase_until = time.time() + min(1.4, timeout * 0.4)
    while time.time() < deadline:
        for sel in selectors:
            try:
                for inp in driver.find_elements(By.CSS_SELECTOR, sel):
                    if _is_interactable(driver, inp):
                        return inp
            except Exception:
                continue
        if time.time() < fast_phase_until:
            time.sleep(0.08)
        else:
            time.sleep(0.14)
    return None


def _set_daily_budget(driver: WebDriver, amount: float, *, ensure_panel: bool = True) -> bool:
    if ensure_panel:
        _open_budget_panel(driver)
    budget_input = _find_budget_input(driver)
    if not budget_input:
        return False
    value_str = f"{amount:.2f}".rstrip("0").rstrip(".")
    _clear_and_type(driver, budget_input, value_str)
    return True


def _set_target_cpa(driver: WebDriver, amount: float) -> bool:
    checkbox = _find_target_cpa_checkbox(driver)
    if not checkbox:
        return False
    state = (checkbox.get_attribute("aria-checked") or "").lower() == "true"
    if not state:
        try:
            checkbox.click()
        except Exception:
            try:
                driver.execute_script("arguments[0].click();", checkbox)
            except Exception:
                return False
    input_el = _find_target_cpa_input(driver)
    if not input_el:
        return False
    value_str = f"{amount:.2f}".rstrip("0").rstrip(".")
    _clear_and_type(driver, input_el, value_str)
    return True


def _decide_budget_and_bidding(
    *,
    budget_min: Optional[float],
    budget_max: Optional[float],
    currency_symbol: str,
    currency_code: str,
    business_name: Optional[str],
    usp: Optional[str],
    site_url: Optional[str],
    campaign_objective_label: Optional[str],
    campaign_objective_reason: Optional[str],
    campaign_goal_label: Optional[str],
    campaign_goal_reason: Optional[str],
) -> Dict[str, object]:
    if budget_min is None or budget_max is None or budget_min <= 0 or budget_max <= 0:
        return {
            "budget_amount": None,
            "budget_amount_display": None,
            "budget_reason": "Диапазон бюджета не задан — использую ввод пользователя.",
            "set_target_cpa": False,
            "target_cpa": None,
            "target_cpa_display": "",
            "target_cpa_reason": "",
        }

    budget_min, budget_max = float(budget_min), float(budget_max)
    if budget_max < budget_min:
        budget_max = budget_min

    default_amount = round((budget_min + budget_max) / 2, 2)
    default_reason = f"Диапазон {budget_min:.0f}-{budget_max:.0f} RUB. Выбираю середину {default_amount:.0f} RUB."
    decision = {
        "budget_amount": default_amount,
        "budget_amount_display": f"{default_amount:.0f} RUB",
        "budget_reason": default_reason,
        "set_target_cpa": False,
        "target_cpa": None,
        "target_cpa_display": "",
        "target_cpa_reason": "",
    }
    if GeminiClient is None:
        return decision

    prompt = {
        "task": (
            "Верни ТОЛЬКО JSON с полями: "
            "'daily_budget' (число), "
            "'budget_reason' (короткое объяснение на русском, <=140 символов), "
            "'set_target_cpa' (true|false), "
            "'target_cpa' (число или null), "
            "'target_cpa_reason' (строка, если set_target_cpa=true, иначе пустая строка)."
        ),
        "constraints": [
            "Исходные цифры заданы в RUB, но итоговый бюджет нужно указать в валюте аккаунта.",
            "Укажи разумную сумму в пределах эквивалентного диапазона. Не завышай бюджет и не делай CPA слишком большим (<=40% дневного бюджета).",
        ],
        "inputs": {
            "rub_range": {"min_rub": budget_min, "max_rub": budget_max},
            "account_currency_symbol": currency_symbol,
            "account_currency_code": currency_code,
            "business_name": (business_name or "").strip(),
            "usp": (usp or "").strip(),
            "site_url": (site_url or "").strip(),
            "campaign_objective": (campaign_objective_label or "").strip(),
            "campaign_objective_reason": (campaign_objective_reason or "").strip(),
            "campaign_goal": (campaign_goal_label or "").strip(),
            "campaign_goal_reason": (campaign_goal_reason or "").strip(),
        },
        "output_schema": {
            "daily_budget": "number",
            "daily_budget_rub_equiv": "number",
            "budget_reason": "string",
            "set_target_cpa": "boolean",
            "target_cpa": "number|null",
            "target_cpa_reason": "string",
        },
        "format": "json_only_no_explanations",
    }

    model = os.getenv("GEMINI_MODEL", "models/gemini-2.0-flash")
    try:
        client = GeminiClient(model=model, temperature=0.15, retries=1, fallback_model=None)
        raw = client.generate_json(json.dumps(prompt, ensure_ascii=False))
        if isinstance(raw, dict):
            amount = _normalize_number(raw.get("daily_budget")) or default_amount
            reason = str(raw.get("budget_reason", "")).strip() or default_reason
            set_cpa = bool(raw.get("set_target_cpa", False))
            cpa_amount = _normalize_number(raw.get("target_cpa"))
            cpa_reason = str(raw.get("target_cpa_reason", "")).strip()
            if not set_cpa or cpa_amount is None or cpa_amount <= 0:
                set_cpa = False
                cpa_amount = None
                cpa_reason = ""
            amount_display = _format_currency_amount(currency_symbol, currency_code, amount)
            cpa_display = ""
            if set_cpa and cpa_amount is not None:
                cpa_display = _format_currency_amount(currency_symbol, currency_code, cpa_amount)
            decision.update(
                {
                    "budget_amount": amount,
                    "budget_amount_display": amount_display,
                    "budget_reason": reason,
                    "set_target_cpa": set_cpa,
                    "target_cpa": cpa_amount,
                    "target_cpa_display": cpa_display,
                    "target_cpa_reason": cpa_reason,
                }
            )
        else:
            logger.warning("Unexpected LLM response for budget: %r", raw)
    except Exception as e:
        logger.warning("LLM budget decision failed: %s — fallback к середине.", e)
        decision["budget_reason"] = default_reason + f" (LLM недоступен: {e})"
    return decision


def run_step4(
    driver: WebDriver,
    *,
    budget_min: Optional[str] = None,
    budget_max: Optional[str] = None,
    business_name: Optional[str] = None,
    usp: Optional[str] = None,
    site_url: Optional[str] = None,
    campaign_objective_label: Optional[str] = None,
    campaign_objective_reason: Optional[str] = None,
    campaign_goal_label: Optional[str] = None,
    campaign_goal_reason: Optional[str] = None,
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
        logger.debug("DemandGen Step4: %s took %.1f ms (total %.1f ms)", label, elapsed_stage, elapsed_total)
        stage_log.append((label, elapsed_stage))
        stage_ts = now
    _maybe_handle_confirm_its_you(driver, emit=emit)
    _mark_stage("initial_confirm")

    # Бюджет + Target CPA
    _open_budget_panel(driver)
    _mark_stage("open_budget_panel")
    budget_input = _find_budget_input(driver)
    currency_symbol, currency_code = _detect_currency_symbol_and_code(driver, budget_input)

    budget_min_num = _normalize_number(budget_min)
    budget_max_num = _normalize_number(budget_max)
    budget_decision = _decide_budget_and_bidding(
        budget_min=budget_min_num,
        budget_max=budget_max_num,
        currency_symbol=currency_symbol,
        currency_code=currency_code,
        business_name=business_name,
        usp=usp,
        site_url=site_url,
        campaign_objective_label=campaign_objective_label,
        campaign_objective_reason=campaign_objective_reason,
        campaign_goal_label=campaign_goal_label,
        campaign_goal_reason=campaign_goal_reason,
    )
    _mark_stage("budget_decision")
    budget_amount = budget_decision["budget_amount"]
    budget_display = budget_decision.get("budget_amount_display")
    budget_reason = str(budget_decision["budget_reason"] or "").strip()
    target_cpa_amount = budget_decision.get("target_cpa")
    target_cpa_display = budget_decision.get("target_cpa_display")
    target_cpa_reason = str(budget_decision.get("target_cpa_reason") or "").strip()
    set_target_cpa = bool(budget_decision.get("set_target_cpa", False))

    if budget_amount is not None and not budget_display:
        budget_display = _format_currency_amount(currency_symbol, currency_code, budget_amount)
    if target_cpa_amount is not None and not target_cpa_display:
        target_cpa_display = _format_currency_amount(currency_symbol, currency_code, target_cpa_amount)

    if budget_amount:
        if _set_daily_budget(driver, float(budget_amount), ensure_panel=False):
            display = budget_display or (f"{currency_symbol}{budget_amount:.2f}" if currency_symbol else f"{budget_amount:.2f} {currency_code}")
            _emit(emit, f"Устанавливаю дневной бюджет: {display}. {budget_reason}")
        else:
            _emit(emit, "Не удалось ввести бюджет — оставляю значение по умолчанию")
            budget_reason += " (не удалось применить — оставил значение по умолчанию)"
    else:
        _emit(emit, "Диапазон бюджета не задан — пропускаю установку")
    _mark_stage("apply_budget")

    if set_target_cpa and target_cpa_amount:
        if _set_target_cpa(driver, float(target_cpa_amount)):
            display_cpa = target_cpa_display or (f"{currency_symbol}{target_cpa_amount:.2f}" if currency_symbol else f"{target_cpa_amount:.2f} {currency_code}")
            reason_text = target_cpa_reason or f"Устанавливаю целевой CPA: {display_cpa}"
            _emit(emit, reason_text)
        else:
            _emit(emit, "Не удалось настроить целевой CPA — продолжу без него")
            target_cpa_amount = None
            target_cpa_display = ""
            target_cpa_reason = "Не удалось применить CPA на интерфейсе, использую автоматическое управление."
    else:
        if target_cpa_reason:
            _emit(emit, f"CPA не задаю: {target_cpa_reason}")
    _mark_stage("apply_target_cpa")

    _maybe_handle_confirm_its_you(driver, emit=emit)
    _mark_stage("post_apply_confirm")

    elapsed = int((time.time() - t0) * 1000)
    current_url = driver.current_url or ""
    if stage_log:
        breakdown = ", ".join(f"{name}={dur:.0f}ms" for name, dur in stage_log)
        logger.info("step4 DemandGen breakdown: %s", breakdown)
    logger.info(
        "step4 DemandGen OK (%d ms). URL=%s | budget=%s | target_cpa=%s",
        elapsed,
        current_url,
        budget_amount,
        target_cpa_amount,
    )
    _emit(emit, "Шаг готов — ожидаю дальнейшие настройки")
    return {
        "new_url": current_url,
        "duration_ms": elapsed,
        "campaign_budget_amount": budget_display or budget_amount,
        "campaign_budget_reason": budget_reason,
        "campaign_target_cpa": target_cpa_display or target_cpa_amount,
        "campaign_target_cpa_reason": target_cpa_reason,
        "campaign_target_cpa_enabled": bool(target_cpa_amount),
        "campaign_objective_label": campaign_objective_label,
        "campaign_objective_reason": campaign_objective_reason,
        "campaign_currency_symbol": currency_symbol,
        "campaign_currency_code": currency_code,
        "timing_breakdown": [{"stage": name, "duration_ms": dur} for name, dur in stage_log],
    }
