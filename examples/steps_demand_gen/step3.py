# -*- coding: utf-8 -*-
"""
Step 3 for Demand Gen campaigns.

На экране после выбора типа кампании:
1. Придумываем уникальное название кампании (LLM → fallback).
2. Выбираем цель кампании (Conversions или Clicks) — решение принимает LLM.
3. Передаём управление базовому шагу 3 (examples.steps.step3) для клика Continue.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Callable, Dict, Optional, Tuple
import json

from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver, WebElement

from examples.steps.step2 import (  # type: ignore
    _dismiss_hover_popups,
    _fallback_campaign_name,
    _find_campaign_name_input_best,
    _generate_campaign_name_via_llm,
    _is_interactable,
    _select_tab_by_datavalue_or_text,
    _set_campaign_name_safe,
    _short_type_label_by_code,
)

logger = logging.getLogger("ads_ai.gads.step3.demand_gen")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

try:
    from ads_ai.llm.gemini import GeminiClient  # type: ignore
except Exception as e:  # pragma: no cover
    GeminiClient = None  # type: ignore
    logger.warning("GeminiClient not available for DemandGen step3: %s", e)


GoalDecision = Tuple[str, str]  # (data_value, label)
GoalDecisionResult = Tuple[str, str, str]  # (data_value, label, reason)

GOAL_MAP: Dict[str, GoalDecision] = {
    "conversions": ("CampaignGoal.conversions", "Conversions"),
    "conversion": ("CampaignGoal.conversions", "Conversions"),
    "conversion_value": ("CampaignGoal.conversions", "Conversions"),
    "sales": ("CampaignGoal.conversions", "Conversions"),
    "clicks": ("CampaignGoal.clicks", "Clicks"),
    "traffic": ("CampaignGoal.clicks", "Clicks"),
}

GOAL_SYNONYMS = {
    "CampaignGoal.conversions": [
        "conversions", "conversion", "sales", "conversion goal", "продажи", "конверсии",
    ],
    "CampaignGoal.clicks": [
        "clicks", "traffic", "клики", "трафик", "engagement",
    ],
}


def _emit(emit: Optional[Callable[[str], None]], text: str) -> None:
    if callable(emit) and isinstance(text, str) and text.strip():
        try:
            emit(text.strip())
        except Exception:
            pass


def _wait_for_campaign_name_input(driver: WebDriver, timeout: float = 20.0) -> WebElement:
    deadline = time.time() + timeout
    fast_phase_until = time.time() + min(1.8, max(0.6, timeout * 0.25))
    last_cleanup = 0.0
    last_fail = None
    while time.time() < deadline:
        try:
            el = _find_campaign_name_input_best(driver)
            if el and _is_interactable(driver, el):
                return el
        except Exception as e:
            last_fail = e
        now = time.time()
        if now >= fast_phase_until and (now - last_cleanup) > 0.22:
            _dismiss_hover_popups(driver)
            last_cleanup = now
            time.sleep(0.16)
        else:
            time.sleep(0.08)
    if last_fail:
        logger.debug("wait_for_campaign_name_input last error: %r", last_fail)
    raise RuntimeError("Поле Campaign name не нашлось или недоступно.")


def _ensure_campaign_goal_panel_open(driver: WebDriver) -> None:
    deadline = time.time() + 3.0
    last_click = 0.0

    while time.time() < deadline:
        try:
            header = driver.find_element(By.CSS_SELECTOR, 'expansion-panel[activityname="CampaignGoalPanel"] .header')
        except Exception:
            return

        try:
            expanded = (header.get_attribute("aria-expanded") or "").lower() == "true"
        except Exception:
            expanded = False
        if expanded:
            return

        now = time.time()
        if now - last_click >= 0.2:
            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center',inline:'nearest'});", header)
            except Exception:
                pass
            try:
                header.click()
            except Exception:
                try:
                    driver.execute_script("arguments[0].click();", header)
                except Exception:
                    pass
            last_click = now
        time.sleep(0.06)


def _decide_campaign_goal_via_llm(
    *,
    business_name: Optional[str],
    usp: Optional[str],
    site_url: Optional[str],
    budget_per_day: Optional[str],
    campaign_type_label: str,
) -> GoalDecisionResult:
    default = GOAL_MAP["conversions"]
    default_reason = "Выбираю «Conversions» по умолчанию: чаще всего бизнесу нужны заявки и продажи."
    if GeminiClient is None:
        return default[0], default[1], default_reason
    prompt = {
        "task": "Return ONLY JSON with fields 'goal' (either 'conversions' or 'clicks') and 'reason' (short rationale in Russian).",
        "constraints": [
            "Decide which bidding focus suits the described business better.",
            "If clear conversion intent (sales, leads, signups) dominate — choose 'conversions'.",
            "If awareness/traffic focus or very low purchase intent — choose 'clicks'.",
            "Keep reason concise (<= 120 chars).",
        ],
        "inputs": {
            "business_name": (business_name or "").strip(),
            "usp": (usp or "").strip(),
            "site_url": (site_url or "").strip(),
            "budget_per_day": (budget_per_day or "").strip(),
            "campaign_type": campaign_type_label,
        },
        "output_schema": {"goal": "conversions|clicks", "reason": "string"},
        "format": "json_only_no_explanations",
    }
    model = os.getenv("GEMINI_MODEL", "models/gemini-2.0-flash")
    try:
        client = GeminiClient(model=model, temperature=0.1, retries=1, fallback_model=None)
        raw = client.generate_json(json.dumps(prompt, ensure_ascii=False))
        goal_raw = ""
        reason_raw = ""
        if isinstance(raw, dict):
            goal_raw = str(raw.get("goal", "")).strip().lower()
            reason_raw = str(raw.get("reason", "")).strip()
        if not goal_raw and isinstance(raw, str):
            goal_raw = raw.strip().lower()
        goal_key = next((k for k in GOAL_MAP if goal_raw == k or goal_raw.startswith(k)), "")
        if goal_key:
            reason = reason_raw or f"LLM: выбрал «{GOAL_MAP[goal_key][1]}» по описанию бизнеса."
            return GOAL_MAP[goal_key][0], GOAL_MAP[goal_key][1], reason
        logger.warning("LLM returned unexpected goal value: %r; fallback to conversions.", raw)
        return default[0], default[1], default_reason
    except Exception as e:
        logger.warning("LLM goal decision failed: %s — fallback to conversions.", e)
        fail_reason = f"LLM недоступен ({e}), выбираю «{default[1]}»."
        return default[0], default[1], fail_reason


def _select_campaign_goal(driver: WebDriver, goal_code: str, timeout: float = 10.0) -> bool:
    synonyms = GOAL_SYNONYMS.get(goal_code, [])
    return _select_tab_by_datavalue_or_text(
        driver,
        data_value=goal_code,
        text_synonyms=[s.lower() for s in synonyms],
        scope_css='expansion-panel[activityname="CampaignGoalPanel"] selection-view .cards',
        timeout=timeout,
    )


def run_step3(
    driver: WebDriver,
    *,
    campaign_type: Optional[str] = None,
    business_name: Optional[str] = None,
    usp: Optional[str] = None,
    site_url: Optional[str] = None,
    budget_per_day: Optional[str] = None,
    budget_clean: Optional[str] = None,
    timeout_total: float = 60.0,
    emit: Optional[Callable[[str], None]] = None,
) -> Dict[str, str]:
    """
    Выполняет Demand Gen шаг 3: имя кампании + выбор цели + continue.
    """
    t0 = time.time()
    campaign_type_code = (campaign_type or "OWNED_AND_OPERATED").strip().upper() or "OWNED_AND_OPERATED"
    type_label = _short_type_label_by_code(campaign_type_code)

    _emit(emit, "Заполняю имя кампании")
    input_el = _wait_for_campaign_name_input(driver, timeout=timeout_total / 2)

    try:
        current_value = input_el.get_attribute("value") or ""
    except Exception:
        current_value = ""

    name_generated = ""
    try:
        name_generated = _generate_campaign_name_via_llm(
            business_name=business_name,
            usp=usp,
            site_url=site_url,
            budget=(budget_clean or budget_per_day or ""),
            campaign_type_label=type_label,
        )
    except Exception as e:
        logger.warning("LLM campaign name failed: %s — fallback.", e)
        name_generated = _fallback_campaign_name(business_name=business_name, campaign_type_code=campaign_type_code)

    if not name_generated.strip():
        name_generated = _fallback_campaign_name(business_name=business_name, campaign_type_code=campaign_type_code)

    if current_value.strip() != name_generated.strip():
        if not _set_campaign_name_safe(driver, name_generated, input_el, attempts=2):
            raise RuntimeError("Не удалось ввести название кампании.")
    _emit(emit, f"Название кампании: «{name_generated}»")

    # --- Goal selection ---
    _emit(emit, "Выбираю цель кампании")
    _ensure_campaign_goal_panel_open(driver)
    goal_code, goal_label, goal_reason = _decide_campaign_goal_via_llm(
        business_name=business_name,
        usp=usp,
        site_url=site_url,
        budget_per_day=budget_per_day,
        campaign_type_label=type_label,
    )
    if not _select_campaign_goal(driver, goal_code, timeout=10.0):
        raise RuntimeError(f"Не удалось выбрать цель кампании {goal_label}.")
    _emit(emit, f"Цель кампании: {goal_label}. {goal_reason}")
    logger.info("DemandGen Step3 goal: %s (%s)", goal_label, goal_reason)

    elapsed_ms = int((time.time() - t0) * 1000)
    logger.info(
        "DemandGen Step3 завершён (%d ms). Имя: %s; Цель: %s (%s)",
        elapsed_ms,
        name_generated,
        goal_label,
        goal_reason,
    )
    _emit(emit, "Имя и цель кампании готовы — передаю следующему шагу")
    return {
        "campaign_name": name_generated,
        "campaign_goal": goal_code,
        "campaign_goal_label": goal_label,
        "campaign_goal_reason": goal_reason,
        "campaign_type": campaign_type_code,
        "duration_ms": elapsed_ms,
    }
