# -*- coding: utf-8 -*-
"""
Шаг 13 (Demand Gen):
- генерирует и заполняет тексты объявления (headline, description) и бизнес-имя;
- поддерживает три режима: «ai_only», «inspired» (LLM ремиксирует переданные примеры),
  «manual» (использовать тексты, заданные пользователем без изменений);
- управляет повторяющимися полями (добавляет строки по кнопке Add) и, при необходимости,
  выбирает call-to-action.
"""

from __future__ import annotations

import logging
import os
import random
import threading
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

from selenium.webdriver.common.by import By  # type: ignore
from selenium.webdriver.common.keys import Keys  # type: ignore
from selenium.webdriver.remote.webdriver import WebDriver, WebElement  # type: ignore

from examples.steps import step4 as base_step4  # type: ignore

try:
    from ads_ai.llm.gemini import GeminiClient  # type: ignore
except Exception:  # pragma: no cover
    GeminiClient = None  # type: ignore


logger = logging.getLogger("ads_ai.gads.step13.demand_gen")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

_emit = base_step4._emit  # type: ignore
_dismiss_soft_dialogs = base_step4._dismiss_soft_dialogs  # type: ignore
_ensure_panel_open = base_step4._ensure_panel_open  # type: ignore
_is_interactable = base_step4._is_interactable  # type: ignore
_maybe_handle_confirm_its_you = base_step4._maybe_handle_confirm_its_you  # type: ignore

LLM_MODEL = os.getenv("GEMINI_MODEL", "models/gemini-2.0-flash")
STEP13_DISABLE_LLM = str(os.getenv("ADS_AI_STEP13_DISABLE_LLM", "")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

TEXT_PANEL_SYNS: Sequence[str] = ("text", "headline", "description", "текст")
HEADLINE_INPUT_SELECTOR = 'multi-asset-editor[debugid="headlines"] input.input-area'
HEADLINE_ADD_BUTTON_SELECTOR = 'multi-asset-editor[debugid="headlines"] material-button.add-asset-button'
DESCRIPTION_INPUT_SELECTOR = 'multi-asset-editor[debugid="descriptions"] input.input-area'
DESCRIPTION_ADD_BUTTON_SELECTOR = 'multi-asset-editor[debugid="descriptions"] material-button.add-asset-button'
BUSINESS_NAME_INPUT_SELECTOR = 'material-input.business-name input.input-area, material-input.business-name input'
CTA_DROPDOWN_BUTTON_SELECTOR = 'material-dropdown-select.cta-text dropdown-button div[role="button"]'
CTA_OPTION_SELECTOR = 'material-dropdown-select-popup [role="option"], material-dropdown-select-popup material-option'

HEADLINE_MAX = 40
DESCRIPTION_MAX = 90
DEFAULT_HEADLINES = 5
DEFAULT_DESCRIPTIONS = 5


class _ConfirmWatcher:
    """Асинхронно закрывает Confirm-it's-you, пока выполняется шаг."""

    def __init__(self, driver: WebDriver, emit: Optional[Callable[[str], None]], interval: float = 0.35) -> None:
        self._driver = driver
        self._emit = emit
        self._interval = max(0.2, float(interval))
        self._stop = False
        self._thread: Optional[threading.Thread] = None

    def _loop(self) -> None:
        while not self._stop:
            try:
                _maybe_handle_confirm_its_you(self._driver, self._emit)
            except Exception:
                pass
            time.sleep(self._interval)

    def __enter__(self) -> "_ConfirmWatcher":
        _maybe_handle_confirm_its_you(self._driver, self._emit)
        if self._thread is None:
            self._stop = False
            self._thread = threading.Thread(target=self._loop, name="step13-confirm-watcher", daemon=True)
            self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self._stop = True
        if self._thread is not None:
            self._thread.join(timeout=1.2)
            self._thread = None
        return False


def _wait_for_any_selector(
    driver: WebDriver,
    selectors: Sequence[str],
    *,
    timeout: float = 10.0,
    require_visible: bool = True,
) -> Optional[WebElement]:
    deadline = time.time() + max(0.5, timeout)
    candidates = [sel for sel in selectors if sel]
    while time.time() < deadline:
        for selector in candidates:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
            except Exception:
                continue
            for element in elements:
                if require_visible and not _is_interactable(driver, element):
                    continue
                return element
        time.sleep(0.2)
    return None


def _js_click(driver: WebDriver, element: WebElement) -> bool:
    try:
        driver.execute_script(
            """
            const el = arguments[0];
            if (!el) return false;
            el.scrollIntoView({block: 'center', inline: 'center'});
            el.click();
            return true;
            """,
            element,
        )
        return True
    except Exception:
        try:
            element.click()
            return True
        except Exception:
            return False


def _set_input_value(driver: WebDriver, element: WebElement, value: str) -> bool:
    try:
        driver.execute_script(
            """
            const el = arguments[0];
            const value = arguments[1];
            if (!el) return false;
            const input = el.matches('input,textarea') ? el : el.querySelector('input,textarea');
            if (!input) return false;
            input.focus();
            input.value = '';
            input.dispatchEvent(new Event('input', { bubbles: true }));
            input.value = value;
            input.dispatchEvent(new Event('input', { bubbles: true }));
            input.dispatchEvent(new Event('change', { bubbles: true }));
            return true;
            """,
            element,
            value or "",
        )
        try:
            element.send_keys(Keys.END)
        except Exception:
            pass
        return True
    except Exception:
        return False


def _normalize_mode(mode: Optional[str]) -> str:
    value = (mode or "ai_only").strip().lower()
    if value in {"ai", "auto", "full_ai", "auto_generate"}:
        return "ai_only"
    if value in {"inspired", "guided", "hybrid"}:
        return "inspired"
    if value in {"manual", "upload_only", "provided"}:
        return "manual"
    return "ai_only"


def _strip_and_limit(items: Iterable[str], limit: int) -> List[str]:
    out: List[str] = []
    for value in items:
        text = str(value or "").strip()
        if text:
            out.append(text)
        if len(out) >= limit:
            break
    return out


def _dedupe_preserve(items: List[str]) -> List[str]:
    seen: set[str] = set()
    result: List[str] = []
    for item in items:
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def _derive_business_name(business_name: Optional[str], site_url: Optional[str]) -> str:
    if business_name and business_name.strip():
        return business_name.strip()
    if site_url:
        try:
            host = urlparse(site_url).netloc or site_url
        except Exception:
            host = site_url
        host = host.replace("www.", "").split(":")[0]
        return host[:25]
    return "Brand"


def _notes_from_seed(seed_assets: Optional[Dict[str, Any]]) -> str:
    if not isinstance(seed_assets, dict):
        return ""
    notes: List[str] = []
    for key in ("headlines", "descriptions", "notes", "image_prompts"):
        value = seed_assets.get(key)
        if isinstance(value, str):
            notes.append(value.strip())
        elif isinstance(value, Iterable):
            merged = [str(v).strip() for v in value if str(v).strip()]
            if merged:
                notes.append("; ".join(merged))
    return " | ".join(filter(None, notes))[:600]


def _fallback_headlines(
    *,
    count: int,
    business_name: Optional[str],
    usp: Optional[str],
) -> List[str]:
    base = business_name or "Brand"
    usp_part = f" {usp.strip()}" if usp else ""
    candidates = [
        f"{base}: Discover Quality{usp_part}",
        f"{base} — Special Offer",
        f"{base} Experts at Work",
        f"{base}: Upgrade Today",
        f"{base} • Smart Choice",
        f"{base} Makes It Easy",
        f"{base} You Can Trust",
    ]
    random.shuffle(candidates)
    return candidates[: max(1, count)]


def _fallback_descriptions(
    *,
    count: int,
    business_name: Optional[str],
    usp: Optional[str],
) -> List[str]:
    usp_text = usp or "We deliver results fast."
    candidates = [
        f"{usp_text} Discover how {business_name or 'our team'} can help you today.",
        f"Unlock better outcomes with {business_name or 'our solution'} — start in minutes.",
        f"Book a call with {business_name or 'experts'} and see measurable growth.",
        f"Trusted by teams worldwide. {usp_text}",
        f"Experience premium support and real impact with {business_name or 'our brand'}.",
    ]
    random.shuffle(candidates)
    return candidates[: max(1, count)]


def _llm_generate_text_assets(
    *,
    headlines_count: int,
    descriptions_count: int,
    business_name: Optional[str],
    usp: Optional[str],
    site_url: Optional[str],
    mode: str,
    seed_notes: str,
    language: str,
    campaign_context: Optional[str],
) -> Tuple[List[str], List[str], str]:
    if STEP13_DISABLE_LLM or GeminiClient is None:
        reason = (
            "LLM недоступна" if GeminiClient is None else "LLM отключена переменной окружения"
        )
        logger.warning("DemandGen step13: пропускаем LLM генерацию (%s)", reason)
        return (
            _fallback_headlines(count=headlines_count, business_name=business_name, usp=usp),
            _fallback_descriptions(count=descriptions_count, business_name=business_name, usp=usp),
            reason,
        )

    instructions = (
        "You help craft Google Ads Demand Gen assets. "
        "Return ONLY JSON with schema {\"headlines\": [\"...\"], \"descriptions\": [\"...\"]}. "
        f"Need {max(1, headlines_count)} headlines (<= {HEADLINE_MAX} characters) "
        f"and {max(1, descriptions_count)} descriptions (<= {DESCRIPTION_MAX} characters). "
        "Language: {language}. No emojis, no ALL CAPS, keep benefits clear. "
        "Headlines must be punchy; descriptions should include a call to action or benefit."
    ).replace("{language}", language or "English")
    context_parts = [
        f"Business name: {business_name or 'Brand'}",
        f"USP: {usp or '—'}",
        f"Website: {site_url or '—'}",
        f"Mode: {mode}",
    ]
    if campaign_context:
        context_parts.append(f"Campaign context: {campaign_context}")
    if seed_notes:
        context_parts.append(f"Inspiration: {seed_notes}")
    context = ". ".join(context_parts)
    prompt = f"{instructions}\n{context}"

    try:
        client = GeminiClient(LLM_MODEL, temperature=0.55 if mode == "inspired" else 0.35, retries=1)
        response = client.generate_json(prompt)
    except Exception as exc:  # pragma: no cover
        logger.warning("DemandGen step13: LLM вызов не удался (%s)", exc)
        return (
            _fallback_headlines(count=headlines_count, business_name=business_name, usp=usp),
            _fallback_descriptions(count=descriptions_count, business_name=business_name, usp=usp),
            str(exc),
        )

    data = response if isinstance(response, dict) else {}
    raw_headlines = data.get("headlines")
    raw_descriptions = data.get("descriptions")
    headlines = (
        _strip_and_limit(raw_headlines, headlines_count)
        if isinstance(raw_headlines, (list, tuple))
        else []
    )
    descriptions = (
        _strip_and_limit(raw_descriptions, descriptions_count)
        if isinstance(raw_descriptions, (list, tuple))
        else []
    )

    if len(headlines) < 1:
        headlines = _fallback_headlines(count=headlines_count, business_name=business_name, usp=usp)
    if len(descriptions) < 1:
        descriptions = _fallback_descriptions(count=descriptions_count, business_name=business_name, usp=usp)
    return headlines[:headlines_count], descriptions[:descriptions_count], "llm"


def _find_empty_input(driver: WebDriver, selector: str) -> Optional[WebElement]:
    try:
        elements = driver.find_elements(By.CSS_SELECTOR, selector)
    except Exception:
        elements = []
    for element in elements:
        try:
            value = (element.get_attribute("value") or "").strip()
        except Exception:
            value = ""
        if not value and _is_interactable(driver, element):
            return element
    return None


def _click_add_button(driver: WebDriver, selector: str) -> bool:
    button = _wait_for_any_selector(driver, [selector], timeout=4.0, require_visible=True)
    if not button:
        return False
    return _js_click(driver, button)


def _apply_list_to_inputs(
    driver: WebDriver,
    *,
    selector: str,
    add_button_selector: str,
    values: List[str],
    max_total: int,
    field_name: str,
) -> List[str]:
    applied: List[str] = []
    for idx, value in enumerate(values[: max_total]):
        target = _find_empty_input(driver, selector)
        attempt = 0
        while not target and attempt < 3:
            if not _click_add_button(driver, add_button_selector):
                break
            time.sleep(0.2)
            target = _find_empty_input(driver, selector)
            attempt += 1
        if not target:
            logger.warning("DemandGen step13: не удалось найти свободное поле для %s #%d", field_name, idx + 1)
            break
        text = value.strip()
        if len(text) > (HEADLINE_MAX if field_name == "headline" else DESCRIPTION_MAX):
            text = text[: (HEADLINE_MAX if field_name == "headline" else DESCRIPTION_MAX)].strip()
        if not _set_input_value(driver, target, text):
            logger.warning("DemandGen step13: не удалось проставить значение для %s #%d", field_name, idx + 1)
            continue
        applied.append(text)
        time.sleep(0.1)
    return applied


def _select_call_to_action(driver: WebDriver, value: Optional[str], timeout: float = 6.0) -> Tuple[str, bool]:
    desired = (value or "").strip()
    if not desired:
        return "(Automated)", False
    button = _wait_for_any_selector(driver, [CTA_DROPDOWN_BUTTON_SELECTOR], timeout=timeout, require_visible=True)
    if not button:
        logger.warning("DemandGen step13: CTA выпадающий список не найден.")
        return "(Automated)", False
    if not _js_click(driver, button):
        logger.warning("DemandGen step13: не удалось открыть список CTA.")
        return "(Automated)", False
    time.sleep(0.3)
    deadline = time.time() + timeout
    while time.time() < deadline:
        option = _wait_for_any_selector(driver, [CTA_OPTION_SELECTOR], timeout=1.0, require_visible=True)
        if not option:
            time.sleep(0.2)
            continue
        try:
            options = driver.find_elements(By.CSS_SELECTOR, CTA_OPTION_SELECTOR)
        except Exception:
            options = []
        for opt in options:
            text = (opt.text or opt.get_attribute("aria-label") or "").strip()
            if not text:
                continue
            if text.lower() == desired.lower():
                if _js_click(driver, opt):
                    return text, True
        time.sleep(0.1)
    logger.warning("DemandGen step13: нужный CTA '%s' не найден, оставляем автоматический.", desired)
    try:
        driver.execute_script("document.body.click();")
    except Exception:
        pass
    return "(Automated)", False


def run_step13(
    driver: WebDriver,
    *,
    mode: str = "ai_only",
    seed_assets: Optional[Dict[str, Any]] = None,
    provided_assets: Optional[Dict[str, Any]] = None,
    business_name: Optional[str] = None,
    usp: Optional[str] = None,
    site_url: Optional[str] = None,
    campaign_context: Optional[str] = None,
    call_to_action: Optional[str] = None,
    language: str = "English",
    desired_headlines: int = DEFAULT_HEADLINES,
    desired_descriptions: int = DEFAULT_DESCRIPTIONS,
    timeout_total: float = 180.0,
    emit: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    started = time.time()
    timings: List[Tuple[str, float]] = []

    def _mark(label: str, anchor: List[float] = [time.time()]) -> None:
        now = time.time()
        timings.append((label, (now - anchor[0]) * 1000.0))
        anchor[0] = now

    _dismiss_soft_dialogs(driver, budget_ms=600)
    normalized_mode = _normalize_mode(mode)
    _emit(emit, f"Шаг 13: подготовка текстов ({normalized_mode})")
    logger.info(
        "DemandGen step13 start | mode=%s | business=%s | site=%s",
        normalized_mode,
        business_name or "-",
        site_url or "-",
    )

    seed_notes = _notes_from_seed(seed_assets)
    manual_headlines = _strip_and_limit(
        provided_assets.get("headlines") if isinstance(provided_assets, dict) else [], desired_headlines
    )
    manual_descriptions = _strip_and_limit(
        provided_assets.get("descriptions") if isinstance(provided_assets, dict) else [], desired_descriptions
    )
    provided_business_name = (
        str(provided_assets.get("business_name")).strip()
        if isinstance(provided_assets, dict) and provided_assets.get("business_name")
        else ""
    )

    _mark("init_collect")

    if normalized_mode == "manual":
        if not manual_headlines:
            raise RuntimeError("В режиме manual необходимо передать хотя бы один headline.")
        if not manual_descriptions:
            raise RuntimeError("В режиме manual необходимо передать хотя бы одно описание.")
        headlines = _dedupe_preserve(manual_headlines)[: max(1, desired_headlines)]
        descriptions = _dedupe_preserve(manual_descriptions)[: max(1, desired_descriptions)]
        source = "manual"
    else:
        _emit(emit, "Генерирую тексты объявления через LLM")
        headlines, descriptions, llm_source = _llm_generate_text_assets(
            headlines_count=max(1, desired_headlines),
            descriptions_count=max(1, desired_descriptions),
            business_name=business_name,
            usp=usp,
            site_url=site_url,
            mode=normalized_mode,
            seed_notes=seed_notes,
            language=language,
            campaign_context=campaign_context,
        )
        source = llm_source
    _mark("generate_texts")

    applied_business_name = provided_business_name or (business_name or "").strip()
    if not applied_business_name:
        applied_business_name = _derive_business_name(business_name, site_url)[:25]

    _emit(emit, "Открываю блок Text / Headline / Description")
    if not _ensure_panel_open(driver, list(TEXT_PANEL_SYNS)):
        logger.warning("DemandGen step13: панель Text не найдена или не была закрыта — продолжаем.")

    with _ConfirmWatcher(driver, emit=emit):
        _emit(emit, f"Заполняю headline ({len(headlines)} шт.)")
        filled_headlines = _apply_list_to_inputs(
            driver,
            selector=HEADLINE_INPUT_SELECTOR,
            add_button_selector=HEADLINE_ADD_BUTTON_SELECTOR,
            values=headlines,
            max_total=max(1, desired_headlines),
            field_name="headline",
        )
        if len(filled_headlines) < 1:
            raise RuntimeError("Не удалось заполнить ни одного headline.")
        _mark("fill_headlines")

        _emit(emit, f"Заполняю descriptions ({len(descriptions)} шт.)")
        filled_descriptions = _apply_list_to_inputs(
            driver,
            selector=DESCRIPTION_INPUT_SELECTOR,
            add_button_selector=DESCRIPTION_ADD_BUTTON_SELECTOR,
            values=descriptions,
            max_total=max(1, desired_descriptions),
            field_name="description",
        )
        if len(filled_descriptions) < 1:
            raise RuntimeError("Не удалось заполнить ни одного description.")
        _mark("fill_descriptions")

        _emit(emit, "Указываю Business name")
        business_input = _wait_for_any_selector(
            driver,
            [BUSINESS_NAME_INPUT_SELECTOR],
            timeout=min(8.0, timeout_total * 0.2),
            require_visible=True,
        )
        if not business_input:
            raise RuntimeError("Поле Business name не найдено.")
        if not _set_input_value(driver, business_input, applied_business_name[:25]):
            raise RuntimeError("Не удалось заполнить поле Business name.")
        _mark("fill_business_name")

        applied_cta, cta_changed = _select_call_to_action(driver, call_to_action)
        _mark("select_cta")

    duration_ms = int((time.time() - started) * 1000)
    logger.info(
        "DemandGen step13 completed (%d ms). Headlines=%d Descriptions=%d CTA=%s",
        duration_ms,
        len(filled_headlines),
        len(filled_descriptions),
        applied_cta,
    )

    timing_breakdown = [{"stage": name, "duration_ms": int(duration)} for name, duration in timings]
    return {
        "mode": normalized_mode,
        "duration_ms": duration_ms,
        "source": source,
        "headlines": {
            "applied": filled_headlines,
            "requested": headlines,
            "limit": max(1, desired_headlines),
        },
        "descriptions": {
            "applied": filled_descriptions,
            "requested": descriptions,
            "limit": max(1, desired_descriptions),
        },
        "business_name": {
            "value": applied_business_name[:25],
            "provided": bool(provided_business_name),
        },
        "call_to_action": {
            "value": applied_cta,
            "changed": cta_changed,
        },
        "timing_breakdown": timing_breakdown,
        "campaign_context": campaign_context,
    }


def run(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Совместимость с автозапуском."""
    return run_step13(*args, **kwargs)
