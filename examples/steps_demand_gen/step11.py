# -*- coding: utf-8 -*-
"""
Шаг 11 (Demand Gen):
- генерирует название объявления через LLM (с безопасным фолбэком);
- заполняет поле «Ad name»;
- выставляет Final URL (или использует сайт компании, если URL не передан).
"""

from __future__ import annotations

import logging
import os
import random
import re
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

from selenium.common.exceptions import NoSuchElementException  # type: ignore
from selenium.webdriver.common.by import By  # type: ignore
from selenium.webdriver.common.keys import Keys  # type: ignore
from selenium.webdriver.remote.webdriver import WebDriver, WebElement  # type: ignore

from examples.steps import step4 as base_step4  # type: ignore

try:
    from ads_ai.llm.gemini import GeminiClient  # type: ignore
except Exception:  # pragma: no cover
    GeminiClient = None  # type: ignore


logger = logging.getLogger("ads_ai.gads.step11.demand_gen")
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

LLM_MODEL = os.getenv("GEMINI_MODEL", "models/gemini-2.0-flash")
STEP11_DISABLE_LLM = str(os.getenv("ADS_AI_STEP11_DISABLE_LLM", "")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

AD_NAME_PANEL_SYNS: Sequence[str] = ["ad name", "название объявления", "название рекламы"]
AD_NAME_INPUT_SELECTORS: Sequence[str] = (
    'material-expansionpanel[aria-label*="Ad name" i] input[aria-label*="Ad name" i]',
    'material-expansionpanel[aria-label*="Ad name" i] material-input input',
    'input[aria-label="Ad name"]',
    'input[aria-label*="ad name" i]',
)
FINAL_URL_PANEL_SYNS: Sequence[str] = ["final url", "конечный url", "landing page", "website"]
FINAL_URL_INPUT_SELECTORS: Sequence[str] = (
    'url-input input[aria-label*="Final URL" i]',
    'material-expansionpanel[aria-label*="Final URL" i] input[aria-label*="Final URL" i]',
    'input[aria-label="Final URL"]',
    'url-input material-input input',
    'url-input input.input-area',
    'url-input input',
    'material-input input.input-area',
)
FINAL_URL_PREFIX_BUTTON_SELECTORS: Sequence[str] = (
    'url-input dropdown-button div[role="button"][aria-label*="URL prefix" i]',
    'url-input dropdown-button div[role="button"]',
    'material-expansionpanel[aria-label*="Final URL" i] dropdown-button div[role="button"]',
)
URL_PREFIX_OPTIONS: Sequence[str] = ("https://", "http://")


class _ConfirmWatcher:
    """Фоново обрабатывает Confirm it's you, пока выполняется шаг."""

    def __init__(self, driver: WebDriver, emit: Optional[Callable[[str], None]], interval: float = 0.35) -> None:
        self._driver = driver
        self._emit = emit
        self._interval = max(0.2, float(interval))
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                _maybe_handle_confirm_its_you(self._driver, self._emit)
            except Exception:
                pass
            self._stop.wait(self._interval)

    def __enter__(self) -> "_ConfirmWatcher":
        if self._thread is None:
            self._stop.clear()
            self._thread = threading.Thread(target=self._loop, name="step11-confirm-watcher", daemon=True)
            self._thread.start()
        try:
            _maybe_handle_confirm_its_you(self._driver, self._emit)
        except Exception:
            pass
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self._stop.set()
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
            const input = el.matches('input') ? el : el.querySelector('input');
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
            element,
            value or "",
        )
        return True
    except Exception as exc:  # pragma: no cover
        logger.debug("Failed to set input value: %s", exc)
        return False


def _get_input_value(driver: WebDriver, element: WebElement) -> str:
    try:
        return str(
            driver.execute_script(
                """
                const el = arguments[0];
                if (!el) return '';
                const input = el.matches('input') ? el : el.querySelector('input');
                if (!input) return '';
                return (input.value || '').trim();
                """,
                element,
            )
            or ""
        ).strip()
    except Exception:
        try:
            return (element.get_attribute("value") or "").strip()
        except Exception:
            return ""


def _read_button_value(driver: WebDriver, element: Optional[WebElement]) -> str:
    if element is None:
        return ""
    try:
        return str(
            driver.execute_script(
                """
                const node = arguments[0];
                if (!node) return '';
                const textNode = node.querySelector('.button-text') || node;
                return (textNode.innerText || textNode.textContent || '').trim();
                """,
                element,
            )
            or ""
        )
    except Exception:
        try:
            return (element.text or "").strip()
        except Exception:
            return ""


def _dropdown_select(driver: WebDriver, button: WebElement, target_label: str, *, timeout: float = 5.0) -> bool:
    target_label = str(target_label or "").strip()
    if not target_label:
        return False
    if not _js_click(driver, button):
        return False

    deadline = time.time() + max(0.8, timeout)
    logged_options = False
    target_lower = target_label.lower()
    while time.time() < deadline:
        try:
            options = driver.find_elements(
                By.CSS_SELECTOR,
                "material-dropdown-select-popup.visible material-select-dropdown-item",
            )
        except Exception:
            options = []
        if not options:
            time.sleep(0.15)
            continue
        if not logged_options:
            labels = [(_read_button_value(driver, opt) or "").strip() for opt in options]
            logger.debug("Dropdown options: %s", labels)
            logged_options = True
        for option in options:
            text = (_read_button_value(driver, option) or "").strip()
            if text.lower() != target_lower:
                continue
            try:
                label_el = option.find_element(By.CSS_SELECTOR, ".label")
            except NoSuchElementException:
                label_el = option
            if _js_click(driver, label_el):
                time.sleep(0.1)
                return True
            try:
                driver.execute_script(
                    """
                    const target = arguments[0];
                    const node = target.querySelector('.label') || target;
                    node.scrollIntoView({block:'center', inline:'center'});
                    node.dispatchEvent(new MouseEvent('click',{bubbles:true,cancelable:true,view:window}));
                    return true;
                    """,
                    option,
                )
                time.sleep(0.1)
                return True
            except Exception:
                continue
        time.sleep(0.1)

    try:
        return bool(
            driver.execute_script(
                """
                const target = arguments[0];
                const items = [...document.querySelectorAll('material-select-dropdown-item')];
                for (const item of items) {
                    const labelNode = item.querySelector('.label');
                    const text = (labelNode ? labelNode.innerText : item.innerText || '').trim().toLowerCase();
                    if (text === target) {
                        (labelNode || item).scrollIntoView({block:'center', inline:'center'});
                        (labelNode || item).dispatchEvent(new MouseEvent('click',{bubbles:true,cancelable:true,view:window}));
                        return true;
                    }
                }
                return false;
                """,
                target_lower,
            )
        )
    except Exception as exc:  # pragma: no cover
        logger.debug("Dropdown JS fallback failed: %s", exc)
        return False


def _dropdown_wait_close(driver: WebDriver, timeout: float = 3.0) -> None:
    deadline = time.time() + max(0.5, timeout)
    while time.time() < deadline:
        try:
            visible = driver.find_elements(By.CSS_SELECTOR, "material-dropdown-select-popup.visible")
        except Exception:
            visible = []
        if not visible:
            return
        time.sleep(0.15)


def _derive_slug(business_name: Optional[str], usp: Optional[str], site_url: Optional[str]) -> str:
    raw = " ".join(filter(None, [(business_name or "").strip(), (usp or "").strip(), (site_url or "").strip()]))
    slug = re.sub(r"[^a-z0-9]+", "-", raw.lower()).strip("-")
    if not slug:
        slug = "ad"
    return slug[:30]


def _fallback_ad_name(
    *,
    business_name: Optional[str],
    usp: Optional[str],
    site_url: Optional[str],
) -> str:
    slug = _derive_slug(business_name, usp, site_url)
    stamp = time.strftime("%y%m%d")
    suffix = random.randint(100, 999)
    candidate = f"{slug}-{stamp}-{suffix}"
    if len(candidate) > 50:
        candidate = candidate[:50]
    if not re.search(r"[a-zA-Z]", candidate):
        candidate = f"ad-{candidate}"
    if not re.search(r"\d", candidate):
        candidate = f"{candidate}-{random.randint(10,99)}"
    return candidate[:60]


def _decide_ad_name(
    *,
    business_name: Optional[str],
    usp: Optional[str],
    site_url: Optional[str],
    campaign_context: Optional[str],
) -> Dict[str, str]:
    fallback = {
        "name": _fallback_ad_name(business_name=business_name, usp=usp, site_url=site_url),
        "reason": "LLM недоступна, использован фолбэк.",
        "source": "fallback",
    }

    if STEP11_DISABLE_LLM or GeminiClient is None:
        if STEP11_DISABLE_LLM:
            fallback["reason"] = "LLM отключена через переменные окружения."
        elif GeminiClient is None:
            fallback["reason"] = "LLM клиент недоступен."
        return fallback

    prompt = f"""
Вы помогаете медиабайеру на шаге создания Google Ads Demand Gen.
Нужно придумать название объявления. Верните ТОЛЬКО JSON без пояснений:
{{
  "ad_name": <строка до 50 символов, латиницей, без кавычек внутри, содержит хотя бы одну букву и одну цифру>,
  "reason": <краткое объяснение выбора, до 140 символов>
}}

Контекст кампании: {campaign_context or "—"}.
Компания: {business_name or "—"}.
УТП: {usp or "—"}.
Сайт: {site_url or "—"}.

Требования:
- Используйте только латиницу, допустимы символы пробел, '-', '_'.
- Название должно быть осмысленным, отражать предложение или аудиторию.
- Содержать хотя бы одну цифру (например, год, размер скидки или код предложения).
""".strip()

    try:
        client = GeminiClient(LLM_MODEL, temperature=0.15, retries=1)
        response = client.generate_json(prompt)
    except Exception as exc:  # pragma: no cover
        logger.warning("DemandGen step11: LLM call failed (%s)", exc)
        fallback["reason"] = "Ошибка LLM, использован безопасный фолбэк."
        return fallback

    if not isinstance(response, dict):
        logger.warning("DemandGen step11: LLM ответ нераспознан, используем фолбэк.")
        fallback["reason"] = "LLM вернула некорректный ответ."
        return fallback

    name = str(response.get("ad_name") or "").strip()
    if not name or len(name) > 60 or not re.search(r"[A-Za-z]", name) or not re.search(r"\d", name):
        name = fallback["name"]
    reason = str(response.get("reason") or "").strip() or "Название выбрано автоматически."

    return {
        "name": name,
        "reason": reason[:160],
        "source": "llm",
    }


def _normalize_final_url(raw: Optional[str], site_url: Optional[str]) -> str:
    value = (raw or "").strip()
    if not value:
        value = (site_url or "").strip()
    if not value:
        return ""
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", value):
        value = f"https://{value}"
    return value


def _split_final_url(value: str) -> Tuple[str, str]:
    if not value:
        return "", ""
    try:
        parsed = urlparse(value)
    except Exception:
        return "https://", value
    scheme = (parsed.scheme or "https").lower()
    prefix = f"{scheme}://"
    remainder = parsed.netloc or ""
    remainder += parsed.path or ""
    if parsed.query:
        remainder += f"?{parsed.query}"
    if parsed.fragment:
        remainder += f"#{parsed.fragment}"
    return prefix, remainder.lstrip("/")


_SCHEME_PREFIX_RE = re.compile(r"^\s*(https?://)", re.IGNORECASE)


def _strip_scheme_prefix(value: Optional[str]) -> str:
    text = (value or "").strip()
    while True:
        match = _SCHEME_PREFIX_RE.match(text)
        if not match:
            break
        text = text[match.end() :]
    return text


def _clear_input_with_shortcuts(element: WebElement) -> bool:
    cleared = False
    for modifier in (Keys.CONTROL, Keys.COMMAND):
        try:
            element.send_keys(modifier, "a")
            element.send_keys(Keys.DELETE)
            cleared = True
            break
        except Exception:
            continue
    if not cleared:
        try:
            element.clear()
            cleared = True
        except Exception:
            pass
    return cleared


def _type_value_via_keys(driver: WebDriver, element: WebElement, value: str) -> bool:
    if not value:
        return False
    if not _js_click(driver, element):
        try:
            element.click()
        except Exception:
            pass
    _clear_input_with_shortcuts(element)
    try:
        element.send_keys(value)
        time.sleep(0.2)
        return True
    except Exception as exc:
        logger.debug("DemandGen step11: send_keys typing failed: %s", exc)
        return False


def _apply_final_url(
    driver: WebDriver,
    *,
    final_url: str,
    prefix_button: Optional[WebElement],
    input_element: WebElement,
) -> Tuple[str, str]:
    logger.info("DemandGen step11: start applying Final URL raw='%s'", final_url)
    target_prefix, target_value = _split_final_url(final_url)
    if not target_value:
        target_value = final_url

    target_value = _strip_scheme_prefix(target_value)
    if not target_value:
        target_value = _strip_scheme_prefix(final_url)
    logger.info(
        "DemandGen step11: resolved target_prefix='%s', target_value='%s'",
        target_prefix,
        target_value,
    )

    current_prefix = _read_button_value(driver, prefix_button)
    logger.info(
        "DemandGen step11: current_prefix='%s', has_prefix_button=%s",
        current_prefix,
        bool(prefix_button),
    )
    if prefix_button and target_prefix and target_prefix.lower() != current_prefix.lower():
        if target_prefix.lower() in {opt.lower() for opt in URL_PREFIX_OPTIONS}:
            if _dropdown_select(driver, prefix_button, target_prefix):
                _dropdown_wait_close(driver)
                current_prefix = target_prefix
            else:
                logger.warning("DemandGen step11: не удалось выбрать префикс URL %s", target_prefix)
        else:
            logger.debug("DemandGen step11: пропускаю выбор префикса %s (нет в списке)", target_prefix)

    if not _set_input_value(driver, input_element, target_value):
        logger.warning("DemandGen step11: _set_input_value вернул False, пробуем ручной ввод")
        _type_value_via_keys(driver, input_element, target_value)
    time.sleep(0.2)
    applied_prefix = _read_button_value(driver, prefix_button) or current_prefix
    applied_value = _get_input_value(driver, input_element)
    if target_value and applied_value.strip() != target_value.strip():
        logger.warning(
            "DemandGen step11: после ввода значение='%s', ожидали='%s', повторяем через send_keys",
            applied_value,
            target_value,
        )
        if _type_value_via_keys(driver, input_element, target_value):
            time.sleep(0.2)
            applied_prefix = _read_button_value(driver, prefix_button) or applied_prefix
            applied_value = _get_input_value(driver, input_element)
    logger.info(
        "DemandGen step11: applied_prefix='%s', applied_value='%s'",
        applied_prefix,
        applied_value,
    )
    return applied_prefix, applied_value


def run_step11(
    driver: WebDriver,
    *,
    business_name: Optional[str] = None,
    site_url: Optional[str] = None,
    usp: Optional[str] = None,
    final_url: Optional[str] = None,
    campaign_context: Optional[str] = None,
    timeout_total: float = 90.0,
    emit: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    started = time.time()
    _dismiss_soft_dialogs(driver, budget_ms=400)
    logger.info(
        "step11 DemandGen start (business=%s, site=%s, usp=%s)",
        (business_name or "").strip() or "-",
        (site_url or "").strip() or "-",
        (usp or "").strip() or "-",
    )

    with _ConfirmWatcher(driver, emit=emit, interval=0.35):
        stage_started = started
        timings: List[Tuple[str, float]] = []

        def _mark_stage(label: str) -> None:
            nonlocal stage_started
            now = time.time()
            duration = (now - stage_started) * 1000.0
            timings.append((label, duration))
            logger.info("DemandGen step11 stage %s | %.1f ms", label, duration)
            stage_started = now

        # --- Ad name ---
        _emit(emit, "Генерирую название объявления через нейросеть")
        if not _ensure_panel_open(driver, list(AD_NAME_PANEL_SYNS)):
            raise RuntimeError("Раздел «Ad name» не найден или недоступен.")
        _mark_stage("open_ad_name_panel")

        ad_name_input = _wait_for_any_selector(
            driver,
            AD_NAME_INPUT_SELECTORS,
            timeout=min(12.0, timeout_total * 0.2),
            require_visible=True,
        )
        if not ad_name_input:
            raise RuntimeError("Поле «Ad name» не найдено.")
        initial_ad_name = _get_input_value(driver, ad_name_input)

        decision = _decide_ad_name(
            business_name=business_name,
            usp=usp,
            site_url=site_url,
            campaign_context=campaign_context,
        )
        ad_name = decision["name"]
        _emit(emit, f"Название объявления: {ad_name}")

        if not _set_input_value(driver, ad_name_input, ad_name):
            try:
                ad_name_input.clear()
            except Exception:
                pass
            try:
                ad_name_input.send_keys(ad_name)
            except Exception as exc:
                raise RuntimeError("Не удалось заполнить поле «Ad name».") from exc
        time.sleep(0.2)
        _mark_stage("apply_ad_name")

        applied_ad_name = _get_input_value(driver, ad_name_input)

        # --- Final URL ---
        requested_final_url = _normalize_final_url(final_url, site_url)
        final_url_info = {
            "requested": requested_final_url,
            "applied_prefix": "",
            "applied_value": "",
        }
        final_url_changed = False

        if requested_final_url:
            if not _ensure_panel_open(driver, list(FINAL_URL_PANEL_SYNS)):
                raise RuntimeError("Раздел «Final URL» не найден или недоступен.")
            _mark_stage("open_final_url_panel")

            final_url_input = _wait_for_any_selector(
                driver,
                FINAL_URL_INPUT_SELECTORS,
                timeout=min(10.0, timeout_total * 0.2),
                require_visible=True,
            )
            if not final_url_input:
                raise RuntimeError("Поле «Final URL» не найдено.")
            initial_final_value = _get_input_value(driver, final_url_input)
            prefix_button = _wait_for_any_selector(
                driver,
                FINAL_URL_PREFIX_BUTTON_SELECTORS,
                timeout=3.0,
                require_visible=False,
            )
            initial_prefix = _read_button_value(driver, prefix_button)
            applied_prefix, applied_value = _apply_final_url(
                driver,
                final_url=requested_final_url,
                prefix_button=prefix_button,
                input_element=final_url_input,
            )
            final_url_info.update(
                {
                    "applied_prefix": applied_prefix,
                    "applied_value": applied_value,
                    "initial_value": initial_final_value,
                    "initial_prefix": initial_prefix,
                }
            )
            final_url_changed = (
                applied_value.strip() != (initial_final_value or "").strip()
                or (applied_prefix or "").strip() != (initial_prefix or "").strip()
            )
            _mark_stage("apply_final_url")
            _emit(emit, f"Final URL установлен: {(applied_prefix or 'https://')}{applied_value}")
        else:
            logger.warning("DemandGen step11: Final URL не задан и не найден URL сайта, шаг пропущен.")

    duration_ms = int((time.time() - started) * 1000)
    logger.info(
        "step11 DemandGen done (%d ms). Ad name='%s' (source=%s)",
        duration_ms,
        applied_ad_name,
        decision.get("source"),
    )
    if timings:
        breakdown = ", ".join(f"{name}={dur:.0f}ms" for name, dur in timings)
        logger.info("DemandGen step11 timing breakdown: %s", breakdown)

    return {
        "ad_name": applied_ad_name,
        "ad_name_source": decision.get("source"),
        "ad_name_reason": decision.get("reason"),
        "ad_name_changes_applied": applied_ad_name.strip() != (initial_ad_name or "").strip(),
        "final_url": final_url_info,
        "final_url_changes_applied": final_url_changed,
        "duration_ms": duration_ms,
        "timing_breakdown": [{"stage": name, "duration_ms": dur} for name, dur in timings],
    }


def run(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Совместимость с автозапуском: examples.steps_demand_gen.step11:run."""
    return run_step11(*args, **kwargs)
