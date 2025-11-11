# -*- coding: utf-8 -*-
"""
Шаг 10 (Demand Gen):
- открывает панель «Audience» и создаёт новую аудиторию;
- генерирует название аудитории и демографические параметры через LLM (fallback при недоступности);
- применяет выбор по полу и возрасту;
- сохраняет аудиторию и возвращает итоговую конфигурацию.
"""

from __future__ import annotations

import logging
import os
import random
import re
import threading
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from selenium.common.exceptions import NoSuchElementException  # type: ignore
from selenium.webdriver.common.by import By  # type: ignore
from selenium.webdriver.remote.webdriver import WebDriver, WebElement  # type: ignore

from examples.steps import step4 as base_step4  # type: ignore

try:
    from ads_ai.llm.gemini import GeminiClient  # type: ignore
except Exception:  # pragma: no cover
    GeminiClient = None  # type: ignore


logger = logging.getLogger("ads_ai.gads.step10.demand_gen")
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
STEP10_DISABLE_LLM = str(os.getenv("ADS_AI_STEP10_DISABLE_LLM", "")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

AUDIENCE_PANEL_SYNS: Sequence[str] = ["audience", "audiences", "аудитор", "целевые аудитории"]
AUDIENCE_NAME_SELECTORS: Sequence[str] = (
    'material-input.name-editor input[aria-label*="audience" i]',
    'persona-builder material-input.name-editor input',
    'material-input.name-editor input',
)
CREATE_AUDIENCE_BUTTON_SELECTORS: Sequence[str] = (
    'material-button[debugid="create-new-button"]',
    'persona-picker-modal material-button[debugid="create-new-button"]',
)
DEMOGRAPHICS_HEADER_SELECTORS: Sequence[str] = (
    "persona-audience-picker material-expansionpanel[aria-label*='Demographics' i] .header",
    "material-expansionpanel.demographic-picker-panel .header",
    "material-expansionpanel .header[aria-label*='Demographics' i]",
)
GENDER_CHECKBOX_SELECTOR = 'demographic-checkbox-picker[debugid="gender-checkbox-picker"] material-checkbox'
AGE_FROM_BUTTON_SELECTOR = (
    "demographic-picker-panel demographic-picker .age-range-picker "
    "material-dropdown-select.range-from-dropdown dropdown-button div[role='button']"
)
AGE_TO_BUTTON_SELECTOR = (
    "demographic-picker-panel demographic-picker .age-range-picker "
    "material-dropdown-select.range-to-dropdown dropdown-button div[role='button']"
)
SAVE_BUTTON_SELECTOR = "material-yes-no-buttons material-button.btn-yes, material-yes-no-buttons .btn-yes"
ADD_AUDIENCE_BUTTON_SELECTORS: Sequence[str] = (
    'material-button[debugid="select-new-button"]',
    'persona-picker-modal material-button[debugid="select-new-button"]',
)
NEW_AUDIENCE_BUTTON_SELECTORS: Sequence[str] = (
    "persona-picker material-button.create-new-button",
    "persona-picker-modal material-button.create-new-button",
    "material-button.empty-suggestions-create-new-button",
)
NEW_AUDIENCE_BUTTON_TEXTS: Sequence[str] = ("+ New Audience", "New Audience")
AUDIENCE_SIDEPANEL_ROOTS: Sequence[str] = (
    "persona-picker-modal .contents",
    "persona-builder",
)

AGE_FROM_OPTIONS = ["18", "25", "35", "45", "55", "65+"]
AGE_TO_OPTIONS = ["24", "34", "44", "54", "64", "65+"]
AGE_ORDER: Dict[str, int] = {
    "18": 18,
    "24": 24,
    "25": 25,
    "34": 34,
    "35": 35,
    "44": 44,
    "45": 45,
    "54": 54,
    "55": 55,
    "64": 64,
    "65": 65,
    "65+": 200,
}
GENDER_LABELS = {"female": "Female", "male": "Male", "unknown": "Unknown"}


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
            self._thread = threading.Thread(target=self._loop, name="step10-confirm-watcher", daemon=True)
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
    timeout: float = 12.0,
    require_visible: bool = True,
) -> Optional[WebElement]:
    deadline = time.time() + max(0.5, timeout)
    selectors = [sel for sel in selectors if sel]
    while time.time() < deadline:
        for selector in selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
            except Exception:
                continue
            for el in elements:
                if require_visible and not _is_interactable(driver, el):
                    continue
                return el
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
        logger.debug("Failed to set audience name: %s", exc)
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
                return input.value || '';
                """,
                element,
            )
            or ""
        ).strip()
    except Exception:
        try:
            return element.get_attribute("value") or ""
        except Exception:
            return ""


def _read_text(driver: WebDriver, element: WebElement) -> str:
    try:
        return str(
            driver.execute_script(
                "return (arguments[0].innerText || arguments[0].textContent || '').trim();",
                element,
            )
            or ""
        )
    except Exception:
        try:
            return (element.text or "").strip()
        except Exception:
            return ""


def _collect_gender_checkboxes(driver: WebDriver) -> List[WebElement]:
    try:
        return driver.find_elements(By.CSS_SELECTOR, GENDER_CHECKBOX_SELECTOR)
    except Exception:
        return []


def _checkbox_state(element: WebElement) -> bool:
    try:
        return str(element.get_attribute("aria-checked") or "").lower() == "true"
    except Exception:
        return False


def _set_checkbox_state(driver: WebDriver, element: WebElement, desired: bool) -> bool:
    current = _checkbox_state(element)
    if current == desired:
        return False
    for _ in range(3):
        try:
            driver.execute_script(
                """
                const el = arguments[0];
                if (!el) return false;
                const target = el.querySelector('.icon-container') || el;
                target.dispatchEvent(new MouseEvent('click', {bubbles:true,cancelable:true,view:window}));
                return true;
                """,
                element,
            )
        except Exception:
            try:
                element.click()
            except Exception:
                pass
        time.sleep(0.2)
        current = _checkbox_state(element)
        if current == desired:
            return True
    return False


def _read_button_value(driver: WebDriver, element: WebElement) -> str:
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
        return _read_text(driver, element)


def _click_button_with_text(
    driver: WebDriver,
    texts: Sequence[str],
    *,
    timeout: float = 6.0,
    emit: Optional[Callable[[str], None]] = None,
) -> bool:
    targets = {text.strip().lower() for text in texts if text and text.strip()}
    if not targets:
        return False
    deadline = time.time() + max(0.5, timeout)
    while time.time() < deadline:
        try:
            buttons = driver.find_elements(By.CSS_SELECTOR, "material-button")
        except Exception:
            buttons = []
        for button in buttons:
            label = _read_text(driver, button).lower()
            if label in targets:
                if _js_click(driver, button):
                    time.sleep(0.2)
                    return True
        _maybe_handle_confirm_its_you(driver, emit)
        time.sleep(0.2)
    return False


def _dropdown_select(driver: WebDriver, button: WebElement, target_label: str, *, timeout: float = 5.0) -> bool:
    target_label = str(target_label or "").strip()
    if not target_label:
        return False
    if not _js_click(driver, button):
        return False

    deadline = time.time() + max(0.8, timeout)
    selected = False
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
            texts = [(_read_text(driver, opt) or "").strip() for opt in options]
            logger.debug("Dropdown options available: %s", texts)
            logged_options = True
        for option in options:
            text = _read_text(driver, option)
            if text.lower() != target_lower:
                continue
            # Пытаемся кликнуть по текстовой части элемента (часто это стабильнее)
            try:
                label_el = option.find_element(By.CSS_SELECTOR, ".label")
            except NoSuchElementException:
                label_el = option
            if _js_click(driver, label_el):
                selected = True
                break
            # резерв: прямой синтетический клик через JS
            try:
                driver.execute_script(
                    """
                    const target = arguments[0];
                    if (!target) return false;
                    const node = target.querySelector('.label') || target;
                    node.scrollIntoView({block:'center', inline:'center'});
                    node.dispatchEvent(new MouseEvent('click',{bubbles:true,cancelable:true,view:window}));
                    return true;
                    """,
                    option,
                )
                time.sleep(0.1)
                selected = True
                break
            except Exception:
                pass
        if selected:
            break
        time.sleep(0.08)

    # если прямой клик не сработал — попробуем выбрать значение целиком через глобальный скрипт
    if not selected:
        try:
            selected = bool(
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
        except Exception as exc:
            logger.debug("Dropdown JS fallback failed: %s", exc)

    time.sleep(0.12)
    return selected


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


def _fallback_audience_name(
    *,
    business_name: Optional[str],
    usp: Optional[str],
    site_url: Optional[str],
) -> str:
    raw = " ".join(filter(None, [(business_name or "").strip(), (usp or "").strip(), (site_url or "").strip()]))
    slug = re.sub(r"[^a-z0-9]+", "-", raw.lower()).strip("-")
    if not slug:
        slug = "audience"
    slug = slug[:28]
    stamp = time.strftime("%y%m%d")
    suffix = random.randint(100, 999)
    return f"{slug}-{stamp}-{suffix}"


def _ensure_unique_audience_name(candidate: str, *, fallback: str, max_len: int = 60) -> str:
    name = (candidate or "").strip()
    if not name:
        name = fallback
    if len(name) > max_len or not re.search(r"[A-Za-z]", name):
        name = fallback

    if name != fallback:
        suffix = f"-{random.randint(100, 999)}"
        budget = max(1, max_len - len(suffix))
        trimmed = name[:budget].rstrip(" -_")
        if not trimmed:
            trimmed = fallback.split("-")[0] or "audience"
        name = f"{trimmed}{suffix}"

    if not re.search(r"\d", name):
        extra = f"-{random.randint(10, 99)}"
        budget = max(1, max_len - len(extra))
        trimmed = name[:budget].rstrip(" -_") or "audience"
        name = f"{trimmed}{extra}"

    return name[:max_len]


def _normalize_gender_selection(raw: Iterable[str]) -> List[str]:
    found: List[str] = []
    for item in raw or []:
        label = str(item or "").strip()
        if not label:
            continue
        norm = label.lower()
        if norm in GENDER_LABELS:
            canon = GENDER_LABELS[norm]
        else:
            canon = next((std for key, std in GENDER_LABELS.items() if std.lower() == norm), "")
        if canon and canon not in found:
            found.append(canon)
    if not found:
        return ["Female", "Male"]
    return found


def _normalize_age_bounds(
    age_from: Optional[str],
    age_to: Optional[str],
    *,
    default_from: str = "18",
    default_to: str = "65+",
) -> Tuple[str, str]:
    from_value = str(age_from or "").strip()
    to_value = str(age_to or "").strip()
    if from_value not in AGE_FROM_OPTIONS:
        from_value = default_from
    if to_value not in AGE_TO_OPTIONS:
        to_value = default_to
    if AGE_ORDER.get(from_value, 0) <= AGE_ORDER.get(to_value, 999):
        return from_value, to_value
    return default_from, default_to


def _decide_audience_profile(
    *,
    business_name: Optional[str],
    usp: Optional[str],
    site_url: Optional[str],
    campaign_context: Optional[str],
) -> Dict[str, Any]:
    fallback = {
        "audience_name": _fallback_audience_name(
            business_name=business_name,
            usp=usp,
            site_url=site_url,
        ),
        "gender": {"include": ["Female", "Male"], "reason": "Fallback audience (LLM unavailable)."},
        "age": {"from": "25", "to": "44", "reason": "Fallback age range"},
        "reason": "Стандартная аудитория по умолчанию.",
        "source": "fallback",
    }

    if STEP10_DISABLE_LLM or GeminiClient is None:
        if STEP10_DISABLE_LLM:
            fallback["reason"] = "LLM отключена через переменные окружения."
        elif GeminiClient is None:
            fallback["reason"] = "LLM клиент недоступен."
        return fallback

    prompt = f"""
Вы помогаете медиабайеру на шаге настройки Google Ads Demand Gen.
Нужно выбрать аудиторию и демографию. Ответьте ТОЛЬКО JSON без пояснений:
{{
  "audience_name": <строка до 50 символов, латиницей, без кавычек и спецсимволов>,
  "gender": {{
    "include": ["Female"|"Male"|"Unknown", ... как минимум одно значение],
    "reason": <краткое объяснение, до 140 символов>
  }},
  "age": {{
    "from": <одно из {AGE_FROM_OPTIONS}>,
    "to": <одно из {AGE_TO_OPTIONS}>,
    "reason": <краткое объяснение, до 140 символов>
  }},
  "summary": <одним предложением опишите выбранную аудиторию, до 160 символов>
}}

Контекст кампании: {campaign_context or "—"}.
Компания: {business_name or "—"}.
УТП: {usp or "—"}.
Сайт: {site_url or "—"}.

Требования:
- gender.include содержит только допустимые значения.
- Возраст from меньше либо равен возрасту to с учётом порядка (65+ = наибольшее).
- Название аудитории должно содержать как минимум одну букву и одну цифру.
"""
    try:
        client = GeminiClient(LLM_MODEL, temperature=0.15, retries=1)
        response = client.generate_json(prompt)
    except Exception as exc:  # pragma: no cover
        logger.warning("DemandGen step10: LLM call failed (%s)", exc)
        fallback["reason"] = "LLM недоступна, использован безопасный фолбэк."
        return fallback

    if not isinstance(response, dict):
        logger.warning("DemandGen step10: LLM вернул не JSON-объект, используем фолбэк.")
        fallback["reason"] = "LLM ответ нераспознан."
        return fallback

    audience_name = str(response.get("audience_name") or "").strip()
    if not audience_name or len(audience_name) > 60:
        audience_name = fallback["audience_name"]

    gender_block = response.get("gender")
    gender_reason = ""
    if isinstance(gender_block, dict):
        gender_reason = str(gender_block.get("reason") or "").strip()
        gender_values = gender_block.get("include")
    else:
        gender_values = response.get("gender")
    gender_include = _normalize_gender_selection(gender_values if isinstance(gender_values, Iterable) else [])

    age_block = response.get("age") if isinstance(response.get("age"), dict) else {}
    age_from = age_block.get("from") if isinstance(age_block, dict) else None
    age_to = age_block.get("to") if isinstance(age_block, dict) else None
    age_reason = str(age_block.get("reason") or "").strip() if isinstance(age_block, dict) else ""
    age_from, age_to = _normalize_age_bounds(age_from, age_to)

    summary = str(response.get("summary") or "").strip()
    if not summary:
        summary = gender_reason or age_reason or "Аудитория подобрана автоматически."

    audience_name = _ensure_unique_audience_name(audience_name, fallback=fallback["audience_name"])

    return {
        "audience_name": audience_name,
        "gender": {"include": gender_include, "reason": gender_reason or summary},
        "age": {"from": age_from, "to": age_to, "reason": age_reason or summary},
        "reason": summary,
        "source": "llm",
    }



def _open_new_audience_flow(
    driver: WebDriver,
    emit: Optional[Callable[[str], None]],
    *,
    timeout: float = 12.0,
) -> bool:
    direct_button = _wait_for_any_selector(
        driver,
        CREATE_AUDIENCE_BUTTON_SELECTORS,
        timeout=min(2.5, timeout),
        require_visible=True,
    )
    if direct_button and _js_click(driver, direct_button):
        time.sleep(0.12)
        return True

    add_button = _wait_for_any_selector(
        driver,
        ADD_AUDIENCE_BUTTON_SELECTORS,
        timeout=min(2.5, timeout),
        require_visible=True,
    )
    if add_button and _js_click(driver, add_button):
        time.sleep(0.15)
        _maybe_handle_confirm_its_you(driver, emit)
        new_button = _wait_for_any_selector(
            driver,
            NEW_AUDIENCE_BUTTON_SELECTORS,
            timeout=max(3.0, timeout - 2.5),
            require_visible=True,
        )
        if new_button and _js_click(driver, new_button):
            time.sleep(0.12)
            return True
        if _click_button_with_text(driver, NEW_AUDIENCE_BUTTON_TEXTS, timeout=max(2.5, timeout - 2.5), emit=emit):
            return True

    # Может появиться прямой create после промежуточных кликов — проверим ещё раз.
    direct_button = _wait_for_any_selector(
        driver,
        CREATE_AUDIENCE_BUTTON_SELECTORS,
        timeout=2.0,
        require_visible=True,
    )
    if direct_button and _js_click(driver, direct_button):
        time.sleep(0.12)
        return True

    return False


def _is_audience_panel_ready(driver: WebDriver) -> bool:
    probes = list(CREATE_AUDIENCE_BUTTON_SELECTORS) + list(ADD_AUDIENCE_BUTTON_SELECTORS) + list(AUDIENCE_SIDEPANEL_ROOTS)
    for selector in probes:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
        except Exception:
            continue
        for element in elements:
            try:
                if _is_interactable(driver, element):
                    return True
            except Exception:
                continue
    return False


def _ensure_demographics_open(driver: WebDriver) -> None:
    for selector in DEMOGRAPHICS_HEADER_SELECTORS:
        try:
            header = driver.find_element(By.CSS_SELECTOR, selector)
        except Exception:
            continue
        if not header:
            continue
        try:
            expanded = str(header.get_attribute("aria-expanded") or "").lower()
        except Exception:
            expanded = ""
        if expanded == "true":
            return
        if _js_click(driver, header):
            time.sleep(0.3)
            return


def _wait_sidepanel_close(driver: WebDriver, timeout: float = 10.0) -> bool:
    deadline = time.time() + max(1.0, timeout)
    while time.time() < deadline:
        visible = False
        for selector in AUDIENCE_SIDEPANEL_ROOTS:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
            except Exception:
                continue
            for el in elements:
                if el.is_displayed():
                    visible = True
                    break
            if visible:
                break
        if not visible:
            return True
        time.sleep(0.25)
    return False


def _click_ad_entry(driver: WebDriver, emit: Optional[Callable[[str], None]], *, timeout: float = 6.0) -> bool:
    deadline = time.time() + max(1.0, timeout)
    tried_js = False
    while time.time() < deadline:
        try:
            buttons = driver.find_elements(By.CSS_SELECTOR, "material-button.select-button")
        except Exception:
            buttons = []
        for button in buttons:
            label = _read_text(driver, button)
            if "ad 1" not in label.lower():
                continue
            if _js_click(driver, button):
                _emit(emit, "Переход к созданию объявления «Ad 1»")
                return True
        if not tried_js:
            tried_js = True
            try:
                clicked = bool(
                    driver.execute_script(
                        """
                        const buttons = [...document.querySelectorAll('material-button.select-button')];
                        for (const btn of buttons) {
                            const text = (btn.innerText || btn.textContent || '').trim().toLowerCase();
                            if (!text.includes('ad 1')) continue;
                            btn.scrollIntoView({block:'center', inline:'center'});
                            try { btn.click(); return true; } catch(e) {}
                            const content = btn.querySelector('.content, .button-text');
                            if (content) {
                                try { content.click(); return true; } catch(e2) {}
                                content.dispatchEvent(new MouseEvent('click',{bubbles:true,cancelable:true,view:window}));
                                return true;
                            }
                            btn.dispatchEvent(new MouseEvent('click',{bubbles:true,cancelable:true,view:window}));
                            return true;
                        }
                        return false;
                        """
                    )
                )
                if clicked:
                    _emit(emit, "Переход к созданию объявления «Ad 1»")
                    return True
            except Exception:
                pass
        time.sleep(0.2)
    return False


def _snapshot_gender_selection(driver: WebDriver) -> List[str]:
    snapshot: List[str] = []
    for checkbox in _collect_gender_checkboxes(driver):
        label = _read_text(driver, checkbox)
        if not label:
            continue
        if _checkbox_state(checkbox):
            snapshot.append(label)
    return snapshot


def _snapshot_age_bounds(driver: WebDriver) -> Tuple[str, str]:
    from_button = _wait_for_any_selector(driver, [AGE_FROM_BUTTON_SELECTOR], timeout=0.1)
    to_button = _wait_for_any_selector(driver, [AGE_TO_BUTTON_SELECTOR], timeout=0.1)
    return (
        _read_button_value(driver, from_button) if from_button else "",
        _read_button_value(driver, to_button) if to_button else "",
    )


def run_step10(
    driver: WebDriver,
    *,
    business_name: Optional[str] = None,
    site_url: Optional[str] = None,
    usp: Optional[str] = None,
    campaign_context: Optional[str] = None,
    timeout_total: float = 120.0,
    emit: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    started = time.time()
    _dismiss_soft_dialogs(driver, budget_ms=500)

    logger.info(
        "step10 DemandGen start (business=%s, site=%s, usp=%s)",
        (business_name or "").strip() or "-",
        (site_url or "").strip() or "-",
        (usp or "").strip() or "-",
    )

    with _ConfirmWatcher(driver, emit=emit, interval=0.35):
        stage_ts = started
        timings: List[Tuple[str, float]] = []

        def _mark_stage(label: str) -> None:
            nonlocal stage_ts
            now = time.time()
            duration = (now - stage_ts) * 1000.0
            timings.append((label, duration))
            logger.info("DemandGen step10 stage %s | %.1f ms", label, duration)
            stage_ts = now

        _emit(emit, "Открываю раздел «Audience»")
        if not _is_audience_panel_ready(driver):
            if not _ensure_panel_open(driver, list(AUDIENCE_PANEL_SYNS)):
                raise RuntimeError("Раздел «Audience» не найден или недоступен.")
        else:
            logger.debug("Audience panel already ready, skip ensure.")
        _mark_stage("open_audience_panel")

        _maybe_handle_confirm_its_you(driver, emit)
        if not _wait_for_any_selector(driver, AUDIENCE_SIDEPANEL_ROOTS, timeout=5.0, require_visible=True):
            logger.debug("Audience panel root not detected, continue.")

        _emit(emit, "Создаю новую аудиторию")
        if not _open_new_audience_flow(driver, emit, timeout=min(10.0, timeout_total * 0.2)):
            raise RuntimeError("Не удалось нажать «Create an audience».")
        _mark_stage("open_new_audience_flow")

        audience_input = _wait_for_any_selector(driver, AUDIENCE_NAME_SELECTORS, timeout=15.0)
        if not audience_input:
            raise RuntimeError("Форма создания аудитории не появилась.")
        _mark_stage("audience_form_ready")

        _maybe_handle_confirm_its_you(driver, emit)

        initial_name = _get_input_value(driver, audience_input)
        initial_gender = _snapshot_gender_selection(driver)
        initial_age_from, initial_age_to = _snapshot_age_bounds(driver)
        _emit(
            emit,
            "Исходные границы возраста в интерфейсе: "
            f"{initial_age_from or 'любой'} – {initial_age_to or 'любой'}",
        )
        logger.debug(
            "DemandGen step10: initial age bounds from UI: from=%s to=%s",
            initial_age_from or "-",
            initial_age_to or "-",
        )

        decision = _decide_audience_profile(
            business_name=business_name,
            usp=usp,
            site_url=site_url,
            campaign_context=campaign_context,
        )

        audience_name = decision.get("audience_name") or _fallback_audience_name(
            business_name=business_name,
            usp=usp,
            site_url=site_url,
        )
        gender_plan = decision.get("gender", {}).get("include", [])
        age_plan = decision.get("age", {})
        age_from = age_plan.get("from")
        age_to = age_plan.get("to")
        age_from, age_to = _normalize_age_bounds(age_from, age_to)
        _mark_stage("llm_decision")

        _emit(emit, f"Название аудитории: {audience_name}")
        if not _set_input_value(driver, audience_input, audience_name):
            try:
                audience_input.clear()
            except Exception:
                pass
            audience_input.send_keys(audience_name)
        time.sleep(0.2)
        _mark_stage("apply_audience_name")

        _emit(emit, "Настраиваю демографию через нейросеть")
        _ensure_demographics_open(driver)
        _maybe_handle_confirm_its_you(driver, emit)

        gender_checkboxes = _collect_gender_checkboxes(driver)
        applied_gender: List[str] = []
        gender_changed = False
        desired_gender = {label.lower(): True for label in gender_plan}

        # сначала выключим все чекбоксы, чтобы избежать смешанных состояний
        for checkbox in gender_checkboxes:
            if _set_checkbox_state(driver, checkbox, False):
                gender_changed = True
        time.sleep(0.2)

        for checkbox in gender_checkboxes:
            label = _read_text(driver, checkbox)
            if not label:
                continue
            should_enable = desired_gender.get(label.lower(), False)
            if should_enable:
                if _set_checkbox_state(driver, checkbox, True):
                    gender_changed = True
                if _checkbox_state(checkbox):
                    applied_gender.append(label)
            elif _checkbox_state(checkbox):
                applied_gender.append(label)

        if not applied_gender:
            # если ничего не включилось — оставим Female+Male.
            for checkbox in gender_checkboxes:
                label = _read_text(driver, checkbox)
                if label.lower() not in {"female", "male"}:
                    continue
                if _set_checkbox_state(driver, checkbox, True):
                    gender_changed = True
                if _checkbox_state(checkbox):
                    applied_gender.append(label)

        from_button = _wait_for_any_selector(driver, [AGE_FROM_BUTTON_SELECTOR], timeout=8.0)
        to_button = _wait_for_any_selector(driver, [AGE_TO_BUTTON_SELECTOR], timeout=8.0)
        age_changed = False
        applied_age_from = initial_age_from
        applied_age_to = initial_age_to

        if from_button and age_from:
            _emit(emit, f"Пробую выбрать возраст от {age_from}")
            logger.info("DemandGen step10: selecting age FROM %s", age_from)
            if _dropdown_select(driver, from_button, age_from):
                _dropdown_wait_close(driver)
                applied_age_from = _read_button_value(driver, from_button)
                age_changed = age_changed or (applied_age_from.lower() != (initial_age_from or "").lower())
                _emit(emit, f"Возраст от установлен: {applied_age_from or '—'}")
                logger.debug("DemandGen step10: applied age FROM %s", applied_age_from or "-")
            else:
                _emit(emit, f"Не удалось выбрать возраст от {age_from}, оставляю {applied_age_from or 'текущее'}")
                logger.warning("DemandGen step10: failed to select age FROM %s", age_from)
        elif not from_button:
            _emit(emit, "Кнопка выпадения возраста «от» не найдена")
            logger.warning("DemandGen step10: age FROM dropdown button missing")

        if to_button and age_to:
            _emit(emit, f"Пробую выбрать возраст до {age_to}")
            logger.info("DemandGen step10: selecting age TO %s", age_to)
            if _dropdown_select(driver, to_button, age_to):
                _dropdown_wait_close(driver)
                applied_age_to = _read_button_value(driver, to_button)
                age_changed = age_changed or (applied_age_to.lower() != (initial_age_to or "").lower())
                _emit(emit, f"Возраст до установлен: {applied_age_to or '—'}")
                logger.debug("DemandGen step10: applied age TO %s", applied_age_to or "-")
            else:
                _emit(emit, f"Не удалось выбрать возраст до {age_to}, оставляю {applied_age_to or 'текущее'}")
                logger.warning("DemandGen step10: failed to select age TO %s", age_to)
        elif not to_button:
            _emit(emit, "Кнопка выпадения возраста «до» не найдена")
            logger.warning("DemandGen step10: age TO dropdown button missing")

        _maybe_handle_confirm_its_you(driver, emit)
        _mark_stage("apply_demographics")

        _emit(
            emit,
            "Выбрана аудитория: "
            f"пол={', '.join(applied_gender) or '—'}, возраст {applied_age_from or '—'}–{applied_age_to or '—'}",
        )
        if decision.get("reason"):
            _emit(emit, f"Обоснование: {decision['reason']}")

        save_button = _wait_for_any_selector(driver, [SAVE_BUTTON_SELECTOR], timeout=10.0)
        if not save_button:
            raise RuntimeError("Не найдена кнопка Save для аудитории.")

        disabled = str(save_button.get_attribute("aria-disabled") or "").lower() == "true"
        if disabled:
            raise RuntimeError("Кнопка Save недоступна — проверьте состояние формы.")

        if not _js_click(driver, save_button):
            raise RuntimeError("Не удалось нажать кнопку Save.")

        audience_saved = _wait_sidepanel_close(driver, timeout=min(12.0, timeout_total * 0.12))
        _mark_stage("save_audience")

        if not _click_ad_entry(driver, emit, timeout=6.0):
            logger.warning("DemandGen step10: не удалось перейти к объявлению Ad 1.")
        else:
            _mark_stage("open_ad1")

        final_name = _get_input_value(driver, audience_input) if not audience_saved else audience_name
        final_gender = applied_gender or _snapshot_gender_selection(driver)
        final_age_from, final_age_to = (
            (applied_age_from, applied_age_to)
            if audience_saved
            else _snapshot_age_bounds(driver)
        )

    duration_ms = int((time.time() - started) * 1000)
    logger.info(
        "step10 DemandGen done (%d ms). Audience='%s' (source=%s) saved=%s",
        duration_ms,
        audience_name,
        decision.get("source"),
        audience_saved,
    )
    if timings:
        breakdown = ", ".join(f"{name}={dur:.0f}ms" for name, dur in timings)
        logger.info("DemandGen step10 timing breakdown: %s", breakdown)

    changes_applied = (
        (audience_name.strip() != (initial_name or "").strip())
        or gender_changed
        or age_changed
    )

    return {
        "audience_name": final_name,
        "audience_decision_source": decision.get("source"),
        "audience_decision_reason": decision.get("reason"),
        "audience_gender_selected": final_gender,
        "audience_gender_reason": decision.get("gender", {}).get("reason"),
        "audience_age_range": {
            "from": decision.get("age", {}).get("from"),
            "to": decision.get("age", {}).get("to"),
            "include_unknown": False,
            "applied_from": final_age_from,
            "applied_to": final_age_to,
            "reason": decision.get("age", {}).get("reason"),
        },
        "audience_parental_status": {
            "include": [],
            "include_unknown": False,
            "reason": "Not configured in DemandGen step10 rewrite.",
        },
        "audience_income_segments": {
            "segments": [],
            "include_unknown": False,
            "from": None,
            "to": None,
            "applied_from": None,
            "applied_to": None,
            "reason": "Not configured in DemandGen step10 rewrite.",
        },
        "duration_ms": duration_ms,
        "audience_saved": audience_saved,
        "audience_changes_applied": changes_applied,
        "timing_breakdown": [{"stage": name, "duration_ms": dur} for name, dur in timings],
    }


def run(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Совместимость с автозапуском: examples.steps_demand_gen.step10:run."""
    return run_step10(*args, **kwargs)
