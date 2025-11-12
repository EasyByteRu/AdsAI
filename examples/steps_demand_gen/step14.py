# -*- coding: utf-8 -*-
"""
examples/steps_demand_gen/step14.py

Шаг 14 (Demand Gen) — заполнение текстовых креативов (Headlines и Descriptions).

Контракт:
- run_step14(driver, *, mode, seed_assets, provided_assets, business_name, usp, site_url, ...)
- run(...) — совместимость.

Режимы:
  • ai_only — полная генерация через LLM
  • inspired — генерация на основе примеров
  • manual — использование готовых текстов

Лимиты:
  • Headline: до 5 штук, максимум 40 символов
  • Description: до 5 штук, максимум 90 символов
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from selenium.webdriver.common.by import By  # type: ignore
from selenium.webdriver.remote.webdriver import WebDriver, WebElement  # type: ignore

# Базовые утилиты шага
from examples.steps import step4 as base_step4  # type: ignore

try:
    from ads_ai.llm.gemini import GeminiClient  # type: ignore
except Exception:  # pragma: no cover
    GeminiClient = None  # type: ignore


# --------------------------------------------------------------------------------------
#                                    ЛОГГЕР
# --------------------------------------------------------------------------------------

logger = logging.getLogger("ads_ai.gads.step14")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

_emit = base_step4._emit  # type: ignore
_dismiss_soft_dialogs = base_step4._dismiss_soft_dialogs  # type: ignore
_is_interactable = base_step4._is_interactable  # type: ignore


# --------------------------------------------------------------------------------------
#                                   КОНСТАНТЫ
# --------------------------------------------------------------------------------------

LLM_MODEL = os.getenv("GEMINI_MODEL", "models/gemini-2.0-flash")
STEP14_DISABLE_LLM = str(os.getenv("ADS_AI_STEP14_DISABLE_LLM", "")).strip().lower() in {
    "1", "true", "yes", "on"
}

MAX_HEADLINES = 5
MAX_DESCRIPTIONS = 5
HEADLINE_MAX_LENGTH = 40
DESCRIPTION_MAX_LENGTH = 90

# Селекторы
HEADLINES_EDITOR_SELECTOR = 'multi-asset-editor[debugid="headlines"]'
DESCRIPTIONS_EDITOR_SELECTOR = 'multi-asset-editor[debugid="descriptions"]'


# --------------------------------------------------------------------------------------
#                                   ИСКЛЮЧЕНИЯ
# --------------------------------------------------------------------------------------

class Step14Error(RuntimeError):
    pass

class UiNotFound(Step14Error):
    pass


# --------------------------------------------------------------------------------------
#                                  УТИЛИТЫ DOM
# --------------------------------------------------------------------------------------

@dataclass
class TimerMarks:
    _anchor: float = field(default_factory=time.time)
    records: List[Tuple[str, int]] = field(default_factory=list)

    def mark(self, label: str) -> None:
        now = time.time()
        self.records.append((label, int((now - self._anchor) * 1000)))
        self._anchor = now


def _js_click(driver: WebDriver, element: WebElement) -> bool:
    """Клик по элементу через JavaScript."""
    try:
        driver.execute_script(
            "try{arguments[0].scrollIntoView({block:'center',inline:'center'});}catch(_){}", 
            element
        )
        element.click()
        return True
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", element)
            return True
        except Exception:
            return False


def _wait_for_element(
    driver: WebDriver,
    selector: str,
    *,
    timeout: float = 5.0,
    require_visible: bool = True,
    parent: Optional[WebElement] = None,
) -> Optional[WebElement]:
    """Ожидание появления элемента."""
    deadline = time.time() + max(0.5, timeout)
    scope = parent if parent else driver
    
    while time.time() < deadline:
        try:
            els = scope.find_elements(By.CSS_SELECTOR, selector)
            for el in els:
                if not require_visible or _is_interactable(driver, el):
                    return el
        except Exception:
            pass
        time.sleep(0.15)
    return None


def _find_input_by_label(
    driver: WebDriver,
    parent: WebElement,
    aria_label: str,
    *,
    index: int = 0,
) -> Optional[WebElement]:
    """Поиск input по aria-label с заданным индексом."""
    try:
        inputs = parent.find_elements(By.CSS_SELECTOR, f'input[aria-label="{aria_label}"]')
        if inputs and 0 <= index < len(inputs):
            return inputs[index]
    except Exception:
        pass
    return None


def _get_all_inputs_by_label(
    parent: WebElement,
    aria_label: str,
) -> List[WebElement]:
    """Получить все input'ы с заданным aria-label."""
    try:
        return parent.find_elements(By.CSS_SELECTOR, f'input[aria-label="{aria_label}"]')
    except Exception:
        return []


def _find_add_button(
    driver: WebDriver,
    parent: WebElement,
    button_text: str,
) -> Optional[WebElement]:
    """Поиск кнопки добавления по тексту."""
    try:
        # Ищем через JavaScript для надёжности
        return driver.execute_script(
            """
            const parent = arguments[0];
            const text = arguments[1].toLowerCase();
            const buttons = parent.querySelectorAll('material-button.add-asset-button, button.add-asset-button, [role="button"].add-asset-button');
            for (const btn of buttons) {
                const btnText = ((btn.innerText || btn.textContent || '') + ' ' + (btn.getAttribute('aria-label') || '')).toLowerCase();
                if (btnText.includes(text)) {
                    const cs = getComputedStyle(btn);
                    const r = btn.getBoundingClientRect();
                    if (cs.display !== 'none' && cs.visibility !== 'hidden' && r.width > 10 && r.height > 10) {
                        return btn;
                    }
                }
            }
            return null;
            """,
            parent,
            button_text,
        )
    except Exception:
        return None


def _set_input_value(
    driver: WebDriver,
    input_el: WebElement,
    value: str,
    *,
    verify: bool = True,
) -> bool:
    """Установка значения в input с проверкой."""
    try:
        # Очистка
        driver.execute_script("arguments[0].value = '';", input_el)
        input_el.clear()
        
        # Установка через send_keys
        input_el.send_keys(value)
        
        # Триггер событий
        driver.execute_script(
            """
            const el = arguments[0];
            el.dispatchEvent(new Event('input', {bubbles: true}));
            el.dispatchEvent(new Event('change', {bubbles: true}));
            el.blur();
            """,
            input_el,
        )
        
        if verify:
            time.sleep(0.2)
            actual = input_el.get_attribute("value") or ""
            if actual.strip() != value.strip():
                logger.warning(
                    "step14: значение не совпало: ожидалось '%s', получено '%s'",
                    value[:50],
                    actual[:50],
                )
                return False
        
        return True
    except Exception as exc:
        logger.warning("step14: не удалось установить значение: %s", exc)
        return False


# --------------------------------------------------------------------------------------
#                              ГЕНЕРАЦИЯ ТЕКСТОВ
# --------------------------------------------------------------------------------------

def _truncate_text(text: str, max_length: int) -> str:
    """Обрезка текста до максимальной длины."""
    s = str(text or "").strip()
    if len(s) <= max_length:
        return s
    return s[:max_length].rsplit(" ", 1)[0].rstrip(".,;:!?")


def _fallback_headlines(
    *,
    business_name: Optional[str],
    usp: Optional[str],
    count: int,
) -> List[str]:
    """Резервные заголовки."""
    base = (business_name or usp or "Great Product").strip()
    pool = [
        f"{base[:30]} – Try Now",
        f"Discover {base[:25]}",
        f"Best {base[:28]} Deals",
        f"{base[:32]} For You",
        f"Shop {base[:30]} Today",
    ]
    result = []
    for text in pool[:count]:
        result.append(_truncate_text(text, HEADLINE_MAX_LENGTH))
    return result


def _fallback_descriptions(
    *,
    business_name: Optional[str],
    usp: Optional[str],
    count: int,
) -> List[str]:
    """Резервные описания."""
    base = (usp or business_name or "Quality products and services").strip()
    pool = [
        f"{base[:80]}. Order now!",
        f"Explore our {base[:65]} collection",
        f"{base[:70]}. Fast delivery",
        f"Top-rated {base[:65]}. Shop today",
        f"{base[:75]}. Limited offer",
    ]
    result = []
    for text in pool[:count]:
        result.append(_truncate_text(text, DESCRIPTION_MAX_LENGTH))
    return result


def _llm_generate_headlines(
    *,
    count: int,
    business_name: Optional[str],
    usp: Optional[str],
    site_url: Optional[str],
    seed_examples: Optional[List[str]] = None,
) -> List[str]:
    """Генерация заголовков через LLM."""
    if STEP14_DISABLE_LLM or GeminiClient is None:
        logger.warning("step14: LLM отключена — fallback заголовки")
        return _fallback_headlines(business_name=business_name, usp=usp, count=count)
    
    instructions = (
        f"Generate {count} catchy headlines for Google Ads Demand Gen campaign. "
        f"Each headline MUST be ≤{HEADLINE_MAX_LENGTH} characters. "
        "Return ONLY JSON {\"headlines\":[\"...\", ...]}. "
        "No quotes inside headlines. English only."
    )
    
    context_parts = []
    if business_name:
        context_parts.append(f"Business: {business_name}")
    if usp:
        context_parts.append(f"USP: {usp}")
    if site_url:
        context_parts.append(f"Website: {site_url}")
    if seed_examples:
        examples = ", ".join(f'"{ex}"' for ex in seed_examples[:3])
        context_parts.append(f"Style examples: {examples}")
    
    context = ". ".join(context_parts) if context_parts else "Generic brand."
    payload = f"{instructions}\n\nContext: {context}"
    
    try:
        client = GeminiClient(LLM_MODEL, temperature=0.7, retries=1)
        resp = client.generate_json(payload)
        arr = (resp or {}).get("headlines")
        
        result: List[str] = []
        if isinstance(arr, list):
            for item in arr[:count]:
                text = str(item or "").strip().strip('"').strip("'")
                if text:
                    result.append(_truncate_text(text, HEADLINE_MAX_LENGTH))
        
        if result:
            logger.info("step14: LLM сгенерировала %d заголовков", len(result))
            return result
        
        logger.warning("step14: LLM вернула пустой ответ — fallback")
        return _fallback_headlines(business_name=business_name, usp=usp, count=count)
    
    except Exception as exc:
        logger.warning("step14: LLM генерация заголовков не удалась: %s", exc)
        return _fallback_headlines(business_name=business_name, usp=usp, count=count)


def _llm_generate_descriptions(
    *,
    count: int,
    business_name: Optional[str],
    usp: Optional[str],
    site_url: Optional[str],
    seed_examples: Optional[List[str]] = None,
) -> List[str]:
    """Генерация описаний через LLM."""
    if STEP14_DISABLE_LLM or GeminiClient is None:
        logger.warning("step14: LLM отключена — fallback описания")
        return _fallback_descriptions(business_name=business_name, usp=usp, count=count)
    
    instructions = (
        f"Generate {count} compelling descriptions for Google Ads Demand Gen campaign. "
        f"Each description MUST be ≤{DESCRIPTION_MAX_LENGTH} characters. "
        "Return ONLY JSON {\"descriptions\":[\"...\", ...]}. "
        "No quotes inside descriptions. English only."
    )
    
    context_parts = []
    if business_name:
        context_parts.append(f"Business: {business_name}")
    if usp:
        context_parts.append(f"USP: {usp}")
    if site_url:
        context_parts.append(f"Website: {site_url}")
    if seed_examples:
        examples = ", ".join(f'"{ex}"' for ex in seed_examples[:3])
        context_parts.append(f"Style examples: {examples}")
    
    context = ". ".join(context_parts) if context_parts else "Generic brand."
    payload = f"{instructions}\n\nContext: {context}"
    
    try:
        client = GeminiClient(LLM_MODEL, temperature=0.7, retries=1)
        resp = client.generate_json(payload)
        arr = (resp or {}).get("descriptions")
        
        result: List[str] = []
        if isinstance(arr, list):
            for item in arr[:count]:
                text = str(item or "").strip().strip('"').strip("'")
                if text:
                    result.append(_truncate_text(text, DESCRIPTION_MAX_LENGTH))
        
        if result:
            logger.info("step14: LLM сгенерировала %d описаний", len(result))
            return result
        
        logger.warning("step14: LLM вернула пустой ответ — fallback")
        return _fallback_descriptions(business_name=business_name, usp=usp, count=count)
    
    except Exception as exc:
        logger.warning("step14: LLM генерация описаний не удалась: %s", exc)
        return _fallback_descriptions(business_name=business_name, usp=usp, count=count)


# --------------------------------------------------------------------------------------
#                              ЗАПОЛНЕНИЕ ПОЛЕЙ
# --------------------------------------------------------------------------------------

def _fill_text_fields(
    driver: WebDriver,
    editor_selector: str,
    aria_label: str,
    button_text: str,
    texts: List[str],
    max_fields: int,
    field_type: str,
) -> List[str]:
    """
    Универсальная функция заполнения текстовых полей (Headlines или Descriptions).
    
    Args:
        driver: WebDriver
        editor_selector: CSS селектор блока редактора
        aria_label: aria-label для input'ов (например, "Headline")
        button_text: текст кнопки добавления (например, "Headline")
        texts: список текстов для заполнения
        max_fields: максимальное количество полей
        field_type: тип поля для логирования ("заголовки" или "описания")
    
    Returns:
        Список реально записанных текстов
    """
    if not texts:
        logger.info("step14: нет текстов для заполнения (%s)", field_type)
        return []
    
    # Найти блок редактора
    editor = _wait_for_element(driver, editor_selector, timeout=5.0, require_visible=False)
    if not editor:
        raise UiNotFound(f"Блок {editor_selector} не найден")
    
    logger.info(
        "step14: заполнение %s | тексты=%d, макс полей=%d",
        field_type,
        len(texts),
        max_fields,
    )
    
    filled: List[str] = []
    
    # Заполнить первое поле
    first_input = _find_input_by_label(driver, editor, aria_label, index=0)
    if not first_input:
        raise UiNotFound(f"Первый input ({aria_label}) не найден")
    
    if _set_input_value(driver, first_input, texts[0], verify=True):
        filled.append(texts[0])
        logger.info("step14: %s [1/%d] = '%s'", field_type, len(texts), texts[0][:50])
    else:
        logger.warning("step14: не удалось заполнить первое поле (%s)", field_type)
    
    # Заполнить остальные поля
    for i, text in enumerate(texts[1:max_fields], start=2):
        # Найти кнопку добавления
        add_button = _find_add_button(driver, editor, button_text)
        if not add_button:
            logger.warning(
                "step14: кнопка добавления не найдена после %d полей (%s)",
                i - 1,
                field_type,
            )
            break
        
        # Проверить, доступна ли кнопка
        try:
            is_disabled = driver.execute_script(
                """
                const btn = arguments[0];
                return btn.hasAttribute('disabled') || 
                       btn.getAttribute('aria-disabled') === 'true' ||
                       btn.classList.contains('is-disabled');
                """,
                add_button,
            )
            if is_disabled:
                logger.warning(
                    "step14: кнопка добавления неактивна после %d полей (%s)",
                    i - 1,
                    field_type,
                )
                break
        except Exception:
            pass
        
        # Кликнуть для добавления нового поля
        if not _js_click(driver, add_button):
            logger.warning(
                "step14: не удалось кликнуть кнопку добавления (%s)",
                field_type,
            )
            break
        
        time.sleep(0.3)
        
        # Найти новое поле (должно быть последним)
        all_inputs = _get_all_inputs_by_label(editor, aria_label)
        if len(all_inputs) < i:
            logger.warning(
                "step14: новое поле не появилось (ожидалось %d, найдено %d) (%s)",
                i,
                len(all_inputs),
                field_type,
            )
            break
        
        new_input = all_inputs[-1]
        
        # Заполнить новое поле
        if _set_input_value(driver, new_input, text, verify=True):
            filled.append(text)
            logger.info("step14: %s [%d/%d] = '%s'", field_type, i, len(texts), text[:50])
        else:
            logger.warning(
                "step14: не удалось заполнить поле %d (%s)",
                i,
                field_type,
            )
    
    logger.info("step14: заполнено %s: %d из %d", field_type, len(filled), len(texts))
    return filled


# --------------------------------------------------------------------------------------
#                                 ОСНОВНАЯ ЛОГИКА
# --------------------------------------------------------------------------------------

def run_step14(
    driver: WebDriver,
    *,
    mode: str = "ai_only",
    seed_assets: Optional[Dict[str, Any]] = None,
    provided_assets: Optional[Dict[str, Any]] = None,
    business_name: Optional[str] = None,
    usp: Optional[str] = None,
    site_url: Optional[str] = None,
    emit: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """
    Заполнение текстовых креативов (Headlines и Descriptions).
    
    Args:
        driver: WebDriver
        mode: режим работы ("ai_only", "inspired", "manual")
        seed_assets: словарь с примерами для режима "inspired"
            {"headlines": [...], "descriptions": [...]}
        provided_assets: словарь с готовыми текстами для режима "manual"
            {"headlines": [...], "descriptions": [...]}
        business_name: название бизнеса
        usp: УТП
        site_url: URL сайта
        emit: функция для вывода сообщений
    
    Returns:
        Словарь с результатом:
        {
            "mode": str,
            "headlines": {"texts": [...], "count": int},
            "descriptions": {"texts": [...], "count": int},
            "duration_ms": int
        }
    """
    started = time.time()
    tm = TimerMarks()
    
    _dismiss_soft_dialogs(driver, budget_ms=600)
    
    # Нормализация режима
    normalized_mode = (mode or "ai_only").strip().lower()
    if normalized_mode in {"ai", "auto", "full_ai", "auto_generate"}:
        normalized_mode = "ai_only"
    elif normalized_mode in {"inspired", "guided", "hybrid", "prompt_guided", "mix"}:
        normalized_mode = "inspired"
    elif normalized_mode in {"manual", "provided"}:
        normalized_mode = "manual"
    else:
        normalized_mode = "ai_only"
    
    _emit(emit, f"Шаг 14: заполнение текстов ({normalized_mode})")
    logger.info(
        "step14 start | mode=%s | business=%s | site=%s",
        normalized_mode,
        business_name or "-",
        site_url or "-",
    )
    
    # Подготовка текстов в зависимости от режима
    headlines: List[str] = []
    descriptions: List[str] = []
    
    if normalized_mode == "ai_only":
        _emit(emit, "Генерация заголовков и описаний через LLM")
        headlines = _llm_generate_headlines(
            count=MAX_HEADLINES,
            business_name=business_name,
            usp=usp,
            site_url=site_url,
        )
        descriptions = _llm_generate_descriptions(
            count=MAX_DESCRIPTIONS,
            business_name=business_name,
            usp=usp,
            site_url=site_url,
        )
        tm.mark("llm_generation")
    
    elif normalized_mode == "inspired":
        seed_headlines = []
        seed_descriptions = []
        
        if isinstance(seed_assets, dict):
            raw_h = seed_assets.get("headlines")
            if isinstance(raw_h, list):
                seed_headlines = [str(x).strip() for x in raw_h if str(x).strip()]
            
            raw_d = seed_assets.get("descriptions")
            if isinstance(raw_d, list):
                seed_descriptions = [str(x).strip() for x in raw_d if str(x).strip()]
        
        _emit(emit, f"Генерация на основе примеров (заголовки: {len(seed_headlines)}, описания: {len(seed_descriptions)})")
        
        headlines = _llm_generate_headlines(
            count=MAX_HEADLINES,
            business_name=business_name,
            usp=usp,
            site_url=site_url,
            seed_examples=seed_headlines,
        )
        descriptions = _llm_generate_descriptions(
            count=MAX_DESCRIPTIONS,
            business_name=business_name,
            usp=usp,
            site_url=site_url,
            seed_examples=seed_descriptions,
        )
        tm.mark("inspired_generation")
    
    else:  # manual
        _emit(emit, "Использование предоставленных текстов")
        
        if isinstance(provided_assets, dict):
            raw_h = provided_assets.get("headlines")
            if isinstance(raw_h, list):
                headlines = [
                    _truncate_text(str(x), HEADLINE_MAX_LENGTH)
                    for x in raw_h
                    if str(x).strip()
                ][:MAX_HEADLINES]
            
            raw_d = provided_assets.get("descriptions")
            if isinstance(raw_d, list):
                descriptions = [
                    _truncate_text(str(x), DESCRIPTION_MAX_LENGTH)
                    for x in raw_d
                    if str(x).strip()
                ][:MAX_DESCRIPTIONS]
        
        if not headlines:
            logger.warning("step14: в manual режиме не предоставлены заголовки — fallback")
            headlines = _fallback_headlines(
                business_name=business_name,
                usp=usp,
                count=MAX_HEADLINES,
            )
        
        if not descriptions:
            logger.warning("step14: в manual режиме не предоставлены описания — fallback")
            descriptions = _fallback_descriptions(
                business_name=business_name,
                usp=usp,
                count=MAX_DESCRIPTIONS,
            )
        
        tm.mark("manual_preparation")
    
    # Заполнение Headlines
    _emit(emit, f"Заполнение заголовков ({len(headlines)} шт.)")
    filled_headlines = _fill_text_fields(
        driver=driver,
        editor_selector=HEADLINES_EDITOR_SELECTOR,
        aria_label="Headline",
        button_text="headline",
        texts=headlines,
        max_fields=MAX_HEADLINES,
        field_type="заголовки",
    )
    tm.mark("fill_headlines")
    
    # Заполнение Descriptions
    _emit(emit, f"Заполнение описаний ({len(descriptions)} шт.)")
    filled_descriptions = _fill_text_fields(
        driver=driver,
        editor_selector=DESCRIPTIONS_EDITOR_SELECTOR,
        aria_label="Description",
        button_text="description",
        texts=descriptions,
        max_fields=MAX_DESCRIPTIONS,
        field_type="описания",
    )
    tm.mark("fill_descriptions")
    
    duration_ms = int((time.time() - started) * 1000)
    
    logger.info(
        "step14 done (%d ms) | заголовки=%d, описания=%d | mode=%s",
        duration_ms,
        len(filled_headlines),
        len(filled_descriptions),
        normalized_mode,
    )
    
    # Вывод итогов
    _emit(emit, f"Заголовки: {', '.join(h[:30] + '...' if len(h) > 30 else h for h in filled_headlines)}")
    _emit(emit, f"Описания: {', '.join(d[:40] + '...' if len(d) > 40 else d for d in filled_descriptions)}")
    
    return {
        "mode": normalized_mode,
        "headlines": {
            "texts": filled_headlines,
            "count": len(filled_headlines),
        },
        "descriptions": {
            "texts": filled_descriptions,
            "count": len(filled_descriptions),
        },
        "duration_ms": duration_ms,
        "timer_marks": tm.records,
    }


def run(driver: WebDriver, **kwargs) -> Dict[str, Any]:
    """Точка входа для обратной совместимости."""
    return run_step14(driver, **kwargs)