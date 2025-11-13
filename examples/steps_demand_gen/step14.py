# -*- coding: utf-8 -*-
"""
examples/steps_demand_gen/step14.py

Шаг 14 (Demand Gen) — заполнение текстовых креативов.

Функциональность:
  • Headlines — заголовки кампании
  • Descriptions — описания кампании
  • Call to Action — язык и текст кнопки
  • Business Name — название бизнеса

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
  • Business Name: максимум 25 символов

Call to Action:
  • Автоматическое определение языка на основе контента (кириллица → Russian, и т.д.)
  • Выбор подходящего текста CTA через LLM из списка доступных опций
  • Поддержка 45+ языков
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

# Call to Action константы
BUSINESS_NAME_MAX_LENGTH = 25

# Доступные языки для Call to Action
CTA_LANGUAGES = [
    "Arabic", "Bulgarian", "Catalan", "Chinese (Hong Kong)", "Chinese (Simplified)",
    "Chinese (Traditional)", "Croatian", "Czech", "Danish", "Dutch", "English",
    "English (Australia)", "English (United Kingdom)", "English (United States)",
    "Estonian", "Filipino", "Finnish", "French", "German", "Greek", "Hebrew",
    "Hindi", "Hungarian", "Indonesian", "Italian", "Japanese", "Korean", "Latvian",
    "Lithuanian", "Malay", "Norwegian", "Polish", "Portuguese (Brazil)",
    "Portuguese (Portugal)", "Romanian", "Russian", "Serbian", "Slovak", "Slovenian",
    "Spanish (Latin America)", "Spanish (Spain)", "Swedish", "Thai", "Turkish",
    "Ukrainian", "Vietnamese"
]

# Доступные тексты Call to Action
CTA_TEXTS = [
    "Apply now", "Book now", "Contact us", "Download", "Learn more",
    "Visit site", "Shop now", "Sign up", "Get quote", "Subscribe", "See more"
]


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
    languages: Optional[List[str]] = None,
    seed_examples: Optional[List[str]] = None,
) -> List[str]:
    """Генерация заголовков через LLM."""
    if STEP14_DISABLE_LLM or GeminiClient is None:
        logger.warning("step14: LLM отключена — fallback заголовки")
        return _fallback_headlines(business_name=business_name, usp=usp, count=count)

    # Определяем язык для генерации
    target_language = "English"
    if languages:
        first_lang = next((lang for lang in languages if lang and lang.strip()), None)
        if first_lang:
            # Преобразуем код или название в читаемый язык для промпта
            lang_lower = first_lang.strip().lower()
            if lang_lower in ["ru", "russian"]:
                target_language = "Russian"
            elif lang_lower in ["de", "german"]:
                target_language = "German"
            elif lang_lower in ["fr", "french"]:
                target_language = "French"
            elif lang_lower in ["es", "spanish"]:
                target_language = "Spanish"
            elif lang_lower in ["it", "italian"]:
                target_language = "Italian"
            elif lang_lower in ["pt", "portuguese"]:
                target_language = "Portuguese"
            elif lang_lower in ["pl", "polish"]:
                target_language = "Polish"
            elif lang_lower in ["tr", "turkish"]:
                target_language = "Turkish"
            elif lang_lower in ["uk", "ua", "ukrainian"]:
                target_language = "Ukrainian"
            elif lang_lower in ["zh", "chinese"]:
                target_language = "Chinese"
            elif lang_lower in ["ja", "japanese"]:
                target_language = "Japanese"
            elif lang_lower in ["ko", "korean"]:
                target_language = "Korean"
            elif lang_lower in ["ar", "arabic"]:
                target_language = "Arabic"
            elif lang_lower in ["hi", "hindi"]:
                target_language = "Hindi"
            elif lang_lower in ["th", "thai"]:
                target_language = "Thai"
            elif lang_lower in ["vi", "vietnamese"]:
                target_language = "Vietnamese"
            elif lang_lower in ["id", "indonesian"]:
                target_language = "Indonesian"
            elif lang_lower in ["nl", "dutch"]:
                target_language = "Dutch"
            elif lang_lower in ["sv", "swedish"]:
                target_language = "Swedish"
            elif lang_lower in ["no", "norwegian"]:
                target_language = "Norwegian"
            elif lang_lower in ["da", "danish"]:
                target_language = "Danish"
            elif lang_lower in ["fi", "finnish"]:
                target_language = "Finnish"
            # Добавляем другие языки по необходимости
            logger.info("step14: язык генерации заголовков: %s (из %s)", target_language, first_lang)
    else:
        logger.info("step14: язык не передан, используем English для заголовков")

    instructions = (
        f"Generate {count} catchy headlines for Google Ads Demand Gen campaign. "
        f"Each headline MUST be ≤{HEADLINE_MAX_LENGTH} characters. "
        "Return ONLY JSON {\"headlines\":[\"...\", ...]}. "
        f"No quotes inside headlines. Generate in {target_language} language."
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
    languages: Optional[List[str]] = None,
    seed_examples: Optional[List[str]] = None,
) -> List[str]:
    """Генерация описаний через LLM."""
    if STEP14_DISABLE_LLM or GeminiClient is None:
        logger.warning("step14: LLM отключена — fallback описания")
        return _fallback_descriptions(business_name=business_name, usp=usp, count=count)

    # Определяем язык для генерации
    target_language = "English"
    if languages:
        first_lang = next((lang for lang in languages if lang and lang.strip()), None)
        if first_lang:
            # Преобразуем код или название в читаемый язык для промпта
            lang_lower = first_lang.strip().lower()
            if lang_lower in ["ru", "russian"]:
                target_language = "Russian"
            elif lang_lower in ["de", "german"]:
                target_language = "German"
            elif lang_lower in ["fr", "french"]:
                target_language = "French"
            elif lang_lower in ["es", "spanish"]:
                target_language = "Spanish"
            elif lang_lower in ["it", "italian"]:
                target_language = "Italian"
            elif lang_lower in ["pt", "portuguese"]:
                target_language = "Portuguese"
            elif lang_lower in ["pl", "polish"]:
                target_language = "Polish"
            elif lang_lower in ["tr", "turkish"]:
                target_language = "Turkish"
            elif lang_lower in ["uk", "ua", "ukrainian"]:
                target_language = "Ukrainian"
            elif lang_lower in ["zh", "chinese"]:
                target_language = "Chinese"
            elif lang_lower in ["ja", "japanese"]:
                target_language = "Japanese"
            elif lang_lower in ["ko", "korean"]:
                target_language = "Korean"
            elif lang_lower in ["ar", "arabic"]:
                target_language = "Arabic"
            elif lang_lower in ["hi", "hindi"]:
                target_language = "Hindi"
            elif lang_lower in ["th", "thai"]:
                target_language = "Thai"
            elif lang_lower in ["vi", "vietnamese"]:
                target_language = "Vietnamese"
            elif lang_lower in ["id", "indonesian"]:
                target_language = "Indonesian"
            elif lang_lower in ["nl", "dutch"]:
                target_language = "Dutch"
            elif lang_lower in ["sv", "swedish"]:
                target_language = "Swedish"
            elif lang_lower in ["no", "norwegian"]:
                target_language = "Norwegian"
            elif lang_lower in ["da", "danish"]:
                target_language = "Danish"
            elif lang_lower in ["fi", "finnish"]:
                target_language = "Finnish"
            # Добавляем другие языки по необходимости
            logger.info("step14: язык генерации описаний: %s (из %s)", target_language, first_lang)
    else:
        logger.info("step14: язык не передан, используем English для описаний")

    instructions = (
        f"Generate {count} compelling descriptions for Google Ads Demand Gen campaign. "
        f"Each description MUST be ≤{DESCRIPTION_MAX_LENGTH} characters. "
        "Return ONLY JSON {\"descriptions\":[\"...\", ...]}. "
        f"No quotes inside descriptions. Generate in {target_language} language."
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
#                         ОПРЕДЕЛЕНИЕ ЯЗЫКА И CALL TO ACTION
# --------------------------------------------------------------------------------------

# Маппинг языковых кодов и названий на полные названия для CTA
LANG_CODE_TO_CTA_LANGUAGE = {
    # Language codes (2-3 chars)
    "en": "English (United States)",
    "ru": "Russian",
    "de": "German",
    "fr": "French",
    "es": "Spanish (Spain)",
    "pt": "Portuguese (Portugal)",
    "it": "Italian",
    "pl": "Polish",
    "tr": "Turkish",
    "uk": "Ukrainian",
    "ua": "Ukrainian",
    "zh": "Chinese (Simplified)",
    "ja": "Japanese",
    "ko": "Korean",
    "th": "Thai",
    "vi": "Vietnamese",
    "ar": "Arabic",
    "hi": "Hindi",
    "id": "Indonesian",
    "nl": "Dutch",
    "sv": "Swedish",
    "no": "Norwegian",
    "da": "Danish",
    "fi": "Finnish",
    "cs": "Czech",
    "sk": "Slovak",
    "hu": "Hungarian",
    "ro": "Romanian",
    "bg": "Bulgarian",
    "hr": "Croatian",
    "sr": "Serbian",
    "sl": "Slovenian",
    "et": "Estonian",
    "lv": "Latvian",
    "lt": "Lithuanian",
    "el": "Greek",
    "he": "Hebrew",
    "ca": "Catalan",
    "ms": "Malay",
    "fil": "Filipino",
    # Simple language names (for fallback)
    "english": "English (United States)",
    "russian": "Russian",
    "german": "German",
    "french": "French",
    "spanish": "Spanish (Spain)",
    "portuguese": "Portuguese (Portugal)",
    "italian": "Italian",
    "polish": "Polish",
    "turkish": "Turkish",
    "ukrainian": "Ukrainian",
    "chinese": "Chinese (Simplified)",
    "japanese": "Japanese",
    "korean": "Korean",
    "thai": "Thai",
    "vietnamese": "Vietnamese",
    "arabic": "Arabic",
    "hindi": "Hindi",
    "indonesian": "Indonesian",
    "dutch": "Dutch",
    "swedish": "Swedish",
    "norwegian": "Norwegian",
    "danish": "Danish",
    "finnish": "Finnish",
    "czech": "Czech",
    "slovak": "Slovak",
    "hungarian": "Hungarian",
    "romanian": "Romanian",
    "bulgarian": "Bulgarian",
    "croatian": "Croatian",
    "serbian": "Serbian",
    "slovenian": "Slovenian",
    "estonian": "Estonian",
    "latvian": "Latvian",
    "lithuanian": "Lithuanian",
    "greek": "Greek",
    "hebrew": "Hebrew",
    "catalan": "Catalan",
    "malay": "Malay",
    "filipino": "Filipino",
}


def _map_language_to_cta(lang: str) -> str:
    """
    Преобразовать язык (код или название) в полное название для CTA dropdown.
    Например: "en" → "English (United States)", "ru" → "Russian"
    """
    if not lang:
        return "English (United States)"

    # Проверяем точное совпадение (если уже полное название)
    lang_normalized = lang.strip()
    if lang_normalized in CTA_LANGUAGES:
        return lang_normalized

    # Проверяем по коду или простому названию
    lang_lower = lang_normalized.lower()
    if lang_lower in LANG_CODE_TO_CTA_LANGUAGE:
        return LANG_CODE_TO_CTA_LANGUAGE[lang_lower]

    # Проверяем, начинается ли название из CTA_LANGUAGES с этого языка
    # Например: "English" → "English (United States)"
    for cta_lang in CTA_LANGUAGES:
        if cta_lang.lower().startswith(lang_lower):
            return cta_lang

    return "English (United States)"


def _detect_language_from_context(
    *,
    languages: Optional[List[str]] = None,
    business_name: Optional[str] = None,
    usp: Optional[str] = None,
    headlines: Optional[List[str]] = None,
    descriptions: Optional[List[str]] = None,
) -> str:
    """
    Определить язык для CTA на основе переданных языков из create_companies.py.
    Если languages не передан, пытаемся определить по контексту (fallback).
    Возвращает полное название языка из CTA_LANGUAGES.
    """
    # Приоритет 1: используем переданные языки из create_companies.py
    if languages:
        first_lang = next((lang for lang in languages if lang and lang.strip()), None)
        if first_lang:
            cta_language = _map_language_to_cta(first_lang)
            logger.info("step14: используем язык из параметров: %s → %s", first_lang, cta_language)
            return cta_language

    # Приоритет 2 (fallback): определяем по контексту текста (упрощенная эвристика)
    all_text = " ".join(filter(None, [
        business_name or "",
        usp or "",
        " ".join(headlines or []),
        " ".join(descriptions or []),
    ])).strip()

    if all_text:
        # Простая эвристика по символам (только основные языки)
        cyrillic_chars = sum(1 for c in all_text if '\u0400' <= c <= '\u04FF')
        if cyrillic_chars > len(all_text) * 0.1:
            logger.info("step14: обнаружена кириллица в тексте, используем Russian")
            return "Russian"

        chinese_chars = sum(1 for c in all_text if '\u4E00' <= c <= '\u9FFF')
        if chinese_chars > 0:
            logger.info("step14: обнаружены китайские символы в тексте, используем Chinese (Simplified)")
            return "Chinese (Simplified)"

        japanese_chars = sum(1 for c in all_text if '\u3040' <= c <= '\u309F' or '\u30A0' <= c <= '\u30FF')
        if japanese_chars > 0:
            logger.info("step14: обнаружены японские символы в тексте, используем Japanese")
            return "Japanese"

        arabic_chars = sum(1 for c in all_text if '\u0600' <= c <= '\u06FF')
        if arabic_chars > len(all_text) * 0.1:
            logger.info("step14: обнаружены арабские символы в тексте, используем Arabic")
            return "Arabic"

    # По умолчанию English (United States)
    logger.info("step14: не удалось определить язык, используем English (United States)")
    return "English (United States)"


def _llm_select_cta_text(
    *,
    business_name: Optional[str] = None,
    usp: Optional[str] = None,
    site_url: Optional[str] = None,
    headlines: Optional[List[str]] = None,
) -> str:
    """
    Выбрать подходящий текст Call to Action через LLM.
    """
    if STEP14_DISABLE_LLM or GeminiClient is None:
        logger.warning("step14: LLM отключена — используем Learn more по умолчанию")
        return "Learn more"

    instructions = (
        f"Select the MOST appropriate Call-to-Action text from this list: {', '.join(CTA_TEXTS)}. "
        "Return ONLY JSON {\"cta_text\": \"...\"}. "
        "Choose based on the business context and campaign goal."
    )

    context_parts = []
    if business_name:
        context_parts.append(f"Business: {business_name}")
    if usp:
        context_parts.append(f"USP: {usp}")
    if site_url:
        context_parts.append(f"Website: {site_url}")
    if headlines:
        context_parts.append(f"Headlines: {', '.join(headlines[:2])}")

    context = ". ".join(context_parts) if context_parts else "Generic business."
    payload = f"{instructions}\n\nContext: {context}"

    try:
        client = GeminiClient(LLM_MODEL, temperature=0.5, retries=1)
        resp = client.generate_json(payload)
        cta_text = (resp or {}).get("cta_text", "").strip()

        # Проверяем, что выбранный текст есть в списке
        if cta_text in CTA_TEXTS:
            logger.info("step14: LLM выбрала CTA текст: %s", cta_text)
            return cta_text

        logger.warning("step14: LLM вернула некорректный CTA текст (%s), используем Learn more", cta_text)
        return "Learn more"

    except Exception as exc:
        logger.warning("step14: LLM выбор CTA текста не удался: %s", exc)
        return "Learn more"


def _llm_generate_business_name(
    *,
    business_name: Optional[str] = None,
    usp: Optional[str] = None,
    site_url: Optional[str] = None,
) -> str:
    """
    Сгенерировать Business name через LLM (макс. 25 символов).
    """
    # Если уже есть business_name, обрезаем и используем его
    if business_name:
        truncated = _truncate_text(business_name, BUSINESS_NAME_MAX_LENGTH)
        if truncated:
            logger.info("step14: используем предоставленное business_name: %s", truncated)
            return truncated

    if STEP14_DISABLE_LLM or GeminiClient is None:
        fallback = _truncate_text(business_name or usp or "My Business", BUSINESS_NAME_MAX_LENGTH)
        logger.warning("step14: LLM отключена — fallback business_name: %s", fallback)
        return fallback

    instructions = (
        f"Generate a concise business name for Google Ads. "
        f"MUST be ≤{BUSINESS_NAME_MAX_LENGTH} characters. "
        "Return ONLY JSON {\"business_name\": \"...\"}. "
        "Make it professional and memorable."
    )

    context_parts = []
    if business_name:
        context_parts.append(f"Current name: {business_name}")
    if usp:
        context_parts.append(f"USP: {usp}")
    if site_url:
        context_parts.append(f"Website: {site_url}")

    context = ". ".join(context_parts) if context_parts else "Generic business."
    payload = f"{instructions}\n\nContext: {context}"

    try:
        client = GeminiClient(LLM_MODEL, temperature=0.7, retries=1)
        resp = client.generate_json(payload)
        generated_name = (resp or {}).get("business_name", "").strip()

        if generated_name and len(generated_name) <= BUSINESS_NAME_MAX_LENGTH:
            logger.info("step14: LLM сгенерировала business_name: %s", generated_name)
            return generated_name

        # Если слишком длинное, обрезаем
        if generated_name:
            truncated = _truncate_text(generated_name, BUSINESS_NAME_MAX_LENGTH)
            logger.warning("step14: обрезаем business_name до %d символов: %s", BUSINESS_NAME_MAX_LENGTH, truncated)
            return truncated

        fallback = _truncate_text(business_name or usp or "My Business", BUSINESS_NAME_MAX_LENGTH)
        logger.warning("step14: LLM вернула пустой ответ — fallback: %s", fallback)
        return fallback

    except Exception as exc:
        fallback = _truncate_text(business_name or usp or "My Business", BUSINESS_NAME_MAX_LENGTH)
        logger.warning("step14: LLM генерация business_name не удалась (%s) — fallback: %s", exc, fallback)
        return fallback


def _select_from_dropdown(
    driver: WebDriver,
    dropdown_button: WebElement,
    item_text: str,
    dropdown_type: str = "dropdown",
) -> bool:
    """
    Выбрать элемент из dropdown меню.

    Args:
        driver: WebDriver
        dropdown_button: элемент кнопки dropdown
        item_text: текст элемента для выбора
        dropdown_type: тип dropdown для логирования

    Returns:
        True если выбор успешен, False иначе
    """
    try:
        # Кликнуть на dropdown
        if not _js_click(driver, dropdown_button):
            logger.warning("step14: не удалось кликнуть на %s dropdown", dropdown_type)
            return False

        time.sleep(0.5)

        # Найти элемент с нужным текстом в списке
        item = driver.execute_script(
            """
            const targetText = arguments[0];
            const items = document.querySelectorAll('material-select-dropdown-item');
            for (const item of items) {
                const label = item.querySelector('.label');
                if (label && label.textContent.trim() === targetText) {
                    return item;
                }
            }
            return null;
            """,
            item_text,
        )

        if not item:
            logger.warning("step14: элемент '%s' не найден в %s dropdown", item_text, dropdown_type)
            return False

        # Кликнуть на элемент
        if not _js_click(driver, item):
            logger.warning("step14: не удалось кликнуть на элемент '%s'", item_text)
            return False

        time.sleep(0.3)
        logger.info("step14: выбран элемент '%s' в %s", item_text, dropdown_type)
        return True

    except Exception as exc:
        logger.warning("step14: ошибка выбора из %s dropdown: %s", dropdown_type, exc)
        return False


def _fill_cta_language(
    driver: WebDriver,
    language: str,
) -> bool:
    """
    Выбрать язык для Call to Action.

    Args:
        driver: WebDriver
        language: название языка (например, "Russian", "English (United States)")

    Returns:
        True если выбор успешен, False иначе
    """
    try:
        # Найти блок Call to Action
        cta_block = _wait_for_element(
            driver,
            'call-to-action',
            timeout=5.0,
            require_visible=False,
        )

        if not cta_block:
            logger.warning("step14: блок Call to Action не найден")
            return False

        # Найти dropdown для языка (второй dropdown в блоке)
        language_dropdown = driver.execute_script(
            """
            const ctaBlock = arguments[0];
            const dropdowns = ctaBlock.querySelectorAll('dropdown-button');
            if (dropdowns.length >= 2) {
                return dropdowns[1]; // Второй dropdown - это язык
            }
            return null;
            """,
            cta_block,
        )

        if not language_dropdown:
            logger.warning("step14: dropdown языка Call to Action не найден")
            return False

        return _select_from_dropdown(driver, language_dropdown, language, "CTA language")

    except Exception as exc:
        logger.warning("step14: ошибка выбора языка CTA: %s", exc)
        return False


def _fill_cta_text(
    driver: WebDriver,
    cta_text: str,
) -> bool:
    """
    Выбрать текст для Call to Action.

    Args:
        driver: WebDriver
        cta_text: текст CTA (например, "Learn more", "Shop now")

    Returns:
        True если выбор успешен, False иначе
    """
    try:
        # Найти блок Call to Action
        cta_block = _wait_for_element(
            driver,
            'call-to-action',
            timeout=5.0,
            require_visible=False,
        )

        if not cta_block:
            logger.warning("step14: блок Call to Action не найден")
            return False

        # Найти dropdown для текста CTA (первый dropdown в блоке)
        text_dropdown = driver.execute_script(
            """
            const ctaBlock = arguments[0];
            const dropdowns = ctaBlock.querySelectorAll('dropdown-button');
            if (dropdowns.length >= 1) {
                return dropdowns[0]; // Первый dropdown - это текст CTA
            }
            return null;
            """,
            cta_block,
        )

        if not text_dropdown:
            logger.warning("step14: dropdown текста Call to Action не найден")
            return False

        return _select_from_dropdown(driver, text_dropdown, cta_text, "CTA text")

    except Exception as exc:
        logger.warning("step14: ошибка выбора текста CTA: %s", exc)
        return False


def _fill_business_name_field(
    driver: WebDriver,
    business_name: str,
) -> bool:
    """
    Заполнить поле Business name.

    Args:
        driver: WebDriver
        business_name: название бизнеса (макс. 25 символов)

    Returns:
        True если заполнение успешно, False иначе
    """
    try:
        # Найти input с aria-label="Business name"
        business_input = _wait_for_element(
            driver,
            'input[aria-label="Business name"]',
            timeout=5.0,
            require_visible=True,
        )

        if not business_input:
            logger.warning("step14: поле Business name не найдено")
            return False

        # Заполнить поле
        truncated_name = _truncate_text(business_name, BUSINESS_NAME_MAX_LENGTH)

        if _set_input_value(driver, business_input, truncated_name, verify=True):
            logger.info("step14: Business name заполнено: '%s'", truncated_name)
            return True

        logger.warning("step14: не удалось заполнить Business name")
        return False

    except Exception as exc:
        logger.warning("step14: ошибка заполнения Business name: %s", exc)
        return False


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
    languages: Optional[List[str]] = None,
    emit: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """
    Заполнение текстовых креативов (Headlines, Descriptions, Call to Action, Business Name).

    Args:
        driver: WebDriver
        mode: режим работы ("ai_only", "inspired", "manual")
        seed_assets: словарь с примерами для режима "inspired"
            {"headlines": [...], "descriptions": [...]}
        provided_assets: словарь с готовыми текстами для режима "manual"
            {"headlines": [...], "descriptions": [...]}
        business_name: название бизнеса (используется для генерации и заполнения)
        usp: УТП
        site_url: URL сайта
        languages: список языков кампании (например, ["en", "ru"], ["English", "Russian"])
        emit: функция для вывода сообщений

    Returns:
        Словарь с результатом:
        {
            "mode": str,
            "headlines": {"texts": [...], "count": int},
            "descriptions": {"texts": [...], "count": int},
            "call_to_action": {
                "language": str,
                "language_success": bool,
                "text": str,
                "text_success": bool
            },
            "business_name": {"value": str, "success": bool},
            "duration_ms": int,
            "timer_marks": [...]
        }

    Новая функциональность:
        - Определение языка контента и выбор соответствующего языка для CTA
        - Генерация и выбор подходящего текста Call to Action через LLM
        - Генерация или обрезка Business name (макс. 25 символов)
        - Заполнение всех полей с подробным логированием
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
            languages=languages,
        )
        descriptions = _llm_generate_descriptions(
            count=MAX_DESCRIPTIONS,
            business_name=business_name,
            usp=usp,
            site_url=site_url,
            languages=languages,
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
            languages=languages,
            seed_examples=seed_headlines,
        )
        descriptions = _llm_generate_descriptions(
            count=MAX_DESCRIPTIONS,
            business_name=business_name,
            usp=usp,
            site_url=site_url,
            languages=languages,
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

    # ==================================================================================
    # НОВАЯ ФУНКЦИОНАЛЬНОСТЬ: Call to Action и Business Name
    # ==================================================================================

    # Определить язык для CTA на основе переданных языков
    _emit(emit, "Определение языка Call to Action")
    detected_language = _detect_language_from_context(
        languages=languages,
        business_name=business_name,
        usp=usp,
        headlines=filled_headlines,
        descriptions=filled_descriptions,
    )
    logger.info("step14: определен язык CTA: %s", detected_language)
    tm.mark("detect_language")

    # Выбрать язык Call to Action
    _emit(emit, f"Выбор языка Call to Action: {detected_language}")
    cta_language_success = _fill_cta_language(driver, detected_language)
    if cta_language_success:
        _emit(emit, f"✓ Язык CTA установлен: {detected_language}")
    else:
        _emit(emit, f"⚠ Не удалось установить язык CTA")
    tm.mark("fill_cta_language")

    # Сгенерировать и выбрать текст Call to Action через LLM
    _emit(emit, "Генерация текста Call to Action через LLM")
    selected_cta_text = _llm_select_cta_text(
        business_name=business_name,
        usp=usp,
        site_url=site_url,
        headlines=filled_headlines,
    )
    logger.info("step14: выбран текст CTA: %s", selected_cta_text)
    tm.mark("llm_select_cta")

    # Выбрать текст Call to Action
    _emit(emit, f"Выбор текста Call to Action: {selected_cta_text}")
    cta_text_success = _fill_cta_text(driver, selected_cta_text)
    if cta_text_success:
        _emit(emit, f"✓ Текст CTA установлен: {selected_cta_text}")
    else:
        _emit(emit, f"⚠ Не удалось установить текст CTA")
    tm.mark("fill_cta_text")

    # Сгенерировать Business name через LLM
    _emit(emit, "Генерация Business name через LLM")
    generated_business_name = _llm_generate_business_name(
        business_name=business_name,
        usp=usp,
        site_url=site_url,
    )
    logger.info("step14: сгенерировано business_name: %s", generated_business_name)
    tm.mark("llm_generate_business_name")

    # Заполнить поле Business name
    _emit(emit, f"Заполнение Business name: {generated_business_name}")
    business_name_success = _fill_business_name_field(driver, generated_business_name)
    if business_name_success:
        _emit(emit, f"✓ Business name заполнено: {generated_business_name}")
    else:
        _emit(emit, f"⚠ Не удалось заполнить Business name")
    tm.mark("fill_business_name")

    duration_ms = int((time.time() - started) * 1000)

    logger.info(
        "step14 done (%d ms) | заголовки=%d, описания=%d | CTA=%s | business_name=%s | mode=%s",
        duration_ms,
        len(filled_headlines),
        len(filled_descriptions),
        selected_cta_text if cta_text_success else "FAILED",
        generated_business_name if business_name_success else "FAILED",
        normalized_mode,
    )

    # Вывод итогов
    _emit(emit, "=" * 60)
    _emit(emit, "ИТОГИ ЗАПОЛНЕНИЯ:")
    _emit(emit, f"Заголовки ({len(filled_headlines)}): {', '.join(h[:30] + '...' if len(h) > 30 else h for h in filled_headlines)}")
    _emit(emit, f"Описания ({len(filled_descriptions)}): {', '.join(d[:40] + '...' if len(d) > 40 else d for d in filled_descriptions)}")
    _emit(emit, f"CTA язык: {detected_language} {'✓' if cta_language_success else '✗'}")
    _emit(emit, f"CTA текст: {selected_cta_text} {'✓' if cta_text_success else '✗'}")
    _emit(emit, f"Business name: {generated_business_name} {'✓' if business_name_success else '✗'}")
    _emit(emit, "=" * 60)

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
        "call_to_action": {
            "language": detected_language,
            "language_success": cta_language_success,
            "text": selected_cta_text,
            "text_success": cta_text_success,
        },
        "business_name": {
            "value": generated_business_name,
            "success": business_name_success,
        },
        "duration_ms": duration_ms,
        "timer_marks": tm.records,
    }


def run(driver: WebDriver, **kwargs) -> Dict[str, Any]:
    """Точка входа для обратной совместимости."""
    return run_step14(driver, **kwargs)