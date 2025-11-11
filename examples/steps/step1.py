# -*- coding: utf-8 -*-
"""
Шаг 1 Google Ads Wizard (жёсткие селекторы + LLM):
- Открыть экран intro: https://ads.google.com/aw/campaigns/new/business
- Выбрать radio "A website" (язык не важен — селекторы по debugid/data-test-id)
- Сгенерировать через LLM (Gemini) "business_name" из УТП/URL/бюджета
- Заполнить поля ("Business name", "Website URL"), если были — перезаписать
- Нажать "Next/Continue" (поиск на любом языке), дождаться ухода со страницы

Входные параметры шага:
- budget_per_day: str | float — бюджет/день (используется в подсказке LLM + лог)
- site_url: str — конечный URL (нормализуем, можно подтвердить через LLM)
- usp: str — УТП/описание бизнеса (ядро генерации названия)

Дополнительно:
- emit: Optional[Callable[[str], None]] — колбэк для UI‑комментариев в реальном времени.
  Если передан, шага шлёт короткие статусы «по делу»: что сейчас делает,
  что получилось и что будет дальше.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Callable, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

from selenium.webdriver.remote.webdriver import WebDriver, WebElement
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

logger = logging.getLogger("ads_ai.gads.step1")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

# ---------- Жёсткие селекторы (язык-независимые) ----------

BUSINESS_PAGE_URLS: List[str] = [
    "https://ads.google.com/aw/campaigns/new/business",
    "https://ads.google.com/aw/campaigns/new/business?hl=en",
]

# Поле "Business name"
BUSINESS_NAME_INPUT_SELECTORS: List[str] = [
    '[data-test-id="business-name-input"] input.input',
    'material-input.business-name-input input.input',
    'div.editor-panel.business-name material-input input.input',
    'div.editor-panel.business-name input.input',
]

# Радио "A website"
WEBSITE_RADIO_SELECTORS: List[str] = [
    '[data-test-id="website-radio"]',
    '[debugid="ad-landing-destination-radio-website"]',
    'material-radio[debugid*="radio-website"]',
    'div.ad-landing-destination-radio-option material-radio.option-radio',
]

# Поле "Website URL"
WEBSITE_URL_INPUT_SELECTORS: List[str] = [
    '[data-test-id="website-input"] input.input',
    'material-input.website-input input.input',
    'div.editor-panel .website-input input.input',
    'div.editor-panel [data-test-id="website-input"] input',
    'input[type="url"]',
]

# «Вперёд» на разных языках
_NEXT_TEXTS = [
    # EN
    "next", "continue", "save and continue",
    # RU/UA
    "далее", "продолжить", "сохранить и продолжить", "далі",
    # ES/PT/FR/IT/DE
    "siguiente", "continuar", "guardar y continuar",
    "avançar", "próximo", "próxima", "suivant", "continuer",
    "avanti", "salva e continua",
    "weiter", "weitergehen",
    # PL/TR/HU/CZ/SK
    "dalej", "kontynuuj",
    "ileri", "devam",
    "tovább",
    "pokračovať", "pokračovat",
    # ZH/JA/KO/TH/VI
    "下一步", "继续", "繼續", "下一頁", "下一页",
    "次へ", "続行",
    "다음", "계속",
    "ถัดไป", "ดำเนินการต่อ",
    "tiếp theo", "tiếp tục",
]
# «Назад» — исключаем при выборе кнопки
_BACK_TEXTS = [
    "back", "назад", "atrás", "zurück", "retour", "voltar",
    "上一页", "上一步", "戻る", "뒤로", "zpět", "späť", "wstecz",
]

# ---------- LLM (Gemini) ----------

try:
    from ads_ai.llm.gemini import GeminiClient  # контракты проекта не меняем
except Exception as e:  # pragma: no cover
    GeminiClient = None  # type: ignore
    logger.warning("GeminiClient not available: %s", e)


def _emit(emit: Optional[Callable[[str], None]], text: str) -> None:
    """Безопасно отправляет комментарий в UI."""
    if callable(emit) and isinstance(text, str) and text.strip():
        try:
            emit(text.strip())
        except Exception:
            pass


def _detect_ui_locale(driver: WebDriver) -> str:
    try:
        lang = driver.execute_script(
            "return (document.documentElement.lang || navigator.language || navigator.userLanguage || '').toString();"
        ) or ""
        return str(lang).strip()
    except Exception:
        return ""


def _normalize_budget(text: str | float) -> str:
    s = str(text).strip()
    s = s.replace(",", ".")
    s = re.sub(r"[^\d.]", "", s)
    if not s:
        return "0"
    # максимум 2 знака после точки
    m = re.match(r"^(\d+)(?:\.(\d{1,2}))?$", s)
    if m:
        return s
    m = re.match(r"^(\d+)\.(\d+)$", s)
    if m:
        return f"{m.group(1)}.{m.group(2)[:2]}"
    return re.sub(r"\D", "", s)


def _normalize_url(url: str) -> str:
    if not url:
        return ""
    u = url.strip()
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+\-.]*://", u):
        u = "https://" + u
    try:
        p = urlparse(u)
        if not p.scheme or not p.netloc:
            return ""
    except Exception:
        return ""
    return re.sub(r"\s+", "", u)


def _loose_load_json(text: str) -> dict:
    if not text:
        return {}
    try:
        return json.loads(text)
    except Exception:
        pass
    m = re.search(r"\{.*\}", text, flags=re.S)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            return {}
    return {}


def _generate_business_name_via_llm(usp: str, site_url: str, budget: str, locale_hint: str) -> str:
    """
    Просим LLM дать ТОЛЬКО краткое имя бренда/бизнеса без лишнего текста.
    Если LLM недоступен — вернём очищенный вариант УТП.
    """
    usp_clean = re.sub(r"\s+", " ", (usp or "").strip())
    site_norm = _normalize_url(site_url)
    if GeminiClient is None:
        logger.warning("LLM is not available; fallback name from USP.")
        return (usp_clean or "New Campaign")

    prompt = {
        "task": "Return ONLY JSON with business_name (string). No prose.",
        "constraints": [
            "Make a natural, human-friendly business/brand name.",
            "Avoid generic words like 'Company', 'Business' unless necessary.",
            "Do not include quotes or emojis."
        ],
        "inputs": {
            "usp": usp_clean,
            "site_url": site_norm,
            "budget_per_day": budget,
            "locale_hint": locale_hint or "auto"
        },
        "output_schema": {"business_name": "string"},
        "format": "json_only_no_explanations",
        "examples": [
            {"usp": "Мы запускаем рекламу с ИИ для e‑commerce.",
             "site_url": "https://easy-byte.ru/",
             "budget_per_day": "3000",
             "json": {"business_name": "EasyByte AI"}}
        ]
    }

    try:
        model = os.getenv("GEMINI_MODEL", "models/gemini-2.0-flash")
        client = GeminiClient(model=model, temperature=0.2, retries=1, fallback_model=None)
        raw = client.generate_json(json.dumps(prompt, ensure_ascii=False))
        data = raw if isinstance(raw, dict) else _loose_load_json(str(raw))
        name = str((data or {}).get("business_name", "")).strip()
        name = re.sub(r"[\"'«»]+", "", name)
        return name or (usp_clean or "New Campaign")
    except Exception as e:
        logger.warning("LLM generation failed: %s", e)
        return (usp_clean or "New Campaign")


# ---------- Selenium helpers ----------

def _wait_visible_any(driver: WebDriver, selectors: Iterable[str], timeout: float = 8.0) -> Optional[WebElement]:
    end = time.time() + timeout
    last_err = None
    while time.time() < end:
        for sel in selectors:
            try:
                el = driver.find_element(By.CSS_SELECTOR, sel)
                if _is_element_interactable(driver, el):
                    return el
            except Exception as e:  # noqa: PERF203
                last_err = e
        time.sleep(0.12)
    if last_err:
        logger.debug("wait_visible_any: last_err=%r", last_err)
    return None


def _is_element_interactable(driver: WebDriver, el: WebElement) -> bool:
    try:
        if not el.is_displayed():
            return False
        aria_disabled = (el.get_attribute("aria-disabled") or "").lower() == "true"
        if not el.is_enabled() or aria_disabled:
            return False
        driver.execute_script("arguments[0].scrollIntoView({block:'center',inline:'nearest'});", el)
        r = el.rect
        if r.get("width", 0) < 8 or r.get("height", 0) < 8:
            return False
        return True
    except Exception:
        return False


def _dispatch_input_change(driver: WebDriver, el: WebElement, value: str) -> None:
    """Материал-инпут: ставим значение через JS + генерим input/change."""
    driver.execute_script(
        """
        const el = arguments[0], val = arguments[1];
        try {
          el.focus();
          el.value = '';
          el.dispatchEvent(new Event('input', {bubbles:true}));
          el.value = val;
          el.dispatchEvent(new Event('input', {bubbles:true}));
          el.dispatchEvent(new Event('change', {bubbles:true}));
        } catch(e) {}
        """,
        el, value,
    )
    try:
        el.send_keys(Keys.END)
    except Exception:
        pass


def _ensure_radio_checked(driver: WebDriver, radio: WebElement) -> None:
    try:
        checked = (radio.get_attribute("aria-checked") or "").lower() == "true"
        if checked:
            return
    except Exception:
        pass
    try:
        radio.click()
    except Exception:
        driver.execute_script(
            """
            const r=arguments[0];
            let root = r.closest('.option-radio, material-radio, .ad-landing-destination-radio-option') || r;
            try { root.click(); } catch(e) {}
            """,
            radio,
        )
    time.sleep(0.15)


def _find_forward_button_any_language(driver: WebDriver) -> Optional[WebElement]:
    """Ищем «Next/Continue» на любом языке. Исключаем 'Back'."""
    try:
        cand = driver.execute_script(
            """
            const NEXT = new Set(arguments[0].map(s => String(s||'').toLowerCase()));
            const BACK = new Set(arguments[1].map(s => String(s||'').toLowerCase()));

            const isVis = (el)=>{ if(!el) return false;
              const cs=getComputedStyle(el); const r=el.getBoundingClientRect();
              if (cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2||cs.pointerEvents==='none') return false;
              return r.width>8 && r.height>8 && r.top<innerHeight && r.left<innerWidth && r.bottom>0 && r.right>0;
            };
            const notDisabled = (el) => {
              const aria=(el.getAttribute('aria-disabled')||'').toLowerCase()==='true';
              const dis=el.hasAttribute('disabled'); return !(aria||dis);
            };
            const texts = (el)=>((el.innerText||el.textContent||'')+' '+(el.getAttribute('aria-label')||'')).trim().toLowerCase();

            const root = document.querySelector('main,[role=main]') || document.body;
            const nodes = [...root.querySelectorAll('button,[role=button],a[role=button]')];

            let best=null, bestScore=-1;
            for (const el of nodes) {
              if (!isVis(el) || !notDisabled(el)) continue;
              const t = texts(el);
              let s = 0;
              for (const w of NEXT) if (w && t.includes(w)) s += 10;
              for (const b of BACK) if (b && t.includes(b)) s -= 10;
              const cls=(el.className||'').toLowerCase();
              if (/(primary|mdc-button--raised|mat-primary)/.test(cls)) s += 3;
              const r = el.getBoundingClientRect();
              s += Math.min(3, Math.max(0, Math.round((r.left / Math.max(1, innerWidth)) * 3)));
              if (s > bestScore) { best = el; bestScore = s; }
            }
            return best || null;
            """,
            _NEXT_TEXTS, _BACK_TEXTS,
        )
        if cand:
            return cand
    except Exception:
        pass
    return None


def _click_forward_and_wait(driver: WebDriver, timeout: float = 15.0) -> bool:
    """Кликаем «вперёд» и ждём ухода со страницы /business."""
    old_url = driver.current_url or ""
    btn = _find_forward_button_any_language(driver)
    if not btn:
        logger.warning("Не удалось найти кнопку Next/Continue (любой язык).")
        return False

    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
        time.sleep(0.05)
        btn.click()
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", btn)
        except Exception:
            return False

    end = time.time() + timeout
    while time.time() < end:
        cur = driver.current_url or ""
        if cur != old_url and "/business" not in cur:
            return True
        time.sleep(0.15)
    return (driver.current_url or "") != old_url


def _dismiss_soft_dialogs(driver: WebDriver, budget_ms: int = 900) -> None:
    """Пробуем закрыть куки/диалоги аккуратно, быстро."""
    t0 = time.time()
    CAND_TEXTS = [
        "accept all", "i agree", "agree", "got it", "ok",
        "принять все", "я согласен", "понятно", "хорошо",
        "同意", "接受", "确定", "知道了", "好",
    ]
    while (time.time() - t0) * 1000 < budget_ms:
        try:
            dialogs = driver.find_elements(By.CSS_SELECTOR, '[role="dialog"], div[aria-modal="true"], .mdc-dialog--open')
            found = False
            for dlg in dialogs:
                if not _is_element_interactable(driver, dlg):
                    continue
                btns = dlg.find_elements(By.CSS_SELECTOR, 'button, [role=button], a[role=button]')
                for b in btns:
                    try:
                        txt = ((b.text or "") + " " + (b.get_attribute("aria-label") or "")).strip().lower()
                    except Exception:
                        txt = ""
                    if txt and any(w in txt for w in CAND_TEXTS):
                        try:
                            b.click()
                        except Exception:
                            try:
                                driver.execute_script("arguments[0].click();", b)
                            except Exception:
                                continue
                        time.sleep(0.18)
                        found = True
                        break
                if found:
                    break
            if not found:
                break
        except Exception:
            break


# ---------- Основной шаг ----------

def run_step1(
    driver: WebDriver,
    *,
    budget_per_day: str | float,
    site_url: str,
    usp: str,
    timeout_open: float = 20.0,
    emit: Optional[Callable[[str], None]] = None,  # <— колбэк комментариев
) -> Tuple[str, str, str]:
    """
    Выполняет первый экран мастера:
      1) открытие,
      2) выбор "A website",
      3) LLM-генерация business_name (из УТП/URL/бюджета),
      4) ввод name + URL,
      5) Next.

    Возвращает (business_name, website_url, budget_sanitized).
    """
    assert str(site_url).strip(), "site_url не может быть пустым"
    budget_clean = _normalize_budget(budget_per_day)
    url_clean = _normalize_url(site_url)
    if not url_clean:
        raise ValueError("Некорректный site_url (не удаётся нормализовать в https://...)")

    # Подсказка языка UI для LLM
    locale_hint = _detect_ui_locale(driver)

    _emit(emit, "Открываю мастер «Бизнес и сайт»")
    # Открываем страницу
    opened = False
    for url in BUSINESS_PAGE_URLS:
        try:
            logger.info("Открываю страницу: %s", url)
            driver.get(url)
            end = time.time() + timeout_open
            while time.time() < end:
                if "campaigns/new" in (driver.current_url or ""):
                    break
                time.sleep(0.1)
            if "campaigns/new" not in (driver.current_url or ""):
                continue
            time.sleep(0.4)
            opened = True
            break
        except Exception as e:
            logger.warning("Не удалось открыть %s: %s", url, e)
    if not opened:
        raise RuntimeError("Не удалось открыть страницу мастера (business intro).")

    _emit(emit, "Если всплывут диалоги — аккуратно закрою")
    _dismiss_soft_dialogs(driver)

    # Генерируем имя бизнеса
    _emit(emit, "Придумываю короткое название бренда из УТП и сайта")
    business_name = _generate_business_name_via_llm(usp=usp, site_url=url_clean, budget=budget_clean, locale_hint=locale_hint)
    logger.info("LLM business_name=%r; budget/day=%s; url=%s", business_name, budget_clean, url_clean)
    _emit(emit, f"Готово: «{business_name}»")

    # Выбираем "A website"
    _emit(emit, "Выбираю вариант «Сайт» как цель перехода")
    radio = _wait_visible_any(driver, WEBSITE_RADIO_SELECTORS, timeout=6.0)
    if radio:
        _ensure_radio_checked(driver, radio)
        logger.info("Радио 'A website' выбрано/подтверждено.")
    else:
        logger.info("Радио 'A website' не найдено — возможно уже выбрано.")

    # Ввод Business name
    _emit(emit, "Заполняю поле «Название бизнеса»")
    bn = _wait_visible_any(driver, BUSINESS_NAME_INPUT_SELECTORS, timeout=8.0)
    if bn:
        _dispatch_input_change(driver, bn, business_name.strip())
        logger.info("Введено Business name: %s", business_name.strip())
    else:
        logger.warning("Поле 'Business name' не найдено — пропускаю (опционально).")
        _emit(emit, "Поле названия не нашёл — двигаюсь дальше")

    # Ввод Website URL (из входных параметров)
    _emit(emit, "Подставляю адрес сайта")
    wu = _wait_visible_any(driver, WEBSITE_URL_INPUT_SELECTORS, timeout=10.0)
    if not wu:
        _emit(emit, "Не вижу поле URL — шаг остановлен")
        raise RuntimeError("Поле 'Website URL' не найдено.")
    _dispatch_input_change(driver, wu, url_clean.strip())
    logger.info("Введён Website URL: %s", url_clean.strip())

    # Next/Continue
    _emit(emit, "Жму «Продолжить»")
    ok = _click_forward_and_wait(driver, timeout=15.0)
    if not ok:
        _emit(emit, "Кнопка не сработала — попробуйте вручную")
        raise RuntimeError("Кнопка Next/Continue не сработала или не изменилась страница.")

    logger.info("Шаг 1 завершён: перешли на следующий экран.")
    _emit(emit, "Экран пройден, идём дальше")
    time.sleep(0.3)
    return business_name, url_clean, budget_clean
