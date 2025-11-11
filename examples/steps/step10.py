# -*- coding: utf-8 -*-
"""
examples/steps/step10.py

Шаг 10 (Publish):
  - Надёжно нажать кнопку «Publish campaign» (приоритет по тексту, не по классам).
  - Вместо ожидания конкретной надписи делаем фиксированную паузу 10 секунд.
  - Если за это время появляются характерные блоки (review/tag/snippet) — фиксируем их, но успех считаем без ожидания.
  - При наличии Google tag сниппета пытаемся его скопировать и вернуть в результате.

Возврат:
    {
      "published_ok": bool,
      "published_indicator": str,  # 'review_message' | 'tag_setup' | 'snippet_found' | 'manual_wait'
      "gtag_snippet": str,         # может быть пустым
      "clicked": bool,             # удалось ли кликнуть «Publish…»
      "duration_ms": int
    }

Контракт функции: run_step10(driver, *, timeout_click=30.0, timeout_publish=600.0, emit=None)
Совместим с автозапуском из ads_ai.web.create_companies (discover/import "examples.steps.step10:run_step10").
"""
from __future__ import annotations

import logging
import time
from typing import Callable, Optional, Dict, Any, List, Tuple

from selenium.webdriver.common.by import By  # type: ignore
from selenium.webdriver.remote.webdriver import WebDriver, WebElement  # type: ignore

from examples.steps.step4 import _maybe_handle_confirm_its_you  # type: ignore

logger = logging.getLogger("ads_ai.gads.step10")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")


# ============================ helpers ============================

def _emit(emit: Optional[Callable[[str], None]], text: str) -> None:
    """Безопасная посылка комментария в UI."""
    if callable(emit) and isinstance(text, str) and text.strip():
        try:
            emit(text.strip())
        except Exception:
            pass


def _maybe_handle_confirm_async(
    driver: WebDriver,
    *,
    emit: Optional[Callable[[str], None]] = None,
    timeout: float = 6.0,
    interval: float = 0.35,
) -> bool:
    handled = False
    if timeout <= 0:
        return bool(_maybe_handle_confirm_its_you(driver, emit))
    deadline = time.time() + timeout
    interval = max(0.1, min(interval, 1.0))
    while time.time() < deadline:
        if _maybe_handle_confirm_its_you(driver, emit):
            handled = True
        time.sleep(interval)
    return handled


def _is_interactable(driver: WebDriver, el: WebElement) -> bool:
    try:
        if not el.is_displayed():
            return False
        if not el.is_enabled():
            return False
        if (el.get_attribute("aria-disabled") or "").lower() == "true":
            return False
        driver.execute_script("arguments[0].scrollIntoView({block:'center',inline:'nearest'});", el)
        r = el.rect
        return r.get("width", 0) >= 8 and r.get("height", 0) >= 8
    except Exception:
        return False


def _robust_click(driver: WebDriver, el: WebElement) -> bool:
    """Серии попыток: обычный click → JS click → синтетические mouse события."""
    _maybe_handle_confirm_its_you(driver, emit=None)
    if not _is_interactable(driver, el):
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        except Exception:
            pass
    try:
        el.click()
        return True
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            try:
                driver.execute_script(
                    """
                    const el=arguments[0];
                    const r=el.getBoundingClientRect();
                    const x=Math.floor(r.left + Math.max(2, r.width/2));
                    const y=Math.floor(r.top  + Math.max(2, r.height/2));
                    const mk=(t)=>new MouseEvent(t,{view:window,bubbles:true,cancelable:true,clientX:x,clientY:y});
                    el.dispatchEvent(mk('mousedown')); el.dispatchEvent(mk('mouseup')); el.dispatchEvent(mk('click'));
                    """,
                    el,
                )
                return True
            except Exception:
                return False


def _find_publish_button(driver: WebDriver) -> Optional[WebElement]:
    """
    Ищем видимые кнопки по тексту ('Publish campaign' → приоритет, затем 'Publish').
    Безопасно для локализованных интерфейсов — пытаемся ещё по английскому слову 'publish'.
    """
    _maybe_handle_confirm_its_you(driver, emit=None)
    js = r"""
    const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e), r=e.getBoundingClientRect();
      if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2) return false;
      return r.width>10 && r.height>10 && r.right>0 && r.bottom>0;};
    const all=[...document.querySelectorAll('button')].filter(isVis);
    const norm=t=>(String(t||'').trim().replace(/\s+/g,' ').toLowerCase());
    const label=(b)=>{
      let s=(b.innerText||b.textContent||'').trim();
      if(!s && b.getAttribute('aria-label')) s=b.getAttribute('aria-label');
      return norm(s);
    };
    // exact preferred phrases
    let btn = all.find(b=>['publish campaign','launch campaign'].includes(label(b)));
    if (btn) return btn;
    // contains 'publish'
    btn = all.find(b=>label(b).includes('publish'));
    if (btn) return btn;
    // guessed primary next/publish classes
    btn = all.find(b=>b.classList.contains('button-next')||b.classList.contains('mdc-button--unelevated'));
    if (btn && label(btn)) return btn;
    return null;
    """
    try:
        el = driver.execute_script(js)
        if el:
            return el  # type: ignore
    except Exception:
        pass

    # Фоллбек — XPath по тексту
    xpaths = [
        "//button[normalize-space(.)='Publish campaign']",
        "//button[.//span[normalize-space(text())='Publish campaign']]",
        "//button[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'publish')]",
    ]
    for xp in xpaths:
        try:
            els = driver.find_elements(By.XPATH, xp)
            for el in els:
                if _is_interactable(driver, el):
                    return el
        except Exception:
            continue
    return None


def _click_publish(driver: WebDriver, timeout_click: float) -> bool:
    t0 = time.time()
    while (time.time() - t0) < max(2.0, timeout_click):
        _maybe_handle_confirm_its_you(driver, emit=None)
        btn = _find_publish_button(driver)
        if not btn:
            time.sleep(0.5)
            continue
        ok = _robust_click(driver, btn)
        if ok:
            return True
        time.sleep(0.6)
    return False


def _check_publish_signals_and_snippet(driver: WebDriver) -> Tuple[str, str]:
    """
    Возвращает (indicator, snippet). indicator ∈ {'review_message','tag_setup','snippet_found',''}.
    snippet — текст кода Google tag (если найден).
    """
    _maybe_handle_confirm_its_you(driver, emit=None)
    js = r"""
    const has = (re, s) => re.test(String(s||''));
    const txt = (el) => (el && (el.innerText||el.textContent||'').trim()) || '';
    const bodyText = (document.body && (document.body.innerText||document.body.textContent||'')) || '';
    const reviewTitle = [...document.querySelectorAll('.blg-title,.ads-ufo-subhead,.blg-subhead1,.blg-subhead2')]
      .map(txt).some(t => /your ads will go live after a review/i.test(t));
    const reviewText = /your ads will go live after a review/i.test(bodyText);
    const tagSetupEl = document.querySelector('gte-setup-selection');
    const tagSetupText = /choose how to set up a google tag/i.test(bodyText);
    const snippetEl = document.querySelector('div.ogt-snippet[aria-label*=\"Google tag\" i]') 
                   || document.querySelector('ogt-snippet .ogt-snippet')
                   || document.querySelector('.ogt-snippet');
    const snippet = snippetEl ? txt(snippetEl) : '';

    let indicator = '';
    if (reviewTitle || reviewText) indicator = 'review_message';
    else if (tagSetupEl || tagSetupText) indicator = 'tag_setup';
    else if (snippet) indicator = 'snippet_found';

    return { indicator, snippet };
    """
    try:
        res = driver.execute_script(js) or {}
        ind = str(res.get("indicator") or "")
        snip = str(res.get("snippet") or "")
        return ind, snip
    except Exception:
        return "", ""


def _try_click_copy_for_snippet(driver: WebDriver) -> bool:
    """Пробуем нажать кнопку Copy рядом со сниппетом (если есть)."""
    try:
        _maybe_handle_confirm_its_you(driver, emit=None)
        el = driver.execute_script(
            "return document.querySelector('div.ogt-snippet .ogt-content-copy-button') || document.querySelector('.ogt-content-copy-button');"
        )
        if el:
            return _robust_click(driver, el)  # type: ignore
    except Exception:
        pass
    return False


# ============================ main ============================

def run_step10(
    driver: WebDriver,
    *,
    timeout_click: float = 30.0,
    timeout_publish: float = 600.0,
    emit: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """
    Нажимает «Publish campaign» и ждёт признаков публикации.
    На успехе пытается вытащить и вернуть Google tag snippet.
    """
    t0 = time.time()
    _maybe_handle_confirm_async(driver, emit=emit, timeout=6.0)

    # 1) Click «Publish…»
    _emit(emit, "Жму «Publish campaign»")
    clicked = _click_publish(driver, timeout_click=timeout_click)
    if not clicked:
        _emit(emit, "Не удалось нажать кнопку публикации — стоп")
        raise RuntimeError("Кнопка «Publish campaign» не найдена/не нажалась.")

    # 2) Фиксированная пауза вместо ожидания конкретного подтверждения
    wait_seconds = 10.0
    try:
        timeout_publish_val = float(timeout_publish)
    except (TypeError, ValueError):
        timeout_publish_val = None
    if timeout_publish_val is not None and timeout_publish_val < wait_seconds:
        wait_seconds = max(0.0, timeout_publish_val)
    wait_seconds_display = f"{wait_seconds:g}"
    _emit(emit, f"Жду {wait_seconds_display} секунд после клика — без ожидания конкретной надписи")
    end_wait = time.time() + wait_seconds
    while time.time() < end_wait:
        _maybe_handle_confirm_its_you(driver, emit=emit)
        time.sleep(0.35)

    # Проверяем, не появились ли полезные подсказки, но успех фиксируем в любом случае
    indicator, snippet = _check_publish_signals_and_snippet(driver)
    if indicator:
        _emit(emit, "Публикация подтверждена ✨")
    else:
        indicator = "manual_wait"
        _emit(emit, "Завершаю шаг после фиксированной паузы — считаю кампанию опубликованной")

    published_ok = True

    if snippet:
        _emit(emit, "Нашёл Google tag — копирую и сохраняю")
        _try_click_copy_for_snippet(driver)

    elapsed = int((time.time() - t0) * 1000)
    logger.info("step10: published_ok=%s, indicator=%s, snippet_len=%d, elapsed=%d ms",
                published_ok, indicator, len(snippet or ""), elapsed)

    # Возвращаем результат (контекст попадёт в БД через create_companies)
    return {
        "published_ok": published_ok,
        "published_indicator": indicator,
        "gtag_snippet": snippet or "",
        "clicked": clicked,
        "duration_ms": elapsed,
    }
