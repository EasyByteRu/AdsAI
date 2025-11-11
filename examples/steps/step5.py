# -*- coding: utf-8 -*-
"""
examples/steps/step5.py

Шаг 5 Google Ads Wizard:
  — Просто нажать кнопку "Skip" на шаге генерации ассетов и дождаться прогресса.
  — НОВОЕ: если в любой момент всплывает окно "Confirm it's you" — проходим 2FA
    через examples.steps.code_for_confrim и продолжаем сценарий.

Контракт:
    run_step5(
        driver: WebDriver,
        *,
        timeout_total: float = 60.0,
        emit: Optional[Callable[[str], None]] = None,  # необязательный колбэк комментариев
    ) -> dict

Возврат:
    {
      "skipped": bool,
      "new_url": str,
      "duration_ms": int
    }
"""

from __future__ import annotations

import logging
import time
from typing import Optional, Dict, Callable

from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver, WebElement
from selenium.common.exceptions import StaleElementReferenceException

# === Confirm-it's-you (2FA) хелпер ===
try:
    # модуль-обработчик из examples/steps/
    from examples.steps.code_for_confrim import (
        handle_confirm_its_you,
        wait_code_from_env_or_file,
    )
except Exception:  # pragma: no cover
    # если модуль не найден — делаем «пустышки» (не ломаем совместимость)
    def handle_confirm_its_you(*args, **kwargs) -> bool:  # type: ignore
        return False
    def wait_code_from_env_or_file(*args, **kwargs) -> Optional[str]:  # type: ignore
        return None


logger = logging.getLogger("ads_ai.gads.step5")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")


def _emit(emit: Optional[Callable[[str], None]], text: str) -> None:
    """Безопасно отправляет короткий комментарий в UI."""
    if callable(emit) and isinstance(text, str) and text.strip():
        try:
            emit(text.strip())
        except Exception:
            pass


# ======== I18N ========

SKIP_SYNS = [
    "skip", "пропустить", "пропуск", "omitir", "saltar", "ignorar", "pular",
    "ignorer", "passer", "überspringen", "überspringe", "auslassen",
    "saltare", "salta", "pomiń", "pomin", "pomijaj",
    "geç", "atla", "пропустити", "пропустити це",
    "跳过", "跳過", "スキップ", "건너뛰기",
    "lewati", "bỏ qua", " bỏ qua",
]

AVOID_SYNS = [
    "generate", "assets", "generate assets", "создать", "создание", "сгенерировать", "генерировать",
    "next", "продолжить", "далее", "back", "назад",
    "more settings", "advanced", "options", "learn more", "подробнее", "настройки",
    "cancel", "отмена",
]


# ======== Confirm watcher ========

def _maybe_handle_confirm_its_you(driver: WebDriver, emit: Optional[Callable[[str], None]]) -> bool:
    """
    Лёгкий «пробник»: если окна подтверждения личности нет — возвращает False (ничего не делает).
    Если диалог уже открыт — вызывает полный проход 2FA (вплоть до ожидания кода),
    по завершении возвращает True. Исключения наружу не выбрасывает.
    """
    try:
        return bool(handle_confirm_its_you(
            driver,
            emit=emit,
            wait_code_cb=wait_code_from_env_or_file,
            timeout_total=180.0,
            max_attempts=3,
        ))
    except Exception:
        return False


# ======== Utils ========

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


def _dismiss_soft_dialogs(driver: WebDriver, budget_ms: int = 800) -> None:
    """Лёгкие попапы (cookies/ok/got it) — закрываем мягко."""
    t0 = time.time()
    CAND = ["accept all", "i agree", "agree", "got it", "ok",
            "принять все", "я согласен", "понятно", "хорошо",
            "同意", "接受", "确定", "知道了", "好"]
    while (time.time() - t0) * 1000 < budget_ms:
        try:
            dialogs = driver.find_elements(By.CSS_SELECTOR, '[role="dialog"], div[aria-modal="true"], .mdc-dialog--open')
            hit = False
            for dlg in dialogs:
                if not _is_interactable(driver, dlg):
                    continue
                for b in dlg.find_elements(By.CSS_SELECTOR, 'button,[role=button],a[role=button]'):
                    txt = ((b.text or "") + " " + (b.get_attribute("aria-label") or "")).strip().lower()
                    if txt and any(w in txt for w in CAND):
                        try:
                            b.click()
                        except Exception:
                            try:
                                driver.execute_script("arguments[0].click();", b)
                            except Exception:
                                continue
                        time.sleep(0.12)
                        hit = True
                        break
                if hit:
                    break
            if not hit:
                break
        except Exception:
            break


def _bounce_scroll(driver: WebDriver) -> None:
    """Принудительно «поднимаем» и «опускаем» страницу, чтобы дорисовался футер с кнопками."""
    try:
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(0.06)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.06)
        driver.execute_script("window.scrollTo(0, Math.floor(document.body.scrollHeight*0.7));")
        time.sleep(0.05)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    except Exception:
        pass


# ======== Skip detection / click ========

def _find_skip_button(driver: WebDriver) -> Optional[WebElement]:
    """
    Ищем именно кнопку Skip. Приоритет:
      1) .buttons .button-skip
      2) Любая <button> с текстом из SKIP_SYNS, исключая AVOID_SYNS и контейнеры с дополнительными настройками
    """
    _maybe_handle_confirm_its_you(driver, emit=None)

    # 1) Жёсткие селекторы
    for sel in [
        '.buttons button.button-skip',
        'gac-flow-buttons .buttons button.button-skip',
        'button.button-skip',
    ]:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            if _is_interactable(driver, el):
                return el
        except Exception:
            continue

    # 2) По тексту/aria-label
    try:
        el = driver.execute_script(
            """
            const SKIP = new Set(arguments[0].map(s=>String(s||'').toLowerCase()));
            const AVOID = new Set(arguments[1].map(s=>String(s||'').toLowerCase()));
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e), r=e.getBoundingClientRect();
              if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2||cs.pointerEvents==='none') return false;
              return r.width>8 && r.height>8 && r.bottom>0 && r.right>0;};
            const notDisabled=e=>!( (e.getAttribute('aria-disabled')||'').toLowerCase()==='true' || e.hasAttribute('disabled') );
            const badRoot = (n)=>!!(n.closest('.additionalSettingsContainer') || n.closest('.more-settings-panel') || n.closest('[role="dialog"], modal, [aria-modal="true"]'));
            const nodes = [...document.querySelectorAll('gac-flow-buttons .buttons button, .buttons button, button')].filter(isVis).filter(notDisabled);
            let best=null, score=-1;
            for(const n of nodes){
              if(badRoot(n)) continue;
              const cls=(n.className||'').toLowerCase();
              const txt=((n.innerText||n.textContent||'')+' '+(n.getAttribute('aria-label')||'')).trim().toLowerCase();
              if([...AVOID].some(a=> txt.includes(a))) continue;
              const isSkip = cls.includes('button-skip') || [...SKIP].some(w=> txt===w || txt.includes(w));
              if(!isSkip) continue;
              let s=0;
              if(cls.includes('button-skip')) s+=10;
              if([...SKIP].some(w=> txt.includes(w))) s+=6;
              try{ const r=n.getBoundingClientRect(); s+= Math.min(6, Math.max(0, Math.floor((r.top/Math.max(1, innerHeight))*6))); }catch(e){}
              if(s>score){ best=n; score=s; }
            }
            return best||null;
            """,
            SKIP_SYNS, AVOID_SYNS
        )
        if el:
            return el
    except Exception:
        pass

    return None


def _click_skip(driver: WebDriver) -> Optional[WebElement]:
    _bounce_scroll(driver)
    _maybe_handle_confirm_its_you(driver, emit=None)

    btn = _find_skip_button(driver)
    if not btn:
        logger.warning("Skip button not found.")
        return None
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
        time.sleep(0.05)
        btn.click()
        return btn
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", btn)
            return btn
        except Exception:
            try:
                # синтетический клик по центру
                driver.execute_script(
                    """
                    const el=arguments[0];
                    const r=el.getBoundingClientRect();
                    const x=Math.floor(r.left + Math.max(2, r.width/2));
                    const y=Math.floor(r.top  + Math.max(2, r.height/2));
                    const mk=(t)=>new MouseEvent(t,{view:window,bubbles:true,cancelable:true,clientX:x,clientY:y});
                    el.dispatchEvent(mk('mousedown')); el.dispatchEvent(mk('mouseup')); el.dispatchEvent(mk('click'));
                    """,
                    btn
                )
                return btn
            except Exception:
                return None


def _wait_progress_after_skip(
    driver: WebDriver,
    old_url: str,
    btn: Optional[WebElement],
    timeout: float = 30.0,
    emit: Optional[Callable[[str], None]] = None,
) -> bool:
    """
    Ждём смену URL или исчезновение/инвалидность кнопки Skip.
    По пути «прощупываем» окно "Confirm it's you": если всплыло — проходим 2FA и продолжаем.
    """
    end = time.time() + max(5.0, timeout)
    while time.time() < end:
        # Confirm 2FA probe (не блокирует, если диалога нет)
        _maybe_handle_confirm_its_you(driver, emit=emit)

        cur = driver.current_url or ""
        if cur != old_url:
            return True
        if btn is not None:
            try:
                # если кнопка стала недоступной/скрытой/стала stale — считаем прогресс
                if not btn.is_displayed():
                    return True
            except StaleElementReferenceException:
                return True
            except Exception:
                return True
        time.sleep(0.2)
    # финальная проверка + один проход Confirm (на случай гонки)
    _maybe_handle_confirm_its_you(driver, emit=emit)
    return (driver.current_url or "") != old_url


# ======== Public API ========

def run_step5(
    driver: WebDriver,
    *,
    timeout_total: float = 60.0,
    emit: Optional[Callable[[str], None]] = None,
) -> Dict[str, object]:
    """
    Нажать "Skip" и дождаться перехода на следующий шаг мастера.
    Параллельно везде «прощупывает» появление окна Confirm-it's-you и, если оно всплыло,
    проходит 2FA без разрыва сценария.
    """
    t0 = time.time()
    _dismiss_soft_dialogs(driver, budget_ms=700)

    # На всякий — короткая проверка Confirm в начале
    _maybe_handle_confirm_its_you(driver, emit=emit)

    old_url = driver.current_url or ""
    _emit(emit, "Пропускаю генерацию ассетов: жму «Skip»")
    logger.info("step5: нажимаю Skip…")
    btn = _click_skip(driver)
    if btn is None:
        # вторая попытка после мягкой зачистки и «раскачки»
        _emit(emit, "Кнопку «Пропустить» не вижу — пробую ещё раз")
        _dismiss_soft_dialogs(driver, budget_ms=800)
        _bounce_scroll(driver)
        _maybe_handle_confirm_its_you(driver, emit=emit)
        btn = _click_skip(driver)
        if btn is None:
            _emit(emit, "Кнопку «Пропустить» не нашёл — останавливаюсь")
            raise RuntimeError("Step5: кнопка Skip не найдена или не нажалась.")

    if not _wait_progress_after_skip(driver, old_url, btn, timeout=timeout_total, emit=emit):
        # ещё одна попытка клика, иногда первый клик триггерит ленивую загрузку
        _emit(emit, "Переход не подтвердился — нажимаю «Пропустить» повторно")
        _dismiss_soft_dialogs(driver, budget_ms=600)
        _maybe_handle_confirm_its_you(driver, emit=emit)
        btn2 = _click_skip(driver) | btn  # bitwise OR безопасен, но вернём любой «живой» WebElement
        if not _wait_progress_after_skip(driver, old_url, btn2, timeout=max(12.0, timeout_total/2), emit=emit):
            _emit(emit, "Переход после «Пропустить» не произошёл — стоп")
            raise RuntimeError("Step5: переход после Skip не произошёл по таймауту.")

    new_url = driver.current_url or ""
    elapsed = int((time.time() - t0) * 1000)
    logger.info("step5: OK (%d ms). URL: %s", elapsed, new_url)
    _emit(emit, "Готово — перешёл дальше")
    return {
        "skipped": True,
        "new_url": new_url,
        "duration_ms": elapsed,
    }
