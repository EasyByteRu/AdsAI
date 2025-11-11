# -*- coding: utf-8 -*-
"""
bulk_remove.py — выбор кампаний по именам на /aw/campaigns, отметка чекбоксов и удаление (Remove).

Публичные функции:
    • remove_campaigns_by_names(driver, names, *, open_url=True, timeout=120.0, emit=None) -> dict
    • init_bulk_remove(app, settings) -> None   # регистрирует POST /api/gads/bulk_remove

Контракт remove_campaigns_by_names:
    {
      "matched": [str, ...],          # какие имена реально нашли на странице
      "not_found": [str, ...],        # какие не нашли (по текущей странице)
      "selected_count": int,          # сколько строк отмечено чекбоксами
      "remove_clicked": bool,         # получилось кликнуть пункт Remove в меню (toolbelt/row)
      "confirm_clicked": bool,        # получилось подтвердить в диалоге (Confirm/Remove)
      "duration_ms": int,             # длительность операции
    }

Дополнительно:
- Эндпоинт принимает profile_id ИЛИ company_id (по БД), чтобы запускать ровно тот AdsPower‑профиль, где создана кампания.
- Тумблер HEADLESS_SWITCH (1/0) вверху файла задаёт дефолтный headless-режим. ENV ADS_AI_HEADLESS также поддерживается.

Основное: приоритетный путь удаления — через верхнюю плашку (Secondary toolbelt):
  кнопка "Edit" → пункт "Remove" → диалог "Confirm".
Резерв: выпадающее «меню статуса» в строке.
"""

from __future__ import annotations

import importlib
import os
import sys
import time
import re
import threading
from typing import Iterable, Optional, Callable, Dict, List, Tuple, Any

# ---------- Flask ----------
from flask import Flask, Response, jsonify, request, session

try:
    from ads_ai.config.settings import Settings  # noqa: F401
except Exception:  # pragma: no cover
    class Settings:  # простая заглушка
        pass

# ---------- Selenium ----------
from selenium.webdriver.remote.webdriver import WebDriver, WebElement
from selenium.webdriver.common.by import By


# =============================================================================
#                           HEADLESS SWITCH (1/0)
# =============================================================================

# 1 = headless ВКЛ, 0 = ВЫКЛ. Можно переопределить через ENV ADS_AI_HEADLESS=1/0
HEADLESS_SWITCH: int = 1
try:
    _env = os.getenv("ADS_AI_HEADLESS")
    if _env is not None:
        HEADLESS_SWITCH = 1 if str(_env).strip().lower() in ("1", "true", "on", "yes") else 0
except Exception:
    pass
HEADLESS_DEFAULT: bool = bool(HEADLESS_SWITCH)


def _as_bool(v: Any, default: Optional[bool] = None) -> bool:
    """Надёжный парсер bool: понимает 1/0/true/false/on/off/yes/no."""
    if v is None:
        return bool(default) if default is not None else False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        try:
            return bool(int(v))
        except Exception:
            return bool(default) if default is not None else False
    s = str(v).strip().lower()
    if s in ("1", "true", "t", "y", "yes", "on"):
        return True
    if s in ("0", "false", "f", "n", "no", "off"):
        return False
    return bool(default) if default is not None else False


# =============================================================================
#                       Доступ к общему WebDriver из /console
# =============================================================================

def _get_app_state() -> Any:
    """
    Достаём ads_ai.web.app._state — в нём должны быть driver, lock, busy.
    Без прямых импортов, чтобы не ловить циклические зависимости.
    """
    cand_modules = [
        sys.modules.get("ads_ai.web.app"),
        sys.modules.get("__main__"),
        sys.modules.get("app"),
    ]
    for mod in cand_modules:
        if not mod:
            continue
        try:
            state = getattr(mod, "_state", None)
        except Exception:
            state = None
        if state is not None:
            return state
    try:
        mod = importlib.import_module("ads_ai.web.app")
        return getattr(mod, "_state", None)
    except Exception:
        return None


def _acquire_shared_driver(timeout: float = 25.0) -> Tuple[Optional[Tuple[Any, Any, Any]], Optional[str]]:
    """
    Пытаемся занять общий WebDriver из консоли (/console). Возвращаем (driver, state, lock).
    Вторая компонента — строковый код ошибки при неудаче.
    """
    state = _get_app_state()
    if not state:
        return None, "app_state_missing"
    driver = getattr(state, "driver", None)
    lock = getattr(state, "lock", None)
    if driver is None or lock is None:
        return None, "driver_unavailable"

    lock_wait = max(0.1, float(timeout))
    try:
        acquired = lock.acquire(timeout=lock_wait)
    except Exception:
        return None, "driver_lock_error"
    if not acquired:
        return None, "driver_lock_timeout"

    busy_wait_until = time.time() + min(lock_wait, 15.0)
    stale_after = float(getattr(state, "busy_stale_sec", 180.0) or 180.0)
    while getattr(state, "busy", False):
        busy_since = float(getattr(state, "busy_since", 0.0) or 0.0)
        now = time.time()
        if busy_since and stale_after > 0 and (now - busy_since) > stale_after:
            # считаем «зависшую» занятость — сбрасываем
            try:
                state.busy = False
                setattr(state, "busy_since", 0.0)
            except Exception:
                pass
            break
        if now >= busy_wait_until:
            try:
                lock.release()
            except Exception:
                pass
            return None, "driver_busy"
        remaining = max(0.05, busy_wait_until - now)
        try:
            lock.release()
        except Exception:
            pass
        time.sleep(min(0.4, remaining))
        try:
            reacquired = lock.acquire(timeout=remaining)
        except Exception:
            return None, "driver_lock_error"
        if not reacquired:
            return None, "driver_lock_timeout"
    try:
        state.busy = True
        setattr(state, "busy_since", time.time())
    except Exception:
        pass
    return (driver, state, lock), None


def _release_shared_driver(holder: Optional[Tuple[Any, Any, Any]]) -> None:
    if not holder:
        return
    _, state, lock = holder
    try:
        setattr(state, "busy", False)
        setattr(state, "busy_since", 0.0)
    except Exception:
        pass
    try:
        lock.release()
    except Exception:
        pass


# =============================================================================
#               ЛОКАЛЬНЫЙ МЕНЕДЖЕР ДРАЙВЕРОВ (по profile_id)
# =============================================================================

class _LocalDrv:
    def __init__(self) -> None:
        self.driver: Optional[Any] = None
        self.profile_id: Optional[str] = None
        self.headless: bool = HEADLESS_DEFAULT
        self.lock = threading.RLock()

_local = _LocalDrv()


def _ensure_big_viewport(driver: Any) -> None:
    """Гарантируем нормальный вьюпорт для стабильной отрисовки/кликов (через CDP)."""
    try:
        dims = driver.execute_script(
            "return {w: window.innerWidth||0, h: window.innerHeight||0, dpr: window.devicePixelRatio||1}"
        ) or {}
        w = int(dims.get("w") or 0)
        h = int(dims.get("h") or 0)
        if w >= 1280 and h >= 720:
            return
    except Exception:
        pass
    try:
        target_w = int(os.getenv("ADS_AI_PREVIEW_W", "1600"))
        target_h = int(os.getenv("ADS_AI_PREVIEW_H", "1000"))
        target_dpr = float(os.getenv("ADS_AI_PREVIEW_DPR", "1"))
        driver.execute_cdp_cmd("Emulation.setDeviceMetricsOverride", {
            "mobile": False,
            "width": target_w,
            "height": target_h,
            "deviceScaleFactor": target_dpr,
            "screenWidth": target_w,
            "screenHeight": target_h,
        })
        try:
            driver.execute_cdp_cmd("Emulation.setVisibleSize", {"width": target_w, "height": target_h})
        except Exception:
            pass
        try:
            driver.execute_cdp_cmd("Emulation.setPageScaleFactor", {"pageScaleFactor": 1})
        except Exception:
            pass
    except Exception:
        pass


def _start_driver(profile_id: str, *, headless: bool) -> Any:
    """
    Пытаемся стартовать AdsPower профиль (приоритет). Если недоступно — обычный Chrome.
    """
    # 1) AdsPower
    try:
        adsp = importlib.import_module("ads_ai.browser.adspower")
    except Exception:
        adsp = None

    if adsp is not None:
        start_fn = getattr(adsp, "start_adspower", None) or getattr(adsp, "start", None)
        if start_fn:
            api_base = os.getenv("ADSP_API_BASE") or "http://local.adspower.net:50325"
            token = os.getenv("ADSP_API_TOKEN") or ""
            try:
                drv = start_fn(
                    profile=profile_id,
                    headless=headless,
                    api_base=api_base,
                    token=token,
                    window_size="1440,900",
                )
                try:
                    setattr(drv, "_adspower_profile_id", profile_id)
                    drv.set_page_load_timeout(25)
                    drv.set_script_timeout(15)
                except Exception:
                    pass
                _ensure_big_viewport(drv)
                return drv
            except Exception:
                pass

    # 2) Фоллбек: обычный Chrome
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
    except Exception as e:
        raise RuntimeError(f"Невозможно запустить браузер: {e}")

    opts = Options()
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1440,900")
    if headless:
        opts.add_argument("--headless=new")
        opts.add_argument("--hide-scrollbars")
        opts.add_argument("--mute-audio")

    drv = webdriver.Chrome(options=opts)
    try:
        setattr(drv, "_adspower_profile_id", profile_id)
    except Exception:
        pass
    _ensure_big_viewport(drv)
    return drv


def _get_or_create_driver(profile_id: str, *, headless: bool) -> Any:
    """
    Возвращает драйвер, гарантированно привязанный к данному profile_id/headless.
    """
    d = _local.driver
    if d is not None and _local.profile_id == profile_id and _local.headless == headless:
        _ensure_big_viewport(d)
        return d

    with _local.lock:
        # закрыть предыдущий, если другой профиль/режим
        if _local.driver is not None and (_local.profile_id != profile_id or _local.headless != headless):
            try:
                _local.driver.quit()
            except Exception:
                pass
            _local.driver = None
            _local.profile_id = None

        if _local.driver is None:
            drv = _start_driver(profile_id, headless=headless)
            _local.driver = drv
            _local.profile_id = profile_id
            _local.headless = headless
            try:
                drv.get("https://ads.google.com/aw/overview")
            except Exception:
                pass
            _ensure_big_viewport(drv)

    return _local.driver


def _close_local_driver() -> None:
    """
    Мягко закрывает локальный драйвер (ветка profile_id/company_id) и очищает состояние.
    Общий драйвер из /console сюда не попадает.
    """
    with _local.lock:
        drv = _local.driver
        _local.driver = None
        _local.profile_id = None
        try:
            if drv is not None:
                drv.quit()
        except Exception:
            pass


# =============================================================================
#                                Вспомогалки Selenium
# =============================================================================

def _emit(emit: Optional[Callable[[str], None]], text: str) -> None:
    if callable(emit) and text and text.strip():
        try:
            emit(text.strip())
        except Exception:
            pass


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
    """Три способа клика: .click → JS click → синтетическая мышь по центру прямоугольника."""
    try:
        if not _is_interactable(driver, el):
            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            except Exception:
                pass
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
                    el
                )
                return True
            except Exception:
                return False


def _dismiss_soft_dialogs(driver: WebDriver, budget_ms: int = 900) -> None:
    """Закрываем мягкие диалоги (cookies / got it / ok)."""
    t0 = time.time()
    CAND = [
        "accept all", "i agree", "agree", "got it", "ok",
        "принять все", "я согласен", "понятно", "хорошо",
        "同意", "接受", "确定", "知道了", "好",
    ]
    while (time.time() - t0) * 1000 < budget_ms:
        try:
            dialogs = driver.find_elements(By.CSS_SELECTOR, '[role="dialog"], div[aria-modal="true"], .mdc-dialog--open')
            hit = False
            for dlg in dialogs:
                if not _is_interactable(driver, dlg):
                    continue
                for b in dlg.find_elements(By.CSS_SELECTOR, 'button,[role="button"],a[role="button"]'):
                    txt = ((b.text or "") + " " + (b.get_attribute("aria-label") or "")).strip().lower()
                    if txt and any(w in txt for w in CAND):
                        try:
                            b.click()
                        except Exception:
                            try:
                                driver.execute_script("arguments[0].click();", b)
                            except Exception:
                                continue
                        time.sleep(0.18)
                        hit = True
                        break
                if hit:
                    break
            if not hit:
                break
        except Exception:
            break


def _await_table_ready(driver: WebDriver, timeout: float = 18.0) -> bool:
    """Ждём появление основной ESS-таблицы кампаний (role=grid)."""
    end = time.time() + max(2.0, timeout)
    while time.time() < end:
        try:
            ok = driver.execute_script(
                """
                const isVis=(e)=>{if(!e) return false;
                  const cs=getComputedStyle(e),r=e.getBoundingClientRect();
                  if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.15||cs.pointerEvents==='none') return false;
                  return r.width>100 && r.height>60 && r.bottom>0 && r.right>0;};
                const grids=[...document.querySelectorAll('.ess-table-canvas[role="grid"], [role="grid"][aria-label*="Campaign"]')].filter(isVis);
                return grids.length>0;
                """
            )
            if ok:
                return True
        except Exception:
            pass
        time.sleep(0.25)
    return False


def _await_toolbelt_selected(driver: WebDriver, timeout: float = 6.0) -> bool:
    """Ждём появления плашки Toolbelt с текстом 'selected'."""
    end = time.time() + max(1.5, timeout)
    while time.time() < end:
        try:
            vis = driver.execute_script(
                """
                const isVis=(e)=>{ if(!e) return false;
                   const cs=getComputedStyle(e), r=e.getBoundingClientRect();
                   if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.15) return false;
                   return r.width>200 && r.height>30 && r.bottom>0 && r.right>0; };
                const tb=[...document.querySelectorAll('toolbelt-bar.secondary-toolbelt,[aria-label*="Secondary toolbelt"]')].filter(isVis);
                if(!tb.length) return false;
                const txt=(tb[0].innerText||'').toLowerCase();
                return txt.includes('selected');
                """
            )
            if vis:
                return True
        except Exception:
            pass
        time.sleep(0.15)
    return False


def _select_rows_by_names(driver: WebDriver, names: List[str]) -> Tuple[int, List[str], List[str], List[WebElement]]:
    """
    Отмечает чекбоксы для строк, где имя кампании совпало (полное совпадение или вхождение, регистронезависимо).
    Возвращает (selected_count, matched_names, not_found_names, rows_selected)
    """
    names_clean = [re.sub(r"\s+", " ", str(n or "")).strip() for n in names]
    want = [n for n in names_clean if n]
    want_l = [n.lower() for n in want]
    if not want:
        return 0, [], names_clean, []

    rows = driver.execute_script(
        """
        const wanted = new Set(arguments[0] || []);
        const rows = [...document.querySelectorAll('.particle-table-row')];
        const hits = [];
        for (const row of rows) {
          const a = row.querySelector('campaign-name-generic a.ess-cell-link, a.ess-cell-link[aria-label]');
          const txt = ((a?.innerText||a?.textContent||a?.getAttribute('aria-label')||'')+'').trim().toLowerCase();
          if (!txt) continue;
          for (const w of wanted) {
            if (txt===w || txt.includes(w)) { hits.push(row); break; }
          }
        }
        return hits;
        """,
        want_l
    ) or []

    matched: List[str] = []
    selected_rows: List[WebElement] = []

    for row in rows:
        # разные версии Angular/Material: несколько вариантов чекбокса
        cb: Optional[WebElement] = None
        for sel in (
            "tools-cell mat-checkbox .mat-checkbox-inner-container",
            "mat-checkbox .mat-checkbox-inner-container",
            "mat-checkbox .mat-checkbox-container",
            "mat-checkbox input[type='checkbox']",
            "[role='checkbox']",
        ):
            try:
                cb = row.find_element(By.CSS_SELECTOR, sel)
                if cb:
                    break
            except Exception:
                cb = None
        if not cb:
            continue

        if _robust_click(driver, cb):
            selected_rows.append(row)
            try:
                a = row.find_element(By.CSS_SELECTOR, "campaign-name-generic a.ess-cell-link, a.ess-cell-link[aria-label]")
                nm = ((a.text or "") or (a.get_attribute("aria-label") or "")).strip()
                if nm:
                    matched.append(nm)
            except Exception:
                pass
            time.sleep(0.06)

    matched_lower = [m.lower() for m in matched]
    not_found = [orig for (orig, low) in zip(want, want_l)
                 if all((low not in m) and (m not in low) for m in matched_lower)]

    return len(selected_rows), matched, not_found, selected_rows


def _open_status_menu_for_row(driver: WebDriver, row: WebElement) -> bool:
    """Резервный путь: в строке кликаем иконку выпадающего меню статуса."""
    try:
        el = row.find_element(By.CSS_SELECTOR, ".ess-edit-icon.ess-dropdown-icon, .ess-edit-icon.ess-dropdown-icon.transparent")
        return _robust_click(driver, el)
    except Exception:
        try:
            el = row.find_element(By.CSS_SELECTOR, "[aria-label*='Edit'][aria-haspopup='true']")
            return _robust_click(driver, el)
        except Exception:
            return False


def _open_toolbelt_edit_menu(driver: WebDriver) -> bool:
    """
    Приоритетный путь: в верхней плашке (Secondary toolbelt) кликаем кнопку "Edit" (aria-label='Bulk edit' или текст "Edit").
    """
    # Ждём появления самой плашки (после выбора чекбоксов)
    _await_toolbelt_selected(driver, timeout=6.0)

    # Пытаемся найти кнопку "Edit"
    for _ in range(8):
        try:
            el = driver.execute_script(
                """
                const isVis=e=>{ if(!e) return false;
                  const cs=getComputedStyle(e), r=e.getBoundingClientRect();
                  if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.15) return false;
                  return r.width>40 && r.height>20 && r.bottom>0 && r.right>0; };

                const bars=[...document.querySelectorAll('toolbelt-bar.secondary-toolbelt,[aria-label*="Secondary toolbelt"]')].filter(isVis);
                const pick=(n)=>{
                  const t=((n.getAttribute('aria-label')||'')+' '+(n.innerText||n.textContent||'')).trim().toLowerCase();
                  return t.includes('bulk edit') || t==='edit' || t.includes(' edit');
                };

                for (const bar of bars){
                  const btns=[...bar.querySelectorAll('.trigger-button, material-button, [role="button"]')].filter(isVis);
                  for(const b of btns){ if(pick(b)) return b; }
                }
                // fallback: глобальный поиск (на случай когда DOM размонтирован странно)
                const pool=[...document.querySelectorAll('material-button.trigger-button,[role="button"]')].filter(isVis);
                for(const b of pool){ if(pick(b)) return b; }
                return null;
                """
            )
            if el and _robust_click(driver, el):  # type: ignore
                time.sleep(0.2)
                return True
        except Exception:
            pass
        time.sleep(0.15)
    return False


def _await_any_menu_popup(driver: WebDriver, timeout: float = 6.0) -> bool:
    """Ждём появления popup меню (toolbelt/row) — material-popup / popup-wrapper .visible."""
    end = time.time() + max(1.5, timeout)
    while time.time() < end:
        try:
            ok = driver.execute_script(
                """
                const isVis=e=>{ if(!e) return false;
                  const cs=getComputedStyle(e), r=e.getBoundingClientRect();
                  if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.15) return false;
                  return r.width>120 && r.height>60 && r.bottom>0 && r.right>0; };

                const pop1=[...document.querySelectorAll('menu-popup .popup, material-popup.popup, .menu-popup.popup')].filter(isVis);
                const pop2=[...document.querySelectorAll('.popup-wrapper.visible')].filter(isVis);
                return (pop1.length + pop2.length) > 0;
                """
            )
            if ok:
                return True
        except Exception:
            pass
        time.sleep(0.12)
    return False


def _click_remove_in_open_menu(driver: WebDriver) -> bool:
    """В уже открытом меню находим пункт Remove/Удалить и кликаем."""
    for _ in range(14):
        try:
            el = driver.execute_script(
                """
                const isVis=e=>{ if(!e) return false;
                  const cs=getComputedStyle(e), r=e.getBoundingClientRect();
                  if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.15) return false;
                  return r.width>30 && r.height>16 && r.bottom>0 && r.right>0; };
                const pool = [
                  ...document.querySelectorAll('material-select-item.menu-item[role="menuitem"], [role="menuitem"], material-select-item.menu-item')
                ].filter(isVis);
                const pick = (node)=> {
                  const txt = ((node.getAttribute('aria-label')||'')+' '+(node.innerText||node.textContent||'')).trim().toLowerCase();
                  if (!txt) return false;
                  return txt.includes('remove') || txt.includes('удалить');
                };
                for (const n of pool) { if (pick(n)) return n; }
                return null;
                """
            )
            if el and _robust_click(driver, el):  # type: ignore
                return True
        except Exception:
            pass
        time.sleep(0.15)
    return False


def _confirm_remove_dialog_if_any(driver: WebDriver, timeout: float = 18.0) -> bool:
    """
    Если открылся диалог подтверждения — кликаем «Confirm»/«Remove»/«Удалить»/«Подтвердить».
    Игнорируем «Cancel/Отмена».
    """
    end = time.time() + max(4.0, timeout)
    while time.time() < end:
        try:
            btn = driver.execute_script(
                """
                const isVis=e=>{ if(!e) return false;
                  const cs=getComputedStyle(e), r=e.getBoundingClientRect();
                  if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.15) return false;
                  return r.width>40 && r.height>20 && r.bottom>0 && r.right>0; };

                const NEG = ['cancel', 'отмена'];
                const POS = ['confirm', 'remove', 'удалить', 'подтвердить', 'yes', 'delete'];

                const dialogs=[...document.querySelectorAll('[role="dialog"], material-dialog, .mdc-dialog--open')].filter(isVis);

                // эвристика: берем кнопки из футера/контейнера действий
                const buttons = [];
                for (const d of dialogs){
                  // дополнительно учитываем заголовок «permanently remove» для уверенности
                  const lab=((d.getAttribute('aria-label')||'')+' '+(d.innerText||'')).toLowerCase();
                  const headerHit = lab.includes('permanently remove') || lab.includes('remove ') || lab.includes('удалить');
                  const pool=[...d.querySelectorAll('button,[role=button],material-button,a[role=button]')].filter(isVis);
                  for (const b of pool){
                    const t=((b.getAttribute('aria-label')||'')+' '+(b.innerText||b.textContent||'')).trim().toLowerCase();
                    if (!t) continue;
                    // исключаем cancel/отмена
                    if (NEG.some(k=>t.includes(k))) continue;
                    // целимся в confirm/remove
                    if (POS.some(k=>t.includes(k)) || headerHit){
                      buttons.push(b);
                    }
                  }
                }
                // предпочитаем те, где текст явно POS
                const score = (b)=>{
                  const t=((b.getAttribute('aria-label')||'')+' '+(b.innerText||b.textContent||'')).trim().toLowerCase();
                  let s = 0;
                  for (const k of POS){ if (t.includes(k)) s+=2; }
                  for (const k of NEG){ if (t.includes(k)) s-=3; }
                  return s;
                };
                buttons.sort((a,b)=>score(b)-score(a));
                return buttons[0] || null;
                """
            )
            if btn and _robust_click(driver, btn):  # type: ignore
                return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


# =============================================================================
#                         Основная функция удаления кампаний
# =============================================================================

def remove_campaigns_by_names(
    driver: WebDriver,
    names: Iterable[str],
    *,
    open_url: bool = True,
    timeout: float = 120.0,
    emit: Optional[Callable[[str], None]] = None,
) -> Dict[str, object]:
    """
    Открывает список кампаний, отмечает чекбоксы у заданных имён и жмёт Remove через плашку Edit (toolbelt).
    Резервный путь — меню в строке.
    """
    t0 = time.time()
    _emit(emit, "Готовлю список кампаний…")

    if open_url:
        try:
            driver.get("https://ads.google.com/aw/campaigns")
        except Exception:
            pass

    if not _await_table_ready(driver, timeout=min(18.0, timeout * 0.3)):
        _dismiss_soft_dialogs(driver, budget_ms=900)
        if not _await_table_ready(driver, timeout=10.0):
            raise RuntimeError("Таблица кампаний не доступна (role=grid не найден).")

    _dismiss_soft_dialogs(driver, budget_ms=700)

    clean_names = [str(n or "").strip() for n in names]
    _emit(emit, f"Ищу кампании: {', '.join([n for n in clean_names if n][:6])}{'…' if len(clean_names)>6 else ''}")
    selected_count, matched, not_found, rows = _select_rows_by_names(driver, clean_names)

    if selected_count <= 0:
        return {
            "matched": matched,
            "not_found": not_found or clean_names,
            "selected_count": 0,
            "remove_clicked": False,
            "confirm_clicked": False,
            "duration_ms": int((time.time() - t0) * 1000),
        }

    _emit(emit, f"Отмечено чекбоксами: {selected_count}")

    remove_clicked = False
    confirm_clicked = False

    # === Приоритет: верхняя плашка Edit → Remove ===
    try:
        if _open_toolbelt_edit_menu(driver):
            if _await_any_menu_popup(driver, timeout=6.0):
                remove_clicked = _click_remove_in_open_menu(driver)
    except Exception:
        pass

    # === Резерв: меню в строке ===
    if not remove_clicked:
        try:
            if rows and _open_status_menu_for_row(driver, rows[0]):
                time.sleep(0.2)
                remove_clicked = _click_remove_in_open_menu(driver)
        except Exception:
            pass

    if not remove_clicked:
        _emit(emit, "Не удалось найти пункт «Remove» ни в Toolbelt, ни в меню строки.")
        return {
            "matched": matched,
            "not_found": not_found,
            "selected_count": selected_count,
            "remove_clicked": False,
            "confirm_clicked": False,
            "duration_ms": int((time.time() - t0) * 1000),
        }

    _emit(emit, "Нажал «Remove», подтверждаю…")
    confirm_clicked = _confirm_remove_dialog_if_any(driver, timeout=min(20.0, timeout * 0.4))
    time.sleep(0.6)

    return {
        "matched": matched,
        "not_found": not_found,
        "selected_count": selected_count,
        "remove_clicked": bool(remove_clicked),
        "confirm_clicked": bool(confirm_clicked),
        "duration_ms": int((time.time() - t0) * 1000),
    }


# =============================================================================
#                         Регистрация Flask-эндпоинта
# =============================================================================

def init_bulk_remove(app: Flask, settings: Settings) -> None:
    """
    Регистрирует POST /api/gads/bulk_remove (эндпоинт: api_gads_bulk_remove)

    Запрос:
      {
        "names": [str, ...],               # обязательный список имён кампаний
        "open_url": bool?,                 # по умолчанию true
        "timeout": float?,                 # дефолт 120.0
        "profile_id": str?,                # профиль AdsPower, в котором удаляем
        "company_id": int?,                # вместо profile_id — возьмём профайл из БД компаний
        "headless": bool?|0|1              # дефолт HEADLESS_SWITCH
      }

    Ответ:
      { "ok": true, "names": [...], "bulk_remove": {...}, "bulk_remove_logs": [...], "profile_id": "..." }
      или { "ok": false, "error": "...", "bulk_remove_logs": [...] }
    """
    if "api_gads_bulk_remove" in app.view_functions:
        return  # уже зарегистрирован

    def _resolve_profile_id(data: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
        """
        Возвращает (profile_id, err). Сначала берём profile_id, иначе пытаемся достать по company_id из БД.
        """
        pid = str(data.get("profile_id") or "").strip() or None
        if pid:
            return pid, None

        comp_id = data.get("company_id")
        if comp_id in (None, ""):
            return None, None
        try:
            # БД компаний (используется UI /companies, /company, /companies/list)
            from ads_ai.web.list_companies import CompanyDB  # type: ignore
            db = CompanyDB()
            try:
                # Совместимость с реализациями .get(company_id, user_email?) и .get(company_id)
                row = db.get(int(comp_id), session.get("user_email") or "")  # type: ignore[arg-type]
            except TypeError:
                row = db.get(int(comp_id))  # type: ignore[call-arg]
            if row and row.profile_id:
                return str(row.profile_id), None
            return None, "company_not_found_or_no_profile"
        except Exception:
            return None, "company_lookup_failed"

    @app.post("/api/gads/bulk_remove")
    def api_gads_bulk_remove() -> Response:
        data = request.get_json(silent=True, force=True) or {}
        names_raw = data.get("names") or []
        open_url = _as_bool(data.get("open_url"), True)
        headless = _as_bool(data.get("headless"), HEADLESS_DEFAULT)
        try:
            timeout = float(data.get("timeout", 120.0))
        except Exception:
            timeout = 120.0

        if not isinstance(names_raw, (list, tuple)):
            return jsonify({"ok": False, "error": "invalid_payload"}), 400

        # Чистим/дедуплицируем имена
        seen = set()
        names: List[str] = []
        for n in names_raw:
            s = str(n or "").strip()
            if not s:
                continue
            k = s.lower()
            if k not in seen:
                seen.add(k)
                names.append(s)

        if not names:
            return jsonify({"ok": False, "error": "empty_names"}), 400

        logs: List[str] = []

        def _log(msg: str) -> None:
            msg = (msg or "").strip()
            if msg:
                logs.append(msg)

        # Определяем профиль (если передан)
        pid, pid_err = _resolve_profile_id(data)

        # Если profile_id не указан — пробуем общий драйвер из /console (прежнее поведение).
        if not pid:
            holder, acquire_err = _acquire_shared_driver(timeout=25.0)
            if not holder:
                # общий драйвер недоступен, а профайл не указан → ошибка
                return jsonify({"ok": False, "error": (acquire_err or pid_err or "driver_unavailable"), "bulk_remove_logs": logs}), 503

            driver = holder[0]
            try:
                result = remove_campaigns_by_names(
                    driver, names, open_url=open_url, timeout=timeout, emit=_log
                )
                return jsonify({
                    "ok": True,
                    "names": names,
                    "bulk_remove": result,
                    "bulk_remove_logs": logs,
                    "profile_id": getattr(driver, "_adspower_profile_id", "") or "",
                    "headless": getattr(_local, "headless", HEADLESS_DEFAULT),
                })
            except Exception as e:
                return jsonify({
                    "ok": False,
                    "error": f"{type(e).__name__}: {e}",
                    "bulk_remove_logs": logs,
                }), 500
            finally:
                _release_shared_driver(holder)

        # ИНАЧЕ — гарантированно работаем в нужном профиле (headless по умолчанию из тумблера/запроса)
        try:
            driver = _get_or_create_driver(pid, headless=headless)
            _ensure_big_viewport(driver)
            result = remove_campaigns_by_names(
                driver, names, open_url=open_url, timeout=timeout, emit=_log
            )
            return jsonify({
                "ok": True,
                "names": names,
                "bulk_remove": result,
                "bulk_remove_logs": logs,
                "profile_id": pid,
                "headless": headless,
            })
        except Exception as e:
            return jsonify({
                "ok": False,
                "error": f"{type(e).__name__}: {e}",
                "bulk_remove_logs": logs,
            }), 500
        finally:
            # ВАЖНО: локальный драйвер закрываем автоматически после операции
            _close_local_driver()
