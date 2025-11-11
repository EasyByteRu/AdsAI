# -*- coding: utf-8 -*-
"""
Готовый помощник для обработки окна "Confirm it's you" (Google / 2FA TOTP).

— Что нового:
  • Надёжный вызов провайдера кода (через именованные аргументы + авто‑подбор сигнатуры).
  • Подробные логи: окна, URL, заголовки, iframes, ошибки под полем, этапы ожиданий.
  • Поиск поля TOTP в iframes (до 3 уровней).
  • Устойчивый ввод: JS input/change + CDP Input.insertText (на активном элементе).
  • Улучшено переключение окон:
      – после клика Confirm пытаемся перейти в окно/вкладку accounts.google.com;
      – после подтверждения ищем и переключаемся на вкладку ads.google.com (в любом хэндле);
      – при невозможности — мягкий фолбэк: возврат в исходное окно или открытие /aw/overview.
  • Совместим со старым способом ввода кода (ENV/файл). Если ваш /api/confirm/submit записывает
    код в ads_ai_data/totp_code.txt, этот хелпер подхватит его без изменений в шагах.

Интеграция не меняется:
    from examples.steps.code_for_confrim import handle_confirm_its_you, wait_code_from_env_or_file
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import re
import time
import logging
import inspect
import urllib.parse
import contextvars
from pathlib import Path
from typing import Callable, Optional, Sequence, Tuple, List

from selenium.webdriver.remote.webdriver import WebDriver, WebElement
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

EmitFn = Optional[Callable[[str], None]]
CodeProvider = Optional[Callable[..., Optional[str]]]

_TOTP_SECRET_CTX: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "ads_ai_totp_secret",
    default=None,
)

# -----------------------------------------------------------------------------
# ЛОГИРОВАНИЕ
# -----------------------------------------------------------------------------
logger = logging.getLogger("ads_ai.gads.confirm")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

DEBUG_ENV = str(os.getenv("ADS_AI_CONFIRM_DEBUG", "1")).strip().lower() in ("1", "true", "yes", "on")
HOST_PREF_ADS = "ads.google.com"
HOST_ACCOUNTS = "accounts.google.com"
EXTRA_WAIT_AFTER_RETURN = float(os.getenv("ADS_AI_CONFIRM_WAIT_AFTER_RETURN", "3.0") or "3.0")


def _safe_emit(emit: EmitFn, text: str) -> None:
    if not emit or not text:
        return
    try:
        emit(text)
    except Exception:
        pass


def _dbg(msg: str, emit: EmitFn = None) -> None:
    """Короткий one‑liner в UI (если включён DEBUG_ENV) + отладка в логах."""
    try:
        logger.debug(msg)
    finally:
        if DEBUG_ENV:
            _safe_emit(emit, f"🔎 {msg}")


# -----------------------------------------------------------------------------
# TOTP вспомогательные функции
# -----------------------------------------------------------------------------
def normalize_totp_secret(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.lower().startswith("otpauth://"):
        return raw
    secret_part, suffix = raw, ""
    if "|" in raw:
        secret_part, suffix = raw.split("|", 1)
    clean_secret = re.sub(r"[^A-Z2-7]", "", secret_part.upper())
    if not clean_secret:
        return None
    suffix_tokens = [tok.strip() for tok in re.split(r"[;,]", suffix) if tok.strip()]
    if suffix_tokens:
        return clean_secret + "|" + ",".join(suffix_tokens)
    return clean_secret


def current_profile_totp_secret() -> Optional[str]:
    """Возвращает otp_secret, установленный для текущего потока (если есть)."""
    try:
        return _TOTP_SECRET_CTX.get()
    except LookupError:
        return None


def set_profile_totp_secret(secret: Optional[str]) -> None:
    """
    Сохраняет otp_secret (per-thread), чтобы wait_code_from_env_or_file мог генерировать код автоматически.
    """
    normalized = normalize_totp_secret(secret)
    _TOTP_SECRET_CTX.set(normalized)


def clear_profile_totp_secret() -> None:
    """Сбрасывает установленный ранее otp_secret (возвращаемся к значению по умолчанию)."""
    _TOTP_SECRET_CTX.set(None)


def _parse_totp_config(value: Optional[str]) -> Optional[Tuple[bytes, int, int]]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    digits = 6
    period = 30
    secret_str = raw

    if raw.lower().startswith("otpauth://"):
        try:
            parsed = urllib.parse.urlparse(raw)
            params = urllib.parse.parse_qs(parsed.query or "")
            secret_param = (params.get("secret") or [""])[0].strip()
            if not secret_param:
                return None
            secret_clean = re.sub(r"[^A-Z2-7]", "", secret_param.upper())
            if not secret_clean:
                return None
            secret_str = secret_clean
            if "digits" in params:
                try:
                    digits = int(params["digits"][0])
                except Exception:
                    digits = 6
            if "period" in params:
                try:
                    period = int(params["period"][0])
                except Exception:
                    period = 30
        except Exception:
            return None
    else:
        suffix = ""
        if "|" in raw:
            secret_str, suffix = raw.split("|", 1)
        secret_clean = re.sub(r"[^A-Z2-7]", "", secret_str.upper())
        if not secret_clean:
            return None
        secret_str = secret_clean
        digits_set = False
        period_set = False
        if suffix:
            for token in re.split(r"[;,]", suffix):
                token = token.strip()
                if not token:
                    continue
                low = token.lower()
                if low.startswith("digits="):
                    try:
                        digits = int(low.split("=", 1)[1])
                        digits_set = True
                    except Exception:
                        continue
                elif low.startswith("period=") or low.startswith("step=") or low.startswith("interval="):
                    try:
                        period = int(low.split("=", 1)[1])
                        period_set = True
                    except Exception:
                        continue
                elif low.startswith("t="):
                    try:
                        period = int(low.split("=", 1)[1])
                        period_set = True
                    except Exception:
                        continue
                elif low.isdigit():
                    try:
                        val = int(low)
                    except Exception:
                        continue
                    if not digits_set:
                        digits = val
                        digits_set = True
                    elif not period_set:
                        period = val
                        period_set = True
    if digits not in (6, 7, 8):
        digits = 6
    if period <= 0:
        period = 30
    pad = (-len(secret_str)) % 8
    padded = secret_str + ("=" * pad)
    try:
        secret_bytes = base64.b32decode(padded, casefold=True)
    except Exception:
        return None
    if not secret_bytes:
        return None
    return secret_bytes, digits, period


def generate_totp_code(secret: Optional[str], now: Optional[float] = None) -> Optional[str]:
    cfg = _parse_totp_config(secret)
    if not cfg:
        return None
    key, digits, period = cfg
    ts = time.time() if now is None else float(now)
    counter = int(ts // period)
    msg = counter.to_bytes(8, "big")
    digest = hmac.new(key, msg, hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    code_int = int.from_bytes(digest[offset:offset + 4], "big") & 0x7FFFFFFF
    token = code_int % (10 ** digits)
    return str(token).zfill(digits)


# -----------------------------------------------------------------------------
# БАЗОВЫЕ УТИЛИТЫ
# -----------------------------------------------------------------------------
def _is_interactable(el: WebElement) -> bool:
    try:
        if not el.is_displayed():
            return False
        if not el.is_enabled():
            return False
        aria_disabled = (el.get_attribute("aria-disabled") or "").strip().lower()
        if aria_disabled in ("1", "true"):
            return False
        r = el.rect or {}
        return (r.get("width", 0) or 0) > 6 and (r.get("height", 0) or 0) > 6
    except Exception:
        return False


def _robust_click(driver: WebDriver, el: WebElement) -> bool:
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center',inline:'nearest'});", el)
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
                    const ev=(t)=>new MouseEvent(t,{view:window,bubbles:true,cancelable:true,clientX:x,clientY:y});
                    el.dispatchEvent(ev('mousedown')); el.dispatchEvent(ev('mouseup')); el.dispatchEvent(ev('click'));
                    """,
                    el,
                )
                return True
            except Exception:
                return False


def _dispatch_input_change(driver: WebDriver, el: WebElement, value: str) -> None:
    """Вставляет value и синтетические события input/change (устойчивее, чем чистый send_keys)."""
    driver.execute_script(
        """
        const el = arguments[0], val = String(arguments[1]||'');
        try {
          el.focus();
          if (el.select) try{ el.select(); }catch(_){}
          el.value = '';
          el.dispatchEvent(new Event('input', {bubbles:true}));
          el.value = val;
          el.dispatchEvent(new Event('input', {bubbles:true}));
          el.dispatchEvent(new Event('change', {bubbles:true}));
        } catch(e) {}
        """,
        el,
        value,
    )
    # финальный «пинок», чтобы курсор оказался в конце
    try:
        el.send_keys(Keys.END)
    except Exception:
        pass


def _cdp_insert_text(driver: WebDriver, text: str) -> None:
    """CDP-ввод в активный элемент — полезно в headless/аккаунтах."""
    try:
        driver.execute_cdp_cmd("Input.insertText", {"text": text})
    except Exception:
        pass


def _warm_cdp(driver: WebDriver) -> None:
    """Лёгкий прогрев CDP для стабильности ввода."""
    try:
        driver.execute_cdp_cmd("Page.enable", {})
    except Exception:
        pass
    try:
        driver.execute_cdp_cmd("Runtime.runIfWaitingForDebugger", {})
    except Exception:
        pass


def _cur_snapshot(driver: WebDriver) -> str:
    try:
        url = (driver.current_url or "").strip()
    except Exception:
        url = ""
    try:
        title = (driver.title or "").strip()
    except Exception:
        title = ""
    return f"url={url or '—'} | title={title or '—'}"


def _log_handles(driver: WebDriver, emit: EmitFn = None, prefix: str = "handles") -> None:
    try:
        hs = driver.window_handles
    except Exception:
        hs = []
    lines = []
    for i, h in enumerate(hs):
        line = f"#{i}: {h}"
        try:
            driver.switch_to.window(h)
            url = (driver.current_url or "").strip()
            title = (driver.title or "").strip()
            line += f" | {url or '—'} | {title or '—'}"
        except Exception:
            line += " | switch failed"
        lines.append(line)
    msg = f"{prefix}: total={len(hs)} | " + " || ".join(lines) if lines else f"{prefix}: total=0"
    logger.info(msg)
    if DEBUG_ENV:
        _safe_emit(emit, "🔎 " + msg)


# -----------------------------------------------------------------------------
# ПОИСК ДИАЛОГА И КОНТРОЛЛОВ
# -----------------------------------------------------------------------------
def _locate_confirm_dialog(driver: WebDriver) -> Optional[WebElement]:
    try:
        el = driver.execute_script(
            """
            const isVis=(e)=>{ if(!e) return false; const cs = getComputedStyle(e);
              if(cs.display==='none'||cs.visibility==='hidden'||parseFloat(cs.opacity||'1')<0.2) return false;
              const r=e.getBoundingClientRect(); return r.width>200 && r.height>120; };
            const roots = [...document.querySelectorAll('[role="dialog"], material-dialog, .mdc-dialog--open, .dialog')]
                           .filter(isVis);
            const KEYS = ['confirm it\\'s you','confirm it’s you','подтвердите, что это вы','подтвердите что это вы'];
            for(const root of roots){
              const txt=((root.getAttribute('aria-label')||'')+' '+(root.innerText||root.textContent||'')).toLowerCase();
              if (KEYS.some(k => txt.includes(k))) return root;
            }
            return null;
            """
        )
        return el  # type: ignore[return-value]
    except Exception:
        return None


def _find_in_dialog(driver: WebDriver, dialog: WebElement, selectors: Sequence[str]) -> Optional[WebElement]:
    for sel in selectors:
        try:
            el = dialog.find_element(By.CSS_SELECTOR, sel)
            if _is_interactable(el):
                return el
        except Exception:
            continue
    # JS-поиск по тексту
    try:
        el = driver.execute_script(
            """
            const root=arguments[0];
            const pick=(btn)=>{ if(!btn) return null;
              const cs=getComputedStyle(btn),r=btn.getBoundingClientRect();
              if(cs.display==='none'||cs.visibility==='hidden'||parseFloat(cs.opacity||'1')<0.2) return null;
              if(r.width<18||r.height<18) return null;
              return btn; };
            const BTNS = [...root.querySelectorAll('button,[role=button],material-button, a[href]')];
            const WANT = new Set(['confirm','подтвердить','continue','продолжить','ок','окей','ok']);
            for(const b of BTNS){
              const txt = ((b.getAttribute('aria-label')||'')+' '+(b.innerText||b.textContent||'')).trim().toLowerCase();
              for(const w of WANT){ if(txt.includes(w)) return pick(b); }
            }
            return null;
            """,
            dialog,
        )
        if el and _is_interactable(el):  # type: ignore[truthy-bool]
            return el  # type: ignore[return-value]
    except Exception:
        pass
    return None


# --- TOTP: top-level ---
def _find_totp_input_top(driver: WebDriver) -> Optional[WebElement]:
    for sel in ('#totpPin', 'input[name="totpPin"]', 'input[type="tel"][name="totpPin"]'):
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            if _is_interactable(el):
                return el
        except Exception:
            pass
    try:
        el = driver.execute_script(
            """
            const cand = [...document.querySelectorAll('input[type="tel"], input[type="text"], input')]
              .find(i=>{
                const ar=(i.getAttribute('aria-label')||'').toLowerCase();
                const ph=(i.getAttribute('placeholder')||'').toLowerCase();
                const nm=(i.getAttribute('name')||'').toLowerCase();
                const ac=(i.getAttribute('autocomplete')||'').toLowerCase();
                return nm==='totpPin'.toLowerCase()
                       || ar.includes('enter code') || ph.includes('enter code') || ac.includes('one-time')
                       || ar.includes('код') || ph.includes('код');
              });
            return cand||null;
            """
        )
        if el and _is_interactable(el):  # type: ignore[truthy-bool]
            return el  # type: ignore[return-value]
    except Exception:
        pass
    return None


# --- TOTP: поиск в iframes (глубина) ---
def _find_totp_frame_path(driver: WebDriver, max_depth: int = 3) -> Optional[List[int]]:
    """
    Возвращает путь индексов iframe до поля TOTP (например [2,0]) или None.
    """
    def dfs(depth: int, path: List[int]) -> Optional[List[int]]:
        try:
            if _find_totp_input_top(driver):
                return path.copy()
        except Exception:
            pass
        if depth >= max_depth:
            return None
        try:
            frames = driver.find_elements(By.CSS_SELECTOR, "iframe")
        except Exception:
            frames = []
        frames = frames[:12]  # разумная отсечка
        for idx, fr in enumerate(frames):
            try:
                driver.switch_to.frame(fr)
            except Exception:
                continue
            res = dfs(depth + 1, path + [idx])
            if res is not None:
                return res
            try:
                driver.switch_to.parent_frame()
            except Exception:
                pass
        return None

    try:
        driver.switch_to.default_content()
    except Exception:
        pass
    return dfs(0, [])


def _switch_to_frame_path(driver: WebDriver, path: List[int]) -> bool:
    try:
        driver.switch_to.default_content()
    except Exception:
        pass
    try:
        for idx in path:
            frames = driver.find_elements(By.CSS_SELECTOR, "iframe")
            if idx >= len(frames):
                return False
            driver.switch_to.frame(frames[idx])
        return True
    except Exception:
        return False


def _find_next_button(driver: WebDriver) -> Optional[WebElement]:
    for sel in ('#totpNext', 'button#totpNext', '[id="totpNext"]'):
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            if _is_interactable(el):
                return el
        except Exception:
            pass
    try:
        el = driver.execute_script(
            """
            const KEYS = ['next','далее','продолжить','verify','подтвердить'];
            const btns = [...document.querySelectorAll('button,[role=button],.VfPpkd-LgbsSe')];
            for(const b of btns){
              const t=((b.getAttribute('aria-label')||'') + ' ' + (b.innerText||b.textContent||'')).trim().toLowerCase();
              const dis = b.hasAttribute('disabled') || (b.getAttribute('aria-disabled')||'')==='true';
              const cs=getComputedStyle(b), r=b.getBoundingClientRect();
              const ok = KEYS.some(k=>t.includes(k)) && !dis && cs.display!=='none' && cs.visibility!=='hidden' && parseFloat(cs.opacity||'1')>=0.2 && r.width>18 && r.height>18;
              if(ok) return b;
            }
            return null;
            """
        )
        if el and _is_interactable(el):  # type: ignore[truthy-bool]
            return el  # type: ignore[return-value]
    except Exception:
        pass
    return None


def _detect_error_text(driver: WebDriver) -> str:
    try:
        txt = driver.execute_script(
            """
            const nodes = [
              document.querySelector('[aria-live="assertive"]'),
              document.querySelector('[aria-live="polite"]'),
              document.querySelector('.o6cuMc'),
              document.querySelector('.zWXRge'),
            ].filter(Boolean);
            let out = '';
            for(const n of nodes){ const t=(n.innerText||n.textContent||'').trim(); if(t) out += t + ' '; }
            return out.trim();
            """
        )
        return (txt or "").strip()
    except Exception:
        return ""


# -----------------------------------------------------------------------------
# ОКНА И ОЖИДАНИЯ
# -----------------------------------------------------------------------------
def _any_window_matches(driver: WebDriver, predicate) -> Optional[str]:
    """Возвращает handle окна, удовлетворяющего predicate(handle, url, title)."""
    try:
        for h in driver.window_handles:
            try:
                driver.switch_to.window(h)
                url = (driver.current_url or "").lower()
                title = (driver.title or "").lower()
                if predicate(h, url, title):
                    return h
            except Exception:
                continue
    except Exception:
        pass
    return None


def _switch_to_host_if_present(driver: WebDriver, host_substr: str, emit: EmitFn = None) -> bool:
    """Переключается на первую вкладку, где URL содержит host_substr."""
    h = _any_window_matches(driver, lambda _h, url, _t: host_substr in url)
    if h:
        try:
            driver.switch_to.window(h)
            _dbg(f"Переключился на окно по хосту '{host_substr}': {h} | {_cur_snapshot(driver)}", emit)
            return True
        except Exception:
            return False
    return False


def _wait_new_window_or_challenge_here(
    driver: WebDriver,
    emit: EmitFn,
    old_handles: Sequence[str],
    timeout: float
) -> Tuple[Optional[str], bool]:
    """
    Ждём:
      • новое окно/вкладка -> вернём handle;
      • появление челенджа в текущем окне (включая iframe) -> (None, True);
      • наличие вкладки accounts.google.com среди уже открытых -> (handle, True);
      • таймаут -> (None, False).
    """
    end = time.time() + max(0.0, timeout)
    old = set(old_handles)
    last_log = 0.0
    while time.time() < end:
        # 1) Появилось новое окно?
        try:
            cur = driver.window_handles
            for h in cur:
                if h not in old:
                    _dbg(f"Обнаружено новое окно: {h}", emit)
                    return h, True
        except Exception:
            pass

        # 2) Вкладка accounts существует среди уже открытых?
        try:
            h = _any_window_matches(driver, lambda _h, url, _t: HOST_ACCOUNTS in url and ("challenge" in url or "signin" in url))
            if h:
                _dbg(f"Нашёл уже открытую вкладку с челенджем: {h}", emit)
                return h, True
        except Exception:
            pass

        # 3) Текущий таб: top/iframe/URL‑челендж
        try:
            if _find_totp_input_top(driver):
                _dbg("Нашёл поле TOTP в текущем документе.", emit)
                return None, True
            path = _find_totp_frame_path(driver, max_depth=3)
            if path is not None:
                _dbg(f"Нашёл поле TOTP во фрейме, путь={path}", emit)
                return None, True
            url = (driver.current_url or "").lower()
            if HOST_ACCOUNTS in url and ("challenge" in url or "signin" in url):
                _dbg(f"URL на challenge, продолжаю: {url}", emit)
                return None, True
        except Exception:
            pass

        if time.time() - last_log > 2.0:
            last_log = time.time()
            _dbg(f"Жду окно/челендж… ({_cur_snapshot(driver)})", emit)
            _log_handles(driver, emit, prefix="during-wait")

        time.sleep(0.25)

    return None, False


def _switch_to_handle(driver: WebDriver, handle: Optional[str], emit: EmitFn = None) -> None:
    if not handle:
        return
    try:
        driver.switch_to.window(handle)
        _dbg(f"Переключился на окно {handle} | {_cur_snapshot(driver)}", emit)
    except Exception as e:
        _dbg(f"Не удалось переключиться на {handle}: {e!r}", emit)


def _switch_back_prefer_ads(driver: WebDriver, prefer_host: str, fallback: Optional[str], emit: EmitFn = None) -> None:
    # 1) Явно попробуем найти любую вкладку ads.google.com
    if _switch_to_host_if_present(driver, prefer_host, emit=emit):
        return

    # 2) Вернуться в исходное окно (если есть)
    if fallback:
        try:
            driver.switch_to.window(fallback)
            _dbg(f"Вернулся в исходное окно {fallback} | {_cur_snapshot(driver)}", emit)
            return
        except Exception:
            pass

    # 3) На крайний случай — первая вкладка
    try:
        hs = driver.window_handles
        if hs:
            driver.switch_to.window(hs[0])
            _dbg(f"Вернулся в первое окно {hs[0]}", emit)
            return
    except Exception:
        pass


def _ensure_on_ads_or_navigate(driver: WebDriver, emit: EmitFn = None) -> None:
    """
    Гарантирует, что мы на вкладке Google Ads; если ни одна вкладка не содержит ads.google.com —
    пробуем открыть обзор напрямую (мягко, без исключений).
    """
    if _switch_to_host_if_present(driver, HOST_PREF_ADS, emit=emit):
        return
    try:
        driver.get("https://ads.google.com/aw/overview")
        _dbg("Навигировал в Google Ads /aw/overview как фолбэк.", emit)
    except Exception:
        pass


def _wait_for_ads_ready(driver: WebDriver, timeout: float = 25.0, emit: EmitFn = None) -> bool:
    """Ждёт полной загрузки вкладки Google Ads (readyState=complete)."""
    end = time.time() + max(1.0, float(timeout))
    last_hint = 0.0
    while time.time() < end:
        try:
            url = (driver.current_url or "").lower()
        except Exception:
            url = ""
        if HOST_PREF_ADS in url:
            try:
                state = str(driver.execute_script("return document.readyState||''") or "")
            except Exception:
                state = ""
            if state.lower() == "complete":
                return True
        if emit and (time.time() - last_hint) > 5.0:
            _safe_emit(emit, "Жду загрузки Google Ads…")
            last_hint = time.time()
        time.sleep(0.5)
    return False


# -----------------------------------------------------------------------------
# Поставщик кода (дефолт) + адаптер вызова
# -----------------------------------------------------------------------------
def _clean_code(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^0-9]", "", s)
    return s


def wait_code_from_env_or_file(emit: EmitFn, timeout_sec: float = 180.0) -> Optional[str]:
    """
    Приоритет:
    1) ENV GOOGLE_TOTP_SECRET — генерируем код локально (TOTP).
    2) ENV GOOGLE_TOTP_CODE — возвращаем как есть.
    3) Файл секрета (ENV ADS_AI_TOTP_SECRET_FILE) — читаем, генерируем код.
    4) Файл кода (ENV ADS_AI_TOTP_FILE или ./ads_ai_data/totp_code.txt) — ждём появления/содержимого.
       После чтения файл очищается.
    """
    secret_ctx = current_profile_totp_secret()
    if secret_ctx:
        code = generate_totp_code(secret_ctx)
        if code:
            _safe_emit(emit, "Код подтверждения сгенерирован из профиля (otp_secret).")
            return code

    secret_env = os.getenv("GOOGLE_TOTP_SECRET", "").strip()
    if secret_env:
        secret_norm = normalize_totp_secret(secret_env)
        code = generate_totp_code(secret_norm)
        if code:
            _safe_emit(emit, "Код подтверждения сгенерирован из GOOGLE_TOTP_SECRET.")
            return code

    env = os.getenv("GOOGLE_TOTP_CODE", "").strip()
    if env:
        code = _clean_code(env)
        if code:
            _safe_emit(emit, "Код подтверждения взят из переменной окружения GOOGLE_TOTP_CODE.")
            return code

    secret_file_env = os.getenv("ADS_AI_TOTP_SECRET_FILE")
    if secret_file_env:
        try:
            secret_path = Path(secret_file_env).expanduser().resolve()
            if secret_path.exists():
                secret_raw = secret_path.read_text(encoding="utf-8", errors="ignore").strip()
                secret_norm = normalize_totp_secret(secret_raw)
                code = generate_totp_code(secret_norm)
                if code:
                    _safe_emit(emit, f"Код подтверждения сгенерирован из файла секрета: {secret_path}.")
                    return code
        except Exception as e:
            logger.warning("Read TOTP secret file failed: %r", e)

    p = os.getenv("ADS_AI_TOTP_FILE") or str(Path(os.getcwd()).joinpath("ads_ai_data", "totp_code.txt"))
    path = Path(p).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)

    _safe_emit(emit, f"Ожидаю код 2FA. Введите 6–8 цифр в файл: {path}")
    logger.info("Waiting TOTP via file: %s", path)

    end = time.time() + max(1.0, float(timeout_sec))
    last_hint = 0.0
    while time.time() < end:
        try:
            if path.exists() and path.stat().st_size > 0:
                raw = path.read_text(encoding="utf-8", errors="ignore")
                code = _clean_code(raw)
                try:
                    path.write_text("", encoding="utf-8")
                except Exception:
                    pass
                if code:
                    logger.info("TOTP code read from file (len=%d).", len(code))
                    return code
        except Exception as e:
            logger.warning("Read TOTP file failed: %r", e)
        if time.time() - last_hint > 5.0:
            _safe_emit(emit, "Код не получен… всё ещё жду (можно обновлять содержимое файла/формы).")
            last_hint = time.time()
        time.sleep(0.5)
    logger.info("TOTP wait timeout.")
    return None


def _call_code_provider(provider: CodeProvider, emit: EmitFn, timeout_sec: float) -> Optional[str]:
    """
    Унифицированный вызов провайдера кода:
      • пытаемся вызвать по именованным параметрам (emit=…, timeout_sec=…),
      • поддерживаем альтернативные имена: timeout / seconds,
      • если не получилось — падаем на позиционные (emit, timeout), затем (timeout, emit).
    Это устраняет ошибки типа float(function).
    """
    if provider is None:
        return None
    try:
        sig = inspect.signature(provider)
    except Exception:
        sig = None

    try:
        if sig:
            params = list(sig.parameters.keys())
            kw = {}
            if "emit" in params:
                kw["emit"] = emit
            if "timeout_sec" in params:
                kw["timeout_sec"] = timeout_sec
            elif "timeout" in params:
                kw["timeout"] = timeout_sec
            elif "seconds" in params:
                kw["seconds"] = timeout_sec

            # если покрыли все обязательные параметры — пробуем kwargs
            if all(sig.parameters[p].default is not inspect._empty or p in kw for p in sig.parameters):
                return provider(**kw)  # type: ignore[misc]

        # позиционные варианты
        try:
            return provider(emit, timeout_sec)  # type: ignore[misc]
        except TypeError:
            return provider(timeout_sec, emit)  # type: ignore[misc]
    except Exception as e:
        logger.warning("Code provider raised: %r", e)
        return None


# -----------------------------------------------------------------------------
# Основной сценарий
# -----------------------------------------------------------------------------
def handle_confirm_its_you(
    driver: WebDriver,
    *,
    emit: EmitFn = None,
    wait_code_cb: CodeProvider = None,
    timeout_total: float = 180.0,
    max_attempts: int = 3,
) -> bool:
    """
    • True  — диалог найден, попытались пройти подтверждение (успешно или нет).
    • False — диалог не обнаружен (ничего не делали).
    """
    t_end = time.time() + max(1.0, float(timeout_total))

    # 0) есть ли диалог?
    try:
        dlg = _locate_confirm_dialog(driver)
    except Exception:
        dlg = None
    if not dlg:
        _dbg("Диалог 'Confirm it’s you' не найден — выхожу.", emit)
        return False

    _safe_emit(emit, "Обнаружено окно «Confirm it’s you». Начинаю подтверждение.")
    _dbg(f"Start confirm. Snap: {_cur_snapshot(driver)}", emit)
    _log_handles(driver, emit, prefix="on-start")

    # 1) Нажимаем Confirm
    try:
        btn = _find_in_dialog(driver, dlg, ('material-button.setup', '.setup.button', 'button'))
        ok = False
        if btn:
            ok = _robust_click(driver, btn)
            _dbg(f"Клик по «Confirm»: {'ok' if ok else 'fail'}", emit)
        if not ok:
            ok = _find_and_click_confirm_anywhere(driver)
            _dbg(f"Клик по «Confirm» (fallback): {'ok' if ok else 'fail'}", emit)
        if not ok:
            _safe_emit(emit, "Не удалось нажать «Confirm» — продолжаю без подтверждения.")
            logger.warning("Confirm click failed; skipping.")
            return True
    except Exception as e:
        _safe_emit(emit, "Клик по «Confirm» не удался — продолжаю без подтверждения.")
        logger.warning("Confirm click raised: %r", e)
        return True

    # 2) Ждём новое окно/вкладку или challenge в текущем окне (включая iframe)
    try:
        origin = driver.current_window_handle
    except Exception:
        origin = None
    try:
        handles_before = driver.window_handles
    except Exception:
        handles_before = []

    _safe_emit(emit, "Ожидаю страницу ввода кода…")
    _dbg("Жду открытие challenge (новое окно/iframe/redirect).", emit)
    new_handle, ok = _wait_new_window_or_challenge_here(
        driver, emit, handles_before, timeout=max(1.0, t_end - time.time())
    )
    if not ok:
        _safe_emit(emit, "Не дождался страницы ввода кода (таймаут). Продолжаю.")
        _log_handles(driver, emit, prefix="after-timeout")
        return True

    # если появился новый/подходящий хэндл — перейдём туда
    if new_handle:
        _switch_to_handle(driver, new_handle, emit)
    else:
        # возможно challenge в текущем окне — но всё равно попробуем явно перейти к accounts
        _switch_to_host_if_present(driver, HOST_ACCOUNTS, emit=emit)

    _warm_cdp(driver)

    # 3) Ввод кода (несколько попыток)
    provider = wait_code_cb or wait_code_from_env_or_file
    attempts = 0
    while time.time() < t_end and attempts < max_attempts:
        attempts += 1
        _dbg(f"Попытка ввода кода #{attempts}.", emit)

        # Найти поле TOTP (top или iframe)
        inp = None
        end_local = time.time() + min(30.0, max(1.0, t_end - time.time()))
        frame_path: Optional[List[int]] = None
        while time.time() < end_local and not inp:
            try:
                driver.switch_to.default_content()
            except Exception:
                pass

            inp = _find_totp_input_top(driver)
            if inp:
                _dbg("Поле TOTP найдено в текущем документе.", emit)
                break
            frame_path = _find_totp_frame_path(driver, max_depth=3)
            if frame_path is not None and _switch_to_frame_path(driver, frame_path):
                inp = _find_totp_input_top(driver)
                if inp:
                    _dbg(f"Поле TOTP найдено во фрейме, путь={frame_path}.", emit)
                    break
            time.sleep(0.2)

        if not inp:
            _safe_emit(emit, "Поле ввода TOTP не найдено. Отменяю подтверждение.")
            _log_handles(driver, emit, prefix="no-totp-input")
            break

        # Получаем код (через адаптер — никаких float(function))
        wait_code_budget = max(5.0, t_end - time.time())
        code = _call_code_provider(provider, emit, wait_code_budget)
        if not code:
            _safe_emit(emit, "Код не получен. Отменяю подтверждение.")
            break

        code_digits = _clean_code(code)
        if not code_digits:
            _safe_emit(emit, "Код пустой/невалидный. Повторите ввод.")
            continue

        # Вводим код
        try:
            _dispatch_input_change(driver, inp, code_digits)
            _cdp_insert_text(driver, "")  # иногда нужно «шевельнуть» CDP, даже пустой вставкой
            _dbg(f"Ввёл {len(code_digits)} цифр кода.", emit)
        except Exception:
            try:
                inp.clear()
                inp.send_keys(code_digits)
                _dbg(f"send_keys: ввёл {len(code_digits)} цифр кода.", emit)
            except Exception as e:
                _safe_emit(emit, "Не удалось ввести код в поле.")
                logger.warning("Input TOTP failed: %r", e)
                continue

        # Нажимаем Next / Verify
        btn_next = _find_next_button(driver)
        if not btn_next:
            _dbg("Кнопка «Next/Verify» не найдена. Жму Enter в поле.", emit)
            try:
                inp.send_keys(Keys.ENTER)
            except Exception:
                pass
        else:
            _robust_click(driver, btn_next)
            _dbg("Клик по «Next/Verify».", emit)

        # 4) Ожидаем результат
        ok, wrong = _wait_after_submit(
            driver,
            emit=emit,
            origin_handle=origin,
            wait_sec=min(35.0, max(1.0, t_end - time.time()))
        )
        if ok:
            _safe_emit(emit, "Код подтверждён.")
            break
        if wrong:
            _safe_emit(emit, "Похоже, код неверный. Попробуем ещё раз…")
            continue
        _dbg("После ввода нет явной реакции (ни успеха, ни ошибки).", emit)
        break

    # 5) Вернуться в окно Google Ads (по хосту), при необходимости — навигировать
    try:
        driver.switch_to.default_content()
    except Exception:
        pass
    _switch_back_prefer_ads(driver, prefer_host=HOST_PREF_ADS, fallback=origin, emit=emit)
    _ensure_on_ads_or_navigate(driver, emit=emit)
    if _wait_for_ads_ready(driver, timeout=25.0, emit=emit):
        if EXTRA_WAIT_AFTER_RETURN > 0:
            _safe_emit(emit, f"Жду {EXTRA_WAIT_AFTER_RETURN:.1f} сек. пока Google Ads стабилизируется…")
            time.sleep(EXTRA_WAIT_AFTER_RETURN)
    else:
        _safe_emit(emit, "Не дождался полной загрузки Google Ads — продолжаю.")
    _warm_cdp(driver)
    _log_handles(driver, emit, prefix="final")
    return True


def _find_and_click_confirm_anywhere(driver: WebDriver) -> bool:
    try:
        el = driver.execute_script(
            """
            const KEYS = ['confirm','подтвердить','continue','продолжить','ok','ок','окей'];
            const cand = [...document.querySelectorAll('button,[role=button],material-button,a[href]')].find(b=>{
              const t=((b.getAttribute('aria-label')||'')+' '+(b.innerText||b.textContent||'')).trim().toLowerCase();
              if (!t) return false;
              return KEYS.some(k=>t.includes(k));
            });
            return cand||null;
            """
        )
        if el and _is_interactable(el):  # type: ignore[truthy-bool]
            return _robust_click(driver, el)  # type: ignore[arg-type]
    except Exception:
        pass
    return False


def _wait_after_submit(
    driver: WebDriver,
    *,
    origin_handle: Optional[str],
    wait_sec: float,
    emit: EmitFn = None
) -> Tuple[bool, bool]:
    """
    Ожидаем итоговое состояние после ввода кода.
    Возвращаем (ok, wrong_code).
    Успех считаем, если:
      • окно челенджа закрылось;
      • поле TOTP исчезло;
      • виден явный переход/редирект на ads.google.com;
      • ПРИ ДОБАВЛЕНИИ: есть ЛЮБАЯ вкладка ads.google.com — переключаемся на неё и считаем успехом.
    """
    end = time.time() + max(1.0, float(wait_sec))
    last_log = 0.0
    try:
        seen_handles = set(driver.window_handles)
    except Exception:
        seen_handles = None

    while time.time() < end:
        # окно челенджа закрылось?
        try:
            cur = driver.window_handles
            if seen_handles and len(cur) < len(seen_handles):
                _dbg("Окно челенджа закрылось — успех.", emit)
                return True, False
        except Exception:
            pass

        # поле исчезло (и во фреймах нет)?
        try:
            if not _find_totp_input_top(driver) and _find_totp_frame_path(driver, max_depth=2) is None:
                _dbg("Поле TOTP исчезло — считаю успехом.", emit)
                return True, False
        except Exception:
            pass

        # явный текст ошибки
        try:
            err = _detect_error_text(driver).lower()
            if err and any(k in err for k in ("wrong", "incorrect", "неверн", "ошиб", "invalid")):
                _dbg(f"Обнаружен текст ошибки: {err!r}", emit)
                return False, True
        except Exception:
            pass

        # редирект из accounts в ads — в любом окне
        try:
            # 1) Текущее окно стало ads?
            url = (driver.current_url or "").lower()
            if (HOST_PREF_ADS in url) or (HOST_ACCOUNTS not in url and "challenge" not in url):
                _dbg(f"URL теперь {url} — считаю успехом.", emit)
                return True, False

            # 2) Есть вкладка с ads среди всех?
            h = _any_window_matches(driver, lambda _h, u, _t: HOST_PREF_ADS in u)
            if h:
                try:
                    driver.switch_to.window(h)
                except Exception:
                    pass
                _dbg(f"Нашёл вкладку с Google Ads и переключился: {h}", emit)
                return True, False
        except Exception:
            pass

        if time.time() - last_log > 1.8:
            last_log = time.time()
            _dbg(f"Жду результат валидации… ({_cur_snapshot(driver)})", emit)

        time.sleep(0.3)

    _dbg("Таймаут ожидания результата после ввода кода.", emit)
    return False, False
