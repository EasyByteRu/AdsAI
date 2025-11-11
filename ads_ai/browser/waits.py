# ads_ai/browser/waits.py
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
Надёжные ожидания рендеринга и активности DOM для SPA (Google Ads и т.п.).

Публичный контракт:
  - ensure_ready_state(driver, timeout=...): None
  - wait_url(driver, pattern, timeout_sec=..., regex=False) -> bool
  - wait_dom_stable(driver, idle_ms=..., timeout_sec=...) -> bool

Особенности:
  • Не бросают исключения наружу — деградируют мягко.
  • Опираются на execute_async_script + MutationObserver, с фоллбеком на поллинг.
  • Учитывают активность: DOM-мутации, события страницы, загрузку ресурсов.
"""

import re
import time
from urllib.parse import unquote

from selenium.common.exceptions import WebDriverException, JavascriptException
from selenium.webdriver.remote.webdriver import WebDriver

__all__ = ["ensure_ready_state", "wait_url", "wait_dom_stable"]


# ----------------------------- Вспомогательные --------------------------------

def _monotonic() -> float:
    return time.monotonic()

def _sleep(sec: float) -> None:
    if sec > 0:
        time.sleep(sec)


# ----------------------------------- API --------------------------------------

def ensure_ready_state(driver: WebDriver, timeout: float = 10.0) -> None:
    """
    Дожидается, пока document.readyState станет 'interactive' или 'complete'.

    Основной путь — async JS с подпиской на readystatechange/load и мягким поллингом.
    Фоллбек — Python-поллинг через execute_script.
    Никогда не бросает исключений наружу.
    """
    timeout = float(max(0.0, timeout))
    js = r"""
    const cb = arguments[arguments.length - 1];
    const timeoutSec = Math.max(0, Number(arguments[0]) || 0);
    const deadline = Date.now() + Math.floor(timeoutSec * 1000);

    function done(ok){ try{ cb(Boolean(ok)); }catch(_){ } }

    try {
      const ok = () => {
        try {
          const rs = (document.readyState || '').toLowerCase();
          return rs === 'interactive' || rs === 'complete';
        } catch(e){ return false; }
      };
      if (ok()) return done(true);

      const onrs = () => { if (ok()) { cleanup(); done(true); } };
      const onload = () => { cleanup(); done(true); };
      function cleanup(){
        try{ document.removeEventListener('readystatechange', onrs); }catch(_){}
        try{ window.removeEventListener('load', onload); }catch(_){}
      }
      document.addEventListener('readystatechange', onrs, { once: true });
      window.addEventListener('load', onload, { once: true });

      (function tick(){
        if (ok()) return done(true);
        if (Date.now() >= deadline) return done(false);
        setTimeout(tick, 50);
      })();
    } catch(e) {
      setTimeout(() => done(false), Math.max(0, deadline - Date.now()));
    }
    """
    try:
        driver.execute_async_script(js, timeout)  # результат нам не критичен
        return
    except (WebDriverException, JavascriptException):
        pass

    # Фоллбек: Python-поллинг
    end = _monotonic() + timeout
    while _monotonic() < end:
        try:
            rs = (driver.execute_script("return document.readyState") or "").lower()
            if rs in ("interactive", "complete"):
                return
        except WebDriverException:
            pass
        _sleep(0.05)
    # Без raise


def wait_url(driver: WebDriver, pattern: str, *, timeout_sec: int = 12, regex: bool = False) -> bool:
    """
    Ждёт, пока текущий URL будет соответствовать шаблону.

      - regex=False: проверка на подстроку `pattern` в href (а также в unquote-варианте)
      - regex=True: search по регулярному выражению (и по unquote-варианту)

    Возвращает True при успехе, иначе False.
    """
    pat = str(pattern or "")
    use_regex = bool(regex)

    rx = None
    if use_regex:
        try:
            rx = re.compile(pat)
        except re.error:
            use_regex = False  # некорректную регулярку трактуем как подстроку

    def _get_url() -> str:
        try:
            js_url = driver.execute_script("return window.location.href") or ""
        except Exception:
            js_url = ""
        try:
            cur = driver.current_url or ""
        except Exception:
            cur = ""
        return js_url or cur

    deadline = _monotonic() + max(0, int(timeout_sec))
    while _monotonic() < deadline:
        url = _get_url()
        try:
            url_u = unquote(url)
        except Exception:
            url_u = url

        if use_regex and rx is not None:
            try:
                if rx.search(url) or rx.search(url_u):
                    return True
            except re.error:
                pass
        else:
            if pat and (pat in url or pat in url_u):
                return True

        _sleep(0.15)

    return False


def wait_dom_stable(driver: WebDriver, *, idle_ms: int = 1000, timeout_sec: int = 12) -> bool:
    """
    Ждём «тишину» DOM не менее idle_ms миллисекунд.
    Реализация через MutationObserver внутри async-script + набор событий,
    которые тоже считаются «активностью».

    Активностью считаем:
      - любые DOM-мутации (attributes/childList/characterData, subtree)
      - readystatechange/load/pageshow/hashchange
      - scroll/resize (часто сопровождают lazy-рендер)
      - прирост количества ресурсов в performance.getEntriesByType('resource')
      - заметное изменение количества DOM-элементов (cheap-проверка)

    Возвращает True, если тишина наступила до дедлайна; иначе False.
    В случае ошибок JS/драйвера — мягкая деградация (False + безопасная задержка).
    """
    idle_ms = max(0, int(idle_ms))
    timeout_ms = max(idle_ms, int(timeout_sec * 1000))

    js = r"""
    const cb = arguments[arguments.length - 1];
    const idleMs = Math.max(0, parseInt(arguments[0] || 0, 10));
    const timeoutMs = Math.max(idleMs, parseInt(arguments[1] || 0, 10));

    const start = Date.now();
    let last = Date.now();

    function now(){ return Date.now(); }
    function mark(){ last = now(); }
    function done(ok){ cleanup(); try { cb(Boolean(ok)); } catch(_){} }

    // -------------------- наблюдатели активности --------------------
    let obs = null;
    let listeners = [];
    let pollTimer = null;
    let rafId = 0;
    let lastResCount = 0;
    let lastDomCount = 0;

    function on(evt, target){
      const t = target || window;
      const h = () => mark();
      try { t.addEventListener(evt, h, {passive:true}); } catch(e){ try{ t.addEventListener(evt, h); }catch(_){} }
      listeners.push({t, evt, h});
    }

    function cleanup(){
      try { if (obs) obs.disconnect(); } catch(_){}
      try { listeners.forEach(({t,evt,h}) => t.removeEventListener(evt, h)); } catch(_){}
      try { if (pollTimer) clearInterval(pollTimer); } catch(_){}
      try { if (rafId) cancelAnimationFrame(rafId); } catch(_){}
    }

    try {
      // MutationObserver
      try {
        obs = new MutationObserver(mark);
        obs.observe(document, {subtree:true, childList:true, attributes:true, characterData:true});
      } catch(e) { obs = null; }

      // базовые события страницы/окна
      on('readystatechange', document);
      on('load', window);
      on('pageshow', window);
      on('hashchange', window);
      on('scroll', window);
      on('resize', window);

      // базовые значения счётчиков
      try {
        lastResCount = (performance && performance.getEntriesByType)
          ? performance.getEntriesByType('resource').length : 0;
      } catch(e){ lastResCount = 0; }
      try {
        lastDomCount = document.getElementsByTagName('*').length;
      } catch(e){ lastDomCount = 0; }

      // периодический поллинг счётчиков
      pollTimer = setInterval(() => {
        try {
          const curRes = (performance && performance.getEntriesByType)
            ? performance.getEntriesByType('resource').length : 0;
          if (curRes > lastResCount) { lastResCount = curRes; mark(); }
        } catch(_){}

        try {
          const curDom = document.getElementsByTagName('*').length;
          if (curDom !== lastDomCount) { lastDomCount = curDom; mark(); }
        } catch(_){}
      }, 120);

      // rAF-тик — полезно при батч-рендерах
      (function rafTick(){
        rafId = requestAnimationFrame(() => {
          try { /* без чтения layout; нам важны интервалы между кадрами */ } catch(_){}
          rafId = 0;
          setTimeout(rafTick, 120);
        });
      })();

      // основной цикл ожидания тишины
      (function tick(){
        const t = now();
        if (t - last >= idleMs) return done(true);
        if (t - start >= timeoutMs) return done(false);
        setTimeout(tick, 100);
      })();

    } catch(e) {
      // если совсем плохо — ждём idle и выходим
      setTimeout(() => done(false), Math.min(timeoutMs, Math.max(0, idleMs)));
    }
    """
    try:
        res = driver.execute_async_script(js, idle_ms, timeout_ms)
        return bool(res)
    except (WebDriverException, JavascriptException):
        # Деградация: пауза без гарантий «тишины»
        try:
            _sleep(min(timeout_ms, idle_ms) / 1000.0)
        except Exception:
            pass
        return False
