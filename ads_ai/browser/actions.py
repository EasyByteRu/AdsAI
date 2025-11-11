# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
import json
import time
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Callable, List, Tuple

from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import (
    WebDriverException,
    MoveTargetOutOfBoundsException,
    StaleElementReferenceException,
    JavascriptException,
)
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement

from ads_ai.browser.selectors import find, exists, find_all
from ads_ai.browser.waits import ensure_ready_state, wait_dom_stable, wait_url
from ads_ai.plan.schema import StepType


# ---------------------------
# Контекст исполнения экшенов
# ---------------------------

@dataclass
class ActionContext:
    driver: WebDriver
    default_wait_sec: int = 12
    step_timeout_sec: int = 35
    var_store: Optional[Any] = None   # ожид.: .set(key, value), опц.: .get(), .render()
    humanizer: Optional[Any] = None   # ожид.: .type_text(el, txt), .hover(el), .smooth_scroll_by(dy)
    post_wait_dom_idle_ms: int = 200  # дефолтный "успокоительный" idle после действия
    # ниже — опциональные объекты; не обязательны, учитываются по duck-typing
    trace: Optional[Any] = None       # ожид.: .write(dict)
    artifacts: Optional[Any] = None   # ожид.: .save_png(driver, name), .save_html(driver, name)


# ---------------------------
# Внутренние утилиты
# ---------------------------

def _now_ts() -> float:
    return time.time()


def _sleep(s: float) -> None:
    if s > 0:
        time.sleep(s)


def _trace(ctx: ActionContext, payload: Dict[str, Any]) -> None:
    """Мягкий трейс: не ломаем выполнение при ошибке трейсера."""
    try:
        if ctx.trace and hasattr(ctx.trace, "write"):
            ctx.trace.write(payload)
    except Exception:
        pass


def _save_artifacts(ctx: ActionContext, name: str) -> None:
    """Снимки при ошибках: png + html. Мягкий fallback в ./artifacts."""
    try:
        if ctx.artifacts and hasattr(ctx.artifacts, "save_png"):
            ctx.artifacts.save_png(ctx.driver, name)
        else:
            Path("artifacts").mkdir(parents=True, exist_ok=True)
            ctx.driver.save_screenshot(str(Path("artifacts") / f"{name}.png"))
    except Exception:
        pass
    try:
        if ctx.artifacts and hasattr(ctx.artifacts, "save_html"):
            ctx.artifacts.save_html(ctx.driver, name)
        else:
            html = ctx.driver.page_source
            Path("artifacts").mkdir(parents=True, exist_ok=True)
            Path("artifacts", f"{name}.html").write_text(html, encoding="utf-8")
    except Exception:
        pass


def _scroll_into_view(d: WebDriver, el: WebElement) -> None:
    """Центрируем элемент во вьюпорте, игнорируя исключения."""
    try:
        d.execute_script(
            "try{arguments[0].scrollIntoView({block:'center', inline:'center', behavior:'instant'});}catch(_){"
            "  try{arguments[0].scrollIntoView({block:'center', inline:'center'});}catch(__){}"
            "}", el
        )
    except WebDriverException:
        pass


def _hover_native(d: WebDriver, el: WebElement) -> None:
    """Безопасный hover с подстраховкой скроллом."""
    try:
        ActionChains(d).move_to_element(el).pause(0.15).perform()
    except MoveTargetOutOfBoundsException:
        _scroll_into_view(d, el)
        try:
            ActionChains(d).move_to_element(el).pause(0.15).perform()
        except Exception:
            pass
    except Exception:
        pass


def _type_text(ctx: ActionContext, el: WebElement, text: str, *, clear: bool = True) -> None:
    """
    Человекоподобный ввод, если есть Humanizer; иначе — безопасный clear()+send_keys.
    Никогда не бросает исключения наружу (чтобы не ронять рантайм).
    """
    if ctx.humanizer:
        try:
            ctx.humanizer.type_text(el, text)
            return
        except Exception:
            # перейдём на дефолтную стратегию
            pass
    # дефолтное поведение
    try:
        if clear:
            el.clear()
    except Exception:
        # бывают input'ы без clear()
        try:
            el.send_keys(Keys.CONTROL, "a")
            el.send_keys(Keys.DELETE)
        except Exception:
            pass
    try:
        # немного "очеловечим" ввод даже без humanizer
        cps = random.uniform(7.0, 11.0)
        for ch in str(text):
            el.send_keys(ch)
            _sleep(random.uniform(0.7, 1.3) * (1.0 / cps))
    except Exception:
        pass


def _click(d: WebDriver, el: WebElement, offset: Optional[Tuple[int, int]] = None) -> None:
    """
    Устойчивый клик: прямой, затем ActionChains, затем JS.
    Поддержка offset-клика (dx, dy) относительно центра элемента.
    """
    _scroll_into_view(d, el)
    # 1) прямой click()
    try:
        if offset is None:
            el.click()
        else:
            ActionChains(d).move_to_element_with_offset(el, offset[0], offset[1]).pause(0.05).click().perform()
        return
    except WebDriverException:
        pass
    # 2) move_to + click
    try:
        if offset is None:
            ActionChains(d).move_to_element(el).pause(0.05).click().perform()
        else:
            ActionChains(d).move_to_element_with_offset(el, offset[0], offset[1]).pause(0.05).click().perform()
        return
    except WebDriverException:
        pass
    # 3) JS-click как последний шанс
    try:
        d.execute_script(
            "try{arguments[0].click();}catch(_){"
            "var e=document.createEvent('MouseEvents');"
            "e.initMouseEvent('click',true,true,window,1,0,0,0,0,false,false,false,false,0,null);"
            "arguments[0].dispatchEvent(e);}", el
        )
    except WebDriverException:
        # пусть упадёт на уровне шага, если критично
        pass


def _post_action_wait(ctx: ActionContext, step: Dict[str, Any]) -> None:
    """
    Управляемое ожидание после действий, которые могут менять DOM/страницу.
    Параметры шага (опц.):
      - ensure_ready: bool
      - wait_dom_idle_ms: int (вместо дефолтных ctx.post_wait_dom_idle_ms)
    """
    if bool(step.get("ensure_ready", False)):
        try:
            ensure_ready_state(ctx.driver, timeout=min(12.0, float(ctx.default_wait_sec)))
        except WebDriverException:
            pass
    idle_ms = int(step.get("wait_dom_idle_ms", ctx.post_wait_dom_idle_ms))
    if idle_ms > 0:
        try:
            wait_dom_stable(ctx.driver, idle_ms=idle_ms, timeout_sec=max(1, idle_ms // 100 + 1))
        except WebDriverException:
            # Если наблюдатель не взлетел — мягкая пауза
            _sleep(min(0.5, idle_ms / 1000.0))


def _timeout(ctx: ActionContext, step: Dict[str, Any], *, key: str = "timeout", fallback: Optional[int] = None) -> int:
    t = step.get(key)
    if t is None:
        return int(ctx.default_wait_sec if fallback is None else fallback)
    try:
        return int(t)
    except Exception:
        return int(ctx.default_wait_sec if fallback is None else fallback)


def _render_value(ctx: ActionContext, value: Any) -> Any:
    """Рендер переменных через var_store.render, если доступно."""
    if isinstance(value, str) and ctx.var_store and hasattr(ctx.var_store, "render"):
        try:
            return ctx.var_store.render(value)
        except Exception:
            return value
    if isinstance(value, list):
        return [_render_value(ctx, v) for v in value]
    if isinstance(value, dict):
        return {k: _render_value(ctx, v) for k, v in value.items()}
    return value


def _redact_step_for_trace(step: Dict[str, Any]) -> Dict[str, Any]:
    """Скрываем потенциально секретные поля при трейсинге."""
    redacted = dict(step)
    for k in ("text", "value", "password", "secret", "token", "authorization", "api_key"):
        if k in redacted and redacted.get("redact", True):
            if isinstance(redacted[k], str) and len(redacted[k]) > 0:
                redacted[k] = "***"
    return redacted


def _action(name: str) -> Callable[[Callable[[ActionContext, Dict[str, Any]], bool]], Callable[[ActionContext, Dict[str, Any]], bool]]:
    """
    Декоратор:
      - рендер переменных в шаге
      - трейс start/end
      - ретраи (step.retries, step.retry_pause_ms)
      - артефакты при исключениях
    """
    def deco(fn: Callable[[ActionContext, Dict[str, Any]], bool]) -> Callable[[ActionContext, Dict[str, Any]], bool]:
        def wrapped(ctx: ActionContext, step: Dict[str, Any]) -> bool:
            started = _now_ts()
            step_r = _render_value(ctx, step)  # безопасный рендер var'ов
            _trace(ctx, {"event": "action_start", "action": name, "step": _redact_step_for_trace(step_r), "ts": started})

            retries = int(step_r.get("retries", 0))
            pause_ms = int(step_r.get("retry_pause_ms", 250))
            attempt = 0
            last_exc: Optional[Exception] = None

            while attempt <= retries:
                try:
                    res = bool(fn(ctx, step_r))
                    _trace(ctx, {
                        "event": "action_end",
                        "action": name,
                        "ok": res,
                        "attempt": attempt,
                        "elapsed_ms": int(( _now_ts() - started ) * 1000)
                    })
                    return res
                except Exception as e:
                    last_exc = e
                    _trace(ctx, {
                        "event": "action_error",
                        "action": name,
                        "attempt": attempt,
                        "error": f"{type(e).__name__}: {e}",
                    })
                    if attempt >= retries:
                        # Сохраним артефакты и пробросим
                        _save_artifacts(ctx, f"error_{name}")
                        raise
                    _sleep(max(0.0, pause_ms / 1000.0))
                    attempt += 1
            # теоретически не дойдём
            if last_exc:
                raise last_exc
            return False
        return wrapped
    return deco


# ---------------------------
# Экшены (по типам шагов)
# ---------------------------

@_action("wait")
def do_wait(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    _sleep(float(step.get("seconds", 0.5)))
    return True


@_action("wait_visible")
def do_wait_visible(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    sel = step.get("selector", "")
    timeout = _timeout(ctx, step)
    return exists(ctx.driver, sel, visible=True, timeout_sec=timeout)


@_action("wait_url")
def do_wait_url(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    pattern = str(step.get("pattern", ""))
    regex = bool(step.get("regex", False))
    timeout = _timeout(ctx, step)
    return wait_url(ctx.driver, pattern, timeout_sec=timeout, regex=regex)


@_action("wait_dom_stable")
def do_wait_dom_stable(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    ms = int(step.get("ms", 1000))
    timeout = _timeout(ctx, step)
    return wait_dom_stable(ctx.driver, idle_ms=ms, timeout_sec=timeout)


@_action("goto")
def do_goto(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    url = step.get("url") or step.get("href")
    if not url:
        raise RuntimeError("goto: url is empty")
    ctx.driver.get(url)
    ensure_ready_state(ctx.driver, timeout=min(15.0, float(ctx.default_wait_sec)))
    _post_action_wait(ctx, step)
    return True


@_action("go_back")
def do_nav_back(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    ctx.driver.back()
    _post_action_wait(ctx, step)
    return True


@_action("go_forward")
def do_nav_forward(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    ctx.driver.forward()
    _post_action_wait(ctx, step)
    return True


@_action("refresh")
def do_refresh(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    ctx.driver.refresh()
    ensure_ready_state(ctx.driver, timeout=min(15.0, float(ctx.default_wait_sec)))
    _post_action_wait(ctx, step)
    return True


@_action("check")
def do_check(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    """
    Параметры:
      selector: str
      present: bool = True
      timeout: int (сек)
      raise: bool = False — если True, кидаем исключение при несоответствии
    """
    sel = step.get("selector", "")
    present = bool(step.get("present", True))
    timeout = _timeout(ctx, step)
    ok = exists(ctx.driver, sel, visible=False, timeout_sec=timeout)
    if bool(step.get("raise", False)) and ok is not present:
        raise AssertionError(f"check failed: present={present}, actual={ok}, selector={sel!r}")
    return ok is present


@_action("scroll")
def do_scroll(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    direction = str(step.get("direction", "down")).lower()
    amount = int(step.get("amount", 600))
    dy = amount if direction == "down" else -amount
    if ctx.humanizer:
        try:
            ctx.humanizer.smooth_scroll_by(dy)
        except Exception:
            try:
                ctx.driver.execute_script("window.scrollBy(0, arguments[0]);", dy)
            except Exception:
                pass
    else:
        try:
            ctx.driver.execute_script("window.scrollBy(0, arguments[0]);", dy)
        except Exception:
            pass
    _sleep(0.15)
    return True


@_action("scroll_to")
def do_scroll_to(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    where = str(step.get("to", "bottom")).lower()
    try:
        if where == "bottom":
            ctx.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        elif where == "top":
            ctx.driver.execute_script("window.scrollTo(0, 0);")
        else:
            raise RuntimeError(f"scroll_to: unknown target '{where}'")
    except Exception:
        return False
    _sleep(0.15)
    return True


@_action("scroll_to_element")
def do_scroll_to_element(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    el = find(ctx.driver, step.get("selector", ""), visible=False, timeout_sec=_timeout(ctx, step))
    if not el:
        return False
    _scroll_into_view(ctx.driver, el)
    _sleep(0.15)
    return True


@_action("click")
def do_click(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    sel = step.get("selector", "")
    timeout = _timeout(ctx, step)
    el = find(ctx.driver, sel, visible=True, timeout_sec=timeout)
    if not el:
        return False

    offset = None
    if "offset_x" in step or "offset_y" in step:
        offset = (int(step.get("offset_x", 0)), int(step.get("offset_y", 0)))

    try:
        # при наличии humanizer — имитируем небольшое наведение
        if ctx.humanizer:
            try:
                ctx.humanizer.hover(el)
            except Exception:
                _hover_native(ctx.driver, el)
        _click(ctx.driver, el, offset)
    except StaleElementReferenceException:
        # попробуем найти элемент ещё раз и повторить клик один раз
        el2 = find(ctx.driver, sel, visible=True, timeout_sec=2)
        if not el2:
            return False
        _click(ctx.driver, el2, offset)

    _post_action_wait(ctx, step)

    # Пороговые ensure после клика (опционально)
    if "ensure_url_contains" in step:
        return wait_url(ctx.driver, str(step["ensure_url_contains"]), timeout_sec=_timeout(ctx, step, fallback=ctx.default_wait_sec))
    if "ensure_visible" in step:
        return exists(ctx.driver, str(step["ensure_visible"]), visible=True, timeout_sec=_timeout(ctx, step))
    if "ensure_invisible" in step:
        return not exists(ctx.driver, str(step["ensure_invisible"]), visible=True, timeout_sec=_timeout(ctx, step))
    return True


@_action("double_click")
def do_double_click(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    el = find(ctx.driver, step.get("selector", ""), visible=True, timeout_sec=_timeout(ctx, step))
    if not el:
        return False
    _scroll_into_view(ctx.driver, el)
    try:
        ActionChains(ctx.driver).double_click(el).perform()
    except Exception:
        return False
    _post_action_wait(ctx, step)
    return True


@_action("context_click")
def do_context_click(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    el = find(ctx.driver, step.get("selector", ""), visible=True, timeout_sec=_timeout(ctx, step))
    if not el:
        return False
    _scroll_into_view(ctx.driver, el)
    try:
        ActionChains(ctx.driver).context_click(el).perform()
    except Exception:
        return False
    _post_action_wait(ctx, step)
    return True


@_action("input")
def do_input(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    """
    Параметры:
      selector: str
      text: str
      clear: bool = True
      submit: bool = False (нажать Enter после ввода)
    """
    el = find(ctx.driver, step.get("selector", ""), visible=False, timeout_sec=_timeout(ctx, step))
    if not el:
        return False
    txt = str(step.get("text", ""))
    clear = bool(step.get("clear", True))
    _type_text(ctx, el, txt, clear=clear)
    if bool(step.get("submit", False)):
        try:
            el.send_keys(Keys.ENTER)
        except Exception:
            pass
    _post_action_wait(ctx, step)
    return True


@_action("press_key")
def do_press_key(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    key_name = str(step.get("key", "")).upper()
    times = int(step.get("times", 1))
    key = getattr(Keys, key_name, None)
    if key is None:
        raise RuntimeError(f"press_key: unknown key: {key_name!r}")
    for _ in range(max(1, times)):
        try:
            ActionChains(ctx.driver).send_keys(key).perform()
        except Exception:
            return False
        _sleep(0.02)
    _post_action_wait(ctx, step)
    return True


@_action("hotkey")
def do_hotkey(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    """
    Примеры:
      keys: ["CONTROL", "A"]
      keys: "CONTROL+SHIFT+T"
      keys: ["ALT", "TAB", "TAB"]  (последние отправятся как send_keys)
    """
    raw = step.get("keys")
    keys: List[str]
    if isinstance(raw, list):
        keys = [str(k) for k in raw]
    elif raw:
        keys = [s.strip() for s in str(raw).split("+") if s.strip()]
    else:
        raise RuntimeError("hotkey: keys empty")

    chain = ActionChains(ctx.driver)
    down: List[str] = []
    for k in keys[:-1]:
        key = getattr(Keys, str(k).upper(), None)
        if key is None:
            raise RuntimeError(f"hotkey: unknown key: {k}")
        chain.key_down(key)
        down.append(key)
    last = keys[-1]
    last_key = getattr(Keys, str(last).upper(), None)
    chain.send_keys(last_key if last_key is not None else str(last))
    for k in reversed(down):
        chain.key_up(k)
    try:
        chain.perform()
    except Exception:
        return False
    _post_action_wait(ctx, step)
    return True


@_action("hover")
def do_hover(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    el = find(ctx.driver, step.get("selector", ""), visible=True, timeout_sec=_timeout(ctx, step))
    if not el:
        return False
    if ctx.humanizer:
        try:
            ctx.humanizer.hover(el)
        except Exception:
            _hover_native(ctx.driver, el)
    else:
        _hover_native(ctx.driver, el)
    return True


@_action("select")
def do_select(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    """
    Поддерживает HTML <select>:
      by: value|text|index  (по умолчанию text)
      value: строка (или индекс для by=index)
    Если элемент — не <select>, пытаемся кликнуть «псевдо-опцию» role=option.
    """
    el = find(ctx.driver, step.get("selector", ""), visible=False, timeout_sec=_timeout(ctx, step))
    if not el:
        return False

    by = str(step.get("by", "text")).lower()
    val = step.get("value", "")

    try:
        tag = (el.tag_name or "").lower()
        typ = (el.get_attribute("type") or "").lower()
    except Exception:
        tag, typ = "", ""

    if tag == "select":
        try:
            s = Select(el)
            if by == "value":
                s.select_by_value(str(val))
            elif by == "text":
                s.select_by_visible_text(str(val))
            elif by == "index":
                s.select_by_index(int(val))
            else:
                raise RuntimeError(f"select: bad 'by' = {by}")
            _post_action_wait(ctx, step)
            return True
        except Exception:
            return False

    # Фоллбек для кастомных селектов: кликнуть элемент и выбрать опцию role=option по тексту
    try:
        _click(ctx.driver, el, None)
        _sleep(0.1)
        if by in ("text", "value"):
            # найдём ближайшую подходящую опцию
            opt = find(ctx.driver, f'role=option["{val}"]', visible=True, timeout_sec=2)
            if not opt:
                # ещё вариант: текст=...
                opt = find(ctx.driver, f'text="{val}"', visible=True, timeout_sec=2)
            if opt:
                _click(ctx.driver, opt, None)
                _post_action_wait(ctx, step)
                return True
    except Exception:
        pass
    return False


@_action("file_upload")
def do_file_upload(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    """
    Загрузка файла в <input type=file>. Если селектор указывает на обёртку — ищем вложенный input[type=file].
    Перед send_keys делаем input видимым (display/visibility) и снимаем disabled.
    """
    sel = step.get("selector", "")
    el = find(ctx.driver, sel, visible=False, timeout_sec=_timeout(ctx, step))
    if not el:
        return False

    path = step.get("path") or ""
    if not os.path.isabs(path):
        path = os.path.abspath(path)
    if not os.path.exists(path):
        raise RuntimeError(f"file_upload: path not found: {path}")

    try:
        tag = (el.tag_name or "").lower()
        typ = (el.get_attribute("type") or "").lower()
    except Exception:
        tag, typ = "", ""
    if not (tag == "input" and typ == "file"):
        try:
            inner = el.find_element("css selector", "input[type='file']")
            if inner:
                el = inner
        except Exception:
            inner2 = find(ctx.driver, "css=input[type='file']", visible=False, timeout_sec=2)
            if inner2:
                el = inner2

    # Сделаем input видимым/активным и загрузим файл
    try:
        ctx.driver.execute_script(
            "try{arguments[0].style.display='block';arguments[0].style.visibility='visible';"
            "arguments[0].removeAttribute('disabled');}catch(_){}", el
        )
    except Exception:
        pass

    try:
        el.send_keys(path)
    except Exception:
        return False

    _post_action_wait(ctx, step)
    return True


def _drag_and_drop_html5_js(d: WebDriver, src: WebElement, dst: WebElement) -> None:
    """HTML5 drag&drop через скрипт с DataTransfer."""
    js = """
    const src = arguments[0], dst = arguments[1];
    const dt = new (window.DataTransfer || window.ClipboardEvent && ClipboardEvent.prototype && ClipboardEvent.prototype.clipboardData && window.DataTransfer || window.DataTransfer)();
    function ev(t, el){ const e = new DragEvent(t, {bubbles:true,cancelable:true,dataTransfer:dt}); el.dispatchEvent(e); }
    ev('dragstart', src); ev('dragenter', dst); ev('dragover', dst); ev('drop', dst); ev('dragend', src);
    """
    d.execute_script(js, src, dst)


@_action("drag_and_drop")
def do_drag_and_drop(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    """
    Перетаскивание:
      - source: селектор источника (обяз.)
      - target: селектор цели ИЛИ to_offset_x/y (смещение)
    """
    src = find(ctx.driver, step.get("source", ""), visible=True, timeout_sec=_timeout(ctx, step))
    if not src:
        return False
    _scroll_into_view(ctx.driver, src)

    if "target" in step and step.get("target"):
        dst = find(ctx.driver, step.get("target", ""), visible=True, timeout_sec=_timeout(ctx, step))
        if not dst:
            return False
        _scroll_into_view(ctx.driver, dst)
        try:
            ActionChains(ctx.driver).click_and_hold(src).move_to_element(dst).pause(0.05).release().perform()
        except WebDriverException:
            # HTML5 fallback
            try:
                _drag_and_drop_html5_js(ctx.driver, src, dst)
            except WebDriverException:
                return False
    else:
        dx = int(step.get("to_offset_x", 0))
        dy = int(step.get("to_offset_y", 0))
        try:
            ActionChains(ctx.driver).click_and_hold(src).move_by_offset(dx, dy).pause(0.05).release().perform()
        except WebDriverException:
            return False

    _post_action_wait(ctx, step)
    return True


@_action("switch_to_frame")
def do_switch_to_frame(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    sel = step.get("selector")
    idx = step.get("index")
    try:
        if sel:
            el = find(ctx.driver, sel, visible=False, timeout_sec=_timeout(ctx, step))
            if not el:
                return False
            ctx.driver.switch_to.frame(el)
            return True
        if idx is not None:
            ctx.driver.switch_to.frame(int(idx))
            return True
        return False
    except Exception:
        return False


@_action("switch_to_default")
def do_switch_to_default(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    try:
        ctx.driver.switch_to.default_content()
        return True
    except Exception:
        return False


@_action("new_tab")
def do_new_tab(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    url = step.get("url") or "about:blank"
    foreground = bool(step.get("foreground", True))
    try:
        ctx.driver.execute_script("window.open(arguments[0], '_blank');", url)
    except Exception:
        return False
    if foreground:
        try:
            ctx.driver.switch_to.window(ctx.driver.window_handles[-1])
            _post_action_wait(ctx, step)
        except Exception:
            return False
    return True


def _switch_to_tab_by(ctx: ActionContext, by: str, value: str) -> bool:
    handles = list(ctx.driver.window_handles or [])
    if not handles:
        return False
    cur = ctx.driver.current_window_handle
    for i, h in enumerate(handles):
        try:
            ctx.driver.switch_to.window(h)
            url = ctx.driver.current_url or ""
            title = ctx.driver.title or ""
        except Exception:
            continue
        if by == "index":
            try:
                idx = int(value)
                if idx < 0:
                    idx = len(handles) + idx
                if i == idx:
                    return True
            except Exception:
                pass
        elif by == "url_contains" and value in url:
            return True
        elif by == "title_contains" and value in title:
            return True
    # вернуть фокус обратно
    try:
        ctx.driver.switch_to.window(cur)
    except Exception:
        pass
    return False


@_action("switch_to_tab")
def do_switch_to_tab(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    by = str(step.get("by", "index")).lower()
    value = str(step.get("value", "0"))
    return _switch_to_tab_by(ctx, by, value)


@_action("close_tab")
def do_close_tab(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    idx = step.get("index")
    try:
        if idx is not None and str(idx).lstrip("-").isdigit():
            handles = ctx.driver.window_handles
            pos = int(idx)
            if pos < 0:
                pos = len(handles) + pos
            if 0 <= pos < len(handles):
                ctx.driver.switch_to.window(handles[pos])
        ctx.driver.close()
        # переключаемся на последнюю оставшуюся
        if ctx.driver.window_handles:
            ctx.driver.switch_to.window(ctx.driver.window_handles[-1])
        return True
    except Exception:
        return False


@_action("extract")
def do_extract(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    """
    Параметры:
      selector: str
      attr: text|html|outer_html|<attr_name>  (по умолчанию text)
      var: имя переменной для var_store
      all: bool — если True, соберём список со всех совпадений
      regex: str (опц.) — если задан, применим к полученному тексту/значению; при all=True вернём список матчей
    """
    selector = step.get("selector", "")
    attr = str(step.get("attr", "text")).lower()
    var = step.get("var")
    many = bool(step.get("all", False))
    rx = step.get("regex")

    def _get_value(el: WebElement) -> Any:
        if attr == "text":
            return el.text
        if attr == "html":
            return el.get_attribute("innerHTML")
        if attr in ("outer_html", "outerhtml"):
            return el.get_attribute("outerHTML")
        return el.get_attribute(attr)

    if many:
        els = find_all(ctx.driver, selector, visible=False, timeout_sec=_timeout(ctx, step))
        values = [_get_value(e) for e in els]
        if rx:
            try:
                pattern = re.compile(str(rx))
                values = [m.group(0) if (m := pattern.search(v or "")) else "" for v in values]
            except re.error:
                values = []
        if ctx.var_store and var:
            try:
                ctx.var_store.set(var, values)
            except Exception:
                pass
        return len(values) > 0

    el = find(ctx.driver, selector, visible=False, timeout_sec=_timeout(ctx, step))
    if not el:
        return False
    val = _get_value(el)
    if rx:
        try:
            m = re.search(str(rx), val or "")
            val = m.group(0) if m else ""
        except re.error:
            val = ""
    if ctx.var_store and var:
        try:
            ctx.var_store.set(var, val)
        except Exception:
            pass
    return True


@_action("assert_text")
def do_assert_text(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    """
    Параметры:
      selector: str
      attr: text|html|<attr_name> (по умолчанию text)
      value: str
      match: contains|equals|regex|startswith|endswith|icontains|iequals  (по умолчанию contains)
    """
    el = find(ctx.driver, step.get("selector", ""), visible=False, timeout_sec=_timeout(ctx, step))
    if not el:
        return False
    attr = str(step.get("attr", "text")).lower()
    want = str(step.get("value", ""))
    how = str(step.get("match", "contains")).lower()

    got = el.text if attr == "text" else (el.get_attribute("innerHTML") if attr == "html" else el.get_attribute(attr))
    got = "" if got is None else str(got)

    if how == "equals":
        ok = (want == got)
    elif how == "iequals":
        ok = (want.lower() == got.lower())
    elif how == "contains":
        ok = (want in got)
    elif how == "icontains":
        ok = (want.lower() in got.lower())
    elif how == "startswith":
        ok = got.startswith(want)
    elif how == "endswith":
        ok = got.endswith(want)
    elif how == "regex":
        try:
            ok = bool(re.search(want, got))
        except re.error:
            ok = False
    else:
        raise RuntimeError(f"assert_text: unknown match mode {how!r}")

    if not ok:
        snippet = got[:300].replace("\n", "\\n")
        raise AssertionError(f"assert_text failed: want({how})={want!r}, got={snippet!r}")
    return True


@_action("evaluate")
def do_evaluate(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    """
    Параметры:
      script: str (JS-код; по умолчанию 'return null;')
      args: list (опц.) — пойдут в arguments по порядку
      var: str (опц.) — сохранить результат в var_store
    """
    script = step.get("script") or "return null;"
    args = step.get("args") or []
    if not isinstance(args, list):
        args = [args]
    try:
        res = ctx.driver.execute_script(script, *args)
    except JavascriptException as e:
        raise RuntimeError(f"evaluate: javascript error: {e.msg}") from e
    if ctx.var_store and step.get("var"):
        try:
            ctx.var_store.set(step["var"], res)
        except Exception:
            pass
    return True


@_action("pause_for_human")
def do_pause_for_human(ctx: ActionContext, step: Dict[str, Any]) -> bool:
    reason = step.get("reason") or "Paused. Press Enter to continue."
    try:
        input(f"[PAUSE] {reason}\n>> Enter to continue...")
    except Exception:
        # если stdin недоступен (демон), просто подождём немного
        _sleep(3.0)
    return True


# ---------------------------
# Регистр экшенов
# ---------------------------

ActionHandler = Callable[[ActionContext, Dict[str, Any]], bool]

ACTIONS: Dict[StepType, ActionHandler] = {
    StepType.WAIT: do_wait,
    StepType.WAIT_VISIBLE: do_wait_visible,
    StepType.WAIT_URL: do_wait_url,
    StepType.WAIT_DOM_STABLE: do_wait_dom_stable,

    StepType.GOTO: do_goto,
    StepType.GO_BACK: do_nav_back,
    StepType.GO_FORWARD: do_nav_forward,
    StepType.REFRESH: do_refresh,

    StepType.CHECK: do_check,

    StepType.SCROLL: do_scroll,
    StepType.SCROLL_TO: do_scroll_to,
    StepType.SCROLL_TO_ELEMENT: do_scroll_to_element,

    StepType.CLICK: do_click,
    StepType.DOUBLE_CLICK: do_double_click,
    StepType.CONTEXT_CLICK: do_context_click,

    StepType.INPUT: do_input,
    StepType.PRESS_KEY: do_press_key,
    StepType.HOTKEY: do_hotkey,

    StepType.HOVER: do_hover,
    StepType.SELECT: do_select,
    StepType.FILE_UPLOAD: do_file_upload,
    StepType.DRAG_AND_DROP: do_drag_and_drop,

    StepType.SWITCH_TO_FRAME: do_switch_to_frame,
    StepType.SWITCH_TO_DEFAULT: do_switch_to_default,

    StepType.NEW_TAB: do_new_tab,
    StepType.SWITCH_TO_TAB: do_switch_to_tab,
    StepType.CLOSE_TAB: do_close_tab,

    StepType.EXTRACT: do_extract,
    StepType.ASSERT_TEXT: do_assert_text,
    StepType.EVALUATE: do_evaluate,

    StepType.PAUSE_FOR_HUMAN: do_pause_for_human,
}
