from __future__ import annotations

"""
Executor for Vision actions: clicks and text input by pixel coordinates.

Инварианты:
- Сигнатуры и возвращаемые типы не изменены.
- Без жёстких зависимостей на трейс/артефакты/метрики ядра: всё опционально через ENV.
- Если CDP/фичи недоступны — graceful degradation с понятными VerifyIssue.

Добавлено (продакшен-акценты):
- Ретраи действий (перепрокрутка/повтор клика/фокуса) с бэкоффом.
- «Умный» выбор кликаемой точки внутри bbox (hit-test через elementFromPoint).
- Доп. тип шага: kind == "double_click".
- Очистка поля перед вводом (env/мета), гибридный ввод + JS-fallback c событиями.
- Пост-верификация ввода и value_mismatch в issues.
- Опциональные артефакты на ошибках: PNG/HTML в ADS_AI_ARTIFACTS_DIR.
- Индивидуальные задержки до/после шага из act.meta (delay_before_ms, delay_after_ms).
- Автосабмит после fill при meta.{submit|press_enter|commit}=True или если значение оканчивается '\n'.
- Безопасное логирование (короткие строки), не шумит.

ENV-переключатели (все опц., значения по умолчанию в скобках):
- ADS_AI_VISION_AUTOSCROLL (1)
- ADS_AI_VISION_ANIMATE (1)
- ADS_AI_VISION_TYPE_HYBRID (1), ADS_AI_VISION_TYPE_CHUNK (5), ADS_AI_VISION_TYPE_DELAY (0.04)
- ADS_AI_VISION_CLEAR_BEFORE (0)
- ADS_AI_VISION_RETRIES (2), ADS_AI_VISION_RETRY_BACKOFF_MS (180)
- ADS_AI_VISION_WAIT_AFTER_MS (400) — используется если аргумент wait_after_ms не задан
- ADS_AI_VISION_POINT_JITTER (3.0) — пикселей внутри bbox
- ADS_AI_VISION_ARTIFACTS (1), ADS_AI_ARTIFACTS_DIR (./artifacts/screenshots)
"""

from typing import Dict, Iterable, List, Optional, Tuple
import os
import time
import random
import math
import pathlib
import traceback

from .schema import Action, ExecResult, VerifyIssue, BBox

from ads_ai.browser.pixel import (
    mouse_click,
    mouse_double_click,
    type_text_cdp,
    move_and_focus,
    press_enter,
    highlight_bbox,
    mouse_move,
)


# ---------- утилиты координат/вьюпорта ----------

def _center(bb: BBox) -> Tuple[float, float]:
    return bb.center()


def _get_viewport_state(driver) -> Tuple[float, float, float, float]:
    try:
        s = driver.execute_script(
            "return {sx:window.pageXOffset||0, sy:window.pageYOffset||0, "
            "vw:window.innerWidth||0, vh:window.innerHeight||0};"
        ) or {}
        return float(s.get("sx") or 0.0), float(s.get("sy") or 0.0), float(s.get("vw") or 0.0), float(s.get("vh") or 0.0)
    except Exception:
        return 0.0, 0.0, 0.0, 0.0


def _scroll_into_view_center(driver, px: float, py: float) -> None:
    try:
        driver.execute_script(
            "var y=arguments[0], x=arguments[1]; var vh=window.innerHeight||0, vw=window.innerWidth||0;"
            "window.scrollTo({top: Math.max(0, y - vh*0.5), left: Math.max(0, x - vw*0.5), behavior: 'instant'});",
            float(py), float(px),
        )
    except Exception:
        pass


def _animate_mouse(driver, tx: float, ty: float, *, steps: int = 10) -> None:
    """Плавное движение курсора вдоль квадратичной кривой из центра вьюпорта к (tx, ty)."""
    try:
        sx, sy, vw, vh = _get_viewport_state(driver)
        cx, cy = vw / 2.0, vh / 2.0
        vx, vy = tx - sx, ty - sy  # координаты цели во вьюпорте
        kx, ky = (cx + vx) / 2.0, (cy + vy) / 2.0
        steps = max(4, int(steps))
        for i in range(1, steps + 1):
            t = i / steps
            x = (1 - t) ** 2 * cx + 2 * (1 - t) * t * kx + t ** 2 * vx
            y = (1 - t) ** 2 * cy + 2 * (1 - t) * t * ky + t ** 2 * vy
            try:
                mouse_move(driver, x, y)
            except Exception:
                break
            time.sleep(0.018)
    except Exception:
        pass


# ---------- hit-test и выбор точки внутри bbox ----------

_JS_PICK_POINT = r"""
(function(x1, y1, x2, y2){
  try{
    var sx = window.pageXOffset||0, sy = window.pageYOffset||0;
    var vx1 = Math.max(0, x1 - sx), vy1 = Math.max(0, y1 - sy);
    var vx2 = Math.max(0, x2 - sx), vy2 = Math.max(0, y2 - sy);
    var w = Math.max(1, vx2 - vx1), h = Math.max(1, vy2 - vy1);

    function visible(el){
      if(!el) return false;
      try{
        var cs = getComputedStyle(el);
        if(cs.visibility==='hidden' || cs.display==='none' || cs.pointerEvents==='none') return false;
      }catch(_){}
      try{
        if(el.getClientRects && el.getClientRects().length===0) return false;
      }catch(_){}
      return true;
    }

    // сетка из 9-16 точек
    var steps = 4;
    for(var iy=0; iy<steps; iy++){
      for(var ix=0; ix<steps; ix++){
        var px = Math.floor(vx1 + (ix + 0.5) * w / steps);
        var py = Math.floor(vy1 + (iy + 0.5) * h / steps);
        var el = document.elementFromPoint(px, py);
        if(visible(el)){
          return {x: px + sx, y: py + sy};
        }
      }
    }
    // центр по умолчанию
    return {x: (vx1+vx2)/2 + sx, y: (vy1+vy2)/2 + sy};
  }catch(e){
    return null;
  }
})(arguments[0], arguments[1], arguments[2], arguments[3]);
"""

def _pick_point_in_bbox(driver, bb: BBox, jitter: float) -> Tuple[float, float]:
    """Выбрать «живую» точку внутри bbox через elementFromPoint; иначе центр.
    Небольшой jitter в пределах bbox, чтобы избегать границ.
    """
    x1, y1, x2, y2 = bb.left, bb.top, bb.right, bb.bottom
    try:
        res = driver.execute_script(_JS_PICK_POINT, float(x1), float(y1), float(x2), float(y2))
        if isinstance(res, dict) and "x" in res and "y" in res:
            px, py = float(res["x"]), float(res["y"])
        else:
            px, py = _center(bb)
    except Exception:
        px, py = _center(bb)

    # jitter в ограничениях bbox
    if jitter > 0.0:
        jx = random.uniform(-jitter, jitter)
        jy = random.uniform(-jitter, jitter)
        px = min(max(px + jx, x1 + 1), x2 - 1)
        py = min(max(py + jy, y1 + 1), y2 - 1)
    return px, py


# ---------- фокус/очистка/ввод ----------

def _try_focus_deep_at(driver, px: float, py: float) -> bool:
    js = r"""
    (function(x, y){
      try{
        var sx = window.pageXOffset||0, sy = window.pageYOffset||0;
        var vx = Math.max(0, x - sx), vy = Math.max(0, y - sy);
        function isEditable(el){
          if(!el) return false;
          var tag = (el.tagName||'').toLowerCase();
          if(tag==='input' || tag==='textarea') return true;
          try{ if(el.isContentEditable) return true; }catch(_){ }
          return false;
        }
        function findDeepEditable(el){
          if(!el) return null;
          if(isEditable(el)) return el;
          try{
            var sr = el.shadowRoot; if(sr){
              var cand = sr.querySelector('input,textarea,[contenteditable="true"], [contenteditable=""]');
              if(cand) return cand;
            }
          }catch(_){ }
          var cur = el;
          for(var i=0; i<6 && cur; i++){
            try{
              var sr2 = cur.shadowRoot; if(sr2){
                var cand2 = sr2.querySelector('input,textarea,[contenteditable="true"], [contenteditable=""]');
                if(cand2) return cand2;
              }
            }catch(_){ }
            cur = cur.parentElement;
          }
          return null;
        }
        var offsets = [[0,0],[4,2],[-3,1],[2,4],[0,6],[-4,3]];
        var editable = null;
        for(var k=0;k<offsets.length;k++){
          var dx = offsets[k][0], dy = offsets[k][1];
          var el = document.elementFromPoint(vx+dx, vy+dy);
          editable = findDeepEditable(el) || (isEditable(el)? el : null);
          if(editable) break;
        }
        if(!editable){
          var ae = document.activeElement; if(isEditable(ae)) editable = ae;
        }
        if(!editable){ return false; }
        try{ editable.focus({preventScroll:true}); }catch(_){ try{ editable.focus(); }catch(__){} }
        try{
          if(editable.setSelectionRange){ var L = (editable.value||'').length; editable.setSelectionRange(L, L); }
        }catch(_){ }
        return true;
      }catch(e){ return false; }
    })(arguments[0], arguments[1]);
    """
    try:
        return bool(driver.execute_script(js, float(px), float(py)))
    except Exception:
        return False


def _clear_active_editable(driver) -> bool:
    """Очистить текущее активное поле ввода корректно, с событиями."""
    js = r"""
    (function(){
      try{
        var ae = document.activeElement;
        if(!ae) return false;
        var tag = (ae.tagName||'').toLowerCase();
        if(tag==='input' || tag==='textarea'){
          var old = ae.value||'';
          if(old.length===0) return true;
          ae.value = '';
          try{ ae.dispatchEvent(new Event('input', {bubbles:true, composed:true})); }catch(_){}
          try{ ae.dispatchEvent(new Event('change', {bubbles:true, composed:true})); }catch(_){}
          return true;
        }
        if(ae && ae.isContentEditable){
          if((ae.textContent||'').length===0) return true;
          ae.textContent = '';
          try{ ae.dispatchEvent(new Event('input', {bubbles:true, composed:true})); }catch(_){}
          try{ ae.dispatchEvent(new Event('change', {bubbles:true, composed:true})); }catch(_){}
          return true;
        }
        return false;
      }catch(e){ return false; }
    })();
    """
    try:
        return bool(driver.execute_script(js))
    except Exception:
        return False


def _programmatic_set_value(driver, px: float, py: float, text: str) -> bool:
    js = r"""
    (function(x, y, value){
      try{
        var sx = window.pageXOffset||0, sy = window.pageYOffset||0;
        var vx = Math.max(0, x - sx), vy = Math.max(0, y - sy);
        function findEditable(el){
          function isEd(n){
            if(!n) return false; var tag=(n.tagName||'').toLowerCase();
            if(tag==='input' || tag==='textarea') return true;
            try{ if(n.isContentEditable) return true; }catch(_){ }
            return false;
          }
          if(isEd(el)) return el;
          try{
            var sr = el && el.shadowRoot;
            if(sr){ var t = sr.querySelector('input,textarea,[contenteditable="true"], [contenteditable=""]'); if(t) return t; }
          }catch(_){}
          var cur = el;
          for(var i=0;i<6 && cur;i++){
            try{
              var sr2 = cur.shadowRoot;
              if(sr2){ var t2 = sr2.querySelector('input,textarea,[contenteditable="true"], [contenteditable=""]'); if(t2) return t2; }
            }catch(_){}
            cur = cur.parentElement;
          }
          return null;
        }
        var el = document.elementFromPoint(vx, vy);
        var target = findEditable(el) || document.activeElement;
        if(!target) return false;
        var tag = (target.tagName||'').toLowerCase();
        if(tag==='input' || tag==='textarea'){
          try{ target.focus({preventScroll:true}); }catch(_){ try{ target.focus(); }catch(__){} }
          try{ target.value = String(value); }catch(_){ return false; }
          try{ target.dispatchEvent(new Event('input', {bubbles:true, composed:true})); }catch(_){ }
          try{ target.dispatchEvent(new Event('change', {bubbles:true, composed:true})); }catch(_){ }
          return true;
        }
        try{
          if(target.isContentEditable){
            target.focus();
            target.textContent = String(value);
            target.dispatchEvent(new Event('input', {bubbles:true, composed:true}));
            target.dispatchEvent(new Event('change', {bubbles:true, composed:true}));
            return true;
          }
        }catch(_){}
        return false;
      }catch(e){ return false; }
    })(arguments[0], arguments[1], arguments[2]);
    """
    try:
        return bool(driver.execute_script(js, float(px), float(py), str(text)))
    except Exception:
        return False


def _type_hybrid(driver, text: str, *, chunk: int = 6, delay: float = 0.04) -> None:
    if not text:
        return
    text = str(text)
    chunk = max(1, int(chunk))
    for i in range(0, len(text), chunk):
        part = text[i : i + chunk]
        try:
            type_text_cdp(driver, part)
        except Exception:
            break
        time.sleep(max(0.0, float(delay)))


def _active_value(driver) -> Tuple[bool, str]:
    """Считать текущее значение активного ввода (input/textarea/contenteditable)."""
    js = r"""
    (function(){
      try{
        var ae = document.activeElement; if(!ae) return [false, ''];
        var tag = (ae.tagName||'').toLowerCase();
        if(tag==='input' || tag==='textarea'){ return [true, String(ae.value||'')]; }
        if(ae.isContentEditable){ return [true, String(ae.textContent||'')]; }
        return [false, ''];
      }catch(e){ return [false, '']; }
    })();
    """
    try:
        ok, val = driver.execute_script(js)
        return bool(ok), str(val or "")
    except Exception:
        return False, ""


# ---------- артефакты на ошибке ----------

def _artifact_dir() -> Optional[pathlib.Path]:
    if os.getenv("ADS_AI_VISION_ARTIFACTS", "1").lower() not in ("1", "true", "yes", "on"):
        return None
    base = os.getenv("ADS_AI_ARTIFACTS_DIR", "./artifacts/screenshots")
    try:
        p = pathlib.Path(base).resolve()
        p.mkdir(parents=True, exist_ok=True)
        return p
    except Exception:
        return None


def _save_screenshot(driver, label: str) -> Optional[str]:
    p = _artifact_dir()
    if not p:
        return None
    ts = int(time.time() * 1000)
    path = p / f"vision_{label}_{ts}.png"
    try:
        png = driver.get_screenshot_as_png()
        path.write_bytes(png)
        return str(path)
    except Exception:
        return None


def _save_dom(driver, label: str) -> Optional[str]:
    p = _artifact_dir()
    if not p:
        return None
    ts = int(time.time() * 1000)
    path = p / f"vision_{label}_{ts}.html"
    try:
        html = driver.page_source or ""
        path.write_text(html, encoding="utf-8", errors="ignore")
        return str(path)
    except Exception:
        return None


# ---------- основная логика ----------

def execute(
    driver,
    actions: Iterable[Action],
    id2bbox: Dict[str, BBox],
    *,
    highlight: bool = False,
    wait_after_ms: int = 400,
) -> ExecResult:
    """Execute pixel-level actions in order. Returns ExecResult.

    - highlight=True рисует временную рамку вокруг цели.
    - wait_after_ms: базовая стабилизационная задержка (дополняется wait_* при наличии).
    """

    applied: List[Action] = []
    issues: List[VerifyIssue] = []
    logs: List[str] = []
    changed = False

    # waits (ленивый импорт)
    try:
        from ads_ai.browser.waits import ensure_ready_state, wait_dom_stable  # type: ignore
    except Exception:
        ensure_ready_state = None  # type: ignore
        wait_dom_stable = None  # type: ignore

    # ENV-настройки
    env_wait_after = int(os.getenv("ADS_AI_VISION_WAIT_AFTER_MS", str(wait_after_ms or 400)))
    wait_after_ms = env_wait_after if wait_after_ms is None else wait_after_ms
    do_autoscroll = os.getenv("ADS_AI_VISION_AUTOSCROLL", "1").lower() in ("1", "true", "yes", "on")
    do_animate = os.getenv("ADS_AI_VISION_ANIMATE", "1").lower() in ("1", "true", "yes", "on")
    type_hybrid = os.getenv("ADS_AI_VISION_TYPE_HYBRID", "1").lower() in ("1", "true", "yes", "on")
    type_chunk = int(os.getenv("ADS_AI_VISION_TYPE_CHUNK", "5") or 5)
    type_delay = float(os.getenv("ADS_AI_VISION_TYPE_DELAY", "0.04") or 0.04)
    clear_before = os.getenv("ADS_AI_VISION_CLEAR_BEFORE", "0").lower() in ("1", "true", "yes", "on")
    retries = max(0, int(os.getenv("ADS_AI_VISION_RETRIES", "2") or 2))
    retry_backoff_ms = max(0, int(os.getenv("ADS_AI_VISION_RETRY_BACKOFF_MS", "180") or 180))
    point_jitter = max(0.0, float(os.getenv("ADS_AI_VISION_POINT_JITTER", "3.0") or 3.0))

    t0 = time.perf_counter()

    # helper: стабилизация
    def _stabilize(idle_ms: int = 220, timeout_sec: int = 3) -> None:
        try:
            if ensure_ready_state:
                ensure_ready_state(driver, timeout=3.0)
            if wait_dom_stable:
                wait_dom_stable(driver, idle_ms=int(idle_ms), timeout_sec=int(timeout_sec))
        except Exception:
            pass

    # helper: выполнить действие с ретраями
    def _do_action_once(act: Action) -> Tuple[bool, Optional[str]]:
        """Возвращает (ok, err_msg)."""
        bb = id2bbox.get(act.id)
        if not bb:
            return False, "bbox_missing"
        if bb.width <= 0 or bb.height <= 0:
            return False, f"bbox_invalid {bb.as_tuple()}"

        # выбрать точку
        x, y = _pick_point_in_bbox(driver, bb, jitter=point_jitter)

        # автоскролл
        if do_autoscroll:
            _scroll_into_view_center(driver, x, y)

        # хайлайт
        if highlight:
            try:
                highlight_bbox(driver, bb.as_tuple())
            except Exception:
                pass

        # задержка из меты (до)
        try:
            mb = getattr(act, "meta", None) or {}
            d_before = int(mb.get("delay_before_ms") or 0)
            if d_before > 0:
                time.sleep(d_before / 1000.0)
        except Exception:
            pass

        # анимация подводки курсора
        if do_animate and act.kind in ("click", "double_click", "focus", "fill"):
            _animate_mouse(driver, x, y, steps=10)

        # исполнение
        if act.kind == "click":
            mouse_click(driver, x, y)
            return True, None

        if act.kind == "double_click":
            try:
                mouse_double_click(driver, x, y)
            except Exception:
                # fallback: два обычных клика
                mouse_click(driver, x, y)
                time.sleep(0.05)
                mouse_click(driver, x, y)
            return True, None

        if act.kind == "focus":
            move_and_focus(driver, x, y)
            return True, None

        if act.kind == "press_enter":
            press_enter(driver)
            return True, None

        if act.kind == "fill":
            val = "" if act.value is None else str(act.value)

            # фокус на элемент
            move_and_focus(driver, x, y)
            time.sleep(0.06)
            focused = _try_focus_deep_at(driver, x, y)

            # очистка (env или meta.clear=True)
            do_clear = clear_before or bool((getattr(act, "meta", None) or {}).get("clear"))
            if do_clear:
                _clear_active_editable(driver)

            # ввод
            try:
                if type_hybrid:
                    _type_hybrid(driver, val, chunk=type_chunk, delay=type_delay)
                else:
                    type_text_cdp(driver, val)
            except Exception:
                focused = False  # форснуть fallback-путь

            # проверка наличия текста
            try:
                ok1, cur = _active_value(driver)
                if not ok1 or len(cur) == 0:
                    # программный set + события
                    if not _programmatic_set_value(driver, x, y, val):
                        # ещё одна попытка CDP
                        try:
                            move_and_focus(driver, x, y)
                            time.sleep(0.05)
                            type_text_cdp(driver, val)
                        except Exception:
                            pass
            except Exception:
                pass

            # пост-верификация: value_mismatch
            ok2, cur2 = _active_value(driver)
            if ok2 and (cur2 or val):
                if cur2 != val:
                    return False, f"value_mismatch expected_len={len(val)} got_len={len(cur2)}"

            # опциональный submit/Enter
            try:
                meta = getattr(act, "meta", None) or {}
                wants_submit = bool(meta.get("submit") or meta.get("press_enter") or meta.get("commit"))
                if not wants_submit and val.endswith("\n"):
                    wants_submit = True
                if wants_submit:
                    press_enter(driver)
            except Exception:
                pass

            return True, None

        return False, f"unknown_action:{act.kind}"

    # основной цикл с ретраями
    for act in list(actions or []):
        # быстрые проверки bbox
        bb = id2bbox.get(act.id)
        if not bb:
            issues.append(VerifyIssue(id=act.id, reason="bbox_missing"))
            logs.append(f"skip {act.id}: bbox_missing")
            continue
        if bb.width <= 0 or bb.height <= 0:
            issues.append(VerifyIssue(id=act.id, reason="bbox_invalid"))
            logs.append(f"skip {act.id}: bbox_invalid {bb.as_tuple()}")
            continue

        attempt = 0
        last_err: Optional[str] = None
        while True:
            attempt += 1
            try:
                ok, err = _do_action_once(act)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                ok, err = False, f"exec_failed:{e}"
                # артефакты при исключении
                sp = _save_screenshot(driver, f"{act.kind}_exception")
                dp = _save_dom(driver, f"{act.kind}_exception")
                if sp:
                    logs.append(f"artifact_screenshot={sp}")
                if dp:
                    logs.append(f"artifact_dom={dp}")
                # короткий трейс в логи (одной строкой)
                tb = traceback.format_exc(limit=1).strip().replace("\n", " | ")
                logs.append(f"trace: {tb}")

            if ok:
                changed = True if act.kind in ("click", "double_click", "fill") else changed
                applied.append(act)
                msg = f"{act.kind} {act.id}"
                if act.kind == "fill":
                    msg += f" len={len(str(act.value or ''))}"
                # индивидуальная задержка после шага
                try:
                    ma = getattr(act, "meta", None) or {}
                    d_after = int(ma.get("delay_after_ms") or 0)
                except Exception:
                    d_after = 0
                logs.append(msg)
                # стабилизация
                _stabilize()
                # пост-задержки
                if d_after > 0:
                    time.sleep(d_after / 1000.0)
                if wait_after_ms and wait_after_ms > 0:
                    time.sleep(wait_after_ms / 1000.0)
                break  # к следующему действию

            # не ок: оформить issue/решить, повторять ли
            last_err = err or "exec_failed"
            if attempt <= retries + 1:
                # небольшой бэкофф + повторная прокрутка
                time.sleep(retry_backoff_ms / 1000.0)
                try:
                    # легкая «встряска» страницы перед повтором
                    sx, sy, vw, vh = _get_viewport_state(driver)
                    driver.execute_script("window.scrollBy(0, Math.round(Math.random()*12-6));")
                except Exception:
                    pass
                _stabilize()
                continue
            else:
                # предел ретраев
                sev = "warn" if act.kind in ("focus", "press_enter") else "error"
                issues.append(VerifyIssue(id=act.id, reason=last_err, severity=sev))
                logs.append(f"fail {act.id}: {last_err}")
                # сохранить артефакты на окончательной неудаче
                sp = _save_screenshot(driver, f"{act.kind}_fail")
                dp = _save_dom(driver, f"{act.kind}_fail")
                if sp:
                    logs.append(f"artifact_screenshot={sp}")
                if dp:
                    logs.append(f"artifact_dom={dp}")
                # даже при fail продолжаем остальные шаги
                # короткая пауза, чтобы страница пришла в себя
                time.sleep(0.12)
                break

    dt_ms = int((time.perf_counter() - t0) * 1000)
    return ExecResult(changed=changed, applied=applied, issues=issues, logs=logs, duration_ms=dt_ms)


__all__ = ["execute"]
