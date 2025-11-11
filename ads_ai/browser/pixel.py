from __future__ import annotations

"""
Low‑level pixel interactions via Chrome DevTools Protocol (CDP).

This module provides small helpers to perform coordinate‑based actions:
 - mouse_move / mouse_click / mouse_double_click
 - type_text_cdp (text injection independent of focused element behavior)
 - key_press / press_enter
 - highlight_bbox (debug overlay)

CDP is required (Chromium‑based drivers). If CDP is not available, functions
raise RuntimeError with a clear message so callers can degrade gracefully.
"""

from typing import Any, Dict, Optional, Tuple, Union

try:  # Optional imports for graceful fallback
    from selenium.webdriver.remote.webdriver import WebDriver
except Exception:  # pragma: no cover
    WebDriver = Any  # type: ignore


Coord = Union[int, float]
BBoxLike = Union[Tuple[Coord, Coord, Coord, Coord], Dict[str, Any]]


def _supports_cdp(d: WebDriver) -> bool:
    return hasattr(d, "execute_cdp_cmd")


def _cdp(d: WebDriver, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
    if not _supports_cdp(d):
        raise RuntimeError("CDP is not supported by this driver (need Chromium/DevTools)")
    # type: ignore[attr-defined]
    return d.execute_cdp_cmd(method, params)  # pyright: ignore[reportAttributeAccessIssue]


def mouse_move(d: WebDriver, x: Coord, y: Coord, *, modifiers: int = 0) -> None:
    _cdp(
        d,
        "Input.dispatchMouseEvent",
        {"type": "mouseMoved", "x": float(x), "y": float(y), "button": "none", "modifiers": int(modifiers), "clickCount": 0},
    )


def mouse_click(
    d: WebDriver,
    x: Coord,
    y: Coord,
    *,
    button: str = "left",
    click_count: int = 1,
    modifiers: int = 0,
) -> None:
    b = button.lower()
    if b not in ("left", "right", "middle"):
        b = "left"
    _cdp(
        d,
        "Input.dispatchMouseEvent",
        {"type": "mouseMoved", "x": float(x), "y": float(y), "button": "none", "modifiers": int(modifiers), "clickCount": 0},
    )
    _cdp(
        d,
        "Input.dispatchMouseEvent",
        {"type": "mousePressed", "x": float(x), "y": float(y), "button": b, "clickCount": int(click_count), "modifiers": int(modifiers)},
    )
    _cdp(
        d,
        "Input.dispatchMouseEvent",
        {"type": "mouseReleased", "x": float(x), "y": float(y), "button": b, "clickCount": int(click_count), "modifiers": int(modifiers)},
    )


def mouse_double_click(d: WebDriver, x: Coord, y: Coord, *, button: str = "left", modifiers: int = 0) -> None:
    mouse_click(d, x, y, button=button, click_count=2, modifiers=modifiers)


def type_text_cdp(d: WebDriver, text: str) -> None:
    if not text:
        return
    _cdp(d, "Input.insertText", {"text": str(text)})


def key_press(d: WebDriver, key: str, *, modifiers: int = 0) -> None:
    """Press a key using CDP (keydown+keyup). Accepts e.g. "Enter", "Tab", "Escape"."""
    k = str(key or "")
    payload: Dict[str, Any] = {"type": "keyDown", "key": k, "modifiers": int(modifiers)}
    _cdp(d, "Input.dispatchKeyEvent", payload)
    payload_up = dict(payload)
    payload_up["type"] = "keyUp"
    _cdp(d, "Input.dispatchKeyEvent", payload_up)


def press_enter(d: WebDriver) -> None:
    key_press(d, "Enter")


def move_and_focus(d: WebDriver, x: Coord, y: Coord) -> None:
    """Move mouse to (x,y) and perform a light focusing click."""
    mouse_click(d, x, y, button="left", click_count=1)


def _bbox_from_like(b: BBoxLike) -> Tuple[float, float, float, float]:
    if isinstance(b, (tuple, list)) and len(b) >= 4:
        x1, y1, x2, y2 = b[:4]
        return float(x1), float(y1), float(x2), float(y2)
    if isinstance(b, dict):
        def _v(*keys: str, default: float = 0.0) -> float:
            for k in keys:
                if k in b:
                    try:
                        return float(b[k])  # type: ignore[arg-type]
                    except Exception:
                        pass
            return default
        return _v("x1", "left"), _v("y1", "top"), _v("x2", "right"), _v("y2", "bottom")
    raise TypeError("Unsupported bbox format")


def highlight_bbox(
    d: WebDriver,
    bbox: BBoxLike,
    *,
    color: str = "rgba(65,105,225,0.35)",
    border: str = "2px solid rgba(65,105,225,0.9)",
    duration_ms: int = 700,
) -> None:
    """Draw a temporary rectangle over the page for debugging/diagnostics."""
    x1, y1, x2, y2 = _bbox_from_like(bbox)
    w = max(1, int(x2 - x1))
    h = max(1, int(y2 - y1))
    js = (
        "(function(){"
        "var d=document.createElement('div');"
        "d.setAttribute('data-adsai-bbox','1');"
        "d.style.position='fixed';"
        f"d.style.left='{int(x1)}px';d.style.top='{int(y1)}px';"
        f"d.style.width='{w}px';d.style.height='{h}px';"
        f"d.style.background='{color}';d.style.border='{border}';"
        "d.style.borderRadius='6px';"
        "d.style.zIndex='2147483647';"
        "document.body.appendChild(d);"
        f"setTimeout(function(){{try{{d.remove();}}catch(_)}}, {int(duration_ms)});"
        "})();"
    )
    try:
        d.execute_script(js)
    except Exception:
        # Non-fatal: overlay is purely diagnostic
        pass


__all__ = [
    "mouse_move",
    "mouse_click",
    "mouse_double_click",
    "type_text_cdp",
    "key_press",
    "press_enter",
    "move_and_focus",
    "highlight_bbox",
]

