# ads_ai/plan/schema.py
from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional


DEFAULT_WAIT_SEC = 12
STEP_TIMEOUT_SEC = 35


class StepType(str, Enum):
    CLICK = "click"
    DOUBLE_CLICK = "double_click"
    CONTEXT_CLICK = "context_click"

    INPUT = "input"
    PRESS_KEY = "press_key"     # key: "ENTER" | "ESCAPE" | ...
    HOTKEY = "hotkey"           # keys: ["CTRL","A"] или "CTRL+A"

    WAIT = "wait"
    WAIT_VISIBLE = "wait_visible"
    WAIT_URL = "wait_url"
    WAIT_DOM_STABLE = "wait_dom_stable"

    GOTO = "goto"
    GO_BACK = "go_back"
    GO_FORWARD = "go_forward"
    REFRESH = "refresh"

    CHECK = "check"
    LOOP_UNTIL = "loop_until"

    SCROLL = "scroll"
    SCROLL_TO = "scroll_to"
    SCROLL_TO_ELEMENT = "scroll_to_element"

    HOVER = "hover"
    SELECT = "select"
    FILE_UPLOAD = "file_upload"
    DRAG_AND_DROP = "drag_and_drop"

    SWITCH_TO_FRAME = "switch_to_frame"
    SWITCH_TO_DEFAULT = "switch_to_default"

    NEW_TAB = "new_tab"
    SWITCH_TO_TAB = "switch_to_tab"
    CLOSE_TAB = "close_tab"

    EXTRACT = "extract"
    ASSERT_TEXT = "assert_text"
    EVALUATE = "evaluate"

    PAUSE_FOR_HUMAN = "pause_for_human"


ALLOWED_KEYS: Dict[StepType, set[str]] = {
    StepType.CLICK: {"type", "selector"},
    StepType.DOUBLE_CLICK: {"type", "selector"},
    StepType.CONTEXT_CLICK: {"type", "selector"},

    StepType.INPUT: {"type", "selector", "text"},
    StepType.PRESS_KEY: {"type", "key"},
    StepType.HOTKEY: {"type", "keys"},

    StepType.WAIT: {"type", "seconds"},
    StepType.WAIT_VISIBLE: {"type", "selector", "timeout"},
    StepType.WAIT_URL: {"type", "pattern", "regex", "timeout"},
    StepType.WAIT_DOM_STABLE: {"type", "ms", "timeout"},

    StepType.GOTO: {"type", "url"},
    StepType.GO_BACK: {"type"},
    StepType.GO_FORWARD: {"type"},
    StepType.REFRESH: {"type"},

    StepType.CHECK: {"type", "selector", "present", "timeout"},
    StepType.LOOP_UNTIL: {"type", "selector", "present", "timeout", "tick"},

    StepType.SCROLL: {"type", "direction", "amount"},
    StepType.SCROLL_TO: {"type", "to"},
    StepType.SCROLL_TO_ELEMENT: {"type", "selector"},

    StepType.HOVER: {"type", "selector"},
    StepType.SELECT: {"type", "selector", "by", "value"},
    StepType.FILE_UPLOAD: {"type", "selector", "path"},
    StepType.DRAG_AND_DROP: {"type", "source", "target", "to_offset_x", "to_offset_y"},

    StepType.SWITCH_TO_FRAME: {"type", "selector", "index"},
    StepType.SWITCH_TO_DEFAULT: {"type"},

    StepType.NEW_TAB: {"type"},
    StepType.SWITCH_TO_TAB: {"type", "by", "value"},
    StepType.CLOSE_TAB: {"type", "index"},

    StepType.EXTRACT: {"type", "selector", "attr", "var"},
    StepType.ASSERT_TEXT: {"type", "selector", "attr", "match", "value"},
    StepType.EVALUATE: {"type", "script", "var"},

    StepType.PAUSE_FOR_HUMAN: {"type", "reason"},
}


def _ensure_list_keys(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x) for x in v]
    s = str(v)
    # "CTRL+A" → ["CTRL","A"]
    if "+" in s:
        return [p.strip() for p in s.split("+") if p.strip()]
    return [s]


def validate_step(raw: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("Шаг должен быть объектом")

    stype = str(raw.get("type") or "").lower()
    try:
        t = StepType(stype)
    except Exception:
        raise ValueError(f"Неизвестный тип шага: {raw.get('type')}")

    allowed = ALLOWED_KEYS[t]
    out = {k: raw[k] for k in raw.keys() if k in allowed}

    # --- required by type ---
    required: Dict[StepType, List[str]] = {
        StepType.CLICK: ["selector"],
        StepType.DOUBLE_CLICK: ["selector"],
        StepType.CONTEXT_CLICK: ["selector"],

        StepType.INPUT: ["selector", "text"],
        StepType.PRESS_KEY: ["key"],
        StepType.HOTKEY: ["keys"],

        StepType.WAIT: ["seconds"],
        StepType.WAIT_VISIBLE: ["selector"],
        StepType.WAIT_URL: ["pattern"],
        StepType.WAIT_DOM_STABLE: [],

        StepType.GOTO: ["url"],
        StepType.GO_BACK: [],
        StepType.GO_FORWARD: [],
        StepType.REFRESH: [],

        StepType.CHECK: ["selector", "present"],
        StepType.LOOP_UNTIL: ["selector", "present"],

        StepType.SCROLL: ["direction", "amount"],
        StepType.SCROLL_TO: ["to"],
        StepType.SCROLL_TO_ELEMENT: ["selector"],

        StepType.HOVER: ["selector"],
        StepType.SELECT: ["selector", "by", "value"],
        StepType.FILE_UPLOAD: ["selector", "path"],
        StepType.DRAG_AND_DROP: ["source"],  # target ИЛИ offset

        StepType.SWITCH_TO_FRAME: [],
        StepType.SWITCH_TO_DEFAULT: [],

        StepType.NEW_TAB: [],
        StepType.SWITCH_TO_TAB: ["by", "value"],
        StepType.CLOSE_TAB: [],

        StepType.EXTRACT: ["selector", "attr", "var"],
        StepType.ASSERT_TEXT: ["selector", "value"],
        StepType.EVALUATE: ["script"],

        StepType.PAUSE_FOR_HUMAN: [],
    }

    for r in required[t]:
        if r not in out:
            raise ValueError(f"Отсутствует обязательное поле '{r}' в шаге {t.value}")

    # --- defaults / normalization ---
    if t == StepType.WAIT:
        out["seconds"] = float(out.get("seconds", 0.5))

    if t == StepType.SCROLL:
        out["direction"] = str(out.get("direction", "down")).lower()
        out["amount"] = int(out.get("amount", 600))

    if t == StepType.SCROLL_TO:
        out["to"] = str(out.get("to", "bottom")).lower()

    if t == StepType.LOOP_UNTIL:
        out["timeout"] = int(out.get("timeout", STEP_TIMEOUT_SEC))
        tick = out.get("tick") or {"type": "wait", "seconds": 1}
        if not isinstance(tick, dict):
            tick = {"type": "wait", "seconds": 1}
        try:
            out["tick"] = validate_step(tick)
        except Exception:
            out["tick"] = {"type": "wait", "seconds": 1.0}

    if t == StepType.WAIT_VISIBLE:
        out["timeout"] = int(out.get("timeout", DEFAULT_WAIT_SEC))

    if t == StepType.WAIT_URL:
        out["regex"] = bool(out.get("regex", False))
        out["timeout"] = int(out.get("timeout", DEFAULT_WAIT_SEC))

    if t == StepType.WAIT_DOM_STABLE:
        out["ms"] = int(out.get("ms", 1000))
        out["timeout"] = int(out.get("timeout", DEFAULT_WAIT_SEC))

    if t == StepType.SELECT:
        out["by"] = str(out.get("by", "text")).lower()

    if t == StepType.ASSERT_TEXT:
        out["attr"] = str(out.get("attr", "text")).lower()
        out["match"] = str(out.get("match", "contains")).lower()

    if t == StepType.HOTKEY:
        out["keys"] = _ensure_list_keys(out.get("keys"))

    if t == StepType.DRAG_AND_DROP:
        # обязательный source и либо target, либо offsets
        if "target" not in out and not all(k in out for k in ("to_offset_x", "to_offset_y")):
            raise ValueError("drag_and_drop требует 'target' или пару 'to_offset_x'/'to_offset_y'")
        if "to_offset_x" in out:
            out["to_offset_x"] = int(out["to_offset_x"])
        if "to_offset_y" in out:
            out["to_offset_y"] = int(out["to_offset_y"])

    return {"type": t.value, **out}


def validate_plan(raw: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw, list):
        raise ValueError("План должен быть массивом шагов")
    plan: List[Dict[str, Any]] = []
    for idx, item in enumerate(raw):
        try:
            plan.append(validate_step(item))
        except Exception as e:
            # Мягко игнорируем битые шаги; логирование сделаем на уровне рантайма
            continue
    return plan
