from __future__ import annotations

"""
Lightweight types for the Vision pipeline (OCR → LLM plan → Execute → Verify).

This module has zero heavy dependencies and can be safely imported anywhere.
It intentionally uses plain dataclasses + typing, and exposes small helpers for
parsing JSON-ish payloads returned by LLMs.
"""

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Iterable, List, Literal, Optional, Tuple, Union

Number = Union[int, float]


# ----------------------------- Geometry ---------------------------------

@dataclass(frozen=True)
class BBox:
    """Axis-aligned bounding box in pixel coordinates (inclusive-exclusive).

    Coordinates follow (x1, y1, x2, y2) where (x1, y1) is top-left and
    (x2, y2) is bottom-right. Expected: x2 >= x1, y2 >= y1.
    """

    x1: Number
    y1: Number
    x2: Number
    y2: Number

    def as_tuple(self) -> Tuple[Number, Number, Number, Number]:
        return (self.x1, self.y1, self.x2, self.y2)

    @property
    def width(self) -> float:
        try:
            return float(self.x2) - float(self.x1)
        except Exception:
            return 0.0

    @property
    def height(self) -> float:
        try:
            return float(self.y2) - float(self.y1)
        except Exception:
            return 0.0

    def center(self) -> Tuple[float, float]:
        return (float(self.x1) + self.width / 2.0, float(self.y1) + self.height / 2.0)

    def contains(self, x: Number, y: Number) -> bool:
        xf, yf = float(x), float(y)
        return float(self.x1) <= xf <= float(self.x2) and float(self.y1) <= yf <= float(self.y2)

    def clip(self, max_w: Optional[Number] = None, max_h: Optional[Number] = None) -> "BBox":
        """Clamp bbox to the image size (if provided)."""
        x1, y1, x2, y2 = self.as_tuple()
        if max_w is not None:
            x1 = max(0.0, min(float(max_w), float(x1)))
            x2 = max(0.0, min(float(max_w), float(x2)))
        if max_h is not None:
            y1 = max(0.0, min(float(max_h), float(y1)))
            y2 = max(0.0, min(float(max_h), float(y2)))
        if x2 < x1:
            x1, x2 = x2, x1
        if y2 < y1:
            y1, y2 = y2, y1
        return BBox(x1, y1, x2, y2)

    @staticmethod
    def from_any(obj: Any) -> "BBox":
        """Coerce list/tuple/dict into BBox.

        Accepts:
          - [x1, y1, x2, y2]
          - {"x1":..., "y1":..., "x2":..., "y2":...}
          - {"left":..., "top":..., "right":..., "bottom":...}
        """
        if isinstance(obj, (list, tuple)) and len(obj) >= 4:
            x1, y1, x2, y2 = obj[:4]
            return BBox(float(x1), float(y1), float(x2), float(y2))
        if isinstance(obj, dict):
            def _val(*keys: str, default: Number = 0.0) -> Number:
                for k in keys:
                    if k in obj:
                        try:
                            return float(obj[k])  # type: ignore[arg-type]
                        except Exception:
                            pass
                return default

            x1 = _val("x1", "left")
            y1 = _val("y1", "top")
            x2 = _val("x2", "right")
            y2 = _val("y2", "bottom")
            return BBox(x1, y1, x2, y2)
        raise TypeError(f"Unsupported bbox format: {type(obj)!r}")


# ----------------------------- OCR / Actions ----------------------------

@dataclass(frozen=True)
class OCRItem:
    """Single OCR block detected on screenshot."""

    id: str
    bbox: BBox
    text: str
    score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["bbox"] = self.bbox.as_tuple()
        return d


ActionKind = Literal["fill", "click", "focus", "press_enter"]


@dataclass(frozen=True)
class Action:
    """Executable action produced by the planner."""

    id: str
    kind: ActionKind
    value: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {"id": self.id, "action": self.kind}
        if self.value is not None:
            d["value"] = self.value
        return d

    @staticmethod
    def from_any(obj: Any) -> "Action":
        if not isinstance(obj, dict):
            raise TypeError("Action must be an object")
        _id = str(obj.get("id") or obj.get("target") or obj.get("idx") or "").strip()
        if not _id:
            raise ValueError("Action.id is required")
        kind_raw = str(obj.get("action") or obj.get("kind") or "").strip().lower()
        if kind_raw not in {"fill", "click", "focus", "press_enter"}:
            raise ValueError(f"Unsupported action kind: {kind_raw!r}")
        val = obj.get("value")
        if val is not None:
            val = str(val)
        return Action(id=_id, kind=kind_raw, value=val)  # type: ignore[arg-type]


@dataclass(frozen=True)
class Plan:
    actions: List[Action] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {"actions": [a.to_dict() for a in self.actions]}

    @staticmethod
    def from_any(obj: Any) -> "Plan":
        if isinstance(obj, dict) and isinstance(obj.get("actions"), list):
            return Plan(actions=[Action.from_any(a) for a in obj["actions"]])
        if isinstance(obj, list):
            return Plan(actions=[Action.from_any(a) for a in obj])
        raise TypeError("Plan must be an object with 'actions' or a list of actions")


# ----------------------------- Verification / Exec ----------------------

Severity = Literal["warn", "error"]


@dataclass(frozen=True)
class VerifyIssue:
    id: str
    reason: str
    severity: Severity = "error"

    def to_dict(self) -> Dict[str, Any]:
        return {"id": self.id, "reason": self.reason, "severity": self.severity}


@dataclass(frozen=True)
class ExecResult:
    changed: bool
    applied: List[Action] = field(default_factory=list)
    issues: List[VerifyIssue] = field(default_factory=list)
    logs: List[str] = field(default_factory=list)
    duration_ms: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "changed": bool(self.changed),
            "applied": [a.to_dict() for a in self.applied],
            "issues": [i.to_dict() for i in self.issues],
            "logs": list(self.logs),
            "duration_ms": self.duration_ms,
        }


# ----------------------------- Helpers ----------------------------------

def id_bbox_map(items: Iterable[OCRItem]) -> Dict[str, BBox]:
    return {it.id: it.bbox for it in items}


def parse_ocr_items(obj: Any) -> List[OCRItem]:
    out: List[OCRItem] = []
    if not isinstance(obj, list):
        return out
    for i, it in enumerate(obj):
        if not isinstance(it, dict):
            continue
        _id = str(it.get("id") or f"id{i}")
        try:
            bb = BBox.from_any(it.get("bbox") or it.get("box") or [0, 0, 0, 0])
        except Exception:
            continue
        txt = str(it.get("text") or it.get("label") or "")
        try:
            score = float(it.get("score") or 0.0)
        except Exception:
            score = 0.0
        out.append(OCRItem(id=_id, bbox=bb, text=txt, score=score))
    return out


def parse_actions(obj: Any) -> List[Action]:
    try:
        return Plan.from_any(obj).actions
    except Exception:
        return []


__all__ = [
    "BBox",
    "OCRItem",
    "Action",
    "Plan",
    "VerifyIssue",
    "ExecResult",
    "id_bbox_map",
    "parse_ocr_items",
    "parse_actions",
]

