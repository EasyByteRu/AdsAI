# ads_ai/plan/repair.py
from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ads_ai.llm.gemini import GeminiClient
from ads_ai.plan.schema import validate_step, StepType
from ads_ai.tracing.trace import JsonlTrace


__all__ = ["LLMRepairer", "make_default_repairer"]


@dataclass
class LLMRepairer:
    """
    Ремонт шага:
      1) Пытаемся через Gemini (контекст: DOM, TASK, HISTORY, VARS)
      2) Если не вышло — делаем локальные «безопасные» фиксы (selector healing и дефолты)
    Возвращаем ВАЛИДНЫЙ шаг (через schema.validate_step) или None.
    """
    ai: GeminiClient
    trace: Optional[JsonlTrace] = None
    max_backoff_sec: float = 3.0

    def repair_step(
        self,
        html_view: str,
        task: str,
        history: List[Dict[str, Any]],
        failing_step: Dict[str, Any],
        vars_map: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        # --- 1) LLM-починка ---
        try:
            if self.trace:
                self.trace.write({"event": "repair_llm_start", "failing": failing_step})
            raw = self.ai.repair_step(html_view, task, history, failing_step, vars_map)
            if raw:
                try:
                    fixed = validate_step(raw)
                    if self.trace:
                        self.trace.write({"event": "repair_llm_ok", "out": fixed})
                    return fixed
                except Exception as e:
                    if self.trace:
                        self.trace.write({"event": "repair_llm_invalid", "err": repr(e), "raw": raw})
        except Exception as e:
            if self.trace:
                self.trace.write({"event": "repair_llm_error", "err": repr(e)})

        # --- 2) Локальные эвристики (безопасные) ---
        healed = self._heuristic_heal(failing_step)
        if healed:
            try:
                fixed = validate_step(healed)
                if self.trace:
                    self.trace.write({"event": "repair_heuristic_ok", "out": fixed})
                return fixed
            except Exception as e:
                if self.trace:
                    self.trace.write({"event": "repair_heuristic_invalid", "err": repr(e), "raw": healed})

        if self.trace:
            self.trace.write({"event": "repair_failed"})
        return None

    # --------------------------------------------------------------------- #
    #                       HEURISTICS (SAFE HEALING)                       #
    # --------------------------------------------------------------------- #

    def _heuristic_heal(self, step: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Мини-фиксы, которые не меняют смысл шага:
          - чистим selector от шумных конструкций (:nth-child, лишние пробелы)
          - стабилизируем text=... (схлопывание пробелов)
          - проставляем дефолты для wait_url/assert/select/… когда поля опущены
        """
        t = str(step.get("type") or "").lower()
        try:
            st = StepType(t)
        except Exception:
            return None

        s = dict(step)  # не мутируем исходник

        # общая нормализация селектора
        if "selector" in s and isinstance(s["selector"], str):
            sel = s["selector"].strip()
            sel = self._cleanup_nth_child(sel)
            sel = self._cleanup_spaces(sel)
            sel = self._normalize_text_selector(sel)
            s["selector"] = sel

        # точечные дефолты (если автор шага забыл)
        if st == StepType.WAIT_URL:
            s.setdefault("regex", False)
            s.setdefault("timeout", 12)

        if st == StepType.WAIT_DOM_STABLE:
            s.setdefault("ms", 1000)
            s.setdefault("timeout", 12)

        if st == StepType.WAIT_VISIBLE:
            s.setdefault("timeout", 12)

        if st == StepType.WAIT:
            try:
                sec = float(s.get("seconds", 0.8))
                if sec <= 0:
                    sec = 0.8
                s["seconds"] = sec
            except Exception:
                s["seconds"] = 0.8

        if st == StepType.SELECT:
            s["by"] = str(s.get("by", "text")).lower()

        if st == StepType.ASSERT_TEXT:
            s["attr"] = str(s.get("attr", "text")).lower()
            s["match"] = str(s.get("match", "contains")).lower()

        if st == StepType.SWITCH_TO_TAB:
            s["by"] = str(s.get("by", "index")).lower()
            s["value"] = str(s.get("value", "0"))

        # safety: пустой селектор в шагах, где он обязателен — не «угадываем»
        if st in {
            StepType.CLICK, StepType.DOUBLE_CLICK, StepType.CONTEXT_CLICK,
            StepType.INPUT, StepType.HOVER, StepType.SCROLL_TO_ELEMENT,
            StepType.SELECT, StepType.FILE_UPLOAD, StepType.EXTRACT, StepType.ASSERT_TEXT,
            StepType.SWITCH_TO_FRAME, StepType.CHECK, StepType.DRAG_AND_DROP
        }:
            if not str(s.get("selector", "")).strip():
                return None

        return s

    @staticmethod
    def _cleanup_nth_child(sel: str) -> str:
        # убираем :nth-child(n) и :nth-of-type(n) — они ломкие
        sel = re.sub(r":nth-child\(\s*\d+\s*\)", "", sel)
        sel = re.sub(r":nth-of-type\(\s*\d+\s*\)", "", sel)
        return sel

    @staticmethod
    def _cleanup_spaces(sel: str) -> str:
        # схлопываем множественные пробелы, убираем пробелы перед/после > , .
        sel = re.sub(r"\s+", " ", sel)
        sel = re.sub(r"\s*>\s*", ">", sel)
        sel = re.sub(r"\s*,\s*", ",", sel)
        sel = sel.strip()
        return sel

    @staticmethod
    def _normalize_text_selector(sel: str) -> str:
        # text="   Foo   Bar  "  -> text=Foo Bar
        if sel.startswith("text="):
            body = sel[5:].strip().strip("'").strip('"')
            body = re.sub(r"\s+", " ", body).strip()
            return f"text={body}"
        return sel


def make_default_repairer(ai: GeminiClient, trace: Optional[JsonlTrace] = None) -> LLMRepairer:
    return LLMRepairer(ai=ai, trace=trace)
