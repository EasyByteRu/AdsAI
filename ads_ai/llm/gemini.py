# ads_ai/llm/gemini.py
from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional, Union, Tuple, TypedDict, Literal

from ads_ai.utils.json_tools import extract_first_json, safe_str
from ads_ai.llm.prompts import (
    plan_prompt,
    repair_prompt,
    # новые промпты для Plan-and-Execute (не ломающие обратную совместимость):
    outline_prompt,
    subgoal_steps_prompt,
    verify_or_adjust_prompt,
)

try:
    import google.generativeai as genai
except ModuleNotFoundError as e:
    raise RuntimeError("Не найден google-generativeai. Установи: pip install google-generativeai") from e


# ------------------------------ Типы ответов (поддержка IDE/типизации) ------------------------------

class Subgoal(TypedDict, total=False):
    id: str
    title: str
    goal: str
    done_when: str
    notes: str


class OutlineResult(TypedDict, total=False):
    subgoals: List[Subgoal]


class VerifyAdjustResult(TypedDict, total=False):
    status: Literal["ok", "retry", "blocked"]
    reason: str
    fix_steps: List[Dict[str, Any]]


# ------------------------------ Класс клиента Gemini ------------------------------

class GeminiClient:
    """
    Удобная обёртка над google-generativeai для задач планирования/ремонта шагов.

    Важные принципы:
      - Возвращаем ТОЛЬКО парснутый JSON (list|dict) либо безопасные дефолты.
      - Любые ошибки LLM/сети => ретраи + fallback_model (если задан).
      - HTML/Task/History/Vars предварительно нормализуются (safe_str + обрезка).
    """

    # внутренние лимиты на размер подсказки (страхуемся от крайне больших DOM)
    _MAX_HTML_CHARS = 200_000  # жёсткий верх на html_view внутри клиента (runtime уже режет, но продублируем)
    _MAX_TASK_CHARS = 8_000    # защита от чрезмерно длинных задач

    def __init__(self, model: str, temperature: float = 0.15, retries: int = 2, fallback_model: Optional[str] = None):
        api_key = os.getenv("GOOGLE_API_KEY")
        if not api_key:
            # Не валим процесс — кто-то может создавать объект заранее; но без ключа нет смысла дергать API.
            raise RuntimeError("Переменная окружения GOOGLE_API_KEY (GEMINI_API_KEY) не задана")
        genai.configure(api_key=api_key)

        self.model_name = model
        self.fallback_model = fallback_model
        self.temperature = float(temperature)
        self.retries = int(retries)

        self._primary = genai.GenerativeModel(self.model_name)
        # Параметры генерации: оставляем минимально детерминированную выдачу
        self._cfg = genai.GenerationConfig(temperature=self.temperature)

    # ------------------------------ Низкоуровневый вызов ------------------------------

    def _call_llm(self, text: str) -> str:
        """
        Единая точка общения с моделью:
          - ретраи c backoff,
          - fallback модель по необходимости.
        Возвращает сырой текст от LLM (может содержать Markdown — выше по стеку мы парсим JSON).
        """
        err: Optional[Exception] = None
        for attempt in range(self.retries + 1):
            try:
                res = self._primary.generate_content([text], generation_config=self._cfg)
                if res and getattr(res, "text", None):
                    return res.text
            except Exception as e:
                err = e
                time.sleep(0.7 * (attempt + 1))  # экспоненциальный backoff (мягкий)

        # fallback-модель (по возможности)
        if self.fallback_model:
            try:
                fm = genai.GenerativeModel(self.fallback_model)
                res = fm.generate_content([text], generation_config=self._cfg)
                if res and getattr(res, "text", None):
                    return res.text
            except Exception as e:
                err = e

        raise RuntimeError(f"LLM failed: {err}")

    # ------------------------------ Вспомогательные утилиты ------------------------------

    @staticmethod
    def _clip(s: str, max_chars: int) -> str:
        """Обрезаем слишком длинные строки (в т.ч. чтобы не выбивать лимиты контекста модели)."""
        if not s:
            return ""
        s = safe_str(s)
        return s[:max_chars]

    @staticmethod
    def _as_json_array(raw: Any) -> List[Any]:
        """Гарантированно вернуть JSON-массив (или пустой список)."""
        return raw if isinstance(raw, list) else []

    @staticmethod
    def _as_json_object(raw: Any) -> Dict[str, Any]:
        """Гарантированно вернуть JSON-объект (или пустой словарь)."""
        return raw if isinstance(raw, dict) else {}

    @staticmethod
    def _normalize_outline(obj: Any) -> OutlineResult:
        """Мягкая валидация/нормализация ответа outline."""
        out: OutlineResult = {"subgoals": []}
        data = obj if isinstance(obj, dict) else {}
        subgoals = data.get("subgoals")
        if isinstance(subgoals, list):
            cleaned: List[Subgoal] = []
            for sg in subgoals:
                if not isinstance(sg, dict):
                    continue
                # минимальный набор для полезности
                sg_id = str(sg.get("id") or "").strip()
                title = str(sg.get("title") or "").strip()
                goal = str(sg.get("goal") or "").strip()
                done_when = str(sg.get("done_when") or "").strip()
                notes = str(sg.get("notes") or "").strip()
                if not (sg_id and title):
                    # сгенерируем простые id/title при их отсутствии
                    if not sg_id:
                        sg_id = f"sg{len(cleaned)+1}"
                    if not title:
                        title = goal[:64] or f"Subgoal {len(cleaned)+1}"
                cleaned.append(
                    {"id": sg_id, "title": title, "goal": goal, "done_when": done_when, "notes": notes}
                )
            out["subgoals"] = cleaned
        return out

    @staticmethod
    def _normalize_verify(obj: Any) -> VerifyAdjustResult:
        """Мягкая валидация/нормализация verify_or_adjust ответа."""
        data = obj if isinstance(obj, dict) else {}
        status = str(data.get("status") or "").lower()
        if status not in {"ok", "retry", "blocked"}:
            status = "retry"
        reason = str(data.get("reason") or "").strip()
        fix_steps = data.get("fix_steps")
        if not isinstance(fix_steps, list):
            fix_steps = []
        # Усечём потенциально лишние поля в fix_steps — рантайм всё равно валидирует по schema
        cleaned_steps: List[Dict[str, Any]] = []
        for st in fix_steps:
            if isinstance(st, dict) and "type" in st:
                cleaned_steps.append(st)
        return {"status": status, "reason": reason, "fix_steps": cleaned_steps}

    # ------------------------------ Публичные удобные методы ------------------------------

    def generate_text(self, text: str) -> str:
        """Сырой текст из модели (без JSON-гарантий)."""
        return self._call_llm(text)

    def generate_json(self, text: str) -> Optional[Union[List[Any], Dict[str, Any]]]:
        """
        Парс JSON из ответа модели. Возвращает list|dict или None (если JSON не смогли извлечь).
        """
        raw = self._call_llm(text)
        return extract_first_json(raw)

    # ------------------------------ Визуальные подсказки (скриншоты) ------------------------------

    # generate_json_with_image removed per request

    # ------------------------------ Совместимые API (не менять) ------------------------------

    def plan_full(
        self,
        html_view: str,
        task: str,
        done_history: List[Dict[str, Any]],
        vars_map: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Сгенерировать массив шагов под текущий DOM и цель (исторический контракт).
        Возвращает безопасный список шагов (может быть пустым).
        """
        prompt = plan_prompt(
            self._clip(html_view, self._MAX_HTML_CHARS),
            self._clip(task, self._MAX_TASK_CHARS),
            done_history,
            vars_map,
        )
        raw = self._call_llm(prompt)
        obj = extract_first_json(raw)
        return self._as_json_array(obj)

    def repair_step(
        self,
        html_view: str,
        task: str,
        history: List[Dict[str, Any]],
        failing_step: Dict[str, Any],
        vars_map: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """
        Починить один проблемный шаг. Возвращает валидный объект шага или None.
        """
        prompt = repair_prompt(
            self._clip(html_view, self._MAX_HTML_CHARS),
            self._clip(task, self._MAX_TASK_CHARS),
            history,
            failing_step,
            vars_map,
        )
        raw = self._call_llm(prompt)
        obj = extract_first_json(raw)
        data = self._as_json_object(obj)
        return data or None

    # ------------------------------ Новые API для Plan-and-Execute ------------------------------

    def plan_outline(self, task: str) -> OutlineResult:
        """
        Разложить большую цель на компактный список подцелей.
        Возвращает объект вида {"subgoals": [...]}, даже если LLM ответила частично.
        """
        prompt = outline_prompt(self._clip(task, self._MAX_TASK_CHARS))
        raw = self._call_llm(prompt)
        obj = extract_first_json(raw)
        return self._normalize_outline(obj)

    def plan_subgoal_steps(
        self,
        html_view: str,
        task: str,
        subgoal: Dict[str, Any],
        done_history: List[Dict[str, Any]],
        vars_map: Dict[str, Any],
        *,
        max_steps: int = 6,
    ) -> List[Dict[str, Any]]:
        """
        Сгенерировать короткий список шагов для КОНКРЕТНОЙ подцели.
        Возвращает список (может быть пустым).
        """
        # Небольшая страховка на случай неверного max_steps
        ms = max(1, min(int(max_steps or 6), 12))
        prompt = subgoal_steps_prompt(
            self._clip(html_view, self._MAX_HTML_CHARS),
            self._clip(task, self._MAX_TASK_CHARS),
            subgoal,
            done_history,
            vars_map,
            max_steps=ms,
        )
        raw = self._call_llm(prompt)
        obj = extract_first_json(raw)
        return self._as_json_array(obj)

    def verify_or_adjust(
        self,
        html_view: str,
        task: str,
        subgoal: Dict[str, Any],
        last_steps: List[Dict[str, Any]],
        vars_map: Dict[str, Any],
    ) -> VerifyAdjustResult:
        """
        Проверить, достигнута ли подцель, и при необходимости предложить короткий фикс.
        Всегда возвращает нормализованный объект (status/reason/fix_steps).
        """
        prompt = verify_or_adjust_prompt(
            self._clip(html_view, self._MAX_HTML_CHARS),
            self._clip(task, self._MAX_TASK_CHARS),
            subgoal,
            last_steps,
            vars_map,
        )
        raw = self._call_llm(prompt)
        obj = extract_first_json(raw)
        return self._normalize_verify(obj)
