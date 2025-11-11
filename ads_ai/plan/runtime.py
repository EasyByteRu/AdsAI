# ads_ai/plan/runtime.py
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement

from ads_ai.config.settings import Settings
from ads_ai.plan.schema import StepType, validate_step, validate_plan
from ads_ai.browser.actions import ACTIONS, ActionContext
from ads_ai.browser.selectors import find, exists
from ads_ai.browser.waits import ensure_ready_state
from ads_ai.browser.humanize import Humanizer
from ads_ai.browser.guards import Guards
from ads_ai.tracing.trace import JsonlTrace
from ads_ai.tracing.artifacts import Artifacts, take_screenshot, save_html_snapshot
from ads_ai.utils.json_tools import safe_str


# ---- Вспомогательные сущности ------------------------------------------------

@dataclass
class ExecStats:
    total_steps: int = 0
    ok_steps: int = 0
    repairs: int = 0
    skips: int = 0
    replans: int = 0
    dom_stable_waits: int = 0
    loops_guard_trips: int = 0


@dataclass
class RunResult:
    done_steps: List[Dict[str, Any]]
    planned_total: int
    stats: ExecStats
    replan_suggested: bool = False


class _VarRenderer:
    """Рендерит ${var} в шагах на основе var_store.get(name, default)."""
    def __init__(self, var_store: Optional[Any]):
        self._store = var_store

    def _get(self, key: str) -> str:
        if not self._store:
            return ""
        getter = getattr(self._store, "get", None)
        if callable(getter):
            try:
                v = getter(key, "")
                return "" if v is None else str(v)
            except Exception:
                return ""
        # допускаем .vars dict
        try:
            return str(getattr(self._store, "vars", {}).get(key, ""))
        except Exception:
            return ""

    def render(self, val: Any) -> Any:
        import re
        if isinstance(val, str):
            def repl(m):
                return self._get(m.group(1))
            return re.sub(r"\$\{([A-Za-z0-9_]+)\}", repl, val)
        if isinstance(val, dict):
            return {k: self.render(v) for k, v in val.items()}
        if isinstance(val, list):
            return [self.render(v) for v in val]
        return val


# ---- Runtime -----------------------------------------------------------------

class Runtime:
    """
    Универсальный исполнитель плана шагов.
    - Ремонт шагов делегируется объекту repairer (duck-typing: .repair_step(html, task, history, failing_step, vars)->dict|None)
    - Перепланирование (если нужно) отдаётся наверх через on_replan(plan, history)->List[Dict]|None

    ДОПОЛНИТЕЛЬНО (расширение, не ломающее контракты):
    - run_incremental(ai, task, ...) — Plan-and-Execute цикл с подцелями (outline -> subgoal steps -> verify/fix).
      Внутри использует стандартный run(), не меняя его контракт и поведение.
    """

    def __init__(
        self,
        driver: WebDriver,
        settings: Settings,
        artifacts: Artifacts,
        trace: JsonlTrace,
        run_id: str,
        *,
        var_store: Optional[Any] = None,
        repairer: Optional[Any] = None,
        on_replan: Optional[Callable[[List[Dict[str, Any]], List[Dict[str, Any]]], Optional[List[Dict[str, Any]]]]] = None,
    ) -> None:
        self.d = driver
        self.s = settings
        self.art = artifacts
        self.trace = trace
        self.run_id = run_id

        self.var_store = var_store
        self.varr = _VarRenderer(var_store)
        self.repairer = repairer
        self.on_replan = on_replan

        self.hum = Humanizer(driver=self.d, cfg=self.s.humanize)
        self.guards = Guards(driver=self.d, guards_cfg=self.s.guards, browser_cfg=self.s.browser)

        self.ctx = ActionContext(
            driver=self.d,
            default_wait_sec=self.s.browser.default_wait_sec,
            step_timeout_sec=self.s.browser.step_timeout_sec,
            var_store=self.var_store,
        )

        self.history_done: List[Dict[str, Any]] = []
        self.plan: List[Dict[str, Any]] = []
        self.task: str = ""
        self.step_idx: int = 0
        self.stats = ExecStats()

    # ---- Вспомогательные методы ---------------------------------------------

    def _dom_view(self) -> str:
        html = self.guards.dom_snapshot()
        return safe_str(html)[: int(self.s.browser.max_dom_chars)]

    def _trace_step_result(self, ok: bool, err: Optional[str], step: Dict[str, Any], tsec: float, nested: bool) -> None:
        rec: Dict[str, Any] = {"event": "step_result", "ok": ok, "err": err, "t": round(tsec, 3), "step": step}
        if not nested:
            label = ("after_" if ok else "fail_") + str(step.get("type"))
            rec["screenshot"] = str(take_screenshot(self.d, self.art, label))
            snap_path = save_html_snapshot(self._dom_view(), self.art)
            rec["dom_snap"] = str(snap_path)
        self.trace.write(rec)

    def _execute_input_humanized(self, step: Dict[str, Any]) -> bool:
        sel = step.get("selector", "")
        text = str(step.get("text", ""))
        el = find(self.d, sel, visible=False, timeout_sec=self.s.browser.default_wait_sec)
        if not el:
            return False
        try:
            el.clear()
        except Exception:
            pass
        self.hum.type_text(el, text)
        return True

    def _execute_hover_humanized(self, step: Dict[str, Any]) -> bool:
        el = find(self.d, step.get("selector", ""), visible=True, timeout_sec=self.s.browser.default_wait_sec)
        if not el:
            return False
        self.hum.hover(el)
        return True

    def _execute_scroll_humanized(self, step: Dict[str, Any]) -> bool:
        direction = str(step.get("direction", "down")).lower()
        amount = int(step.get("amount", 600))
        dy = amount if direction == "down" else -amount
        self.hum.smooth_scroll_by(dy)
        return True

    def _execute_step(self, step: Dict[str, Any], *, nested: bool = False) -> bool:
        # Переменные в полях шага
        act = self.varr.render(step)
        tname = str(act.get("type")).lower()

        started = time.time()
        ok = False
        err: Optional[str] = None

        try:
            st = StepType(tname)
        except Exception:
            self._trace_step_result(False, f"unknown_step_type:{tname}", act, time.time() - started, nested)
            return False

        try:
            # Переключаем на humanized для части действий
            if self.s.humanize.enabled and st == StepType.INPUT:
                ok = self._execute_input_humanized(act)
            elif self.s.humanize.enabled and st == StepType.HOVER:
                ok = self._execute_hover_humanized(act)
            elif self.s.humanize.enabled and st == StepType.SCROLL:
                ok = self._execute_scroll_humanized(act)
            else:
                handler = ACTIONS.get(st)
                if not handler:
                    err = f"no_handler:{st.value}"
                else:
                    ok = bool(handler(self.ctx, act))
                    if st == StepType.WAIT_DOM_STABLE and ok:
                        self.stats.dom_stable_waits += 1
        except AssertionError as e:
            ok, err = False, f"assertion:{e}"
        except Exception as e:
            ok, err = False, f"exception:{e}"

        self._trace_step_result(ok, err, act, time.time() - started, nested)
        return ok

    def _captcha_guard(self) -> None:
        try:
            if self.guards.detect_captcha():
                shot = str(take_screenshot(self.d, self.art, "captcha_detected"))
                self.trace.write({"event": "captcha_detected", "screenshot": shot})
        except Exception:
            pass

    def _maybe_loop_guard(self) -> bool:
        tripped = self.guards.loop_guard_update(self.history_done[-self.s.guards.loop_dom_hash_window :])
        if tripped:
            self.stats.loops_guard_trips += 1
            self.trace.write({"event": "loop_guard_tripped"})
        return tripped

    def _validate_or_none(self, step: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not step:
            return None
        try:
            return validate_step(step)
        except Exception:
            return None

    def _known_vars_for_prompt(self) -> Dict[str, Any]:
        vs = getattr(self.var_store, "vars", None)
        if isinstance(vs, dict):
            return vs
        getter = getattr(self.var_store, "get", None)
        if callable(getter):
            # не знаем всех ключей — возвращаем пусто (repairer может работать и без vars)
            return {}
        return {}

    # ---- Публичные методы -----------------------------------------------------

    def set_plan(self, plan: List[Dict[str, Any]], task: str) -> None:
        """Перед запуском: валидируем и сохраняем план/задачу."""
        self.plan = validate_plan(plan)
        self.task = task or ""
        self.history_done.clear()
        self.step_idx = 0
        self.stats = ExecStats()

    def run(self) -> RunResult:
        """Выполняем ранее заданный план (через set_plan)."""
        if not self.plan:
            self.trace.write({"event": "run_empty_plan"})
            return RunResult(done_steps=[], planned_total=0, stats=self.stats)

        self.trace.write({"event": "run_start", "planned_total": len(self.plan), "task": self.task})

        same_step_counter = 0
        last_step_sig: Optional[str] = None
        consecutive_repairs = 0
        consecutive_skips = 0
        replan_suggested = False

        # Начальный ensure_ready_state для стабильности
        try:
            ensure_ready_state(self.d, timeout=10.0)
        except Exception:
            pass

        while self.step_idx < len(self.plan):
            # Лимит шагов
            if self.stats.total_steps >= self.s.limits.max_steps_per_task:
                self.trace.write({"event": "limit_reached", "limit": self.s.limits.max_steps_per_task})
                break

            step = self.plan[self.step_idx]
            step_sig = json.dumps(step, ensure_ascii=False, sort_keys=True)

            if step_sig == last_step_sig:
                same_step_counter += 1
            else:
                same_step_counter = 0
            last_step_sig = step_sig

            if same_step_counter >= self.s.limits.max_same_step:
                self.trace.write({"event": "same_step_break", "count": same_step_counter})
                break

            self.trace.write({"event": "step_start", "idx": self.step_idx, "step": step})
            ok = self._execute_step(step)
            self.stats.total_steps += 1
            self._captcha_guard()

            if ok:
                self.stats.ok_steps += 1
                self.history_done.append(step)
                self.step_idx += 1
                consecutive_repairs = 0
                consecutive_skips = 0

                # proactive repair: проверяем следующий шаг
                if self.repairer and self.step_idx < len(self.plan):
                    nxt = self.plan[self.step_idx]
                    nxt_sel = nxt.get("selector") or ""
                    nxt_type = (nxt.get("type") or "").lower()
                    needs_selector = nxt_type in {
                        StepType.CLICK.value, StepType.INPUT.value, StepType.SCROLL_TO_ELEMENT.value,
                        StepType.CHECK.value, StepType.HOVER.value, StepType.SELECT.value
                    }
                    if needs_selector and nxt_sel:
                        vis = nxt_type in {StepType.CLICK.value, StepType.HOVER.value}
                        if not exists(self.d, self.varr.render(nxt_sel), visible=vis, timeout_sec=2):
                            self.trace.write({"event": "next_step_looks_broken", "next_idx": self.step_idx, "step": nxt})
                            repaired = self._validate_or_none(
                                self.repairer.repair_step(
                                    self._dom_view(), self.task, self.history_done, nxt, self._known_vars_for_prompt()
                                )
                            )
                            if repaired:
                                self.plan[self.step_idx] = repaired
                                self.trace.write({"event": "repair_applied_proactive", "idx": self.step_idx, "new": repaired})
                            else:
                                # пропускаем этот следующий шаг как мусорный
                                shot = str(take_screenshot(self.d, self.art, "skip_proactive"))
                                self.trace.write({"event": "step_skip", "idx": self.step_idx, "reason": "proactive_repair_failed", "screenshot": shot})
                                self.step_idx += 1
                                self.stats.skips += 1
                                consecutive_skips += 1

                # loop guard + потенциальный replan
                if self._maybe_loop_guard():
                    if self.on_replan:
                        tail = self.on_replan(self.plan, self.history_done)
                        if tail:
                            # валидация хвоста
                            tail_valid = validate_plan(tail)
                            self.plan = self.history_done + tail_valid
                            self.step_idx = len(self.history_done)
                            self.stats.replans += 1
                            self.trace.write({"event": "replan_applied", "new_tail": len(tail_valid)})
                        else:
                            replan_suggested = True
                            self.trace.write({"event": "replan_suggested_noop"})
                    else:
                        replan_suggested = True
                        self.trace.write({"event": "replan_suggested"})
                continue

            # --- Ремонт текущего шага ---
            repaired_success = False
            if self.repairer:
                self.stats.repairs += 1
                repairs = 0
                backoff = 0.4
                while repairs < self.s.limits.max_repairs_per_step and not repaired_success:
                    repairs += 1
                    self.trace.write({"event": "repair_try", "idx": self.step_idx, "nth": repairs})
                    repaired = self._validate_or_none(
                        self.repairer.repair_step(
                            self._dom_view(), self.task, self.history_done, step, self._known_vars_for_prompt()
                        )
                    )
                    if repaired:
                        self.trace.write({"event": "repair_candidate", "idx": self.step_idx, "new": repaired})
                        if self._execute_step(repaired):
                            self.history_done.append(repaired)
                            self.plan[self.step_idx] = repaired
                            self.step_idx += 1
                            repaired_success = True
                            consecutive_repairs += 1
                            consecutive_skips = 0
                            break
                    time.sleep(backoff)
                    backoff = min(backoff * 1.8, 3.0)

            if not repaired_success:
                shot = str(take_screenshot(self.d, self.art, "skip_step"))
                self.trace.write({"event": "step_skip", "idx": self.step_idx, "step": step, "screenshot": shot})
                self.step_idx += 1
                self.stats.skips += 1
                consecutive_skips += 1
                consecutive_repairs = 0

            # триггеры переплана по сериям ремонтов/скипов
            if consecutive_repairs >= self.s.limits.replan_after_repairs or consecutive_skips >= self.s.limits.replan_after_skips:
                if self.on_replan:
                    tail = self.on_replan(self.plan, self.history_done)
                    if tail:
                        tail_valid = validate_plan(tail)
                        self.plan = self.history_done + tail_valid
                        self.step_idx = len(self.history_done)
                        self.stats.replans += 1
                        self.trace.write({"event": "replan_applied", "new_tail": len(tail_valid)})
                        consecutive_repairs = 0
                        consecutive_skips = 0
                    else:
                        replan_suggested = True
                        self.trace.write({"event": "replan_suggested_noop"})
                else:
                    replan_suggested = True
                    self.trace.write({"event": "replan_suggested"})

        self.trace.write({
            "event": "run_done",
            "stats": self.stats.__dict__,
            "done_count": len(self.history_done),
            "planned_total": len(self.plan),
            "replan_suggested": replan_suggested,
        })
        return RunResult(
            done_steps=list(self.history_done),
            planned_total=len(self.plan),
            stats=self.stats,
            replan_suggested=replan_suggested,
        )

    # ---- Инкрементальное выполнение (Plan-and-Execute) -----------------------

    def run_incremental(
        self,
        ai: Any,
        task: str,
        *,
        max_steps_per_subgoal: int = 6,
        verify_rounds: int = 1,
    ) -> RunResult:
        """
        Инкрементальный режим:
          1) outline подцелей (если доступно ai.plan_outline),
          2) для каждой подцели → сгенерировать короткий план по текущему DOM (ai.plan_subgoal_steps),
          3) исполнить стандартным run(),
          4) проверить достижение (ai.verify_or_adjust), при необходимости доиграть fix_steps.

        Безопасность:
          - Если у ai нет нужных методов, делаем fallback на единый plan_full + run().
          - Любые ответы LLM дополнительно валидируются validate_plan.
          - Статистика и история аккуратно агрегируются между подцелями.

        Возвращает агрегированный RunResult по всем подцелям.
        """
        # Запишем старт события
        self.trace.write({"event": "incremental_start", "task": task})

        # Агрегаторы
        agg_done: List[Dict[str, Any]] = []
        agg_stats = ExecStats()
        agg_planned_total = 0
        any_replan_suggested = False

        # Вспомогательный агрегатор статистики
        def _accumulate(stat: ExecStats, add: ExecStats) -> None:
            stat.total_steps += add.total_steps
            stat.ok_steps += add.ok_steps
            stat.repairs += add.repairs
            stat.skips += add.skips
            stat.replans += add.replans
            stat.dom_stable_waits += add.dom_stable_waits
            stat.loops_guard_trips += add.loops_guard_trips

        # Если у клиента нет новых API — fallback на полный план
        has_outline = hasattr(ai, "plan_outline")
        has_substeps = hasattr(ai, "plan_subgoal_steps")
        has_verify = hasattr(ai, "verify_or_adjust")
        if not (has_outline and has_substeps):
            # Пишем замечание и переходим к старой схеме
            self.trace.write({"event": "incremental_fallback_plan_full"})
            steps = []
            try:
                steps = ai.plan_full(self._dom_view(), task, [], self._known_vars_for_prompt()) or []
            except Exception as e:
                self.trace.write({"event": "llm_error", "where": "plan_full", "err": repr(e)})
            steps_valid = validate_plan(steps)
            self.set_plan(steps_valid, task)
            res = self.run()
            agg_done.extend(res.done_steps)
            _accumulate(agg_stats, res.stats)
            agg_planned_total += len(steps_valid)
            any_replan_suggested = any_replan_suggested or res.replan_suggested

            self.trace.write({
                "event": "incremental_done",
                "fallback": True,
                "stats": agg_stats.__dict__,
                "done_count": len(agg_done),
                "planned_total": agg_planned_total,
                "replan_suggested": any_replan_suggested,
            })
            return RunResult(done_steps=agg_done, planned_total=agg_planned_total, stats=agg_stats, replan_suggested=any_replan_suggested)

        # 1) Outline подцелей
        outline = {"subgoals": []}
        try:
            outline = ai.plan_outline(task) or {"subgoals": []}
        except Exception as e:
            self.trace.write({"event": "llm_error", "where": "plan_outline", "err": repr(e)})

        subgoals: List[Dict[str, Any]] = outline.get("subgoals", []) if isinstance(outline, dict) else []
        self.trace.write({"event": "outline", "count": len(subgoals), "subgoals": subgoals[:6]})  # не засоряем трейс

        # Если outline пуст — fallback на единый план
        if not subgoals:
            self.trace.write({"event": "outline_empty_fallback_plan_full"})
            steps = []
            try:
                steps = ai.plan_full(self._dom_view(), task, [], self._known_vars_for_prompt()) or []
            except Exception as e:
                self.trace.write({"event": "llm_error", "where": "plan_full", "err": repr(e)})
            steps_valid = validate_plan(steps)
            self.set_plan(steps_valid, task)
            res = self.run()
            agg_done.extend(res.done_steps)
            _accumulate(agg_stats, res.stats)
            agg_planned_total += len(steps_valid)
            any_replan_suggested = any_replan_suggested or res.replan_suggested

            self.trace.write({
                "event": "incremental_done",
                "fallback": True,
                "stats": agg_stats.__dict__,
                "done_count": len(agg_done),
                "planned_total": agg_planned_total,
                "replan_suggested": any_replan_suggested,
            })
            return RunResult(done_steps=agg_done, planned_total=agg_planned_total, stats=agg_stats, replan_suggested=any_replan_suggested)

        # 2) Проходим по подцелям
        for idx, sg in enumerate(subgoals, start=1):
            sg_title = str(sg.get("title") or sg.get("goal") or f"Subgoal {idx}")
            self.trace.write({"event": "subgoal_start", "idx": idx, "title": sg_title, "sg": sg})

            # Сгенерируем короткий список шагов для подцели
            steps_for_sg: List[Dict[str, Any]] = []
            try:
                steps_for_sg = ai.plan_subgoal_steps(
                    self._dom_view(),
                    task,
                    sg,
                    agg_done,  # Важно: HISTORY_DONE = уже выполненные ранее
                    self._known_vars_for_prompt(),
                    max_steps=max_steps_per_subgoal,
                ) or []
            except Exception as e:
                self.trace.write({"event": "llm_error", "where": "plan_subgoal_steps", "err": repr(e), "sg": sg_title})

            steps_valid = validate_plan(steps_for_sg)
            self.trace.write({"event": "subgoal_plan", "idx": idx, "title": sg_title, "count": len(steps_valid)})

            # Если план пуст — попробуем верификацию (вдруг подцель уже выполнена)
            if not steps_valid and has_verify:
                try:
                    vr = ai.verify_or_adjust(self._dom_view(), task, sg, [], self._known_vars_for_prompt()) or {}
                except Exception as e:
                    vr = {}
                    self.trace.write({"event": "llm_error", "where": "verify_or_adjust", "err": repr(e), "sg": sg_title})
                self.trace.write({"event": "verify_result", "idx": idx, "title": sg_title, "result": vr})
                if isinstance(vr, dict) and vr.get("status") == "retry":
                    fix = validate_plan(vr.get("fix_steps") or [])
                    if fix:
                        self.set_plan(fix, f"{task} :: {sg_title} (fix)")
                        res_fix = self.run()
                        agg_done.extend(res_fix.done_steps)
                        _accumulate(agg_stats, res_fix.stats)
                        agg_planned_total += len(fix)
                        any_replan_suggested = any_replan_suggested or res_fix.replan_suggested
                # завершаем эту подцель и идем дальше
                self.trace.write({"event": "subgoal_done", "idx": idx, "title": sg_title})
                continue

            # Исполняем основной набор шагов для подцели
            self.set_plan(steps_valid, f"{task} :: {sg_title}")
            res = self.run()
            agg_done.extend(res.done_steps)
            _accumulate(agg_stats, res.stats)
            agg_planned_total += len(steps_valid)
            any_replan_suggested = any_replan_suggested or res.replan_suggested

            # 3) Верификация/коррекция (опционально)
            if has_verify and verify_rounds > 0:
                try:
                    vr = ai.verify_or_adjust(self._dom_view(), task, sg, res.done_steps, self._known_vars_for_prompt()) or {}
                except Exception as e:
                    vr = {}
                    self.trace.write({"event": "llm_error", "where": "verify_or_adjust", "err": repr(e), "sg": sg_title})
                self.trace.write({"event": "verify_result", "idx": idx, "title": sg_title, "result": vr})

                rounds_left = int(verify_rounds)
                while rounds_left > 0 and isinstance(vr, dict) and vr.get("status") == "retry":
                    rounds_left -= 1
                    fix_steps = validate_plan(vr.get("fix_steps") or [])
                    if not fix_steps:
                        break
                    self.set_plan(fix_steps, f"{task} :: {sg_title} (fix)")
                    res_fix = self.run()
                    agg_done.extend(res_fix.done_steps)
                    _accumulate(agg_stats, res_fix.stats)
                    agg_planned_total += len(fix_steps)
                    any_replan_suggested = any_replan_suggested or res_fix.replan_suggested

                    # повторим проверку, если ещё остались раунды
                    try:
                        vr = ai.verify_or_adjust(self._dom_view(), task, sg, res_fix.done_steps, self._known_vars_for_prompt()) or {}
                    except Exception as e:
                        vr = {}
                        self.trace.write({"event": "llm_error", "where": "verify_or_adjust", "err": repr(e), "sg": sg_title})
                    self.trace.write({"event": "verify_result", "idx": idx, "title": sg_title, "result": vr})

            self.trace.write({"event": "subgoal_done", "idx": idx, "title": sg_title})

        # 4) Завершение инкрементального режима
        self.trace.write({
            "event": "incremental_done",
            "stats": agg_stats.__dict__,
            "done_count": len(agg_done),
            "planned_total": agg_planned_total,
            "replan_suggested": any_replan_suggested,
        })
        return RunResult(
            done_steps=agg_done,
            planned_total=agg_planned_total,
            stats=agg_stats,
            replan_suggested=any_replan_suggested,
        )
