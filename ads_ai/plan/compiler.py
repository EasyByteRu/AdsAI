# ads_ai/plan/compiler.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

from ads_ai.plan.schema import StepType, validate_step, validate_plan
from ads_ai.utils.json_tools import safe_str


__all__ = [
    "CompileContext",
    "CompileOptions",
    "CompileResult",
    "MacroRegistry",
    "PlanCompiler",
    "compile_plan",           # удобный шорткат
    "create_default_registry" # фабрика с базовыми макросами
]


# ----------------------------- модели результата -----------------------------

@dataclass
class CompileContext:
    """Контекст компиляции (то, что знаем на этапе подготовки плана)."""
    task: str = ""
    vars_map: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CompileOptions:
    """Опции компиляции."""
    strict: bool = False          # если True — любая ошибка валит компиляцию
    expand_macros: bool = True
    normalize_aliases: bool = True
    render_strings_with_vars: bool = False  # подставлять ${var} прямо в компиляторе (обычно False: рендерит рантайм)


@dataclass
class CompileResult:
    steps: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    def ok(self) -> bool:
        return not self.errors


# --------------------------------- утилиты -----------------------------------

def _render_value(val: Any, vars_map: Dict[str, Any]) -> Any:
    """Лёгкий рендер ${var} и ${name:-fallback} для compile-time подстановок."""
    if not isinstance(val, str):
        if isinstance(val, dict):
            return {k: _render_value(v, vars_map) for k, v in val.items()}
        if isinstance(val, list):
            return [_render_value(v, vars_map) for v in val]
        return val

    import re
    pat = re.compile(r"\$\{([A-Za-z0-9_]+)(?::-(.*?))?\}")
    def repl(m: "re.Match[str]") -> str:
        key = m.group(1)
        fallback = m.group(2)
        got = vars_map.get(key, fallback if fallback is not None else "")
        return "" if got is None else str(got)
    return pat.sub(repl, val)


ALIASES: Dict[str, Tuple[str, Callable[[Dict[str, Any]], Dict[str, Any]] | None]] = {
    # type_alias : (canonical_type, optional_field_adapter)
    "sleep": ("wait", None),
    "open": ("goto", None),
    "ensure_url": ("wait_url", None),
    "dom_idle": ("wait_dom_stable", None),
    "assert_contains": ("assert_text", lambda s: {**s, "match": s.get("match", "contains"), "attr": s.get("attr", "text")}),
    "extract_text": ("extract", lambda s: {**s, "attr": "text"}),
    "press": ("press_key", None),
    "keys": ("hotkey", None),
}


# ------------------------------ Macro Registry -------------------------------

MacroFn = Callable[[Dict[str, Any], CompileContext], List[Dict[str, Any]]]


class MacroRegistry:
    def __init__(self) -> None:
        self._macros: Dict[str, MacroFn] = {}

    def register(self, name: str, fn: MacroFn) -> None:
        self._macros[str(name).lower()] = fn

    def has(self, name: str) -> bool:
        return str(name).lower() in self._macros

    def expand(self, name: str, node: Dict[str, Any], ctx: CompileContext) -> List[Dict[str, Any]]:
        fn = self._macros.get(str(name).lower())
        if not fn:
            raise KeyError(f"macro not found: {name}")
        return fn(node, ctx)


# ------------------------------- базовые макросы -----------------------------

def _m_group(node: Dict[str, Any], ctx: CompileContext) -> List[Dict[str, Any]]:
    """
    { "macro": "group", "steps": [ ... ] }
    Просто разворачивает список вложенных шагов/макросов как есть (рекурсия на верхнем уровне компиляции).
    """
    steps = node.get("steps") or []
    if not isinstance(steps, list):
        return []
    return steps


def _m_if_var(node: Dict[str, Any], ctx: CompileContext) -> List[Dict[str, Any]]:
    """
    { "macro":"if_var", "name":"flag", "equals": "yes", "steps":[...] }
    Условия:
      - equals: строгое сравнение с контекстной переменной
      - exists: True/False — проверка наличия в vars_map
      - truthy: True — приводим к bool
    Если условий нет, трактуем как truthy.
    """
    name = str(node.get("name") or "").strip()
    steps = node.get("steps") or []
    if not name or not isinstance(steps, list):
        return []

    val = ctx.vars_map.get(name, None)
    if "equals" in node:
        cond = (str(val) == str(node.get("equals")))
    elif "exists" in node:
        cond = bool((name in ctx.vars_map) == bool(node.get("exists")))
    else:
        cond = bool(val)

    return steps if cond else []


def _m_foreach(node: Dict[str, Any], ctx: CompileContext) -> List[Dict[str, Any]]:
    """
    { "macro":"foreach", "list":"items" | [...], "as":"item", "steps":[...] }
    Если list — строка, трактуем как имя переменной в ctx.vars_map. Иначе — используем массив как есть.
    Внутри steps доступна подстановка ${item} или ${item.field} (когда элемент — dict).
    """
    raw_list = node.get("list")
    alias = str(node.get("as") or "item").strip()
    steps = node.get("steps") or []
    if not isinstance(steps, list):
        return []

    # извлекаем список
    if isinstance(raw_list, str):
        seq = ctx.vars_map.get(raw_list, [])
    else:
        seq = raw_list
    if not isinstance(seq, list):
        return []

    out: List[Dict[str, Any]] = []
    for it in seq:
        # формируем локальный vars_map для итерации
        local_vars = dict(ctx.vars_map)
        if isinstance(it, dict):
            # ${item} -> json, ${item.foo} -> значение
            local_vars[alias] = it
            for k, v in it.items():
                local_vars[f"{alias}.{k}"] = v
        else:
            local_vars[alias] = it
        # рендерим шаги итерации compile-time (чтобы в рантайме не разруливать ${item})
        rendered = _render_value({"steps": steps}, local_vars).get("steps", [])
        out.extend(rendered if isinstance(rendered, list) else [])
    return out


def create_default_registry() -> MacroRegistry:
    reg = MacroRegistry()
    reg.register("group", _m_group)
    reg.register("if_var", _m_if_var)
    reg.register("foreach", _m_foreach)
    return reg


# --------------------------------- Compiler ----------------------------------

class PlanCompiler:
    def __init__(self, registry: Optional[MacroRegistry] = None, options: Optional[CompileOptions] = None) -> None:
        self.registry = registry or create_default_registry()
        self.options = options or CompileOptions()

    # -- публичный API --

    def compile(self, raw_plan: Any, ctx: CompileContext) -> CompileResult:
        res = CompileResult()
        items = self._as_list(raw_plan)
        if items is None:
            res.errors.append("План должен быть массивом шагов/макросов")
            return res

        # 1) нормализация / алиасы / макропроход
        flat: List[Dict[str, Any]] = []
        for idx, node in enumerate(items):
            try:
                flat.extend(self._expand_node(node, ctx))
            except Exception as e:
                msg = f"Ошибка макрорасширения на шаге {idx}: {safe_str(repr(e))[:180]}"
                if self.options.strict:
                    res.errors.append(msg)
                    return res
                res.warnings.append(msg)

        # 2) финальная валидация
        validated: List[Dict[str, Any]] = []
        for i, st in enumerate(flat):
            try:
                if self.options.render_strings_with_vars:
                    st = _render_value(st, ctx.vars_map)
                v = validate_step(self._normalize_alias(st) if self.options.normalize_aliases else st)
                validated.append(v)
            except Exception as e:
                msg = f"Шаг {i} отброшен валидатором: {safe_str(str(e))[:200]}"
                if self.options.strict:
                    res.errors.append(msg)
                    return res
                res.warnings.append(msg)

        res.steps = validated
        return res

    # -- внутренности --

    @staticmethod
    def _as_list(x: Any) -> Optional[List[Any]]:
        if isinstance(x, list):
            return x
        return None

    def _normalize_alias(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Меняем алиасы типа на канонические шаги + адаптируем поля."""
        t = str(step.get("type") or step.get("macro") or "").lower()
        if not t:
            return step
        if t in ALIASES:
            canon, adapter = ALIASES[t]
            s = dict(step)
            s["type"] = canon
            s.pop("macro", None)
            if adapter:
                try:
                    s = adapter(s)  # подправляем поля (например, attr/match)
                except Exception:
                    pass
            return s
        # если в ноде не type, а macro — пусть останется как macro (до расширения)
        return step

    def _expand_node(self, node: Any, ctx: CompileContext) -> List[Dict[str, Any]]:
        """Возвращает плоский список шагов (макросы развёрнуты)."""
        if not isinstance(node, dict):
            return []
        # 1) алиасы сразу нормализуем (может превратить macro->type)
        node = self._normalize_alias(node)

        # 2) если это макрос — разворачиваем
        mname = str(node.get("macro") or "").lower()
        if mname:
            if not self.options.expand_macros:
                # Оставим как есть — но валидатор позже всё равно отфутболит;
                # логичнее здесь игнорировать, а не класть в финальный план.
                return []
            if not self.registry.has(mname):
                # неизвестный макрос — варним и выкидываем
                raise KeyError(f"unknown macro: {mname}")
            expanded = self.registry.expand(mname, node, ctx) or []
            flat: List[Dict[str, Any]] = []
            for ch in expanded:
                flat.extend(self._expand_node(ch, ctx))  # рекурсия (вложенные макросы)
            return flat

        # 3) это обычный шаг: потенциально подрендерим строки на compile-time (если опция включена)
        if self.options.render_strings_with_vars:
            node = _render_value(node, ctx.vars_map)

        # 4) отдаём как единичный шаг
        return [node]


# -------------------------------- шорткаты -----------------------------------

def compile_plan(
    raw_plan: Any,
    *,
    ctx: Optional[CompileContext] = None,
    registry: Optional[MacroRegistry] = None,
    options: Optional[CompileOptions] = None,
) -> CompileResult:
    """Удобный вызов без ручного конструирования компилятора."""
    compiler = PlanCompiler(registry=registry, options=options)
    return compiler.compile(raw_plan, ctx or CompileContext())
