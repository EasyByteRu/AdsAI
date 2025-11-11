# ads_ai/plugins/registry.py
from __future__ import annotations

import importlib
import inspect
from dataclasses import dataclass, field
from types import ModuleType
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from ads_ai.tracing.trace import JsonlTrace
from ads_ai.utils.json_tools import safe_str

# шаги и экшены
from ads_ai.plan.schema import StepType
from ads_ai.browser.actions import ActionContext  # тип контекста рантайма

# макросы компилятора
from ads_ai.plan.compiler import MacroRegistry, create_default_registry


# Тип хендлера шага
StepHandler = Callable[[ActionContext, Dict[str, Any]], bool]
# Обёртка поверх хендлера (например, для трейсинга/ретраев/метрик)
StepWrapper = Callable[[StepHandler], StepHandler]
# Хуки
BeforeHook = Callable[[ActionContext, StepType, Dict[str, Any]], None]
AfterHook = Callable[[ActionContext, StepType, Dict[str, Any], bool], None]
ErrorHook = Callable[[ActionContext, StepType, Dict[str, Any], BaseException], None]
# Макрос
MacroFn = Callable[[Dict[str, Any], Any], List[Dict[str, Any]]]


@dataclass
class _StepPatches:
    overrides: Dict[StepType, StepHandler] = field(default_factory=dict)
    wrappers_specific: Dict[StepType, List[StepWrapper]] = field(default_factory=dict)
    wrappers_global: List[StepWrapper] = field(default_factory=list)


@dataclass
class _Hooks:
    before: List[BeforeHook] = field(default_factory=list)
    after: List[AfterHook] = field(default_factory=list)
    on_error: List[ErrorHook] = field(default_factory=list)


@dataclass
class _Macros:
    by_name: Dict[str, MacroFn] = field(default_factory=dict)


@dataclass
class LoadedPlugin:
    name: str
    module: ModuleType
    obj: Any | None = None  # инстанс/функция, если есть
    registered: bool = False


class PluginRegistry:
    """
    Реестр плагинов/хуков/макросов и слой патчинга ACTIONS.
    Идемпотентный: повторные apply_actions_patch не создают «матрёшку» обёрток.
    """

    def __init__(self, *, trace: Optional[JsonlTrace] = None, name: str = "default") -> None:
        self.name = name
        self.trace = trace
        self._patches = _StepPatches()
        self._hooks = _Hooks()
        self._macros = _Macros()
        self._loaded: Dict[str, LoadedPlugin] = {}
        self._original_actions: Dict[Any, StepHandler] = {}
        self._installed = False

    # ------------------------------------------------------------------ #
    #                        STEP HANDLERS / WRAPPERS                    #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _to_step_type(key: Union[str, StepType]) -> StepType:
        if isinstance(key, StepType):
            return key
        try:
            return StepType(str(key).lower())
        except Exception as e:
            raise ValueError(f"unknown step type: {key}") from e

    def register_step_override(self, step: Union[str, StepType], handler: StepHandler) -> None:
        st = self._to_step_type(step)
        self._patches.overrides[st] = handler
        if self.trace:
            self.trace.write({"event": "plugin_step_override", "step": st.value, "handler": getattr(handler, "__name__", "lambda")})

    def add_step_wrapper(self, wrapper: StepWrapper, step: Union[str, StepType, None] = None) -> None:
        """Если step=None — глобальный враппер для всех шагов."""
        if step is None:
            self._patches.wrappers_global.append(wrapper)
            tag = "*"
        else:
            st = self._to_step_type(step)
            self._patches.wrappers_specific.setdefault(st, []).append(wrapper)
            tag = st.value
        if self.trace:
            self.trace.write({"event": "plugin_step_wrapper", "scope": tag, "wrapper": getattr(wrapper, "__name__", "lambda")})

    def add_before_step_hook(self, fn: BeforeHook) -> None:
        self._hooks.before.append(fn)
        if self.trace:
            self.trace.write({"event": "plugin_hook_added", "hook": "before", "fn": getattr(fn, "__name__", "lambda")})

    def add_after_step_hook(self, fn: AfterHook) -> None:
        self._hooks.after.append(fn)
        if self.trace:
            self.trace.write({"event": "plugin_hook_added", "hook": "after", "fn": getattr(fn, "__name__", "lambda")})

    def add_error_hook(self, fn: ErrorHook) -> None:
        self._hooks.on_error.append(fn)
        if self.trace:
            self.trace.write({"event": "plugin_hook_added", "hook": "error", "fn": getattr(fn, "__name__", "lambda")})

    # ------------------------------------------------------------------ #
    #                               MACROS                               #
    # ------------------------------------------------------------------ #

    def register_macro(self, name: str, fn: MacroFn) -> None:
        k = str(name).strip().lower()
        if not k:
            raise ValueError("macro name is empty")
        self._macros.by_name[k] = fn
        if self.trace:
            self.trace.write({"event": "plugin_macro_registered", "name": k})

    def make_macro_registry(self, base: Optional[MacroRegistry] = None) -> MacroRegistry:
        reg = base or create_default_registry()
        for k, fn in self._macros.by_name.items():
            reg.register(k, fn)
        return reg

    # ------------------------------------------------------------------ #
    #                            ACTIONS PATCH                           #
    # ------------------------------------------------------------------ #

    def _wrap_with_hooks(self, st: StepType, base: StepHandler) -> StepHandler:
        hooks = self._hooks
        def wrapped(ctx: ActionContext, step: Dict[str, Any]) -> bool:
            # before
            for fn in hooks.before:
                try:
                    fn(ctx, st, step)
                except Exception as e:
                    if self.trace:
                        self.trace.write({"event": "hook_before_error", "step": st.value, "err": safe_str(repr(e))[:200]})
            # exec
            ok: bool = False
            try:
                ok = bool(base(ctx, step))
            except BaseException as e:
                # error hook
                for eh in hooks.on_error:
                    try:
                        eh(ctx, st, step, e)
                    except Exception as ee:
                        if self.trace:
                            self.trace.write({"event": "hook_error_error", "step": st.value, "err": safe_str(repr(ee))[:200]})
                raise
            finally:
                # after
                for fn in hooks.after:
                    try:
                        fn(ctx, st, step, ok)
                    except Exception as e:
                        if self.trace:
                            self.trace.write({"event": "hook_after_error", "step": st.value, "err": safe_str(repr(e))[:200]})
            return ok
        return wrapped

    def _apply_wrappers(self, st: StepType, base: StepHandler) -> StepHandler:
        # порядок: конкретные → глобальные, как регистрировали (FIFO)
        for w in self._patches.wrappers_specific.get(st, []):
            try:
                base = w(base)
            except Exception as e:
                if self.trace:
                    self.trace.write({"event": "wrapper_specific_error", "step": st.value, "err": safe_str(repr(e))[:200]})
        for w in self._patches.wrappers_global:
            try:
                base = w(base)
            except Exception as e:
                if self.trace:
                    self.trace.write({"event": "wrapper_global_error", "step": st.value, "err": safe_str(repr(e))[:200]})
        return base

    def _compose_handler(self, st: StepType, original: StepHandler) -> StepHandler:
        base = self._patches.overrides.get(st, original)
        base = self._apply_wrappers(st, base)
        return self._wrap_with_hooks(st, base)

    def apply_actions_patch(self, actions: Dict[Any, StepHandler]) -> Dict[Any, StepHandler]:
        """
        Оборачивает/заменяет хендлеры в ACTIONS in-place (и возвращает его для удобства).
        Идемпотентно: повторные вызовы не накапливают обёртки.
        """
        # восстановим оригинал при повторном патче
        if self._installed and self._original_actions:
            for k, v in self._original_actions.items():
                actions[k] = v
        # запомним текущие как оригинальные
        self._original_actions = dict(actions)

        for k, orig in list(actions.items()):
            try:
                # ключи ACTIONS обычно StepType; если вдруг строка — приведём
                st = self._to_step_type(k if isinstance(k, StepType) else str(getattr(k, "value", k)))
            except Exception:
                # пропускаем экзотические ключи
                continue
            try:
                actions[k] = self._compose_handler(st, orig)
            except Exception as e:
                if self.trace:
                    self.trace.write({"event": "actions_patch_error", "step": getattr(st, "value", str(k)), "err": safe_str(repr(e))[:200]})

        self._installed = True
        if self.trace:
            self.trace.write({"event": "actions_patched", "count": len(actions)})
        return actions

    # ------------------------------------------------------------------ #
    #                          PLUGINS LOADING                           #
    # ------------------------------------------------------------------ #

    def load_plugins(self, modules: List[str]) -> None:
        """
        Загружает плагины по путям вида:
          - "pkg.mod" — ищет в модуле: callable `setup(registry)` или переменную `plugin`
          - "pkg.mod:setup" — вызывает указанную функцию (setup(registry))
          - "pkg.mod:PluginClass" — инстанцирует без аргументов и ищет .setup(self, registry) или .register(registry)
        """
        for spec in modules or []:
            try:
                mod_name, obj_name = self._split_spec(spec)
                mod = importlib.import_module(mod_name)
                loaded = LoadedPlugin(name=spec, module=mod)
                self._loaded[spec] = loaded

                obj = None
                if obj_name:
                    obj = getattr(mod, obj_name)
                else:
                    # авто-поиск
                    obj = getattr(mod, "setup", None) or getattr(mod, "register", None) or getattr(mod, "plugin", None)

                if callable(obj):
                    # функция setup(registry) | register(registry)
                    obj(self)
                    loaded.obj = obj
                    loaded.registered = True
                    self._log_loaded(spec, "callable")
                    continue

                if inspect.isclass(obj):
                    inst = obj()  # type: ignore[call-arg]
                    loaded.obj = inst
                    # метод setup|register
                    if hasattr(inst, "setup") and callable(inst.setup):  # type: ignore[attr-defined]
                        inst.setup(self)  # type: ignore[arg-type]
                        loaded.registered = True
                    elif hasattr(inst, "register") and callable(inst.register):  # type: ignore[attr-defined]
                        inst.register(self)  # type: ignore[arg-type]
                        loaded.registered = True
                    self._log_loaded(spec, "class")
                    continue

                # если obj не найден — попытка plugin.setup
                plug = getattr(mod, "plugin", None)
                if plug is not None:
                    if callable(plug):
                        plug(self)  # plugin(registry)
                        loaded.obj = plug
                        loaded.registered = True
                        self._log_loaded(spec, "plugin_callable")
                    elif hasattr(plug, "setup") and callable(plug.setup):  # type: ignore[attr-defined]
                        plug.setup(self)  # type: ignore[arg-type]
                        loaded.obj = plug
                        loaded.registered = True
                        self._log_loaded(spec, "plugin_object")
                    continue

                # ничего подходящего не нашли — просто отметим модуль
                self._log_loaded(spec, "module_only")
            except Exception as e:
                if self.trace:
                    self.trace.write({"event": "plugin_load_error", "spec": spec, "err": safe_str(repr(e))[:300]})

    @staticmethod
    def _split_spec(spec: str) -> Tuple[str, Optional[str]]:
        if ":" in spec:
            mod, obj = spec.split(":", 1)
            return mod.strip(), obj.strip() or None
        return spec.strip(), None

    def _log_loaded(self, spec: str, kind: str) -> None:
        if self.trace:
            self.trace.write({"event": "plugin_loaded", "spec": spec, "kind": kind})

    # ------------------------------------------------------------------ #
    #                           INTROSPECTION                            #
    # ------------------------------------------------------------------ #

    def list_plugins(self) -> List[str]:
        return list(self._loaded.keys())

    def list_macros(self) -> List[str]:
        return list(self._macros.by_name.keys())

    def list_overrides(self) -> List[str]:
        return [st.value for st in self._patches.overrides.keys()]

    def list_wrappers(self) -> Dict[str, int]:
        out: Dict[str, int] = {"*": len(self._patches.wrappers_global)}
        for st, ws in self._patches.wrappers_specific.items():
            out[st.value] = len(ws)
        return out
