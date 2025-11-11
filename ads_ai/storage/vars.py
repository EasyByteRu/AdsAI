# ads_ai/storage/vars.py
from __future__ import annotations

import json
import os
import re
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Mapping, MutableMapping, Optional


__all__ = ["VarStore", "NamespacedVarStore"]


def _ensure_parent_dir(path: Path) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


def _atomic_write_json(path: Path, payload: dict) -> None:
    """Пишем атомарно: tmp → rename."""
    _ensure_parent_dir(path)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _now_ts() -> float:
    return float(time.time())


class VarStore:
    """
    Персистентная память для переменных агента.
    Формат файла:
    {
      "_meta": {"version": 1, "created": 1710000000.0, "updated": 1710001111.0},
      "vars": { "last_url": "...", "token": "..." }
    }
    """

    VERSION = 1

    def __init__(self, path: os.PathLike | str, *, autosave: bool = True) -> None:
        self.path = Path(path)
        self.autosave = bool(autosave)
        self._lock = threading.RLock()
        self._meta: Dict[str, Any] = {"version": self.VERSION, "created": _now_ts(), "updated": _now_ts()}
        self._vars: Dict[str, Any] = {}
        self._dirty = False
        self._load_if_exists()

    # -------------------------- базовое API --------------------------

    @property
    def vars(self) -> Dict[str, Any]:
        """Живой словарь (копию наружу не отдаём, но пригодится VarRenderer через getattr(...,'vars'))."""
        return self._vars

    def get(self, key: str, default: Any = "") -> Any:
        with self._lock:
            return self._vars.get(key, default)

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._vars[key] = value
            self._touch()
            self._maybe_save()

    def has(self, key: str) -> bool:
        with self._lock:
            return key in self._vars

    def pop(self, key: str, default: Any = None) -> Any:
        with self._lock:
            val = self._vars.pop(key, default)
            self._touch()
            self._maybe_save()
            return val

    def update(self, mapping: Mapping[str, Any] | None = None, **kwargs: Any) -> None:
        if not mapping and not kwargs:
            return
        with self._lock:
            if mapping:
                for k, v in mapping.items():
                    self._vars[k] = v
            if kwargs:
                for k, v in kwargs.items():
                    self._vars[k] = v
            self._touch()
            self._maybe_save()

    def clear(self) -> None:
        with self._lock:
            self._vars.clear()
            self._touch()
            self._maybe_save()

    def save(self) -> None:
        with self._lock:
            payload = {"_meta": dict(self._meta), "vars": self._vars}
            _atomic_write_json(self.path, payload)
            self._dirty = False

    def load(self) -> None:
        with self._lock:
            self._load_if_exists()

    # -------------------------- утилиты --------------------------

    def render(self, val: Any) -> Any:
        """
        Рендерит ${var} в строках. Для dict/list — рекурсивно.
        Поддержка дефолта: ${name:-fallback}
        """
        if isinstance(val, str):
            pat = re.compile(r"\$\{([A-Za-z0-9_]+)(?::-(.*?))?\}")
            def repl(m: re.Match[str]) -> str:
                key = m.group(1)
                fallback = m.group(2)
                got = self.get(key, fallback if fallback is not None else "")
                return "" if got is None else str(got)
            return pat.sub(repl, val)
        if isinstance(val, dict):
            return {k: self.render(v) for k, v in val.items()}
        if isinstance(val, list):
            return [self.render(v) for v in val]
        return val

    @contextmanager
    def batch(self) -> Iterator[None]:
        """
        Батч-режим: временно отключаем автосейв и сохраняем один раз в конце (если были изменения).
        Пример:
            with store.batch():
                store.set("a", 1)
                store.set("b", 2)
        """
        with self._lock:
            old = self.autosave
            self.autosave = False
        try:
            yield
        finally:
            with self._lock:
                self.autosave = old
                if self._dirty:
                    self.save()

    # -------------------------- внутренности --------------------------

    def _maybe_save(self) -> None:
        if self.autosave and self._dirty:
            self.save()

    def _touch(self) -> None:
        self._dirty = True
        self._meta["updated"] = _now_ts()

    def _load_if_exists(self) -> None:
        if not self.path.exists():
            _ensure_parent_dir(self.path)
            # первый сейв — чтобы создать файл и каталог аккуратно
            self.save()
            return
        try:
            with self.path.open("r", encoding="utf-8") as f:
                payload = json.load(f) or {}
            meta = payload.get("_meta") or {}
            data = payload.get("vars") or {}
            if not isinstance(data, dict):
                data = {}
            self._meta = {
                "version": int(meta.get("version", self.VERSION)),
                "created": float(meta.get("created", _now_ts())),
                "updated": float(meta.get("updated", _now_ts())),
            }
            self._vars = dict(data)
            self._dirty = False
        except Exception:
            # коррапт? — делаем бэкап и начинаем с чистого листа
            try:
                bak = self.path.with_suffix(self.path.suffix + ".corrupt")
                os.replace(self.path, bak)
            except Exception:
                pass
            self._vars = {}
            self._meta = {"version": self.VERSION, "created": _now_ts(), "updated": _now_ts()}
            self._dirty = True
            self._maybe_save()


class NamespacedVarStore:
    """
    Неймспейсы поверх VarStore: ключи префиксуются 'ns.'.
    Удобно разделять переменные разных сценариев.
    """
    def __init__(self, base: VarStore, namespace: str) -> None:
        self.base = base
        self.ns = str(namespace).strip().strip(".")
        self.prefix = f"{self.ns}." if self.ns else ""

    @property
    def vars(self) -> Dict[str, Any]:
        # Представляем только свой скоуп
        return {k[len(self.prefix):]: v for k, v in self.base.vars.items() if k.startswith(self.prefix)}

    def _fq(self, key: str) -> str:
        return f"{self.prefix}{key}"

    def get(self, key: str, default: Any = "") -> Any:
        return self.base.get(self._fq(key), default)

    def set(self, key: str, value: Any) -> None:
        self.base.set(self._fq(key), value)

    def pop(self, key: str, default: Any = None) -> Any:
        return self.base.pop(self._fq(key), default)

    def has(self, key: str) -> bool:
        return self.base.has(self._fq(key))

    def update(self, mapping: Mapping[str, Any] | None = None, **kwargs: Any) -> None:
        if mapping:
            mapping = {self._fq(k): v for k, v in mapping.items()}
            self.base.update(mapping)
        if kwargs:
            kwargs = {self._fq(k): v for k, v in kwargs.items()}
            self.base.update(kwargs)

    def clear(self) -> None:
        # чистим только свой неймспейс
        for k in list(self.base.vars.keys()):
            if k.startswith(self.prefix):
                self.base.pop(k, None)

    def render(self, val: Any) -> Any:
        """
        Рендер через базовый, но с поддержкой локальных ключей:
        сначала ищем ${key} в своём неймспейсе, если пусто — глобал.
        """
        if isinstance(val, str):
            pat = re.compile(r"\$\{([A-Za-z0-9_]+)(?::-(.*?))?\}")
            def repl(m: re.Match[str]) -> str:
                key = m.group(1)
                fallback = m.group(2)
                got = self.get(key, None)
                if got is None:
                    got = self.base.get(key, fallback if fallback is not None else "")
                return "" if got is None else str(got)
            return pat.sub(repl, val)
        if isinstance(val, dict):
            return {k: self.render(v) for k, v in val.items()}
        if isinstance(val, list):
            return [self.render(v) for v in val]
        return val
