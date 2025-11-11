# ads_ai/tracing/trace.py
from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from ads_ai.utils.paths import ensure_dir


class JsonlTrace:
    """
    Потокобезопасный JSONL-трейс.

    Особенности:
      - Безопасная сериализация: несерилизуемые объекты превращаются в строки.
      - Автоматическое добавление поля "ts" (unix time, float).
      - Мягкая ротация по размеру (ENV: TRACING_MAX_BYTES, TRACING_MAX_BACKUPS).
      - Если path=None — трейс в /dev/null (no-op).

    Формат строки: одна JSON-запись на строку (UTF-8, без ASCII-экранирования).
    """

    # Значения по умолчанию (разумные и безопасные)
    _DEFAULT_MAX_BYTES = 5 * 1024 * 1024  # 5 MB
    _DEFAULT_BACKUPS = 3                  # trace.jsonl.1 .. .3

    def __init__(self, path: Optional[Path]) -> None:
        self.path = path
        self._lock = threading.Lock()

        # Настройки ротации читаем один раз при инициализации.
        self._max_bytes = self._read_env_int("TRACING_MAX_BYTES", self._DEFAULT_MAX_BYTES)
        self._max_backups = max(0, self._read_env_int("TRACING_MAX_BACKUPS", self._DEFAULT_BACKUPS))

        if self.path:
            ensure_dir(self.path.parent)

    # --------------------------- Публичное API --------------------------- #

    def write(self, rec: Dict[str, Any]) -> None:
        """
        Записать запись в JSONL.

        Поведение:
          - Если path=None — тихо выходим.
          - Добавляем поле "ts", не мутируя исходный словарь.
          - Любые ошибки сериализации/IO — подавляем (трейс не должен валить рантайм).
        """
        if not self.path:
            return

        try:
            payload = dict(rec) if isinstance(rec, dict) else {"event": "log", "payload": str(rec)}
            payload.setdefault("ts", time.time())

            line = self._safe_dumps(payload) + "\n"

            with self._lock:
                self._rotate_if_needed_unlocked()
                # NB: режим "a" гарантирует дозапись в конец даже при множестве процессов.
                with self.path.open("a", encoding="utf-8") as f:  # type: ignore[union-attr]
                    f.write(line)
                    # Явный flush не обязателен (файл закрывается), но оставим на будущее:
                    # f.flush()
        except Exception:
            # Никогда не валим рабочий процесс из-за логирования
            return

    # --------------------------- Внутренние утилиты ---------------------- #

    @staticmethod
    def _read_env_int(name: str, default: int) -> int:
        """Мягкое чтение int из ENV; любые ошибки — default."""
        try:
            raw = os.getenv(name, "").strip()
            if not raw:
                return default
            val = int(raw)
            # Специальный случай: 0 → отключить ротацию/бэкапы
            return max(0, val)
        except Exception:
            return default

    @staticmethod
    def _json_default(obj: Any) -> Any:
        """
        Фоллбек для несерилизуемых значений.
        По возможности возвращаем человекочитаемое представление.
        """
        try:
            # bytes → компактная пометка (без раздувания лога base64)
            if isinstance(obj, (bytes, bytearray)):
                return f"<bytes:{len(obj)}>"
            # Path, Exception и т.п. → строка
            return str(obj)
        except Exception:
            return "<unserializable>"

    @classmethod
    def _safe_dumps(cls, obj: Dict[str, Any]) -> str:
        """Сериализуем в JSON, не падая на нестандартных типах, сохраняем Unicode."""
        try:
            return json.dumps(obj, ensure_ascii=False, default=cls._json_default)
        except Exception:
            # Последняя линия обороны: грубая деградация
            try:
                return json.dumps({"event": "trace_error", "payload": str(obj)[:1000]}, ensure_ascii=False)
            except Exception:
                return '{"event":"trace_error","payload":"<unserializable>"}'

    def _rotate_if_needed_unlocked(self) -> None:
        """
        Ротация по размеру файла. Вызывать ТОЛЬКО под self._lock.
        Алгоритм:
          - Если ограничение выключено (max_bytes==0) — ничего не делаем.
          - Если текущий файл > max_bytes — сдвигаем .N → .N+1 и .jsonl → .1.
        """
        if not self.path or self._max_bytes <= 0:
            return

        try:
            if not self.path.exists():
                return
            size = self.path.stat().st_size
            if size <= self._max_bytes:
                return

            # Если бэкапы выключены — просто обрезаем файл.
            if self._max_backups == 0:
                # Переоткрыть файл в truncate-режиме
                with self.path.open("w", encoding="utf-8"):
                    pass
                return

            # Сдвиг: .(max_backups-1) -> .(max_backups), ..., .1 -> .2
            for i in range(self._max_backups - 1, 0, -1):
                src = self._backup_path(i)
                dst = self._backup_path(i + 1)
                if src.exists():
                    try:
                        if dst.exists():
                            dst.unlink(missing_ok=True)  # py>=3.8
                        src.rename(dst)
                    except Exception:
                        # Пропускаем проблемы с конкретным файлом бэкапа
                        pass

            # Текущий файл -> .1
            first = self._backup_path(1)
            try:
                if first.exists():
                    first.unlink(missing_ok=True)
                self.path.rename(first)
            except Exception:
                # Если не смогли переименовать — пробуем просто обрезать
                try:
                    with self.path.open("w", encoding="utf-8"):
                        pass
                except Exception:
                    pass

        except Exception:
            # Любые ошибки ротации не должны ломать процесс записи
            return

    def _backup_path(self, idx: int) -> Path:
        """
        Имя бэкапа: <file>.{idx}
        Пример: trace.jsonl.1, trace.jsonl.2 ...
        """
        assert self.path is not None
        return self.path.with_name(self.path.name + f".{idx}")


# --------------------------- Контекст и фабрика ------------------------------- #

@dataclass
class TraceContext:
    run_id: str
    file: Path


def make_trace(traces_dir: Path, run_id: str) -> tuple[JsonlTrace, TraceContext]:
    """
    Создаёт трейс и контекст для заданного run_id.
    Папка будет создана при необходимости.
    """
    file = traces_dir / f"{run_id}.jsonl"
    return JsonlTrace(file), TraceContext(run_id=run_id, file=file)
