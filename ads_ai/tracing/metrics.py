# ads_ai/tracing/metrics.py
from __future__ import annotations

import time
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Optional, Iterator, Mapping
from contextlib import contextmanager

from ads_ai.tracing.trace import JsonlTrace


# ------------------------------ Счётчики --------------------------------------

@dataclass
class Counters:
    """
    Базовые счётчики выполнения плана/задачи.

    ВАЖНО: не добавляйте сюда новые поля без реальной необходимости —
    это часть «контракта» с анализом логов и тестами.
    """
    total_steps: int = 0
    ok_steps: int = 0
    repairs: int = 0
    skips: int = 0
    replans: int = 0
    dom_stable_waits: int = 0
    loops_guard_trips: int = 0

    def inc(self, **kwargs: int) -> None:
        """
        Безопасное увеличение нескольких счётчиков.
        Игнорирует неизвестные ключи, значения приводятся к int и нормализуются (минимум 0 или отрицательное? допускаем).
        """
        for k, v in kwargs.items():
            if hasattr(self, k):
                try:
                    cur = getattr(self, k)
                    setattr(self, k, int(cur) + int(v))
                except Exception:
                    # в случае редких ошибок приведения типов — пропускаем
                    continue


# ------------------------------ Секундомер ------------------------------------

class Stopwatch:
    """
    Простой секундомер с поддержкой контекстного менеджера.
    Использует монотонные часы (perf_counter) для корректного измерения интервалов.
    """
    __slots__ = ("_t0", "elapsed")

    def __init__(self) -> None:
        self._t0: Optional[float] = None
        self.elapsed: float = 0.0  # секунды

    def start(self) -> "Stopwatch":
        """Запустить секундомер (повторный запуск без остановки перезапишет старт)."""
        self._t0 = time.perf_counter()
        return self

    def stop(self) -> "Stopwatch":
        """Остановить секундомер и накопить интервал в elapsed."""
        if self._t0 is not None:
            self.elapsed += max(0.0, time.perf_counter() - self._t0)
            self._t0 = None
        return self

    def reset(self) -> None:
        """Обнулить накопленное время и остановить секундомер."""
        self._t0 = None
        self.elapsed = 0.0

    def running(self) -> bool:
        """Возвращает True, если секундомер запущен."""
        return self._t0 is not None

    def __enter__(self) -> "Stopwatch":
        return self.start()

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()


# ------------------------------ Метрики рантайма -------------------------------

@dataclass
class Metrics:
    """
    Агрегатор метрик рантайма с удобной сериализацией в трейс.

    Состав:
      - counters: общие счётчики (см. Counters)
      - timers: именованные секундомеры (по ключам)
      - extras: произвольные поля (метаданные), которые хочется унести в отчёт

    Совместимость:
      - существующие методы inc_*, timing(), emit() — без изменений.
    """
    counters: Counters = field(default_factory=Counters)
    timers: Dict[str, Stopwatch] = field(default_factory=dict)
    extras: Dict[str, Any] = field(default_factory=dict)

    # ---- Counters helpers -------------------------------------------------

    def inc_total(self, n: int = 1) -> None:
        self.counters.inc(total_steps=n)

    def inc_ok(self, n: int = 1) -> None:
        self.counters.inc(ok_steps=n)

    def inc_repairs(self, n: int = 1) -> None:
        self.counters.inc(repairs=n)

    def inc_skips(self, n: int = 1) -> None:
        self.counters.inc(skips=n)

    def inc_replans(self, n: int = 1) -> None:
        self.counters.inc(replans=n)

    def inc_dom_stable(self, n: int = 1) -> None:
        self.counters.inc(dom_stable_waits=n)

    def inc_loop_trips(self, n: int = 1) -> None:
        self.counters.inc(loops_guard_trips=n)

    def inc_bulk(self, values: Mapping[str, int]) -> None:
        """
        Пакетное увеличение счётчиков: metrics.inc_bulk({'ok_steps': 2, 'skips': 1})
        Неизвестные поля игнорируются.
        """
        try:
            self.counters.inc(**{k: int(v) for k, v in values.items()})
        except Exception:
            # мягкая деградация — ничего не инкрементируем, не поднимаем исключение
            pass

    # ---- Timers helpers ---------------------------------------------------

    def get_timer(self, name: str) -> Stopwatch:
        """Вернёт существующий секундомер либо создаст новый."""
        if name not in self.timers:
            self.timers[name] = Stopwatch()
        return self.timers[name]

    def stop_all(self) -> None:
        """Остановить все активные секундомеры (полезно при аварийном выходе)."""
        for sw in self.timers.values():
            if sw.running():
                sw.stop()

    @contextmanager
    def timing(self, name: str) -> Iterator[None]:
        """
        Контекстный менеджер измерения времени:
            with metrics.timing("step_execute"):
                ...
        """
        sw = self.get_timer(name)
        sw.start()
        try:
            yield
        finally:
            sw.stop()

    # ---- Extras -----------------------------------------------------------

    def set_extra(self, key: str, value: Any) -> None:
        """Сохранить произвольное поле в отчёт (JSON-сериализуемое или приводимое к строке)."""
        self.extras[key] = value

    # ---- Служебные операции ----------------------------------------------

    def reset(self) -> None:
        """Полный сброс метрик: счётчики, таймеры, extras."""
        self.counters = Counters()
        for sw in self.timers.values():
            sw.reset()
        self.timers.clear()
        self.extras.clear()

    def merge_from(self, other: "Metrics") -> None:
        """
        Слить метрики из `other` в текущие.
        Полезно для агрегирования результатов нескольких подзадач.
        """
        if not isinstance(other, Metrics):
            return
        # counters
        self.counters.inc(**asdict(other.counters))
        # timers — аккумулируем по именам
        for name, sw in other.timers.items():
            if name not in self.timers:
                self.timers[name] = Stopwatch()
            # добавляем накопленное время
            self.timers[name].elapsed += max(0.0, float(sw.elapsed))
        # extras — при конфликте берём значение из other (свежие данные важнее)
        self.extras.update(other.extras)

    def snapshot(self) -> Dict[str, Any]:
        """
        Безопасная копия метрик (plain dict), которую можно логировать/тестировать
        без риска модифицировать исходный объект.
        """
        return self.as_dict()

    # ---- Emission ---------------------------------------------------------

    def as_dict(self) -> Dict[str, Any]:
        """
        Представление метрик в виде словаря.
        - timers округляются до микросекунд (6 знаков) для стабильности логов.
        """
        return {
            "counters": asdict(self.counters),
            "timers": {k: round(float(v.elapsed), 6) for k, v in self.timers.items()},
            "extras": self._safe_extras(self.extras),
        }

    def emit(self, trace: JsonlTrace, event: str = "metrics") -> None:
        """
        Отправить снимок метрик в трейс.
        Любые ошибки сериализации или IO подавляются (логирование — best-effort).
        """
        try:
            trace.write({"event": event, **self.as_dict()})
        except Exception:
            # Никогда не валим рабочий код из-за сбоя логирования
            return

    # ---- Внутренности -----------------------------------------------------

    @staticmethod
    def _safe_extras(extras: Dict[str, Any]) -> Dict[str, Any]:
        """
        Мягкая нормализация extras: приводим сложные типы к строке, чтобы не ломать JSON.
        """
        out: Dict[str, Any] = {}
        for k, v in extras.items():
            try:
                # быстрый путь — если JSON-совместимо, пусть trace сам сериализует
                if isinstance(v, (str, int, float, bool)) or v is None:
                    out[k] = v
                else:
                    out[k] = str(v)
            except Exception:
                out[k] = "<unserializable>"
        return out
