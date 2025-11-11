# ads_ai/utils/time.py
from __future__ import annotations

import random
import time


def sleep_s(sec: float) -> None:
    time.sleep(max(0.0, float(sec)))


def jitter_ms(min_ms: int, max_ms: int) -> float:
    min_ms = max(0, int(min_ms))
    max_ms = max(min_ms, int(max_ms))
    return random.uniform(min_ms, max_ms) / 1000.0
