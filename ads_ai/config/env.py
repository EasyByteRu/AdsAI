# ads_ai/config/env.py
from __future__ import annotations

import os
from typing import Optional, Any, Callable


def load_env(dotenv_path: Optional[str] = None) -> None:
    """
    Загружаем переменные окружения из .env, если python-dotenv доступен.
    Никаких ошибок наружу — работаем мягко.
    """
    try:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=dotenv_path)
    except Exception:
        pass


def getenv(name: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(name, default)


def getenv_required(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Переменная окружения {name} не задана")
    return val


def _coerce(getter: Callable[[str, Optional[str]], Optional[str]], caster: Callable[[str], Any], name: str, default: Any):
    raw = getter(name, None)
    if raw is None:
        return default
    try:
        return caster(raw)
    except Exception:
        return default


def getenv_bool(name: str, default: bool = False) -> bool:
    return _coerce(getenv, lambda s: str(s).strip().lower() in {"1","true","yes","y","on"}, name, default)


def getenv_int(name: str, default: int = 0) -> int:
    return _coerce(getenv, int, name, default)


def getenv_float(name: str, default: float = 0.0) -> float:
    return _coerce(getenv, float, name, default)
