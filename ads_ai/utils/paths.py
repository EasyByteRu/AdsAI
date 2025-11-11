# ads_ai/utils/paths.py
from __future__ import annotations

from pathlib import Path


def project_root() -> Path:
    # ads_ai/utils/paths.py → parents[2] = корень проекта
    return Path(__file__).resolve().parents[2]


def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p
