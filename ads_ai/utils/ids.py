# ads_ai/utils/ids.py
from __future__ import annotations

import time
import uuid
import hashlib


def now_id(prefix: str) -> str:
    return f"{prefix}_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}"


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", "ignore")).hexdigest()
