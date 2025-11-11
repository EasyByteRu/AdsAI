# ads_ai/utils/json_tools.py
from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Union


def safe_str(s: str) -> str:
    s = re.sub(r'[\ud800-\udfff]', '', s)
    return s.encode("utf-8", "ignore").decode("utf-8", "ignore")


def extract_first_json(text: str) -> Optional[Union[List[Any], Dict[str, Any]]]:
    if not text:
        return None
    t = text.strip()
    if t.startswith("```"):
        t = re.sub(r"^```+\w*\n?", "", t).rsplit("```", 1)[0].strip()
    for candidate in (t, text):
        try:
            obj = json.loads(candidate)
            if isinstance(obj, (list, dict)):
                return obj
        except Exception:
            pass
    start_positions = [m.start() for m in re.finditer(r'[\[\{]', t)]
    for pos in start_positions:
        chunk = t[pos:]
        stack = []
        end = None
        for i, ch in enumerate(chunk):
            if ch in "[{":
                stack.append(ch)
            elif ch in "]}":
                if not stack:
                    break
                top = stack.pop()
                if (top, ch) not in [("[", "]"), ("{", "}")]:
                    break
                if not stack:
                    end = i + 1
                    break
        if end:
            try:
                obj = json.loads(chunk[:end])
                if isinstance(obj, (list, dict)):
                    return obj
            except Exception:
                continue
    return None
