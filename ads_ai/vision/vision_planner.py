from __future__ import annotations

"""
Vision planner: builds actionable steps from (screenshot + OCR items + context).

Pipeline:
  - build_prompt(...) → compact instruction with OCR summary
  - call_gemini_json_with_image(...) → returns parsed JSON (dict/list)
  - plan_actions(...) → returns List[Action] (validated, with heuristic fallback)

No heavy imports at module import time. google-generativeai is imported lazily.
"""

from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import os

from .schema import Action, OCRItem, Plan, parse_actions
from ads_ai.utils.json_tools import extract_first_json, safe_str


# ----------------------------- Heuristics ---------------------------------

def _match_score(text: str, needles: Sequence[str]) -> int:
    t = text.lower()
    score = 0
    for n in needles:
        n = n.lower()
        if n in t:
            score += 1 + (2 if len(n) > 3 else 0)
    return score


def _pick_best_by_text(items: Iterable[OCRItem], needles: Sequence[str]) -> Optional[OCRItem]:
    best: Optional[Tuple[int, OCRItem]] = None
    for it in items:
        s = _match_score(it.text, needles)
        if s <= 0:
            continue
        if not best or s > best[0]:
            best = (s, it)
    return best[1] if best else None


def _heuristic_actions(ocr: List[OCRItem], ctx: Dict[str, Any]) -> List[Action]:
    actions: List[Action] = []
    url = str(ctx.get("url") or ctx.get("landing_url") or "").strip()
    utp = str(ctx.get("utp") or ctx.get("description") or "").strip()
    company = (utp.split("\n")[0] if utp else "").strip() or "Моя компания"

    url_item = _pick_best_by_text(ocr, ["url", "http", "веб", "страниц", "website"])
    name_item = _pick_best_by_text(ocr, ["как называется", "название", "компан", "организа", "name"])
    next_item = _pick_best_by_text(ocr, ["далее", "продолжить", "next", "continue"])

    if name_item:
        actions.append(Action(id=name_item.id, kind="fill", value=company))
    if url_item and url:
        actions.append(Action(id=url_item.id, kind="fill", value=url))
    if next_item:
        actions.append(Action(id=next_item.id, kind="click"))
    return actions


# ----------------------------- Prompting ----------------------------------

def build_prompt(ocr_items: List[OCRItem], context: Dict[str, Any], *, max_items: int = 160) -> str:
    """Build compact instruction for the LLM with OCR summary and data context.

    The model must respond with strict JSON only:
      {"actions": [ {"id": "id7", "action": "fill", "value": "..."}, {"id": "id9", "action": "click"} ]}
    Allowed actions: fill, click. Don't invent ids not present in OCR list.
    """
    data_lines = [
        f"url={safe_str(context.get('url') or context.get('landing_url') or '')}",
        f"budget={safe_str(context.get('budget') or context.get('budget_daily') or '')}",
        f"currency={safe_str(context.get('currency') or context.get('currency_sign') or '')}",
        f"goal={safe_str(context.get('goal') or '')}",
        f"utp={safe_str(context.get('utp') or context.get('description') or '')}",
        f"geo={safe_str(context.get('geo') or '')}",
        f"language={safe_str(context.get('language') or 'ru')}",
    ]
    header = (
        "Ты — ассистент для автотестов UI. Есть скриншот мастера Google Ads и результат OCR. "
        "Нужно вернуть ТОЛЬКО JSON со списком действий, без пояснений. Разрешены два действия: "
        "fill (ввести текст) и click (клик по кнопке). Для fill обязательно поле value. "
        "Используй ТОЛЬКО id из OCR. При заполнении сосредоточься на: название компании (кратко из УТП) и URL веб-страницы. "
        "После заполнения нажми кнопку Далее/Продолжить, если она есть."
    )
    ocr_lines: List[str] = []
    for it in ocr_items[: max_items]:
        bb = it.bbox
        ocr_lines.append(
            f"- id={it.id}; bbox=[{int(bb.x1)},{int(bb.y1)},{int(bb.x2)},{int(bb.y2)}]; text={safe_str(it.text)}"
        )
    schema = (
        "Строгая схема ответа: {\"actions\": [ {\"id\": \"id1\", \"action\": \"fill\", \"value\": \"TEXT\"}, "
        "{\"id\": \"id9\", \"action\": \"click\"} ]}. Только JSON."
    )
    return "\n".join([
        header,
        "",
        "Данные:",
        *data_lines,
        "",
        "OCR items (id → bbox → text):",
        *ocr_lines,
        "",
        schema,
    ])


# ----------------------------- LLM Call -----------------------------------

def _call_gemini_json_with_image(
    prompt: str,
    image_bytes: bytes,
    *,
    model: str = "models/gemini-2.0-flash",
    api_key: Optional[str] = None,
    retries: int = 2,
    fallback_model: Optional[str] = "models/gemini-2.0-flash",
) -> Optional[Any]:
    try:
        import google.generativeai as genai  # type: ignore
    except Exception as e:  # pragma: no cover
        return None

    key = api_key or os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")  # type: ignore[name-defined]
    if not key:
        return None
    try:
        genai.configure(api_key=key)
        primary = genai.GenerativeModel(model)
        cfg = genai.GenerationConfig(temperature=0.1)
    except Exception:
        return None

    parts = [safe_str(prompt), {"mime_type": "image/png", "data": image_bytes}]
    err: Optional[Exception] = None
    for attempt in range(max(0, retries) + 1):
        try:
            res = primary.generate_content(parts, generation_config=cfg)
            txt = getattr(res, "text", None)
            if res and isinstance(txt, str):
                return extract_first_json(txt)
            if res and txt is not None:
                try:
                    return extract_first_json(str(txt))
                except Exception:
                    pass
        except Exception as e:
            err = e
            continue

    if fallback_model:
        try:
            fm = genai.GenerativeModel(fallback_model)
            res = fm.generate_content(parts, generation_config=cfg)
            txt = getattr(res, "text", None)
            if res and isinstance(txt, str):
                return extract_first_json(txt)
            if res and txt is not None:
                try:
                    return extract_first_json(str(txt))
                except Exception:
                    pass
        except Exception as e:
            err = e
            return None
    return None


# ----------------------------- Planner API --------------------------------

def plan_actions(
    png_bytes: bytes,
    ocr_items: List[OCRItem],
    context: Dict[str, Any],
    *,
    use_llm: bool = True,
    model: str = "models/gemini-2.0-flash",
    api_key: Optional[str] = None,
    retries: int = 2,
    fallback_model: Optional[str] = "models/gemini-2.0-flash",
) -> List[Action]:
    """Return actionable plan: List[Action].

    Uses Gemini multimodal call if available and allowed. Falls back to
    heuristic mapping if LLM unavailable or returns empty/invalid JSON.
    """
    if not isinstance(ocr_items, list) or not png_bytes:
        return []

    actions: List[Action] = []
    if use_llm:
        try:
            prompt = build_prompt(ocr_items, context)
        except Exception:
            prompt = safe_str(str(context))
        try:
            obj = _call_gemini_json_with_image(
                prompt, png_bytes, model=model, api_key=api_key, retries=retries, fallback_model=fallback_model
            )
            actions = parse_actions(obj)
        except Exception:
            actions = []

    # Validate ids and enrich if needed
    ids = {it.id for it in ocr_items}
    actions = [a for a in actions if a.id in ids and a.kind in ("fill", "click")]
    if not actions:
        actions = _heuristic_actions(ocr_items, context)
    return actions


__all__ = [
    "build_prompt",
    "plan_actions",
]
