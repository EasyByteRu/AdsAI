from __future__ import annotations

"""
EasyOCR wrapper for the Vision pipeline.

Design goals:
- Lazy imports (no heavy deps on module import).
- Clear, typed interface based on ads_ai.vision.schema.
- Works with bytes input (PNG/JPEG) without requiring cv2/PIL at runtime.

If EasyOCR (and its transitive deps like torch) are not installed, the
functions will raise a descriptive RuntimeError so callers can degrade
gracefully or instruct the user to install optional packages.
"""

from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple
import os
import tempfile
import uuid
import contextlib

from .schema import BBox, OCRItem


# ----------------------------- Internal utils -----------------------------

@dataclass(frozen=True)
class _ReaderKey:
    langs: Tuple[str, ...]
    gpu: bool


_READERS: dict[_ReaderKey, object] = {}


def _env_bool(name: str, default: bool = False) -> bool:
    v = str(os.getenv(name, "")).strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "on")


def _split_langs(langs: Optional[Iterable[str]]) -> Tuple[str, ...]:
    if langs:
        seq = [str(x).strip() for x in langs if str(x).strip()]
        return tuple(seq or ("en",))
    raw = os.getenv("ADS_AI_OCR_LANGS") or os.getenv("ADS_AI_VISION_OCR_LANGS") or "ru,en"
    seq = [s.strip() for s in raw.replace(";", ",").split(",") if s.strip()]
    return tuple(seq or ("en",))


def _detect_gpu_enabled() -> bool:
    # Default: CPU (to avoid torch installation requirement)
    want_gpu = _env_bool("ADS_AI_VISION_OCR_GPU", False)
    if not want_gpu:
        return False
    with contextlib.suppress(Exception):
        import torch  # type: ignore

        return bool(getattr(torch, "cuda", None) and torch.cuda.is_available())
    return False


def _import_easyocr():  # type: ignore[return-type]
    try:
        import easyocr  # type: ignore

        return easyocr
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "EasyOCR is not installed. Install optional deps: pip install easyocr torch torchvision"
        ) from e


# ----------------------------- Public API ---------------------------------

def get_reader(langs: Optional[Iterable[str]] = None, gpu: Optional[bool] = None):
    """Return (and cache) an EasyOCR Reader instance.

    langs: sequence like ["ru","en"]. If None, taken from env ADS_AI_OCR_LANGS.
    gpu: force GPU if True/False. If None, use ADS_AI_VISION_OCR_GPU (default False).
    """
    easyocr = _import_easyocr()
    langs_t = _split_langs(langs)
    use_gpu = bool(_detect_gpu_enabled() if gpu is None else gpu)
    key = _ReaderKey(langs_t, use_gpu)
    rdr = _READERS.get(key)
    if rdr is not None:
        return rdr
    # EasyOCR Reader constructor can be expensive — cache per (langs, gpu)
    rdr = easyocr.Reader(list(langs_t), gpu=use_gpu)  # type: ignore[arg-type]
    _READERS[key] = rdr
    return rdr


def _poly_to_bbox(poly: Sequence[Sequence[float]]) -> BBox:
    # EasyOCR returns 4 points (quadrilateral). Convert to AABB.
    xs: List[float] = []
    ys: List[float] = []
    for p in (poly or []):
        try:
            xs.append(float(p[0]))
            ys.append(float(p[1]))
        except Exception:
            continue
    if not xs or not ys:
        return BBox(0.0, 0.0, 0.0, 0.0)
    return BBox(min(xs), min(ys), max(xs), max(ys))


def recognize(
    png_bytes: bytes,
    *,
    langs: Optional[Iterable[str]] = None,
    gpu: Optional[bool] = None,
    min_score: float = 0.5,
    limit: int = 200,
) -> List[OCRItem]:
    """Run OCR on the provided PNG/JPEG bytes and return OCRItem list.

    - Uses a temp file to avoid hard deps on cv2/PIL for byte decoding.
    - Filters by min_score and caps the result length.
    - Items are sorted top-to-bottom, then left-to-right for stability.
    """
    if not png_bytes:
        return []
    rdr = get_reader(langs=langs, gpu=gpu)

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as f:
            f.write(png_bytes)
            tmp_path = f.name
        # detail=1 -> list of [bbox(list of 4 points), text, score]
        data = rdr.readtext(tmp_path, detail=1, paragraph=False)  # type: ignore[attr-defined]
    finally:
        if tmp_path:
            with contextlib.suppress(Exception):
                os.remove(tmp_path)

    items: List[OCRItem] = []
    for i, row in enumerate(data or []):
        try:
            poly, text, score = row[0], row[1], float(row[2])
        except Exception:
            # alternative tuple ordering in some versions
            try:
                poly = row[0]
                text = row[1]
                score = float(row[2])
            except Exception:
                continue
        if not text:
            continue
        if score < float(min_score):
            continue
        bb = _poly_to_bbox(poly)
        item = OCRItem(id=f"id{i+1}", bbox=bb, text=str(text), score=score)
        items.append(item)

    # sort rows: y, then x
    items.sort(key=lambda it: (it.bbox.y1, it.bbox.x1))
    if limit and len(items) > int(limit):
        items = items[: int(limit)]
    return items


def crop(png_bytes: bytes, bbox: BBox) -> bytes:
    """Return cropped PNG bytes for the given bbox.

    Uses Pillow if available; otherwise returns the original bytes.
    """
    try:
        from PIL import Image  # type: ignore
        import io

        with Image.open(io.BytesIO(png_bytes)) as im:
            x1, y1, x2, y2 = bbox.as_tuple()
            x1, y1, x2, y2 = int(x1), int(y1), int(x2), int(y2)
            x1 = max(0, x1)
            y1 = max(0, y1)
            x2 = max(x1 + 1, x2)
            y2 = max(y1 + 1, y2)
            box = (x1, y1, x2, y2)
            out = im.crop(box)
            buf = io.BytesIO()
            out.save(buf, format="PNG")
            return buf.getvalue()
    except Exception:
        return png_bytes


__all__ = [
    "get_reader",
    "recognize",
    "crop",
]

