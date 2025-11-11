from __future__ import annotations

"""
Verification helpers for the Vision pipeline.

Two complementary checks:
  1) OCR-based: re-checks the screenshot (per-field crop if possible) to see
     whether expected text is present near the bbox of the target id.
  2) DOM-based (best-effort): queries elementFromPoint at bbox center and tries
     to read value/text from input/textarea/contentEditable.

All functions are defensive: absence of optional deps (EasyOCR/Pillow) or
errors during probing lead to soft warnings rather than hard failures.
"""

from typing import Dict, Iterable, List, Optional, Tuple

from .schema import BBox, VerifyIssue


# ----------------------------- Geometry utils -----------------------------

def _iou(a: BBox, b: BBox) -> float:
    ax1, ay1, ax2, ay2 = a.as_tuple()
    bx1, by1, bx2, by2 = b.as_tuple()
    ix1, iy1 = max(ax1, bx1), max(ay1, by1)
    ix2, iy2 = min(ax2, bx2), min(ay2, by2)
    iw, ih = max(0.0, ix2 - ix1), max(0.0, iy2 - iy1)
    inter = iw * ih
    if inter <= 0:
        return 0.0
    aw, ah = max(0.0, ax2 - ax1), max(0.0, ay2 - ay1)
    bw, bh = max(0.0, bx2 - bx1), max(0.0, by2 - by1)
    union = aw * ah + bw * bh - inter
    if union <= 0:
        return 0.0
    return float(inter) / float(union)


def _norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


# ----------------------------- OCR-based verify ---------------------------

def verify_ocr(
    png_bytes: bytes,
    fills: Dict[str, str],
    id2bbox: Dict[str, BBox],
    *,
    langs: Optional[Iterable[str]] = None,
    gpu: Optional[bool] = None,
    min_score: float = 0.4,
    min_iou: float = 0.05,
) -> List[VerifyIssue]:
    """Verify that expected filled texts appear near their target bboxes.

    Strategy:
      - If Pillow is available, crop each bbox and run OCR per-crop for higher signal;
        else run OCR once on the full image and pick the best-overlapping block.
      - For each (id->expected), check that normalized expected text is a substring
        of the recognized text (also normalized). Minor deviations are tolerated.
    """
    issues: List[VerifyIssue] = []
    if not png_bytes or not fills:
        return issues

    # Lazy imports; guard if EasyOCR not installed
    try:
        from .ocr import recognize, crop  # type: ignore
    except Exception:
        # If OCR unavailable, return soft warnings (caller may ignore OCR stage)
        for fid in fills.keys():
            issues.append(VerifyIssue(id=fid, reason="ocr_unavailable", severity="warn"))
        return issues

    # Try per-crop OCR (preferred)
    per_crop_ok = True
    for fid, exp in fills.items():
        bb = id2bbox.get(fid)
        if not bb:
            issues.append(VerifyIssue(id=fid, reason="bbox_missing"))
            continue
        try:
            sub = crop(png_bytes, bb)
            rows = recognize(sub, langs=langs, gpu=gpu, min_score=min_score, limit=40)
            joined = _norm(" ".join([r.text for r in rows]))
            if _norm(str(exp)) not in joined:
                issues.append(VerifyIssue(id=fid, reason="ocr_mismatch", severity="warn"))
        except Exception:
            per_crop_ok = False
            break

    if per_crop_ok:
        return issues

    # Fallback: single pass on full image, match by IoU
    try:
        rows = recognize(png_bytes, langs=langs, gpu=gpu, min_score=min_score, limit=300)
    except Exception:
        for fid in fills.keys():
            issues.append(VerifyIssue(id=fid, reason="ocr_failed", severity="warn"))
        return issues

    for fid, exp in fills.items():
        bb = id2bbox.get(fid)
        if not bb:
            issues.append(VerifyIssue(id=fid, reason="bbox_missing"))
            continue
        best_txt = ""
        best_iou = 0.0
        for r in rows:
            i = _iou(bb, r.bbox)
            if i > best_iou:
                best_iou = i
                best_txt = r.text
        if best_iou < min_iou:
            issues.append(VerifyIssue(id=fid, reason="ocr_overlap_low", severity="warn"))
            continue
        if _norm(str(exp)) not in _norm(best_txt):
            issues.append(VerifyIssue(id=fid, reason="ocr_mismatch", severity="warn"))

    return issues


# ----------------------------- DOM-based verify ---------------------------

def verify_dom(
    driver,
    fills: Dict[str, str],
    id2bbox: Dict[str, BBox],
    *,
    radius_px: int = 8,
) -> List[VerifyIssue]:
    """Try to read value/text near bbox center using elementFromPoint.

    It's a best-effort probe; absence of DOM or JS errors produce warn issues.
    """
    issues: List[VerifyIssue] = []
    if not fills:
        return issues

    js = r"""
    const cx = Number(arguments[0]||0), cy = Number(arguments[1]||0), r = Number(arguments[2]||6);
    function at(x,y){ try{ return document.elementFromPoint(x,y); }catch(e){ return null; } }
    function snap(el){
      if(!el) return null;
      const tag = (el.tagName||'').toLowerCase();
      const role = (el.getAttribute && el.getAttribute('role')) || '';
      let value = '';
      try{
        if (tag==='input' || tag==='textarea') value = el.value||'';
        else if (el.isContentEditable) value = el.innerText||el.textContent||'';
        else value = (el.innerText||el.textContent||'');
      }catch(e){ value=''; }
      const aria = (el.getAttribute && el.getAttribute('aria-label')) || '';
      return {tag, role, value: String(value||''), aria: String(aria||'')};
    }
    const pts = [
      [cx, cy], [cx+r, cy], [cx-r, cy], [cx, cy+r], [cx, cy-r]
    ];
    const outs = [];
    for (const p of pts){ const el = at(p[0], p[1]); outs.push(snap(el)); }
    return outs;
    """

    for fid, exp in fills.items():
        bb = id2bbox.get(fid)
        if not bb:
            issues.append(VerifyIssue(id=fid, reason="bbox_missing"))
            continue
        cx, cy = bb.center()
        try:
            outs = driver.execute_script(js, float(cx), float(cy), int(radius_px)) or []
        except Exception:
            issues.append(VerifyIssue(id=fid, reason="dom_probe_failed", severity="warn"))
            continue
        found = False
        for r in outs or []:
            try:
                val = _norm(str((r or {}).get("value") or ""))
                if _norm(str(exp)) and (_norm(str(exp)) in val):
                    found = True
                    break
            except Exception:
                continue
        if not found:
            issues.append(VerifyIssue(id=fid, reason="dom_mismatch", severity="warn"))

    return issues


__all__ = [
    "verify_ocr",
    "verify_dom",
]

