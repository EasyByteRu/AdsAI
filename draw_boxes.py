#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
draw_boxes.py
Простой инструмент для визуализации боксов, возвращаемых LLM/vision-пайплайном.

Использование:
  python draw_boxes.py --image /path/to/image.png --json result.json --out out.png

JSON ожидается в формате, совместимом с промптом (fields + meta).
Если bbox == null, элемент пропускается.
Если указаны bbox_norm (0..1), они будут конвертированы в пиксели по meta.image_width/height.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

from PIL import Image, ImageDraw, ImageFont


def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def norm_to_pixels(bbox_norm: Dict[str, float], img_w: int, img_h: int) -> Dict[str, int]:
    x1 = int(round(bbox_norm["x1"] * img_w))
    y1 = int(round(bbox_norm["y1"] * img_h))
    x2 = int(round(bbox_norm["x2"] * img_w))
    y2 = int(round(bbox_norm["y2"] * img_h))
    # clamp
    x1 = max(0, min(img_w - 1, x1))
    x2 = max(0, min(img_w - 1, x2))
    y1 = max(0, min(img_h - 1, y1))
    y2 = max(0, min(img_h - 1, y2))
    return {"x1": x1, "y1": y1, "x2": x2, "y2": y2}


def ensure_bbox_pixels(
    bbox: Optional[Dict[str, Any]],
    bbox_norm: Optional[Dict[str, Any]],
    meta: Dict[str, Any],
) -> Optional[Dict[str, int]]:
    """
    Возвращает bbox в пикселях (целые) или None.
    """
    if bbox and all(k in bbox and bbox[k] is not None for k in ("x1", "y1", "x2", "y2")):
        img_w = int(meta.get("image_width") or meta.get("width") or 0)
        img_h = int(meta.get("image_height") or meta.get("height") or 0)
        try:
            x1 = int(round(bbox["x1"]))
            y1 = int(round(bbox["y1"]))
            x2 = int(round(bbox["x2"]))
            y2 = int(round(bbox["y2"]))
        except Exception:
            return None
        if img_w and img_h:
            x1 = max(0, min(img_w - 1, x1))
            x2 = max(0, min(img_w - 1, x2))
            y1 = max(0, min(img_h - 1, y1))
            y2 = max(0, min(img_h - 1, y2))
        return {"x1": x1, "y1": y1, "x2": x2, "y2": y2}
    if bbox_norm and all(k in bbox_norm and bbox_norm[k] is not None for k in ("x1", "y1", "x2", "y2")):
        img_w = int(meta.get("image_width") or meta.get("width") or 0)
        img_h = int(meta.get("image_height") or meta.get("height") or 0)
        if img_w and img_h:
            return norm_to_pixels(bbox_norm, img_w, img_h)
    return None


def get_text_size(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> Tuple[int, int]:
    """
    Надёжный способ получить размеры текста.
    Попытаться textbbox -> font.getsize -> fallback estimate.
    """
    try:
        # Pillow >= 8.0
        bbox = draw.textbbox((0, 0), text, font=font)
        w = bbox[2] - bbox[0]
        h = bbox[3] - bbox[1]
        return w, h
    except Exception:
        try:
            return font.getsize(text)
        except Exception:
            # грубая оценка: ширина ~ 7px/символ, высота ~ font.size or 12
            approx_w = max(50, len(text) * 7)
            approx_h = getattr(font, "size", 12) if hasattr(font, "size") else 12
            return approx_w, approx_h


def draw_annotations(image_path: Path, data: Dict[str, Any], out_path: Path, line_width: int = 3) -> None:
    img = Image.open(image_path).convert("RGBA")
    draw = ImageDraw.Draw(img)
    img_w, img_h = img.size

    meta = data.get("meta", {})
    if not meta.get("image_width"):
        meta["image_width"] = img_w
    if not meta.get("image_height"):
        meta["image_height"] = img_h

    # choose font
    try:
        font = ImageFont.truetype("DejaVuSans.ttf", size=14)
    except Exception:
        font = ImageFont.load_default()

    fields = data.get("fields", [])
    # colors cycle (RGBA)
    colors = [
        (255, 0, 0, 220),
        (0, 160, 255, 220),
        (0, 200, 0, 220),
        (255, 165, 0, 220),
        (160, 32, 240, 220),
    ]

    for i, field in enumerate(fields):
        bbox = field.get("bbox")
        bbox_norm = field.get("bbox_norm")
        pix = ensure_bbox_pixels(bbox, bbox_norm, meta)
        if not pix:
            continue

        x1, y1, x2, y2 = pix["x1"], pix["y1"], pix["x2"], pix["y2"]
        x1, x2 = min(x1, x2), max(x1, x2)
        y1, y2 = min(y1, y2), max(y1, y2)

        color = colors[i % len(colors)]
        # draw rectangle outline (multiple offsets to simulate line width)
        for offset in range(line_width):
            draw.rectangle([x1 - offset, y1 - offset, x2 + offset, y2 + offset], outline=color)

        # caption: id | label | confidence
        label_text = field.get("label") or ""
        caption = f"{field.get('id','')} {label_text[:60]}".strip()
        conf = field.get("confidence")
        if conf is not None:
            try:
                conf_str = f"{float(conf):.2f}"
            except Exception:
                conf_str = str(conf)
            caption = f"{caption} [{conf_str}]"

        # calculate text size robustly
        text_w, text_h = get_text_size(draw, caption, font)
        padding = 4
        rect_bg = (255, 255, 255, 220)
        # text position: try above bbox, else inside top-left
        tx = x1
        ty = y1 - text_h - 2 * padding
        if ty < 0:
            ty = y1 + padding

        # draw semi-opaque background for text
        draw.rectangle([tx, ty, tx + text_w + 2 * padding, ty + text_h + 2 * padding], fill=rect_bg)
        draw.text((tx + padding, ty + padding), caption, fill=(0, 0, 0, 255), font=font)

    # Save output
    out_format = out_path.suffix.lower().lstrip(".")
    if out_format in ("jpg", "jpeg"):
        rgb = Image.new("RGB", img.size, (255, 255, 255))
        rgb.paste(img, mask=img.split()[3])  # use alpha as mask
        rgb.save(out_path, quality=90)
    else:
        img.save(out_path)

    print(f"[ok] Saved annotated image to: {out_path}")


def main() -> None:
    p = argparse.ArgumentParser(description="Draw bounding boxes from LLM/Vision JSON on an image")
    p.add_argument("--image", "-i", type=Path, required=True, help="Path to input image")
    p.add_argument("--json", "-j", type=Path, required=True, help="Path to JSON file (LLM result)")
    p.add_argument("--out", "-o", type=Path, default=Path("image_with_boxes.png"), help="Output image path")
    p.add_argument("--line-width", type=int, default=3, help="Rectangle line width")
    args = p.parse_args()

    if not args.image.exists():
        raise SystemExit(f"Image not found: {args.image}")
    if not args.json.exists():
        raise SystemExit(f"JSON not found: {args.json}")

    data = load_json(args.json)
    draw_annotations(args.image, data, args.out, line_width=args.line_width)


if __name__ == "__main__":
    main()
