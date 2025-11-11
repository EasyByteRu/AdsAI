# -*- coding: utf-8 -*-
"""
examples/steps/step7.py

Шаг 7 (на странице с активной группой ассетов):
  1) Headline(s) — заполнить привлекательными заголовками (<=30 симв), автодобавление до 15.
     Цель: min(15, n_ads).
  2) Long headline(s) — заполнить длинными заголовками (<=90 симв), РОВНО до 5 шт.
     Цель: 5.
  3) Description(s) — заполнить (<=90 симв). Кол-во: min(n_ads, 5).
  4) Images — "Add images" → вкладка "Upload":
     - Сгенерировать n_ads изображений (Runware) с миксом ориентаций.
     - Гарантировать минимум 1 альбомное (landscape).
     - Для n_ads=5 рандомно выбрать один из наборов: (1L+4S) | (2L+3S) | (3L+2S).
     - Размеры для генерации удовлетворяют требованиям Runware (кратны 64).
     - Отправить файлы через input[type=file], выждать 3–5с на каждый, дождаться активной Save и нажать.

Языки:
  - Если выбран только русский — всё генерируем на русском.
  - Если выбраны ru и en — делим примерно пополам (RU получает округление вверх).

Контракт:
    run_step7(
        driver: WebDriver,
        *,
        n_ads: int,
        business_name: str | None = None,
        usp: str | None = None,
        site_url: str | None = None,
        languages: list[str] | None = None,   # например ['ru'] или ['ru','en']
        image_prompt: str | None = None,      # кастомный общий промпт для картинок (будет расширен LLM)
        timeout_total: float = 300.0,
        emit: Optional[Callable[[str], None]] = None,
    ) -> dict

Политика генерации: ТОЛЬКО ИИ (LLM/Runware), без шаблонных фоллбеков.

СОХРАНЕНИЕ ИЗОБРАЖЕНИЙ:
  - Базовая папка: ENV ADS_AI_IMAGES_BASE, иначе ./companies/images
  - Имя папки: slug(business_name) | slug(domain(site_url)) | ENV ADS_AI_COMPANY_ID | id-<uuid8>
  - Имена файлов: ad_<NN>_<orient>_<WxH>.jpg (с уникализацией при конфликте)
"""

from __future__ import annotations

import io
import json
import logging
import os
import random
import re
import tempfile
import time
import uuid
import unicodedata
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Dict, Tuple, Callable, Set
from urllib.parse import urlparse, parse_qs, unquote

import requests
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webdriver import WebDriver, WebElement

# Pillow — опционально (компрессия изображений)
try:
    from PIL import Image  # type: ignore
except Exception:  # pragma: no cover
    Image = None  # type: ignore

# ---------- Логирование ----------
LOG_LEVEL = (os.getenv("ADS_AI_LOG_LEVEL") or "INFO").strip().upper()
logger = logging.getLogger("ads_ai.gads.step7")
if not logger.handlers:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
else:
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))


def _mk_run_id() -> str:
    return uuid.uuid4().hex[:8]


@contextmanager
def _stage(name: str, run_id: str, **meta: object):
    """Логирует старт/финиш стадии с длительностью и контекстом."""
    meta_kv = " ".join(f"{k}={v!r}" for k, v in meta.items() if v is not None)
    logger.info("[run=%s] ▶ %s %s", run_id, name, meta_kv and f"({meta_kv})" or "")
    t0 = time.time()
    try:
        yield
    except Exception as e:
        logger.exception("[run=%s] ✖ %s FAILED: %s", run_id, name, e)
        raise
    finally:
        dt = (time.time() - t0) * 1000.0
        logger.info("[run=%s] ◀ %s done in %.0f ms", run_id, name, dt)


def _heartbeat(run_id: str, label: str, every_iters: int, it: int, extra: str = "") -> None:
    """Редкие маяки из длинных циклов — чтобы было видно, что не зависло."""
    if every_iters > 0 and it % every_iters == 0:
        logger.debug("[run=%s] … %s: iter=%d %s", run_id, label, it, extra)


# ---------- LLM (Gemini) ----------
try:
    from ads_ai.llm.gemini import GeminiClient  # type: ignore
except Exception as e:  # pragma: no cover
    GeminiClient = None  # type: ignore
    logger.warning("GeminiClient not available: %s", e)

# ---------- Константы ----------
HEADLINE_MAX = 30
DESC_MAX = 90
LONG_HEADLINE_MAX = 90

HEADLINES_MAX_TOTAL = 15
LONG_HEADLINES_MAX_TOTAL = 5
DESCRIPTIONS_MAX_TOTAL = 5

# Пост-обработка загружаемых файлов (компрессия/ресайз)
MAX_LONG_EDGE: int = 1200
JPEG_Q: int = 78

# ---------- Runware ----------
DEFAULT_RUNWARE_API_KEY = os.getenv("RUNWARE_API_KEY", "2texOEYSQNN0tUFmr2ZaVbX6J62cbquL")
DEFAULT_RUNWARE_MODEL_ID = os.getenv("RUNWARE_MODEL_ID", "runware:100@1")
DEFAULT_RUNWARE_URL = os.getenv("RUNWARE_URL", "https://api.runware.ai/v1")
RUNWARE_RETRIES = 4

# Базовая NEG, далее расширяется контекстом
RUNWARE_NEG_BASE = "no text, no captions, no logos, no trademarks, no watermark, no QR, no barcodes, no charts or graphs, no UI screenshots, no stock icons, no clipart, no NSFW"

# ====== Размеры для генерации: кратные 64 ======
SQUARE_CHOICES: List[Tuple[int, int]] = [(1024, 1024), (1152, 1152)]
LANDSCAPE_SIZES: List[Tuple[int, int]] = [(1024, 576), (1216, 640), (1280, 704), (1408, 768)]

# ====== Поведение LLM генерации изображений ======
IMAGE_PROMPTS_MAX_ROUNDS = 4   # сколько раундов дозапросов делаем максимум
DIVERSITY_SIM_THR_DEFAULT = 0.55  # для поиска похожих пар
DIVERSITY_SIM_THR_RELAX = 0.62    # последний проход — чуть мягче «похожесть»

# ====== Утилиты ======

def _emit(emit: Optional[Callable[[str], None]], text: str) -> None:
    if callable(emit) and isinstance(text, str) and text.strip():
        try:
            emit(text.strip())
        except Exception:
            pass

def _limit_len(s: str, n: int) -> str:
    if s is None:
        return ""
    ss = re.sub(r"\s+", " ", str(s)).strip()
    return ss if len(ss) <= n else ss[:n].rstrip()

def _strip_quotes(s: str) -> str:
    return (s or "").replace('"', "").replace("'", "").strip()

def _asciiize(s: str) -> str:
    if not s:
        return ""
    ss = "".join(ch if ord(ch) < 128 else " " for ch in s)
    ss = re.sub(r"\s+", " ", ss).strip()
    ss = ss.replace('"', "").replace("'", "")
    return ss

def _has_cyrillic(s: str | None) -> bool:
    if not s:
        return False
    return bool(re.search(r"[\u0400-\u04FF]", s))

def _domain_to_brand(host: str) -> str:
    host = (host or "").lower().strip()
    host = re.sub(r"^https?://", "", host)
    host = host.split("/")[0]
    if not host or host == "ads.google.com":
        return "Brand"
    parts = host.split(".")
    core = parts[-2] if len(parts) >= 2 else parts[0]
    core = re.sub(r"[^a-z0-9\-]+", " ", core).strip()
    if not core:
        return "Brand"
    return core[:1].upper() + core[1:]

def _extract_site_from_current_url(driver: WebDriver) -> Optional[str]:
    try:
        u = driver.current_url or ""
        q = parse_qs(urlparse(u).query)
        if "cmpnInfo" in q:
            try:
                js = json.loads(unquote(q["cmpnInfo"][0]))
                for k in ("57", "site_url", "url"):
                    v = js.get(k)
                    if isinstance(v, str) and v.startswith("http"):
                        return v
            except Exception:
                pass
        if "preUrl" in q:
            try:
                pre = unquote(q["preUrl"][0])
                if "%7B" in pre or "{" in pre:
                    js = json.loads(unquote(pre.split("&cmpnInfo=")[-1]))
                    for k in ("57", "site_url", "url"):
                        v = js.get(k)
                        if isinstance(v, str) and v.startswith("http"):
                            return v
            except Exception:
                pass
    except Exception:
        pass
    return None

def _is_interactable(driver: WebDriver, el: WebElement) -> bool:
    try:
        if not el.is_displayed(): return False
        if not el.is_enabled(): return False
        if (el.get_attribute("aria-disabled") or "").lower() == "true": return False
        driver.execute_script("arguments[0].scrollIntoView({block:'center',inline:'nearest'});", el)
        r = el.rect
        return r.get("width", 0) >= 8 and r.get("height", 0) >= 8
    except Exception:
        return False

def _robust_click(driver: WebDriver, el: WebElement) -> bool:
    if not _is_interactable(driver, el):
        try: driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        except Exception: pass
    try:
        el.click()
        return True
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            try:
                driver.execute_script(
                    """
                    const el=arguments[0];
                    const r=el.getBoundingClientRect();
                    const x=Math.floor(r.left + Math.max(2, r.width/2));
                    const y=Math.floor(r.top  + Math.max(2, r.height/2));
                    const mk=(t)=>new MouseEvent(t,{view:window,bubbles:true,cancelable:true,clientX:x,clientY:y});
                    el.dispatchEvent(mk('mousedown')); el.dispatchEvent(mk('mouseup')); el.dispatchEvent(mk('click'));
                    """,
                    el
                )
                return True
            except Exception:
                return False

def _dispatch_input_change(driver: WebDriver, el: WebElement, value: str) -> None:
    driver.execute_script(
        """
        const el = arguments[0], val = arguments[1];
        try {
          el.focus();
          el.value = '';
          el.dispatchEvent(new Event('input', {bubbles:true}));
          el.value = val;
          el.dispatchEvent(new Event('input', {bubbles:true}));
          el.dispatchEvent(new Event('change', {bubbles:true}));
        } catch(e) {}
        """,
        el, value,
    )
    try:
        el.send_keys(Keys.END)
    except Exception:
        pass

def _dismiss_soft_dialogs(driver: WebDriver, budget_ms: int = 900) -> None:
    t0 = time.time()
    CAND = ["accept all", "i agree", "agree", "got it", "ok",
            "принять все", "я согласен", "понятно", "хорошо",
            "同意", "接受", "确定", "知道了", "好"]
    while (time.time() - t0) * 1000 < budget_ms:
        try:
            dialogs = driver.find_elements(By.CSS_SELECTOR, '[role="dialog"], div[aria-modal="true"], .mdc-dialog--open')
            hit = False
            for dlg in dialogs:
                if not _is_interactable(driver, dlg): continue
                for b in dlg.find_elements(By.CSS_SELECTOR, 'button,[role=button],a[role=button]'):
                    txt = ((b.text or "") + " " + (b.get_attribute("aria-label") or "")).strip().lower()
                    if txt and any(w in txt for w in CAND):
                        try: b.click()
                        except Exception:
                            try: driver.execute_script("arguments[0].click();", b)
                            except Exception: continue
                        time.sleep(0.12)
                        hit = True
                        break
                if hit: break
            if not hit: break
        except Exception:
            break

def _ensure_panel_open_by_label(driver: WebDriver, label_keys: List[str]) -> bool:
    try:
        return bool(driver.execute_script(
            """
            const KEYS = new Set(arguments[0].map(s=>String(s||'').toLowerCase()));
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
              if(cs.display==='none'||cs.visibility==='hidden'||parseFloat(cs.opacity||'1')<0.2) return false;
              return r.width>20 && r.height>20;};
            const headers=[...document.querySelectorAll('.main-header .header[role=button]')];
            for(const h of headers){
              const txt = ((h.getAttribute('aria-label')||'')+' '+(h.innerText||h.textContent||'')).trim().toLowerCase();
              if([...KEYS].some(k=> txt.includes(k))){
                const exp=(h.getAttribute('aria-expanded')||'').toLowerCase()==='true';
                if(!exp){
                  try{ h.click(); }catch(e){ try{ h.querySelector('material-icon, .expand-button')?.click(); }catch(e2){} }
                }else{
                  if(!isVis(h.closest('.panel')?.querySelector('.main'))){
                    try{ h.click(); }catch(e){}
                  }
                }
              }
            }
            return true;
            """,
            label_keys
        ))
    except Exception:
        return False


# ====== Языки и LLM ======

def _normalize_lang_code(code: str) -> str:
    c = (code or "").lower()
    if "ru" in c: return "ru"
    if "en" in c: return "en"
    return "en"

def _normalize_languages(langs: Optional[List[str]], business_name: Optional[str], usp: Optional[str]) -> List[str]:
    if langs:
        out = []
        for x in langs:
            y = _normalize_lang_code(x)
            if y not in out:
                out.append(y)
        return out or ["en"]
    if _has_cyrillic(business_name) or _has_cyrillic(usp):
        return ["ru"]
    return ["en"]

def _split_counts(total: int, langs: List[str]) -> tuple[int, int]:
    total = max(0, int(total))
    has_ru = "ru" in langs
    has_en = "en" in langs
    if has_ru and has_en:
        ru_n = (total + 1) // 2
        en_n = total - ru_n
        return ru_n, en_n
    if has_ru:
        return total, 0
    return 0, total  # только en

def _loose_json(text: str) -> Dict:
    try:
        return json.loads(text)
    except Exception:
        pass
    m = re.search(r"\{.*\}", text or "", flags=re.S)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            return {}
    return {}

def _gemini_json_text(prompt_text: str, temperature: float = 0.6, retries: int = 1) -> Dict:
    if GeminiClient is None:
        raise RuntimeError("LLM (Gemini) недоступна: отсутствует клиент или API-ключ.")
    model = os.getenv("GEMINI_MODEL", "models/gemini-2.0-flash")
    client = GeminiClient(model=model, temperature=temperature, retries=retries, fallback_model=None)
    logger.debug("LLM request (model=%s, temp=%.2f): %s", model, temperature, _limit_len(prompt_text, 280))
    raw = client.generate_json(prompt_text)
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        data = _loose_json(raw)
        if data:
            return data
    raise RuntimeError("LLM (Gemini) вернула некорректный ответ (ожидался JSON по схеме).")


# ====== Санитаризация текста под политику Google Ads ======

# Диапазоны emoji/символов, стрелок и пр.
_EMOJI_RE = re.compile(
    "["                             # объединённый класс
    "\U0001F1E6-\U0001F1FF"         # флаги
    "\U0001F300-\U0001F5FF"         # символы и пиктограммы
    "\U0001F600-\U0001F64F"         # смайлы
    "\U0001F680-\U0001F6FF"         # транспорт/символы
    "\U0001F700-\U0001F77F"
    "\U0001F780-\U0001F7FF"
    "\U0001F800-\U0001F8FF"
    "\U0001F900-\U0001F9FF"
    "\U0001FA00-\U0001FA6F"
    "\U0001FA70-\U0001FAFF"
    "\u2190-\u21FF"                 # стрелки
    "\u2600-\u27BF"                 # разное
    "]+",
    flags=re.UNICODE
)

# Точечные запрещённые/декоративные символы
_DECOR_RE = re.compile(r"[•·▪●◦□◆◇■☑☐❑❒✔✓✅✗✘✖★☆❋❉✦✧❖♥❤♡❣♦♣♠☀☁☂☔⚠️❗️❕❓️❔⬛⬜]", re.UNICODE)

# Карта нормализаций «умной» пунктуации и знаков
_PUNCT_MAP = {
    "“": "", "”": "", "„": "", "‟": "", "‹": "", "›": "", "«": "", "»": "", "’": "", "‘": "",
    "—": "-", "–": "-", "−": "-", "‑": "-", "‒": "-", "‐": "-",
    "…": ".", "·": ".", "∙": ".", "•": "-",
    "™": "", "®": "", "©": "",
    "№": "", "→": "-", "←": "-", "↔": "-", "↗": "-", "↘": "-", "➡": "-", "➜": "-", "➤": "-",
    "\u00A0": " ",  # NBSP
}

# Разрешаем: буквы/цифры, пробел, базовая пунктуация . , ; : - ( ) / % + ? !
_ALLOWED_CHARS_RE = re.compile(r"[^A-Za-zА-Яа-яЁё0-9 \.,;:\-\(\)/%\+\!\?]", re.UNICODE)

# смайлики ascii
_ASCII_EMOTICON_RE = re.compile(r"[:;=8][\-^]?[)D(\[\]Pp/\\|]+", re.UNICODE)

def _normalize_text_chars(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = "".join(_PUNCT_MAP.get(ch, ch) for ch in s)
    s = _EMOJI_RE.sub("", s)
    s = _DECOR_RE.sub("", s)
    # убираем zero-width/управляющие
    s = re.sub(r"[\u2000-\u200F\u2028\u2029\u202A-\u202E\u2060-\u206F\uFEFF]", " ", s)
    return s

def _strip_disallowed_and_fix_spaces(s: str, allow_exclaim: bool) -> str:
    if not allow_exclaim:
        s = s.replace("!", "")
    # удаляем неразрешённые символы
    s = _ALLOWED_CHARS_RE.sub("", s)
    # убираем ascii-смайлы
    s = _ASCII_EMOTICON_RE.sub("", s)
    # схлопываем повторы знаков
    s = re.sub(r"([.,;:\-()/+%])\1{1,}", r"\1", s)
    s = re.sub(r"([!?]){2,}", r"\1", s)
    # пробелы около пунктуации
    s = re.sub(r"\s+([.,;:!?])", r"\1", s)
    s = re.sub(r"([.,;:!?])([^\s])", r"\1 \2", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def _ga_sanitize_text(text: str, kind: str, max_len: int, lang: Optional[str] = None) -> str:
    """
    kind: 'headline' | 'long_headline' | 'description'
    - для заголовков: '!' запрещён полностью
    - для описаний: разрешён максимум один '!'
    """
    s = (text or "").strip()
    if not s:
        return ""
    s = _normalize_text_chars(s)
    allow_exclaim = (kind == "description")
    s = _strip_disallowed_and_fix_spaces(s, allow_exclaim=allow_exclaim)
    # для заголовков убираем финальную пунктуацию
    if kind in ("headline", "long_headline"):
        s = re.sub(r"[.,;:!?]+$", "", s).strip()
    return _limit_len(s, max_len)

# ---- генерация локализованных заголовков/описаний (ТОЛЬКО LLM, с санитаризацией)

def _gen_headlines_localized(business_name: Optional[str], usp: Optional[str], site_url: Optional[str],
                             target: int, lang: str) -> List[str]:
    target = max(0, min(HEADLINES_MAX_TOTAL, int(target or 0)))
    if target == 0:
        return []
    usp_text = (usp or "").strip()
    lang_label = "Russian" if lang == "ru" else "English"

    results: List[str] = []
    seen = set()
    attempts = 0
    while len(results) < target and attempts < 4:
        need = target - len(results)
        prompt = (
            "You are a senior ads copywriter. Generate HIGH-CONVERTING ad headlines.\n"
            f"Language: {lang_label}. Max length per item: {HEADLINE_MAX} characters.\n"
            "Constraints:\n"
            "- Benefit-first, varied wording; minimal punctuation.\n"
            "- No emojis. No ALL CAPS. No decorative symbols (arrows, stars, checkmarks).\n"
            "- Do NOT mention any brand or business name.\n"
            f"- Stay strictly in {lang_label}; do not mix languages.\n"
            f"Context/USP: {usp_text or '—'}\n\n"
            "Return ONLY valid compact JSON with this exact schema: {\"headlines\": [\"h1\", \"h2\", ...]}\n"
            f"Requested count: {need}\n"
        )
        data = _gemini_json_text(prompt, temperature=0.55, retries=1)
        arr = data.get("headlines") if isinstance(data, dict) else None
        if not isinstance(arr, list) or not arr:
            raise RuntimeError("LLM (Gemini) не вернула headlines.")
        for x in arr:
            s = _ga_sanitize_text(str(x), "headline", HEADLINE_MAX, lang)
            if not s:
                continue
            k = s.lower()
            if k not in seen:
                results.append(s)
                seen.add(k)
                if len(results) >= target:
                    break
        attempts += 1

    if len(results) < target:
        raise RuntimeError(f"LLM (Gemini) сгенерировала только {len(results)}/{target} заголовков (после очистки).")
    logger.info("LLM: сгенерировано коротких заголовков: %d (lang=%s, target=%d)", len(results), lang, target)
    return results[:target]

def _gen_long_headlines_localized(business_name: Optional[str], usp: Optional[str], site_url: Optional[str],
                                  target: int, lang: str) -> List[str]:
    target = max(0, min(LONG_HEADLINES_MAX_TOTAL, int(target or 0)))
    if target == 0:
        return []
    usp_text = (usp or "").strip()
    lang_label = "Russian" if lang == "ru" else "English"

    results: List[str] = []
    seen = set()
    attempts = 0
    while len(results) < target and attempts < 4:
        need = target - len(results)
        prompt = (
            "You are a senior ads copywriter. Write LONG AD HEADLINES (up to 90 characters).\n"
            f"Language: {lang_label}. Max length per item: {LONG_HEADLINE_MAX} characters.\n"
            "Style:\n"
            "- Benefit-first, one clear idea; light punctuation; no decorative symbols.\n"
            "- No emojis. No ALL CAPS. No brand or business names.\n"
            f"- Stay strictly in {lang_label}; do not mix languages.\n"
            f"Context/USP: {usp_text or '—'}\n\n"
            "Return ONLY valid compact JSON with this exact schema: {\"long_headlines\": [\"l1\", \"l2\", ...]}\n"
            f"Requested count: {need}\n"
        )
        data = _gemini_json_text(prompt, temperature=0.6, retries=1)
        arr = data.get("long_headlines") if isinstance(data, dict) else None
        if not isinstance(arr, list) or not arr:
            raise RuntimeError("LLM (Gemini) не вернула long_headlines.")
        for x in arr:
            s = _ga_sanitize_text(str(x), "long_headline", LONG_HEADLINE_MAX, lang)
            if not s:
                continue
            k = s.lower()
            if k not in seen:
                results.append(s)
                seen.add(k)
                if len(results) >= target:
                    break
        attempts += 1

    if len(results) < target:
        raise RuntimeError(f"LLM (Gemini) сгенерировала только {len(results)}/{target} длинных заголовков (после очистки).")
    logger.info("LLM: сгенерировано длинных заголовков: %d (lang=%s, target=%d)", len(results), lang, target)
    return results[:target]

def _gen_descriptions_localized(business_name: Optional[str], usp: Optional[str], site_url: Optional[str],
                                target: int, lang: str) -> List[str]:
    target = max(0, min(DESCRIPTIONS_MAX_TOTAL, int(target or 0)))
    if target == 0:
        return []
    usp_text = (usp or "").strip()
    lang_label = "Russian" if lang == "ru" else "English"

    results: List[str] = []
    seen = set()
    attempts = 0
    while len(results) < target and attempts < 4:
        need = target - len(results)
        prompt = (
            "You are a senior ads copywriter. Write concise ad DESCRIPTIONS.\n"
            f"Language: {lang_label}. Max length per item: {DESC_MAX} characters.\n"
            "Structure: benefit → proof/feature → clear CTA.\n"
            "Constraints:\n"
            "- Basic punctuation only. No emojis or decorative symbols.\n"
            "- Do NOT mention any brand or business name.\n"
            f"- Stay strictly in {lang_label}; do not mix languages.\n"
            f"Context/USP: {usp_text or '—'}\n\n"
            "Return ONLY valid compact JSON with this exact schema: {\"descriptions\": [\"d1\", \"d2\", ...]}\n"
            f"Requested count: {need}\n"
        )
        data = _gemini_json_text(prompt, temperature=0.6, retries=1)
        arr = data.get("descriptions") if isinstance(data, dict) else None
        if not isinstance(arr, list) or not arr:
            raise RuntimeError("LLM (Gemini) не вернула descriptions.")
        for x in arr:
            s = _ga_sanitize_text(str(x), "description", DESC_MAX, lang)
            if not s:
                continue
            k = s.lower()
            if k not in seen:
                results.append(s)
                seen.add(k)
                if len(results) >= target:
                    break
        attempts += 1

    if len(results) < target:
        raise RuntimeError(f"LLM (Gemini) сгенерировала только {len(results)}/{target} описаний (после очистки).")
    logger.info("LLM: сгенерировано описаний: %d (lang=%s, target=%d)", len(results), lang, target)
    return results[:target]


# ====== Изображения: динамическая NEG, разнообразие, дедупликация ======

def _has_tech_semantics(*parts: Optional[str]) -> bool:
    text = " ".join(p or "" for p in parts).lower()
    kws = [
        "ai","ml","machine learning","neural","neuron","deep","model",
        "data","analytics","cloud","saas","software","devops","kubernetes","server",
        "datacenter","database","sql","it","digital","code","algorithm","pipeline"
    ]
    return any(k in text for k in kws)

def _build_negative_clause(usp: Optional[str], user_prompt: Optional[str], site_url: Optional[str]) -> str:
    neg = RUNWARE_NEG_BASE
    if not _has_tech_semantics(usp, user_prompt, site_url):
        extra = ", no server racks, no circuit boards, no AI brain imagery, no neural connections, no code, no holograms, no futuristic digital backgrounds"
        neg = f"{neg}{extra}"
    return neg

def _orientation_hint(orient: str) -> str:
    return (
        "Landscape orientation (wide ~1.8–1.9:1), horizontal framing, subject optimized for wide."
        if (orient or "").lower().startswith("land")
        else "Square 1:1 aspect ratio, centered clean composition."
    )

def _decide_orientation_mix(n: int) -> List[str]:
    """Возвращает массив 'landscape'/'square' длиной n (минимум 1 landscape)."""
    n = max(1, int(n))
    if n == 1:
        return ["landscape"]
    if n == 5:
        pattern = random.choice(["1L4S", "2L3S", "3L2S"])
        if pattern == "1L4S":
            arr = ["landscape"] * 1 + ["square"] * 4
        elif pattern == "2L3S":
            arr = ["landscape"] * 2 + ["square"] * 3
        else:
            arr = ["landscape"] * 3 + ["square"] * 2
        random.shuffle(arr)
        return arr
    candidates = [1]
    if n >= 2: candidates.append(2)
    if n >= 3: candidates.append(3)
    if n >= 4: candidates.append(max(1, n // 2))
    c_land = max(1, min(n, int(random.choice(candidates))))
    arr = ["landscape"] * c_land + ["square"] * (n - c_land)
    random.shuffle(arr)
    return arr

def _dims_for_orientation(orient: str) -> Tuple[int, int]:
    if (orient or "").lower().startswith("land"):
        w, h = random.choice(LANDSCAPE_SIZES)
    else:
        w, h = random.choice(SQUARE_CHOICES)
    return _ensure_runware_dims(w, h)

def _apply_orientation_to_prompt(base_prompt: str, orient: str, negative_clause: str) -> str:
    """Вставляет ориентационный хинт перед NEG (NEG остаётся в конце)."""
    p = (base_prompt or "").strip()
    neg_lower = negative_clause.lower().strip()
    hint = _orientation_hint(orient)
    if neg_lower in p.lower():
        idx = p.lower().rfind(neg_lower)
        main = p[:idx].rstrip().rstrip(".")
        tail = p[idx:]
        composed = f"{main}. {hint} {tail}"
    else:
        composed = f"{p}. {hint}. {negative_clause}"
    return _limit_len(_strip_quotes(composed), 480)

# --- похожесть и диверсификация ---

_STOPWORDS: Set[str] = {
    "a","an","the","and","or","of","for","to","with","on","in","by","from","at","into","over","under",
    "up","down","near","this","that","these","those","is","are","be","as","it","its","their","your",
    "our","his","her","them","they","you","we","i","no","not","without","clean","modern","minimal",
    "product","service","ad","ads","image","photo","photographic","photorealistic","realistic","look",
    "style","scene","shot","frame","background","composition","subject"
}

def _norm_tokens(s: str) -> Set[str]:
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    toks = [t for t in s.split() if t and len(t) > 2 and t not in _STOPWORDS]
    return set(toks)

def _jaccard(a: Set[str], b: Set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    if inter == 0:
        return 0.0
    return inter / float(len(a | b))

def _find_similar_pairs(prompts: List[str], thr: float = DIVERSITY_SIM_THR_DEFAULT) -> List[Tuple[int,int,float]]:
    pairs: List[Tuple[int,int,float]] = []
    mats = [ _norm_tokens(p) for p in prompts ]
    n = len(prompts)
    for i in range(n):
        for j in range(i+1, n):
            sim = _jaccard(mats[i], mats[j])
            if sim >= thr:
                pairs.append((i, j, sim))
    return pairs

def _llm_diversify(prompts_to_fix: List[str], context: str, negative_clause: str, want: int) -> List[str]:
    """Переписывает конфликтные промпты на принципиально иные сцены."""
    if not prompts_to_fix:
        return []
    joined = "\\n".join(f"- {p}" for p in prompts_to_fix)
    prompt = (
        "You are an ads art director. Rewrite each prompt BELOW into a DISTINCT concept so that none of them overlap in nouns/adjectives/scene logic.\n"
        "Keep photorealism. Each ≤ 300 chars BEFORE the negative clause.\n"
        "For each rewritten prompt, end with this EXACT negative clause: "
        f"'{negative_clause}'.\n"
        "Vary angles (top-down/eye-level/45°), camera distance (macro/close/medium/wide), lighting (daylight/golden hour/studio), "
        "setting (studio/lifestyle/outdoor/in-use), human presence (hands/people vs none), static vs action.\n"
        "Do NOT mention brand names or put any visible text.\n"
        f"Context/USP: {context or '—'}\n\n"
        "INPUT PROMPTS:\n"
        f"{joined}\n\n"
        "Return JSON with schema: {\"prompts\": [\"p1\", \"p2\", ...]} with exactly the same count as input."
    )
    data = _gemini_json_text(prompt, temperature=0.8, retries=1)
    arr = data.get("prompts") if isinstance(data, dict) else None
    out: List[str] = []
    if isinstance(arr, list):
        for p in arr:
            s = _limit_len(_strip_quotes(str(p)), 360)
            if negative_clause.lower() not in s.lower():
                s = _limit_len(f"{s}. {negative_clause}", 480)
            out.append(s)
    return out[:want]

def _ensure_unique_prompts(prompts: List[str],
                           usp: Optional[str],
                           user_prompt: Optional[str],
                           negative_clause: str,
                           *,
                           thr: float = DIVERSITY_SIM_THR_DEFAULT,
                           max_passes: int = 2) -> List[str]:
    """До max_passes проходов: поиск похожих → ремикс LLM только конфликтных."""
    out = prompts[:]
    for _pass in range(max_passes):
        pairs = _find_similar_pairs(out, thr=thr)
        if not pairs:
            break
        idxs: Set[int] = set()
        for i, j, _ in pairs:
            idxs.add(i); idxs.add(j)
        to_fix = [out[k] for k in sorted(idxs)]
        context = " | ".join(filter(None, [usp or "", user_prompt or ""]))
        rewrites = _llm_diversify(to_fix, context, negative_clause, want=len(to_fix))
        if len(rewrites) == len(to_fix):
            for n, k in enumerate(sorted(idxs)):
                out[k] = rewrites[n]
        else:
            logger.warning("LLM diversify returned %d of %d; keep originals for missing.", len(rewrites), len(to_fix))
            for n, k in enumerate(sorted(idxs)):
                if n < len(rewrites):
                    out[k] = rewrites[n]
    return out

def _llm_generate_prompts_batch(count: int,
                                context: str,
                                negative_clause: str,
                                brand_hint: str,
                                variation_cues: List[str]) -> List[str]:
    """Один батч генерации промптов под заданные variation_cues (ровно count)."""
    prompt = (
        "You are an ad art director + prop stylist.\n"
        "Task: create COMPLETELY UNIQUE photorealistic AD IMAGE prompts tailored to the USP/domain.\n"
        "Rules:\n"
        "- English only. No quotes. 1 prompt per list item.\n"
        "- Each prompt ≤ 300 chars BEFORE the negative clause.\n"
        "- Each MUST specify: specific subject, micro-action, environment, props (category cues), mood, lighting, camera distance & angle, composition, background cleanliness, color palette inspired by brand name ONLY as abstract vibes.\n"
        "- STRICT: Do NOT repeat key nouns/adjectives across prompts; vary shot type (macro/close/medium/wide), angle (top-down/eye-level/45°), setting (studio/lifestyle/outdoor), time of day, human presence (hands/people vs none), static vs action.\n"
        "- End EVERY prompt with this EXACT negative clause: "
        f"'{negative_clause}'.\n"
        "- Never include visible text/logos in scene. No brand names in prompt.\n"
        "- Avoid generic tech metaphors (servers/brain/circuits/holograms/code) UNLESS they are literally core to the USP text.\n\n"
        f"Context/USP: {context or '—'}\n"
        f"Brand inspiration word (no logo/text rendering): {brand_hint}\n"
        f"Variation cues to cover (1 per prompt, in order): {', '.join(variation_cues)}\n\n"
        "Return JSON ONLY: {\"prompts\": [\"p1\", \"p2\", ...]} with exactly the requested count."
    )
    data = _gemini_json_text(prompt, temperature=0.85, retries=1)
    arr = data.get("prompts") if isinstance(data, dict) else None
    results: List[str] = []
    if isinstance(arr, list):
        for p in arr:
            s = _limit_len(_strip_quotes(str(p)), 360)
            if negative_clause.lower() not in s.lower():
                s = _limit_len(f"{s}. {negative_clause}", 480)
            results.append(s)
    return results[:count]

def _llm_topup_unique(existing: List[str],
                      need: int,
                      context: str,
                      negative_clause: str) -> List[str]:
    """
    Дозапрашивает недостающие промпты, избегая пересечений с existing.
    """
    if need <= 0:
        return []
    joined_exist = "\\n".join(f"- {p}" for p in existing[:24])  # достаточно контекста
    prompt = (
        "You already have a set of ad image prompts listed below. "
        "Generate NEW prompts that are DISTINCT in nouns/adjectives/scene logic (no overlaps with listed ones).\n"
        "Keep photorealism, ≤ 300 chars BEFORE the negative clause, 1 per item.\n"
        f"End every prompt with this EXACT negative clause: '{negative_clause}'.\n"
        "Vary angle, camera distance, lighting, setting, human presence, motion.\n"
        "Do not include any visible text/logos or brand names.\n"
        f"Context/USP: {context or '—'}\n\n"
        "ALREADY HAVE:\n"
        f"{joined_exist}\n\n"
        "Return JSON: {\"prompts\": [\"p1\", \"p2\", ...]} with EXACTLY the requested count."
    )
    data = _gemini_json_text(prompt, temperature=0.9, retries=1)
    arr = data.get("prompts") if isinstance(data, dict) else None
    out: List[str] = []
    if isinstance(arr, list):
        for p in arr:
            s = _limit_len(_strip_quotes(str(p)), 360)
            if negative_clause.lower() not in s.lower():
                s = _limit_len(f"{s}. {negative_clause}", 480)
            out.append(s)
    return out[:need]

def _gen_image_prompts_diverse(n: int,
                               business_name: Optional[str],
                               usp: Optional[str],
                               site_url: Optional[str],
                               user_prompt: Optional[str],
                               negative_clause: str) -> List[str]:
    """
    Генерация РОВНО n уникальных промптов (ТОЛЬКО LLM), без тематических пресетов.
    Разнообразие сцены зашивается в инструкцию LLM.
    Воздерживаемся от падения: выполняем до IMAGE_PROMPTS_MAX_ROUNDS дозапросов у LLM.
    """
    n = max(1, int(n))
    brand_hint = _domain_to_brand(site_url or "")
    context = " | ".join(filter(None, [business_name or "", usp or "", site_url or "", user_prompt or ""]))

    # Пул вариативных подсказок — перемешаем и будем вынимать партиями.
    base_grid = [
        "macro 100mm lens detail",
        "close-up 50mm hands-in-use",
        "medium shot lifestyle with person",
        "wide shot environment/context",
        "top-down flat-lay on neutral background",
        "45-degree angle product hero on seamless",
        "golden hour warm light",
        "cool daylight 5600K studio softbox",
        "motion/action freeze, dynamic moment",
        "minimal packshot with long soft shadow",
        "storefront/exterior or workspace context",
        "ingredients/tools spread composition",
        "moody low-key studio rim light",
        "bright high-key studio light",
        "outdoor candid street vibe"
    ]
    random.shuffle(base_grid)

    results: List[str] = []
    rounds = 0
    while len(results) < n and rounds < IMAGE_PROMPTS_MAX_ROUNDS:
        missing = n - len(results)
        # Берём cues из пула, при нехватке — добавим синтетические
        cues: List[str] = []
        while len(cues) < missing and base_grid:
            cues.append(base_grid.pop())
        while len(cues) < missing:
            cues.append(f"unique variation {uuid.uuid4().hex[:6]}")

        # 1) Пробуем сгенерировать батч недостающих
        batch = _llm_generate_prompts_batch(missing, context, negative_clause, brand_hint, cues)
        if len(batch) < missing:
            logger.warning("LLM вернула %d/%d промптов; дозапрошу недостающее", len(batch), missing)
            # 1b) Дозапрос top-up на оставшиеся (избегая уже имеющихся и batch)
            tmp_all = results + batch
            need2 = missing - len(batch)
            more = _llm_topup_unique(tmp_all, need2, context, negative_clause)
            batch.extend(more)

        # 2) Сшиваем, санитизируем повторы точные
        seen_low = {s.lower().strip(): True for s in results}
        merged: List[str] = []
        for s in batch:
            key = s.lower().strip()
            if key and key not in seen_low:
                merged.append(s)
                seen_low[key] = True

        if merged:
            results.extend(merged)

        # 3) Прогоняем лёгкую диверсификацию для похожих
        results = _ensure_unique_prompts(results, usp, user_prompt, negative_clause,
                                         thr=DIVERSITY_SIM_THR_DEFAULT, max_passes=1)
        rounds += 1

    # Последний шанс: если ещё не хватило — делаем ремикс части имеющихся, чтобы добрать N
    if len(results) < n and results:
        need = n - len(results)
        sample = results[:min(len(results), need)]
        rew = _llm_diversify(sample, context, negative_clause, want=len(sample))
        # возможно, LLM вернёт меньше — это ок
        for s in rew:
            key = s.lower().strip()
            if key and key not in {x.lower().strip() for x in results}:
                results.append(s)

        # Ещё один мягкий прогон диверсификации
        results = _ensure_unique_prompts(results, usp, user_prompt, negative_clause,
                                         thr=DIVERSITY_SIM_THR_RELAX, max_passes=1)

    if len(results) < n:
        # Крайний случай — всё равно поднимем исключение с подробной диагностикой.
        raise RuntimeError(f"LLM (Gemini) сгенерировала только {len(results)}/{n} уникальных промптов изображений (после {rounds} раундов).")

    return results[:n]


# ====== Runware генерация изображений ======

@dataclass
class RunwareConfig:
    api_key: str = DEFAULT_RUNWARE_API_KEY
    model_id: str = DEFAULT_RUNWARE_MODEL_ID
    base_url: str = DEFAULT_RUNWARE_URL

def _pick_runware_config() -> RunwareConfig:
    cfg = RunwareConfig(
        api_key=DEFAULT_RUNWARE_API_KEY.strip(),
        model_id=(DEFAULT_RUNWARE_MODEL_ID or "runware:100@1").strip(),
        base_url=(DEFAULT_RUNWARE_URL or "https://api.runware.ai/v1").strip(),
    )
    logger.debug("Runware cfg: base=%s model=%s", cfg.base_url, cfg.model_id)
    return cfg

def _snap_dim(v: int) -> int:
    """Приводит размер к диапазону 128..2048 и кратности 64."""
    try:
        v = int(v)
    except Exception:
        v = 512
    v = max(128, min(2048, v))
    rem = v % 64
    if rem:
        v = v - rem
        if v < 128:
            v = 128
    return v

def _ensure_runware_dims(w: int, h: int) -> Tuple[int, int]:
    sw, sh = _snap_dim(w), _snap_dim(h)
    return sw, sh

def _runware_generate_image(prompt: str, cfg: RunwareConfig,
                            width: int, height: int,
                            retries: int = RUNWARE_RETRIES, timeout: int = 180) -> bytes:
    """POST → imageURL → bytes."""
    width, height = _ensure_runware_dims(width, height)
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {cfg.api_key}"}
    payload = [{
        "taskType": "imageInference",
        "taskUUID": str(uuid.uuid4()),
        "positivePrompt": prompt,
        "model": cfg.model_id,
        "numberResults": 1,
        "height": int(height),
        "width": int(width),
        "outputType": "URL",
        "outputFormat": "JPEG",
    }]

    backoff = 1.0
    errors: List[str] = []
    for attempt in range(1, retries + 1):
        try:
            logger.info("Runware: запрос %d/%d (%dx%d)", attempt, retries, width, height)
            r = requests.post(cfg.base_url, json=payload, headers=headers, timeout=timeout)
            if r.status_code >= 400:
                msg = r.text[:300].replace("\n", " ")
                errors.append(f"HTTP {r.status_code}: {msg}")
                raise RuntimeError(errors[-1])
            j = r.json()
            data = j.get("data")
            if not isinstance(data, list) or not data or not isinstance(data[0], dict):
                errors.append(f"unexpected json: {json.dumps(j)[:200]}")
                raise RuntimeError(errors[-1])
            img_url = data[0].get("imageURL") or data[0].get("url") or data[0].get("imageUrl")
            if not img_url or not isinstance(img_url, str):
                errors.append(f"no imageURL in response: {json.dumps(data[0])[:160]}")
                raise RuntimeError(errors[-1])
            r2 = requests.get(img_url, timeout=timeout)
            if not r2.ok or not r2.content:
                errors.append(f"fetch image failed: {r2.status_code}")
                raise RuntimeError(errors[-1])
            logger.debug("Runware: изображение получено (%d bytes)", len(r2.content or b""))
            return r2.content
        except Exception as e:
            if attempt >= retries:
                logger.error("Runware: исчерпаны попытки — %s", " | ".join(errors[-5:] or [str(e)]))
                raise RuntimeError("Runware generation failed: " + " | ".join(errors[-5:] or [str(e)]))
            jitter = random.uniform(0.0, 0.5)
            logger.warning("Runware issue (%s), retry %d/%d in %.1fs", e, attempt, retries, backoff + jitter)
            time.sleep(backoff + jitter)
            backoff = min(backoff * 2, 8.0)

# --- (историческая запись во временный файл) ---
def _write_image_file(raw: bytes, suffix: str = ".jpg") -> str:
    fd, path = tempfile.mkstemp(prefix="adsai_img_", suffix=suffix)
    os.close(fd)
    try:
        if Image is not None:
            im = Image.open(io.BytesIO(raw))
            if im.mode in ("RGBA", "LA") or (im.mode == "P" and "transparency" in im.info):
                bg = Image.new("RGB", im.size, (255, 255, 255))
                bg.paste(im, mask=im.split()[-1])
                im = bg
            elif im.mode != "RGB":
                im = im.convert("RGB")
            w, h = im.size
            scale = 1.0
            if max(w, h) > MAX_LONG_EDGE:
                scale = MAX_LONG_EDGE / float(max(w, h))
            if scale < 1.0:
                im = im.resize((int(w * scale), int(h * scale)))
            im.save(path, format="JPEG", quality=JPEG_Q, optimize=True, progressive=True)
            return path
    except Exception:
        pass
    with open(path, "wb") as f:
        f.write(raw)
    return path


# ====== Сохранение изображений в целевой каталог ======

_RU_TO_LAT = {
    "а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"e","ж":"zh","з":"z","и":"i",
    "й":"y","к":"k","л":"l","м":"m","н":"n","о":"o","п":"p","р":"r","с":"s","т":"t",
    "у":"u","ф":"f","х":"h","ц":"c","ч":"ch","ш":"sh","щ":"sch","ъ":"","ы":"y","ь":"",
    "э":"e","ю":"yu","я":"ya"
}

def _slugify(text: str) -> str:
    s = (text or "").strip().lower()
    out: List[str] = []
    for ch in s:
        if ch in _RU_TO_LAT:
            out.append(_RU_TO_LAT[ch])
        elif "a" <= ch <= "z" or "0" <= ch <= "9":
            out.append(ch)
        elif ch in (" ", ".", "_", "-"):
            out.append("-")
        else:
            out.append("-")
    slug = re.sub(r"-{2,}", "-", "".join(out)).strip("-")
    return slug

def _short_uuid() -> str:
    return uuid.uuid4().hex[:8]

def _compute_images_dir(business_name: Optional[str], site_url: Optional[str]) -> str:
    """
    Итоговый путь: <BASE>/<FOLDER>, где:
      BASE   = ENV ADS_AI_IMAGES_BASE | ./companies/images
      FOLDER = slug(business_name) | slug(domain(site_url)) | ENV ADS_AI_COMPANY_ID | id-<uuid8>
    """
    base = (os.getenv("ADS_AI_IMAGES_BASE") or "").strip() or str(Path.cwd() / "companies" / "images")
    env_id = (os.getenv("ADS_AI_COMPANY_ID") or "").strip()
    if business_name:
        folder = _slugify(business_name)
    elif site_url:
        try:
            host = urlparse(site_url).netloc or site_url
            host = re.sub(r"^www\.", "", host)
        except Exception:
            host = site_url
        folder = _slugify(host)
    elif env_id:
        folder = _slugify(env_id)
    else:
        folder = f"id-{_short_uuid()}"
    if not folder:
        folder = f"id-{_short_uuid()}"
    path = Path(base) / folder
    path.mkdir(parents=True, exist_ok=True)
    return str(path.resolve())

def _safe_filename(stem: str) -> str:
    s = _slugify(stem)
    return s or f"img-{_short_uuid()}"

def _save_image_file(raw: bytes, dest_dir: str, base_stem: str, suffix: str = ".jpg") -> str:
    """
    Сохраняет bytes → JPEG в dest_dir с компрессией/ресайзом.
    Имя файла уникализируется при коллизии.
    Возвращает абсолютный путь.
    """
    Path(dest_dir).mkdir(parents=True, exist_ok=True)
    stem = _safe_filename(base_stem)
    candidate = Path(dest_dir) / f"{stem}{suffix}"
    idx = 2
    while candidate.exists():
        candidate = Path(dest_dir) / f"{stem}-{idx:02d}{suffix}"
        idx += 1

    path = str(candidate.resolve())
    try:
        if Image is not None:
            im = Image.open(io.BytesIO(raw))
            if im.mode in ("RGBA", "LA") or (im.mode == "P" and "transparency" in im.info):
                bg = Image.new("RGB", im.size, (255, 255, 255))
                bg.paste(im, mask=im.split()[-1])
                im = bg
            elif im.mode != "RGB":
                im = im.convert("RGB")
            w, h = im.size
            scale = 1.0
            if max(w, h) > MAX_LONG_EDGE:
                scale = MAX_LONG_EDGE / float(max(w, h))
            if scale < 1.0:
                im = im.resize((int(w * scale), int(h * scale)))
            im.save(path, format="JPEG", quality=JPEG_Q, optimize=True, progressive=True)
            return path
    except Exception:
        pass

    with open(path, "wb") as f:
        f.write(raw)
    return path


# ====== Поиск и ввод для Headline/Long headline/Description ======

def _find_headline_inputs(driver: WebDriver) -> List[WebElement]:
    try:
        els = driver.find_elements(By.CSS_SELECTOR, 'multi-text-input.headlines input.input')
        return [e for e in els if _is_interactable(driver, e)]
    except Exception:
        return []

def _find_long_headline_inputs(driver: WebDriver) -> List[WebElement]:
    try:
        els = driver.find_elements(By.CSS_SELECTOR, 'multi-text-input.long-headlines input.input')
        return [e for e in els if _is_interactable(driver, e)]
    except Exception:
        return []

def _find_description_inputs(driver: WebDriver) -> List[WebElement]:
    try:
        els = driver.find_elements(By.CSS_SELECTOR, 'multi-text-input.descriptions input.input')
        return [e for e in els if _is_interactable(driver, e)]
    except Exception:
        return []

def _click_add_headline(driver: WebDriver) -> None:
    try:
        btn = driver.execute_script(
            """
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
              if(cs.display==='none'||cs.visibility==='hidden'||parseFloat(cs.opacity||'1')<0.2) return false;
              return r.width>10 && r.height>10 && r.right>0 && r.bottom>0;};
            return [...document.querySelectorAll('multi-text-input.headlines material-button.button')]
                .find(b=>isVis(b) && ((b.getAttribute('aria-label')||'').toLowerCase().includes('add headline')
                    || (b.innerText||'').toLowerCase().includes('headline')
                    || (b.innerText||'').toLowerCase().includes('заголовок'))) || null;
            """
        )
        if btn: _robust_click(driver, btn)  # type: ignore
    except Exception:
        pass

def _click_add_long_headline(driver: WebDriver) -> None:
    try:
        btn = driver.execute_script(
            """
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
              if(cs.display==='none'||cs.visibility==='hidden'||parseFloat(cs.opacity||'1')<0.2) return false;
              return r.width>10 && r.height>10 && r.right>0 && r.bottom>0;};
            return [...document.querySelectorAll('multi-text-input.long-headlines material-button.button')]
                .find(b=>isVis(b) && (
                    (b.getAttribute('aria-label')||'').toLowerCase().includes('add long headline')
                    || (b.getInnerText?.()||b.innerText||'').toLowerCase().includes('long headline')
                    || (b.innerText||'').toLowerCase().includes('длинный')
                )) || null;
            """
        )
        if btn: _robust_click(driver, btn)  # type: ignore
    except Exception:
        pass

def _click_add_description(driver: WebDriver) -> None:
    try:
        btn = driver.execute_script(
            """
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
              if(cs.display==='none'||cs.visibility==='hidden'||parseFloat(cs.opacity||'1')<0.2) return false;
              return r.width>10 && r.height>10 && r.right>0 && r.bottom>0;};
            return [...document.querySelectorAll('multi-text-input.descriptions material-button.button')]
                .find(b=>isVis(b) && ((b.getAttribute('aria-label')||'').toLowerCase().includes('add description')
                    || (b.innerText||'').toLowerCase().includes('description')
                    || (b.innerText||'').toLowerCase().includes('описание'))) || null;
            """
        )
        if btn: _robust_click(driver, btn)  # type: ignore
    except Exception:
        pass

def _fill_headlines(driver: WebDriver, texts: List[str], max_total: int, run_id: str) -> int:
    filled = 0
    target = min(max_total, len(texts))
    tries = 0
    logger.info("[run=%s] Ввод коротких заголовков: target=%d", run_id, target)
    while filled < target and tries < 120:
        tries += 1
        if tries % 10 == 0:
            logger.debug("[run=%s] headlines: try=%d filled=%d/%d", run_id, tries, filled, target)
        inputs = _find_headline_inputs(driver)
        target_input = None
        for inp in inputs:
            val = (inp.get_attribute("value") or "").strip()
            if not val:
                target_input = inp
                break
        if not target_input:
            _click_add_headline(driver)
            time.sleep(0.2)
            continue
        txt = _limit_len(texts[filled], HEADLINE_MAX)
        _dispatch_input_change(driver, target_input, txt)
        time.sleep(0.15)
        filled += 1
    if filled < target:
        logger.warning("[run=%s] headlines: не удалось заполнить все (%d/%d)", run_id, filled, target)
    else:
        logger.info("[run=%s] headlines: заполнено %d/%d", run_id, filled, target)
    return filled

def _fill_long_headlines(driver: WebDriver, texts: List[str], max_total: int, run_id: str) -> int:
    filled = 0
    target = min(max_total, len(texts))
    tries = 0
    logger.info("[run=%s] Ввод длинных заголовков: target=%d", run_id, target)
    while filled < target and tries < 80:
        tries += 1
        if tries % 10 == 0:
            logger.debug("[run=%s] long_headlines: try=%d filled=%d/%d", run_id, tries, filled, target)
        inputs = _find_long_headline_inputs(driver)
        target_input = None
        for inp in inputs:
            val = (inp.get_attribute("value") or "").strip()
            if not val:
                target_input = inp
                break
        if not target_input:
            _click_add_long_headline(driver)
            time.sleep(0.2)
            continue
        txt = _limit_len(texts[filled], LONG_HEADLINE_MAX)
        _dispatch_input_change(driver, target_input, txt)
        time.sleep(0.15)
        filled += 1
    if filled < target:
        logger.warning("[run=%s] long_headlines: не удалось заполнить все (%d/%d)", run_id, filled, target)
    else:
        logger.info("[run=%s] long_headlines: заполнено %d/%d", run_id, filled, target)
    return filled

def _fill_descriptions(driver: WebDriver, texts: List[str], max_total: int, run_id: str) -> int:
    filled = 0
    target = min(max_total, len(texts))
    tries = 0
    logger.info("[run=%s] Ввод описаний: target=%d", run_id, target)
    while filled < target and tries < 80:
        tries += 1
        if tries % 10 == 0:
            logger.debug("[run=%s] descriptions: try=%d filled=%d/%d", run_id, tries, filled, target)
        inputs = _find_description_inputs(driver)
        target_input = None
        for inp in inputs:
            val = (inp.get_attribute("value") or "").strip()
            if not val:
                target_input = inp
                break
        if not target_input:
            _click_add_description(driver)
            time.sleep(0.2)
            continue
        txt = _limit_len(texts[filled], DESC_MAX)
        _dispatch_input_change(driver, target_input, txt)
        time.sleep(0.15)
        filled += 1
    if filled < target:
        logger.warning("[run=%s] descriptions: не удалось заполнить все (%d/%d)", run_id, filled, target)
    else:
        logger.info("[run=%s] descriptions: заполнено %d/%d", run_id, filled, target)
    return filled


# ====== Диалог Images: открыть, Upload, загрузить, Save ======

def _open_images_uploader(driver: WebDriver) -> bool:
    try:
        btn = driver.execute_script(
            """
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
              if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2) return false;
              return r.width>10 && r.height>10 && r.right>0 && r.bottom>0;};
            return [...document.querySelectorAll('[navi-id="add-media-button"], [aria-label*="Add images"], [aria-label*="Add image"]')]
                .find(isVis) || null;
            """
        )
        if btn: return _robust_click(driver, btn)  # type: ignore
    except Exception:
        pass
    return False

def _get_media_dialog_root(driver: WebDriver) -> Optional[WebElement]:
    try:
        el = driver.execute_script(
            """
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
              if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2) return false;
              return r.width>300 && r.height>200 && r.right>0 && r.bottom>0;};
            const dialogs=[...document.querySelectorAll('[role=dialog],[aria-modal=true],slidedialog-wrapper,[data-test-id*=dialog]')].filter(isVis);
            for(const d of dialogs){
              const t = ((d.getAttribute('aria-label')||'') + ' ' + (d.innerText||'') + ' ' + (d.querySelector('h2,h3,.title')?.innerText||'')).toLowerCase();
              if(t.includes('image') || t.includes('images') || t.includes('media') || t.includes('изображ')) return d;
            }
            return dialogs[0] || null;
            """
        )
        return el
    except Exception:
        return None

def _wait_media_picker(driver: WebDriver, timeout: float = 22.0, run_id: str = "") -> Optional[WebElement]:
    end = time.time() + timeout
    i = 0
    while time.time() < end:
        i += 1
        root = _get_media_dialog_root(driver)
        if root:
            logger.info("[run=%s] Диалог изображений найден (%.1fs)", run_id, timeout - (end - time.time()))
            return root
        if i % 8 == 0:
            logger.debug("[run=%s] Жду диалог изображений… (%ds/%ds)", run_id, int(i*0.25), int(timeout))
        time.sleep(0.25)
    logger.warning("[run=%s] Диалог изображений не появился за %.1fs", run_id, timeout)
    return None

def _select_tab_in_dialog(driver: WebDriver, dialog: WebElement, tab_labels: List[str]) -> bool:
    try:
        return bool(driver.execute_script(
            """
            const dlg=arguments[0], LABELS=new Set(arguments[1].map(s=>String(s||'').toLowerCase()));
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
              if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2) return false;
              return r.width>10 && r.height>10 && r.right>0 && r.bottom>0;};
            const tabs=[...dlg.querySelectorAll('[role=tab],tab-button,material-tab,button')].filter(isVis);
            for(const t of tabs){
              const txt=((t.getAttribute('aria-label')||'')+' '+(t.innerText||t.textContent||'')).trim().toLowerCase();
              if([...LABELS].some(k=>txt.includes(k))){
                try{ t.click(); }catch(e){ try{ t.querySelector('span, .tab-title')?.click(); }catch(e2){} }
                return true;
              }
            }
            return false;
            """,
            dialog, tab_labels
        ))
    except Exception:
        return False

def _ensure_file_input_visible(driver: WebDriver, dialog: WebElement) -> Optional[WebElement]:
    """Сделать существующий input[type=file] пригодным для send_keys (без системного окна)."""
    try:
        el = driver.execute_script(
            """
            const dlg=arguments[0];
            const inp = dlg.querySelector('input[type="file"]');
            if(!inp) return null;
            const st=inp.style; st.display='block'; st.visibility='visible'; st.opacity=1;
            st.position='fixed'; st.left='0'; st.top='0'; st.width='1px'; st.height='1px'; st.zIndex=2147483647;
            return inp;
            """,
            dialog
        )
        return el
    except Exception:
        return None

def _wait_images_then_click_save(driver: WebDriver, dialog: WebElement,
                                 min_wait_s: float, extra_timeout_s: float = 180.0, run_id: str = "") -> bool:
    """Фикс-пауза → ожидание активной Save → клик."""
    logger.info("[run=%s] Жду обработку загруженных изображений (%.1fs + до %.1fs)", run_id, min_wait_s, extra_timeout_s)
    time.sleep(max(0.0, min_wait_s))
    end = time.time() + max(extra_timeout_s, 1.0)
    it = 0
    while time.time() < end:
        it += 1
        try:
            save_btn = driver.execute_script(
                """
                const dlg=arguments[0];
                const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
                  if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2) return false;
                  return r.width>10 && r.height>10 && r.right>0 && r.bottom>0;};
                const cand=[...dlg.querySelectorAll('material-button.confirm-button, [data-test-id="confirm-button"], button, [role=button]')]
                    .find(b=>{
                        if(!isVis(b)) return false;
                        const t=((b.innerText||b.textContent||'')+' '+(b.getAttribute('aria-label')||'')).trim().toLowerCase();
                        const disabled = b.hasAttribute('disabled') || b.classList.contains('is-disabled')
                                      || (b.getAttribute('aria-disabled')||'false')==='true';
                        return !disabled && (t==='save' || t.includes('save') || t=='сохранить' || t.includes('сохранить'));
                    });
                return cand||null;
                """,
                dialog
            )
            if save_btn and _robust_click(driver, save_btn):  # type: ignore
                logger.info("[run=%s] Нажал Save — подтверждаю выбор изображений", run_id)
                return True
        except Exception:
            pass
        if it % 16 == 0:
            logger.debug("[run=%s] … жду активной Save (%ds осталось)", run_id, int(end - time.time()))
        time.sleep(0.25)
    logger.warning("[run=%s] Кнопка Save так и не активировалась", run_id)
    return False


# ====== Основной шаг ======

def run_step7(
    driver: WebDriver,
    *,
    n_ads: int,
    business_name: Optional[str] = None,
    usp: Optional[str] = None,
    site_url: Optional[str] = None,
    languages: Optional[List[str]] = None,
    image_prompt: Optional[str] = None,
    timeout_total: float = 300.0,
    emit: Optional[Callable[[str], None]] = None,
) -> Dict[str, object]:
    run_id = _mk_run_id()
    t0 = time.time()
    logger.info("[run=%s] === START step7 (n_ads=%s, bn=%r, site=%r) ===", run_id, n_ads, business_name, site_url)

    with _stage("Dismiss soft dialogs", run_id, budget_ms=900):
        _dismiss_soft_dialogs(driver, budget_ms=900)

    _emit(emit, "Генерирую тексты и изображения для группы ассетов")

    if not site_url:
        site_url = _extract_site_from_current_url(driver)
        if site_url:
            logger.info("[run=%s] обнаружен site_url из query: %s", run_id, site_url)

    # ---- Языковая политика
    with _stage("Detect languages", run_id):
        langs = _normalize_languages(languages, business_name, usp)
        logger.info("[run=%s] Languages detected: %s", run_id, ",".join(langs))
        _emit(emit, f"Языки для генерации: {', '.join(langs)}")

    # ---- План по количеству
    n_ads = max(1, int(n_ads or 1))
    headlines_target = min(HEADLINES_MAX_TOTAL, n_ads)
    long_headlines_target = LONG_HEADLINES_MAX_TOTAL
    descriptions_target = min(DESCRIPTIONS_MAX_TOTAL, n_ads)
    images_target = n_ads

    # Разделение по языкам
    ru_hl, en_hl = _split_counts(headlines_target, langs)
    ru_lh, en_lh = _split_counts(long_headlines_target, langs)
    ru_ds, en_ds = _split_counts(descriptions_target, langs)
    logger.info("[run=%s] targets: HL=%d (ru=%d,en=%d) | LHL=%d (ru=%d,en=%d) | DS=%d (ru=%d,en=%d) | IMG=%d",
                run_id, headlines_target, ru_hl, en_hl, long_headlines_target, ru_lh, en_lh, descriptions_target, ru_ds, en_ds, images_target)
    _emit(emit, f"Короткие заголовки: ru={ru_hl}, en={en_hl}; длинные: ru={ru_lh}, en={en_lh}; описания: ru={ru_ds}, en={en_ds}")

    # ---- Генерация текстов (ТОЛЬКО LLM, уже с санитаризацией)
    with _stage("Generate headlines", run_id):
        _emit(emit, "Генерирую короткие заголовки")
        headlines: List[str] = []
        if ru_hl: headlines += _gen_headlines_localized(None, usp, site_url, ru_hl, "ru")
        if en_hl: headlines += _gen_headlines_localized(None, usp, site_url, en_hl, "en")

    with _stage("Generate long headlines", run_id):
        _emit(emit, "Генерирую длинные заголовки")
        long_headlines: List[str] = []
        if ru_lh: long_headlines += _gen_long_headlines_localized(None, usp, site_url, ru_lh, "ru")
        if en_lh: long_headlines += _gen_long_headlines_localized(None, usp, site_url, en_lh, "en")

    with _stage("Generate descriptions", run_id):
        _emit(emit, "Генерирую описания")
        descriptions: List[str] = []
        if ru_ds: descriptions += _gen_descriptions_localized(None, usp, site_url, ru_ds, "ru")
        if en_ds: descriptions += _gen_descriptions_localized(None, usp, site_url, en_ds, "en")

    if len(headlines) < headlines_target:
        _emit(emit, f"Недостаточно коротких заголовков ({len(headlines)}/{headlines_target}) — стоп")
        logger.error("[run=%s] Недостаточно коротких заголовков: %d/%d", run_id, len(headlines), headlines_target)
        raise RuntimeError(f"Недостаточно коротких заголовков от LLM: {len(headlines)}/{headlines_target}.")
    if len(long_headlines) < long_headlines_target:
        _emit(emit, f"Недостаточно длинных заголовков ({len(long_headlines)}/{long_headlines_target}) — стоп")
        logger.error("[run=%s] Недостаточно длинных заголовков: %d/%d", run_id, len(long_headlines), long_headlines_target)
        raise RuntimeError(f"Недостаточно длинных заголовков от LLM: {len(long_headlines)}/{long_headlines_target}.")
    if len(descriptions) < descriptions_target:
        _emit(emit, f"Недостаточно описаний ({len(descriptions)}/{descriptions_target}) — стоп")
        logger.error("[run=%s] Недостаточно описаний: %d/%d", run_id, len(descriptions), descriptions_target)
        raise RuntimeError(f"Недостаточно описаний от LLM: {len(descriptions)}/{descriptions_target}.")

    # ---- Заполнение Headline(s)
    with _stage("Fill headlines", run_id, target=headlines_target):
        _emit(emit, f"Заполняю короткие заголовки ({headlines_target})")
        _ensure_panel_open_by_label(driver, ["headline", "заголовок"])
        hl_filled = _fill_headlines(driver, headlines, max_total=HEADLINES_MAX_TOTAL, run_id=run_id)
        if hl_filled < min(headlines_target, HEADLINES_MAX_TOTAL):
            _emit(emit, f"Не удалось заполнить все заголовки ({hl_filled}/{headlines_target}) — стоп")
            raise RuntimeError(f"Не удалось заполнить все заголовки: {hl_filled}/{headlines_target}")

    # ---- Заполнение Long headline(s)
    with _stage("Fill long headlines", run_id, target=long_headlines_target):
        _emit(emit, "Заполняю длинные заголовки (5)")
        _ensure_panel_open_by_label(driver, ["long headline", "long headlines", "длинный", "длинные", "длинный заголовок"])
        lhl_filled = _fill_long_headlines(driver, long_headlines, max_total=LONG_HEADLINES_MAX_TOTAL, run_id=run_id)
        if lhl_filled < long_headlines_target:
            _emit(emit, f"Не удалось заполнить все длинные заголовки ({lhl_filled}/{long_headlines_target}) — стоп")
            raise RuntimeError(f"Не удалось заполнить все длинные заголовки: {lhl_filled}/{long_headlines_target}")

    # ---- Заполнение Description(s)
    with _stage("Fill descriptions", run_id, target=descriptions_target):
        _emit(emit, f"Заполняю описания ({descriptions_target})")
        _ensure_panel_open_by_label(driver, ["description", "описание"])
        desc_filled = _fill_descriptions(driver, descriptions, max_total=DESCRIPTIONS_MAX_TOTAL, run_id=run_id)
        if desc_filled < min(descriptions_target, DESCRIPTIONS_MAX_TOTAL):
            _emit(emit, f"Не удалось заполнить все описания ({desc_filled}/{descriptions_target}) — стоп")
            raise RuntimeError(f"Не удалось заполнить все описания: {desc_filled}/{descriptions_target}")

    # ---- Images: открыть диалог и загрузить
    with _stage("Open images uploader", run_id):
        _emit(emit, "Открываю загрузчик изображений")
        if not _open_images_uploader(driver):
            _emit(emit, "Кнопка «Add images» не нажалась — стоп")
            raise RuntimeError("Кнопка 'Add images' не нажалась.")
        dialog = _wait_media_picker(driver, timeout=22.0, run_id=run_id)
        if not dialog:
            _emit(emit, "Диалог изображений не появился — стоп")
            raise RuntimeError("Диалог загрузки изображений не появился.")
        ok_tab = _select_tab_in_dialog(driver, dialog, ["upload", "загрузка", "загрузить"])
        logger.info("[run=%s] Переключился на вкладку Upload: %s", run_id, ok_tab)
        time.sleep(0.3)

    cfg = _pick_runware_config()

    # Динамическая NEG по контексту
    negative_clause = _build_negative_clause(usp, image_prompt, site_url)
    logger.info("[run=%s] NEG: %s", run_id, negative_clause)

    with _stage("Prepare image prompts", run_id, n=images_target):
        _emit(emit, f"Генерирую промпты для {images_target} изображений")
        base_prompts = _gen_image_prompts_diverse(
            images_target, business_name, usp, site_url, image_prompt, negative_clause
        )

        orientations = _decide_orientation_mix(images_target)
        dims_list = [_dims_for_orientation(orient) for orient in orientations]
        c_land = orientations.count("landscape")
        logger.info("[run=%s] Ориентации: %s", run_id, orientations)
        logger.info("[run=%s] Размеры: %s", run_id, dims_list)
        _emit(emit, f"Схема ориентаций: landscape={c_land}, square={images_target - c_land}")

    finp = None
    with _stage("Find file input", run_id):
        for i in range(25):
            finp = _ensure_file_input_visible(driver, dialog)
            if finp:
                break
            _heartbeat(run_id, "жду input[type=file]", every_iters=5, it=i + 1)
            time.sleep(0.20)
        if not finp:
            _emit(emit, "Поле выбора файла не найдено — стоп")
            raise RuntimeError("input[type=file] не найден на вкладке Upload.")
        logger.info("[run=%s] file-input готов к send_keys", run_id)

    # === Итоговый каталог для сохранения изображений ===
    images_dir = _compute_images_dir(business_name, site_url)
    logger.info("[run=%s] Каталог изображений: %s", run_id, images_dir)
    _emit(emit, f"Каталог изображений: {images_dir}")

    images_uploaded = 0
    image_files: List[str] = []
    image_meta: List[Dict[str, object]] = []

    with _stage("Generate+upload images", run_id, total=images_target):
        for i in range(images_target):
            orient = orientations[i]
            w, h = dims_list[i]
            prompt_i = _apply_orientation_to_prompt(base_prompts[i], orient, negative_clause)
            _emit(emit, f"Генерирую и загружаю изображение {i+1}/{images_target} ({orient} {w}×{h})")
            logger.debug("[run=%s] Prompt[%d]: %s", run_id, i + 1, _limit_len(prompt_i, 240))

            raw = _runware_generate_image(prompt_i, cfg, width=w, height=h, retries=RUNWARE_RETRIES, timeout=int(timeout_total))

            # Имя файла: ad_<NN>_<orient>_<WxH>.jpg
            base_name = f"ad_{i+1:02d}_{orient}_{w}x{h}"
            path = _save_image_file(raw, images_dir, base_name, suffix=".jpg")  # ← СРАЗУ В ЦЕЛЕВОЙ КАТАЛОГ
            logger.info("[run=%s] Сохранил %s (%s %dx%d)", run_id, os.path.basename(path), orient, w, h)

            image_files.append(path)
            image_meta.append({"index": i + 1, "orientation": orient, "width": w, "height": h, "file": os.path.basename(path), "abs_path": path})

            # Загружаем в Google Ads через input[type=file]
            try:
                finp.send_keys(path)
            except Exception as e:
                logger.exception("[run=%s] send_keys на input[file] провалился (%s)", run_id, e)
                raise

            pause = random.uniform(3.0, 5.0)
            time.sleep(pause)
            images_uploaded += 1
            logger.info("[run=%s] Изображение %d/%d загружено (%s, %.1fs пауза)", run_id, images_uploaded, images_target, os.path.basename(path), pause)

        if images_uploaded < images_target:
            _emit(emit, f"Загружено {images_uploaded}/{images_target} изображений — стоп")
            raise RuntimeError(f"Загружено изображений меньше целевого: {images_uploaded}/{images_target}")

    with _stage("Confirm images (Save)", run_id):
        min_wait = max(5.0, images_uploaded * 0.75)
        _emit(emit, "Сохраняю изображения")
        if not _wait_images_then_click_save(
            driver, dialog, min_wait_s=min_wait,
            extra_timeout_s=max(30.0, timeout_total / 2),
            run_id=run_id
        ):
            _emit(emit, "Кнопка «Save» не активировалась — стоп")
            raise RuntimeError("Кнопка Save не активировалась после загрузки изображений.")

    # Ждём закрытие диалога
    with _stage("Wait dialog close", run_id):
        for i in range(60):
            if not _get_media_dialog_root(driver):
                logger.debug("[run=%s] Диалог закрыт (i=%d)", run_id, i)
                break
            if i % 10 == 0:
                logger.debug("[run=%s] … жду закрытия диалога (i=%d)", run_id, i)
            time.sleep(0.2)

    try:
        ok = driver.execute_script(
            """
            const host = document.querySelector('[navi-id="add-media-button"]')?.closest('.actions')?.parentElement || document.body;
            const thumbs = host.querySelector('img, .asset-thumbnail, .mdc-card__media, .image-tile');
            return !!thumbs;
            """
        )
        if not ok:
            logger.warning("[run=%s] Не увидел миниатюры изображений после Save — возможно, UI отличается.", run_id)
    except Exception:
        pass

    elapsed = int((time.time() - t0) * 1000)
    logger.info(
        "[run=%s] step7: OK (%d ms). HL=%d/%d | LHL=%d/%d | DS=%d/%d | IMG=%d/%d | dir=%s",
        run_id, elapsed,
        hl_filled, headlines_target,
        lhl_filled, long_headlines_target,
        desc_filled, descriptions_target,
        images_uploaded, images_target, images_dir
    )
    _emit(emit, f"Готово: загружено {images_uploaded}/{images_target} изображений")

    return {
        "headlines_target": headlines_target,
        "headlines_filled": hl_filled,
        "long_headlines_target": long_headlines_target,
        "long_headlines_filled": lhl_filled,
        "descriptions_target": descriptions_target,
        "descriptions_filled": desc_filled,
        "images_target": images_target,
        "images_uploaded": images_uploaded,
        "images_dir": images_dir,
        "image_files": image_files,
        "image_orientations": orientations,
        "image_meta": image_meta,
        "duration_ms": elapsed,
        "run_id": run_id,
    }
