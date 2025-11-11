# -*- coding: utf-8 -*-
"""
examples/steps/step8.py

Шаг 8 (Search themes / ключевые запросы):
  - Сгенерировать до 50 релевантных запросов (слово/фраза), каждый ≤ 80 символов.
  - Языки: любая комбинация ISO-кодов (ru, en, de, fr, es, pt, it, pl, tr, uk/ua, zh, ja, ko, th, vi, ar, hi, id,
    nl, sv, no, da, fi, he, el, cs, sk, ro, hu, bg, sr, hr, sl, …).
    * при нескольких языках — равномерное распределение.
  - Ввод в UI: по одному запросу + Enter; затем "Next".
  - Валидация: чипы с ошибкой (.disapproved-token / error_outline) удаляем и догенерируем замену (до 3 проходов).

ВАЖНО:
  - НИКАКИХ фоллбеков/пресетов/шаблонов. Только LLM (Gemini).
  - Если LLM недоступна или отвечает не по схеме — возбуждаем RuntimeError
    с понятным текстом и не продолжаем.

Контракт:
    run_step8(
        driver: WebDriver,
        *,
        n_keywords: int = 50,
        business_name: str | None = None,
        usp: str | None = None,
        site_url: str | None = None,
        languages: list[str] | None = None,
        clear_existing: bool = False,
        timeout_total: float = 240.0,
        emit: Optional[Callable[[str], None]] = None,   # комментарии в UI (необязательный)
    ) -> dict
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import List, Optional, Dict, Any, Callable
from urllib.parse import urlparse, parse_qs, unquote

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webdriver import WebDriver, WebElement

logger = logging.getLogger("ads_ai.gads.step8")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

# ---------- LLM (Gemini) ----------
try:
    from ads_ai.llm.gemini import GeminiClient  # type: ignore
except Exception as e:  # pragma: no cover
    GeminiClient = None  # type: ignore
    logger.warning("GeminiClient not available: %s", e)

ITEM_MAX_LEN = 80
THEMES_LIMIT_UI = 50
REPLACEMENT_ROUNDS = 3


# ====== Вспомогательные утилиты ======

def _emit(emit: Optional[Callable[[str], None]], text: str) -> None:
    """Короткий безопасный комментарий в UI."""
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

def _has_cyrillic(s: Optional[str]) -> bool:
    if not s:
        return False
    return bool(re.search(r"[\u0400-\u04FF]", s))

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


# ====== Языки ======

LANG_CODE_NAME: Dict[str, str] = {
    "ru": "Russian", "en": "English", "de": "German", "fr": "French", "es": "Spanish",
    "pt": "Portuguese", "it": "Italian", "pl": "Polish", "tr": "Turkish", "uk": "Ukrainian", "ua": "Ukrainian",
    "zh": "Chinese", "ja": "Japanese", "ko": "Korean", "th": "Thai", "vi": "Vietnamese",
    "ar": "Arabic", "hi": "Hindi", "id": "Indonesian", "nl": "Dutch", "sv": "Swedish",
    "no": "Norwegian", "da": "Danish", "fi": "Finnish", "he": "Hebrew", "el": "Greek",
    "cs": "Czech", "sk": "Slovak", "ro": "Romanian", "hu": "Hungarian", "bg": "Bulgarian",
    "sr": "Serbian", "hr": "Croatian", "sl": "Slovenian",
}

def _normalize_lang_code(code: str) -> str:
    c = (code or "").lower().split("-")[0]  # en-US -> en
    return c if c in LANG_CODE_NAME else "en"

def _normalize_languages(langs: Optional[List[str]], business_name: Optional[str], usp: Optional[str]) -> List[str]:
    if langs:
        out: List[str] = []
        for x in langs:
            y = _normalize_lang_code(x)
            if y not in out:
                out.append(y)
        return out or ["en"]
    if _has_cyrillic(business_name) or _has_cyrillic(usp):
        return ["ru"]
    return ["en"]

def _distribute_counts(total: int, langs: List[str]) -> Dict[str, int]:
    L = max(1, len(langs))
    base = total // L
    rest = total % L
    out: Dict[str, int] = {}
    for i, lg in enumerate(langs):
        out[lg] = base + (1 if i < rest else 0)
    return out


# ====== UI-помощники ======

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
        el.click(); return True
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", el); return True
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
                    """, el
                ); return True
            except Exception:
                return False

def _ensure_panel_open_by_label(driver: WebDriver, label_keys: List[str]) -> None:
    try:
        driver.execute_script(
            """
            const KEYS = new Set(arguments[0].map(s=>String(s||'').toLowerCase()));
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
              if(cs.display==='none'||cs.visibility==='hidden'||parseFloat(cs.opacity||'1')<0.2) return false;
              return r.width>20 && r.height>20;};
            const headers=[...document.querySelectorAll('.main-header .header[role=button]')];
            for(const h of headers){
              const t=((h.getAttribute('aria-label')||'')+' '+(h.innerText||h.textContent||'')).trim().toLowerCase();
              if([...KEYS].some(k=>t.includes(k))){
                const exp=(h.getAttribute('aria-expanded')||'').toLowerCase()==='true';
                if(!exp){ try{ h.click(); }catch(e){ try{ h.querySelector('material-icon,.expand-button')?.click(); }catch(e2){} } }
                else { if(!isVis(h.closest('.panel')?.querySelector('.main'))){ try{ h.click(); }catch(e){} } }
              }
            }
            """,
            label_keys
        )
    except Exception:
        pass

def _ensure_search_themes_open(driver: WebDriver) -> None:
    _ensure_panel_open_by_label(driver, ["search themes", "темы поиска"])

def _find_search_input(driver: WebDriver) -> Optional[WebElement]:
    sels = [
        'search-theme-plugin input.search-input',
        'multi-chip-dialog-input.search-theme-input input.search-input',
        'multi-chip-dialog-input input.search-input',
        'input.search-input[placeholder*="Add search themes"]',
    ]
    for s in sels:
        try:
            el = driver.find_element(By.CSS_SELECTOR, s)
            if _is_interactable(driver, el):
                return el
        except Exception:
            continue
    return None

def _get_existing_chip_texts(driver: WebDriver) -> List[str]:
    try:
        arr = driver.execute_script(
            """
            const host = document.querySelector('search-theme-plugin') || document;
            const chips = [...host.querySelectorAll('material-chips .chip, .mdc-chip, material-chip, .acx-chip')];
            const out = [];
            for (const c of chips) {
              const t = ((c.innerText||'') + ' ' + (c.getAttribute('aria-label')||'')).trim();
              if (t) out.push(t);
            }
            return out;
            """
        )
        if isinstance(arr, list):
            cleaned = []
            for t in arr:
                t = str(t)
                t = re.sub(r"(?i)\bremove\b", "", t).replace("×", "")
                t = re.sub(r"\s+", " ", t).strip()
                if t:
                    cleaned.append(t)
            return cleaned
    except Exception:
        pass
    return []

def _clear_all_search_chips(driver: WebDriver, search_input: WebElement, max_remove: int = 200) -> int:
    removed = 0
    try:
        search_input.click(); time.sleep(0.05)
        for _ in range(max_remove):
            search_input.send_keys(Keys.BACK_SPACE)
            time.sleep(0.02)
            removed += 1
    except Exception:
        pass
    return removed

def _enter_theme(driver: WebDriver, input_el: WebElement, text: str) -> bool:
    text = _limit_len(text, ITEM_MAX_LEN)
    if not text: return False
    try:
        input_el.click(); time.sleep(0.03)
        input_el.send_keys(text); time.sleep(0.02)
        input_el.send_keys(Keys.ENTER); time.sleep(0.06)
        return True
    except Exception:
        return False

def _click_next(driver: WebDriver) -> bool:
    try:
        btn = driver.execute_script(
            """
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e), r=e.getBoundingClientRect();
              if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2) return false;
              return r.width>10 && r.height>10 && r.right>0 && r.bottom>0;};
            const prio = [...document.querySelectorAll('.buttons button')].filter(isVis)
              .find(b => (b.innerText||'').trim().toLowerCase()==='next');
            if (prio) return prio;
            const cand = [...document.querySelectorAll('button')].filter(isVis)
              .find(b => (b.innerText||'').trim().toLowerCase()==='next');
            return cand || null;
            """
        )
        if btn:
            return _robust_click(driver, btn)  # type: ignore
    except Exception:
        pass
    return False


# ====== Поиск/удаление неподходящих чипов ======

def _find_disapproved_chips(driver: WebDriver) -> List[WebElement]:
    try:
        chips = driver.execute_script(
            """
            const host = document.querySelector('search-theme-plugin') || document;
            const isVis = e => { if(!e) return false;
              const cs=getComputedStyle(e), r=e.getBoundingClientRect();
              if(cs.display==='none'||cs.visibility==='hidden'||parseFloat(cs.opacity||'1')<0.2) return false;
              return r.width>10 && r.height>10 && r.right>0 && r.bottom>0; };
            const nodes = [...host.querySelectorAll('material-chip, .mdc-chip, .chip')].filter(isVis);
            const bad = [];
            for (const n of nodes) {
              const cls = (n.className||'').toString().toLowerCase();
              const hasErr = cls.includes('disapproved') || n.querySelector('[aria-label=\"Error\"], .mdc-chip__icon--trailing[aria-label=\"Error\"]');
              if (hasErr) bad.push(n);
            }
            return bad;
            """
        )
        return list(chips) if isinstance(chips, list) else []
    except Exception:
        return []

def _chip_text(driver: WebDriver, chip: WebElement) -> str:
    try:
        return driver.execute_script(
            "const n=arguments[0]; const c=n.querySelector('.content'); return (c?(c.innerText||c.textContent):n.innerText)||'';", chip
        ) or ""
    except Exception:
        return ""

def _delete_chip(driver: WebDriver, chip: WebElement) -> bool:
    try:
        btn = driver.execute_script(
            "const n=arguments[0]; return n.querySelector('.delete-button,[aria-label^=\"Delete\"],[aria-label*=\"Delete\"]);", chip
        )
        if btn:
            return _robust_click(driver, btn)  # type: ignore
    except Exception:
        pass
    return False


# ====== LLM-генерация: контекст + темы (СТРОГО ТЕКСТОМ) ======

def _loose_json(text: str) -> Dict[str, Any]:
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

def _gemini_json_text(prompt_text: str, temperature: float = 0.6, retries: int = 1) -> Dict[str, Any]:
    """
    Обращение к Gemini строго текстовым промптом. Если LLM отсутствует — валим с RuntimeError.
    Если ответ не парсится в JSON — валим с RuntimeError (без фоллбеков).
    """
    if GeminiClient is None:
        raise RuntimeError("LLM (Gemini) недоступна: отсутствует клиент или API-ключ.")
    model = os.getenv("GEMINI_MODEL", "models/gemini-2.0-flash")
    client = GeminiClient(model=model, temperature=temperature, retries=retries, fallback_model=None)
    raw = client.generate_json(prompt_text)
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        data = _loose_json(raw)
        if data:
            return data
    raise RuntimeError("LLM (Gemini) недоступна или вернула некорректный ответ (ожидался JSON).")


# Список «запрещённых» маркетинговых мусорных слов (двуязычный)
_BAN_PATTERNS = [
    r"\b(cpc|ctr|cpa|roas|romi)\b",
    r"\bcreatives?\b", r"\bкреатив(ы|ов|ами)?\b",
    r"\bfeed(s)?\b", r"\bфид(ы|ов)?\b", r"\bтоварн(ый|ые)\s+фид(ы)?\b",
    r"\bумн(ая|ые|ый)\s+реклам\w*\b", r"\bsmart\s+ad(s)?\b",
    r"\bкак\s+(увеличить|поднять)\s+(онлайн\s+)?продаж\w*\b",
    r"\blead(s)?\b", r"\bleadgen\b", r"\bлид(ы|огенерац)\w*\b",
    r"\bмаркетинг\w*\b", r"\bпродвижени\w*\b", r"\bреклам\w*\b",
    r"\bаналитик\w*\b(?:сквозн\w*)?", r"\bатрибуци\w*\b",
    r"\bseo\b", r"\bsmm\b",
    r"\bgoogle\s+ads?\b", r"\bяндекс\s+директ\b", r"\btiktok\s+ads?\b",
]
_BAN_RX = re.compile("(" + "|".join(_BAN_PATTERNS) + ")", flags=re.I | re.U)


def _llm_extract_context(usp: Optional[str], site_url: Optional[str]) -> Dict[str, Any]:
    """
    Просим LLM вытащить предметную область: категория/товары/синонимы/запрещённые области.
    Это привязывает темы к реальному бизнесу (например, «продажа б/у ПК»).
    """
    usp_text = (usp or "").strip()
    domain_hint = ""
    try:
        if site_url:
            domain_hint = urlparse(site_url).netloc or ""
    except Exception:
        pass

    prompt = (
        "You are a product/domain analyst. Extract domain-specific context for generating SEARCH THEMES.\n"
        "Return ONLY JSON with this schema (all fields required):\n"
        "{\n"
        "  \"category\": \"string\",              // compact domain category (e.g., 'used computers', 'manicure scissors')\n"
        "  \"products\": [\"...\"],               // 5-15 concrete product/offer nouns/phrases from this business\n"
        "  \"use_cases\": [\"...\"],             // 5-15 buyer intents/use cases that map to the products\n"
        "  \"banned_topics\": [\"...\"],         // clear phrases to AVOID (marketing/ads/growth/feeds/etc.)\n"
        "  \"must_terms\": [\"...\"],            // important domain terms to prefer (units, models, compat, materials)\n"
        "  \"geo_or_locale\": \"string\"         // free text if locale can be inferred, else \"\"\n"
        "}\n\n"
        f"Context (USP): {usp_text or '—'}\n"
        f"Domain hint: {domain_hint or '—'}\n"
        "Rules:\n"
        "- NO marketing/ads/growth jargon. DO NOT include feeds, creatives, CPC/CTR/ROAS, analytics, etc.\n"
        "- Focus ONLY on the user's actual products/services and how people would search for them.\n"
        "- Use concise English nouns in arrays; avoid long sentences there.\n"
    )
    data = _gemini_json_text(prompt, temperature=0.3, retries=1)
    # Мягкая валидация
    ctx = {k: (data.get(k) or []) for k in ("products", "use_cases", "banned_topics", "must_terms")}
    ctx["category"] = str(data.get("category") or "").strip()
    ctx["geo_or_locale"] = str(data.get("geo_or_locale") or "").strip()
    return ctx


def _is_allowed_theme(s: str, usp: Optional[str], ctx: Dict[str, Any]) -> bool:
    """Фильтр мусора: запрещаем маркетинговую дичь, если её нет в USP явно."""
    if not s or len(s) < 2:
        return False
    if _BAN_RX.search(s):
        if usp and _BAN_RX.search(usp):
            return True
        banned_custom = [str(x) for x in (ctx.get("banned_topics") or []) if isinstance(x, str)]
        if any(re.search(re.escape(x), s, flags=re.I) for x in banned_custom if x.strip()):
            return False
        return False
    return True


def _llm_queries_for_lang(n: int, lang_code: str, usp: Optional[str], site_url: Optional[str],
                          ctx: Dict[str, Any], avoid: List[str]) -> List[str]:
    """
    Просим LLM вернуть JSON {"queries":[...]}:
      — НИ ОДНОГО маркетингового слова (реклама/маркетинг/фиды/креативы/ROAS…),
      — Только предметка из category/products/use_cases/must_terms,
      — Сборная смесь коротких 1–3 слов и естественных фраз.
    """
    n = max(1, int(n))
    lang_name = LANG_CODE_NAME.get(lang_code, "English")
    usp_text = (usp or "").strip()

    category = str(ctx.get("category") or "").strip()
    products = [str(x).strip() for x in (ctx.get("products") or []) if str(x).strip()]
    use_cases = [str(x).strip() for x in (ctx.get("use_cases") or []) if str(x).strip()]
    must_terms = [str(x).strip() for x in (ctx.get("must_terms") or []) if str(x).strip()]
    banned_topics = [str(x).strip() for x in (ctx.get("banned_topics") or []) if str(x).strip()]

    avoid_list = [_limit_len(a, ITEM_MAX_LEN) for a in (avoid or []) if a]
    avoid_blob = "; ".join(sorted(set(avoid_list)))[:600] if avoid_list else ""

    prompt = (
        "You are a senior ads strategist. Generate HIGHLY RELEVANT SEARCH THEMES strictly about the user's products/service.\n"
        f"Language: {lang_name}. Max length per item: {ITEM_MAX_LEN} characters.\n"
        "Output mix: ~40% short keywords (1–3 words), ~60% natural search phrases.\n"
        "HARD RULES:\n"
        "- NO marketing/ads/growth/analytics/feed/creative jargon AT ALL unless literally present in USP.\n"
        "- DO NOT include brand or competitor names.\n"
        f"- Stay strictly in {lang_name}; do not mix languages.\n"
        "- Keep each item compact, human search-like.\n\n"
        "Domain Context:\n"
        f"  category: {category or '—'}\n"
        f"  products: {products or '—'}\n"
        f"  use_cases: {use_cases or '—'}\n"
        f"  must_terms: {must_terms or '—'}\n"
        f"  banned_topics: {banned_topics or '—'}\n"
        f"USP: {usp_text or '—'}\n"
        f"Avoid (and close variants): {avoid_blob or '—'}\n\n"
        "Return ONLY JSON: {\"queries\": [\"q1\", \"q2\", ...]} with exactly the requested count."
    )

    data = _gemini_json_text(prompt, temperature=0.55, retries=1)
    arr = data.get("queries") if isinstance(data, dict) else None
    if not isinstance(arr, list) or not arr:
        raise RuntimeError("LLM (Gemini) недоступна или вернула пустой список поисковых тем.")
    out: List[str] = []
    seen = set()
    for s in arr:
        t = _limit_len(_strip_quotes(str(s)), ITEM_MAX_LEN)
        t = re.sub(r"\s+", " ", t).strip()
        if not t:
            continue
        lk = t.lower()
        if lk in seen:
            continue
        if not _is_allowed_theme(t, usp, ctx):
            continue
        out.append(t)
        seen.add(lk)
        if len(out) >= n:
            break
    if len(out) < n:
        logger.warning("LLM themes filtered to %d/%d by domain rules.", len(out), n)
    return out[:n]


# ====== Основной шаг ======

def run_step8(
    driver: WebDriver,
    *,
    n_keywords: int = THEMES_LIMIT_UI,
    business_name: Optional[str] = None,
    usp: Optional[str] = None,
    site_url: Optional[str] = None,
    languages: Optional[List[str]] = None,
    clear_existing: bool = False,
    timeout_total: float = 240.0,
    emit: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    t0 = time.time()
    _emit(emit, "Готовлю поисковые темы по реальной предметке")

    # Подхватываем site_url из URL, если не передали
    if not site_url:
        site_url = _extract_site_from_current_url(driver)

    # Нормализуем языки
    langs = _normalize_languages(languages, business_name, usp)
    _emit(emit, f"Языки для тем: {', '.join(langs)}")
    logger.info("step8: languages=%s", ",".join(langs))

    # Откроем секцию, найдём поле ввода
    _ensure_search_themes_open(driver)
    inp = _find_search_input(driver)
    if not inp:
        _emit(emit, "Поле ввода поисковых тем не найдено — стоп")
        raise RuntimeError("Search themes: поле ввода не найдено.")

    # Уже введённые чипы
    existing = _get_existing_chip_texts(driver)
    existing_set = set(x.strip().lower() for x in existing)
    logger.info("Search themes: уже есть %d chip(ов).", len(existing))

    # Очистка при необходимости
    if clear_existing and existing:
        _emit(emit, "Очищаю ранее добавленные темы")
        _clear_all_search_chips(driver, inp)
        time.sleep(0.2)
        existing = _get_existing_chip_texts(driver)
        existing_set = set(x.strip().lower() for x in existing)
        logger.info("Search themes: после очистки осталось %d chip(ов).", len(existing))

    # План по количеству
    free_slots = max(0, THEMES_LIMIT_UI - len(existing))
    target_total = max(1, min(int(n_keywords or THEMES_LIMIT_UI), free_slots))
    if target_total <= 0:
        logger.info("Search themes: свободных слотов нет — жму Next.")
        _emit(emit, "Свободных слотов нет — продолжаю")
        _click_next(driver)
        elapsed = int((time.time() - t0) * 1000)
        return {"themes_target": 0, "themes_entered": 0, "languages_detected": langs, "values": [], "duration_ms": elapsed}

    # Распределение по языкам
    lang_counts = _distribute_counts(target_total, langs)
    logger.info("Search themes: план по языкам: %s", lang_counts)
    _emit(emit, "Извлекаю предметный контекст из УТП/домена")

    # Контекст домена (категория/товары/кейсы/важные термины/бан-темы)
    ctx = _llm_extract_context(usp, site_url)

    # ===== 1) Первая генерация LLM и ввод =====
    to_avoid = list(existing)  # избегаем дубликатов с уже существующими
    generated_all: List[str] = []
    for lg in langs:
        cnt = lang_counts.get(lg, 0)
        if cnt <= 0:
            continue
        items = _llm_queries_for_lang(cnt, lg, usp, site_url, ctx, to_avoid)
        # Чистка/лимит и доп-фильтр
        seen_local = set()
        cleaned: List[str] = []
        for s in items:
            s = _limit_len(s, ITEM_MAX_LEN)
            k = s.lower()
            if not s or k in seen_local:
                continue
            if not _is_allowed_theme(s, usp, ctx):
                continue
            seen_local.add(k); cleaned.append(s)
            if len(cleaned) >= cnt:
                break
        generated_all.extend(cleaned)
        to_avoid.extend(cleaned)

    # Дедуп по уже существующим
    filtered: List[str] = []
    seen_global = set(existing_set)
    for q in generated_all:
        k = q.strip().lower()
        if not k or k in seen_global:
            continue
        seen_global.add(k)
        filtered.append(q)
        if len(filtered) >= target_total:
            break

    if not filtered:
        _emit(emit, "LLM вернула пустой/нерелевантный набор — стоп")
        raise RuntimeError("LLM (Gemini) сгенерировала пустой или полностью отфильтрованный набор поисковых тем.")

    logger.info("Search themes: target=%d, generated=%d, to_enter=%d", target_total, len(generated_all), len(filtered))
    _emit(emit, f"Ввожу темы в интерфейс ({len(filtered)})")

    # Вводим
    for q in filtered:
        ok = _enter_theme(driver, inp, q)
        if not ok:
            _emit(emit, f"Не удалось ввести тему: {q} — стоп")
            raise RuntimeError(f"Не удалось ввести тему: {q}")
        time.sleep(0.04)

    # ===== 2) Валидация и замены (до REPLACEMENT_ROUNDS) =====
    for round_i in range(REPLACEMENT_ROUNDS):
        bad_chips = _find_disapproved_chips(driver)
        if not bad_chips:
            break

        _emit(emit, f"Найдены отклонённые темы — заменяю (итерация {round_i+1})")
        bad_texts: List[str] = []
        for chip in bad_chips:
            txt = _chip_text(driver, chip) or ""
            bad_texts.append(txt.strip())
            _delete_chip(driver, chip)
            time.sleep(0.03)

        existing_now = _get_existing_chip_texts(driver)
        need = max(0, target_total - len(existing_now))
        if need <= 0:
            break

        # добор через LLM (с расширенным avoid)
        avoid_set = set(a.lower() for a in (existing_now + bad_texts + filtered + to_avoid))
        repl_all: List[str] = []
        repl_counts = _distribute_counts(need, langs)
        for lg in langs:
            c = repl_counts.get(lg, 0)
            if c <= 0:
                continue
            repl = _llm_queries_for_lang(c, lg, usp, site_url, ctx, sorted(avoid_set))
            for s in repl:
                s = _limit_len(s, ITEM_MAX_LEN)
                lk = s.lower()
                if not s or lk in avoid_set:
                    continue
                if not _is_allowed_theme(s, usp, ctx):
                    continue
                avoid_set.add(lk)
                repl_all.append(s)
                if len(repl_all) >= need:
                    break
            if len(repl_all) >= need:
                break

        if not repl_all:
            _emit(emit, "Не удалось подобрать замены — стоп")
            raise RuntimeError("LLM (Gemini) не смогла подобрать замены для неподходящих поисковых тем.")

        for q in repl_all:
            if len(_get_existing_chip_texts(driver)) >= THEMES_LIMIT_UI:
                break
            ok = _enter_theme(driver, inp, q)
            if not ok:
                _emit(emit, f"Не удалось ввести тему (замена): {q} — стоп")
                raise RuntimeError(f"Не удалось ввести тему (замена): {q}")
            time.sleep(0.04)

    # Финальная проверка
    final_list = _get_existing_chip_texts(driver)
    if len(final_list) < min(target_total, THEMES_LIMIT_UI):
        if _find_disapproved_chips(driver):
            _emit(emit, "Часть тем отклонена и замены не найдены — стоп")
            raise RuntimeError("Некоторые поисковые темы отклонены интерфейсом и не удалось подобрать корректную замену.")

    # Next
    _emit(emit, "Готово — жму «Next»")
    _click_next(driver)

    elapsed = int((time.time() - t0) * 1000)
    logger.info("step8: OK (%d ms). THEMES=%d/%d", elapsed, min(len(final_list), target_total), target_total)
    _emit(emit, f"Тем добавлено: {min(len(final_list), target_total)}/{target_total}")
    return {
        "themes_target": target_total,
        "themes_entered": min(len(final_list), target_total),
        "languages_detected": langs,
        "values": final_list[:target_total],
        "duration_ms": elapsed,
    }
