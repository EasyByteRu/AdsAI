# -*- coding: utf-8 -*-
"""
examples/steps/step6.py

Шаг 6 Google Ads Wizard (на одном экране со следующим шагом, но делаем свою часть):
  1) Asset group name — сгенерировать и надёжно ввести.
  2) Brand guidelines → Business name — сгенерировать ИЛИ принять извне, строго ≤ 25 символов.
  3) Brand guidelines → Logos — нажать "Add logos" → открыть диалог (slidealog) →
     вкладка "Upload" → загрузить файл через input[type=file] (без нативного окна) →
     подождать ~5 секунд пока ассет обработается (автовыбор) → нажать "Save" →
     убедиться, что логотип появился в галерее.

Контракт:
    run_step6(driver, *, business_name=None, site_url=None, usp=None, logo_prompt=None,
              timeout_total=180.0, emit=None) -> dict

Возврат:
    {
      "asset_group_name": str,
      "business_name": str,
      "logo_uploaded": bool,
      "logo_file": str | "",
      "duration_ms": int
    }
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import tempfile
import time
import uuid
import random
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Callable
from urllib.parse import urlparse, parse_qs, unquote

import requests
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webdriver import WebDriver, WebElement

# Pillow — опционально (компрессия/ресайз лого)
try:
    from PIL import Image  # type: ignore
except Exception:  # pragma: no cover
    Image = None  # type: ignore

logger = logging.getLogger("ads_ai.gads.step6")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

# ---------- LLM (Gemini) ----------
try:
    from ads_ai.llm.gemini import GeminiClient  # type: ignore
except Exception as e:  # pragma: no cover
    GeminiClient = None  # type: ignore
    logger.warning("GeminiClient not available: %s", e)

# ---------- Runware defaults ----------
DEFAULT_RUNWARE_API_KEY = os.getenv("RUNWARE_API_KEY", "2texOEYSQNN0tUFmr2ZaVbX6J62cbquL")
DEFAULT_RUNWARE_MODEL_ID = os.getenv("RUNWARE_MODEL_ID", "runware:100@1")
DEFAULT_RUNWARE_URL = os.getenv("RUNWARE_URL", "https://api.runware.ai/v1")

MAX_LONG_EDGE: int = 800
JPEG_Q: int = 70
RUNWARE_RETRIES: int = 4


# ====== Утилиты ======

def _emit(emit: Optional[Callable[[str], None]], text: str) -> None:
    """Безопасно шлём короткий комментарий в UI."""
    if callable(emit) and isinstance(text, str) and text.strip():
        try:
            emit(text.strip())
        except Exception:
            pass

def _limit_len(s: str, n: int) -> str:
    if s is None:
        return ""
    ss = str(s).strip()
    return ss if len(ss) <= n else ss[:n]

def _strip_quotes(s: str) -> str:
    return (s or "").replace('"', "").replace("'", "").strip()

def _asciiize(s: str) -> str:
    if not s:
        return ""
    ss = "".join(ch if ord(ch) < 128 else " " for ch in s)
    ss = re.sub(r"\s+", " ", ss).strip()
    ss = ss.replace('"', "").replace("'", "")
    return ss

def _unique_suffix() -> str:
    # UTC, чтобы имена были повторяемыми вне локали
    return time.strftime("%y%m%d%H%M%S", time.gmtime())

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
                try:
                    if not dlg.is_displayed():  # type: ignore[attr-defined]
                        continue
                except Exception:
                    continue
                for b in dlg.find_elements(By.CSS_SELECTOR, 'button,[role="button"],a[role="button]'):
                    txt = ((b.text or "") + " " + (b.get_attribute("aria-label") or "")).strip().lower()
                    if txt and any(w in txt for w in CAND):
                        try:
                            b.click()
                        except Exception:
                            try:
                                driver.execute_script("arguments[0].click();", b)
                            except Exception:
                                continue
                        time.sleep(0.14)
                        hit = True
                        break
                if hit:
                    break
            if not hit:
                break
        except Exception:
            break


# ====== LLM: имена и промпт лого (ВАЖНО: сериализация prompt в строку) ======

def _gemini_json_call(payload: dict, *, temperature: float = 0.2, retries: int = 1) -> dict:
    """
    Унифицированный вызов JSON-задачи Gemini.
    GeminiClient.generate_json ожидает строку/контент; отдаём сериализованный JSON.
    """
    if GeminiClient is None:
        raise RuntimeError("LLM unavailable")
    model = os.getenv("GEMINI_MODEL", "models/gemini-2.0-flash")
    client = GeminiClient(model=model, temperature=temperature, retries=retries, fallback_model=None)
    raw = client.generate_json(json.dumps(payload, ensure_ascii=False))
    return raw if isinstance(raw, dict) else {}

def _generate_asset_group_name_llm(business_name: Optional[str], usp: Optional[str], site_url: Optional[str]) -> str:
    payload = {
        "task": "Return ONLY compact JSON with 'name' (<= 45 chars).",
        "constraints": ["Prefix with 'AG' or 'Asset Group' short.", "No quotes or emojis."],
        "inputs": {"business_name": (business_name or "").strip(), "usp": (usp or "").strip(), "site_url": (site_url or "").strip()},
        "output_schema": {"name": "string"},
        "format": "json_only_no_explanations",
        "examples": [{"inputs": {"business_name": "EasyByte AI", "usp": "E-commerce AI"}, "json": {"name": "AG | EasyByte Core"}}]
    }
    data = _gemini_json_call(payload, temperature=0.2, retries=1)
    name = _strip_quotes(str(data.get("name", ""))).strip()
    return _limit_len(name, 45) or "AG | Brand " + _unique_suffix()

def _generate_business_name_llm(usp: Optional[str], site_url: Optional[str]) -> str:
    payload = {
        "task": "Return ONLY JSON with 'business_name' (<= 25 chars, readable brand name).",
        "constraints": ["No emojis.", "Avoid quotes."],
        "inputs": {"usp": (usp or "").strip(), "site_url": (site_url or "").strip()},
        "output_schema": {"business_name": "string"},
        "format": "json_only_no_explanations",
        "examples": [{"inputs": {"usp": "Нейросети для e-commerce", "site_url": "https://easy-byte.ru"}, "json": {"business_name": "EasyByte AI"}}]
    }
    data = _gemini_json_call(payload, temperature=0.2, retries=1)
    name = _strip_quotes(str(data.get("business_name", ""))).strip()
    return _limit_len(name, 25) or "Brand"

def _generate_logo_prompt_llm(business_name: str, usp: Optional[str], site_url: Optional[str]) -> str:
    sys = ("You are an expert brand designer and prompt engineer. Create a concise English prompt (<=300 chars) "
           "for a minimalist flat vector-like logo (icon + logomark). No visible text, no watermark. "
           "Clean, modern, high-contrast, centered.")
    payload = {
        "task": sys,
        "inputs": {"business_name": _strip_quotes(business_name), "usp": (usp or "").strip(), "domain_hint": _domain_to_brand(site_url or "")},
        "output_schema": {"prompt": "string"},
        "format": "json_only_no_explanations",
        "examples": [{"inputs": {"business_name": "EasyByte AI", "usp": "Neural tools for e-commerce", "domain_hint": "easy-byte"},
                      "json": {"prompt": "Minimalist flat vector logo, abstract byte mark + subtle AI symbol, high contrast, centered, clean background, professional tech brand aesthetic, no text, no watermark, SVG-like render"}}]
    }
    data = _gemini_json_call(payload, temperature=0.3, retries=1)
    p = _strip_quotes(str(data.get("prompt", ""))).strip()
    return _limit_len(p, 300)

def _fallback_asset_group_name(biz: Optional[str]) -> str:
    base = _strip_quotes(biz or "").strip() or "Brand"
    return _limit_len(f"AG | {base} {_unique_suffix()}", 45)

def _fallback_business_name(site_url: Optional[str], usp: Optional[str]) -> str:
    brand = _domain_to_brand(site_url or "") if site_url else "Brand"
    if usp:
        kw = re.sub(r"\s+", " ", re.sub(r"[^\w ]+", " ", usp)).strip().split()
        if kw:
            hint = kw[0][:6]
            if hint and hint.lower() not in brand.lower():
                brand = f"{brand} {hint}"
    return _limit_len(_strip_quotes(brand), 25) or "Brand"

def _fallback_logo_prompt(business_name: str, usp: Optional[str], site_url: Optional[str]) -> str:
    parts = [
        f"Minimalist flat logo, vector-style, high-contrast icon + logomark for '{_strip_quotes(business_name)}'",
        "clean, modern, professional, centered composition, brand-friendly",
    ]
    if usp: parts.append(f"brand theme: {_asciiize(usp)[:60]}")
    if site_url: parts.append(f"brand domain hint: {_domain_to_brand(site_url)}")
    parts.append("no watermark, no visible text, SVG-like look")
    return ". ".join(parts)


# ====== URL-подхват site_url из current_url ======

def _extract_site_from_current_url(driver: WebDriver) -> Optional[str]:
    try:
        u = driver.current_url or ""
        q = parse_qs(urlparse(u).query)
        if "cmpnInfo" in q:
            try:
                js = json.loads(unquote(q["cmpnInfo"][0]))
                for k in ("57", "site_url", "url"):
                    v = js.get(k)
                    if isinstance(v, str) and v.startswith("http"): return v
            except Exception:
                pass
        if "preUrl" in q:
            try:
                pre = unquote(q["preUrl"][0])
                if "%7B" in pre or "{" in pre:
                    js = json.loads(unquote(pre.split("&cmpnInfo=")[-1]))
                    for k in ("57", "site_url", "url"):
                        v = js.get(k)
                        if isinstance(v, str) and v.startswith("http"): return v
            except Exception:
                pass
    except Exception:
        pass
    return None


# ====== Поиск нужных инпутов ======

def _find_asset_group_name_input(driver: WebDriver) -> Optional[WebElement]:
    sels = [
        'material-expansionpanel .name-input input.input',
        'material-expansionpanel[section_id] .name-input input.input',
        'input.input[aria-labelledby*=Asset][aria-labelledby*=group][aria-labelledby*=name]',
    ]
    for s in sels:
        try:
            el = driver.find_element(By.CSS_SELECTOR, s)
            if el.is_displayed() and el.is_enabled():  # type: ignore[attr-defined]
                return el
        except Exception:
            continue
    try:
        el = driver.execute_script(
            """
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e), r=e.getBoundingClientRect();
              if(cs.display==='none'||cs.visibility==='hidden'||parseFloat(cs.opacity||'1')<0.2) return false;
              return r.width>10 && r.height>10 && r.bottom>0 && r.right>0;};
            const headers=[...document.querySelectorAll('.main-header .header[role=button]')];
            for(const h of headers){
              const t=((h.getAttribute('aria-label')||'')+' '+(h.innerText||h.textContent||'')).toLowerCase();
              if(t.includes('asset group') && t.includes('name')){
                const panel=h.closest('.panel');
                const inp = panel && panel.querySelector('input.input,input.input-area');
                if(inp && isVis(inp)) return inp;
              }
            }
            return null;
            """
        )
        if el: return el
    except Exception:
        pass
    return None

def _find_brand_business_name_input(driver: WebDriver) -> Optional[WebElement]:
    sels = [
        'brand-profile-editor .business-name input.input',
        'brand-profile-editor text-input.business-name input.input',
        'brand-profile-editor input.input[aria-labelledby*="Business name"]',
    ]
    for s in sels:
        try:
            el = driver.find_element(By.CSS_SELECTOR, s)
            if el.is_displayed() and el.is_enabled():  # type: ignore[attr-defined]
                return el
        except Exception:
            continue
    try:
        el = driver.execute_script(
            """
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e), r=e.getBoundingClientRect();
              if(cs.display==='none'||cs.visibility==='hidden'||parseFloat(cs.opacity||'1')<0.2) return false;
              return r.width>10 && r.height>10 && r.bottom>0 && r.right>0;};
            const headers=[...document.querySelectorAll('.main-header .header[role=button]')];
            for(const h of headers){
              const t=((h.getAttribute('aria-label')||'')+' '+(h.innerText||h.textContent||'')).toLowerCase();
              if(t.includes('brand') && t.includes('guidelines')){
                const panel=h.closest('.panel');
                const inp = panel && panel.querySelector('input.input,input.input-area');
                if(inp && isVis(inp)) {
                  const lab=(inp.getAttribute('aria-labelledby')||'')+(inp.getAttribute('aria-label')||'');
                  if(lab.toLowerCase().includes('business')||lab.toLowerCase().includes('name')) return inp;
                }
              }
            }
            return null;
            """
        )
        if el: return el
    except Exception:
        pass
    return None

def _ensure_panel_open_by_label(driver: WebDriver, label_keys: List[str]) -> None:
    try:
        driver.execute_script(
            """
            const KEYS = new Set(arguments[0].map(s=>String(s||'').toLowerCase()));
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
              if(cs.display==='none'||cs.visibility==='hidden'||parseFloat(cs.opacity||'1')<0.2) return false;
              return r.width>20 && r.height>20;};
            const headers=[...document.querySelectorAll('.main-header .header[role=button], .main-header[role=heading] .header[role=button]')];
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
            """,
            label_keys
        )
    except Exception:
        pass

def _ensure_brand_guidelines_open(driver: WebDriver) -> None:
    _ensure_panel_open_by_label(driver, ["brand guidelines", "бренд", "брендовые", "брендовые рекомендации"])


# ====== Runware лого ======

@dataclass
class RunwareConfig:
    api_key: str
    model_id: str
    base_url: str = DEFAULT_RUNWARE_URL

def _pick_runware_config() -> RunwareConfig:
    return RunwareConfig(
        api_key=DEFAULT_RUNWARE_API_KEY.strip(),
        model_id=(DEFAULT_RUNWARE_MODEL_ID or "runware:100@1").strip(),
        base_url=(DEFAULT_RUNWARE_URL or "https://api.runware.ai/v1").strip(),
    )

def _runware_generate_logo(prompt: str, cfg: RunwareConfig,
                           width: int = 512, height: int = 512,
                           retries: int = RUNWARE_RETRIES, timeout: int = 180) -> bytes:
    """
    POST на cfg.base_url (https://api.runware.ai/v1), payload — массив задач {taskType=imageInference}.
    Ответ: data[0].imageURL → скачиваем bytes. Ретраи с экспоненциальной паузой.
    """
    width = max(128, min(2048, int(width)))
    height = max(128, min(2048, int(height)))

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

            return r2.content

        except Exception as e:
            if attempt >= retries:
                raise RuntimeError("Runware generation failed: " + " | ".join(errors[-5:] or [str(e)]))
            jitter = random.uniform(0.0, 0.5)
            logger.warning("Runware issue (%s), retry %d/%d in %.1fs", e, attempt, retries, backoff + jitter)
            time.sleep(backoff + jitter)
            backoff = min(backoff * 2, 8.0)


def _write_logo_file(raw: bytes, suffix: str = ".jpg") -> str:
    fd, path = tempfile.mkstemp(prefix="adsai_logo_", suffix=suffix)
    os.close(fd)
    try:
        if Image is not None:
            im = Image.open(io.BytesIO(raw)).convert("RGB")
            w, h = im.size
            scale = 1.0
            if max(w, h) > MAX_LONG_EDGE:
                scale = MAX_LONG_EDGE / float(max(w, h))
            if scale < 1.0:
                im = im.resize((int(w * scale), int(h * scale)))
            im.save(path, format="JPEG", quality=JPEG_Q, optimize=True)
            return path
    except Exception:
        pass
    with open(path, "wb") as f:
        f.write(raw)
    return path


# ====== Диалог выбора логотипов (slidealog) ======

def _open_logo_uploader(driver: WebDriver) -> bool:
    _ensure_brand_guidelines_open(driver)
    # Пробуем «умный» поиск кнопки "Add logos"
    try:
        btn = driver.execute_script(
            """
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
              if(cs.display==='none'||cs.visibility==='hidden'||parseFloat(cs.opacity||'1')<0.2) return false;
              return r.width>10 && r.height>10 && r.bottom>0 && r.right>0;};
            const cand=[...document.querySelectorAll('media-gallery.logo-gallery .add-button,[aria-label*="Add logos"],[aria-label*="Add logo"],[aria-label*="Добавить логотип"]')].filter(isVis);
            return cand[0]||null;
            """
        )
        if btn:
            try:
                btn.click()  # type: ignore
                return True
            except Exception:
                try:
                    driver.execute_script("arguments[0].click();", btn)  # type: ignore
                    return True
                except Exception:
                    pass
    except Exception:
        pass
    # Фоллбек по тексту
    try:
        for b in driver.find_elements(By.XPATH, "//button[.//*[normalize-space(text())='Add logos'] or normalize-space(text())='Add logos']"):
            try:
                b.click()
                return True
            except Exception:
                try:
                    driver.execute_script("arguments[0].click();", b)
                    return True
                except Exception:
                    continue
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
            const dialogs=[...document.querySelectorAll('slidealog-wrapper[role=dialog], [role=dialog]')].filter(isVis);
            for(const d of dialogs){
              const t = ((d.getAttribute('aria-label')||'') + ' ' + (d.innerText||'') + ' ' + (d.querySelector('h2.title')?.innerText||'')).toLowerCase();
              if(t.includes('logo')) return d;
            }
            return dialogs[0] || null;
            """
        )
        return el
    except Exception:
        return None

def _wait_media_picker(driver: WebDriver, timeout: float = 22.0) -> Optional[WebElement]:
    end = time.time() + timeout
    while time.time() < end:
        root = _get_media_dialog_root(driver)
        if root: return root
        time.sleep(0.25)
    return None

def _select_tab_in_dialog(driver: WebDriver, dialog: WebElement, tab_labels: List[str]) -> bool:
    try:
        return bool(driver.execute_script(
            """
            const dlg=arguments[0], LABELS=new Set(arguments[1].map(s=>String(s||'').toLowerCase()));
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
              if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2) return false;
              return r.width>10 && r.height>10 && r.right>0 && r.bottom>0;};
            const tabs=[...dlg.querySelectorAll('tab-button[role=tab], [role=tab]')].filter(isVis);
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
    """Сделать существующий input[type=file] видимым (для send_keys) без открытия нативного окна."""
    try:
        el = driver.execute_script(
            """
            const dlg=arguments[0];
            const inp = dlg.querySelector('assets-upload-tab input[type="file"], input[type="file"]');
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

def _click_upload_from_computer(driver: WebDriver, dialog: WebElement) -> None:
    try:
        btn = driver.execute_script(
            """
            const dlg=arguments[0];
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
              if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2) return false;
              return r.width>10 && r.height>10 && r.right>0 && r.bottom>0;};
            const b = [...dlg.querySelectorAll('.upload-from-computer-button,[aria-label*="Upload from computer"]')].find(isVis);
            return b||null;
            """,
            dialog
        )
        if btn:
            try:
                btn.click()  # type: ignore
            except Exception:
                try:
                    driver.execute_script("arguments[0].click();", btn)  # type: ignore
                except Exception:
                    pass
            time.sleep(0.2)
    except Exception:
        pass

def _wait_5s_then_click_save(driver: WebDriver, dialog: WebElement, min_wait_s: float = 5.0, timeout_s: float = 60.0) -> bool:
    time.sleep(max(0.0, min_wait_s))  # обязательная фикс-пауза
    end = time.time() + max(timeout_s, 1.0)
    while time.time() < end:
        try:
            save_btn = driver.execute_script(
                """
                const dlg=arguments[0];
                const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
                  if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2) return false;
                  return r.width>10 && r.height>10 && r.right>0 && r.bottom>0;};
                const cand=[...dlg.querySelectorAll(
                    'material-button.confirm-button, [data-test-id="confirm-button"], button, [role=button]'
                )].find(b=>{
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
            if save_btn:
                try:
                    save_btn.click()  # type: ignore
                    return True
                except Exception:
                    try:
                        driver.execute_script("arguments[0].click();", save_btn)  # type: ignore
                        return True
                    except Exception:
                        pass
        except Exception:
            pass
        time.sleep(0.25)
    return False


# ====== Основной шаг ======

def run_step6(
    driver: WebDriver,
    *,
    business_name: Optional[str] = None,
    site_url: Optional[str] = None,
    usp: Optional[str] = None,
    logo_prompt: Optional[str] = None,
    timeout_total: float = 180.0,
    emit: Optional[Callable[[str], None]] = None,
) -> Dict[str, object]:
    t0 = time.time()
    _dismiss_soft_dialogs(driver, budget_ms=900)

    # Подхват site_url из query, если не дали
    if not site_url:
        site_url = _extract_site_from_current_url(driver)
        if site_url:
            logger.info("step6: обнаружен site_url из query: %s", site_url)

    # ----- 1) Asset group name -----
    _emit(emit, "Задаю имя группе ассетов")
    try:
        ag_name = _generate_asset_group_name_llm(business_name, usp, site_url)
    except Exception as e:
        # Ключевой фикс к падению: теперь сюда мы не должны попадать из-за формата prompt;
        # но оставляем fallback для надёжности.
        logger.warning("LLM asset group name failed: %s — fallback.", e)
        ag_name = _fallback_asset_group_name(business_name or (site_url and _domain_to_brand(site_url)) or "Brand")
        _emit(emit, f"LLM недоступен — использую имя: {ag_name}")

    _ensure_panel_open_by_label(driver, ["asset group name", "группа ассетов", "имя группы"])
    ag_input = _find_asset_group_name_input(driver)
    if not ag_input:
        _emit(emit, "Поле имени группы ассетов не найдено — стоп")
        raise RuntimeError("Asset group name: поле ввода не найдено.")
    _dispatch_input_change(driver, ag_input, ag_name)
    time.sleep(0.15)
    logger.info("Asset group name установлен: %s", ag_name)
    _emit(emit, f"Имя группы: «{ag_name}»")

    # ----- 2) Business name (≤ 25) -----
    _ensure_brand_guidelines_open(driver)
    bn_input = _find_brand_business_name_input(driver)

    if not business_name:
        _emit(emit, "Придумаю краткое бизнес-имя (≤ 25)")
        try:
            business_name = _generate_business_name_llm(usp, site_url)
        except Exception as e:
            logger.warning("LLM business_name failed: %s — fallback.", e)
            business_name = _fallback_business_name(site_url, usp)
    business_name = _limit_len(_strip_quotes(business_name), 25)

    if not bn_input:
        _emit(emit, "Поле «Business name» не найдено — стоп")
        raise RuntimeError("Brand guidelines: поле 'Business name' не найдено.")
    _dispatch_input_change(driver, bn_input, business_name)
    time.sleep(0.15)
    logger.info("Business name установлен: %s", business_name)
    _emit(emit, f"Бизнес-имя: «{business_name}»")

    # ----- 3) Logos: Add → Upload → send_keys → 5s wait → Save -----
    logo_file = ""
    uploaded = False
    try:
        _emit(emit, "Добавляю логотип: открываю загрузчик")
        if not _open_logo_uploader(driver):
            _emit(emit, "Кнопка «Add logos» не нажалась — стоп")
            raise RuntimeError("Кнопка 'Add logos' не нажалась.")

        dialog = _wait_media_picker(driver, timeout=22.0)
        if not dialog:
            _emit(emit, "Диалог логотипов не появился — стоп")
            raise RuntimeError("Диалог выбора логотипов не появился.")

        # Промпт для Runware
        if logo_prompt and logo_prompt.strip():
            final_prompt = logo_prompt.strip()
        else:
            _emit(emit, "Собираю промпт для генерации лого")
            try:
                final_prompt = _generate_logo_prompt_llm(business_name, usp, site_url)
                if not final_prompt:
                    raise RuntimeError("empty llm prompt")
            except Exception as e:
                logger.warning("LLM logo prompt failed: %s — fallback.", e)
                final_prompt = _fallback_logo_prompt(business_name, usp, site_url)

        logger.info("Runware prompt: %s", _limit_len(final_prompt, 160))
        _emit(emit, "Генерирую логотип")

        # На вкладку Upload
        _select_tab_in_dialog(driver, dialog, ["upload", "загрузка", "загрузить"])
        time.sleep(0.2)

        # Генерация и сохранение файла
        cfg = _pick_runware_config()
        raw = _runware_generate_logo(final_prompt, cfg, width=512, height=512, retries=RUNWARE_RETRIES, timeout=int(timeout_total))
        logo_file = _write_logo_file(raw, suffix=".jpg")
        logger.info("Runware: логотип готов: %s", logo_file)
        _emit(emit, "Логотип сгенерирован — загружаю файл")

        # input[type=file] (без нативного окна)
        finp = _ensure_file_input_visible(driver, dialog)
        if not finp:
            logger.info("Upload input не найден сразу — пробую 'Upload from computer' (фолбэк)")
            _click_upload_from_computer(driver, dialog)
            for _ in range(35):
                finp = _ensure_file_input_visible(driver, dialog)
                if finp:
                    break
                time.sleep(0.2)

        if not finp:
            _emit(emit, "Не нашёл поле выбора файла — стоп")
            raise RuntimeError("input[type=file] не найден в диалоге Upload.")

        try:
            finp.send_keys(logo_file)   # абсолютный путь
        except Exception as e:
            _emit(emit, "Отправка файла не удалась — стоп")
            raise RuntimeError(f"send_keys(file) не удался: {e!s}")

        _emit(emit, "Жду обработку логотипа и сохраняю")
        if not _wait_5s_then_click_save(driver, dialog, min_wait_s=5.0, timeout_s=max(30.0, timeout_total/2)):
            _emit(emit, "Кнопка «Save» не активировалась — стоп")
            raise RuntimeError("Кнопка Save не стала активной после загрузки.")

        # Ждём закрытие диалога
        for _ in range(60):
            if not _get_media_dialog_root(driver):
                break
            time.sleep(0.2)

        # Проверяем появление миниатюры в основной галерее логотипов
        _emit(emit, "Проверяю, что логотип появился в галерее")
        for _ in range(40):
            try:
                ok = driver.execute_script(
                    """
                    const gal = document.querySelector('media-gallery.logo-gallery .gallery-container');
                    if(!gal) return false;
                    const hasThumb = gal.querySelector('img, .asset-thumbnail, .mdc-card__media');
                    return !!hasThumb;
                    """
                )
                if ok:
                    uploaded = True
                    break
            except Exception:
                pass
            time.sleep(0.25)

        if uploaded:
            logger.info("Логотип успешно добавлен.")
            _emit(emit, "Логотип добавлен")
        else:
            _emit(emit, "Не увидел миниатюру лого — шаг продолжу без него")
            raise RuntimeError("Не увидел миниатюру лого в галерее после сохранения.")

    except Exception as e:
        logger.warning("Логотип не был добавлен: %s", e)
        uploaded = False
        # не валим шаг — логотип опционален

    elapsed = int((time.time() - t0) * 1000)
    return {
        "asset_group_name": ag_name,
        "business_name": business_name,
        "logo_uploaded": uploaded,
        "logo_file": logo_file if uploaded else "",
        "duration_ms": elapsed,
    }
