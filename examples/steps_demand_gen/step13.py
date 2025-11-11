# -*- coding: utf-8 -*-
"""
examples/steps/step12.py

Шаг 13 (Demand Gen) — устойчивый аплоад логотипов в UI Google Ads.
Полный редизайн + детальные логи и диагностика.

СОХРАНЁННЫЙ КОНТРАКТ:
- run_step13(driver, *, mode, seed_assets, provided_assets, business_name, usp, site_url,
             campaign_context, desired_logo_count, timeout_total, emit) -> Dict
- run(...) — совместимость.

Ключевые улучшения:
  • BUGFIX: добавлена функция _select_panel(...) (раньше вызывалась, но не была определена).
  • Подробные логи по каждому действию (от поиска кнопки «Add…» до клика по Save/Done).
  • Верификация открытия модалки по росту числа .cdk-overlay-pane.
  • Снимки состояния диалога (okButtons, confirmBtn, loaders, canvases, editors, previews, fileHints).
  • Скриншоты в storage_dir/debug (включаются флагом DEBUG_SCREENSHOTS=1).
  • Повторная активация Upload-вкладки и ре-поиск свежего input перед КАЖДЫМ attach.
  • Любой сигнал приёмки: рост превью, имя файла, активный Save/Done, прогресс, кроппер.
  • Антидедупликация по пути/MD5/basename + skip, если имя уже «засветилось» в диалоге.
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import logging
import os
import random
import re
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import urlparse

import requests
from selenium.common.exceptions import StaleElementReferenceException  # type: ignore
from selenium.webdriver.common.action_chains import ActionChains  # type: ignore
from selenium.webdriver.common.by import By  # type: ignore
from selenium.webdriver.remote.webdriver import WebDriver, WebElement  # type: ignore

# Базовые утилиты шага (emit/guards/visibility/confirm)
from examples.steps import step4 as base_step4  # type: ignore

try:
    from PIL import Image  # type: ignore
except Exception:  # pragma: no cover
    Image = None  # type: ignore

try:
    from ads_ai.llm.gemini import GeminiClient  # type: ignore
except Exception:  # pragma: no cover
    GeminiClient = None  # type: ignore


# --------------------------------------------------------------------------------------
#                                    Л О Г И
# --------------------------------------------------------------------------------------

logger = logging.getLogger("ads_ai.gads.step13")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

_emit = base_step4._emit  # type: ignore
_dismiss_soft_dialogs = base_step4._dismiss_soft_dialogs  # type: ignore
_ensure_panel_open = base_step4._ensure_panel_open  # type: ignore
_is_interactable = base_step4._is_interactable  # type: ignore
_maybe_handle_confirm_its_you = base_step4._maybe_handle_confirm_its_you  # type: ignore


# --------------------------------------------------------------------------------------
#                                    К О Н С Т А Н Т Ы
# --------------------------------------------------------------------------------------

LLM_MODEL = os.getenv("GEMINI_MODEL", "models/gemini-2.0-flash")
STEP12_DISABLE_LLM = str(os.getenv("ADS_AI_STEP12_DISABLE_LLM", "")).strip().lower() in {"1", "true", "yes", "on"}

DEFAULT_RUNWARE_API_KEY = os.getenv("RUNWARE_API_KEY", "")
DEFAULT_RUNWARE_MODEL_ID = os.getenv("RUNWARE_MODEL_ID", "runware:100@1")
DEFAULT_RUNWARE_URL = os.getenv("RUNWARE_URL", "https://api.runware.ai/v1")

RUNWARE_IMAGE_WIDTH = 1024
RUNWARE_IMAGE_HEIGHT = 768
RUNWARE_LOGO_SIZE = 768
RUNWARE_RETRIES = 4

DEBUG_SCREENSHOTS = str(os.getenv("ADS_AI_STEP12_DEBUG_SHOTS", "")).strip().lower() in {"1", "true", "yes", "on"}

MEDIA_PANEL_SYNS: Sequence[str] = ("media", "assets", "медиа", "креатив", "изображения")
IMAGES_SECTION_SYNS: Sequence[str] = ("images", "изображения", "фото", "pictures")
LOGOS_SECTION_SYNS: Sequence[str] = ("logos", "логотипы", "лого", "brandmark")

IMAGE_ADD_BUTTON_SELECTOR = 'multi-asset-picker[debugid="image-picker"] material-button[debugid="add-asset"]'
LOGO_ADD_BUTTON_SELECTOR  = 'multi-asset-picker[debugid="logo-picker"] material-button[debugid="add-asset"]'

ASSET_DIALOG_ROOT_SELECTORS: Sequence[str] = (
    'focus-trap[aria-modal="true"]', 'focus-trap[role="dialog"]',
    '.cdk-overlay-pane focus-trap', '.cdk-overlay-pane [role="dialog"]',
    '.cdk-overlay-pane', 'focus-trap', '[role="dialog"]',
    'slidealog-wrapper[role="dialog"]',
)

UPLOAD_TAB_MATCHES: Sequence[str] = ("upload", "загруз", "добавить")

NEGATIVE_PROMPT = (
    "no text, no watermark, no blurry parts, no distorted hands, no extra limbs, "
    "no UI chrome, photorealistic, professional lighting"
)

# FileChooser — опционально (по умолчанию off)
_FILE_CHOOSER_SUPPORTED: Optional[bool] = True if str(os.getenv("ADS_AI_STEP12_ALLOW_FILE_CHOOSER", "")).strip().lower() in {"1", "true", "yes", "on"} else False

CONFIRM_BUTTON_PREFER_SELECTORS: Sequence[str] = (
    '[data-test-id="confirm-button"]',
    'material-button[data-test-id="confirm-button"]',
    '.confirm-button-group [data-test-id="confirm-button"]',
    '.footer [data-test-id="confirm-button"]',
)

CHOOSE_FILES_TEXT_MATCHES: Sequence[str] = (
    "upload from computer", "choose files to upload", "choose files",
    "загрузить с компьютера", "загрузка с компьютера",
    "выберите файлы для загрузки", "выберите файлы", "выбрать файлы",
)

DEFAULT_IMAGE_COUNT = 4
DEFAULT_LOGO_COUNT  = 2


# --------------------------------------------------------------------------------------
#                                     И С К Л Ю Ч Е Н И Я
# --------------------------------------------------------------------------------------

class Step12Error(RuntimeError):
    pass

class UiNotFound(Step12Error):
    pass

class UploadTimeout(Step12Error):
    pass

class GenerationError(Step12Error):
    pass


# --------------------------------------------------------------------------------------
#                                   В С П О М О Г А Т Е Л Ь Н О Е
# --------------------------------------------------------------------------------------

def _select_panel(driver: WebDriver, panel_syns: Sequence[str]) -> None:
    """
    ВАЖНО: В предыдущей версии вызывалась, но не была определена — из-за этого панель не открывалась.
    """
    ok = _ensure_panel_open(driver, list(panel_syns))
    logger.info("step13: ensure_panel_open(panel=%s) -> %s", list(panel_syns), ok)
    if not ok:
        raise UiNotFound(f"Панель с одним из заголовков {panel_syns} не найдена или неактивна.")

def _js_click(driver: WebDriver, element: WebElement) -> bool:
    try:
        driver.execute_script("try{arguments[0].scrollIntoView({block:'center',inline:'center'});}catch(_){ }", element)
        element.click()
        return True
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", element)
            return True
        except Exception:
            return False

def _mouse_click(driver: WebDriver, element: WebElement) -> bool:
    try:
        ActionChains(driver).move_to_element(element).pause(0.05).click().perform()
        return True
    except Exception:
        return _js_click(driver, element)

def _wait_for_any_selector(
    driver: WebDriver,
    selectors: Sequence[str],
    *,
    timeout: float = 10.0,
    require_visible: bool = True,
) -> Optional[WebElement]:
    deadline = time.time() + max(timeout, 0.5)
    candidates = [s for s in selectors if s]
    while time.time() < deadline:
        for sel in candidates:
            try:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
            except Exception:
                continue
            for el in els:
                if not require_visible or _is_interactable(driver, el):
                    return el
        time.sleep(0.2)
    return None

def _hash_file(path: str) -> Optional[str]:
    try:
        h = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None

def _dedupe_files(paths: Iterable[str]) -> List[str]:
    seen_paths: Set[str] = set()
    seen_hashes: Set[str] = set()
    seen_names: Set[str] = set()
    out: List[str] = []
    dropped: List[str] = []
    for raw in paths:
        if not raw:
            continue
        try:
            norm = str(Path(raw).expanduser().resolve())
        except Exception:
            norm = str(Path(raw).expanduser())
        name = Path(norm).name.lower()
        if norm in seen_paths or name in seen_names:
            dropped.append(name); continue
        h = _hash_file(norm)
        if h and h in seen_hashes:
            dropped.append(name); continue
        seen_paths.add(norm); seen_names.add(name)
        if h: seen_hashes.add(h)
        out.append(norm)
    if dropped:
        logger.warning("step13: удалены дубли перед загрузкой: %s", list(dict.fromkeys(dropped))[:8])
    return out

@dataclass
class TimerMarks:
    _anchor: float = field(default_factory=time.time)
    records: List[Tuple[str, int]] = field(default_factory=list)
    def mark(self, label: str) -> None:
        now = time.time()
        self.records.append((label, int((now - self._anchor) * 1000)))
        self._anchor = now


# --------------------------------------------------------------------------------------
#                              Д И А Г Н О С Т И К А / С К Р И Н Ы
# --------------------------------------------------------------------------------------

def _maybe_shot(driver: WebDriver, path: Path, label: str) -> None:
    if not DEBUG_SCREENSHOTS:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        driver.save_screenshot(str(path))
        logger.info("step13: screenshot saved [%s] -> %s", label, path)
    except Exception as exc:
        logger.debug("step13: screenshot failed [%s]: %s", label, exc)


# --------------------------------------------------------------------------------------
#                               С Л Е Д И М   К О Н Ф И Р М
# --------------------------------------------------------------------------------------

class _ConfirmWatcher:
    """Параллельный сторож диалогов Confirm-it's-you."""
    def __init__(self, driver: WebDriver, emit: Optional[Callable[[str], None]], interval: float = 0.35) -> None:
        self.drv = driver
        self.emit = emit
        self.interval = max(0.2, float(interval))
        self._stop = False
        self._thread: Optional[threading.Thread] = None
    def _loop(self) -> None:
        while not self._stop:
            try:
                _maybe_handle_confirm_its_you(self.drv, self.emit)
            except Exception:
                pass
            time.sleep(self.interval)
    def __enter__(self) -> "_ConfirmWatcher":
        _maybe_handle_confirm_its_you(self.drv, self.emit)
        self._stop = False
        self._thread = threading.Thread(target=self._loop, name="step12-confirm-watcher", daemon=True)
        self._thread.start()
        return self
    def __exit__(self, exc_type, exc, tb) -> bool:
        self._stop = True
        if self._thread:
            self._thread.join(timeout=1.2)
        return False


# --------------------------------------------------------------------------------------
#                              Д И А Л О Г   U P L O A D
# --------------------------------------------------------------------------------------

class UploadDialog:
    """Работа с модалкой ассетов: открытие, вкладка Upload, input[type=file], Save/Done, кроппер, ack-сигналы."""

    def __init__(self, driver: WebDriver, kind: str, storage_dir: Path):
        self.drv = driver
        self.kind = "images" if (kind or "").lower() != "logos" else "logos"
        self.root: Optional[WebElement] = None
        self.storage_dir = storage_dir

    def _overlay_count(self) -> int:
        try:
            return int(self.drv.execute_script("return document.querySelectorAll('.cdk-overlay-pane').length||0;") or 0)
        except Exception:
            return 0

    # ---------- Opening ----------

    def open(self) -> None:
        logger.info("step13: [%s] открытие диалога — ensure panel + поиск кнопки Add", self.kind)
        _select_panel(self.drv, MEDIA_PANEL_SYNS)

        synonyms = IMAGES_SECTION_SYNS if self.kind == "images" else LOGOS_SECTION_SYNS
        try:
            self.drv.execute_script(
                """
                const tags = arguments[0]; const set=new Set(tags.map(t=>String(t||'').toLowerCase()));
                const hs=[...document.querySelectorAll('element-title span, material-expansionpanel .header, h2, h3')];
                for(const h of hs){ const txt=(h.innerText||h.textContent||'').trim().toLowerCase();
                  if(!txt) continue; if([...set].some(t=>txt.includes(t))){ try{h.scrollIntoView({block:'center'})}catch(_){ } break; } }
                """,
                list(synonyms),
            )
        except Exception:
            pass

        before = self._overlay_count()
        logger.info("step13: overlay panes before click: %d", before)

        btn_selector = IMAGE_ADD_BUTTON_SELECTOR if self.kind == "images" else LOGO_ADD_BUTTON_SELECTOR
        btn = _wait_for_any_selector(self.drv, [btn_selector], timeout=3.0, require_visible=True)
        if not btn:
            # tolerant-поиск по текстам
            try:
                btn = self.drv.execute_script(
                    """
                    const kind=String(arguments[0]||'').toLowerCase();
                    const keys = kind==='images'
                      ? ['add images','add image','добавить изображ','загрузить изображ','фото','картин']
                      : ['add logos','add logo','добавить лого','добавить логотип','загрузить логотип'];
                    const isVis=e=>{ if(!e) return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
                      if(cs.display==='none'||cs.visibility==='hidden'||parseFloat(cs.opacity||'1')<.2) return false; return r.width>10&&r.height>10&&r.bottom>0&&r.right>0; };
                    const btns=[...document.querySelectorAll('button,[role=button],material-button,a[role=button]')].filter(isVis);
                    return btns.find(b=>{const t=((b.getAttribute('aria-label')||'')+' '+(b.innerText||b.textContent||'')).toLowerCase();
                      return t && keys.some(k=>t.includes(k)); }) || null;
                    """,
                    self.kind,
                )
            except Exception:
                btn = None

        if not btn:
            raise UiNotFound(f"Кнопка добавления ассетов для {self.kind} не найдена.")

        if not _js_click(self.drv, btn):
            raise UiNotFound(f"Не удалось кликнуть по кнопке добавления {self.kind}.")

        # ждём рост оверлеев ИЛИ появление известных корней
        for i in range(48):
            now = self._overlay_count()
            dlg = _wait_for_any_selector(self.drv, ASSET_DIALOG_ROOT_SELECTORS, timeout=0.25, require_visible=False)
            logger.debug("step13: wait modal (%s) tick=%d | overlays %d→%d | dlg=%s", self.kind, i, before, now, bool(dlg))
            if now > before or dlg:
                self.root = dlg or _wait_for_any_selector(self.drv, ASSET_DIALOG_ROOT_SELECTORS, timeout=3.0, require_visible=False)
                break
            time.sleep(0.1)

        if not self.root:
            _maybe_shot(self.drv, self.storage_dir / "debug" / f"open_{self.kind}_failed.png", f"open-{self.kind}-failed")
            raise UiNotFound("Диалог загрузки ассетов не появился.")
        logger.info("step13: открыт диалог %s (overlays=%d)", self.kind, self._overlay_count())
        _maybe_shot(self.drv, self.storage_dir / "debug" / f"open_{self.kind}_ok.png", f"open-{self.kind}-ok")

    # ---------- Upload tab ----------

    def _find_upload_tab(self) -> Optional[WebElement]:
        if not self.root:
            return None
        try:
            return self.drv.execute_script(
                """
                const root=arguments[0]; const labels=new Set((arguments[1]||[]).map(s=>String(s||'').toLowerCase()));
                const scopes=[root, root.shadowRoot, root.closest?.('.cdk-overlay-pane')||null, document].filter(Boolean);
                const isVis=e=>{ if(!e) return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
                  if(cs.display==='none'||cs.visibility==='hidden'||parseFloat(cs.opacity||'1')<.2) return false; return r.width>8&&r.height>8&&r.right>0&&r.bottom>0; };
                for(const sc of scopes){
                  const nodes=sc.querySelectorAll?.('[role="tab"], tab-button, material-tab, .mdc-tab, button[aria-label], .tab-button, button.tab-button')||[];
                  for(const n of nodes){
                    if(!isVis(n)) continue;
                    const t=((n.getAttribute('aria-label')||'')+' '+(n.innerText||n.textContent||'')).toLowerCase();
                    if(!t) continue; for(const lbl of labels){ if(lbl && t.includes(lbl)) return n; }
                  }
                }
                return null;
                """,
                self.root, list(UPLOAD_TAB_MATCHES),
            )
        except Exception:
            return None

    @staticmethod
    def _wait_tab_selected(el: WebElement, timeout: float) -> bool:
        dl = time.time() + max(0.2, timeout)
        while time.time() < dl:
            try:
                aria = (el.get_attribute("aria-selected") or "").lower() == "true"
                cls  = (el.get_attribute("class") or "").lower()
                if aria or any(k in cls for k in ("active", "selected", "mdc-tab--active")):
                    return True
            except StaleElementReferenceException:
                return True
            except Exception:
                pass
            time.sleep(0.1)
        return False

    def ensure_upload_tab(self) -> bool:
        for attempt in range(1, 4):
            tab = self._find_upload_tab()
            logger.info("step13: [%s] попытка активировать вкладку Upload #%d -> %s", self.kind, attempt, bool(tab))
            if not tab:
                time.sleep(0.25); continue
            if _mouse_click(self.drv, tab) and self._wait_tab_selected(tab, 2.0):
                _maybe_shot(self.drv, self.storage_dir / "debug" / f"tab_upload_{self.kind}_ok.png", f"tab-upload-{self.kind}-ok")
                return True
            time.sleep(0.25)
        _maybe_shot(self.drv, self.storage_dir / "debug" / f"tab_upload_{self.kind}_fail.png", f"tab-upload-{self.kind}-fail")
        return False

    # ---------- File input ----------

    def _ensure_file_input_visible(self, scope: WebElement) -> Optional[WebElement]:
        try:
            return self.drv.execute_script(
                """
                const root=arguments[0];
                const sels=[
                  'drop-zone input[type="file"]','.drop-zone input[type="file"]','.drop-zone-container input[type="file"]',
                  '.asset-uploader input[type="file"]','.drop-body input[type="file"]','assets-upload-tab input[type="file"]',
                  '.upload-button input[type="file"]','input[type="file"][accept*="image"]','input[type="file"][multiple]','input[type="file"]'
                ];
                let input=null;
                for(const s of sels){ try{ const v=root.querySelector(s); if(v){ input=v; break; } }catch(_){ } }
                if(!input){ const pane=root.closest?.('.cdk-overlay-pane')||document;
                  for(const s of sels){ const v=pane.querySelector(s); if(v){ input=v; break; } } }
                if(!input) return null;
                const st=input.style;
                st.setProperty('display','block','important'); st.setProperty('visibility','visible','important'); st.setProperty('opacity','1','important');
                st.setProperty('position','absolute','important'); st.setProperty('left','0px','important'); st.setProperty('top','0px','important');
                st.setProperty('width','2px','important'); st.setProperty('height','2px','important'); st.setProperty('z-index','2147483647','important');
                st.setProperty('pointer-events','auto','important');
                input.removeAttribute('hidden'); input.removeAttribute('aria-hidden'); input.classList?.remove?.('hidden','cdk-visually-hidden');
                try{ (input.closest('drop-zone,.drop-zone,.drop-zone-container,.drop-body,assets-upload-tab,.asset-uploader')||input).scrollIntoView({block:'center'}) }catch(_){ }
                return input;
                """,
                scope,
            )
        except Exception:
            return None

    def _click_upload_from_computer(self) -> bool:
        if not self.root:
            return False
        try:
            btn = self.drv.execute_script(
                """
                const dlg=arguments[0]; const keys=new Set((arguments[1]||[]).map(s=>String(s||'').toLowerCase()));
                const isVis=e=>{ if(!e) return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
                  if(cs.display==='none'||cs.visibility==='hidden'||parseFloat(cs.opacity||'1')<.2) return false; return r.width>10&&r.height>10&&r.right>0&&r.bottom>0; };
                const btns=[...dlg.querySelectorAll('button,[role=button],material-button,a[role=button]')].filter(isVis);
                return btns.find(b=>{const t=((b.getAttribute('aria-label')||'')+' '+(b.innerText||b.textContent||'')).toLowerCase();
                  return [...keys].some(k=>t.includes(k)); }) || null;
                """,
                self.root, list(CHOOSE_FILES_TEXT_MATCHES),
            )
            if not btn:
                logger.info("step13: [%s] кнопка 'Upload from computer' не найдена", self.kind)
                return False
            ok = _mouse_click(self.drv, btn)
            logger.info("step13: [%s] клик по 'Upload from computer' -> %s", self.kind, ok)
            return ok
        except Exception:
            return False

    def locate_input(self, *, try_click_upload_button: bool = True) -> Optional[WebElement]:
        if not self.root:
            return None
        el = self._ensure_file_input_visible(self.root)
        logger.info("step13: [%s] поиск input[type=file] первичный -> %s", self.kind, bool(el))
        if el:
            return el
        if try_click_upload_button:
            self._click_upload_from_computer()
            for i in range(35):
                el = self._ensure_file_input_visible(self.root)
                logger.debug("step13: [%s] re-check input[type=file] #%d -> %s", self.kind, i + 1, bool(el))
                if el:
                    return el
                time.sleep(0.15)
        return None

    # ---------- State snapshots / save ----------

    def snapshot(self) -> Dict[str, Any]:
        if not self.root:
            return {}
        try:
            return self.drv.execute_script(
                """
                const dlg=arguments[0]; const pane=dlg?.closest?.('.cdk-overlay-pane')||dlg||document;
                const isVis = el => { if(!el) return false; const cs=getComputedStyle(el),r=el.getBoundingClientRect();
                  if(cs.display==='none'||cs.visibility==='hidden'||parseFloat(cs.opacity||'1')<.2) return false;
                  return r.width>6&&r.height>6&&r.right>0&&r.bottom>0; };
                const textOf = el => (el && (el.innerText||el.textContent)||'').trim();
                const qa = sel => [...pane.querySelectorAll(sel)];

                const okButtons=[]; qa('material-button, button, [role="button"], .mdc-button').forEach(b=>{
                  if(!isVis(b)) return;
                  const t=(textOf(b)+' '+(b.getAttribute('aria-label')||'')).toLowerCase();
                  const dis=(b.getAttribute('aria-disabled')||'')==='true'||b.hasAttribute('disabled')||(b.className||'').toLowerCase().includes('is-disabled');
                  if(/(save|select|done|add|continue|apply|применить|продолжить|сохран|готово|выбрать|добавить)/.test(t)) okButtons.push({text:t,disabled:!!dis});
                });

                let confirmBtnState={exists:false,disabled:true};
                try{
                  const sels=['[data-test-id="confirm-button"]','material-button[data-test-id="confirm-button"]','.confirm-button-group [data-test-id="confirm-button"]','.footer [data-test-id="confirm-button"]'];
                  let el=null; for(const s of sels){ const found=pane.querySelectorAll(s); for(const b of found){ if(isVis(b)){ el=b; break; } } if(el) break; }
                  if(!el && pane!==document){ for(const s of sels){ const found=document.querySelectorAll(s); for(const b of found){ if(isVis(b)){ el=b; break; } } if(el) break; } }
                  if(el){ const dis=(el.getAttribute('aria-disabled')||'')==='true'||el.hasAttribute('disabled')||(el.className||'').toLowerCase().includes('is-disabled'); confirmBtnState={exists:true,disabled:!!dis}; }
                }catch(_){}

                const fileHints=[]; const add=val=>{const s=String(val||'').trim(); if(s) fileHints.push(s);};
                const nodes=new Set(); const push=(sel)=>{ try{ qa(sel).forEach(x=>nodes.add(x)); }catch(_){ } };
                ['.row','.tile','.upload-item','.asset-tile','li','.mdc-list-item','.filename','.file-name','[data-asset-id]','[data-row-id]','[data-qa-id*="asset"]','[data-test-id*="asset"]','[class*="asset-card"]','[class*="asset-row"]','[class*="asset-item"]','[class*="preview"]','[class*="thumb"]','[class*="thumbnail"]','material-card','upload-asset-card','gm-asset-card','[aria-label*=".jpg"]','[aria-label*=".jpeg"]','[aria-label*=".png"]','[aria-label*=".webp"]','[aria-label*=".svg"]','[title*=".jpg"]','[title*=".jpeg"]','[title*=".png"]','[title*=".webp"]','[title*=".svg"]'].forEach(push);
                nodes.forEach(n=>{ add(textOf(n)); try{ add(n.getAttribute('aria-label')); add(n.getAttribute('title')); add(n.getAttribute('data-filename')); add(n.getAttribute('data-file-name')); add(n.getAttribute('data-name')); add(n.getAttribute('data-asset-name')); }catch(_){ }
                  try{ const ds=n.dataset||{}; ['assetName','filename','fileName','name','value','assetname'].forEach(k=>{ if(ds[k]) add(ds[k]); }); }catch(_){ } });

                const previews = (()=>{
                  const sels=['assets-upload-tab .preview','assets-upload-tab .upload-item','assets-upload-tab img','.asset-preview','.uploaded-file','.upload-card','.image-preview','.image-tile','.asset-tile','.mdc-card__media','asset-tile img','img[src^="blob:"]','img[src^="data:"]','.library-grid .tile img','.library-list .row img','img[src*="googleusercontent.com"]','[class*="thumb"] img','[class*="preview"] img','[data-asset-id] img','[data-row-id] img','[class*="asset-card"] img','[class*="upload-card"] img'];
                  const seen=new Set(); let cnt=0;
                  const addIf=n=>{ if(n&&isVis(n)&&!seen.has(n)){ seen.add(n); cnt++; } };
                  for(const s of sels){ qa(s).forEach(addIf); }
                  qa('[style*="background-image"]').forEach(node=>{
                    if(!isVis(node)) return; const bg=(node.style&&node.style.backgroundImage)||getComputedStyle(node).backgroundImage||''; if(!bg) return;
                    const low=bg.toLowerCase(); if(!/(blob:|data:image|googleusercontent\\.com)/.test(low)) return;
                    const host=node.closest('assets-upload-tab,.asset-uploader,.drop-zone,.drop-body,[data-test-id*="asset"],[class*="asset"],[class*="upload"]'); if(host && pane.contains(host)) addIf(node);
                  });
                  qa('[data-asset-id], [data-row-id], [data-qa-id*="asset"], [data-test-id*="asset"], [class*="asset-card"], material-card').forEach(card=>{
                    if(!isVis(card)||seen.has(card)) return;
                    const inner=card.querySelector('img,canvas,video'); if(inner && isVis(inner)) addIf(card); else {
                      const bg=(card.style&&card.style.backgroundImage)||getComputedStyle(card).backgroundImage||'';
                      if(bg && /(blob:|data:image|googleusercontent\\.com)/.test(bg.toLowerCase())) addIf(card);
                    }
                  });
                  return cnt;
                })();

                let onUpload=false;
                try{ qa('[role="tab"], material-tab, .mdc-tab').forEach(t=>{
                  const txt=(textOf(t)+' '+(t.getAttribute('aria-label')||'')).toLowerCase();
                  const active=(t.getAttribute('aria-selected')||'')==='true'||(t.className||'').toLowerCase().includes('active');
                  if(/(upload|загруз)/.test(txt) && active) onUpload=true; });
                }catch(_){}

                const loaders=qa('.progress, .mdc-linear-progress, [aria-busy="true"], .is-loading, .loading, mat-progress-bar').length;
                const canvases=qa('canvas').length;
                const editors=qa('[class*="crop"], [class*="editor"], [class*="edit"], [class*="adjust"]').length;

                const inputFileNames=[]; try{ pane.querySelectorAll('input[type="file"]').forEach(inp=>{
                  if(inp.files && inp.files.length){ for(let i=0;i<Math.min(6,inp.files.length);i++){ const f=inp.files[i]; if(f && f.name) inputFileNames.push(f.name); } } }); }catch(_){}

                const errorMessages=[]; try{
                  const es=['[role="alert"]','.error','.error-message','.error-text','.warning','.warning-message','.mdc-snackbar','.mat-snack-bar-container','[class*="error"]','[class*="alert"]','[class*="warning"]','[data-test-id*="error"]','[data-test-id*="alert"]'];
                  const seen=new Set();
                  es.forEach(s=>{ try{ qa(s).forEach(e=>{ if(!isVis(e)) return; const txt=textOf(e); if(!txt||txt.length<3) return;
                    const l=txt.toLowerCase(); if(l.includes('error')||l.includes('warning')||l.includes('invalid')||l.includes('failed')||l.includes('rejected')||l.includes('not allowed')||l.includes('ошибк')||l.includes('недопустим')||l.includes('отклонен')||l.includes('не удалось')){
                      if(!seen.has(txt)&&txt.length<500){ errorMessages.push(txt); seen.add(txt); } } }); }catch(_){ } });
                }catch(_){}

                return { okButtons, confirmBtnState, loaders, canvases, editors, previews,
                         fileHints: fileHints.slice(0,14), inputFileNames: inputFileNames.slice(0,14),
                         onUpload, errorMessages: errorMessages.slice(0,5) };
                """,
                self.root,
            ) or {}
        except Exception:
            return {}

    def _button_is_enabled(self, el: WebElement) -> bool:
        try:
            aria = (el.get_attribute("aria-disabled") or "").lower().strip()
            dis  = el.get_attribute("disabled")
            cls  = (el.get_attribute("class") or "").lower()
            return not dis and (aria not in {"true", "1"}) and ("is-disabled" not in cls)
        except Exception:
            return False

    def _locate_confirm_button(self, *, enabled_only: bool = True) -> Optional[WebElement]:
        if not self.root:
            return None
        try:
            el = self.drv.execute_script(
                """
                const pane=arguments[0]?.closest?.('.cdk-overlay-pane')||arguments[0]||document;
                const sels=arguments[1]||[];
                const isVis=el=>{ if(!el) return false; const cs=getComputedStyle(el),r=el.getBoundingClientRect?.()||{width:0,height:0,bottom:0,right:0};
                  if(cs.display==='none'||cs.visibility==='hidden'||parseFloat(cs.opacity||'1')<.2) return false; return r.width>10&&r.height>10&&r.bottom>0&&r.right>0; };
                for(const s of sels){ const found=pane.querySelectorAll(s); for(const b of found){ if(isVis(b)) return b; } }
                if(pane!==document){ for(const s of sels){ const found=document.querySelectorAll(s); for(const b of found){ if(isVis(b)) return b; } } }
                return null;
                """,
                self.root, list(CONFIRM_BUTTON_PREFER_SELECTORS),
            )
        except Exception:
            el = None
        if not el and self.root:
            try:
                el = self.drv.execute_script(
                    """
                    const pane=arguments[0]?.closest?.('.cdk-overlay-pane')||arguments[0]||document;
                    const isVis=el=>{ if(!el) return false; const cs=getComputedStyle(el),r=el.getBoundingClientRect();
                      if(cs.display==='none'||cs.visibility==='hidden'||parseFloat(cs.opacity||'1')<.2) return false; return r.width>10&&r.height>10&&r.bottom>0&&r.right>0; };
                    const btns=[...pane.querySelectorAll('button,[role=button],material-button,.mdc-button')].filter(isVis);
                    return btns.find(b=>{const t=((b.getAttribute('aria-label')||'')+' '+(b.innerText||b.textContent||'')).toLowerCase();
                      const dis=(b.getAttribute('aria-disabled')||'')==='true'||b.hasAttribute('disabled')||b.classList.contains('is-disabled');
                      return !dis && /(save|select|done|add|continue|apply|применить|продолжить|сохран|готово|выбрать|добавить)/.test(t);
                    })||null;
                    """,
                    self.root,
                )
            except Exception:
                el = None
        if not el:
            return None
        return el if (not enabled_only or self._button_is_enabled(el)) else None

    def wait_save_and_click(self, *, min_wait: float = 3.0, timeout: float = 120.0) -> bool:
        dl = time.time() + max(1.0, timeout)
        earliest = time.time() + max(0.0, min_wait)
        logger.info("step13: жду активации Save/Select/Done (мин. ожидание %.1fs, таймаут %.1fs)", min_wait, timeout)
        while time.time() < dl:
            btn = self._locate_confirm_button(enabled_only=True)
            if not btn:
                probe = self._locate_confirm_button(enabled_only=False)
                if probe:
                    logger.info("step13: confirm-button присутствует, но дизейблен — ждём…")
            else:
                if time.time() >= earliest and self._button_is_enabled(btn):
                    try:
                        self.drv.execute_script("try{arguments[0].scrollIntoView({block:'center'})}catch(_){ }", btn)
                    except Exception:
                        pass
                    if _mouse_click(self.drv, btn):
                        time.sleep(0.25)
                        _maybe_shot(self.drv, self.storage_dir / "debug" / f"click_save_{self.kind}.png", f"click-save-{self.kind}")
                        logger.info("step13: клик по Save/Select/Done -> OK")
                        return True
            time.sleep(0.25)
        return False

    def wait_closed(self, *, timeout: float = 15.0) -> bool:
        if not self.root:
            return True
        logger.info("step13: ожидаю закрытия диалога (%s), таймаут=%.1fs", self.kind, timeout)
        dl = time.time() + max(0.5, timeout)
        while time.time() < dl:
            try:
                still = self.drv.execute_script(
                    """
                    const dlg=arguments[0]; if(!dlg) return false;
                    const pane=dlg.closest?.('.cdk-overlay-pane');
                    const inDom=document.body && (document.body.contains(dlg)||(pane&&document.body.contains(pane)));
                    const vis=inDom ? (getComputedStyle(pane||dlg).display!=='none' && getComputedStyle(pane||dlg).visibility!=='hidden') : false;
                    return inDom && vis;
                    """,
                    self.root,
                )
                if not still:
                    logger.info("step13: диалог закрыт (%s)", self.kind)
                    return True
            except Exception:
                return True
            time.sleep(0.2)
        logger.info("step13: диалог (%s) не закрылся вовремя", self.kind)
        return False


# --------------------------------------------------------------------------------------
#                              П Р И Ё М К А   /   Э В Е Н Т Ы
# --------------------------------------------------------------------------------------

def _extract_names_from_snap(snap: Dict[str, Any]) -> Set[str]:
    names: Set[str] = set()
    for raw in snap.get("fileHints") or []:
        s = str(raw or "")
        for m in re.findall(r"([A-Za-z0-9._-]+\.(?:jpe?g|png|webp|svg))", s, flags=re.I):
            names.add(m.lower())
    for raw in snap.get("inputFileNames") or []:
        s = str(raw or "").strip().lower()
        if s:
            names.add(s)
    return names

def _detect_editor(snap: Dict[str, Any]) -> bool:
    return int(snap.get("canvases") or 0) > 0 or int(snap.get("editors") or 0) > 0

def _ok_button_enabled_snap(snap: Dict[str, Any]) -> bool:
    for b in snap.get("okButtons") or []:
        if isinstance(b, dict) and not bool(b.get("disabled")):
            return True
    st = snap.get("confirmBtnState") or {}
    return bool(isinstance(st, dict) and st.get("exists") and not st.get("disabled"))

def _synthesize_drop_events(driver: WebDriver, input_el: WebElement) -> None:
    try:
        driver.execute_script(
            """
            const input=arguments[0];
            const fire=(el,ev)=>{ try{ el && el.dispatchEvent(new Event(ev,{bubbles:true,cancelable:true})); }catch(_){ } };
            fire(input,'input'); fire(input,'change');
            const zone=input.closest('drop-zone,.drop-zone,.drop-zone-container,.drop-body,assets-upload-tab,.asset-uploader');
            if(zone){ fire(zone,'input'); fire(zone,'change'); }
            try{
              const files=input.files;
              if(files && files.length && zone && window.DataTransfer && window.DragEvent){
                const dt=new DataTransfer(); for(let i=0;i<files.length;i++) dt.items.add(files[i]);
                const de=type=>zone.dispatchEvent(new DragEvent(type,{dataTransfer:dt,bubbles:true,cancelable:true}));
                de('dragenter'); de('dragover'); de('drop');
              }
            }catch(_){}
            """,
            input_el,
        )
    except Exception:
        pass


# --------------------------------------------------------------------------------------
#                           CDP / TOKEN  /  FILECHOOSER
# --------------------------------------------------------------------------------------

def _ensure_input_token(driver: WebDriver, input_el: WebElement, token: Optional[str] = None) -> str:
    value = token or uuid.uuid4().hex[:10]
    try:
        driver.execute_script(
            """
            const el=arguments[0], val=arguments[1];
            try{ el.setAttribute('data-step12-token', val); }catch(_){}
            try{ window.__step12Inputs = window.__step12Inputs || {}; window.__step12Inputs[val]=el; }catch(_){}
            return val;
            """,
            input_el, value,
        )
    except Exception:
        pass
    return value

def _scan_for_token_node(node: Dict[str, Any], token: str) -> Optional[int]:
    stack = [node]
    while stack:
        cur = stack.pop()
        attrs = {}
        try:
            raw = cur.get("attributes") or []
            it = iter(raw)
            while True:
                try:
                    k = next(it); v = next(it)
                    attrs[str(k)] = str(v)
                except StopIteration:
                    break
        except Exception:
            pass
        if attrs.get("data-step12-token") == token and (cur.get("nodeName") or "").lower() == "input":
            nid = cur.get("nodeId")
            if nid:
                return int(nid)
        for key in ("children", "shadowRoots", "pseudoElements"):
            arr = cur.get(key) or []
            if isinstance(arr, list):
                stack.extend(arr)
        for key in ("templateContent", "contentDocument"):
            sub = cur.get(key)
            if isinstance(sub, dict):
                stack.append(sub)
    return None

def _cdp_attach_files_by_token(driver: WebDriver, token: str, files: List[str]) -> bool:
    try:
        expr = (
            "(()=>{const tok=%s;const map=window.__step12Inputs||{};"
            "const el=(map&&map[tok])&&document.contains(map[tok])?map[tok]:document.querySelector('input[type=\"file\"][data-step12-token=\"'+tok+'\"]');"
            "if(el){window.__step12Inputs=window.__step12Inputs||{};window.__step12Inputs[tok]=el;}return el||null;})()"
        ) % json.dumps(token)
        rt = driver.execute_cdp_cmd("Runtime.evaluate", {"expression": expr, "objectGroup": "step12", "includeCommandLineAPI": False, "silent": True}) or {}
        obj = rt.get("result") or {}
        object_id = obj.get("objectId"); backend_id = obj.get("backendNodeId"); node_id = obj.get("nodeId")

        if object_id:
            try:
                info = driver.execute_cdp_cmd("DOM.requestNode", {"objectId": object_id}) or {}
                node_id = info.get("nodeId") or node_id
                backend_id = info.get("backendNodeId") or backend_id
            except Exception:
                pass

        if not node_id:
            doc = driver.execute_cdp_cmd("DOM.getDocument", {"depth": -1, "pierce": True}) or {}
            root = doc.get("root") or {}
            nid = _scan_for_token_node(root, token)
            if nid:
                node_id = nid

        if node_id:
            try:
                driver.execute_cdp_cmd("DOM.focus", {"nodeId": node_id})
            except Exception:
                pass
            driver.execute_cdp_cmd("DOM.setFileInputFiles", {"nodeId": node_id, "files": files})
            return True

        if backend_id:
            try:
                pushed = driver.execute_cdp_cmd("DOM.pushNodesByBackendIdsToFrontend", {"backendNodeIds": [backend_id]}) or {}
                ids = pushed.get("nodeIds") or []
                if ids:
                    nid = ids[0]
                    try:
                        driver.execute_cdp_cmd("DOM.focus", {"nodeId": nid})
                    except Exception:
                        pass
                    driver.execute_cdp_cmd("DOM.setFileInputFiles", {"nodeId": nid, "files": files})
                    return True
            except Exception:
                pass

        return False
    except Exception:
        return False

def _dispatch_input_change_by_token(driver: WebDriver, token: str) -> None:
    try:
        driver.execute_script(
            """
            const tok=arguments[0]; window.__step12Inputs=window.__step12Inputs||{};
            let input=window.__step12Inputs[tok];
            if(!input || !document.contains(input)){ input=document.querySelector('input[type="file"][data-step12-token="'+tok+'"]'); if(input) window.__step12Inputs[tok]=input; }
            const fire=(el,ev)=>{ try{el.dispatchEvent(new Event(ev,{bubbles:true,cancelable:true}))}catch(_){ } };
            if(input){ fire(input,'input'); fire(input,'change'); const zone=input.closest('drop-zone,.drop-zone,.drop-zone-container,.drop-body,assets-upload-tab,.asset-uploader'); if(zone){ fire(zone,'input'); fire(zone,'change'); } }
            """,
            token,
        )
    except Exception:
        pass

def _file_chooser_attach(driver: WebDriver, input_el: WebElement, files: List[str]) -> bool:
    if not _FILE_CHOOSER_SUPPORTED:
        return False
    try:
        try:
            driver.execute_cdp_cmd("Page.setInterceptFileChooserDialog", {"enabled": True})
        except Exception:
            return False
        try:
            input_el.click()
        except Exception:
            try:
                driver.execute_script("arguments[0].click();", input_el)
            except Exception:
                return False
        time.sleep(0.3)
        try:
            driver.execute_cdp_cmd("Page.handleFileChooserDialog", {"action": "accept", "files": files})
            return True
        except Exception:
            return False
    finally:
        try:
            driver.execute_cdp_cmd("Page.setInterceptFileChooserDialog", {"enabled": False})
        except Exception:
            pass


# --------------------------------------------------------------------------------------
#                           О С Н О В Н О Й   А П Л О А Д Е Р
# --------------------------------------------------------------------------------------

class Uploader:
    """Крепление файлов в диалог с подтверждением «по любому сигналу» и защитой от дублей."""

    def __init__(self, driver: WebDriver, dialog: UploadDialog, *, kind: str):
        self.drv = driver
        self.dlg = dialog
        self.kind = kind
        self.known_names: Set[str] = set()   # имена, засветившиеся в диалоге
        self.attempted: Set[str] = set()     # имена, по которым уже делали attach()

    def _input_supports_multiple(self, input_el: WebElement) -> bool:
        try:
            attr = (input_el.get_attribute("multiple") or "").strip().lower()
        except Exception:
            attr = ""
        if attr and attr not in {"false", "0", "null"}:
            return True
        try:
            return bool(self.drv.execute_script("return !!(arguments[0] && arguments[0].multiple);", input_el))
        except Exception:
            return False

    def _ack_wait(self, start_count: int, filename: Optional[str], *, is_first: bool, t_first: float = 24.0, t_next: float = 10.0) -> Tuple[bool, Dict[str, Any]]:
        timeout = float(t_first if is_first else t_next)
        dl = time.time() + max(0.5, timeout)
        last_snap: Dict[str, Any] = {}
        while time.time() < dl:
            now_count = int((self.dlg.snapshot() or {}).get("previews") or 0)
            snap = self.dlg.snapshot()
            last_snap = snap
            errors = snap.get("errorMessages") or []
            if errors:
                logger.warning("step13: ошибки диалога: %s", "; ".join(errors[:3]))
            if now_count >= start_count + 1:
                return True, snap
            if filename and Path(filename).name.lower() in _extract_names_from_snap(snap):
                return True, snap
            if _ok_button_enabled_snap(snap) or _detect_editor(snap) or int(snap.get("loaders") or 0) > 0:
                return True, snap
            time.sleep(0.25)
        return False, last_snap

    def _refresh_latest_input(self, current: WebElement, token: Optional[str]) -> WebElement:
        try:
            return self.drv.execute_script(
                """
                const old=arguments[0], tok=String(arguments[1]||'').trim();
                const cs=document.querySelectorAll('input[type="file"]'); if(!cs||!cs.length) return old;
                const fresh=cs[cs.length-1]; const st=fresh.style;
                st.setProperty('display','block','important'); st.setProperty('visibility','visible','important'); st.setProperty('opacity','1','important');
                st.setProperty('position','absolute','important'); st.setProperty('left','0px','important'); st.setProperty('top','0px','important');
                st.setProperty('width','2px','important'); st.setProperty('height','2px','important'); st.setProperty('z-index','2147483647','important'); st.setProperty('pointer-events','auto','important');
                fresh.removeAttribute('hidden'); fresh.removeAttribute('aria-hidden'); fresh.classList?.remove?.('hidden','cdk-visually-hidden');
                if(tok){ try{ fresh.setAttribute('data-step12-token', tok); window.__step12Inputs=window.__step12Inputs||{}; window.__step12Inputs[tok]=fresh; }catch(_){ } }
                return fresh;
                """,
                current, token or "",
            ) or current
        except Exception:
            return current

    def _log_input_snapshot(self, input_el: Optional[WebElement], *, label: str) -> None:
        info = {}
        try:
            info = self.drv.execute_script(
                """
                const el=arguments[0];
                if(!el) return {exists:false};
                const r=el.getBoundingClientRect?.()||{width:0,height:0,top:0,left:0};
                const files=[]; if(el.files && el.files.length){ for(let i=0;i<Math.min(4,el.files.length);i++){ const f=el.files[i]; files.push({name:f&&f.name||'?', size:f&&f.size||0, type:f&&f.type||''}); } }
                return { exists:true, multiple:!!el.multiple, disabled:!!el.disabled, accept: el.getAttribute('accept')||'',
                         rect:{w:r.width,h:r.height,t:r.top,l:r.left}, files_len:(el.files&&el.files.length)||0, files_preview:files };
                """,
                input_el,
            ) or {}
        except Exception:
            pass
        logger.info("step13: [%s] input snapshot = %s", label, info)

    def attach(self, files: List[str], *, prefer_cdp: bool = True) -> None:
        if not self.dlg.root:
            raise UiNotFound("Диалог не инициализирован.")

        files = _dedupe_files(files)
        if not files:
            raise Step12Error("Нет валидных файлов для загрузки.")

        base_count = int((self.dlg.snapshot() or {}).get("previews") or 0)
        snap0 = self.dlg.snapshot()
        self.known_names |= _extract_names_from_snap(snap0)
        logger.info("step13: старт attach (%s), базовый previews=%d, известные имена=%s",
                    self.kind, base_count, list(sorted(self.known_names))[:6])

        input_el = self.dlg.locate_input()
        if not input_el:
            raise UiNotFound("Не найден input[type=file] на вкладке Upload.")
        token = _ensure_input_token(self.drv, input_el)

        self._log_input_snapshot(input_el, label=f"{self.kind} pre-attach")
        supports_multiple = self._input_supports_multiple(input_el)

        queue = [p for p in files if Path(p).name.lower() not in self.known_names]
        if not queue:
            logger.info("step13: все кандидаты уже присутствуют в диалоге — ничего крепить не нужно")
            return

        if supports_multiple and len(queue) > 1:
            self._attach_batch(input_el, token, queue, prefer_cdp=prefer_cdp, base_count=base_count)
        else:
            for i, p in enumerate(queue, 1):
                self._attach_one(p, input_el, token, is_first=(i == 1), prefer_cdp=prefer_cdp)
                # после каждого — подстрахуемся вкладкой и свежим input
                self.dlg.ensure_upload_tab()
                input_el = self.dlg.locate_input() or self._refresh_latest_input(input_el, token)

    def _attach_batch(self, input_el: WebElement, token: str, batch: List[str], *, prefer_cdp: bool, base_count: int) -> None:
        if not batch:
            return
        for p in batch:
            self.attempted.add(Path(p).name.lower())

        self.dlg.ensure_upload_tab()
        input_el = self.dlg.locate_input() or input_el
        token = _ensure_input_token(self.drv, input_el, token)

        joined = "\n".join(batch)
        via = "none"; ok = False
        try:
            if prefer_cdp:
                ok = _cdp_attach_files_by_token(self.drv, token, batch)
                if ok:
                    _dispatch_input_change_by_token(self.drv, token)
                    via = "cdp"
            if not ok:
                input_el.send_keys(joined)
                via = "send_keys"; ok = True
        except Exception as exc:
            logger.debug("step13: batch attach failed (%s)", exc)

        _synthesize_drop_events(self.drv, input_el)
        accepted, snap = self._ack_wait(base_count, batch[0] if batch else None, is_first=(base_count == 0))
        if not accepted:
            logger.warning("step13: batch via=%s не дал видимого ack — последовательный режим", via)
            for i, p in enumerate(batch, 1):
                self._attach_one(p, input_el, token, is_first=(i == 1 and base_count == 0), prefer_cdp=prefer_cdp)
                self.dlg.ensure_upload_tab()
                input_el = self.dlg.locate_input() or self._refresh_latest_input(input_el, token)
        else:
            logger.info("step13: batch via=%s принят модалкой", via)
            if _detect_editor(snap):
                self._close_editor()
            self.known_names |= _extract_names_from_snap(snap)
            for p in batch:
                self.known_names.add(Path(p).name.lower())

    def _attach_one(self, path: str, input_el: WebElement, token: str, *, is_first: bool, prefer_cdp: bool) -> None:
        base_name = Path(path).name.lower()
        if base_name in self.attempted or base_name in self.known_names:
            logger.info("step13: SKIP '%s' (уже попытан или известен в диалоге)", base_name)
            return
        self.attempted.add(base_name)

        self.dlg.ensure_upload_tab()
        input_el = self.dlg.locate_input() or input_el
        token = _ensure_input_token(self.drv, input_el, token)

        start_count = int((self.dlg.snapshot() or {}).get("previews") or 0)
        logger.info("step13: attach '%s' (start previews=%d)", base_name, start_count)
        self._log_input_snapshot(input_el, label=f"{self.kind} pre-attach(one)")

        try:
            self.drv.execute_script("try{arguments[0].value='';}catch(_){ }", input_el)
        except Exception:
            pass

        attached_via = "none"; send_exc: Optional[Exception] = None

        def _try_send_keys() -> bool:
            nonlocal send_exc
            try:
                input_el.send_keys(path)
                return True
            except Exception as exc:
                send_exc = exc; return False

        ok = False
        if prefer_cdp:
            ok = _cdp_attach_files_by_token(self.drv, token, [path])
            if ok:
                _dispatch_input_change_by_token(self.drv, token)
                attached_via = "cdp"
        if not ok:
            ok = _try_send_keys()
            attached_via = "send_keys" if ok else attached_via
        if not ok:
            input_el = self._refresh_latest_input(input_el, token)
            ok = _cdp_attach_files_by_token(self.drv, token, [path])
            if ok:
                _dispatch_input_change_by_token(self.drv, token)
                attached_via = "refreshed_cdp"
            else:
                ok = _try_send_keys()
                attached_via = "refreshed_send_keys" if ok else attached_via
        if not ok:
            if _file_chooser_attach(self.drv, input_el, [path]):
                attached_via = "file_chooser"; ok = True

        logger.info("step13: attach via=%s ok=%s", attached_via, ok)

        _synthesize_drop_events(self.drv, input_el)
        try:
            files_len_after = int(self.drv.execute_script("return (arguments[0].files && arguments[0].files.length)||0;", input_el) or 0)
        except Exception:
            files_len_after = 0

        accepted, snap = self._ack_wait(start_count, path, is_first=is_first)
        if not accepted and files_len_after > 0:
            logger.info("step13: dialog ack=false, но input.files.length=%s — считаю принятым", files_len_after)
            accepted = True

        if not accepted:
            self.dlg.ensure_upload_tab()
            _dispatch_input_change_by_token(self.drv, token)
            accepted, snap = self._ack_wait(start_count, path, is_first=False, t_first=6.0, t_next=4.0)
            if not accepted:
                if base_name in _extract_names_from_snap(snap):
                    accepted = True
                else:
                    self.dlg.ensure_upload_tab()
                    input_el = self.dlg.locate_input() or self._refresh_latest_input(input_el, token)
                    token = _ensure_input_token(self.drv, input_el, token)
                    alt_ok = _cdp_attach_files_by_token(self.drv, token, [path])
                    if alt_ok:
                        _dispatch_input_change_by_token(self.drv, token)
                    else:
                        try:
                            input_el.send_keys(path); alt_ok = True
                        except Exception:
                            pass
                    if not alt_ok and _file_chooser_attach(self.drv, input_el, [path]):
                        alt_ok = True
                    _synthesize_drop_events(self.drv, input_el)
                    accepted, snap = self._ack_wait(start_count, path, is_first=False, t_first=8.0, t_next=4.0)
                    if not accepted:
                        errs = snap.get("errorMessages") or []
                        if errs:
                            logger.warning("step13: SKIP '%s' — отклонено UI: %s", base_name, "; ".join(errs[:3]))
                            return
                        logger.warning("step13: SKIP '%s' — UI не показал приёмку (via=%s, send_exc=%s)", base_name, attached_via, send_exc)
                        return

        if accepted and _detect_editor(snap):
            self._close_editor()

        self.known_names |= _extract_names_from_snap(snap)
        self.known_names.add(base_name)

    def _close_editor(self) -> None:
        dl = time.time() + 18.0
        was_editor = False
        while time.time() < dl:
            snap = self.dlg.snapshot()
            if _detect_editor(snap):
                was_editor = True
                btn = self.dlg._locate_confirm_button(enabled_only=True)
                if btn:
                    _mouse_click(self.drv, btn)
                    time.sleep(0.4)
                    for _ in range(20):
                        if not _detect_editor(self.dlg.snapshot()):
                            logger.info("step13: редактор закрыт (Done/Save)")
                            return
                        time.sleep(0.2)
            else:
                if was_editor:
                    return
                break
            time.sleep(0.2)


# --------------------------------------------------------------------------------------
#                                Г Е Н Е Р А Т О Р   А С С Е Т О В
# --------------------------------------------------------------------------------------

@dataclass
class RunwareConfig:
    api_key: str
    model_id: str
    base_url: str

def _pick_runware_config() -> RunwareConfig:
    return RunwareConfig(
        api_key=(DEFAULT_RUNWARE_API_KEY or "").strip(),
        model_id=(DEFAULT_RUNWARE_MODEL_ID or "runware:100@1").strip(),
        base_url=(DEFAULT_RUNWARE_URL or "https://api.runware.ai/v1").strip(),
    )

def _ensure_runware_dims(w: int, h: int) -> Tuple[int, int]:
    def _snap(v: int) -> int:
        v = max(128, min(2048, int(v or 512))); rem = v % 64
        if rem: v = max(128, v - rem)
        return v
    return _snap(w), _snap(h)

def _runware_generate_image(prompt: str, cfg: RunwareConfig, *, width: int, height: int, retries: int = RUNWARE_RETRIES, timeout: int = 120) -> bytes:
    width, height = _ensure_runware_dims(width, height)
    headers = {"Content-Type": "application/json"}
    if cfg.api_key:
        headers["Authorization"] = f"Bearer {cfg.api_key}"
    payload = [{
        "taskType": "imageInference", "taskUUID": str(uuid.uuid4()), "positivePrompt": prompt,
        "model": cfg.model_id, "numberResults": 1, "height": int(height), "width": int(width),
        "outputType": "URL", "outputFormat": "JPEG",
    }]
    backoff = 1.0
    attempts: List[str] = []
    for attempt in range(1, max(1, retries) + 1):
        try:
            resp = requests.post(cfg.base_url, json=payload, headers=headers, timeout=timeout)
            if resp.status_code >= 400:
                attempts.append(f"http {resp.status_code}")
                raise RuntimeError(resp.text[:200])
            data = resp.json()
            items = data.get("data")
            if not isinstance(items, list) or not items:
                attempts.append("empty-data")
                raise RuntimeError("Runware response malformed")
            image_url = items[0].get("imageURL") or items[0].get("url") or items[0].get("imageUrl")
            if not image_url:
                attempts.append("no-image-url")
                raise RuntimeError("Runware response missing image URL")
            img = requests.get(image_url, timeout=timeout)
            if not img.ok or not img.content:
                attempts.append(f"fetch-{img.status_code}")
                raise RuntimeError("Failed to download generated image")
            return img.content
        except Exception as exc:
            logger.warning("Runware attempt %d/%d failed: %s", attempt, retries, exc)
            if attempt >= retries:
                raise GenerationError(f"Runware generation failed: {' | '.join(attempts) or str(exc)}") from exc
            time.sleep(min(6.0, backoff + random.uniform(0.0, 0.6)))
            backoff *= 1.8
    raise GenerationError("Runware generation exhausted.")

def _write_image_file(raw: bytes, dest_dir: Path, stem: str, *, jpeg_quality: int = 82) -> str:
    dest_dir.mkdir(parents=True, exist_ok=True)
    safe = "".join(ch if (ch.isalnum() or ch in "-_.") else "-" for ch in (stem or "asset").lower()).strip("-") or f"img-{uuid.uuid4().hex[:6]}"
    out = dest_dir / f"{safe}.jpg"
    if out.exists():
        out = dest_dir / f"{safe}-{uuid.uuid4().hex[:4]}.jpg"
    try:
        if Image is not None:
            im = Image.open(io.BytesIO(raw))
            if im.mode in ("RGBA", "LA") or (im.mode == "P" and "transparency" in im.info):
                bg = Image.new("RGB", im.size, (255, 255, 255)); bg.paste(im, mask=im.split()[-1]); im = bg
            elif im.mode != "RGB":
                im = im.convert("RGB")
            im.save(str(out), format="JPEG", quality=jpeg_quality, optimize=True, progressive=True)
            return str(out)
    except Exception:
        pass
    with open(out, "wb") as fh:
        fh.write(raw)
    return str(out)

def _fallback_image_prompts(*, count: int, business_name: Optional[str], usp: Optional[str], mode: str) -> List[str]:
    base = (usp or business_name or "brand").strip() or "brand"
    pool = [
        f"{base} lifestyle photo, people using product, candid, natural light, rule of thirds, {NEGATIVE_PROMPT}",
        f"{base} product close-up on neutral background, studio light, soft shadows, {NEGATIVE_PROMPT}",
        f"{base} hero banner, bold colors, modern composition, {NEGATIVE_PROMPT}",
        f"{base} real environment, aspirational mood, depth of field, {NEGATIVE_PROMPT}",
        f"{base} benefits in action, authentic smile, cinematic lighting, {NEGATIVE_PROMPT}",
    ]
    random.shuffle(pool)
    return pool[: max(1, count)]

def _fallback_logo_prompts(*, count: int, business_name: Optional[str], usp: Optional[str]) -> List[str]:
    base = (business_name or "Brand").strip()
    pool = [
        f"minimal flat logo for {base}, geometric shapes, 2-color palette, clean white background",
        f"monogram {base[:1].upper()} logo, balanced, modern sans, white background",
        f"{base} wordmark, bold sans-serif, spacing focus, white background",
        f"{base} badge logo, symmetric grid, tidy lines, white background",
    ]
    random.shuffle(pool)
    return pool[: max(1, count)]

def _llm_generate_image_prompts(*, count: int, business_name: Optional[str], usp: Optional[str], site_url: Optional[str], mode: str, seed_notes: Optional[str]) -> List[str]:
    if STEP12_DISABLE_LLM or GeminiClient is None:
        logger.warning("step13: LLM отключена — fallback промпты для изображений")
        return _fallback_image_prompts(count=count, business_name=business_name, usp=usp, mode=mode)
    clue = (seed_notes or "").strip()
    instructions = (
        "You generate high-quality photo prompts for Google Ads Demand Gen. "
        "Return ONLY JSON {\"prompts\":[\"...\"]}. "
        f"Need {max(1, count)} distinct prompts, English, <=260 chars each, no quotes. "
        "Each prompt must mention subject, activity, surroundings, mood, lighting, composition. "
        f"Always append: \"{NEGATIVE_PROMPT}\"."
    )
    ctx = f"Brand: {business_name or '—'}. USP: {usp or '—'}. Website: {site_url or '—'}. Mode: {mode}. "
    if clue:
        ctx += f"Inspired by: {clue}."
    payload = instructions + "\n" + ctx
    try:
        client = GeminiClient(LLM_MODEL, temperature=0.6, retries=1)
        resp = client.generate_json(payload)
        arr = (resp or {}).get("prompts")
        out: List[str] = []
        if isinstance(arr, list):
            for it in arr[: max(1, count)]:
                s = str(it or "").strip().strip('"').strip("'")
                if not s:
                    continue
                if NEGATIVE_PROMPT.lower() not in s.lower():
                    s = f"{s}. {NEGATIVE_PROMPT}"
                out.append(s)
        return out or _fallback_image_prompts(count=count, business_name=business_name, usp=usp, mode=mode)
    except Exception as exc:
        logger.warning("step13: LLM промпты (images) не получены: %s", exc)
        return _fallback_image_prompts(count=count, business_name=business_name, usp=usp, mode=mode)

def _llm_generate_logo_prompts(*, count: int, business_name: Optional[str], usp: Optional[str], seed_notes: Optional[str]) -> List[str]:
    if STEP12_DISABLE_LLM or GeminiClient is None:
        return _fallback_logo_prompts(count=count, business_name=business_name, usp=usp)
    instructions = (
        "Design logo prompts for text-to-image. "
        "Return ONLY JSON {\"prompts\":[\"...\"]}. "
        f"Need {max(1, count)} variants, <=200 chars each, English, no quotes. "
        "Mention style, key shapes, 2-3 color palette, mood. White background."
    )
    ctx = f"Business name: {business_name or 'Generic Brand'}. USP: {usp or '—'}."
    if seed_notes:
        ctx += f" Inspiration: {seed_notes.strip()}."
    payload = instructions + "\n" + ctx
    try:
        client = GeminiClient(LLM_MODEL, temperature=0.45, retries=1)
        resp = client.generate_json(payload)
        arr = (resp or {}).get("prompts")
        out: List[str] = []
        if isinstance(arr, list):
            for it in arr[: max(1, count)]:
                s = str(it or "").strip()
                if s:
                    out.append(s)
        return out or _fallback_logo_prompts(count=count, business_name=business_name, usp=usp)
    except Exception as exc:
        logger.warning("step13: LLM промпты (logos) не получены: %s", exc)
        return _fallback_logo_prompts(count=count, business_name=business_name, usp=usp)

def _ensure_storage_dir(business_name: Optional[str], site_url: Optional[str]) -> Path:
    base = (os.getenv("ADS_AI_IMAGES_BASE") or "").strip() or str(Path.cwd() / "companies" / "images")
    folder = (business_name or "").strip() or (urlparse(site_url).netloc.replace("www.", "") if site_url else f"id-{uuid.uuid4().hex[:8]}")
    safe = "".join(ch if (ch.isalnum() or ch in "-_.") else "-" for ch in folder.lower()).strip("-") or f"id-{uuid.uuid4().hex[:8]}"
    path = Path(base) / safe
    path.mkdir(parents=True, exist_ok=True)
    return path

def _generate_images_via_runware(*, prompts: List[str], dest_dir: Path, label_prefix: str, width: int, height: int) -> List[str]:
    cfg = _pick_runware_config()
    if not cfg.api_key:
        raise GenerationError("Runware API key не задан. Установите RUNWARE_API_KEY или переключитесь на manual.")
    out: List[str] = []
    for i, p in enumerate(prompts, 1):
        try:
            raw = _runware_generate_image(p, cfg, width=width, height=height)
            out.append(_write_image_file(raw, dest_dir, f"{label_prefix}-{i:02d}"))
        except Exception as exc:
            logger.error("step13: Runware картинка #%d не сгенерирована: %s", i, exc)
    return out

def _collect_manual_assets(items: Optional[Iterable[str]], *, dest_dir: Path) -> List[str]:
    if not items:
        return []
    result: List[str] = []
    for ref in items:
        s = str(ref or "").strip()
        if not s:
            continue
        p = Path(s)
        if p.exists():
            result.append(str(p.resolve())); continue
        if s.startswith("data:image/"):
            try:
                head, b64 = s.split(",", 1)
                raw = base64.b64decode(b64)
                ext = ".png" if ("image/png" in head) else ".jpg"
                out = dest_dir / f"ref-{uuid.uuid4().hex[:6]}{ext}"
                out.parent.mkdir(parents=True, exist_ok=True)
                out.write_bytes(raw)
                result.append(str(out.resolve())); continue
            except Exception:
                pass
        if s.startswith(("http://", "https://")):
            try:
                r = requests.get(s, timeout=30)
                if r.ok and r.content:
                    ext = Path(urlparse(s).path).suffix or ".jpg"
                    out = dest_dir / f"ref-{uuid.uuid4().hex[:6]}{ext}"
                    out.write_bytes(r.content)
                    result.append(str(out.resolve())); continue
            except Exception:
                pass
    return _dedupe_files(result)


# --------------------------------------------------------------------------------------
#                                      М Е Й Н
# --------------------------------------------------------------------------------------

def run_step13(
    driver: WebDriver,
    *,
    mode: str = "ai_only",
    seed_assets: Optional[Dict[str, Any]] = None,
    provided_assets: Optional[Dict[str, Any]] = None,
    business_name: Optional[str] = None,
    usp: Optional[str] = None,
    site_url: Optional[str] = None,
    campaign_context: Optional[str] = None,
    desired_logo_count: int = DEFAULT_LOGO_COUNT,
    timeout_total: float = 240.0,
    emit: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    started = time.time()
    tm = TimerMarks()

    _dismiss_soft_dialogs(driver, budget_ms=600)

    normalized_mode = (mode or "ai_only").strip().lower()
    if normalized_mode in {"ai", "auto", "full_ai", "auto_generate"}:
        normalized_mode = "ai_only"
    elif normalized_mode in {"inspired", "guided", "hybrid", "prompt_guided", "mix"}:
        normalized_mode = "inspired"
    elif normalized_mode in {"manual", "upload_only", "provided"}:
        normalized_mode = "manual"
    else:
        normalized_mode = "ai_only"

    _emit(emit, f"Шаг 13: подготовка логотипов ({normalized_mode})")
    logger.info("step13 start | mode=%s | business=%s | site=%s | desired logos=%d",
                normalized_mode, business_name or "-", site_url or "-", desired_logo_count)

    storage_dir = _ensure_storage_dir(business_name, site_url)
    tm.mark("init")

    seed_notes = ""
    if isinstance(seed_assets, dict):
        agg: List[str] = []
        for k in ("headlines", "descriptions", "image_prompts", "notes"):
            v = seed_assets.get(k)
            if isinstance(v, str) and v.strip():
                agg.append(v.strip())
            elif isinstance(v, Iterable):
                agg.extend([str(x).strip() for x in v if str(x).strip()])
        seed_notes = " | ".join(agg)[:600]

    manual_logos  = _collect_manual_assets((provided_assets or {}).get("logos"),  dest_dir=Path(storage_dir))
    tm.mark("collect_manual_assets")

    logo_prompts:  List[str] = []
    logos_to_upload:  List[str] = []
    generation_note = ""

    if normalized_mode == "manual":
        logos_to_upload = manual_logos[: max(1, desired_logo_count)] if manual_logos else []
        if not logos_to_upload:
            _emit(emit, "Логотипы не переданы — можно загрузить позже вручную.")
    else:
        _emit(emit, "Генерирую промпты для логотипов")
        cnt_logo = max(1, desired_logo_count)
        logo_prompts = _llm_generate_logo_prompts(count=cnt_logo, business_name=business_name, usp=usp, seed_notes=seed_notes)
        tm.mark("generate_logo_prompts")

        _emit(emit, f"Генерирую логотипы через Runware ({len(logo_prompts)} шт.)")
        logos_to_upload = _generate_images_via_runware(
            prompts=logo_prompts, dest_dir=Path(storage_dir), label_prefix="demandgen-logo",
            width=RUNWARE_LOGO_SIZE, height=RUNWARE_LOGO_SIZE,
        )
        tm.mark("generate_logos_runware")

        if len(logos_to_upload) < 1 and manual_logos:
            _emit(emit, "Использую предоставленные логотипы (генерация не удалась)")
            logos_to_upload = manual_logos[: max(1, desired_logo_count)]

    logos_to_upload = _dedupe_files(logos_to_upload)

    if not logos_to_upload:
        logger.warning("step13: логотипы не подготовлены, но продолжаем (можно загрузить позже)")
        # Не выбрасываем ошибку для логотипов, так как они опциональны

    # ---------- Загрузка в UI ----------
    with _ConfirmWatcher(driver, emit=emit):
        # Logos
        if logos_to_upload:
            _emit(emit, "Загружаю логотипы")
            dlg_logos = UploadDialog(driver, "logos", storage_dir=Path(storage_dir))
            dlg_logos.open()
            if not dlg_logos.ensure_upload_tab():
                logger.warning("step13: вкладка Upload (logos) не активировалась — tolerant режим")
            time.sleep(0.2)

            _maybe_shot(driver, Path(storage_dir) / "debug" / "before_attach_logos.png", "before-attach-logos")

            uploader_logos = Uploader(driver, dlg_logos, kind="logos")
            uploader_logos.attach(logos_to_upload, prefer_cdp=True)

            if not dlg_logos.wait_save_and_click(min_wait=5.0, timeout=180.0):
                snap = dlg_logos.snapshot()
                logger.info("step13: logos Save не активировалась, финальный snap=%s", snap)
                _maybe_shot(driver, Path(storage_dir) / "debug" / "logos_save_not_active.png", "logos-save-not-active")
                raise UploadTimeout("Кнопка сохранения логотипов не активировалась.")
            dlg_logos.wait_closed(timeout=20.0)
            tm.mark("upload_logos")
        else:
            logger.info("step13: логотипы пропущены (нет файлов).")

    duration_ms = int((time.time() - started) * 1000)
    logger.info("step13 done (%d ms) | uploaded logos=%d | mode=%s",
                duration_ms, len(logos_to_upload), normalized_mode)

    return {
        "mode": normalized_mode,
        "duration_ms": duration_ms,
        "storage_dir": str(storage_dir),
        "logos": {
            "mode": normalized_mode,
            "source": "llm+runware" if normalized_mode != "manual" else "manual",
            "files": logos_to_upload,
            "prompts": logo_prompts,
            "note": generation_note,
        },
        "timing_breakdown": [{"stage": name, "duration_ms": dur} for name, dur in tm.records],
        "campaign_context": campaign_context,
    }


def run(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Совместимость с автозапуском через модульный загрузчик."""
    return run_step13(*args, **kwargs)
