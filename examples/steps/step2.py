# -*- coding: utf-8 -*-
"""
Шаг 2 Google Ads Wizard:
1) Выбрать "Create a campaign without guidance" (язык-агностично)
2) Выбрать тип кампании (по умолчанию Performance Max / UBERVERSAL)
3) Нажать Continue; если после Continue всплыл "Campaign name" — сгенерировать имя и нажать Continue ещё раз
4) Если ПОСЛЕ второго Continue всплыл picker "Create a new campaign or finish a saved draft?" —
   кликнуть ТОЛЬКО кнопку "Start new" (футер .after-footer .button-group-right .new-button — футер может быть СНАРУЖИ <picker>)
5) Закрывать онбординг-карусель "Guided steps" (modal-carousel) КРЕСТИКОМ, если появляется
6) Надёжно дождаться перехода на следующий экран (construction-layout)

Антимисклик и анти-псевдонавигация — см. комментарии в коде.

Дополнительно:
- emit: Optional[Callable[[str], None]] — колбэк для UI‑комментариев «по делу»
  (например: «беру режим без подсказок», «генерирую имя кампании», «жму продолжить»).
"""

from __future__ import annotations

import logging
import os
import re
import time
from datetime import datetime
import json
from typing import List, Optional, Dict, Any, Tuple, Callable
from urllib.parse import urlparse, parse_qsl

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webdriver import WebDriver, WebElement
from selenium.webdriver.common.action_chains import ActionChains

logger = logging.getLogger("ads_ai.gads.step2")
if not logger.handlers:  # fallback
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

# ---------- LLM (Gemini) ----------
try:
    from ads_ai.llm.gemini import GeminiClient
except Exception as e:  # pragma: no cover
    GeminiClient = None  # type: ignore
    logger.warning("GeminiClient not available: %s", e)


def _emit(emit: Optional[Callable[[str], None]], text: str) -> None:
    """Безопасная посылка комментария в UI."""
    if callable(emit) and isinstance(text, str) and text.strip():
        try:
            emit(text.strip())
        except Exception:
            pass


def _generate_campaign_name_via_llm(
    *,
    business_name: Optional[str],
    usp: Optional[str],
    site_url: Optional[str],
    budget: Optional[str],
    campaign_type_label: str = "Performance Max",
) -> str:
    """Просим LLM вернуть ТОЛЬКО JSON {"campaign_name": "..."} (ASCII, <=45)."""
    if GeminiClient is None:
        raise RuntimeError("LLM unavailable")

    brand = (business_name or "").strip()
    prompt = {
        "task": "Return ONLY compact JSON with field 'campaign_name' (string, <= 45 chars, English).",
        "constraints": [
            "ASCII-friendly (no emojis, no quotes).",
            "Include a short type hint like 'PMax'/'Search' at start.",
            "Readable marketing-style name, not generic 'Campaign'.",
        ],
        "inputs": {
            "business_name": brand,
            "usp": (usp or "").strip(),
            "site_url": (site_url or "").strip(),
            "budget_per_day": (budget or "").strip(),
            "campaign_type": campaign_type_label,
        },
        "output_schema": {"campaign_name": "string"},
        "format": "json_only_no_explanations",
        "examples": [
            {"inputs": {"business_name": "EasyByte AI", "campaign_type": "Performance Max"},
             "json": {"campaign_name": "PMax | EasyByte AI Core"}}
        ]
    }
    model = os.getenv("GEMINI_MODEL", "models/gemini-2.0-flash")
    client = GeminiClient(model=model, temperature=0.2, retries=1, fallback_model=None)
    prompt_payload = json.dumps(prompt, ensure_ascii=False)
    raw = client.generate_json(prompt_payload)
    data: Dict[str, Any] = raw if isinstance(raw, dict) else _loose_json(str(raw))
    name = str((data or {}).get("campaign_name", "")).strip()
    name = _asciiize(name)[:45] if name else ""
    if not name:
        raise RuntimeError("LLM returned empty campaign_name")
    return name


def _loose_json(text: str) -> Dict[str, Any]:
    try:
        import json
        return json.loads(text)
    except Exception:
        pass
    m = re.search(r"\{.*\}", text or "", flags=re.S)
    if m:
        try:
            import json
            return json.loads(m.group(0))
        except Exception:
            return {}
    return {}


def _asciiize(s: str) -> str:
    if not s:
        return ""
    ss = "".join(ch if ord(ch) < 128 else " " for ch in s)
    ss = re.sub(r"\s+", " ", ss).strip()
    ss = ss.replace('"', "").replace("'", "")
    return ss


def _unique_suffix() -> str:
    return datetime.utcnow().strftime("%y%m%d%H%M%S")


def _short_type_label_by_code(code: str) -> str:
    code = (code or "").strip().upper()
    return {
        "UBERVERSAL": "PMax",
        "SEARCH": "Search",
        "DISPLAY": "Display",
        "VIDEO": "Video",
        "SHOPPING": "Shopping",
        "OWNED_AND_OPERATED": "DemandGen",
        "MULTIPLE": "App",
    }.get(code, "Campaign")


def _fallback_campaign_name(*, business_name: Optional[str], campaign_type_code: str) -> str:
    base = _asciiize(business_name or "").strip() or "Brand"
    tlabel = _short_type_label_by_code(campaign_type_code)
    return f"{tlabel} | {base} {_unique_suffix()}"[:45]


def _extract_numeric_suffix(value: str, *, min_digits: int = 4) -> str:
    if not value:
        return ""
    pattern = rf"(\d{{{min_digits},}})\s*$"
    match = re.search(pattern, value)
    return match.group(1) if match else ""


def _derive_unique_business_name(
    *,
    original_name: Optional[str],
    campaign_name: Optional[str],
    suffix: Optional[str] = None,
    type_label: Optional[str] = None,
    max_len: int = 25,
) -> str:
    suffix_clean = re.sub(r"\D+", "", suffix or "")
    if len(suffix_clean) < 4:
        suffix_clean = _unique_suffix()
    suffix_clean = suffix_clean.strip()

    type_label_ascii = _asciiize(type_label or "").strip()
    prefix = f"{type_label_ascii} | " if type_label_ascii else ""

    max_len = max(len(prefix) + len(suffix_clean) + 2, max_len, 8)
    if len(prefix) >= max_len:
        return (prefix + suffix_clean)[-max_len:]
    if len(suffix_clean) >= max_len - len(prefix):
        tail = suffix_clean[-(max_len - len(prefix)):]
        return (prefix + tail).strip()

    allowance = max_len - len(prefix) - len(suffix_clean) - 1
    if allowance <= 0:
        core = suffix_clean[-(max_len - len(prefix)):]
        return (prefix + core).strip()

    base_candidates: List[str] = []
    if campaign_name:
        base_candidates.append(campaign_name)
        if "|" in campaign_name:
            base_candidates.insert(0, campaign_name.split("|", 1)[1])
        base_candidates.extend(re.split(r"[|—–-]+", campaign_name))
    if original_name:
        base_candidates.insert(0, original_name)

    base_clean = ""
    for cand in base_candidates:
        cand_ascii = _asciiize(cand or "")
        cand_ascii = re.sub(r"[^\w\s-]", " ", cand_ascii)
        cand_ascii = re.sub(r"\s+", " ", cand_ascii).strip(" -_|")
        if cand_ascii:
            base_clean = cand_ascii
            break
    if not base_clean:
        base_clean = "Brand"

    base_trimmed = base_clean[:allowance].rstrip(" -_|")
    if not base_trimmed:
        base_trimmed = base_clean[:allowance].strip(" -_|")
    if not base_trimmed:
        base_trimmed = "Brand"[:allowance].strip(" -_|") or "Brand"

    candidate_core = f"{base_trimmed} {suffix_clean}".strip()
    candidate = f"{prefix}{candidate_core}".strip()
    if len(candidate) > max_len:
        candidate = candidate[:max_len].rstrip(" -_|")
        if not candidate:
            candidate = suffix_clean[-(max_len - len(prefix)):]
            if prefix:
                candidate = (prefix + candidate)[-max_len:]
    return candidate


# ---------- Селекторы/синонимы ----------

NO_GUIDANCE_DATA_VALUE = "No objective"
NO_GUIDANCE_SYNONYMS = [
    "create a campaign without guidance", "no objective",
    "создать кампанию без подсказок", "без подсказок", "без рекомендаций", "без руководства", "без цели",
    "sin orientación", "sin objetivo",
    "sem orientação", "sem objetivo",
    "sans assistance", "sans conseil", "sans objectif",
    "senza guida", "senza obiettivo",
    "ohne anleitung", "ohne ziel",
    "bez wskazówek", "bez celu",
    "yönlendirme olmadan", "amacı yok",
    "útmutató nélkül", "cél nélkül",
    "bez vedení", "bez cíle",
    "bez pokynov", "bez cieľa",
    "不提供指南", "无指导", "沒有指引", "无目标", "沒有目標",
    "ガイダンスなし", "目的なし",
    "가이드 없이", "목표 없음",
    "ไม่มีคำแนะนำ",
    "không có hướng dẫn", "không mục tiêu",
]

PERFMAX_DATA_VALUE = "UBERVERSAL"
PERFMAX_SYNONYMS = [
    "performance max",
    "максимальная эффективность",
    "máximo rendimiento", "rendimiento máximo",
    "máximo desempenho", "maximização de desempenho",
    "performances maximales", "maximisation des performances",
    "massimizzazione del rendimento",
    "leistungsmaximierung",
    "maksymalna skuteczność",
    "maksimum performans",
    "teljesítmény maximalizálás",
    "maximalizace výkonu",
    "maximalizácia výkonu",
    "最大成效", "效能最大化",
    "パフォーマンス マックス",
    "퍼포먼스 맥스",
    "เพิ่มประสิทธิภาพสูงสุด",
    "tối đa hiệu suất",
]

_NEXT_TEXTS = [
    "continue", "next", "save and continue",
    "продолжить", "далее", "сохранить и продолжить", "далі",
    "continuar", "siguiente", "guardar y continuar",
    "avançar", "próximo", "прóxima",
    "continuer", "suivant",
    "avanti", "salva e continua",
    "weiter", "weitergehen",
    "kontynuuj", "dalej",
    "devam", "ileri",
    "tovább",
    "pokračovat", "pokračovať",
    "继续", "下一步", "繼續", "下一頁", "下一页",
    "続行", "次へ",
    "계속", "다음",
    "ดำเนินการต่อ", "ถัดไป",
    "tiếp tục", "tiếp theo",
]
_BACK_TEXTS = ["back", "назад", "atrás", "zurück", "retour", "voltar", "上一页", "上一步", "戻る", "뒤로", "zpět", "späť", "wstecz"]

CAMPAIGN_NAME_LABEL_SYNONYMS = [
    "campaign name", "name your campaign", "campaign title",
    "название кампании", "имя кампании", "назва кампанії",
    "nombre de la campaña", "nome da campanha", "nom de la campagne",
    "nome della campagna", "kampagnenname", "nazwa kampanii", "kampanya adı",
    "название рекламной кампании",
]

SEARCH_FIELD_SYNONYMS = [
    "search", "поиск", "buscar", "suche", "recherche", "ricerca",
    "найти", "ищите", "buscar campaña", "search for a page or campaign",
]

CAMPAIGN_NAME_INPUT_SELECTORS: List[str] = [
    'material-input.campaign-name-input input.input',
    'material-input.campaign-name-input input.input-area',
    'div.campaign-name-view material-input.campaign-name-input input.input',
    'div.campaign-name-view material-input.campaign-name-input input.input-area',
    'input.input[aria-label*="Campaign name"]',
    'input.input-area[aria-label*="Campaign name"]',
]

CONTINUE_HARD_SELECTORS: List[str] = [
    'button.btn-yes',
    'button[aria-label="Continue to the next step"]',
    '.container.right-align button.btn-yes',
    'button.button-next',
    '.buttons .button-next',
]

_START_NEW_TEXTS = [
    "start new", "create new", "start a new", "new campaign",
    "начать новую", "начать новую кампанию", "создать новую", "новая",
    "iniciar nueva", "iniciar nuevo", "crear nueva", "crear nuevo",
    "iniciar nova", "iniciar novo", "criar nova", "criar novo",
    "créer une nouvelle", "nouvelle campagne",
    "avvia nuova", "avvia nuovo", "nuova campagna",
    "neu starten", "neue kampagne",
    "rozpocznij nową", "utwórz nową",
    "yeni başlat", "yeni kampanya",
    "нова кампанія", "почати нову",
]
_GO_BACK_TEXTS = ["go back", "back", "назад", "volver", "retour", "zurück", "indietro", "voltar"]

# ---------- Базовые утилиты Selenium/JS ----------

def _is_interactable(driver: WebDriver, el: WebElement) -> bool:
    try:
        if not el.is_displayed():
            return False
        if not el.is_enabled() or (el.get_attribute("aria-disabled") or "").lower() == "true":
            return False
        driver.execute_script("arguments[0].scrollIntoView({block:'center',inline:'nearest'});", el)
        r = el.rect
        return r.get("width", 0) >= 8 and r.get("height", 0) >= 8
    except Exception:
        return False


def _dismiss_soft_dialogs(driver: WebDriver, budget_ms: int = 900) -> None:
    """Софтовые тултипы/куки и т.п."""
    t0 = time.time()
    CAND = ["accept all", "i agree", "agree", "got it", "ok",
            "принять все", "я согласен", "понятно", "хорошо",
            "同意", "接受", "确定", "知道了", "好"]
    while (time.time() - t0) * 1000 < budget_ms:
        try:
            dialogs = driver.find_elements(By.CSS_SELECTOR, '[role="dialog"], div[aria-modal="true"], .mdc-dialog--open')
            hit = False
            for dlg in dialogs:
                if not _is_interactable(driver, dlg):
                    continue
                for b in dlg.find_elements(By.CSS_SELECTOR, 'button,[role=button],a[role=button]'):
                    txt = ((b.text or "") + " " + (b.get_attribute("aria-label") or "")).strip().lower()
                    if txt and any(w in txt for w in CAND):
                        try:
                            b.click()
                        except Exception:
                            try:
                                driver.execute_script("arguments[0].click();", b)
                            except Exception:
                                continue
                        time.sleep(0.18)
                        hit = True
                        break
                if hit:
                    break
            if not hit:
                break
        except Exception:
            break


# ---------- Точечный игнор перекрывающих ховер‑попапов + УХОД КУРСОРА ----------

_OVERLAY_MARKERS = [
    "popup", "tooltip", "suggest", "selections", "gm-popup", "balloon",
    "clickabletooltip", "hovercard"
]
_OVERLAY_EXPLICIT_SELECTORS = [
    ".material-popup", "material-popup",
    ".location-suggest-popup",
    ".selections.visible", ".selections[pane-id]",
    ".gm-popup", ".gm-popup .popup-header",
    ".popup-header",
    # распространённые тултипы/оверлеи
    ".mdc-tooltip", ".material-tooltip", "[role='tooltip']",
    ".cdk-overlay-container .cdk-overlay-pane", ".cdk-overlay-backdrop",
    ".tooltip-surface", ".mat-tooltip", ".mat-tooltip-panel",
]

def _suppress_overlays_over_element(driver: WebDriver, el: WebElement, token: str) -> int:
    """Временно отключает pointer-events у перекрывающих цель всплывашек (только overlays, не модалки)."""
    try:
        return int(driver.execute_script(
            """
            const el = arguments[0];
            const token = arguments[1];
            const markers = arguments[2];
            const explicit = arguments[3];
            if(!el) return 0;

            const within = (x,y,rect) => (x>=rect.left && x<=rect.right && y>=rect.top && y<=rect.bottom);
            const rect = el.getBoundingClientRect();
            const pts = [
              [Math.floor(rect.left + rect.width/2), Math.floor(rect.top + rect.height/2)],
              [Math.floor(rect.left + 8), Math.floor(rect.top + 8)],
              [Math.floor(rect.right - 8), Math.floor(rect.top + 8)],
              [Math.floor(rect.left + 8), Math.floor(rect.bottom - 8)],
              [Math.floor(rect.right - 8), Math.floor(rect.bottom - 8)]
            ];

            const isCandidate = (n)=>{
              if(!n || n===document.documentElement || n===document.body) return false;
              const role=(n.getAttribute && (n.getAttribute('role')||'').toLowerCase())||'';
              if(role==='dialog') return false;
              if(n.closest && (n.closest('picker,.picker,[role=dialog]'))) return false;

              const cls=(n.className||'').toString().toLowerCase();
              const id=(n.id||'').toString().toLowerCase();
              const name = (n.localName||'').toLowerCase();
              const text = cls + ' ' + id + ' ' + name;

              for(const m of markers){ if(m && text.includes(m)) return true; }
              try{ for(const sel of explicit){ if(n.matches && n.matches(sel)) return true; } }catch(e){}
              try{
                const cs=getComputedStyle(n);
                if((cs.position==='fixed' || cs.position==='absolute') && cs.pointerEvents!=='none'){
                  const r=n.getBoundingClientRect();
                  if(r.width>20 && r.height>20) return true;
                }
              }catch(e){}
              return false;
            };

            const seen = new Set();
            const touched = [];
            for(const [x,y] of pts){
              if(!within(x,y,rect)) continue;
              let top = document.elementFromPoint(x,y);
              let hops=0;
              while(top && hops<7){
                if(isCandidate(top)) { seen.add(top); break; }
                top = top.parentElement; hops++;
              }
            }

            const mark = (n)=>{
              try{
                if(n.getAttribute('data-adsai-pe')===token) return;
                n.setAttribute('data-adsai-pe', token);
                n.style.setProperty('pointer-events','none','important');
                touched.push(n);
              }catch(e){}
            };
            seen.forEach(mark);
            return touched.length;
            """,
            el, token, _OVERLAY_MARKERS, _OVERLAY_EXPLICIT_SELECTORS
        ))
    except Exception:
        return 0


def _restore_suppressed_overlays(driver: WebDriver, token: str) -> None:
    try:
        driver.execute_script(
            """
            const token = arguments[0];
            document.querySelectorAll('[data-adsai-pe]').forEach(n=>{
              try{
                if(n.getAttribute('data-adsai-pe')===token){
                  n.style.removeProperty('pointer-events');
                  n.removeAttribute('data-adsai-pe');
                }
              }catch(e){}
            });
            """,
            token
        )
    except Exception:
        pass


def _park_mouse(driver: WebDriver) -> bool:
    """
    Паркует курсор в безопасной точке окна (угол/край без оверлеев), чтобы снять :hover у любых элементов.
    Реально двигает указатель с помощью ActionChains.
    """
    try:
        pt = driver.execute_script(
            """
            const markers = arguments[0], explicit = arguments[1];
            const isOverlay = (n)=>{
              if(!n) return false;
              const role=(n.getAttribute && (n.getAttribute('role')||'').toLowerCase())||'';
              if(role==='dialog') return true;
              if(n.closest && n.closest('picker,.picker,[role=dialog]')) return true;
              const cls=(n.className||'').toString().toLowerCase();
              const id=(n.id||'').toString().toLowerCase();
              const name=(n.localName||'').toLowerCase();
              const text=cls+' '+id+' '+name;
              for(const m of markers){ if(m && text.includes(m)) return true; }
              try{ for(const sel of explicit){ if(n.matches && n.matches(sel)) return true; } }catch(e){}
              return false;
            };
            const cands = [
              [10,10],
              [innerWidth-12,10],
              [10,innerHeight-12],
              [innerWidth-12,innerHeight-12],
              [Math.floor(innerWidth/2), 8],
              [8, Math.floor(innerHeight/2)]
            ];
            let pick=cands[0];
            for(const [x,y] of cands){
              const top=document.elementFromPoint(x,y);
              let ok=true, hop=0, n=top;
              while(n && hop<6){
                if(isOverlay(n)){ ok=false; break; }
                n=n.parentElement; hop++;
              }
              if(ok){ pick=[x,y]; break; }
            }
            let anchor=document.getElementById('adsai-mouse-park');
            if(!anchor){
              anchor=document.createElement('div');
              anchor.id='adsai-mouse-park';
              anchor.style.cssText='position:fixed;width:6px;height:6px;opacity:0;pointer-events:none;z-index:2147483647;left:0;top:0;';
              document.body.appendChild(anchor);
            }
            anchor.style.left=(pick[0]-3)+'px';
            anchor.style.top =(pick[1]-3)+'px';
            return pick;
            """,
            _OVERLAY_MARKERS, _OVERLAY_EXPLICIT_SELECTORS
        )
        anchor = driver.find_element(By.ID, "adsai-mouse-park")
        ActionChains(driver).move_to_element(anchor).pause(0.05).perform()
        driver.execute_script("const n=document.getElementById('adsai-mouse-park'); if(n) n.remove();")
        logger.debug("Mouse parked at %s", pt)
        return True
    except Exception as e:
        try:
            html = driver.find_element(By.TAG_NAME, "html")
            ActionChains(driver).move_to_element_with_offset(html, 5, 5).pause(0.03).perform()
            return True
        except Exception:
            logger.debug("park_mouse fallback failed: %s", e)
            return False


def _mouse_jiggle(driver: WebDriver, *, amplitude: int = 14, repeats: int = 2) -> bool:
    """Реальное подёргивание мыши для снятия :hover/tooltips."""
    try:
        _park_mouse(driver)
        driver.execute_script(
            """
            let a = document.getElementById('adsai-mouse-jiggle');
            if(!a){
              a = document.createElement('div');
              a.id = 'adsai-mouse-jiggle';
              a.style.cssText = 'position:fixed;left:12px;top:12px;width:6px;height:6px;opacity:0;pointer-events:none;z-index:2147483647;';
              document.body.appendChild(a);
            }
            """
        )
        anchor = driver.find_element(By.ID, "adsai-mouse-jiggle")
        chain = ActionChains(driver).move_to_element(anchor).pause(0.04)
        for _ in range(max(1, repeats)):
            chain = (chain
                     .move_by_offset(amplitude, 0).pause(0.04)
                     .move_by_offset(0, amplitude).pause(0.04)
                     .move_by_offset(-amplitude, 0).pause(0.04)
                     .move_by_offset(0, -amplitude).pause(0.04))
        chain.perform()
        driver.execute_script("const a=document.getElementById('adsai-mouse-jiggle'); if(a) a.remove();")
        return True
    except Exception as e:
        logger.debug("mouse_jiggle failed: %s", e)
        return _park_mouse(driver)


def _synthetic_click(driver: WebDriver, el: WebElement) -> None:
    try:
        driver.execute_script(
            """
            const el=arguments[0];
            const r=el.getBoundingClientRect();
            const x = Math.floor(r.left + Math.max(2, r.width/2));
            const y = Math.floor(r.top  + Math.max(2, r.height/2));
            const mk = (t)=>new MouseEvent(t,{view:window,bubbles:true,cancelable:true,clientX:x,clientY:y});
            el.dispatchEvent(mk('mousedown')); el.dispatchEvent(mk('mouseup')); el.dispatchEvent(mk('click'));
            """,
            el
        )
    except Exception:
        pass


def _robust_click(driver: WebDriver, el: WebElement, *, label: str = "click") -> bool:
    """Клик: уводим курсор, временно «пробиваем» всплывашки, затем кликаем тремя способами."""
    if not _is_interactable(driver, el):
        return False
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center',inline:'nearest'});", el)
    except Exception:
        pass
    time.sleep(0.05)
    _mouse_jiggle(driver, amplitude=12, repeats=1)

    token = f"pe_{int(time.time()*1000)%100000}"
    suppressed = _suppress_overlays_over_element(driver, el, token)
    try:
        try:
            el.click()
            return True
        except Exception:
            pass
        try:
            driver.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            pass
        _synthetic_click(driver, el)
        return True
    except Exception as e:
        logger.debug("robust_click(%s) failed: %s", label, e)
        return False
    finally:
        if suppressed:
            _restore_suppressed_overlays(driver, token)


def _dismiss_hover_popups(driver: WebDriver) -> None:
    """Мягкая зачистка hover‑popup'ов + обязательный уход курсора."""
    _park_mouse(driver)
    try:
        driver.execute_script(
            "const kd=new KeyboardEvent('keydown',{key:'Escape',code:'Escape',keyCode:27,bubbles:true});"
            "const ku=new KeyboardEvent('keyup',{key:'Escape',code:'Escape',keyCode:27,bubbles:true});"
            "document.dispatchEvent(kd);document.dispatchEvent(ku);"
        )
    except Exception:
        pass
    try:
        driver.execute_script("if(document.activeElement) document.activeElement.blur();")
    except Exception:
        pass
    try:
        driver.execute_script(
            "const ev=(t)=>new MouseEvent(t,{view:window,bubbles:true,cancelable:true,clientX:5,clientY:5});"
            "document.body.dispatchEvent(ev('mousedown'));document.body.dispatchEvent(ev('mouseup'));document.body.dispatchEvent(ev('click'));"
        )
    except Exception:
        pass
    _mouse_jiggle(driver, amplitude=10, repeats=1)
    time.sleep(0.05)


# ---------- Guided steps (modal-carousel) ----------

def _is_guided_steps_visible(driver: WebDriver) -> bool:
    try:
        return bool(driver.execute_script(
            """
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e), r=e.getBoundingClientRect();
              if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2||cs.pointerEvents==='none') return false;
              return r.width>8 && r.height>8 && r.bottom>0 && r.right>0 && r.top<innerHeight && r.left<innerWidth;};
            const modals=[...document.querySelectorAll('.modal.modal-carousel[role=dialog], .modal-carousel[role=dialog]')].filter(isVis);
            if(modals.length===0) return false;
            for(const m of modals){
              const hasIntro = !!m.querySelector('introduction-flow-card,.carousel-page-container');
              if(hasIntro) return true;
            }
            return false;
            """
        ))
    except Exception:
        return False


def _find_guided_steps_close_button(driver: WebDriver) -> Optional[WebElement]:
    try:
        el = driver.execute_script(
            """
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e), r=e.getBoundingClientRect();
              if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2||cs.pointerEvents==='none') return false;
              return r.width>8 && r.height>8 && r.bottom>0 && r.right>0 && r.top<innerHeight && r.left<innerWidth;};
            const mods=[...document.querySelectorAll('.modal.modal-carousel[role=dialog], .modal-carousel[role=dialog]')].filter(isVis);
            for(const m of mods){
              let btn = m.querySelector('material-button.carousel-close-button, .carousel-close-button');
              if(btn && isVis(btn)) return btn;
              btn = m.querySelector('introduction-flow-card material-button.close-button, introduction-flow-card .close-button');
              if(btn && isVis(btn)) return btn;
              const pool=[...m.querySelectorAll('button,material-button,[role=button],a[role=button]')].filter(isVis);
              for(const b of pool){
                const t=((b.innerText||b.textContent||'')+' '+(b.getAttribute('aria-label')||'')).trim().toLowerCase();
                if(t.includes('close')) return b;
              }
            }
            return null;
            """
        )
        if el:
            return el
    except Exception:
        pass
    return None


def _close_guided_steps_if_any(driver: WebDriver, appear_timeout: float = 0.6, disappear_timeout: float = 8.0) -> bool:
    t_end = time.time() + max(0.25, appear_timeout)
    appeared = False
    while time.time() < t_end:
        if _is_guided_steps_visible(driver):
            appeared = True
            break
        time.sleep(0.1)
    if not appeared:
        return False

    btn = _find_guided_steps_close_button(driver)
    if not btn:
        logger.warning("Guided steps modal обнаружена, но крестик не найден — пропускаю.")
        return True

    _robust_click(driver, btn, label="guided_close")

    t_dis = time.time() + max(0.5, disappear_timeout)
    while time.time() < t_dis:
        if not _is_guided_steps_visible(driver):
            logger.info("Guided steps: модалка закрыта крестиком.")
            return True
        time.sleep(0.12)

    logger.warning("Guided steps: модалка не исчезла по таймауту.")
    return True


# ---------- Continue / Next ----------

def _find_continue_button_any_language(driver: WebDriver) -> Optional[WebElement]:
    try:
        el = driver.execute_script(
            """
            const NEXT = new Set(arguments[0].map(s=>String(s||'').toLowerCase()));
            const BACK = new Set(arguments[1].map(s=>String(s||'').toLowerCase()));
            const isVis=(e)=>{if(!e) return false; const cs=getComputedStyle(e), r=e.getBoundingClientRect();
              if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2||cs.pointerEvents==='none') return false;
              return r.width>8 && r.height>8 && r.top<innerHeight && r.left<innerWidth && r.bottom>0 && r.right>0;};
            const notDisabled=(e)=>!( (e.getAttribute('aria-disabled')||'').toLowerCase()==='true' || e.hasAttribute('disabled') );
            const root=document.querySelector('main,[role=main]')||document.body;
            const nodes=[...root.querySelectorAll('button,[role=button],a[role=button]')].filter(isVis).filter(notDisabled);
            let best=null, score=-1;
            for(const n of nodes){
              const t=((n.innerText||n.textContent||'')+' '+(n.getAttribute('aria-label')||'')).trim().toLowerCase();
              let s=0;
              for(const w of NEXT) if(w && t.includes(w)) s+=10;
              for(const b of BACK) if(b && t.includes(b)) s-=10;
              const cls=(n.className||'').toLowerCase();
              if(/(primary|mdc-button--raised|mdc-button--unelevated|mat-primary|button-next|highlighted)/.test(cls)) s+=3;
              const r=n.getBoundingClientRect(); s+=Math.min(3, Math.max(0, Math.round((r.left/Math.max(1,innerWidth))*3)));
              if(s>score){best=n; score=s;}
            }
            return best||null;
            """,
            _NEXT_TEXTS, _BACK_TEXTS,
        )
        if el:
            return el
    except Exception:
        pass
    for sel in CONTINUE_HARD_SELECTORS:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            if _is_interactable(driver, el):
                return el
        except Exception:
            continue
    return None


def _click_continue_button(driver: WebDriver, *, skip_preflight: bool = False) -> bool:
    try:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    except Exception:
        pass

    if skip_preflight:
        btn = _find_continue_button_any_language(driver)
        if btn and _is_interactable(driver, btn):
            try:
                btn.click()
                return True
            except Exception:
                try:
                    driver.execute_script("arguments[0].click();", btn)
                    return True
                except Exception:
                    pass
        # fall through to the full preflight flow if the fast path failed

    _close_guided_steps_if_any(driver, appear_timeout=0.2, disappear_timeout=4.0)
    _dismiss_hover_popups(driver)

    btn = _find_continue_button_any_language(driver)
    if not btn:
        logger.warning("Кнопка Continue/Next не найдена.")
        return False
    return _robust_click(driver, btn, label="continue")


# ---------- Выбор карточек/вкладок ----------

def _is_tab_selected_js(driver: WebDriver, el: WebElement) -> bool:
    try:
        return bool(driver.execute_script(
            """
            const n=arguments[0];
            if(!n) return false;
            const isSel=(e)=>{
              if(!e) return false;
              const ac=(e.getAttribute('aria-checked')||'').toLowerCase()==='true';
              const as=(e.getAttribute('aria-selected')||'').toLowerCase()==='true';
              const ap=(e.getAttribute('aria-pressed')||'').toLowerCase()==='true';
              const cls=(e.className||'').toLowerCase();
              const classOn=/(\bselected\b|\bis-selected\b|\bactive\b|\bchecked\b)/.test(cls);
              const inputChecked=e.matches('input[type=radio],input[type=checkbox]')?!!e.checked:
                                 !!e.querySelector('input[type=radio]:checked,input[type=checkbox]:checked');
              const descSel=!!e.querySelector('.item.selected,.is-selected,[aria-selected="true"],[aria-checked="true"]');
              return ac||as||ap||classOn||inputChecked||descSel;
            };
            let root=n.closest('[role="tab"],.selection-item,selection-card,.item,.card,.card-wrapper')||n;
            return isSel(root)||isSel(n);
            """,
            el
        ))
    except Exception:
        return False


def _click_tab_root(driver: WebDriver, el: WebElement) -> None:
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.05)
        _robust_click(driver, el, label="tab")
    except Exception:
        try:
            driver.execute_script(
                """
                const el=arguments[0];
                const root = el.closest('[role="tab"],.selection-item,selection-card,.item,.card,.card-wrapper') || el;
                try{ root.click(); }catch(e){}
                """,
                el
            )
        except Exception:
            pass
    time.sleep(0.15)


def _select_tab_by_datavalue_or_text(
    driver: WebDriver,
    data_value: Optional[str],
    text_synonyms: List[str],
    scope_css: Optional[str] = None,
    timeout: float = 12.0,
) -> bool:
    end = time.time() + timeout
    last_err = None
    while time.time() < end:
        try:
            if data_value:
                nodes = driver.find_elements(By.CSS_SELECTOR, f'{scope_css+" " if scope_css else ""}[role="tab"][data-value]')
                cand: Optional[WebElement] = None
                for n in nodes:
                    try:
                        dv = (n.get_attribute("data-value") or "").strip().lower()
                        if dv == data_value.strip().lower():
                            cand = n
                            break
                    except Exception:
                        continue
                if cand and _is_interactable(driver, cand):
                    if not _is_tab_selected_js(driver, cand):
                        _click_tab_root(driver, cand)
                    if _is_tab_selected_js(driver, cand):
                        return True

            nodes = driver.find_elements(By.CSS_SELECTOR, f'{scope_css+" " if scope_css else ""}[role="tab"], {scope_css+" " if scope_css else ""}dynamic-component.selection-item[role="tab"]')
            for n in nodes:
                if not _is_interactable(driver, n):
                    continue
                try:
                    blob = ((n.text or "") + " " + (n.get_attribute("aria-label") or "")).strip().lower()
                except Exception:
                    blob = ""
                if blob and any(s in blob for s in text_synonyms):
                    if not _is_tab_selected_js(driver, n):
                        _click_tab_root(driver, n)
                    if _is_tab_selected_js(driver, n):
                        return True
        except Exception as e:
            last_err = e
        time.sleep(0.12)
    if last_err:
        logger.debug("select_tab failed: %r", last_err)
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


# ---------- Диалог черновика ----------

def _is_picker_visible(driver: WebDriver) -> bool:
    try:
        return bool(driver.execute_script(
            """
            const START=new Set(arguments[0].map(s=>String(s||'').toLowerCase()));
            const BACK=new Set(arguments[1].map(s=>String(s||'').toLowerCase()));
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e), r=e.getBoundingClientRect();
              if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2||cs.pointerEvents==='none') return false;
              return r.width>8 && r.height>8 && r.bottom>0 && r.right>0 && r.top<innerHeight && r.left<innerWidth;};

            const anyPicker=[...document.querySelectorAll('picker,.picker')].some(isVis);
            if(anyPicker) return true;

            const footers=[...document.querySelectorAll('.after-footer,.button-group-right')].filter(isVis);
            for(const f of footers){
              const nodes=[...f.querySelectorAll('material-button,button,[role=button],a[role=button]')].filter(isVis);
              if(nodes.length<1) continue;
              const hasStart=nodes.some(n=>{
                const t=((n.innerText||n.textContent||'')+' '+(n.getAttribute('aria-label')||'')).trim().toLowerCase();
                if(!t) return false;
                if((n.className||'').toLowerCase().includes('new-button')) return true;
                for(const w of START) if(w && t.includes(w)) return true;
                return false;
              });
              const hasBack=nodes.some(n=>{
                const t=((n.innerText||n.textContent||'')+' '+(n.getAttribute('aria-label')||'')).trim().toLowerCase();
                if(!t) return false;
                for(const b of BACK) if(b && t.includes(b)) return true;
                return false;
              });
              if(hasStart && hasBack) return true;
            }
            return false;
            """,
            _START_NEW_TEXTS, _GO_BACK_TEXTS
        ))
    except Exception:
        return False


def _find_picker_start_new_button(driver: WebDriver) -> Optional[WebElement]:
    try:
        el = driver.execute_script(
            """
            const START=new Set(arguments[0].map(s=>String(s||'').toLowerCase()));
            const NOBACK=new Set(arguments[1].map(s=>String(s||'').toLowerCase()));
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e), r=e.getBoundingClientRect();
              if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2||cs.pointerEvents==='none') return false;
              return r.width>8 && r.height>8 && r.bottom>0 && r.right>0 && r.top<innerHeight && r.left<innerWidth;};

            const containers=[...document.querySelectorAll('.after-footer,.button-group-right')].filter(isVis);
            let best=null, score=-1;
            for(const c of containers){
              const nodes=[...c.querySelectorAll('material-button,button,[role=button],a[role=button]')].filter(isVis);
              for(const n of nodes){
                const txt=((n.innerText||n.textContent||'')+' '+(n.getAttribute('aria-label')||'')).trim().toLowerCase();
                if(!txt) continue;
                let s=0;
                if((n.className||'').toLowerCase().includes('new-button')) s+=12;
                for(const w of START) if(w && txt.includes(w)) s+=10;
                for(const b of NOBACK) if(b && txt.includes(b)) s-=30;
                if(s>score){best=n; score=s;}
              }
            }
            return best||null;
            """,
            _START_NEW_TEXTS, _GO_BACK_TEXTS,
        )
        if el:
            return el
    except Exception:
        pass
    try:
        el = driver.find_element(By.CSS_SELECTOR, '.after-footer .button-group-right material-button.new-button')
        if _is_interactable(driver, el):
            return el
    except Exception:
        pass
    return None


def _await_draft_picker_and_click_start_new(driver: WebDriver, appear_timeout: float = 8.0, disappear_timeout: float = 15.0) -> bool:
    t_end = time.time() + max(0.5, appear_timeout)
    appeared = False
    while time.time() < t_end:
        if _is_picker_visible(driver):
            appeared = True
            break
        time.sleep(0.15)
    if not appeared:
        return False

    btn = _find_picker_start_new_button(driver)
    if not btn:
        logger.warning("Диалог черновика обнаружен, но кнопка 'Start new' не найдена в футере.")
        return True

    _robust_click(driver, btn, label="start_new")
    logger.info("Обнаружен диалог черновика — нажал 'Start new'.")

    t_dis = time.time() + max(0.5, disappear_timeout)
    while time.time() < t_dis:
        if not _is_picker_visible(driver):
            return True
        time.sleep(0.15)

    logger.warning("Диалог не исчез по таймауту, возможно, клик обрабатывается лениво.")
    return True


# ---------- Антимисклик: Campaign name ----------

def _find_campaign_name_input_best(driver: WebDriver) -> Optional[WebElement]:
    try:
        el = driver.execute_script(
            """
            const LABELS=new Set(arguments[0].map(s=>String(s||'').toLowerCase()));
            const AVOID=new Set(arguments[1].map(s=>String(s||'').toLowerCase()));
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
              if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.15||cs.pointerEvents==='none') return false;
              return r.width>10 && r.height>10 && r.bottom>0 && r.right>0 && r.top<innerHeight && r.left<innerWidth;};
            const bad=(e)=>{
              const t=(e.getAttribute('type')||'text').toLowerCase();
              const rl=(e.getAttribute('role')||'').toLowerCase();
              const pl=((e.getAttribute('placeholder')||'')+' '+(e.getAttribute('aria-label')||'')).toLowerCase();
              if(t==='search') return true;
              if(rl==='combobox') return true;
              if((e.getAttribute('readonly')||'').toLowerCase()==='true') return true;
              if((e.getAttribute('aria-readonly')||'').toLowerCase()==='true') return true;
              if((e.getAttribute('aria-disabled')||'').toLowerCase()==='true') return true;
              if([...AVOID].some(w=> pl.includes(w))) return true;
              return false;
            };

            const inputs=[...document.querySelectorAll('input.input, input.input-area')].filter(isVis);
            let best=null, score=-1;
            for(const inp of inputs){
              if(bad(inp)) continue;
              let s=0;
              const aria=(inp.getAttribute('aria-label')||'').toLowerCase();
              const id=inp.id||'';

              let labText='';
              if(id){
                const lab=document.querySelector(`label[for="${id}"], [id="${id}"] ~ label .label-text`);
                if(lab) labText=(lab.innerText||lab.textContent||'').trim().toLowerCase();
              }
              const near=(inp.closest('material-input,.baseline,.input-container,.campaign-name-view')||document).innerText||'';
              const nearLow=near.trim().toLowerCase();

              const inNameBox = !!(inp.closest('.campaign-name-input, .campaign-name-view'));
              if(inNameBox) s+=20;

              if([...LABELS].some(w=> aria.includes(w))) s+=12;
              if(labText && [...LABELS].some(w=> labText.includes(w))) s+=10;
              if(nearLow && [...LABELS].some(w=> nearLow.includes(w))) s+=6;

              const cls=(inp.className||'').toLowerCase();
              if(/search/.test(cls)) s-=8;

              const auto=(inp.getAttribute('aria-autocomplete')||'').toLowerCase();
              if(!auto) s+=2
              else if(auto=='none') s+=1
              else s-=4;

              if(s>score){best=inp; score=s;}
            }
            return best||null;
            """,
            CAMPAIGN_NAME_LABEL_SYNONYMS, SEARCH_FIELD_SYNONYMS
        )
        if el:
            return el
    except Exception:
        pass

    for sel in CAMPAIGN_NAME_INPUT_SELECTORS:
        try:
            node = driver.find_element(By.CSS_SELECTOR, sel)
            if _is_interactable(driver, node):
                return node
        except Exception:
            continue
    return None


def _set_campaign_name_safe(driver: WebDriver, value: str, input_el: WebElement, attempts: int = 2) -> bool:
    value = value or ""
    for _ in range(max(1, attempts)):
        try:
            driver.execute_script("arguments[0].focus();", input_el)
        except Exception:
            pass

        _dispatch_input_change(driver, input_el, value)
        time.sleep(0.12)
        try:
            ok = driver.execute_script("return arguments[0].value===arguments[1];", input_el, value)
            if ok:
                return True
        except Exception:
            pass

        try:
            input_el.clear()
        except Exception:
            pass
        try:
            input_el.send_keys(value)
            time.sleep(0.08)
            ok2 = driver.execute_script("return arguments[0].value && arguments[0].value.length>0;", input_el)
            if ok2:
                return True
        except Exception:
            pass

        time.sleep(0.15)
    return False


# ---------- Экранные сигнатуры и «значимая» смена URL ----------

_INTERESTING_QUERY_KEYS = {"step", "stage", "flow", "workflowId", "campaignId", "draftId", "wizardStep", "create", "edit"}

def _is_significant_url_change(old_url: str, new_url: str) -> Tuple[bool, str]:
    """Значимая смена: host/path; query — только по «шаговым» ключам. fragment игнорируем."""
    if not old_url or not new_url:
        return False, "empty"
    o, n = urlparse(old_url), urlparse(new_url)
    if (o.scheme, o.netloc) != (n.scheme, n.netloc):
        return True, "host"
    op, np = (o.path or "").rstrip("/"), (n.path or "").rstrip("/")
    if op != np:
        return True, "path"
    if (o.fragment or "") != (n.fragment or ""):
        return False, "fragment"
    if (o.query or "") != (n.query or ""):
        qo, qn = dict(parse_qsl(o.query, keep_blank_values=True)), dict(parse_qsl(n.query, keep_blank_values=True))
        keys_changed = set(qo.keys()) ^ set(qn.keys())
        keys_intersection = keys_changed & _INTERESTING_QUERY_KEYS
        if keys_intersection:
            return True, f"query:{','.join(sorted(keys_intersection))}"
        return False, "query-noise"
    return False, "same"


def _is_selection_screen_visible(driver: WebDriver) -> bool:
    try:
        return bool(driver.execute_script(
            """
            const isVis=e=>{if(!e)return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
              if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.15||cs.pointerEvents==='none') return false;
              return r.width>40 && r.height>40 && r.bottom>0 && r.right>0;};
            const cards = [...document.querySelectorAll('selection-view .cards[role=tablist], div.panel.panel--construction-selection-cards, [data-value="No objective"]')].filter(isVis);
            return cards.length>0;
            """
        ))
    except Exception:
        return False


def _is_construction_layout_visible(driver: WebDriver) -> bool:
    try:
        return bool(driver.execute_script(
            """
            const isVis=e=>{if(!e)return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
              if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.1||cs.pointerEvents==='none') return false;
              return r.width>40 && r.height>40 && r.bottom>0 && r.right>0;};
            const roots = [...document.querySelectorAll('construction-layout, .construction-layout-root')].filter(isVis);
            if(roots.length===0) return false;
            const panels=[...document.querySelectorAll('material-expansionpanel, .panel.themeable')].filter(isVis);
            return panels.length>0;
            """
        ))
    except Exception:
        return False


def _await_post_continue_state(
    driver: WebDriver,
    old_url: str,
    appear_timeout: float = 12.0,
    overall_timeout: float = 25.0,
) -> Tuple[str, Optional[WebElement]]:
    """
    После 1-го Continue ждём один из исходов:
      - ("name_appeared", <input>) — появился инпут Campaign name
      - ("construction_ready", None) — виден экран construction-layout
      - ("no_progress", None) — остались на текущем экране
    """
    gate = time.time() + max(0.5, appear_timeout)
    end = time.time() + max(appear_timeout, overall_timeout)
    stable_construction_ticks = 0
    last_park = 0.0

    while time.time() < end:
        if (time.time() - last_park) > 0.6:
            try:
                hovered = bool(driver.execute_script(
                    """
                    const qs = ['material-popup','.material-popup','.location-suggest-popup','.selections.visible','.selections[pane-id]','.gm-popup','.gm-popup .popup-header','.popup-header','.mdc-tooltip','[role=tooltip]','.cdk-overlay-pane'];
                    const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
                      if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.15||cs.pointerEvents==='none') return false;
                      return r.width>12 && r.height>12 && r.bottom>0 && r.right>0 && r.top<innerHeight && r.left<innerWidth;};
                    return qs.some(sel=>[...document.querySelectorAll(sel)].some(isVis));
                    """
                ))
                if hovered:
                    _park_mouse(driver)
                    last_park = time.time()
            except Exception:
                pass

        el = _find_campaign_name_input_best(driver)
        if el:
            return "name_appeared", el

        cur = driver.current_url or ""
        changed, reason = _is_significant_url_change(old_url, cur)
        if changed:
            if _is_construction_layout_visible(driver):
                logger.info("Значимая смена URL (%s) + construction-layout виден.", reason)
                return "construction_ready", None
            time.sleep(0.25)
            if _is_construction_layout_visible(driver):
                logger.info("Значимая смена URL (%s) + construction-layout проявился с задержкой.", reason)
                return "construction_ready", None

        if _is_construction_layout_visible(driver):
            stable_construction_ticks += 1
            if stable_construction_ticks >= 3:
                return "construction_ready", None
        else:
            stable_construction_ticks = 0

        if time.time() > gate and not _is_selection_screen_visible(driver):
            time.sleep(0.2)

        time.sleep(0.18)

    return "no_progress", None


# ---------- Ожидания переходов после имени ----------

def _wait_url_change_or_name_disappear(driver: WebDriver, old_url: str, timeout: float = 25.0) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        cur = driver.current_url or ""
        changed, _ = _is_significant_url_change(old_url, cur)
        if changed and _is_construction_layout_visible(driver):
            return True
        el = _find_campaign_name_input_best(driver)
        if not el and not _is_picker_visible(driver):
            if _is_construction_layout_visible(driver):
                return True
        time.sleep(0.18)
    cur = driver.current_url or ""
    changed, _ = _is_significant_url_change(old_url, cur)
    return changed and _is_construction_layout_visible(driver)


def _wait_url_change_or_picker_disappear(driver: WebDriver, old_url: str, timeout: float = 25.0) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        cur = driver.current_url or ""
        changed, _ = _is_significant_url_change(old_url, cur)
        if changed and _is_construction_layout_visible(driver):
            return True
        if not _is_picker_visible(driver):
            if _is_construction_layout_visible(driver):
                return True
        time.sleep(0.18)
    cur = driver.current_url or ""
    changed, _ = _is_significant_url_change(old_url, cur)
    return changed and _is_construction_layout_visible(driver)


# ---------- Публичная функция шага ----------

def run_step2(
    driver: WebDriver,
    *,
    choose_type: str = "UBERVERSAL",
    business_name: Optional[str] = None,
    usp: Optional[str] = None,
    site_url: Optional[str] = None,
    budget_per_day: Optional[str] = None,
    budget_clean: Optional[str] = None,
    timeout_total: float = 60.0,
    emit: Optional[Callable[[str], None]] = None,  # <— колбэк комментариев
) -> Dict[str, str]:
    """
    Выполняет шаг 2 строго по сценарию.
    Возвращает: {"campaign_type": <data-value>, "campaign_name": <name|''>, "business_name": <унифицированное имя|''>}.
    """
    t0 = time.time()
    _dismiss_soft_dialogs(driver)

    # 1) Цель: без подсказок
    _emit(emit, "Беру режим «без подсказок», настроим всё вручную")
    logger.info("Выбираю 'Create a campaign without guidance'…")
    ok = _select_tab_by_datavalue_or_text(
        driver, data_value=NO_GUIDANCE_DATA_VALUE, text_synonyms=NO_GUIDANCE_SYNONYMS,
        scope_css='div.panel.panel--construction-selection-cards', timeout=10.0,
    )
    if not ok:
        ok = _select_tab_by_datavalue_or_text(driver, NO_GUIDANCE_DATA_VALUE, NO_GUIDANCE_SYNONYMS, None, 5.0)
    if not ok:
        _emit(emit, "Не вижу переключатель «без подсказок» — стоп")
        raise RuntimeError("Не удалось выбрать 'Create a campaign without guidance'.")
    _emit(emit, "Готово: подсказки отключены")

    # 2) Тип кампании
    target_code = (choose_type or "UBERVERSAL").strip()
    human_label = _short_type_label_by_code(target_code)
    _emit(emit, f"Выбираю тип кампании: {human_label}")
    logger.info("Выбираю тип кампании: %s", target_code)
    ok = _select_tab_by_datavalue_or_text(
        driver, data_value=target_code,
        text_synonyms=PERFMAX_SYNONYMS if target_code.upper()=="UBERVERSAL" else [target_code.lower()],
        scope_css=None, timeout=10.0,
    )
    if not ok:
        _emit(emit, f"Тип {human_label} не нашёл — стоп")
        raise RuntimeError(f"Не удалось выбрать тип кампании '{target_code}'.")
    _emit(emit, "Тип выбран — двигаемся дальше")

    _close_guided_steps_if_any(driver, appear_timeout=0.8, disappear_timeout=8.0)

    campaign_name_value = ""
    business_name_generated = ""

    def _ensure_business_name(cname: str) -> None:
        nonlocal business_name_generated
        if business_name_generated or not cname:
            return
        suffix_local = _extract_numeric_suffix(cname)
        business_name_generated = _derive_unique_business_name(
            original_name=business_name,
            campaign_name=cname,
            suffix=suffix_local,
            type_label=human_label,
            max_len=45,
        )
        if business_name_generated:
            logger.info("Унифицирую Business name: %s", business_name_generated)
            _emit(emit, f"Обновляю бизнес-имя: «{business_name_generated}»")

    def _compute_name_once() -> str:
        nonlocal campaign_name_value
        if campaign_name_value:
            _ensure_business_name(campaign_name_value)
            return campaign_name_value
        try:
            cname = _generate_campaign_name_via_llm(
                business_name=business_name,
                usp=usp,
                site_url=site_url,
                budget=(budget_clean or budget_per_day or ""),
                campaign_type_label=human_label,
            )
        except Exception as e:
            logger.warning("LLM campaign_name failed: %s — fallback.", e)
            cname = _fallback_campaign_name(business_name=business_name, campaign_type_code=target_code)
        campaign_name_value = cname
        _ensure_business_name(cname)
        return campaign_name_value

    # 3) Первый Continue
    _emit(emit, "Жму «Продолжить»")
    logger.info("Нажимаю Continue/Next (1/2)…")
    _dismiss_soft_dialogs(driver, budget_ms=600)
    _close_guided_steps_if_any(driver, appear_timeout=0.5, disappear_timeout=6.0)
    _dismiss_hover_popups(driver)

    old_url_1 = driver.current_url or ""
    if not _click_continue_button(driver):
        _close_guided_steps_if_any(driver, appear_timeout=1.0, disappear_timeout=8.0)
        _dismiss_soft_dialogs(driver, budget_ms=800)
        _dismiss_hover_popups(driver)
        if not _click_continue_button(driver):
            _emit(emit, "Кнопка «Продолжить» не нажалась — останавливаюсь")
            raise RuntimeError("Кнопка Continue/Next (1/2) не нажалась.")

    # 4) Ждём ИСТИННЫЙ прогресс/имя
    outcome, name_el = _await_post_continue_state(driver, old_url_1, appear_timeout=10.0, overall_timeout=28.0)

    if outcome == "construction_ready":
        _emit(emit, "Экран сменился — перехожу к настройке кампании")
        logger.info("Экран перешёл дальше (construction-layout).")
    elif outcome == "name_appeared" and name_el is not None:
        _emit(emit, "Попросили имя кампании — сейчас придумаю короткое")
        logger.info("После первого Continue всплыл 'Campaign name' — генерирую имя…")

        _mouse_jiggle(driver, amplitude=16, repeats=2)  # убрать hover перед вводом

        cname = _compute_name_once()
        _emit(emit, f"Имя кампании: «{cname}»")
        target_input = _find_campaign_name_input_best(driver) or name_el
        ok_set = _set_campaign_name_safe(driver, cname, target_input, attempts=2)
        if not ok_set:
            target_input = _find_campaign_name_input_best(driver) or target_input
            ok_set = _set_campaign_name_safe(driver, cname, target_input, attempts=2)
        if not ok_set:
            _emit(emit, "Не удалось ввести имя — стоп")
            raise RuntimeError("Не удалось надёжно ввести Campaign name (анти‑мисклик защита).")
        logger.info("Заполнено Campaign name: %s", cname)

        try:
            target_input.send_keys(Keys.TAB)
        except Exception:
            pass
        _mouse_jiggle(driver, amplitude=16, repeats=2)
        time.sleep(0.2)

        # 5) Второй Continue
        _emit(emit, "Имя готово — продолжаю")
        logger.info("Нажимаю Continue/Next (2/2)…")
        _dismiss_soft_dialogs(driver, budget_ms=600)
        _close_guided_steps_if_any(driver, appear_timeout=0.4, disappear_timeout=6.0)
        _dismiss_hover_popups(driver)
        _mouse_jiggle(driver, amplitude=12, repeats=1)

        old_url_2 = driver.current_url or ""
        if not _click_continue_button(driver):
            _close_guided_steps_if_any(driver, appear_timeout=0.8, disappear_timeout=8.0)
            _dismiss_soft_dialogs(driver, budget_ms=700)
            _dismiss_hover_popups(driver)
            _mouse_jiggle(driver, amplitude=12, repeats=1)
            if not _click_continue_button(driver):
                _emit(emit, "Повторное «Продолжить» не сработало — стоп")
                raise RuntimeError("Кнопка Continue/Next (2/2) не нажалась после ввода имени.")

        # Диалог черновика
        picker_seen = _await_draft_picker_and_click_start_new(driver, appear_timeout=8.0, disappear_timeout=15.0)
        if picker_seen:
            _emit(emit, "Вижу выбор черновика — жму «Start new»")
            if not _wait_url_change_or_picker_disappear(driver, old_url_2, timeout=25.0):
                _await_draft_picker_and_click_start_new(driver, appear_timeout=2.0, disappear_timeout=10.0)
                if not _wait_url_change_or_picker_disappear(driver, old_url_2, timeout=18.0):
                    _emit(emit, "После «Start new» переход не случился — стоп")
                    raise RuntimeError("После 'Start new' переход не произошёл.")
        else:
            if not _wait_url_change_or_name_disappear(driver, old_url_2, timeout=25.0):
                _dismiss_soft_dialogs(driver, budget_ms=800)
                _close_guided_steps_if_any(driver, appear_timeout=0.6, disappear_timeout=6.0)
                _dismiss_hover_popups(driver)
                _mouse_jiggle(driver, amplitude=12, repeats=1)
                if not _click_continue_button(driver):
                    _emit(emit, "Повторное «Продолжить» не сработало — стоп")
                    raise RuntimeError("Повторный Continue (2/2) не нажался.")
                if not _wait_url_change_or_name_disappear(driver, old_url_2, timeout=20.0):
                    _emit(emit, "Переход после имени не подтвердился — стоп")
                    raise RuntimeError("После ввода имени переход не произошёл.")
    else:
        # no_progress — повторяем Continue после зачистки ховеров
        _emit(emit, "Прогресса нет — ещё раз жму «Продолжить»")
        logger.info("После первого Continue прогресса нет — увод курсора, зачистка попапов и повторный клик.")
        _dismiss_hover_popups(driver)
        _close_guided_steps_if_any(driver, appear_timeout=0.5, disappear_timeout=6.0)
        if not _click_continue_button(driver):
            _dismiss_hover_popups(driver)
            if not _click_continue_button(driver):
                _emit(emit, "Кнопка не нажимается — стоп")
                raise RuntimeError("Повторный Continue (1/2) не нажался (после no_progress).")

        outcome2, name_el2 = _await_post_continue_state(driver, old_url_1, appear_timeout=8.0, overall_timeout=25.0)
        if outcome2 == "construction_ready":
            _emit(emit, "Готово, перешёл к настройке кампании")
            logger.info("Экран перешёл дальше со второй попытки (construction-layout).")
        elif outcome2 == "name_appeared" and name_el2 is not None:
            _emit(emit, "Запроcили имя кампании — сейчас заполню")
            logger.info("Вторая попытка: всплыл 'Campaign name'. Заполняю…")

            _mouse_jiggle(driver, amplitude=16, repeats=2)

            cname = _compute_name_once()
            _emit(emit, f"Имя кампании: «{cname}»")
            target_input = _find_campaign_name_input_best(driver) or name_el2
            if not _set_campaign_name_safe(driver, cname, target_input, attempts=2):
                _emit(emit, "Не удаётся ввести имя — стоп")
                raise RuntimeError("Не удалось ввести Campaign name со второй попытки.")

            try:
                target_input.send_keys(Keys.TAB)
            except Exception:
                pass
            _mouse_jiggle(driver, amplitude=16, repeats=2)
            time.sleep(0.15)

            old_url_2 = driver.current_url or ""
            _dismiss_hover_popups(driver)
            _mouse_jiggle(driver, amplitude=12, repeats=1)
            if not _click_continue_button(driver):
                _dismiss_hover_popups(driver)
                _mouse_jiggle(driver, amplitude=12, repeats=1)
                if not _click_continue_button(driver):
                    _emit(emit, "Кнопка «Продолжить» не нажалась — стоп")
                    raise RuntimeError("Кнопка Continue (2/2) не нажалась (ветка no_progress).")

            picker_seen = _await_draft_picker_and_click_start_new(driver, appear_timeout=8.0, disappear_timeout=15.0)
            if picker_seen:
                _emit(emit, "Нажал «Start new» в выборе черновика")
                if not _wait_url_change_or_picker_disappear(driver, old_url_2, timeout=25.0):
                    _emit(emit, "После «Start new» переход не случился — стоп")
                    raise RuntimeError("После 'Start new' переход не произошёл (ветка no_progress).")
            else:
                if not _wait_url_change_or_name_disappear(driver, old_url_2, timeout=25.0):
                    _emit(emit, "Переход после имени не подтвердился — стоп")
                    raise RuntimeError("После ввода имени переход не произошёл (ветка no_progress).")
        else:
            _emit(emit, "Экран не сменился — проверьте тип кампании")
            raise RuntimeError("После Continue экран не сменился: вероятно, попапы блокируют или тип кампании не выбран.")

    elapsed = int((time.time() - t0) * 1000)
    logger.info(
        "Шаг 2 завершён (%d ms). Тип: %s; Campaign name: %s; Business name: %s",
        elapsed,
        target_code,
        campaign_name_value or "<not set>",
        business_name_generated or (business_name or "<unchanged>"),
    )
    _emit(emit, "Шаг готов — двигаюсь к следующему")
    result: Dict[str, str] = {"campaign_type": target_code, "campaign_name": campaign_name_value}
    if business_name_generated:
        result["business_name"] = business_name_generated
    return result
