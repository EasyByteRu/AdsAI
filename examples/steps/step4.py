# -*- coding: utf-8 -*-
"""
ads_ai/gads/step4.py

Шаг 4 Google Ads Wizard (Campaign settings):
  1) Locations → выбрать "Enter another location" и по очереди добавить переданные локации (ввод → Enter).
  2) Languages → перед добавлением очистить уже выбранные языки (крестики в чипсах), затем добавить новые.
  3) EU political ads → выбрать "No, this campaign doesn't have EU political ads".
  4) Нажать Next/Continue и дождаться перехода.
  5) НОВОЕ: если в любой момент всплывает окно безопасности "Confirm it's you" —
     вызываем обработчик 2FA, вводим код и возвращаемся к шагу.

Контракт:
    run_step4(
        driver: WebDriver,
        *,
        locations: Iterable[str] | None = None,
        languages: Iterable[str] | None = None,
        eu_political_ads_no: bool = True,
        timeout_total: float = 90.0,
        emit: Optional[Callable[[str], None]] = None,
    ) -> dict

Возврат:
    {
      "locations_added": [str,...],
      "languages_selected": [str,...],
      "eu_political_ads": "no"|"yes"|"skip",
      "new_url": str,
      "duration_ms": int
    }
"""

from __future__ import annotations

import logging
import time
from typing import Callable, Iterable, List, Optional, Dict, Tuple

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webdriver import WebDriver, WebElement
from selenium.common.exceptions import StaleElementReferenceException

# === Confirm-it's-you (2FA) хелпер ===
try:
    # модуль-обработчик из examples/steps/
    from examples.steps.code_for_confrim import (
        handle_confirm_its_you,
        wait_code_from_env_or_file,
    )
except Exception:  # pragma: no cover
    # если модуль не найден — делаем «пустышки» (не ломаем совместимость)
    def handle_confirm_its_you(*args, **kwargs) -> bool:  # type: ignore
        return False
    def wait_code_from_env_or_file(*args, **kwargs) -> Optional[str]:  # type: ignore
        return None

logger = logging.getLogger("ads_ai.gads.step4")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

def _emit(emit: Optional[Callable[[str], None]], text: str) -> None:
    """Безопасно отправляет короткий комментарий в UI."""
    if callable(emit) and isinstance(text, str) and text.strip():
        try:
            emit(text.strip())
        except Exception:
            pass

# ======== I18N словари ========

PANEL_LOCATIONS = [
    "locations", "местополож", "располож", "локаци", "ubicaciones", "localizações", "localizacoes", "lieux",
    "standorte", "località", "localita", "lokalizacje", "konumlar", "розташув", "місця", "位置", "位置設定", "ubicazioni"
]
PANEL_LANGUAGES = [
    "languages", "язык", "языки", "idiomas", "línguas", "linguas", "langues", "sprachen",
    "lingue", "języki", "diller", "мови", "语言", "言語"
]
PANEL_EU_ADS = [
    "eu political", "политич", "політич", "políticas de la ue", "publicidade política da ue",
    "annonces politiques ue", "eu-politische", "annunci politici ue", "reklamy polityczne ue",
    "siyasi reklamlar ab", "політичні оголошення єс", "欧盟 政治 广告"
]

ENTER_ANOTHER_LOCATION_SYNS = [
    "enter another location", "enter a location", "add location", "custom location",
    "введите", "другая локация", "другое местоположение", "другой регион", "добавить локацию",
    "ingresar otra ubicación", "otra ubicación",
    "inserir outra localização", "outra localização",
    "autre lieu", "autre emplacement",
    "weitere standort", "anderen standort",
    "inserisci un'altra località", "altra località",
    "inna lokalizacja",
    "başka konum",
    "інше місце", "іншу локацію",
    "输入位置", "其他位置", "別の場所"
]

NEXT_SYNS = [
    "continue", "next", "save and continue",
    "продолжить", "далее", "сохранить и продолжить", "далі",
    "continuar", "siguiente", "guardar y continuar",
    "avançar", "próximo", "próxima",
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
BACK_SYNS = ["back", "назад", "atrás", "zurück", "retour", "voltar", "上一页", "上一步", "戻る", "뒤로", "zpět", "späť", "wstecz"]

# Слова, которые НЕ являются Next и часто уводят не туда (напр. "More settings")
AVOID_NEXT_MISCLICK = [
    "more settings", "advanced settings", "more options", "show details", "learn more",
    "подробнее", "дополнительные настройки", "больше настроек", "другие параметры",
    "свернуть", "развернуть", "настройки", "опции", "дополнительно",
    "cancel", "отмена"
]

NO_SYNS = [
    "no", "нет", "не", "não", "não,", "nein", "non", "no,", "nie", "hayır", "ні", "不是", "いいえ", "아니오"
]

# Коды → имена языков (EN), чтобы нормально искать в списке
LANG_CODE_TO_NAME = {
    "en": "English", "ru": "Russian", "de": "German", "fr": "French", "es": "Spanish",
    "pt": "Portuguese", "it": "Italian", "pl": "Polish", "tr": "Turkish", "uk": "Ukrainian",
    "ua": "Ukrainian", "zh": "Chinese", "ja": "Japanese", "ko": "Korean", "th": "Thai",
    "vi": "Vietnamese", "ar": "Arabic", "hi": "Hindi", "id": "Indonesian",
}


# ======== Confirm watcher ========

def _is_confirm_dialog_visible(driver: WebDriver) -> bool:
    try:
        return bool(
            driver.execute_script(
                """
                const isVis=(e)=>{ if(!e) return false; const cs=getComputedStyle(e);
                    if(cs.display==='none'||cs.visibility==='hidden'||parseFloat(cs.opacity||'1')<0.2) return false;
                    const r=e.getBoundingClientRect(); return r.width>200 && r.height>120; };
                const roots=[...document.querySelectorAll('[role="dialog"], material-dialog, .mdc-dialog--open, .dialog')].filter(isVis);
                const KEYS=['confirm it\\'s you','confirm it’s you','подтвердите, что это вы','подтвердите что это вы'];
                for(const root of roots){
                    const txt=((root.getAttribute('aria-label')||'')+' '+(root.innerText||root.textContent||'')).toLowerCase();
                    if(KEYS.some(k=>txt.includes(k))) return true;
                }
                return false;
                """
            )
        )
    except Exception:
        return False


def _maybe_handle_confirm_its_you(driver: WebDriver, emit: Optional[Callable[[str], None]]) -> bool:
    """
    Лёгкий «пробник»: если окна подтверждения личности нет — возвращает False (ничего не делает).
    Если диалог уже открыт — вызывает полный проход 2FA (вплоть до ожидания кода),
    по завершении возвращает True. Исключения наружу не выбрасывает.
    """
    try:
        if not _is_confirm_dialog_visible(driver):
            return False
    except Exception:
        pass
    try:
        return bool(handle_confirm_its_you(
            driver,
            emit=emit,
            wait_code_cb=wait_code_from_env_or_file,
            timeout_total=180.0,
            max_attempts=3,
        ))
    except Exception:
        return False


# ======== Утилиты ========

def _is_interactable(driver: WebDriver, el: WebElement) -> bool:
    try:
        if not el.is_displayed():
            return False
        if not el.is_enabled():
            return False
        if (el.get_attribute("aria-disabled") or "").lower() == "true":
            return False
        driver.execute_script("arguments[0].scrollIntoView({block:'center',inline:'nearest'});", el)
        r = el.rect
        return r.get("width", 0) >= 8 and r.get("height", 0) >= 8
    except Exception:
        return False


def _dismiss_soft_dialogs(driver: WebDriver, budget_ms: int = 900) -> None:
    """Лёгкие попапы (cookies/ok/got it) — закрываем мягко."""
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
                        time.sleep(0.15)
                        hit = True
                        break
                if hit:
                    break
            if not hit:
                break
        except Exception:
            break


def _ensure_panel_open(driver: WebDriver, synonyms: List[str]) -> bool:
    """Открыть нужную секцию (Locations/Languages/EU ads) по заголовку/aria-label (любой язык)."""
    _maybe_handle_confirm_its_you(driver, emit=None)
    try:
        return bool(driver.execute_script(
            """
            const KEYS=new Set(arguments[0].map(s=>String(s||'').toLowerCase()));
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
              if(cs.display==='none'||cs.visibility==='hidden'||parseFloat(cs.opacity||'1')<0.2) return false;
              return r.width>20 && r.height>20;};
            const headers=[...document.querySelectorAll('.main-header .header[role=button], .main-header[role=heading] .header[role=button]')];
            let toggled=false;
            for(const h of headers){
              const t=((h.getAttribute('aria-label')||'')+' '+(h.innerText||h.textContent||'')).toLowerCase();
              if([...KEYS].some(k=>t.includes(k))){
                const exp=(h.getAttribute('aria-expanded')||'').toLowerCase()==='true';
                if(!exp){
                  try{ h.click(); toggled=true; }catch(e){ try{ h.querySelector('material-icon, .expand-button')?.click(); toggled=true; }catch(e2){} }
                }else{
                  if(!isVis(h.closest('.panel')?.querySelector('.main'))){
                    try{ h.click(); toggled=true; }catch(e){}
                  }
                }
              }
            }
            return toggled || true;
            """,
            synonyms
        ))
    except Exception:
        return False


# ======== Locations ========

def _select_enter_another_location(driver: WebDriver) -> bool:
    """В радиогруппе локаций выбрать 'Enter another location' (i18n-поиск)."""
    _maybe_handle_confirm_its_you(driver, emit=None)
    try:
        el = driver.execute_script(
            """
            const SYNS=new Set(arguments[0].map(s=>String(s||'').toLowerCase()));
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
              if(cs.display==='none'||cs.visibility==='hidden'||parseFloat(cs.opacity||'1')<0.2) return false;
              return r.width>20 && r.height>20;};
            const scope=document.querySelector('basic-geopicker-editor, location-with-options') || document;
            const radios=[...scope.querySelectorAll('material-radio, .radio[role=radio]')].filter(isVis);
            let target=null;
            for(const r of radios){
              const text=((r.innerText||r.textContent||'')+' '+(r.getAttribute('aria-label')||'')).toLowerCase();
              if([...SYNS].some(s=>text.includes(s))){ target=r; break; }
            }
            if(!target) return false;
            const checked=(target.getAttribute('aria-checked')||'').toLowerCase()==='true' || !!target.querySelector('input[type=radio]:checked');
            if(!checked){
              try{ target.click(); }catch(e){ try{ target.querySelector('.content,.icon-container')?.click(); }catch(e2){} }
            }
            return true;
            """,
            ENTER_ANOTHER_LOCATION_SYNS
        )
        return bool(el)
    except Exception:
        return False


def _find_location_input(driver: WebDriver) -> Optional[WebElement]:
    sels = [
        'custom-location-input location-suggest-input input.input',
        'location-suggest-input input.input',
        'basic-geopicker-editor input.input'
    ]
    for s in sels:
        try:
            el = driver.find_element(By.CSS_SELECTOR, s)
            if _is_interactable(driver, el):
                return el
        except Exception:
            continue
    return None


def _location_add_one(driver: WebDriver, query: str, wait_ms: int = 3000) -> bool:
    """Ввод локации и выбор первого подходящего предложения (ArrowDown/Enter + фоллбеки)."""
    _maybe_handle_confirm_its_you(driver, emit=None)

    inp = _find_location_input(driver)
    if not inp:
        return False

    try:
        driver.execute_script("arguments[0].focus();", inp)
    except Exception:
        pass
    try:
        inp.clear()
    except Exception:
        pass

    _maybe_handle_confirm_its_you(driver, emit=None)

    try:
        inp.send_keys(query)
    except Exception:
        return False

    time.sleep(0.25)

    # Попробуем выбрать первую подсказку: ArrowDown → Enter
    try:
        inp.send_keys(Keys.ARROW_DOWN)
        time.sleep(0.1)
        inp.send_keys(Keys.ENTER)
    except Exception:
        pass

    # Если не сработало, пробуем просто Enter
    time.sleep(0.2)
    try:
        inp.send_keys(Keys.ENTER)
    except Exception:
        pass

    # Фоллбек: клик по первому элементу списка подсказок
    try:
        driver.execute_script(
            """
            const pop=document.querySelector('.location-suggest-popup, material-popup[arialabel="Location suggestions"]');
            if(pop){
              const opt=pop.querySelector('[role=option], material-select-dropdown-item.list-item, .list-item.item, material-list .list-group [role=option]');
              if(opt){ opt.click(); }
            }
            """
        )
    except Exception:
        pass

    # Подождём, пока подсказка скроется/очистится инпут
    t_end = time.time() + (wait_ms / 1000.0)
    while time.time() < t_end:
        _maybe_handle_confirm_its_you(driver, emit=None)
        try:
            vis = driver.execute_script(
                """
                const pop=document.querySelector('.location-suggest-popup, material-popup[arialabel="Location suggestions"]');
                const isVis=(e)=>{if(!e) return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
                  if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2) return false;
                  return r.width>8 && r.height>8 && r.bottom>0 && r.right>0;};
                return pop && isVis(pop);
                """
            )
            if not vis:
                return True
        except Exception:
            return True
        time.sleep(0.15)

    return True


def _add_locations(driver: WebDriver, locations: Iterable[str], emit: Optional[Callable[[str], None]] = None) -> List[str]:
    added: List[str] = []
    if not locations:
        return added

    _emit(emit, "Перехожу к разделу «География»")
    _ensure_panel_open(driver, PANEL_LOCATIONS)
    if _select_enter_another_location(driver):
        _emit(emit, "Режим ввода локаций включён")

    for raw in locations:
        _maybe_handle_confirm_its_you(driver, emit=emit)
        q = (raw or "").strip()
        if not q:
            continue
        _emit(emit, f"Добавляю локацию: {q}")
        ok = _location_add_one(driver, q)
        logger.info("Locations: add '%s' -> %s", q, "ok" if ok else "fail")
        if ok:
            added.append(q)
        time.sleep(0.2)
    return added


# ======== Languages ========

def _normalize_language_name(name: str) -> str:
    s = (name or "").strip()
    if not s:
        return s
    low = s.lower()
    if low in LANG_CODE_TO_NAME:
        return LANG_CODE_TO_NAME[low]
    if len(s) in (2, 3) and low in LANG_CODE_TO_NAME:
        return LANG_CODE_TO_NAME[low]
    return s[0].upper() + s[1:] if s else s


def _find_language_input(driver: WebDriver) -> Optional[WebElement]:
    sels = [
        'language-selector material-auto-suggest-input input.input',
        'languages material-auto-suggest-input input.input',
        'material-auto-suggest-input input.input',
    ]
    for s in sels:
        try:
            el = driver.find_element(By.CSS_SELECTOR, s)
            if _is_interactable(driver, el):
                return el
        except Exception:
            continue
    return None


def _language_is_already_selected(driver: WebDriver, name: str) -> bool:
    try:
        return bool(driver.execute_script(
            """
            const needle=String(arguments[0]||'').toLowerCase();
            const chips=[...document.querySelectorAll('.chips[aria-label="Selected languages"] material-chip .content, .chips[aria-label="Selected languages"] material-chip .content *')];
            for(const c of chips){
              const t=(c.innerText||c.textContent||'').trim().toLowerCase();
              if(t && (t===needle || t.includes(needle))) return true;
            }
            return false;
            """,
            name
        ))
    except Exception:
        return False


def _select_language_from_popup(driver: WebDriver, name: str) -> bool:
    try:
        return bool(driver.execute_script(
            """
            const needle=String(arguments[0]||'').toLowerCase();
            const pops=[...document.querySelectorAll('material-popup.selections[aria-expanded="true"], material-popup.selections[aria-expanded="true"] .popup, .selections.visible .popup, .selections.visible')]
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
              if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2) return false;
              return r.width>8 && r.height>8 && r.bottom>0 && r.right>0;};
            for(const p of pops){
              if(!isVis(p)) continue;
              const items=[...p.querySelectorAll('material-select-dropdown-item.list-item, [role=option]')];
              let cand=null;
              for(const it of items){
                const t=(it.innerText||it.textContent||'').trim().toLowerCase();
                if(t.includes(needle)){ cand=it; break; }
              }
              if(!cand && items.length>0) cand=items[0];
              if(cand){
                try{ cand.click(); }catch(e){ try{ cand.querySelector('material-checkbox,.content,.icon')?.click(); }catch(e2){} }
                return true;
              }
            }
            return false;
            """,
            name
        ))
    except Exception:
        return False


def _clear_selected_languages(driver: WebDriver, max_rounds: int = 4) -> int:
    _ensure_panel_open(driver, PANEL_LANGUAGES)
    removed = 0
    for _ in range(max_rounds):
        _maybe_handle_confirm_its_you(driver, emit=None)
        btns: List[WebElement] = []
        for sel in [
            '.chips[aria-label="Selected languages"] material-chip .delete-button',
            '.chips[aria-label="Selected languages"] [aria-label$=" remove"]',
            '[aria-label="Selected languages"] .delete-button',
        ]:
            try:
                btns.extend(driver.find_elements(By.CSS_SELECTOR, sel))
            except Exception:
                continue
        filtered = []
        for b in btns:
            try:
                if _is_interactable(driver, b):
                    filtered.append(b)
            except Exception:
                continue
        if not filtered:
            break
        for b in filtered:
            try:
                label = (b.get_attribute("aria-label") or "").strip()
                logger.info("Languages: remove chip %r", label or "<chip>")
                try:
                    b.click()
                except Exception:
                    try:
                        driver.execute_script("arguments[0].click();", b)
                    except Exception:
                        pass
                time.sleep(0.08)
                removed += 1
            except StaleElementReferenceException:
                removed += 1
            except Exception:
                continue
        time.sleep(0.15)
    logger.info("Languages: cleared %d selected chip(s).", removed)
    return removed


def _add_language(driver: WebDriver, lang: str) -> bool:
    _maybe_handle_confirm_its_you(driver, emit=None)

    name = _normalize_language_name(lang)
    if not name:
        return False

    _ensure_panel_open(driver, PANEL_LANGUAGES)
    if _language_is_already_selected(driver, name):
        logger.info("Language already selected: %s", name)
        return True

    inp = _find_language_input(driver)
    if not inp:
        return False

    try:
        driver.execute_script("arguments[0].focus();", inp)
    except Exception:
        pass
    try:
        inp.clear()
    except Exception:
        pass

    _maybe_handle_confirm_its_you(driver, emit=None)

    try:
        inp.send_keys(name)
    except Exception:
        return False

    popup_deadline = time.time() + 1.8
    picked = False
    while time.time() < popup_deadline:
        _maybe_handle_confirm_its_you(driver, emit=None)
        picked = _select_language_from_popup(driver, name)
        if picked or _language_is_already_selected(driver, name):
            break
        time.sleep(0.06)

    if not picked and not _language_is_already_selected(driver, name):
        try:
            inp.send_keys(Keys.ARROW_DOWN)
            time.sleep(0.05)
            inp.send_keys(Keys.ENTER)
        except Exception:
            pass

    t_end = time.time() + 2.8
    while time.time() < t_end:
        _maybe_handle_confirm_its_you(driver, emit=None)
        if _language_is_already_selected(driver, name):
            return True
        time.sleep(0.08)
    return _language_is_already_selected(driver, name)


def _add_languages(driver: WebDriver, languages: Iterable[str], clear_before: bool = True, emit: Optional[Callable[[str], None]] = None) -> List[str]:
    selected: List[str] = []
    _ensure_panel_open(driver, PANEL_LANGUAGES)
    if clear_before:
        _emit(emit, "Очищаю выбранные языки")
        _clear_selected_languages(driver)
    for lang in languages:
        _maybe_handle_confirm_its_you(driver, emit=emit)
        if not lang:
            continue
        name = _normalize_language_name(lang)
        _emit(emit, f"Добавляю язык: {name}")
        ok = _add_language(driver, lang)
        logger.info("Language: add '%s' -> %s", lang, "ok" if ok else "fail")
        if ok:
            selected.append(_normalize_language_name(lang))
        time.sleep(0.06)
    return selected


# ======== EU Political Ads ========

def _choose_eu_political_ads_no(driver: WebDriver) -> bool:
    _maybe_handle_confirm_its_you(driver, emit=None)
    _ensure_panel_open(driver, PANEL_EU_ADS)
    try:
        return bool(driver.execute_script(
            """
            const NO=new Set(arguments[0].map(s=>String(s||'').toLowerCase()));
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e),r=e.getBoundingClientRect();
              if(cs.display==='none'||cs.visibility==='hidden'||parseFloat(cs.opacity||'1')<0.2) return false;
              return r.width>20 && r.height>20;};
            let root=document.querySelector('eu-political-ads')?.closest('.panel') || document;
            if(!root){
              const headers=[...document.querySelectorAll('.main-header .header[role=button]')];
              for(const h of headers){
                const t=((h.getAttribute('aria-label')||'')+' '+(h.innerText||h.textContent||'')).toLowerCase();
                if(t.includes('eu') && (t.includes('polit') || t.includes('полит') || t.includes('polít') || t.includes('politic'))){
                  root=h.closest('.panel')||document;
                  break;
                }
              }
            }
            const group=(root||document).querySelector('material-radio-group') || document;
            let no = group.querySelector('.noOption input[type=radio], .noOption [role=radio], .noOption');
            if(!no){
              const radios=[...group.querySelectorAll('input[type=radio], material-radio, [role=radio]')].filter(isVis);
              for(const r of radios){
                const lab=(r.getAttribute('aria-label')||'') + ' ' + (r.innerText||r.textContent||'');
                const tl=lab.trim().toLowerCase();
                if([...NO].some(n=> tl.startsWith(n+' ') || tl===n || tl.includes(' '+n+' ') )){
                  no=r; break;
                }
              }
            }
            if(!no) return false;
            try{
              if(no.tagName && no.tagName.toLowerCase()==='input'){ no.click(); return true; }
              const inp=no.querySelector('input[type=radio]') || no;
              inp.click(); return true;
            }catch(e){
              try{ no.querySelector('.mdc-radio__native-control, .radio-content, label, .mdc-radio, .content')?.click(); return true; }catch(e2){}
            }
            return false;
            """,
            NO_SYNS
        ))
    except Exception:
        return False


# ======== Next / переход ========

def _bounce_scroll(driver: WebDriver) -> None:
    """Немного «раскачиваем» скролл, чтобы принудить лэйаут/ленивую отрисовку футера с кнопками."""
    try:
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(0.08)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.08)
        driver.execute_script("window.scrollTo(0, Math.floor(document.body.scrollHeight*0.6));")
        time.sleep(0.06)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    except Exception:
        pass


def _find_next_button_any_language(driver: WebDriver) -> Optional[WebElement]:
    """
    Ищем правильную кнопку Next/Continue. Приоритет:
      1) Последняя видимая <button.button-next> вне .more-settings-panel / .additionalSettingsContainer.
      2) Кнопка с текстом из NEXT_SYNS в контейнере .buttons или в gac-flow-buttons.
      3) Любая <button> с .button-next.
    """
    # Фаза 1: JS-скан по всему документу — последняя видимая button.button-next
    _maybe_handle_confirm_its_you(driver, emit=None)
    try:
        el = driver.execute_script(
            """
            const AVOID_SEL = '.additionalSettingsContainer, .more-settings-panel, [role="dialog"], modal, [aria-modal="true"]';
            const isVis = (e)=>{ if(!e) return false;
              const cs=getComputedStyle(e), r=e.getBoundingClientRect();
              if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2||cs.pointerEvents==='none') return false;
              return r.width>8 && r.height>8 && r.bottom>0 && r.right>0; };
            const all = [...document.querySelectorAll('button.button-next, .buttons button.button-next, gac-flow-buttons button.button-next')];
            const good = all.filter(b => isVis(b) && !b.closest(AVOID_SEL));
            if (good.length) return good[good.length-1];
            return null;
            """
        )
        if el:
            return el  # уже нашли «правильный» вариант
    except Exception:
        pass

    # Фаза 2: контейнеры .buttons / gac-flow-buttons по тексту
    try:
        el = driver.execute_script(
            """
            const NEXT=new Set(arguments[0].map(s=>String(s||'').toLowerCase()));
            const BACK=new Set(arguments[1].map(s=>String(s||'').toLowerCase()));
            const AVOID=new Set(arguments[2].map(s=>String(s||'').toLowerCase()));
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e), r=e.getBoundingClientRect();
              if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2||cs.pointerEvents==='none') return false;
              return r.width>8 && r.height>8 && r.top<innerHeight && r.left<innerWidth && r.bottom>0 && r.right>0; };
            const notDisabled=e=>!( (e.getAttribute('aria-disabled')||'').toLowerCase()==='true' || e.hasAttribute('disabled') );
            const root = document;
            const nodes = [...root.querySelectorAll('gac-flow-buttons .buttons button, .buttons button, gac-flow-buttons button, button')].filter(isVis).filter(notDisabled);
            let best=null, score=-1;
            for(const n of nodes){
              const cls=(n.className||'').toLowerCase();
              const txt=(()=>{const s=[n.innerText||n.textContent||'', n.getAttribute('aria-label')||'']; return s.join(' ').trim().toLowerCase();})();
              if(!txt && !cls.includes('button-next')) continue;
              if(n.closest('.additionalSettingsContainer') || n.closest('.more-settings-panel')) continue;
              let s=0;
              const hasNextWord=[...NEXT].some(w=> txt.includes(w));
              const looksNext=cls.includes('button-next') || cls.includes('mdc-button--unelevated') || cls.includes('highlighted');
              if(!hasNextWord && !looksNext) continue;
              if([...BACK].some(b=> txt.includes(b))) continue;
              if([...AVOID].some(a=> txt.includes(a))) continue;
              if(hasNextWord) s+=12;
              if(looksNext) s+=6;
              try{ const r=n.getBoundingClientRect(); s += Math.min(6, Math.max(0, Math.floor((r.top/Math.max(1, innerHeight))*6))); }catch(e){}
              if(s>score){best=n; score=s;}
            }
            return best||null;
            """,
            NEXT_SYNS, BACK_SYNS, AVOID_NEXT_MISCLICK
        )
        if el:
            return el
    except Exception:
        pass

    # Фаза 3: жёсткие селекторы Selenium (как раньше), вдруг DOM «успокоился»
    for sel in [
        'gac-flow-buttons .buttons button.button-next',
        '.buttons button.button-next',
        'button.button-next',
        '.buttons .mdc-button.button-next',
        '.buttons .button-next.highlighted',
    ]:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            if _is_interactable(driver, el):
                return el
        except Exception:
            continue

    return None


def _click_next(driver: WebDriver) -> Optional[WebElement]:
    # Принудим lazy-отрисовку футера и проскроллимся вниз
    _bounce_scroll(driver)
    _maybe_handle_confirm_its_you(driver, emit=None)
    btn = _find_next_button_any_language(driver)
    if not btn:
        logger.warning("Next/Continue button not found.")
        return None
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
        time.sleep(0.06)
        btn.click()
        return btn
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", btn)
            return btn
        except Exception:
            # жёсткий синтетический клик — даже если перекрыто
            try:
                driver.execute_script(
                    """
                    const el=arguments[0];
                    const r=el.getBoundingClientRect();
                    const x=Math.floor(r.left + Math.max(2, r.width/2));
                    const y=Math.floor(r.top  + Math.max(2, r.height/2));
                    const ev=(t)=>new MouseEvent(t,{view:window,bubbles:true,cancelable:true,clientX:x,clientY:y});
                    el.dispatchEvent(ev('mousedown')); el.dispatchEvent(ev('mouseup')); el.dispatchEvent(ev('click'));
                    """,
                    btn
                )
                return btn
            except Exception:
                return None


def _wait_url_change_or_button_stale(
    driver: WebDriver,
    old_url: str,
    btn: Optional[WebElement],
    timeout: float = 30.0,
    emit: Optional[Callable[[str], None]] = None,
) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        # Confirm 2FA probe (не блокирует, если диалога нет)
        _maybe_handle_confirm_its_you(driver, emit=emit)

        cur = driver.current_url or ""
        if cur != old_url:
            return True
        if btn is not None:
            try:
                if not btn.is_displayed():
                    return True
            except StaleElementReferenceException:
                return True
            except Exception:
                return True
        time.sleep(0.2)

    # финальная проверка + один проход Confirm (на случай гонки состояний)
    _maybe_handle_confirm_its_you(driver, emit=emit)
    return (driver.current_url or "") != old_url


# ======== Публичная функция ========

def run_step4(
    driver: WebDriver,
    *,
    locations: Optional[Iterable[str]] = None,
    languages: Optional[Iterable[str]] = None,
    eu_political_ads_no: bool = True,
    timeout_total: float = 90.0,
    emit: Optional[Callable[[str], None]] = None,
) -> Dict[str, object]:
    """
    Исполняет шаг 4 (Locations → Languages → EU political ads → Next).
    Параллельно везде «прощупывает» появление окна Confirm-it's-you и, если оно всплыло,
    проходит 2FA без разрыва сценария.
    """
    t0 = time.time()
    stage_ts = t0
    stage_log: List[Tuple[str, float]] = []

    def _mark_stage(label: str) -> None:
        nonlocal stage_ts
        now = time.time()
        elapsed_stage = (now - stage_ts) * 1000.0
        elapsed_total = (now - t0) * 1000.0
        logger.debug("Step4 timing: %s took %.1f ms (total %.1f ms)", label, elapsed_stage, elapsed_total)
        stage_log.append((label, elapsed_stage))
        stage_ts = now

    _dismiss_soft_dialogs(driver, budget_ms=800)
    _mark_stage("soft_dialogs_start")

    # На всякий — короткая проверка Confirm в начале
    _maybe_handle_confirm_its_you(driver, emit=emit)
    _mark_stage("confirm_probe_initial")

    # Locations
    if locations:
        _emit(emit, "Добавляю географию показа")
    else:
        _emit(emit, "Локации не переданы — пропускаю")
    loc_added = _add_locations(driver, list(locations or []), emit=emit)
    _mark_stage("locations")

    # После блока — ещё раз проверим Confirm
    _maybe_handle_confirm_its_you(driver, emit=emit)
    _mark_stage("confirm_after_locations")

    # Languages
    langs = list(languages or [])
    if languages is not None:
        _emit(emit, "Перехожу к выбору языков")
        logger.info("Languages: clearing previously selected before adding new ones…")
        lang_selected = _add_languages(driver, langs, clear_before=True, emit=emit)
    else:
        _emit(emit, "Языки не переданы — пропускаю")
        lang_selected = _add_languages(driver, [], clear_before=False, emit=emit)
    _mark_stage("languages")

    _maybe_handle_confirm_its_you(driver, emit=emit)
    _mark_stage("confirm_after_languages")

    # EU political ads (No)
    eu_status = "skip"
    if eu_political_ads_no:
        _emit(emit, "Подтверждаю: это не политическая реклама ЕС")
        eu_status = "no" if _choose_eu_political_ads_no(driver) else "skip"
    else:
        eu_status = "skip"
    _mark_stage("eu_political_ads")

    _maybe_handle_confirm_its_you(driver, emit=emit)

    # Next — именно кнопка Next/Continue, НЕ "More settings"
    _dismiss_soft_dialogs(driver, budget_ms=600)
    _mark_stage("pre_continue_cleanup")
    old_url = driver.current_url or ""
    _emit(emit, "Жму «Продолжить»")
    logger.info("step4: нажимаю Next/Continue…")
    btn = _click_next(driver)
    _mark_stage("continue_click")

    if btn is None:
        _dismiss_soft_dialogs(driver, budget_ms=800)
        _maybe_handle_confirm_its_you(driver, emit=emit)
        btn = _click_next(driver)
        if btn is None:
            _emit(emit, "Кнопку «Продолжить» не нашёл — останавливаюсь")
            raise RuntimeError("Step4: Next/Continue не найдена или не нажалась (возможно, попали в 'More settings').")

    if not _wait_url_change_or_button_stale(driver, old_url, btn, timeout=timeout_total, emit=emit):
        # повторная попытка
        _dismiss_soft_dialogs(driver, budget_ms=700)
        _emit(emit, "Повторяю «Продолжить»")
        _maybe_handle_confirm_its_you(driver, emit=emit)
        btn2 = _click_next(driver) or btn
        if not _wait_url_change_or_button_stale(driver, old_url, btn2, timeout=max(12.0, timeout_total/2), emit=emit):
            _emit(emit, "Переход не подтвердился — стоп")
            raise RuntimeError("Step4: переход после Next/Continue не произошёл по таймауту.")
    _mark_stage("continue_wait")

    new_url = driver.current_url or ""
    elapsed = int((time.time() - t0) * 1000)
    if stage_log:
        breakdown = ", ".join(f"{name}={dur:.0f}ms" for name, dur in stage_log)
        logger.info("step4 breakdown: %s", breakdown)
    logger.info("step4: OK (%d ms). URL: %s | locations=%s | languages=%s | eu_ads=%s",
                elapsed, new_url, loc_added, lang_selected, eu_status)
    _emit(emit, "Шаг готов — перехожу к следующему экрану")
    return {
        "locations_added": loc_added,
        "languages_selected": lang_selected,
        "eu_political_ads": eu_status,
        "new_url": new_url,
        "duration_ms": elapsed,
        "timing_breakdown": [{"stage": name, "duration_ms": dur} for name, dur in stage_log],
    }
