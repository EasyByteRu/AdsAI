# -*- coding: utf-8 -*-
"""
ads_ai/gads/step3.py

Шаг 3 Google Ads Wizard:
— На экране ничего не заполняем. Логика:
   1) Если открыт диалог "Save as a campaign draft?" — жмём Cancel и ждём закрытия.
   2) Жмём Next/Continue.
   3) Если после клика всплыл тот же диалог — снова жмём Cancel и повторяем Next.
   4) Ждём переход (смена URL или исчезновение/устаревание кнопки).
   5) НОВОЕ: если в любой момент всплывает окно безопасности "Confirm it's you" —
      вызываем обработчик 2FA, вводим код и возвращаемся к шагу.

Контракт:
    run_step3(driver: WebDriver, *, timeout_total: float = 45.0, emit: Optional[Callable[[str], None]] = None) -> dict
Возврат:
    {"clicked": bool, "new_url": str, "duration_ms": int}

emit — необязательный колбэк для UI-комментариев «по делу» (передаётся рантаймом).
"""

from __future__ import annotations

import logging
import time
from typing import Callable, Optional, Dict

from selenium.webdriver.common.by import By
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

logger = logging.getLogger("ads_ai.gads.step3")
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

# --- Многоязычные синонимы ---
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

_CANCEL_TEXTS = [
    "cancel", "отмена", "закрыть", "закрытие", "anuluj", "annuler", "abbrechen",
    "cancelar", "annulla", "avbryt", "avsluta",
    "anular", "취소", "キャンセル", "取消", "取消する",
]

_SAVE_DRAFT_HEADERS = [
    "save as a campaign draft", "save as draft", "save draft",
    "сохранить как черновик", "черновик кампании",
    "guardar como borrador", "guardar borrador",           # ES
    "salvar como rascunho", "salvar rascunho",             # PT
    "als entwurf speichern",                               # DE
    "enregistrer comme brouillon",                         # FR
    "salva come bozza",                                    # IT
]

_SAVE_TEXTS = ["save", "сохранить", "guardar", "salvar", "speichern", "enregistrer", "salva"]
_DISCARD_TEXTS = ["discard", "не сохранять", "удалить", "verwerfen", "ignorer", "descartar", "scarta"]

# Жёсткие селекторы для типичного DOM Google Ads (Next)
_NEXT_HARD_SELECTORS = [
    ".buttons .button-next",
    "button.button-next",
    "button.mdc-button--unelevated.button-next",
    "button.mdc-button--unelevated.highlighted",
    "button.btn-yes",
]


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
    """Закрываем мягкие диалоги (cookies/ok/got it). Не бросает исключений."""
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
                for b in dlg.find_elements(By.CSS_SELECTOR, 'button,[role="button"],a[role="button"]'):
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


# ---------- Диалог "Save as a campaign draft?" ----------

def _is_save_draft_dialog_visible(driver: WebDriver) -> bool:
    """
    Эвристика видимости искомого диалога:
      — есть видимый <material-dialog> или [role=dialog] с заголовком, включающим SAVE_DRAFT_HEADERS
        ИЛИ
      — есть видимый футер диалога с кнопками Save/Discard и слева Cancel.
    """
    try:
        return bool(driver.execute_script(
            """
            const HEAD = new Set(arguments[0].map(s=>String(s||'').toLowerCase()));
            const SAVE = new Set(arguments[1].map(s=>String(s||'').toLowerCase()));
            const DISC = new Set(arguments[2].map(s=>String(s||'').toLowerCase()));
            const CANCEL = new Set(arguments[3].map(s=>String(s||'').toLowerCase()));
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e), r=e.getBoundingClientRect();
              if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2||cs.pointerEvents==='none') return false;
              return r.width>8 && r.height>8 && r.bottom>0 && r.right>0 && r.top<innerHeight && r.left<innerWidth;};
            const dialogs=[...document.querySelectorAll('material-dialog,[role=dialog],.mdc-dialog--open')].filter(isVis);
            for(const d of dialogs){
              const header=(d.querySelector('header, .dialog-header, [role=heading]')?.innerText||'').trim().toLowerCase();
              if(header && [...HEAD].some(h=> header.includes(h))) return true;
              // сигнатура по футеру
              const footer=d.querySelector('footer,.after-footer,.button-group-right,.dual-side-dialog-footer');
              if(!footer || !isVis(footer)) continue;
              const btns=[...footer.querySelectorAll('button,material-button,[role=button],a[role=button]')].filter(isVis);
              if(btns.length<1) continue;
              let hasSave=false, hasDiscard=false, hasCancel=false;
              for(const b of btns){
                const t=((b.innerText||b.textContent||'')+' '+(b.getAttribute('aria-label')||'')).trim().toLowerCase();
                for(const s of SAVE)    if(s && t.includes(s))    hasSave=true;
                for(const s of DISC)    if(s && t.includes(s))    hasDiscard=true;
                for(const s of CANCEL)  if(s && t.includes(s))    hasCancel=true;
              }
              if(hasSave && hasDiscard && hasCancel) return true;
            }
            return false;
            """,
            _SAVE_DRAFT_HEADERS, _SAVE_TEXTS, _DISCARD_TEXTS, _CANCEL_TEXTS
        ))
    except Exception:
        return False


def _find_dialog_cancel_button(driver: WebDriver) -> Optional[WebElement]:
    """
    Возвращает кнопку Cancel внутри искомого диалога.
    Предпочтение — левой секции футера (.left-section) если она есть.
    """
    try:
        el = driver.execute_script(
            """
            const CANCEL = new Set(arguments[0].map(s=>String(s||'').toLowerCase()));
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e), r=e.getBoundingClientRect();
              if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2||cs.pointerEvents==='none') return false;
              return r.width>8 && r.height>8 && r.bottom>0 && r.right>0 && r.top<innerHeight && r.left<innerWidth;};
            const dialogs=[...document.querySelectorAll('material-dialog,[role=dialog],.mdc-dialog--open')].filter(isVis);
            for(const d of dialogs){
              const footer=d.querySelector('footer,.after-footer,.button-group-right,.dual-side-dialog-footer');
              if(!footer || !isVis(footer)) continue;
              const left=footer.querySelector('.left-section') || footer;
              const pool=[...left.querySelectorAll('button,material-button,[role=button],a[role=button]')].filter(isVis);
              // сначала пытаемся найти именно Cancel слева
              let best=null;
              for(const b of pool){
                const t=((b.innerText||b.textContent||'')+' '+(b.getAttribute('aria-label')||'')).trim().toLowerCase();
                for(const c of CANCEL) if(c && t.includes(c)) { best=b; break; }
                if(best) break;
              }
              if(best) return best;
              // иначе ищем во всём футере
              const all=[...footer.querySelectorAll('button,material-button,[role=button],a[role=button]')].filter(isVis);
              for(const b of all){
                const t=((b.innerText||b.textContent||'')+' '+(b.getAttribute('aria-label')||'')).trim().toLowerCase();
                for(const c of CANCEL) if(c && t.includes(c)) return b;
              }
            }
            return null;
            """,
            _CANCEL_TEXTS
        )
        if el:
            return el
    except Exception:
        pass
    return None


def _click_dialog_cancel_if_present(
    driver: WebDriver,
    appear_timeout: float = 0.5,
    disappear_timeout: float = 8.0,
    emit: Optional[Callable[[str], None]] = None,
) -> bool:
    """
    Если виден диалог "Save as a campaign draft?" — жмём Cancel и ждём исчезновения.
    Возвращает True, если диалог был и мы пытались его закрыть (независимо от успешности клика).
    """
    t_end = time.time() + max(0.25, appear_timeout)
    appeared = False
    while time.time() < t_end:
        if _is_save_draft_dialog_visible(driver):
            appeared = True
            break
        time.sleep(0.1)
    if not appeared:
        return False

    _emit(emit, "Появился диалог «Сохранить как черновик?» — жму «Отмена»")
    btn = _find_dialog_cancel_button(driver)
    if btn:
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            time.sleep(0.05)
            btn.click()
        except Exception:
            try:
                driver.execute_script("arguments[0].click();", btn)
            except Exception:
                logger.warning("Cancel в диалоге: клики не удались (оба способа).")
    else:
        logger.warning("Диалог виден, но кнопка Cancel не найдена.")

    # ждём исчезновения диалога
    t_dis = time.time() + max(0.5, disappear_timeout)
    while time.time() < t_dis:
        if not _is_save_draft_dialog_visible(driver):
            _emit(emit, "Диалог закрыт")
            return True
        time.sleep(0.15)

    logger.warning("Диалог не исчез по таймауту — продолжаем сценарий.")
    return True


# ---------- Next/Continue ----------

def _find_next_button_any_language(driver: WebDriver) -> Optional[WebElement]:
    """
    Ищем «Next/Continue» среди видимых кнопок, исключаем «Back».
    Приоритет: текст/aria-label по синонимам + «первичная» стилизация (highlighted/unelevated) + позиция ближе к низу.
    """
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
            const nodes=[...root.querySelectorAll('button,[role=button],a[role=button],material-button')].filter(isVis).filter(notDisabled);
            let best=null, score=-1;
            for(const n of nodes){
              const t=((n.innerText||n.textContent||'')+' '+(n.getAttribute('aria-label')||'')).trim().toLowerCase();
              let s=0;
              for(const w of NEXT) if(w && t.includes(w)) s+=12;
              for(const b of BACK) if(b && t.includes(b)) s-=20;

              const cls=(n.className||'').toLowerCase();
              if(/(button-next|highlighted|mdc-button--unelevated|primary|mat-primary)/.test(cls)) s+=5;

              const r=n.getBoundingClientRect();
              // бонус тем, кто ближе к низу страницы (типично для футер-кнопок)
              const verticalScore = Math.min(8, Math.max(0, Math.floor((r.top/Math.max(1, innerHeight))*8)));
              s += verticalScore;

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

    # Фоллбек: жёсткие селекторы
    for sel in _NEXT_HARD_SELECTORS:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            if _is_interactable(driver, el):
                return el
        except Exception:
            continue
    return None


def _click_next_button(driver: WebDriver) -> Optional[WebElement]:
    """
    Находит и кликает Next/Continue. Возвращает кликнутый элемент (для ожидания staleness) либо None.
    """
    try:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    except Exception:
        pass

    btn = _find_next_button_any_language(driver)
    if not btn:
        logger.warning("Кнопка Next/Continue не найдена.")
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
            logger.warning("Не удалось кликнуть Next/Continue.")
            return None


# ====== Confirm-watcher: «прощупать» и, если диалог уже есть — пройти 2FA ======

def _maybe_handle_confirm_its_you(driver: WebDriver, emit: Optional[Callable[[str], None]]) -> bool:
    """
    Лёгкий «пробник»: если окна подтверждения личности нет — возвращает False (ничего не делает).
    Если диалог уже открыт — вызывает полный проход 2FA (вплоть до ожидания кода),
    по завершении возвращает True.
    Исключения наружу не выбрасывает.
    """
    try:
        return bool(handle_confirm_its_you(
            driver,
            emit=emit,
            wait_code_cb=wait_code_from_env_or_file,
            timeout_total=180.0,     # если окно есть — дадим времени ввести код
            max_attempts=3,
        ))
    except Exception:
        return False


def _wait_url_change_or_button_stale(
    driver: WebDriver,
    old_url: str,
    btn: Optional[WebElement],
    *,
    timeout: float = 25.0,
    emit: Optional[Callable[[str], None]] = None,
) -> bool:
    """
    Успех, если:
      — сменился URL, ИЛИ
      — кнопка исчезла/устарела (stale).

    Во время ожидания регулярно проверяем появление окна "Confirm it's you":
    если обнаружили — проходим 2FA и продолжаем ждать переход.
    """
    end = time.time() + timeout
    while time.time() < end:
        # Confirm 2FA probe (не блокирует, если диалога нет)
        _maybe_handle_confirm_its_you(driver, emit)

        cur = driver.current_url or ""
        if cur != old_url:
            return True
        if btn is not None:
            try:
                visible = btn.is_displayed()
                if not visible:
                    return True
            except StaleElementReferenceException:
                return True
            except Exception:
                return True
        time.sleep(0.2)
    # финальная проверка + одна попытка пройти Confirm (на случай гонки)
    _maybe_handle_confirm_its_you(driver, emit)
    return (driver.current_url or "") != old_url


# ---------- Паблик ----------

def run_step3(
    driver: WebDriver,
    *,
    timeout_total: float = 45.0,
    emit: Optional[Callable[[str], None]] = None,
) -> Dict[str, object]:
    """
    Выполняет шаг 3:
      — закрывает мягкие диалоги (cookies/ok),
      — если открыт "Save as a campaign draft?" — жмём Cancel и ждём закрытия,
      — жмём Next/Continue,
      — если диалог всплыл после клика — жмём Cancel и повторяем Next,
      — ждём переход (смена URL или исчезновение/устаревание кнопки),
      — ПАРАЛЛЕЛЬНО: если где-либо «выстрелит» "Confirm it's you" — аккуратно проходим 2FA.

    Возвращает {"clicked": bool, "new_url": str, "duration_ms": int}
    """
    t0 = time.time()
    _emit(emit, "Ничего не заполняю на этом экране — просто продолжаю")
    _dismiss_soft_dialogs(driver, budget_ms=800)

    # На всякий — короткая проверка Confirm в начале
    _maybe_handle_confirm_its_you(driver, emit)

    # 1) Предварительно закрыть "Save draft?" если вдруг открыт
    if _click_dialog_cancel_if_present(driver, appear_timeout=0.6, disappear_timeout=8.0, emit=emit):
        logger.info("step3: обнаружен диалог 'Save draft?' — нажал Cancel перед Next.")
        # и сразу проверим Confirm после закрытия, вдруг Security «догоняет»
        _maybe_handle_confirm_its_you(driver, emit)

    # 2) Жмём Next
    old_url = driver.current_url or ""
    _emit(emit, "Жму «Продолжить»")
    logger.info("step3: нажимаю Next/Continue…")
    btn = _click_next_button(driver)
    if btn is None:
        _dismiss_soft_dialogs(driver, budget_ms=700)
        # Возможно, всплыл диалог и перекрыл кнопку — закрываем и пробуем снова
        if _click_dialog_cancel_if_present(driver, appear_timeout=0.6, disappear_timeout=8.0, emit=emit):
            _emit(emit, "Диалог закрыл — пробую ещё раз")
            _maybe_handle_confirm_its_you(driver, emit)
        btn = _click_next_button(driver)
        if btn is None:
            _emit(emit, "Кнопку «Продолжить» не нашёл — останавливаюсь")
            raise RuntimeError("Кнопка Next/Continue не найдена или не нажалась.")

    # 3) Если после клика всплыл диалог — жмём Cancel и повторяем Next
    if _click_dialog_cancel_if_present(driver, appear_timeout=1.0, disappear_timeout=8.0, emit=emit):
        logger.info("step3: диалог 'Save draft?' всплыл после Next — нажал Cancel и кликаю Next снова.")
        _emit(emit, "Появился диалог — жму «Отмена» и продолжаю")
        _maybe_handle_confirm_its_you(driver, emit)
        btn = _click_next_button(driver) or btn

    # 4) Ждём переход
    ok = _wait_url_change_or_button_stale(driver, old_url, btn, timeout=timeout_total, emit=emit)
    if not ok:
        # финальный фоллбек
        if _click_dialog_cancel_if_present(driver, appear_timeout=0.8, disappear_timeout=6.0, emit=emit):
            _emit(emit, "Ещё раз закрыл диалог — жму «Продолжить»")
            _maybe_handle_confirm_its_you(driver, emit)
            btn2 = _click_next_button(driver) or btn
            ok = _wait_url_change_or_button_stale(driver, old_url, btn2, timeout=max(10.0, timeout_total/2), emit=emit)
        else:
            _emit(emit, "Повторяю «Продолжить»")
            _maybe_handle_confirm_its_you(driver, emit)
            btn2 = _click_next_button(driver) or btn
            ok = _wait_url_change_or_button_stale(driver, old_url, btn2, timeout=max(10.0, timeout_total/2), emit=emit)

        if not ok:
            _emit(emit, "Переход не подтвердился — стоп")
            raise RuntimeError("Переход после Next/Continue не произошёл по таймауту.")

    new_url = driver.current_url or ""
    elapsed = int((time.time() - t0) * 1000)
    logger.info("step3: OK (%d ms). URL: %s", elapsed, new_url)
    _emit(emit, "Готово: перешёл на следующий экран")
    return {"clicked": True, "new_url": new_url, "duration_ms": elapsed}
