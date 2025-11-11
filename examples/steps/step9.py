# -*- coding: utf-8 -*-
"""
examples/steps/step9.py

Шаг 9 (Budget):
  - Выбрать вариант "Set custom budget".
  - Определить валюту аккаунта по символу/лейблу поля (€, $, £, ₽, AU$, CA$, ...).
  - Конвертировать переданный бюджет (в рублях/день) в валюту аккаунта.
  - Ввести значение и нажать "Next".

Контракт:
    run_step9(
        driver: WebDriver,
        *,
        budget_per_day: float,          # сумма в RUB/день (из CLI --budget)
        timeout_total: float = 180.0,
        emit: Optional[Callable[[str], None]] = None,  # необязательный колбэк комментариев
    ) -> dict

Политика: без LLM. Если не удалось определить валюту — RuntimeError.
Курсы: надёжная цепочка без ключей (cbr-xml-daily → exchangerate.host → frankfurter.app → floatrates).
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple, Callable

import requests
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webdriver import WebDriver, WebElement

from examples.steps.step4 import _maybe_handle_confirm_its_you  # type: ignore

logger = logging.getLogger("ads_ai.gads.step9")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")


# ====== Small UI emit helper ======

def _emit(emit: Optional[Callable[[str], None]], text: str) -> None:
    """Короткий безопасный комментарий в UI."""
    if callable(emit) and isinstance(text, str) and text.strip():
        try:
            emit(text.strip())
        except Exception:
            pass


def _maybe_handle_confirm_async(
    driver: WebDriver,
    *,
    emit: Optional[Callable[[str], None]] = None,
    timeout: float = 6.0,
    interval: float = 0.35,
) -> bool:
    """
    Вторично проверяет диалог Confirm it's you, пока не исчезнет.
    """
    handled = False
    if timeout <= 0:
        return bool(_maybe_handle_confirm_its_you(driver, emit))
    deadline = time.time() + timeout
    interval = max(0.1, min(interval, 1.0))
    while time.time() < deadline:
        if _maybe_handle_confirm_its_you(driver, emit):
            handled = True
        time.sleep(interval)
    return handled


# ====== UI helpers ======

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

def _robust_click(driver: WebDriver, el: WebElement) -> bool:
    try:
        if not _is_interactable(driver, el):
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    except Exception:
        pass
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

def _dispatch_input_change(driver: WebDriver, el: WebElement, value: str) -> None:
    _maybe_handle_confirm_its_you(driver, emit=None)
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

def _click_next(driver: WebDriver) -> bool:
    # Локали: Next / Далее / Continue / Продолжить
    try:
        _maybe_handle_confirm_its_you(driver, emit=None)
        btn = driver.execute_script(
            """
            const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e), r=e.getBoundingClientRect();
              if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2) return false;
              return r.width>10 && r.height>10 && r.right>0 && r.bottom>0;};
            const texts = new Set(['next','далее','continue','продолжить']);
            const nodes = [...document.querySelectorAll('.buttons .button-next, button,[role=button]')].filter(isVis);
            for (const b of nodes) {
              const t=((b.innerText||b.textContent||'')+' '+(b.getAttribute('aria-label')||'')).trim().toLowerCase();
              for (const s of texts){ if (t===s || t.includes(s)) return b; }
            }
            return null;
            """
        )
        if btn:
            return _robust_click(driver, btn)  # type: ignore
    except Exception:
        pass
    return False


# ====== Поиск панели и поля бюджета ======

def _open_custom_budget_panel(driver: WebDriver, timeout: float = 12.0) -> WebElement:
    """
    Находит панель 'Set custom budget' и раскрывает её. Возвращает корневой <material-expansionpanel>.
    """
    end = time.time() + timeout
    while time.time() < end:
        _maybe_handle_confirm_its_you(driver, emit=None)
        try:
            panel = driver.execute_script(
                """
                const match = (txt) => {
                  txt = String(txt||'').toLowerCase();
                  return txt.includes('set custom budget') || txt.includes('custom budget')
                         || txt.includes('собственный бюджет') || txt.includes('установить бюджет')
                         || txt.includes('задать бюджет');
                };
                const panels = [...document.querySelectorAll('material-expansionpanel')];
                for (const p of panels) {
                  const header = p.querySelector('.main-header .header');
                  const headerText = ((header?.innerText||'') + ' ' + (header?.getAttribute('aria-label')||''));
                  if (match(headerText)) {
                    const exp = (header?.getAttribute('aria-expanded')||'').toLowerCase() === 'true';
                    if (!exp) { try { header.click(); } catch(e) { try { p.querySelector('.expand-button')?.click(); } catch(e2) {} } }
                    return p;
                  }
                }
                return null;
                """
            )
            if panel:
                for _ in range(30):
                    _maybe_handle_confirm_its_you(driver, emit=None)
                    try:
                        header = panel.find_element(By.CSS_SELECTOR, ".main-header .header")  # type: ignore
                        if (header.get_attribute("aria-expanded") or "").lower() == "true":
                            return panel  # type: ignore
                    except Exception:
                        pass
                    time.sleep(0.2)
        except Exception:
            pass
        time.sleep(0.25)
    raise RuntimeError("Панель 'Set custom budget' не найдена или не раскрылась.")

def _locate_budget_input_and_symbol(driver: WebDriver, panel: WebElement) -> Tuple[WebElement, str]:
    """
    В раскрытой панели ищет material-input[type=money64], возвращает (inputEl, leadingText).
    """
    try:
        input_el = driver.execute_script(
            """
            const p=arguments[0];
            const money = p.querySelector('material-input[type="money64"]');
            if (!money) return [null, ''];
            const lead = (money.querySelector('.leading-text')?.innerText||'').trim();
            const inp  = money.querySelector('input.input.input-area') || money.querySelector('input.input-area');
            return [inp||null, lead];
            """,
            panel
        )
        if isinstance(input_el, list) and input_el[0]:
            return input_el[0], str(input_el[1] or "")
    except Exception:
        pass
    # Вторая попытка — общий поиск по странице
    try:
        money = driver.find_element(By.CSS_SELECTOR, 'material-input[type="money64"]')
        lead = ""
        try:
            lead_el = money.find_element(By.CSS_SELECTOR, ".leading-text")
            lead = (lead_el.text or "").strip()
        except Exception:
            lead = ""
        inp = money.find_element(By.CSS_SELECTOR, "input.input-area")
        return inp, lead
    except Exception:
        raise RuntimeError("Поле бюджета (money64) не найдено в панели.")


# ====== Распознавание валюты ======

@dataclass(frozen=True)
class CurrencyInfo:
    code: str
    symbol: str
    decimals: int = 2

_SYMBOL_TO_INFO: Dict[str, CurrencyInfo] = {
    "€": CurrencyInfo("EUR", "€", 2),
    "₽": CurrencyInfo("RUB", "₽", 2),
    "£": CurrencyInfo("GBP", "£", 2),
    "¥": CurrencyInfo("JPY", "¥", 0),  # если это CNY — уточним ниже
    "₩": CurrencyInfo("KRW", "₩", 0),
    "₫": CurrencyInfo("VND", "₫", 0),
    "₪": CurrencyInfo("ILS", "₪", 2),
    "₺": CurrencyInfo("TRY", "₺", 2),
    "R$": CurrencyInfo("BRL", "R$", 2),
    "A$": CurrencyInfo("AUD", "A$", 2),
    "AU$": CurrencyInfo("AUD", "AU$", 2),
    "C$": CurrencyInfo("CAD", "C$", 2),
    "CA$": CurrencyInfo("CAD", "CA$", 2),
    "HK$": CurrencyInfo("HKD", "HK$", 2),
    "S$": CurrencyInfo("SGD", "S$", 2),
    "NZ$": CurrencyInfo("NZD", "NZ$", 2),
    "$": CurrencyInfo("USD", "$", 2),  # дефолт для одиночного '$'
}

_CODE_TO_DECIMALS: Dict[str, int] = {
    "JPY": 0, "KRW": 0, "VND": 0,
    "KWD": 3, "BHD": 3, "JOD": 3, "OMR": 3, "TND": 3,
}

def _detect_currency(driver: WebDriver, panel: WebElement, leading_text: str) -> CurrencyInfo:
    lt = (leading_text or "").strip()

    # Прямой код в тексте
    code_match = re.search(r"\b([A-Z]{3})\b", lt)
    if code_match:
        code = code_match.group(1).upper()
        dec = _CODE_TO_DECIMALS.get(code, 2)
        return CurrencyInfo(code=code, symbol=lt, decimals=dec)

    # Маппинг по символам/префиксам
    if lt in _SYMBOL_TO_INFO:
        info = _SYMBOL_TO_INFO[lt]
        if info.code == "JPY":  # попытка отделить CNY
            try:
                body_text = (driver.find_element(By.TAG_NAME, "body").text or "").upper()
                if "CNY" in body_text or "RMB" in body_text or "CN¥" in body_text:
                    return CurrencyInfo("CNY", "¥", 2)
            except Exception:
                pass
        return info

    for pref, code in [("US$", "USD"), ("AU$", "AUD"), ("CA$", "CAD"), ("HK$", "HKD"), ("SGD", "SGD")]:
        if lt.upper().startswith(pref):
            dec = _CODE_TO_DECIMALS.get(code, 2)
            return CurrencyInfo(code=code, symbol=lt, decimals=dec)

    # Поиск упоминания кода на странице
    try:
        body_text = (driver.find_element(By.TAG_NAME, "body").text or "").upper()
        m = re.search(r"\b(USD|EUR|RUB|GBP|JPY|CNY|CAD|AUD|NZD|CHF|SEK|NOK|DKK|PLN|CZK|RON|HUF|TRY|BRL|MXN|ZAR|ILS|AED|SAR|HKD|SGD|INR|THB|IDR|MYR|PHP|TWD|KRW)\b", body_text)
        if m:
            code = m.group(1)
            dec = _CODE_TO_DECIMALS.get(code, 2)
            sym = lt if lt else (_SYMBOL_TO_INFO.get("$", CurrencyInfo("USD", "$")).symbol if code == "USD" else "")
            return CurrencyInfo(code=code, symbol=sym, decimals=dec)
    except Exception:
        pass

    if lt == "$":
        logger.warning("Currency: '$' распознано как USD по умолчанию (эвристика).")
        return _SYMBOL_TO_INFO["$"]

    raise RuntimeError(f"Не удалось распознать валюту по leading-text='{lt}'.")


# ====== Конвертация RUB → Target (мульти-источники) ======

@dataclass
class FxResult:
    amount_out: float
    rate: float           # курс: 1 RUB = ? target
    source: str

def _fx_from_cbr_latest(amount_rub: float, target_code: str, timeout: float = 6.0) -> Optional[FxResult]:
    """
    https://www.cbr-xml-daily.ru/latest.js
    {'base':'RUB','rates':{'USD':0.0109, 'EUR':0.0101, ...}}
    """
    try:
        r = requests.get("https://www.cbr-xml-daily.ru/latest.js", timeout=timeout)
        if not r.ok: return None
        j = r.json()
        rates = j.get("rates") or {}
        val = rates.get(target_code.upper())
        if isinstance(val, (int, float)) and val > 0:
            amt = float(amount_rub) * float(val)
            return FxResult(amount_out=amt, rate=float(val), source="cbr.latest.js")
    except Exception:
        return None
    return None

def _fx_from_cbr_daily(amount_rub: float, target_code: str, timeout: float = 6.0) -> Optional[FxResult]:
    """
    https://www.cbr-xml-daily.ru/daily_json.js
    'Valute': {'EUR': {'Value': 100.12}}  -> 1 EUR = 100.12 RUB  =>  1 RUB = 1/100.12 EUR
    """
    try:
        r = requests.get("https://www.cbr-xml-daily.ru/daily_json.js", timeout=timeout)
        if not r.ok: return None
        j = r.json()
        valute = j.get("Valute") or {}
        rec = valute.get(target_code.upper())
        if isinstance(rec, dict) and isinstance(rec.get("Value"), (int, float)):
            rub_per_unit = float(rec["Value"])
            if rub_per_unit > 0:
                rate = 1.0 / rub_per_unit
                amt = float(amount_rub) * rate
                return FxResult(amount_out=amt, rate=rate, source="cbr.daily_json")
    except Exception:
        return None
    return None

def _fx_from_exchangerate_host(amount_rub: float, target_code: str, timeout: float = 6.0) -> Optional[FxResult]:
    try:
        url = f"https://api.exchangerate.host/convert?from=RUB&to={target_code.upper()}&amount={float(amount_rub)}"
        r = requests.get(url, timeout=timeout)
        if not r.ok: return None
        j = r.json()
        res = j.get("result")
        info = j.get("info", {})
        rate = info.get("rate") or (float(res) / float(amount_rub) if res not in (None, 0) else None)
        if res not in (None, "") and rate:
            return FxResult(amount_out=float(res), rate=float(rate), source="exchangerate.host")
    except Exception:
        return None
    return None

def _fx_from_frankfurter(amount_rub: float, target_code: str, timeout: float = 6.0) -> Optional[FxResult]:
    try:
        url = f"https://api.frankfurter.app/latest?amount={float(amount_rub)}&from=RUB&to={target_code.upper()}"
        r = requests.get(url, timeout=timeout)
        if not r.ok: return None
        j = r.json()
        rates = j.get("rates") or {}
        if target_code.upper() in rates:
            converted = float(rates[target_code.upper()])
            if converted > 0:
                rate = converted / float(amount_rub) if amount_rub else 0.0
                return FxResult(amount_out=converted, rate=rate, source="frankfurter.app")
    except Exception:
        return None
    return None

def _fx_from_floatrates(amount_rub: float, target_code: str, timeout: float = 6.0) -> Optional[FxResult]:
    """
    https://www.floatrates.com/daily/rub.json
    ключи: 'usd', 'eur', ... поля: {'rate': 0.0109, ...}  -> 1 RUB = rate TARGET
    """
    try:
        r = requests.get("https://www.floatrates.com/daily/rub.json", timeout=timeout)
        if not r.ok: return None
        j = r.json()
        rec = j.get(target_code.lower())
        if isinstance(rec, dict) and isinstance(rec.get("rate"), (int, float)):
            rate = float(rec["rate"])
            if rate > 0:
                amt = float(amount_rub) * rate
                return FxResult(amount_out=amt, rate=rate, source="floatrates")
    except Exception:
        return None
    return None

def _fx_convert_rub(amount_rub: float, target_code: str, timeout_per_source: float = 6.0) -> FxResult:
    """
    Пытаемся по цепочке источников; возвращаем первый удачный.
    """
    target_code = target_code.upper().strip()
    sources = (
        lambda: _fx_from_cbr_latest(amount_rub, target_code, timeout_per_source),
        lambda: _fx_from_cbr_daily(amount_rub, target_code, timeout_per_source),
        lambda: _fx_from_exchangerate_host(amount_rub, target_code, timeout_per_source),
        lambda: _fx_from_frankfurter(amount_rub, target_code, timeout_per_source),
        lambda: _fx_from_floatrates(amount_rub, target_code, timeout_per_source),
    )
    errors: list[str] = []
    for fn in sources:
        try:
            res = fn()
            if isinstance(res, FxResult) and res.rate > 0 and res.amount_out > 0:
                return res
        except Exception as e:
            errors.append(str(e))
            continue
    raise RuntimeError(f"Не удалось получить курс конверсии RUB→{target_code} (все источники недоступны).")


# ====== Форматирование под валюту ======

def _format_amount_for_currency(value: float, code: str, decimals: int) -> str:
    d = max(0, int(decimals))
    q = round(float(value), d)
    if d == 0:
        return str(int(round(q)))
    # без тысячных разделителей, точка как десятичный разделитель
    return f"{q:.{d}f}"


# ====== Основной шаг ======

def run_step9(
    driver: WebDriver,
    *,
    budget_per_day: float,
    timeout_total: float = 180.0,
    emit: Optional[Callable[[str], None]] = None,
) -> Dict[str, object]:
    """
    :param driver: Selenium WebDriver (AdsPower).
    :param budget_per_day: бюджет/день в RUB, из CLI --budget.
    :param timeout_total: общий мягкий лимит на шаг.
    """
    t0 = time.time()
    if budget_per_day is None or float(budget_per_day) <= 0:
        _emit(emit, "Бюджет должен быть > 0 — стоп")
        raise RuntimeError("Некорректный бюджет (RUB/день) — должен быть > 0.")
    _maybe_handle_confirm_async(driver, emit=emit, timeout=6.0)

    # 1) Открыть панель Custom Budget
    _emit(emit, "Открываю раздел «Set custom budget»")
    panel = _open_custom_budget_panel(driver, timeout=min(12.0, max(6.0, timeout_total * 0.4)))

    # 2) Найти поле и leading-text (символ валюты/лейбл)
    input_el, leading = _locate_budget_input_and_symbol(driver, panel)
    logger.info("Budget: найдено поле ввода, leading-text='%s'", leading or "")
    _emit(emit, "Определяю валюту аккаунта")

    # 3) Распознать валюту и точность
    cur = _detect_currency(driver, panel, leading)
    decimals = _CODE_TO_DECIMALS.get(cur.code, cur.decimals)
    logger.info("Budget: распознана валюта аккаунта %s (symbol='%s', decimals=%d)", cur.code, cur.symbol, decimals)

    # 4) Конвертировать RUB → target
    if cur.code.upper() == "RUB":
        fx = FxResult(amount_out=float(budget_per_day), rate=1.0, source="identity")
        _emit(emit, f"Валюта аккаунта RUB — конвертация не требуется")
    else:
        _emit(emit, f"Конвертирую {float(budget_per_day):.2f} RUB/день → {cur.code}")
        per_source_timeout = 5.0
        try:
            fx = _fx_convert_rub(float(budget_per_day), cur.code, timeout_per_source=per_source_timeout)
        except Exception as e:
            slack = max(0.0, timeout_total - (time.time() - t0))
            if slack > 20.0:
                logger.warning("FX: повторная попытка с увеличенным таймаутом из-за ошибки: %s", e)
                fx = _fx_convert_rub(float(budget_per_day), cur.code, timeout_per_source=8.0)
            else:
                _emit(emit, "Не удалось получить курс — стоп")
                raise
    logger.info("Budget: FX RUB→%s rate=%.8f via %s, amount_out=%.6f", cur.code, fx.rate, fx.source, fx.amount_out)

    # 5) Ввести значение
    value_str = _format_amount_for_currency(fx.amount_out, cur.code, decimals)
    _emit(emit, f"Ввожу бюджет: {value_str} {cur.code}/день")
    _maybe_handle_confirm_async(driver, emit=emit, timeout=3.0)
    _dispatch_input_change(driver, input_el, value_str)
    time.sleep(0.2)

    # 6) Базовая проверка и Next
    try:
        after = input_el.get_attribute("value") or ""
        if not after:
            _emit(emit, "Поле бюджета пустое после ввода — стоп")
            raise RuntimeError("Поле бюджета пустое после ввода.")
    except Exception:
        pass

    _emit(emit, "Жму «Next»")
    _maybe_handle_confirm_async(driver, emit=emit, timeout=4.0)
    if not _click_next(driver):
        _emit(emit, "Кнопка «Next» не нажалась — стоп")
        raise RuntimeError("Кнопка 'Next' не нажалась на шаге бюджета.")

    elapsed = int((time.time() - t0) * 1000)
    logger.info("step9: OK (%d ms). RUB/day=%.2f -> %s %s/day (src=%s)",
                elapsed, float(budget_per_day), cur.code, value_str, fx.source)
    _emit(emit, "Бюджет установлен — перехожу дальше")

    # В converted_per_day постараемся отдать float по введённой строке
    numeric_match = re.match(r"^\d+(?:[.,]\d+)?$", value_str)
    converted = float(value_str.replace(",", ".")) if numeric_match else fx.amount_out

    return {
        "currency_code": cur.code,
        "currency_symbol": cur.symbol,
        "decimals": decimals,
        "rub_per_day": float(budget_per_day),
        "converted_per_day": converted,
        "fx_rate_rub_to_target": fx.rate,
        "fx_source": fx.source,
        "duration_ms": elapsed,
    }
