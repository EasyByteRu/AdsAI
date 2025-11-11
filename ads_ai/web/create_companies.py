# ads_ai/web/create_companies.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import base64
import copy
import hashlib
import hmac
import html
import importlib
import sys
import inspect
import json
import os
import pkgutil
import re
import sqlite3
import threading
import time
import queue
import urllib.parse
import uuid
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Callable, Dict, List, Optional, Tuple

from flask import (
    Flask, Response, jsonify, make_response, request, session,
    stream_with_context, send_file
)
from werkzeug.utils import secure_filename

try:
    from examples.steps.code_for_confrim import (  # type: ignore
        normalize_totp_secret as _cf_normalize_totp_secret,
        generate_totp_code as _cf_generate_totp_code,
    )
except Exception:  # pragma: no cover - fallback if module not available
    _cf_normalize_totp_secret = None  # type: ignore
    _cf_generate_totp_code = None  # type: ignore

# Проектный Settings (как в app.py). Если модуль недоступен — используем заглушку.
try:
    from ads_ai.config.settings import Settings  # noqa: F401
except Exception:
    class Settings:  # упрощённая заглушка для автономности
        pass


# =============================================================================
#                             ВНУТРЕННЕЕ СОСТОЯНИЕ
# =============================================================================

@dataclass
class StepSpec:
    number: int
    module_name: str         # "examples.steps.step1"
    runner_name: str         # "run_step1" или "run_step"
    runner: Callable[..., Any]
    label: str               # читаемый ярлык для UI (эвристика)


@dataclass(frozen=True)
class CampaignVariant:
    variant_id: str
    label: str
    choose_type: str
    steps_package: str


_CAMPAIGN_VARIANTS: tuple[CampaignVariant, ...] = (
    CampaignVariant(
        variant_id="PMAX",
        label="Performance Max (PMax)",
        choose_type="UBERVERSAL",
        steps_package="examples.steps",
    ),
    CampaignVariant(
        variant_id="DEMAND_GEN",
        label="Demand Gen",
        choose_type="OWNED_AND_OPERATED",
        steps_package="examples.steps_demand_gen",
    ),
)

_DEFAULT_CAMPAIGN_VARIANT = _CAMPAIGN_VARIANTS[0]
_CAMPAIGN_VARIANTS_BY_ID = {v.variant_id.upper(): v for v in _CAMPAIGN_VARIANTS}
_CAMPAIGN_VARIANTS_BY_TYPE = {v.choose_type.upper(): v for v in _CAMPAIGN_VARIANTS}


def _list_campaign_variants() -> List[CampaignVariant]:
    return list(_CAMPAIGN_VARIANTS)


def _campaign_variant_by_id(variant_id: Optional[str]) -> Optional[CampaignVariant]:
    if not variant_id:
        return None
    return _CAMPAIGN_VARIANTS_BY_ID.get(str(variant_id).strip().upper())


def _campaign_variant_by_type(choose_type: Optional[str]) -> Optional[CampaignVariant]:
    if not choose_type:
        return None
    return _CAMPAIGN_VARIANTS_BY_TYPE.get(str(choose_type).strip().upper())


def _resolve_campaign_variant(
    *,
    variant_id: Optional[str],
    choose_type: Optional[str],
) -> CampaignVariant:
    cand = _campaign_variant_by_id(variant_id)
    if cand:
        return cand
    cand = _campaign_variant_by_type(choose_type)
    if cand:
        return cand
    return _DEFAULT_CAMPAIGN_VARIANT


def _campaign_variants_for_ui() -> List[Dict[str, str]]:
    return [
        {
            "id": v.variant_id,
            "label": v.label,
            "choose_type": v.choose_type,
        }
        for v in _CAMPAIGN_VARIANTS
    ]


class _LocalState:
    """Один активный драйвер на профиль (сохраняем headless-флаг)."""
    def __init__(self):
        self.driver: Any = None
        self.profile_id: Optional[str] = None
        self.headless: bool = True
        self.user_email: Optional[str] = None
        self.lock = threading.Lock()

_local = _LocalState()


@dataclass
class _PendingRun:
    run_id: str
    user_email: str
    profile_id: str
    headless: bool
    cli_inputs: Dict[str, Any]
    context: Dict[str, Any]
    steps_meta: List[Dict[str, Any]]
    record: Dict[str, Any]
    steps_results: List[Dict[str, Any]]
    created_at: float
    record_id: Optional[int] = None
    campaign_variant_id: str = ""
    steps_package: str = _DEFAULT_CAMPAIGN_VARIANT.steps_package


_PENDING_RUNS: Dict[str, _PendingRun] = {}
_PENDING_RUNS_LOCK = threading.Lock()


def _pending_run_prune(max_age: float = 3600.0) -> None:
    now = time.time()
    with _PENDING_RUNS_LOCK:
        stale_keys = [k for k, v in _PENDING_RUNS.items() if (now - v.created_at) > max_age]
        for key in stale_keys:
            _PENDING_RUNS.pop(key, None)


def _pending_run_store(item: _PendingRun) -> None:
    """Saves pending company run info until publish."""
    _pending_run_prune()
    with _PENDING_RUNS_LOCK:
        _PENDING_RUNS[item.run_id] = item


def _pending_run_get(run_id: str, user_email: str) -> Optional[_PendingRun]:
    with _PENDING_RUNS_LOCK:
        item = _PENDING_RUNS.get(run_id)
        if item and item.user_email == user_email:
            return item
        return None


def _pending_run_pop(run_id: str) -> Optional[_PendingRun]:
    with _PENDING_RUNS_LOCK:
        return _PENDING_RUNS.pop(run_id, None)


def _get_state_from_app() -> Any:
    """Пробуем аккуратно достать ads_ai.web.app._state (если открыт /console)."""
    cand_modules = [
        sys.modules.get("ads_ai.web.app"),
        sys.modules.get("__main__"),
        sys.modules.get("app"),
    ]
    for mod in cand_modules:
        if not mod:
            continue
        try:
            state = getattr(mod, "_state", None)
        except Exception:
            state = None
        if state is not None:
            return state
    try:
        mod = importlib.import_module("ads_ai.web.app")
        return getattr(mod, "_state", None)
    except Exception:
        return None


def _require_user_email() -> str:
    """
    Возвращает email авторизованного пользователя или поднимает PermissionError.
    Дополнительная защита, несмотря на глобальный before_request в auth.py.
    """
    email = session.get("user_email")
    if not email:
        raise PermissionError("unauthorized")
    return str(email)


# =============================================================================
#                    2FA: ГЛОБАЛЬНЫЙ БРОКЕР ОЖИДАНИЯ КОДА
# =============================================================================

@dataclass
class _CodeSlot:
    evt: threading.Event
    code: Optional[str] = None
    ts: float = 0.0

# run_id -> _CodeSlot
_CODE_BROKER: Dict[str, _CodeSlot] = {}
_CODE_BROKER_LOCK = threading.Lock()

# thread-local контекст обработчика шагов (нужен, чтобы "изнутри" шага отправить SSE-событие)
_CODE_CTX = threading.local()


def _broker_get_or_create(run_id: str) -> _CodeSlot:
    with _CODE_BROKER_LOCK:
        slot = _CODE_BROKER.get(run_id)
        if not slot:
            slot = _CodeSlot(evt=threading.Event(), code=None, ts=time.time())
            _CODE_BROKER[run_id] = slot
        return slot


def _broker_set_code(run_id: str, code: str) -> bool:
    """Вызывается из POST /api/confirm/submit: кладём код и будим ждущего."""
    code = (code or "").strip()
    if not code:
        return False
    with _CODE_BROKER_LOCK:
        slot = _CODE_BROKER.get(run_id)
        if not slot:
            slot = _CodeSlot(evt=threading.Event(), code=None, ts=time.time())
            _CODE_BROKER[run_id] = slot
        slot.code = code
        slot.ts = time.time()
        try:
            slot.evt.set()
        except Exception:
            pass
        return True


def _broker_pop_code(run_id: str, timeout: float) -> Optional[str]:
    """Блокирующе ждём установки кода, затем забираем и очищаем слот."""
    slot = _broker_get_or_create(run_id)
    try:
        slot.evt.clear()
    except Exception:
        pass
    ok = slot.evt.wait(timeout=max(0.0, float(timeout)))
    if not ok:
        return None
    with _CODE_BROKER_LOCK:
        code = slot.code or ""
        slot.code = None
        try:
            slot.evt.clear()
        except Exception:
            pass
        return code or None


# =============================================================================
#                         ПОИСК И ПОДГОТОВКА ШАГОВ
# =============================================================================

def _human_label(mod: str, num: int) -> str:
    """Эвристика для статуса по имени модуля шага (без «шуток»)."""
    m = mod.lower()

    def has(*keys: str) -> bool:
        return any(k in m for k in keys)

    if has("image", "creative", "asset", "media"):
        return "Готовлю креативы"
    if has("headline", "description", "ad", "ads", "text"):
        return "Готовлю объявления"
    if has("campaign", "pmax", "create", "setup", "builder"):
        return "Создаю кампанию"
    if has("target", "geo", "lang", "keyword", "audience", "theme", "topic"):
        return "Настраиваю таргетинг"
    if has("budget", "bid", "bidding"):
        return "Настраиваю бюджет и ставки"
    if has("verify", "check", "confirm", "review"):
        return "Проверяю и подтверждаю"
    if has("publish", "launch", "start", "run"):
        return "Публикую кампанию"
    return f"Шаг #{num}"


import logging

logger = logging.getLogger("ads_ai.web.create_companies")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

_NO_ACCOUNT_EMAIL = "no_account"


def _is_no_account_value(value: Any) -> bool:
    """Возвращает True, если строка значит «нет аккаунта» (прочерк/пусто)."""
    if value is None:
        return True
    text = str(value).strip()
    if not text:
        return True
    if text.lower() == _NO_ACCOUNT_EMAIL:
        return True
    dash_chars = set("-–—")
    if all(ch in dash_chars for ch in text):
        return True
    return False


def _is_supported_google_email(value: Any) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    return "@gmail.com" in text


def _discover_steps(package_name: Optional[str] = None) -> List[StepSpec]:
    """
    Ищет модули <package_name>.step<N> и их run-функции.
    Возвращает отсортированный список по <N>.
    """
    steps_pkg_name = package_name or _DEFAULT_CAMPAIGN_VARIANT.steps_package
    try:
        steps_pkg = importlib.import_module(steps_pkg_name)
    except Exception as e:
        raise RuntimeError(f"Пакет {steps_pkg_name} недоступен: {e}")

    found: List[StepSpec] = []
    import_errors: List[Tuple[str, str]] = []
    for m in pkgutil.iter_modules(steps_pkg.__path__):
        name = m.name  # например "step1"
        mnum = re.match(r"^step(\d+)$", name)
        if not mnum:
            continue
        n = int(mnum.group(1))
        full_mod = f"{steps_pkg_name}.{name}"
        try:
            mod = importlib.import_module(full_mod)
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            import_errors.append((full_mod, err))
            logger.warning("Пропускаю модуль шага %s: %s", full_mod, err)
            try:
                print(f"[discover_steps] Пропускаю модуль шага {full_mod}: {err}", flush=True)
            except Exception:
                pass
            continue

        fn_name = f"run_step{n}"
        fn = getattr(mod, fn_name, None)
        if not callable(fn):
            fn_name = "run_step"
            fn = getattr(mod, fn_name, None)
        if not callable(fn):
            logger.warning("В модуле %s нет функции %s / run_step — пропуск", full_mod, f"run_step{n}")
            try:
                print(f"[discover_steps] В модуле {full_mod} нет функции {fn_name} — пропуск", flush=True)
            except Exception:
                pass
            continue

        found.append(StepSpec(number=n,
                              module_name=full_mod,
                              runner_name=fn_name,
                              runner=fn,
                              label=_human_label(full_mod, n)))

    found.sort(key=lambda s: s.number)
    if not found:
        details = ""
        if import_errors:
            joined = "; ".join(f"{mod} ({err})" for mod, err in import_errors)
            details = f" (ошибки импорта: {joined})"
        try:
            print(f"[discover_steps] Не найдено ни одного шага в {steps_pkg_name}{details}", flush=True)
        except Exception:
            pass
        raise RuntimeError(f"Не найдено ни одного шага в {steps_pkg_name} (ожидались step1.py, step2.py, ...){details}")
    if import_errors:
        msg = "; ".join(f"{mod}: {err}" for mod, err in import_errors)
        logger.info("Некоторые модули шагов не загружены: %s", msg)
        try:
            print(f"[discover_steps] Некоторые модули не загружены: {msg}", flush=True)
        except Exception:
            pass
    try:
        summary = ", ".join(f"{spec.number}:{spec.module_name}" for spec in found)
        print(f"[discover_steps] Пакет {steps_pkg_name}: найдено шагов {len(found)} ({summary})", flush=True)
    except Exception:
        pass
    return found


def _normalize_multi(values: Optional[str]) -> List[str]:
    if not values:
        return []
    parts = [p.strip() for p in re.split(r"[;,]", values) if p and p.strip()]
    seen: set[str] = set()
    out: List[str] = []
    for v in parts:
        key = v.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(v)
    return out


def _call_step_with_injected_kwargs(
    step: StepSpec,
    driver: Any,
    cli_inputs: Dict[str, Any],
    context: Dict[str, Any],
    emit_cb: Optional[Callable[[str], None]] = None,
) -> Any:
    """
    Вызывает функцию шага с подстановкой аргументов по именам.
    См. примеры step4–step7 для контрактов.
    """
    sig = inspect.signature(step.runner)
    params = sig.parameters

    kwargs: Dict[str, Any] = {}
    if "driver" in params:
        kwargs["driver"] = driver

    mapping_cli_to_args = {
        "budget": "budget_per_day",
        "budget_min": "budget_min",
        "budget_max": "budget_max",
        "url": "site_url",
        "usp": "usp",
        "type": "choose_type",
        "variant": "campaign_variant",
        "campaign_variant_label": "campaign_variant_label",
        "locations": "locations",
        "languages": "languages",
        "n_ads": "n_ads",
        "creative_mode": "mode",
        "creative_seed_assets": "seed_assets",
        "creative_provided_assets": "provided_assets",
    }
    for cli_key, arg_name in mapping_cli_to_args.items():
        if arg_name in params and cli_key in cli_inputs and cli_inputs[cli_key] is not None:
            kwargs[arg_name] = cli_inputs[cli_key]

    # fallback на единственный элемент
    if "location" in params and cli_inputs.get("locations"):
        kwargs["location"] = cli_inputs["locations"][0]
    if "language" in params and cli_inputs.get("languages"):
        kwargs["language"] = cli_inputs["languages"][0]

    # контекст от предыдущих шагов
    for k, v in context.items():
        if k in params and k not in kwargs:
            kwargs[k] = v

    # n_ads по умолчанию
    if "n_ads" in params and "n_ads" not in kwargs:
        kwargs["n_ads"] = 3

    # колбэки комментариев
    if emit_cb is not None:
        for name in ("emit", "report", "progress_cb", "ui_emit", "comment_cb"):
            if name in params:
                kwargs[name] = emit_cb

    return step.runner(**kwargs)


def _update_context_from_result(step_no: int, result: Any, ctx: Dict[str, Any]) -> None:
    """Нормализует результат шага в общий контекст."""
    if isinstance(result, dict):
        ctx.update({k: v for k, v in result.items() if k not in ("comment", "comments")})
        if step_no == 3 and isinstance(result.get("campaign_goal_reason"), str):
            ctx["campaign_goal_reason"] = result["campaign_goal_reason"]
        return
    if step_no == 1 and isinstance(result, (tuple, list)) and len(result) == 3:
        ctx["business_name"] = result[0]
        ctx["website_url"] = result[1]
        ctx["budget_clean"] = result[2]
        return
    if step_no == 2 and isinstance(result, str) and result:
        ctx["campaign_type"] = result
        return
    ctx[f"step{step_no}_result"] = result


# =============================================================================
#                     ДРАЙВЕР / ВЬЮПОРТ / ПРЕВЬЮ
# =============================================================================

def _apply_headless_env(headless: bool) -> None:
    os.environ["ADS_AI_HEADLESS"] = "1" if headless else "0"
    os.environ["HEADLESS"] = "1" if headless else "0"
    os.environ["SELENIUM_HEADLESS"] = "1" if headless else "0"


def _try_minimize_cdp(driver: Any) -> None:
    """Минимизируем окно через CDP (опционально, только если разрешено переменной окружения)."""
    try:
        win = driver.execute_cdp_cmd("Browser.getWindowForTarget", {})
        wid = (win or {}).get("windowId")
        if wid:
            driver.execute_cdp_cmd("Browser.setWindowBounds",
                                   {"windowId": wid, "bounds": {"windowState": "minimized"}})
    except Exception:
        pass


def _ensure_big_viewport(driver: Any) -> None:
    """Гарантируем крупный вьюпорт для скриншота (устраняем «сжатый» вид)."""
    try:
        dims = driver.execute_script(
            "return {w: window.innerWidth||0, h: window.innerHeight||0, dpr: window.devicePixelRatio||1}"
        ) or {}
        w = int(dims.get("w") or 0)
        h = int(dims.get("h") or 0)
        if w >= 1280 and h >= 720:
            return  # норм
    except Exception:
        pass

    try:
        target_w = int(os.getenv("ADS_AI_PREVIEW_W", "1600"))
        target_h = int(os.getenv("ADS_AI_PREVIEW_H", "1000"))
        target_dpr = float(os.getenv("ADS_AI_PREVIEW_DPR", "1"))
        driver.execute_cdp_cmd("Emulation.setDeviceMetricsOverride", {
            "mobile": False,
            "width": target_w,
            "height": target_h,
            "deviceScaleFactor": target_dpr,
            "screenWidth": target_w,
            "screenHeight": target_h,
        })
        try:
            driver.execute_cdp_cmd("Emulation.setVisibleSize", {"width": target_w, "height": target_h})
        except Exception:
            pass
        try:
            driver.execute_cdp_cmd("Emulation.setPageScaleFactor", {"pageScaleFactor": 1})
        except Exception:
            pass
    except Exception:
        pass


def _start_driver(profile_id: str, headless: bool) -> Any:
    """
    Пытается стартовать AdsPower профиль (headless = True/False).
    Если модуль недоступен — поднимает Selenium Chrome (headless поддерживается).
    """
    _apply_headless_env(headless)

    # 1) AdsPower (приоритетный путь)
    adspower = None
    try:
        adspower = importlib.import_module("ads_ai.browser.adspower")
    except Exception:
        adspower = None

    if adspower:
        start_fn = getattr(adspower, "start_adspower", None) or getattr(adspower, "start", None)
        if start_fn:
            api_base = os.getenv("ADSP_API_BASE") or "http://local.adspower.net:50325"
            token = os.getenv("ADSP_API_TOKEN") or ""
            drv = None
            try:
                drv = start_fn(
                    profile=profile_id,
                    headless=headless,
                    api_base=api_base,
                    token=token,
                    window_size="1920,1080",
                )
            except Exception:
                drv = None
            if drv is not None:
                try:
                    setattr(drv, "_adspower_profile_id", profile_id)
                    drv.set_page_load_timeout(25)
                    drv.set_script_timeout(15)
                    _ensure_big_viewport(drv)
                    if not headless:
                        _maximize_and_focus(drv)
                except Exception:
                    pass
                return drv

    # 2) Fallback: обычный Chrome (с поддержкой headless)
    try:
        from selenium import webdriver  # type: ignore
        from selenium.webdriver.chrome.options import Options  # type: ignore
    except Exception as e:
        raise RuntimeError(f"Невозможно запустить браузер: {e}")

    opts = Options()
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1920,1080")
    if headless:
        opts.add_argument("--headless=new")
        opts.add_argument("--hide-scrollbars")
        opts.add_argument("--mute-audio")

    drv = webdriver.Chrome(options=opts)
    try:
        setattr(drv, "_adspower_profile_id", profile_id)
    except Exception:
        pass
    _ensure_big_viewport(drv)
    if not headless:
        _maximize_and_focus(drv)
    return drv


def _maximize_and_focus(driver: Any) -> None:
    try:
        driver.maximize_window()
    except Exception:
        pass
    try:
        driver.switch_to.window(driver.current_window_handle)
        driver.execute_script("try{window.focus()}catch(e){}")
    except Exception:
        pass


def _close_driver_safely(driver: Any) -> None:
    try:
        if hasattr(driver, "quit"):
            driver.quit()
    except Exception:
        pass


def _shutdown_driver(reason: str = "") -> None:
    """
    Полностью закрывает текущий драйвер и профиль AdsPower, очищает локальное состояние.
    Вызывать после любого завершения/остановки запуска.
    """
    drv = None
    pid = None
    with _local.lock:
        try:
            drv = _local.driver
            pid = _local.profile_id
        except Exception:
            drv = None
            pid = None
        finally:
            _local.driver = None
            _local.profile_id = None
            _local.user_email = None
            # headless оставим как есть — его установит следующий запуск

    # Сначала закрываем Selenium-окно
    if drv is not None:
        try:
            _close_driver_safely(drv)
        except Exception:
            pass

    # Затем отдельно просим AdsPower остановить профиль (если это именно он)
    if pid:
        try:
            _stop_adspower_profile(pid)
        except Exception:
            pass


# ====== Дополнительно: явное закрытие профиля AdsPower (HTTP/SDK) ======

def _adsp_env() -> tuple[str, str]:
    base = (os.getenv("ADSP_API_BASE") or "http://local.adspower.net:50325").rstrip("/")
    token = os.getenv("ADSP_API_TOKEN") or ""
    if not re.match(r"^https?://", base, re.I):
        base = "http://" + base
    return base, token


def _http_get_json(url: str, headers: Optional[Dict[str, str]] = None, timeout: float = 4.0) -> tuple[int, Dict[str, Any]]:
    try:
        import requests  # type: ignore
    except Exception:
        requests = None  # type: ignore
    if requests:
        try:
            r = requests.get(url, headers=headers or {}, timeout=timeout)  # type: ignore
            j = {}
            try:
                j = r.json() if r.content else {}
            except Exception:
                j = {}
            return int(r.status_code), j or {}
        except Exception:
            return 0, {}
    # stdlib fallback
    import urllib.request
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
            try:
                j = json.loads(data.decode("utf-8")) if data else {}
            except Exception:
                j = {}
            return int(resp.getcode() or 0), j or {}
    except Exception:
        return 0, {}


def _stop_adspower_profile(profile_id: Optional[str]) -> None:
    """
    Аккуратно останавливает профиль AdsPower (если доступен SDK — через него,
    иначе — через HTTP API). Все ошибки безопасно игнорируются.
    """
    if not profile_id:
        return
    # 1) Попытка через SDK-обёртку проекта
    try:
        adspower = importlib.import_module("ads_ai.browser.adspower")
    except Exception:
        adspower = None
    if adspower:
        for name in ("stop_adspower", "stop", "close"):
            fn = getattr(adspower, name, None)
            if callable(fn):
                try:
                    api_base, token = _adsp_env()
                    fn(profile=str(profile_id), api_base=api_base, token=token)
                    return
                except Exception:
                    pass
    # 2) Фолбэк: HTTP API (пробуем несколько эндпоинтов — у разных версий названия отличаются)
    base, token = _adsp_env()
    headers = {"Authorization": token} if token else {}
    for path in ("api/v1/browser/stop", "api/v1/browser/close", "api/v1/browser/kill", "api/v1/browser/forceStop"):
        url = f"{base}/{path}?user_id={profile_id}"
        try:
            _http_get_json(url, headers=headers, timeout=4.0)
        except Exception:
            pass


def _switch_to_ads_window(driver: Any, prefer_host: str = "ads.google.com") -> bool:
    """Ищем среди открытых хэндлов вкладку с ads.google.com и переключаемся на неё."""
    try:
        for h in driver.window_handles:
            try:
                driver.switch_to.window(h)
                url = (driver.current_url or "").lower()
                if prefer_host in url:
                    return True
            except Exception:
                continue
    except Exception:
        pass
    return False


def _ensure_back_to_ads(driver: Any, max_wait: float = 40.0, prefer_host: str = "ads.google.com") -> bool:
    """
    Гарантированно возвращаем фокус на вкладку Google Ads:
      • если уже на ads — сразу True;
      • иначе пытаемся переключиться на вкладку с ads;
      • ждём до max_wait редиректа из accounts -> ads.
    """
    end = time.time() + max(1.0, float(max_wait))
    while time.time() < end:
        try:
            url = (driver.current_url or "").lower()
            if prefer_host in url:
                return True
        except Exception:
            pass
        # есть ли уже вкладка ads?
        if _switch_to_ads_window(driver, prefer_host=prefer_host):
            return True
        _ensure_big_viewport(driver)
        time.sleep(0.3)
    return False


# =============================================================================
#           ЛОКАЛЬНАЯ ПОДДЕРЖКА ДРАЙВЕРОВ (поднята ВЫШЕ роутов)
# =============================================================================

def _maybe_get_driver(
    requested_profile_id: Optional[str],
    headless: bool,
    user_email: Optional[str],
) -> Any:
    """
    Возвращает текущий драйвер, если:
      • requested_profile_id пуст и есть локальный — вернёт локальный;
      • requested_profile_id совпадает с локальным и headless совпадает — вернёт его;
      • иначе — None.
    Также проверяем внешний driver из app._state (если профиль совпадает).
    """
    user_email = str(user_email or "").strip() or None
    if requested_profile_id:
        if (
            _local.driver is not None
            and _local.profile_id == requested_profile_id
            and _local.headless == headless
            and (_local.user_email == user_email or _local.user_email is None)
        ):
            return _local.driver
        ext = _get_state_from_app()
        try:
            if ext and getattr(ext, "driver", None) is not None:
                d = ext.driver
                pid = getattr(d, "_adspower_profile_id", None)
                owner = getattr(d, "_ads_ai_owner", None)
                if pid and str(pid) == requested_profile_id and (not owner or owner == user_email):
                    return d
        except Exception:
            pass
        return None
    if _local.driver is not None and (_local.user_email == user_email or _local.user_email is None):
        return _local.driver
    return None


def _get_or_create_driver(profile_id: str, headless: bool, user_email: str) -> Any:
    """
    Возвращает driver для данного профиля/режима:
      • если локальный driver уже для этого профиля и того же headless — вернёт его;
      • если локальный driver отличается — закрывает и поднимает новый;
      • если драйвера нет — поднимает новый.
    """
    d = _local.driver
    if (
        d is not None
        and _local.profile_id == profile_id
        and _local.headless == headless
        and (_local.user_email == user_email or _local.user_email is None)
    ):
        _ensure_big_viewport(d)
        return d

    with _local.lock:
        if _local.driver is not None and (
            _local.profile_id != profile_id
            or _local.headless != headless
            or (_local.user_email not in (None, user_email))
        ):
            # аккуратно гасим старый экземпляр + профиль AdsPower
            old_pid = _local.profile_id
            try:
                _close_driver_safely(_local.driver)
            finally:
                _local.driver = None
                _local.profile_id = None
                _local.user_email = None
            try:
                _stop_adspower_profile(old_pid)
            except Exception:
                pass
            _local.headless = headless

        if _local.driver is None:
            drv = _start_driver(profile_id, headless=headless)
            _local.driver = drv
            _local.profile_id = profile_id
            _local.headless = headless
            _local.user_email = user_email
            try:
                setattr(drv, "_ads_ai_owner", user_email)
            except Exception:
                pass
            try:
                drv.get("https://ads.google.com/aw/overview")
                _ensure_big_viewport(drv)
            except Exception:
                pass
        else:
            try:
                setattr(_local.driver, "_ads_ai_owner", user_email)
            except Exception:
                pass

    return _local.driver


# =============================================================================
#                         ПРИМИТИВЫ ПРЕВЬЮ (SSE-кадр)
# =============================================================================

class _PreviewHub:
    """Хаб, который по запросу отдаёт кадр + мета для управления."""
    def __init__(self):
        self._lock = threading.Lock()
        self._last_minimize_ts = 0.0
        self._last_enable_ts = 0.0  # периодический Page.enable для надёжности

    def _warm_cdp(self, driver: Any) -> None:
        now = time.time()
        if (now - self._last_enable_ts) < 5.0:
            return
        self._last_enable_ts = now
        try:
            driver.execute_cdp_cmd("Page.enable", {})
        except Exception:
            pass
        try:
            driver.execute_cdp_cmd("Runtime.runIfWaitingForDebugger", {})
        except Exception:
            pass

    def capture_frame(self, driver: Any, headless: bool) -> Optional[Dict[str, Any]]:
        with self._lock:
            now = time.time()

            if headless and os.getenv("ADS_AI_PREVIEW_MINIMIZE", "0") in ("1", "true", "yes"):
                if (now - self._last_minimize_ts) > 1.0:
                    try:
                        _try_minimize_cdp(driver)
                    finally:
                        self._last_minimize_ts = now

            _ensure_big_viewport(driver)
            self._warm_cdp(driver)

            # 1) CDP JPEG
            data_b64: Optional[str] = None
            fmt = "jpeg"
            try:
                params = {"format": "jpeg", "quality": 82, "fromSurface": True}
                try:
                    params["captureBeyondViewport"] = True  # type: ignore
                except Exception:
                    pass
                res = driver.execute_cdp_cmd("Page.captureScreenshot", params)
                data_b64 = (res or {}).get("data")
            except Exception:
                data_b64 = None

            # 2) Фолбэк — обычный PNG (надёжно на удалённых драйверах)
            if not data_b64:
                try:
                    png = driver.get_screenshot_as_png()
                    data_b64 = base64.b64encode(png).decode("ascii")
                    fmt = "png"
                except Exception:
                    return None

            # 3) Вьюпорт
            vw = vh = dpr = 0
            try:
                dims = driver.execute_script(
                    "return {vw: window.innerWidth||0, vh: window.innerHeight||0, dpr: window.devicePixelRatio||1}"
                )
                vw = int(dims.get("vw") or 0)
                vh = int(dims.get("vh") or 0)
                dpr = float(dims.get("dpr") or 1.0)
            except Exception:
                pass

            return {"data": data_b64, "vw": vw, "vh": vh, "dpr": dpr, "fmt": fmt}


_preview = _PreviewHub()


# =============================================================================
#                          SQLite: сохранение компаний
# =============================================================================

def _db_path() -> str:
    path = os.getenv("ADS_AI_DB", "").strip()
    if not path:
        base = os.path.abspath(os.path.join(os.getcwd(), "ads_ai_data"))
        os.makedirs(base, exist_ok=True)
        path = os.path.join(base, "companies.sqlite3")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path


def _db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path(), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

_MANUAL_UPLOAD_MAX_BYTES = 25 * 1024 * 1024

def _manual_upload_root() -> str:
    base = os.path.abspath(os.path.join(os.getcwd(), "ads_ai_data", "manual_uploads"))
    os.makedirs(base, exist_ok=True)
    return base

def _manual_upload_dir(user_email: str) -> str:
    safe = re.sub(r"[^a-z0-9]+", "_", (user_email or "user").lower()).strip("_") or "user"
    path = os.path.join(_manual_upload_root(), safe)
    os.makedirs(path, exist_ok=True)
    return path


def _db_ensure_column(name: str, ddl_type: str) -> None:
    """Безопасный ALTER TABLE ADD COLUMN IF NOT EXISTS."""
    conn = _db_conn()
    try:
        cur = conn.execute("PRAGMA table_info(companies)")
        cols = {str(r[1]).lower() for r in cur.fetchall()}
        if name.lower() not in cols:
            conn.execute(f"ALTER TABLE companies ADD COLUMN {name} {ddl_type}")
            conn.commit()
    except Exception:
        pass
    finally:
        conn.close()


def _db_init() -> None:
    conn = _db_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS companies(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          created_at TEXT NOT NULL,
          status TEXT,
          profile_id TEXT,
          user_email TEXT,
          headless INTEGER,
          site_url TEXT,
          budget_per_day TEXT,
          usp TEXT,
          campaign_type TEXT,
          locations TEXT,
          languages TEXT,
          n_ads INTEGER,
          business_name TEXT,
          asset_group_name TEXT,
          headlines_json TEXT,
          long_headlines_json TEXT,
          descriptions_json TEXT,
          images_json TEXT,
          image_files_json TEXT,
          extra_json TEXT,
          google_tags TEXT,
          google_tag TEXT
        )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_companies_created_at ON companies(created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_companies_profile ON companies(profile_id)")
        conn.commit()
    finally:
        conn.close()
    # дополнительные колонки — мягко
    _db_ensure_column("user_email", "TEXT")
    _db_ensure_column("google_tags", "TEXT")
    _db_ensure_column("google_tag", "TEXT")  # новый столбец


def _db_insert_company(record: Dict[str, Any]) -> int:
    conn = _db_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO companies(
            created_at, status, profile_id, user_email, headless, site_url, budget_per_day, usp, campaign_type,
            locations, languages, n_ads, business_name, asset_group_name,
            headlines_json, long_headlines_json, descriptions_json,
            images_json, image_files_json, extra_json, google_tags, google_tag
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            record.get("created_at"),
            record.get("status"),
            record.get("profile_id"),
            record.get("user_email"),
            1 if record.get("headless") else 0,
            record.get("site_url"),
            record.get("budget_per_day"),
            record.get("usp"),
            record.get("campaign_type"),
            json.dumps(record.get("locations") or [], ensure_ascii=False),
            json.dumps(record.get("languages") or [], ensure_ascii=False),
            int(record.get("n_ads") or 0),
            record.get("business_name"),
            record.get("asset_group_name"),
            json.dumps(record.get("headlines") or [], ensure_ascii=False),
            json.dumps(record.get("long_headlines") or [], ensure_ascii=False),
            json.dumps(record.get("descriptions") or [], ensure_ascii=False),
            json.dumps(record.get("images") or [], ensure_ascii=False),
            json.dumps(record.get("image_files") or [], ensure_ascii=False),
            json.dumps(record.get("extra") or {}, ensure_ascii=False),
            json.dumps(record.get("google_tags") or [], ensure_ascii=False),
            record.get("google_tag") or None,
        ))
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def _db_list(user_email: str, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
    conn = _db_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM companies WHERE user_email = ? ORDER BY id DESC LIMIT ? OFFSET ?",
            (user_email, limit, offset),
        )
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _db_get_one(rec_id: int, user_email: str) -> Optional[Dict[str, Any]]:
    conn = _db_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM companies WHERE id = ? AND user_email = ?",
            (rec_id, user_email),
        )
        r = cur.fetchone()
        return dict(r) if r else None
    finally:
        conn.close()


def _db_update_publish(
    rec_id: int,
    *,
    user_email: str,
    status: str,
    google_tag: Optional[str],
    publish_meta: Optional[Dict[str, Any]] = None,
) -> None:
    # читаем чтобы аккуратно смержить extra_json
    row = _db_get_one(rec_id, user_email=user_email)
    extra = {}
    if row and row.get("extra_json"):
        try:
            extra = json.loads(row["extra_json"])
        except Exception:
            extra = {}
    extra = extra or {}
    extra["publish"] = {
        "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": status,
        **(publish_meta or {})
    }
    conn = _db_conn()
    try:
        conn.execute(
            "UPDATE companies SET status = ?, google_tag = ?, extra_json = ? WHERE id = ? AND user_email = ?",
            (status, google_tag, json.dumps(extra, ensure_ascii=False), rec_id, user_email),
        )
        conn.commit()
    finally:
        conn.close()


# =============================================================================
#                       Сбор данных из текущего экрана (DOM)
# =============================================================================

def _harvest_texts_from_ui(driver: Any) -> Dict[str, Any]:
    """
    Считывает из текущей страницы мастера:
      - headlines / long_headlines / descriptions
      - asset_group_name, business_name
      - keywords / search_themes / topics (чипсы и мульти-инпуты)
    """
    js = r"""
    const uniq = (arr)=>{ const s=new Set(); const out=[]; for(const v of (arr||[])){ const k=(v||'').trim(); if(k && !s.has(k.toLowerCase())){ s.add(k.toLowerCase()); out.push(k); } } return out; };

    const pickVals = (sel) => {
      try {
        const nodes = [...document.querySelectorAll(sel)];
        const out = [];
        for (const n of nodes) {
          const v = (n.value||'').trim();
          if (v) out.push(v);
        }
        return out;
      } catch(e){ return []; }
    };

    const pickChips = (sels) => {
      const out = [];
      const selList = Array.isArray(sels) ? sels : [sels];
      for(const sel of selList){
        try{
          for(const n of document.querySelectorAll(sel)){
            const t = (n.textContent||'').trim();
            if (t) out.push(t);
          }
        }catch(_){}
      }
      return out;
    };

    const readVal = (sel) => {
      try { const el = document.querySelector(sel); return (el && el.value||'').trim() || ''; } catch(e){ return ''; }
    };

    const result = {};
    // Тексты объявлений
    result.headlines = pickVals('multi-text-input.headlines input.input, [data-qa*="headline"] input.input, input[name*="headline"]');
    result.long_headlines = pickVals('multi-text-input.long-headlines input.input, [data-qa*="long"] input.input');
    result.descriptions = pickVals('multi-text-input.descriptions input.input, [data-qa*="description"] input.input, textarea[name*="description"]');

    // Имена
    result.asset_group_name =
        readVal('material-expansionpanel .name-input input.input') ||
        readVal('material-expansionpanel[section_id] .name-input input.input') || '';
    result.business_name =
        readVal('brand-profile-editor .business-name input.input') ||
        readVal('brand-profile-editor text-input.business-name input.input') || '';

    // Ключевые слова / темы
    const chipSelectors = [
      '.chip', '.mat-chip', '.material-chip', '.aw-chip', 'gads-chip', 'material-chip',
      '[class*="chip"][role="button"]', '[class*="chip"][aria-label]', '[data-qa*="chip"]'
    ];
    const keywordScopes = ['keyword','keywords','search','term','query','запрос','ключ','поиск'];
    const themeScopes   = ['theme','themes','search-theme','topics','тема','темы'];

    const scoped = (scopes) => {
      const out = [];
      try{
        const all = document.querySelectorAll('*');
        for(const el of all){
          const label = ((el.getAttribute('aria-label')||'') + ' ' + (el.className||'')).toLowerCase();
          if (scopes.some(s => label.includes(s))){
            for(const c of el.querySelectorAll(chipSelectors.join(','))){
              const t = (c.textContent||'').trim();
              if (t) out.push(t);
            }
          }
        }
      }catch(_){}
      return out;
    };

    const kwFromInputs = pickVals('[data-qa*="keyword"] input, .keywords input, input[name*="keyword"], multi-text-input.keywords input.input');
    const thFromInputs = pickVals('[data-qa*="theme"] input, .search-themes input, input[name*="theme"], multi-text-input.search-themes input.input');
    const kwScoped = scoped(keywordScopes);
    const thScoped = scoped(themeScopes);

    result.keywords = uniq([...kwFromInputs, ...kwScoped]);
    result.search_themes = uniq([...thFromInputs, ...thScoped]);
    result.topics = uniq([...pickChips(['.topics .chip', '[data-qa*="topic"] .chip'])]);

    return result;
    """
    try:
        data = driver.execute_script(js) or {}

        def _list(v: Any) -> List[str]:
            try:
                return [str(x).strip() for x in (v or []) if str(x).strip()]
            except Exception:
                return []

        return {
            "headlines": _list(data.get("headlines")),
            "long_headlines": _list(data.get("long_headlines")),
            "descriptions": _list(data.get("descriptions")),
            "asset_group_name": (data.get("asset_group_name") or "").strip(),
            "business_name": (data.get("business_name") or "").strip(),
            "keywords": _list(data.get("keywords")),
            "search_themes": _list(data.get("search_themes")),
            "topics": _list(data.get("topics")),
        }
    except Exception:
        return {
            "headlines": [], "long_headlines": [], "descriptions": [],
            "asset_group_name": "", "business_name": "",
            "keywords": [], "search_themes": [], "topics": []
        }


# --------- Публикация: эвристики UI (проверить успех + извлечь Google Tag) ---------

def _detect_published_ui(driver: Any) -> bool:
    js = r"""
    const txt = (n)=> (n && (n.innerText||n.textContent)||'').trim();
    const els = [
      ...document.querySelectorAll('.blg-title, .ads-ufo-subhead, [class*="title"], [class*="subhead"]')
    ];
    const s = els.map(txt).join('\n').toLowerCase();
    if (s.includes('your ads will go live after a review')) return true;
    // запасной вариант — контент страницы «Setup selection»
    if (document.querySelector('.gte-setup, gte-setup-selection')) return true;
    return false;
    """
    try:
        return bool(driver.execute_script(js))
    except Exception:
        return False


def _extract_google_tag_snippet(driver: Any) -> str:
    js = r"""
    function grab(){
      const prefer = [
        '.ogt-snippet[aria-label*="Google tag"]',
        '.ogt-snippet',
        '[aria-label*="Google tag snippet"]',
        '.snippet-card__body .ogt-snippet',
        'pre', 'code', 'textarea'
      ];
      let best = '';
      const textOf = (el)=> (el && (el.innerText||el.textContent)||'').trim();
      for(const sel of prefer){
        for(const el of document.querySelectorAll(sel)){
          const t = textOf(el);
          if (t && t.toLowerCase().includes('googletagmanager.com/gtag/js') && t.includes("gtag(")){
            if (t.length > best.length) best = t;
          }
        }
      }
      if (best) return best;
      // эвристика: собрать сниппет из id=AW-XXXX, если найден
      const html = document.documentElement.innerHTML || '';
      const m = html.match(/gtag\/js\?id=([A-Z]+-\d+)/i);
      if (m){
        const id = m[1];
        return `<!-- Google tag (gtag.js) -->
<script async src="https://www.googletagmanager.com/gtag/js?id=${id}"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){dataLayer.push(arguments);}
  gtag('js', new Date());
  gtag('config', '${id}');
</script>`;
      }
      return '';
    }
    return grab();
    """
    try:
        val = driver.execute_script(js) or ""
        return (val or "").strip()
    except Exception:
        return ""


# =============================================================================
#                 НОРМАЛИЗАЦИЯ ПУТЕЙ ДЛЯ СОХРАНЕНИЯ В БД
# =============================================================================

def _canon_path(p: Any) -> str:
    """
    Приводит путь к каноническому абсолютному виду.
    """
    s = str(p or "").strip()
    if not s:
        return ""
    if s.lower().startswith("file://"):
        s = s[7:]
    try:
        s = os.path.expanduser(s)
        s = os.path.abspath(s)
        s = os.path.normpath(s)
    except Exception:
        pass
    return s


def _normalize_images_and_logo(
    images_meta: List[Dict[str, Any]],
    image_files: List[str],
    context: Dict[str, Any],
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    abs_list = [_canon_path(x) for x in (image_files or []) if x]
    by_basename: Dict[str, str] = {}
    for ap in abs_list:
        base = os.path.basename(ap)
        if base and base not in by_basename:
            by_basename[base] = ap

    fixed_images: List[Dict[str, Any]] = []
    for i, meta in enumerate(images_meta or []):
        m = dict(meta or {})
        f = str(m.get("file") or "").strip()
        if f and not os.path.isabs(f):
            abs_by_base = by_basename.get(os.path.basename(f))
            if abs_by_base:
                m["file"] = abs_by_base
            elif i < len(abs_list):
                m["file"] = abs_list[i]
        elif not f and i < len(abs_list):
            m["file"] = abs_list[i]
        m["file"] = _canon_path(m.get("file"))
        fixed_images.append(m)

    ctx2 = dict(context or {})
    lf = str(ctx2.get("logo_file") or "").strip()
    if lf:
        if not os.path.isabs(lf):
            by_bn = by_basename.get(os.path.basename(lf))
            ctx2["logo_file"] = _canon_path(by_bn or lf)
        else:
            ctx2["logo_file"] = _canon_path(lf)

    return fixed_images, ctx2


def _collect_company_record(
    *,
    profile_id: str,
    headless: bool,
    cli_inputs: Dict[str, Any],
    context: Dict[str, Any],
    steps_meta: List[Dict[str, Any]],
    driver: Any,
    user_email: str,
) -> Dict[str, Any]:
    # Достаём тексты/имена + ключевые слова/темы с экрана
    harvested = _harvest_texts_from_ui(driver)

    images_raw = list(context.get("image_meta") or [])
    image_files_raw = list(context.get("image_files") or [])
    images, ctx_norm = _normalize_images_and_logo(images_raw, image_files_raw, context)

    harvested_bn = (harvested.get("business_name") or "").strip()
    context_bn = (context.get("business_name") or "").strip()
    campaign_name = (context.get("campaign_name") or "").strip()
    if campaign_name:
        business_name_final = campaign_name
    elif context_bn and len(context_bn) > len(harvested_bn):
        business_name_final = context_bn
    else:
        business_name_final = harvested_bn or context_bn

    def _collect_tags(*sources: Any) -> List[str]:
        seen: set[str] = set()
        collected: List[str] = []

        def _visit(value: Any) -> None:
            if value is None:
                return
            if isinstance(value, dict):
                for v in value.values():
                    _visit(v)
                return
            if isinstance(value, (list, tuple, set)):
                for v in value:
                    _visit(v)
                return
            text = str(value).strip()
            if not text:
                return
            key = text.lower()
            if key in seen:
                return
            seen.add(key)
            collected.append(text)

        for src in sources:
            _visit(src)
        return collected

    google_tags = _collect_tags(
        harvested.get("keywords"),
        harvested.get("search_themes"),
        harvested.get("topics"),
        context.get("keywords"),
        context.get("search_themes"),
        context.get("topics"),
    )
    creative_mode_val = context.get("creative_mode") or cli_inputs.get("creative_mode")
    creative_seed_val = context.get("creative_seed_assets") or cli_inputs.get("creative_seed_assets")
    creative_manual_val = context.get("creative_provided_assets") or cli_inputs.get("creative_provided_assets")

    record: Dict[str, Any] = {
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "ok",
        "profile_id": profile_id,
        "user_email": user_email,
        "headless": headless,
        "site_url": cli_inputs.get("url"),
        "budget_per_day": cli_inputs.get("budget"),
        "usp": cli_inputs.get("usp"),
        "campaign_type": context.get("campaign_type") or cli_inputs.get("type"),
        "campaign_variant": cli_inputs.get("variant") or context.get("campaign_variant"),
        "campaign_variant_label": cli_inputs.get("campaign_variant_label") or context.get("campaign_variant_label"),
        "locations": list(cli_inputs.get("locations") or context.get("locations") or []),
        "languages": list(cli_inputs.get("languages") or context.get("languages") or []),
        "n_ads": int(cli_inputs.get("n_ads") or 0),
        "business_name": business_name_final,
        "asset_group_name": harvested.get("asset_group_name") or context.get("asset_group_name") or "",
        "headlines": list(harvested.get("headlines") or []),
        "long_headlines": list(harvested.get("long_headlines") or []),
        "descriptions": list(harvested.get("descriptions") or []),
        "images": images,
        "image_files": [_canon_path(x) for x in image_files_raw],
        "google_tags": google_tags,
        "extra": {
            "context": ctx_norm,
            "steps": steps_meta,
            # ключевые слова/темы
            "keywords": list(harvested.get("keywords") or []),
            "search_themes": list(harvested.get("search_themes") or []),
            "topics": list(harvested.get("topics") or []),
            "harvested": {
                "asset_group_name": harvested.get("asset_group_name") or "",
                "business_name": harvested.get("business_name") or "",
                "headlines": harvested.get("headlines") or [],
                "long_headlines": harvested.get("long_headlines") or [],
                "descriptions": harvested.get("descriptions") or [],
            },
            "campaign_variant": {
                "id": cli_inputs.get("variant") or context.get("campaign_variant"),
                "label": cli_inputs.get("campaign_variant_label") or context.get("campaign_variant_label"),
                "choose_type": cli_inputs.get("type") or context.get("campaign_type"),
            },
            "budget_range": {
                "min": cli_inputs.get("budget_min"),
                "max": cli_inputs.get("budget_max"),
            },
        },
        "google_tag": None,  # заполнится на этапе публикации
    }
    creative_payload = {
        "mode": creative_mode_val,
        "seed_assets": creative_seed_val,
        "provided_assets": creative_manual_val,
    }
    if any(creative_payload.values()):
        record["extra"]["creative"] = creative_payload
    return record


# =============================================================================
#                                   HTML (UI)
# =============================================================================

_CAMPAIGN_TYPE_OPTIONS_HTML = "".join([
    (
        f'<option value="{html.escape(v.variant_id)}" '
        f'data-choose="{html.escape(v.choose_type)}"'
        f'{" selected" if v is _DEFAULT_CAMPAIGN_VARIANT else ""}>'
        f'{html.escape(v.label)}</option>'
    )
    for v in _CAMPAIGN_VARIANTS
])
_CAMPAIGN_VARIANTS_JSON = json.dumps(_campaign_variants_for_ui(), ensure_ascii=False)
_DEFAULT_CAMPAIGN_LABEL_HTML = html.escape(_DEFAULT_CAMPAIGN_VARIANT.label)

PAGE_HTML = """<!doctype html>
<html lang="ru" data-theme="light">
<head>
  <meta charset="utf-8" />
  <title>HyperAI — Создание компаний</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root{--bg:#eef2f7;--bg2:#f6f8fb;--text:#111827;--muted:#6b7280;--glass:rgba(255,255,255,.66);--glass-2:rgba(255,255,255,.5);--border:rgba(17,24,39,.08);--ring:rgba(17,24,39,.06);--neon1:#38bdf8;--neon2:#a78bfa;--neon3:#34d399;--ok:#16a34a;--err:#ef4444;--warn:#f59e0b;--radius:24px;--radius-sm:16px;--shadow:0 10px 30px rgba(15,23,42,.12);--shadow-big:0 30px 80px rgba(15,23,42,.18);--content-max:1480px}
    html[data-theme="dark"]{color-scheme:dark;--bg:#0b1220;--bg2:#0d1423;--text:#e5e7eb;--muted:#94a3b8;--glass:rgba(17,23,41,.55);--glass-2:rgba(17,23,41,.45);--border:rgba(255,255,255,.09);--ring:rgba(56,189,248,.15);--shadow:0 10px 30px rgba(0,0,0,.35)}
    *{box-sizing:border-box}html,body{height:100%;margin:0;color:var(--text);font:14px/1.45 Inter,system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;-webkit-font-smoothing:antialiased}
    body{background:radial-gradient(1200px 800px at 20% -10%, #ffffff 0%, var(--bg) 48%, var(--bg2) 100%),linear-gradient(180deg,#ffffff,var(--bg2))}
    html[data-theme=dark] body{background:radial-gradient(1200px 800px at 20% -10%, #0e1527 0%, var(--bg) 40%, var(--bg2) 100%),linear-gradient(180deg,#0f172a,var(--bg2))}
    .shell{display:grid;grid-template-columns:300px minmax(0,1fr);gap:18px;min-height:100vh;padding:18px;max-width:var(--content-max);margin:0 auto}
    .panel{background:var(--glass);border:1px solid var(--border);border-radius:var(--radius);backdrop-filter:blur(12px) saturate(160%);box-shadow:var(--shadow);overflow:hidden}
    .menu{padding:18px;display:flex;flex-direction:column;gap:12px}.menu .head{height:56px;display:flex;align-items:center;gap:10px;padding:0 6px;font-weight:700}
    .mitem{display:flex;align-items:center;gap:10px;padding:10px 12px;border-radius:14px;background:var(--glass-2);border:1px solid var(--border);cursor:pointer;text-decoration:none;color:inherit}
    .stage{position:relative;display:grid;grid-template-rows:auto 1fr auto;gap:14px;padding:18px}
    .row{display:grid;grid-template-columns:repeat(12,1fr);gap:12px}.fi{display:flex;flex-direction:column;gap:6px}.fi.x12{grid-column:1/-1}.fi.x9{grid-column:span 9}.fi.x6{grid-column:span 6}.fi.x4{grid-column:span 4}.fi.x3{grid-column:span 3}
    label{font-size:12px;color:var(--muted)}.inp,.sel,.ta{width:100%;padding:10px 12px;border-radius:12px;border:1px solid var(--border);background:rgba(255,255,255,.9);color:var(--text)}html[data-theme=dark] .inp,html[data-theme=dark] .sel,html[data-theme=dark] .ta{background:rgba(13,18,30,.7)}.ta{min-height:70px}
    .btn{border:1px solid var(--border);background:linear-gradient(180deg,#fff,#f4f7fb);color:var(--text);border-radius:999px;padding:10px 18px;cursor:pointer;transition:transform .08s ease,box-shadow .25s ease,opacity .2s ease,filter .2s ease}
    .btn:hover{transform:translateY(-1px);box-shadow:0 10px 30px rgba(15,23,42,.15)}.btn.primary{background:radial-gradient(100% 100% at 0% 0%,#67e8f9 0%,#38bdf8 40%,#a78bfa 100%);color:#021018;font-weight:800;letter-spacing:.2px;box-shadow:0 12px 30px rgba(56,189,248,.35),inset 0 0 0 1px rgba(2,16,24,.1)}
    .btn.publish{background:linear-gradient(135deg,#34d399,#10b981);color:#06281e;font-weight:800;box-shadow:0 12px 26px rgba(16,185,129,.35)}
    .btn[disabled]{opacity:.55;cursor:not-allowed;filter:saturate(.7) grayscale(.06)}
    .shot-card{position:relative;display:grid;place-items:center;padding:14px;border-radius:20px;background:rgba(255,255,255,.75);border:1px solid var(--ring);box-shadow:var(--shadow-big)}
    html[data-theme=dark] .shot-card{background:rgba(15,21,38,.65)}canvas.preview{width:100%;height:62vh;background:rgba(0,0,0,.08);border-radius:14px;display:block}
    .overlay{position:absolute;inset:0;display:none;align-items:center;justify-content:center;background:rgba(0,0,0,.08);pointer-events:none}.overlay.show{display:flex}
    .spinner{width:42px;height:42px;border:3px solid rgba(0,0,0,.08);border-top-color:var(--neon1);border-radius:50%;animation:spin 1s linear infinite}@keyframes spin{to{transform:rotate(360deg)}}
    .ticker{position:absolute;left:20px;right:20px;top:20px;z-index:3;display:flex;align-items:center;gap:10px;padding:10px 14px;border-radius:999px;border:1px solid var(--border);background:linear-gradient(135deg,rgba(255,255,255,.85),rgba(255,255,255,.7));backdrop-filter:blur(8px) saturate(160%);box-shadow:var(--shadow);font-weight:700}
    html[data-theme=dark] .ticker{background:linear-gradient(135deg,rgba(12,18,32,.85),rgba(12,18,32,.6))}
    .dot{width:8px;height:8px;border-radius:50%;background:var(--neон1);box-shadow:0 0 0 6px rgba(56,189,248,.15)}.tick-text{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .progress{position:absolute;left:20px;right:20px;top:20px;height:2px;transform:translateY(-8px);background:linear-gradient(90deg,var(--neon1),var(--neon2));border-radius:2px;opacity:.8;clip-path:inset(0 calc(100% - var(--p,0%)) 0 0 round 2px)}
    .tags{padding:0 12px 12px;display:flex;gap:10px;align-items:center;flex-wrap:wrap}.tag{font-size:12px;padding:2px 8px;border-radius:999px;border:1px solid var(--border)}
    .control-bar{padding:0 12px 12px;display:flex;gap:8px;align-items:center;flex-wrap:wrap}.control-on{outline:2px dashed var(--neon1);outline-offset:8px;border-radius:22px}
    .creative-block{grid-column:1/-1;display:flex;flex-direction:column;gap:14px;padding:16px;border-radius:var(--radius-sm);border:1px solid var(--border);background:var(--glass-2)}
    html[data-theme=dark] .creative-block{background:rgba(13,18,30,.7)}
    .creative-head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px}
    .creative-title{font-weight:700;font-size:15px}
    .creative-description{font-size:12px;color:var(--muted);line-height:1.45}
    .creative-modes{display:flex;flex-wrap:wrap;gap:8px}
    .creative-mode-btn{border:1px solid var(--border);background:rgba(255,255,255,.85);color:var(--text);border-radius:999px;padding:8px 16px;cursor:pointer;font-weight:600;transition:all .15s ease}
    html[data-theme=dark] .creative-mode-btn{background:rgba(13,18,30,.75)}
    .creative-mode-btn:hover{transform:translateY(-1px);box-shadow:0 6px 16px rgba(15,23,42,.12)}
    .creative-mode-btn.active{background:linear-gradient(135deg,var(--neon1),var(--neon2));color:#04111d;box-shadow:0 10px 24px rgba(56,189,248,.28)}
    .creative-panels{display:flex;flex-direction:column;gap:14px}
    .creative-panel{display:flex;flex-direction:column;gap:12px;padding:12px 14px;border-radius:18px;border:1px dashed var(--border);background:rgba(255,255,255,.6)}
    html[data-theme=dark] .creative-panel{background:rgba(12,18,30,.6)}
    .creative-panel.hidden{display:none}
    .creative-panel textarea{min-height:80px}
    .manual-assets{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:16px;margin-top:10px}
    .manual-card{display:flex;flex-direction:column;gap:10px;padding:14px;border-radius:16px;border:1px dashed var(--border);background:rgba(255,255,255,.6)}
    html[data-theme=dark] .manual-card{background:rgba(12,18,30,.45)}
    .manual-input-row{display:flex;gap:8px;align-items:center}
    .manual-input-row .inp{flex:1;min-width:200px}
    .manual-inline-note{font-size:12px;color:var(--muted)}
    .manual-upload-row{display:flex;gap:10px;flex-wrap:wrap}
    .manual-upload-row .btn{flex:1;min-width:160px;justify-content:center}
    .btn.ghost{background:rgba(255,255,255,.35);border-style:dashed;font-weight:600}
    html[data-theme=dark] .btn.ghost{background:rgba(255,255,255,.08)}
    .manual-card.fullwidth{grid-column:1/-1}
    .manual-thumb-grid{display:flex;flex-wrap:wrap;gap:10px;margin-top:6px}
    .manual-thumb{width:80px;height:80px;border-radius:18px;border:1px dashed var(--border);position:relative;display:flex;align-items:flex-end;justify-content:center;padding:6px;background:rgba(0,0,0,.04);overflow:hidden}
    html[data-theme=dark] .manual-thumb{background:rgba(255,255,255,.04)}
    .manual-thumb.has-preview{background-size:cover;background-position:center;background-repeat:no-repeat;color:#fff}
    .manual-thumb-label{font-size:11px;font-weight:600;text-align:center;padding:2px 6px;border-radius:999px;background:rgba(255,255,255,.92);color:var(--text);max-width:72px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    html[data-theme=dark] .manual-thumb-label{background:rgba(0,0,0,.65);color:#fff}
    .manual-thumb button{position:absolute;top:5px;right:5px;width:22px;height:22px;border-radius:50%;border:none;background:rgba(0,0,0,.65);color:#fff;font-weight:700;cursor:pointer}
    .manual-thumb button:hover{background:rgba(0,0,0,.8)}
    .manual-chip-list{display:flex;flex-direction:column;gap:8px;margin-top:10px}
    .manual-chip{display:flex;align-items:center;justify-content:space-between;gap:14px;padding:10px 14px;border-radius:14px;border:1px dashed var(--border);background:rgba(255,255,255,.7)}
    html[data-theme=dark] .manual-chip{background:rgba(255,255,255,.08)}
    .manual-chip button{border:none;background:transparent;color:var(--muted);font-size:16px;cursor:pointer}
    .manual-chip button:hover{color:var(--err)}
    .manual-empty{font-size:12px;color:var(--muted);margin-top:6px;display:none}
    .creative-subtitle{font-size:13px;font-weight:600}
    .creative-note{font-size:12px;color:var(--muted);line-height:1.45}
    .creative-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px}
    .creative-grid .col{display:flex;flex-direction:column;gap:6px}
    .creative-badge{padding:4px 10px;border-radius:999px;background:linear-gradient(135deg,var(--neon1),var(--neon3));font-size:11px;font-weight:700;color:#02151d;letter-spacing:.2px}
    html[data-theme=dark] .creative-badge{color:#0d1b25}
    .creative-tag.hidden{display:none!important}
    .hint{font-size:12px;color:var(--muted)}.kbd{font-family:ui-monospace, SFMono-Regular, Menlo, monospace;font-size:12px;background:rgba(0,0,0,.06);padding:2px 6px;border-radius:6px;border:1px solid var(--border)}
  </style>
</head>
<body>
  <div class="shell">
    <!-- LEFT -->
    <aside class="panel menu">
      <div class="head">
        <div style="width:36px;height:36px;border-radius:12px;background:linear-gradient(135deg,var(--neon1),var(--neon2))"></div>
        <div>Меню</div>
      </div>
      <a class="mitem" href="/">Главная</a>
      <a class="mitem" href="/companies/list">Компании</a>
      <a class="mitem" href="/accounts">Аккаунты</a>
      <div style="margin-top:auto" class="muted">Powered by EasyByte</div>
    </aside>

    <!-- CENTER -->
    <section class="panel stage">
      <div>
        <div class="title" style="margin:8px 12px;font-weight:800;letter-spacing:.2px">Параметры запуска</div>
        <div class="row" style="padding:0 12px 12px">
          <div class="fi x9"><label>Website URL</label><input id="url" class="inp" placeholder="https://example.com"/></div>
          <div class="fi x3"><label>Бюджет (мин. в день)</label><input id="budgetMin" class="inp" type="number" min="0" placeholder="100"/></div>
          <div class="fi x3"><label>Бюджет (макс. в день)</label><input id="budgetMax" class="inp" type="number" min="0" placeholder="5000"/></div>

          <div class="fi x6"><label>УТП / описание</label><textarea id="usp" class="ta" placeholder="Нейросети под ключ для e‑commerce"></textarea></div>
          <div class="fi x3"><label>Сколько объявлений (шт.)</label><input id="n_ads" class="inp" type="number" min="1" max="50" step="1" value="3"/></div>
          <div class="fi x3"><label>Тип кампании</label>
            <select id="campaignType" class="sel">__CAMPAIGN_TYPE_OPTIONS__</select>
          </div>

          <div class="fi x6"><label>Гео (через , или ;)</label><input id="locations" class="inp" placeholder="RU, United States, US,CA"/></div>

          <!-- Языки: скрытое поле; виджет рендерится отдельным компонентом (см. исходники) -->
          <div class="fi x6">
            <label>Языки</label>
            <input id="languages" class="inp" value="en"/>
          </div>

          <div class="fi x12 creative-block" id="creativeBlock">
            <div class="creative-head">
              <div>
                <div class="creative-title">Режим креативов</div>
                <div class="creative-description">Выберите, как генерировать визуалы и тексты для Demand Gen. В ручном режиме понадобятся готовые материалы.</div>
              </div>
              <div class="creative-badge">Demand Gen</div>
            </div>
            <div class="creative-modes" role="radiogroup" aria-label="Режим креативов">
              <button type="button" class="creative-mode-btn active" data-mode="ai_only">Автоматика</button>
              <button type="button" class="creative-mode-btn" data-mode="inspired">Микс</button>
              <button type="button" class="creative-mode-btn" data-mode="manual">Ручное</button>
            </div>
            <div class="creative-panels">
              <div class="creative-panel" data-mode="ai_only">
                <div class="creative-subtitle">Полностью нейросеть</div>
                <div class="creative-note">Генерируем изображения и тексты без примеров — достаточно указать УТП и сайт.</div>
              </div>
              <div class="creative-panel hidden" data-mode="inspired">
                <div class="creative-subtitle">Микс: добавьте примеры</div>
                <div class="creative-note">Загрузите несколько готовых образцов, чтобы нейросеть ориентировалась на них.</div>
                <div class="manual-assets">
                  <div class="manual-card">
                    <div class="creative-subtitle">Референсы изображений (до 5)</div>
                    <div class="manual-inline-note">PNG/JPG/WebP до 25 МБ.</div>
                    <div class="manual-upload-row">
                      <input type="file" id="mixImageFile" accept="image/*" multiple hidden />
                      <button type="button" class="btn ghost" id="mixImageUpload" data-label="Загрузить файлы">Загрузить файлы</button>
                    </div>
                    <div class="manual-empty" id="mixImagesEmpty">Загрузите хотя бы один пример изображения.</div>
                    <div class="manual-thumb-grid" id="mixImagesList"></div>
                  </div>
                  <div class="manual-card">
                    <div class="creative-subtitle">Примеры заголовков (до 5)</div>
                    <div class="manual-input-row">
                      <input id="mixHeadlineInput" class="inp" placeholder="Введите заголовок" />
                      <button type="button" class="btn ghost" id="mixHeadlineAdd" data-label="+ Добавить">+ Добавить</button>
                    </div>
                    <div class="manual-empty" id="mixHeadlinesEmpty">Добавьте минимум один пример заголовка.</div>
                    <div class="manual-chip-list" id="mixHeadlinesList"></div>
                  </div>
                  <div class="manual-card">
                    <div class="creative-subtitle">Примеры описаний (до 5)</div>
                    <div class="manual-input-row">
                      <input id="mixDescriptionInput" class="inp" placeholder="Введите описание" />
                      <button type="button" class="btn ghost" id="mixDescriptionAdd" data-label="+ Добавить">+ Добавить</button>
                    </div>
                    <div class="manual-empty" id="mixDescriptionsEmpty">Добавьте минимум одно описание.</div>
                    <div class="manual-chip-list" id="mixDescriptionsList"></div>
                  </div>
                  <div class="manual-card fullwidth">
                    <div class="creative-subtitle">Заметки (опционально)</div>
                    <textarea id="mixSeedNotes" class="ta" placeholder="Стиль, цвета, слоганы, обязательные элементы…"></textarea>
                  </div>
                </div>
              </div>
              <div class="creative-panel hidden" data-mode="manual">
                <div class="creative-subtitle">Ручной режим</div>
                <div class="creative-note">Загрузите готовые файлы — мы подставим их в кампанию как есть. Поддерживаем до 5 изображений и 5 логотипов.</div>
                <div class="manual-assets">
                  <div class="manual-card">
                    <div class="creative-subtitle">Изображения (до 5)</div>
                    <div class="manual-inline-note">PNG/JPG/WebP до 25 МБ.</div>
                    <div class="manual-upload-row">
                      <input type="file" id="manualImageFile" accept="image/*" multiple hidden />
                      <button type="button" class="btn ghost" id="manualImageUpload" data-label="Загрузить файлы">Загрузить файлы</button>
                    </div>
                    <div class="manual-empty" id="manualImagesEmpty">Добавьте до 5 изображений — после загрузки появится превью.</div>
                    <div class="manual-thumb-grid" id="manualImagesList"></div>
                  </div>
                  <div class="manual-card">
                    <div class="creative-subtitle">Логотипы (до 5)</div>
                    <div class="manual-inline-note">Опционально, PNG с прозрачностью до 25 МБ.</div>
                    <div class="manual-upload-row">
                      <input type="file" id="manualLogoFile" accept="image/*" multiple hidden />
                      <button type="button" class="btn ghost" id="manualLogoUpload" data-label="Загрузить логотипы">Загрузить логотипы</button>
                    </div>
                    <div class="manual-empty" id="manualLogosEmpty">Логотипы можно добавить позже в Ads, но лучше загрузить сейчас.</div>
                    <div class="manual-thumb-grid" id="manualLogosList"></div>
                  </div>
                  <div class="manual-card">
                    <div class="creative-subtitle">Заголовки (до 5)</div>
                    <div class="manual-input-row">
                      <input id="manualHeadlineInput" class="inp" placeholder="Введите заголовок" />
                      <button type="button" class="btn ghost" id="manualHeadlineAdd" data-label="+ Добавить">+ Добавить</button>
                    </div>
                    <div class="manual-empty" id="manualHeadlinesEmpty">Добавьте минимум один заголовок.</div>
                    <div class="manual-chip-list" id="manualHeadlinesList"></div>
                  </div>
                  <div class="manual-card">
                    <div class="creative-subtitle">Описания (до 5)</div>
                    <div class="manual-input-row">
                      <input id="manualDescriptionInput" class="inp" placeholder="Введите описание" />
                      <button type="button" class="btn ghost" id="manualDescriptionAdd" data-label="+ Добавить">+ Добавить</button>
                    </div>
                    <div class="manual-empty" id="manualDescriptionsEmpty">Добавьте минимум одно описание.</div>
                    <div class="manual-chip-list" id="manualDescriptionsList"></div>
                  </div>
                  <div class="manual-card fullwidth">
                    <div class="creative-subtitle">Название бренда (опционально)</div>
                    <input id="creativeManualBusiness" class="inp" placeholder="Будет подставлено в объявления"/>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <div class="fi x6"><label>Профиль AdsPower (аккаунт)</label>
            <div style="display:flex;gap:8px">
              <select id="profile" class="sel" style="flex:1"><option value="">— загрузите список —</option></select>
              <button class="btn" id="profilesReload">Обновить</button>
            </div>
            <div class="muted" style="margin-top:6px;font-size:12px">Headless‑превью: окно браузера не мешает, картинка транслируется сюда.</div>
          </div>

          <div class="fi x6">
            <label>Превью и запуск</label>
            <div class="switch">
              <input id="headless" type="checkbox" checked />
              <span>Headless‑превью (без переключения окон)</span>
            </div>
            <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:10px">
              <button class="btn primary" id="run">Запустить создание</button>
            </div>
          </div>

        </div>
      </div>

      <div class="shot-card" id="card" tabindex="0">
        <div class="progress" id="progress" style="--p:0%"></div>
        <div class="ticker" id="ticker"><div class="dot"></div><div class="tick-text" id="tickText">Готовлю браузер…</div></div>
        <div class="overlay" id="overlay"><div class="spinner"></div></div>
        <canvas id="preview" class="preview"></canvas>
      </div>

      <div class="control-bar">
        <button class="btn" id="controlBtn">Взять управление</button>
        <span class="hint">Клик/колёсико/клавиши → в браузер. Для текста — просто печатайте. Горячие: <span class="kbd">Enter</span>, <span class="kbd">Tab</span>, <span class="kbd">Esc</span>, <span class="kbd">← → ↑ ↓</span>, <span class="kbd">Backspace</span>.</span>
      </div>

      <div class="tags">
        <span class="tag">URL: <b id="urlSpan">—</b></span>
        <span class="tag">Профиль: <b id="profileSpan">—</b></span>
        <span class="tag">Тип: <b id="typeSpan">__DEFAULT_CAMPAIGN_LABEL__</b></span>
        <span class="tag">Google: <b id="acctSpan">—</b></span>
        <span class="tag">Режим: <b id="modeSpan">headless</b></span>
        <span class="tag">Статус: <b id="busySpan">idle</b></span>
        <span class="tag creative-tag" id="creativeTag">Креативы: <b id="creativeSpan">Автоматика</b></span>
      </div>
    </section>
  </div>

  <!-- ===== 2FA MODAL (код подтверждения) ===== -->
  <div id="codeModal" class="modal-backdrop" role="dialog" aria-modal="true" aria-labelledby="codeTitle" style="position:fixed;inset:0;display:none;align-items:center;justify-content:center;background:rgba(2,10,20,.42);backdrop-filter:blur(6px) saturate(120%);z-index:1000">
    <div class="modal-card" style="width:min(480px,92vw);border-radius:16px;border:1px solid var(--border);background:var(--glass);box-shadow:var(--shadow-big);padding:16px">
      <h3 id="codeTitle" style="margin:0 0 10px;font-size:18px;">Подтвердите вход</h3>
      <div class="modal-desc" id="codeDesc" style="color:var(--muted);font-size:13px;margin-bottom:8px">Введите 6‑значный код из приложения/почты и нажмите «Подтвердить».</div>
      <input id="codeInput" class="inp" placeholder="Код подтверждения" />
      <div class="modal-actions" style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px;">
        <button class="btn" id="codeCancel">Отмена</button>
        <button class="btn primary" id="codeConfirm">Подтвердить</button>
      </div>
    </div>
  </div>

<script>
const $ = (s)=>document.querySelector(s);
const canvas = $("#preview");
const ctx = canvas.getContext("2d");
const card = $("#card");
const overlay = $("#overlay");
const runBtn = $("#run");
const profileSel = $("#profile");
const urlSpan = $("#urlSpan");
const busySpan = $("#busySpan");
const profileSpan = $("#profileSpan");
const typeSel = $("#campaignType");
const typeSpan = $("#typeSpan");
const acctSpan = $("#acctSpan");
const modeSpan = $("#modeSpan");
const tickText = $("#tickText");
const progress = $("#progress");
const controlBtn = $("#controlBtn");
const headlessChk = $("#headless");
const creativeTag = $("#creativeTag");
const creativeSpan = $("#creativeSpan");
const creativeModeButtons = Array.from(document.querySelectorAll(".creative-mode-btn"));
const creativePanels = Array.from(document.querySelectorAll(".creative-panel"));
let creativeMode = "ai_only";
const CAMPAIGN_VARIANTS = __CAMPAIGN_VARIANTS_JSON__;
const DEFAULT_VARIANT = (Array.isArray(CAMPAIGN_VARIANTS) && CAMPAIGN_VARIANTS.length) ? CAMPAIGN_VARIANTS[0] : null;
const MANUAL_LIMIT = 5;

function isNoAccount(raw){
  const value = String(raw || '').trim();
  if (!value) return true;
  if (value.toLowerCase() === 'no_account') return true;
  return /^[\-\u2013\u2014]+$/.test(value);
}

function normalizeVariantId(value){
  return String(value || "").trim();
}

function findVariantById(rawId){
  const id = normalizeVariantId(rawId).toUpperCase();
  if (!id) return DEFAULT_VARIANT;
  return CAMPAIGN_VARIANTS.find(v => String(v.id || "").toUpperCase() === id) || DEFAULT_VARIANT;
}

function getCurrentVariant(){
  if (!typeSel) return DEFAULT_VARIANT;
  return findVariantById(typeSel.value);
}

function getChooseType(variant){
  if (variant && variant.choose_type) return variant.choose_type;
  return (DEFAULT_VARIANT && DEFAULT_VARIANT.choose_type) ? DEFAULT_VARIANT.choose_type : "UBERVERSAL";
}

function updateTypeTag(){
  if (!typeSpan) return;
  const v = getCurrentVariant();
  typeSpan.textContent = (v && v.label) ? v.label : '—';
}

function creativeModeLabel(mode){
  const value = String(mode || "").toLowerCase();
  if (value === "inspired") return "Микс";
  if (value === "manual") return "Ручное";
  return "Автоматика";
}

function updateCreativeTag(){
  if (creativeSpan){
    creativeSpan.textContent = creativeModeLabel(creativeMode);
  }
}

function setCreativeMode(mode, opts = {}){
  const normalized = String(mode || "").trim().toLowerCase();
  const allowed = ["ai_only", "inspired", "manual"];
  creativeMode = allowed.includes(normalized) ? normalized : "ai_only";
  creativeModeButtons.forEach(btn=>{
    const isActive = (btn.dataset.mode || "") === creativeMode;
    btn.classList.toggle("active", isActive);
    if (isActive && !opts.silent){
      btn.blur();
    }
  });
  creativePanels.forEach(panel=>{
    const shouldHide = (panel.dataset.mode || "") !== creativeMode;
    panel.classList.toggle("hidden", shouldHide);
  });
  updateCreativeTag();
}

if (typeSel){
  typeSel.addEventListener("change", updateTypeTag);
}
creativeModeButtons.forEach(btn=>{
  btn.addEventListener("click", ()=>{
    const mode = btn.dataset.mode || "ai_only";
    setCreativeMode(mode);
  });
});
setCreativeMode("ai_only", { silent: true });
updateTypeTag();

const MANUAL_FILE_MAX_BYTES = 25 * 1024 * 1024;
const manualRefs = {
  images: {
    addBtn: $("#manualImageUpload"),
    fileInput: $("#manualImageFile"),
    list: $("#manualImagesList"),
    empty: $("#manualImagesEmpty"),
  },
  logos: {
    addBtn: $("#manualLogoUpload"),
    fileInput: $("#manualLogoFile"),
    list: $("#manualLogosList"),
    empty: $("#manualLogosEmpty"),
  },
  headlines: {
    input: $("#manualHeadlineInput"),
    addBtn: $("#manualHeadlineAdd"),
    list: $("#manualHeadlinesList"),
    empty: $("#manualHeadlinesEmpty"),
  },
  descriptions: {
    input: $("#manualDescriptionInput"),
    addBtn: $("#manualDescriptionAdd"),
    list: $("#manualDescriptionsList"),
    empty: $("#manualDescriptionsEmpty"),
  },
};
const mixRefs = {
  images: {
    addBtn: $("#mixImageUpload"),
    fileInput: $("#mixImageFile"),
    list: $("#mixImagesList"),
    empty: $("#mixImagesEmpty"),
  },
  headlines: {
    input: $("#mixHeadlineInput"),
    addBtn: $("#mixHeadlineAdd"),
    list: $("#mixHeadlinesList"),
    empty: $("#mixHeadlinesEmpty"),
  },
  descriptions: {
    input: $("#mixDescriptionInput"),
    addBtn: $("#mixDescriptionAdd"),
    list: $("#mixDescriptionsList"),
    empty: $("#mixDescriptionsEmpty"),
  },
};

const creativeManagers = {
  manual: {
    limit: MANUAL_LIMIT,
    assetKeys: new Set(["images", "logos"]),
    refs: manualRefs,
    state: {
      images: [],
      logos: [],
      headlines: [],
      descriptions: [],
    },
  },
  mix: {
    limit: MANUAL_LIMIT,
    assetKeys: new Set(["images"]),
    refs: mixRefs,
    state: {
      images: [],
      headlines: [],
      descriptions: [],
    },
  },
};

const manualState = creativeManagers.manual.state;
const mixState = creativeManagers.mix.state;

function baseNameFromPath(value){
  const cleaned = String(value || "").trim();
  if (!cleaned) return "";
  const urlPart = cleaned.split("?")[0].split("#")[0];
  const parts = urlPart.split(/[\\/]/);
  const last = parts[parts.length - 1];
  return last || cleaned;
}

function isPreviewable(value){
  return /^https?:\/\//i.test(value || "") || String(value || "").trim().startsWith("data:");
}

function isAssetCollection(scopeName, key){
  const mgr = creativeManagers[scopeName];
  return Boolean(mgr && mgr.assetKeys.has(key));
}

function renderCreativeList(scopeName, key){
  const mgr = creativeManagers[scopeName];
  if (!mgr) return;
  const ref = mgr.refs[key];
  if (!ref || !ref.list) return;
  const items = mgr.state[key] || [];
  ref.list.innerHTML = "";
  if (ref.empty){
    ref.empty.style.display = items.length ? "none" : "block";
  }
  items.forEach((entry, index)=>{
    const rawValue = typeof entry === "object" && entry !== null ? entry.value : entry;
    if (!rawValue) return;
    const previewValue = (typeof entry === "object" && entry !== null && entry.preview) || "";
    const labelText = (typeof entry === "object" && entry !== null && entry.name) || baseNameFromPath(rawValue) || "item";
    let node;
    if (isAssetCollection(scopeName, key)){
      node = document.createElement("div");
      node.className = "manual-thumb";
      const previewSource = previewValue || (isPreviewable(rawValue) ? rawValue : "");
      if (previewSource){
        node.classList.add("has-preview");
        const safe = previewSource.replace(/"/g, '\\"');
        node.style.backgroundImage = `url("${safe}")`;
      }
      const label = document.createElement("div");
      label.className = "manual-thumb-label";
      label.textContent = labelText;
      node.appendChild(label);
    } else {
      node = document.createElement("div");
      node.className = "manual-chip";
      const span = document.createElement("span");
      span.textContent = rawValue;
      node.appendChild(span);
    }
    const removeBtn = document.createElement("button");
    removeBtn.type = "button";
    removeBtn.dataset.removeIndex = String(index);
    removeBtn.textContent = "\u00D7";
    node.appendChild(removeBtn);
    ref.list.appendChild(node);
  });
  updateCreativeControls(scopeName, key);
}

function updateCreativeControls(scopeName, key){
  const mgr = creativeManagers[scopeName];
  if (!mgr) return;
  const ref = mgr.refs[key];
  if (!ref) return;
  const bucket = mgr.state[key] || [];
  const atLimit = bucket.length >= mgr.limit;
  if (ref.addBtn){
    const defaultLabel = ref.addBtn.dataset.label || ref.addBtn.textContent;
    if (ref.addBtn.dataset.loading === "1"){
      ref.addBtn.disabled = true;
      ref.addBtn.textContent = "Загружаю…";
    } else {
      ref.addBtn.disabled = atLimit;
      ref.addBtn.textContent = atLimit ? "Лимит 5" : defaultLabel;
    }
  }
  if (ref.input){
    ref.input.disabled = atLimit;
  }
}

function splitLineValues(raw){
  return String(raw || "")
    .split(/\\r?\\n/)
    .map(part => part.trim())
    .filter(Boolean);
}

function addTextValue(scopeName, key, rawValue){
  const mgr = creativeManagers[scopeName];
  if (!mgr) return;
  const ref = mgr.refs[key];
  const bucket = mgr.state[key];
  if (!bucket) return;
  const parts = Array.isArray(rawValue) ? rawValue : splitLineValues(rawValue);
  if (!parts.length) return;
  for (const value of parts){
    if (!value) continue;
    if (bucket.length >= mgr.limit) break;
    bucket.push(value);
  }
  if (ref && ref.input){
    ref.input.value = "";
  }
  renderCreativeList(scopeName, key);
}

function pushAssetEntry(scopeName, key, asset){
  const mgr = creativeManagers[scopeName];
  if (!mgr) return;
  const bucket = mgr.state[key];
  if (!bucket || bucket.length >= mgr.limit) return;
  bucket.push({
    value: asset.value,
    preview: asset.preview || "",
    name: asset.name || baseNameFromPath(asset.value),
  });
  renderCreativeList(scopeName, key);
}

function readFilePreview(file){
  return new Promise((resolve, reject)=>{
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result);
    reader.onerror = reject;
    reader.readAsDataURL(file);
  });
}

async function uploadManualFile(kind, file){
  const form = new FormData();
  form.append("file", file);
  form.append("kind", kind);
  const resp = await fetch("/api/manual-assets/upload", {
    method: "POST",
    body: form,
  });
  const data = await resp.json().catch(()=> ({}));
  if (!resp.ok || !data || !data.ok || !data.path){
    const code = (data && data.error) || (resp.status === 413 ? "too_large" : "upload_failed");
    throw new Error(code);
  }
  return data;
}


function setAssetUploading(scopeName, key, loading){
  const mgr = creativeManagers[scopeName];
  if (!mgr) return;
  const ref = mgr.refs[key];
  if (!ref || !ref.addBtn) return;
  if (loading){
    ref.addBtn.dataset.loading = "1";
    ref.addBtn.disabled = true;
    ref.addBtn.textContent = "Загружаю…";
  } else {
    ref.addBtn.dataset.loading = "0";
    updateCreativeControls(scopeName, key);
  }
}

async function handleAssetFiles(scopeName, key, fileList){
  const mgr = creativeManagers[scopeName];
  if (!mgr) return;
  const files = Array.from(fileList || []);
  if (!files.length) return;
  const bucket = mgr.state[key];
  if (!bucket) return;
  const slots = mgr.limit - bucket.length;
  if (slots <= 0) return;
  for (const file of files.slice(0, slots)){
    if (!file.type || !file.type.startsWith("image/")){
      continue;
    }
    if (file.size > MANUAL_FILE_MAX_BYTES){
      alert(`Файл «${file.name}» слишком большой. Максимальный размер — 25 МБ.`);
      continue;
    }
    try{
      setAssetUploading(scopeName, key, true);
      const [uploadResult, preview] = await Promise.all([
        uploadManualFile(key, file),
        readFilePreview(file),
      ]);
      pushAssetEntry(scopeName, key, { value: uploadResult.path, preview, name: file.name });
    }catch(err){
      console.error(err);
      if (err && err.message === "too_large"){
        alert(`Файл «${file.name}» слишком большой. Максимальный размер — 25 МБ.`);
      } else if (err && err.message === "unsupported_type"){
        alert(`Формат «${file.name}» не поддерживается. Попробуйте PNG/JPG/WebP.`);
      } else {
        alert(`Не удалось загрузить «${file.name}». Попробуйте ещё раз.`);
      }
    }finally{
      setAssetUploading(scopeName, key, false);
    }
  }
}

function initTextCollections(scopeName, keys){
  const mgr = creativeManagers[scopeName];
  if (!mgr) return;
  keys.forEach((key)=>{
    const ref = mgr.refs[key];
    if (!ref) return;
    if (ref.addBtn){
      ref.addBtn.dataset.label = ref.addBtn.dataset.label || ref.addBtn.textContent;
      ref.addBtn.addEventListener("click", ()=>{
        addTextValue(scopeName, key, ref.input ? ref.input.value : "");
      });
    }
    if (ref.input){
      ref.input.addEventListener("keydown", (e)=>{
        if (e.key === "Enter"){
          e.preventDefault();
          addTextValue(scopeName, key, ref.input.value);
        }
      });
    }
    renderCreativeList(scopeName, key);
  });
}

function initAssetCollections(scopeName, keys){
  const mgr = creativeManagers[scopeName];
  if (!mgr) return;
  keys.forEach((key)=>{
    const ref = mgr.refs[key];
    if (!ref || !ref.addBtn || !ref.fileInput) return;
    ref.addBtn.dataset.label = ref.addBtn.dataset.label || ref.addBtn.textContent;
    ref.addBtn.addEventListener("click", ()=>{
      if (ref.addBtn.disabled) return;
      ref.fileInput.click();
    });
    ref.fileInput.addEventListener("change", (e)=>{
      if (e.target.files && e.target.files.length){
        handleAssetFiles(scopeName, key, e.target.files);
      }
      ref.fileInput.value = "";
    });
    renderCreativeList(scopeName, key);
  });
}

function initRemoval(scopeName, keys){
  const mgr = creativeManagers[scopeName];
  if (!mgr) return;
  keys.forEach((key)=>{
    const ref = mgr.refs[key];
    if (!ref || !ref.list) return;
    ref.list.addEventListener("click", (event)=>{
      const target = event.target.closest("[data-remove-index]");
      if (!target) return;
      const idx = Number(target.dataset.removeIndex);
      if (!Number.isFinite(idx)) return;
      const bucket = mgr.state[key];
      if (!bucket) return;
      bucket.splice(idx, 1);
      renderCreativeList(scopeName, key);
    });
  });
}

initAssetCollections("manual", ["images", "logos"]);
initTextCollections("manual", ["headlines", "descriptions"]);
initRemoval("manual", ["images", "logos", "headlines", "descriptions"]);

initAssetCollections("mix", ["images"]);
initTextCollections("mix", ["headlines", "descriptions"]);
initRemoval("mix", ["images", "headlines", "descriptions"]);
(function markActiveMenu(){
  const links = document.querySelectorAll('.menu .mitem[href]');
  const path = location.pathname || '/';
  let best = null, bestScore = -1;
  function score(href){
    if (href === '/') return path === '/' ? 1000 : -1;
    if (path.startsWith(href)) return href.length;
    if (href.startsWith('/companies') && path.startsWith('/companies')) return 50 + href.length;
    if (href.startsWith('/accounts') && path.startsWith('/accounts')) return 50 + href.length;
    return -1;
  }
  links.forEach(a=>{
    const href = a.getAttribute('href') || '';
    const sc = score(href);
    if (sc > bestScore){ bestScore = sc; best = a; }
  });
  if (best) best.classList.add('active');
})();

/* ====================== Preview / AdsPower helpers ====================== */
let lastVW = 0, lastVH = 0;
let previewES = null;

function setBusy(v){ if(v){ overlay.classList.add("show"); busySpan.textContent="busy"; } else { overlay.classList.remove("show"); busySpan.textContent="idle"; } }
function setTicker(text){ tickText.textContent = text||""; }
function setProgress(p){ progress.style.setProperty('--p', Math.max(0, Math.min(100, p)) + '%'); }

const tickerQueue = [];
let tickerPlaying = false;
let tickerTimeout = null;

function stopTicker(){
  if (tickerTimeout){
    clearTimeout(tickerTimeout);
    tickerTimeout = null;
  }
  tickerPlaying = false;
}

function enqueueTicker(text, holdMs = 1700, priority = false){
  if (typeof text !== "string") return;
  const trimmed = text.trim();
  if (!trimmed) return;
  const item = { text: trimmed, hold: holdMs };
  if (priority){
    tickerQueue.unshift(item);
    if (tickerPlaying){
      stopTicker();
    }
  } else {
    tickerQueue.push(item);
  }
  if (!tickerPlaying){
    playNextTicker();
  }
}

function playNextTicker(){
  if (!tickerQueue.length){
    tickerPlaying = false;
    tickerTimeout = null;
    return;
  }
  tickerPlaying = true;
  const item = tickerQueue.shift();
  setTicker(item.text);
  tickerTimeout = setTimeout(()=>{
    tickerPlaying = false;
    tickerTimeout = null;
    playNextTicker();
  }, item.hold || 1700);
}

async function loadProfiles(q=""){
  const url = new URL("/api/adspower/profiles", location.origin);
  if(q) url.searchParams.set('q', q);
  const r = await fetch(url);
  const j = await r.json();
  profileSel.innerHTML = "";
  if (!j.items || !j.items.length){
    profileSel.innerHTML = '<option value="">— не найдено —</option>';
    acctSpan.textContent = '—';
    return;
  }
  for(const it of j.items){
    const opt = document.createElement('option');
    opt.value = it.profile_id;
    const emailRaw = (it.google_email || '').trim();
    if (isNoAccount(emailRaw)) continue;
    if (!emailRaw.toLowerCase().includes('@gmail.com')) continue;
    const rawEmail = emailRaw;
    const googleName = (it.google_name || '').trim();
    const profileLabel = (it.name || '').trim() || it.profile_id;
    const labelParts = [];
    if (googleName) labelParts.push(googleName);
    if (rawEmail) labelParts.push(rawEmail);
    if (profileLabel) labelParts.push(profileLabel);
    labelParts.push(it.profile_id);
    opt.textContent = labelParts.filter(Boolean).join(' — ');
    if (rawEmail){
      opt.dataset.acct = rawEmail;
    } else if (opt.dataset && opt.dataset.acct){
      delete opt.dataset.acct;
    }
    if (googleName){
      opt.dataset.gname = googleName;
    } else if (opt.dataset && opt.dataset.gname){
      delete opt.dataset.gname;
    }
    if (profileLabel){
      opt.dataset.pname = profileLabel;
    } else if (opt.dataset && opt.dataset.pname){
      delete opt.dataset.pname;
    }
    profileSel.appendChild(opt);
  }
  updateAcctTag();
}
$("#profilesReload").addEventListener("click", ()=> loadProfiles().catch(()=>{}));
loadProfiles().catch(()=>{});

// ====== canvas drawing ======
let frameQueue = [];
let decoding = false;

function b64ToBlob(b64, mime){
  const byteString = atob(b64);
  const len = byteString.length;
  const ab = new ArrayBuffer(len);
  const ia = new Uint8Array(ab);
  for (let i=0;i<len;i++) ia[i] = byteString.charCodeAt(i);
  return new Blob([ab], {type: mime});
}

async function decodeFrame(frame){
  const mime = frame.fmt === "png" ? "image/png" : "image/jpeg";
  const blob = b64ToBlob(frame.b64, mime);
  if ('createImageBitmap' in window){
    return await createImageBitmap(blob);
  } else {
    return await new Promise((resolve, reject)=>{
      const url = URL.createObjectURL(blob);
      const img = new Image();
      img.onload = ()=>{ URL.revokeObjectURL(url); resolve(img); };
      img.onerror = (e)=>{ URL.revokeObjectURL(url); reject(e); };
      img.src = url;
    });
  }
}

function drawToCanvas(img, vw, vh){
  const rect = canvas.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  const needW = Math.round(rect.width * dpr);
  const needH = Math.round(rect.height * dpr);
  if (canvas.width !== needW || canvas.height !== needH){
    canvas.width = needW; canvas.height = needH;
  }
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  const basisW = vw || img.width;
  const basisH = vh || img.height;
  const s = Math.min(rect.width/Math.max(1, basisW), rect.height/Math.max(1, basisH));
  const dispW = basisW*s;
  const dispH = basisH*s;
  const left = (rect.width - dispW)/2;
  const top  = (rect.height - dispH)/2;
  ctx.clearRect(0,0,rect.width,rect.height);
  ctx.drawImage(img, left, top, dispW, dispH);
  lastVW = basisW;
  lastVH = basisH;
}

function scheduleDraw(){
  if (decoding || !frameQueue.length) return;
  decoding = true;
  const frame = frameQueue.shift();
  decodeFrame(frame).then(img=>{
    drawToCanvas(img, frame.vw, frame.vh);
  }).catch(()=>{}).finally(()=>{ decoding = false; });
}
function rafLoop(){ scheduleDraw(); requestAnimationFrame(rafLoop); }
rafLoop();

// ====== preview (SSE + Pull fallback) ======
let lastImageAt = 0;
let pullTimer = null;
let pulling = false;

function stopPull(){
  if (pullTimer){ clearInterval(pullTimer); pullTimer=null; }
  pulling = false;
}
async function pullOnce(){
  const pid = String(profileSel.value||"").trim();
  const headless = !!headlessChk.checked;
  if(!pid) return;
  try{
    const u = `/api/shot?profile_id=${encodeURIComponent(pid)}&headless=${headless?1:0}&_=${Date.now()}`;
    const r = await fetch(u, { cache: 'no-store' });
    if(!r.ok) return;
    const blob = await r.blob();
    let img;
    if ('createImageBitmap' in window){ img = await createImageBitmap(blob); }
    else { img = await new Promise((resolve, reject)=>{ const im=new Image(); im.onload=()=>resolve(im); im.onerror=reject; im.src = URL.createObjectURL(blob); }); }
    drawToCanvas(img, lastVW||0, lastVH||0);
    lastImageAt = Date.now();
  }catch(_){}
}
function ensurePull(){
  const now = Date.now();
  if ((now - lastImageAt) > 2000 && !pulling){
    pulling = true;
    pullTimer = setInterval(pullOnce, 800);
  }
}
function cancelPullIfLive(){
  if (pulling && (Date.now() - lastImageAt) < 1200){
    stopPull();
  }
}

function updateAcctTag(){
  const opt = profileSel.selectedOptions && profileSel.selectedOptions[0];
  if (!opt || !opt.dataset){
    acctSpan.textContent = '—';
    return;
  }
  const parts = [];
  if (opt.dataset.gname) parts.push(opt.dataset.gname);
  if (opt.dataset.acct) parts.push(opt.dataset.acct);
  acctSpan.textContent = parts.length ? parts.join(' — ') : '—';
}

function startPreview(){
  const pid = String(profileSel.value||"").trim();
  const headless = !!headlessChk.checked;
  updateAcctTag();
  if(!pid) return;
  if (previewES){ try{ previewES.close(); }catch(_){ } previewES=null; }
  stopPull();
  modeSpan.textContent = headless ? "headless" : "window";
  const url = "/api/preview?profile_id="+encodeURIComponent(pid)+"&headless="+(headless?1:0);
  const es = new EventSource(url);
  es.addEventListener("image", (e)=>{
    try{
      const j = JSON.parse(e.data||"{}");
      if (j && j.data){
        if (frameQueue.length > 2) frameQueue.shift();
        frameQueue.push({ b64:j.data, fmt:j.fmt||"jpeg", vw:j.vw||0, vh:j.vh||0 });
        lastImageAt = Date.now();
        cancelPullIfLive();
      }else{
        ensurePull();
      }
      if (j && (j.vw||j.vh)){ lastVW = j.vw||0; lastVH = j.vh||0; }
    }catch(_){ ensurePull(); }
  });
  es.onerror = ()=>{ try{ es.close(); }catch(_){ } ensurePull(); setTimeout(startPreview, 1000); };
  previewES = es;

  // лёгкий пинг, чтобы отобразить статус/URL
  const u = new URL("/api/status", location.origin);
  u.searchParams.set("profile_id", pid);
  u.searchParams.set("headless", headless? "1":"0");
  fetch(u).then(r=>r.json()).then(j=>{
    urlSpan.textContent = j.url || "—";
    profileSpan.textContent = pid || "—";
    updateAcctTag();
  }).catch(()=>{});
}

// автозапуск превью при смене профиля/режима
profileSel.addEventListener("change", ()=>{ updateAcctTag(); startPreview(); });
headlessChk.addEventListener("change", startPreview);

// ====== кнопка запуска: состояния (ФИКС — единый handler) ======
let pendingRunId = null;
let createdCompanyId = null;
let currentRunId = null; // важен для 2FA submit
let nextAction = 'create'; // 'create' | 'publish'
let publishFinished = false;
let publishRedirectTimer = null;
let autoRestartBudget = 1;

function setRunState(state){
  if (state === 'idle'){
    runBtn.disabled = false;
    runBtn.classList.remove('publish');
    runBtn.classList.add('primary');
    runBtn.textContent = 'Запустить создание';
    nextAction = 'create';
  } else if (state === 'running'){
    runBtn.disabled = true;
    runBtn.classList.remove('publish');
    runBtn.classList.add('primary');
    runBtn.textContent = 'Создание…';
    nextAction = 'create';
  } else if (state === 'publish'){
    runBtn.disabled = false;
    runBtn.classList.remove('primary');
    runBtn.classList.add('publish');
    runBtn.textContent = 'Опубликовать';
    nextAction = 'publish';
  } else if (state === 'publishing'){
    runBtn.disabled = true;
    runBtn.classList.remove('primary');
    runBtn.classList.add('publish');
    runBtn.textContent = 'Публикую…';
    nextAction = 'publish';
  }
}

// ЕДИНСТВЕННЫЙ обработчик клика по кнопке
runBtn.addEventListener('click', ()=>{
  if (runBtn.disabled) return;
  if (nextAction === 'publish') runPublish();
  else runCreate();
});

function redirectToCompany(companyId){
  const cid = Number(companyId || createdCompanyId || 0);
  if (!cid) return false;
  createdCompanyId = cid;
  if (publishRedirectTimer){ clearTimeout(publishRedirectTimer); }
  const targetUrl = '/company/' + encodeURIComponent(cid);
  publishRedirectTimer = setTimeout(()=>{ window.location.assign(targetUrl); }, 400);
  return true;
}

function readLinesFrom(el){
  if (!el) return [];
  return String(el.value || "")
    .split(/\\r?\\n/)
    .map(line => line.trim())
    .filter(Boolean);
}

function readInputValue(el){
  return el ? String(el.value || "").trim() : "";
}

function collectCreativeConfig(){
  const variant = getCurrentVariant();
  if (!variant || String(variant.id || "").toUpperCase() !== "DEMAND_GEN") return null;
  const mode = creativeMode;
  const result = { mode, seed: null, manual: null };
  if (mode === "inspired"){
    const seed = {};
    const seedImages = mixState.images
      .map(item => (typeof item === "object" && item !== null) ? item.value : item)
      .filter(Boolean);
    const seedHeadlines = mixState.headlines.slice().filter(Boolean);
    const seedDescriptions = mixState.descriptions.slice().filter(Boolean);
    const notes = readInputValue($("#mixSeedNotes"));
    if (seedImages.length) seed.images = seedImages;
    if (seedHeadlines.length) seed.headlines = seedHeadlines;
    if (seedDescriptions.length) seed.descriptions = seedDescriptions;
    if (notes) seed.notes = notes;
    result.seed = Object.keys(seed).length ? seed : null;
  } else if (mode === "manual"){
    const images = manualState.images.map(item => (typeof item === "object" && item !== null) ? item.value : item);
    const logos = manualState.logos.map(item => (typeof item === "object" && item !== null) ? item.value : item);
    const headlines = manualState.headlines.slice();
    const descriptions = manualState.descriptions.slice();
    const business = readInputValue($("#creativeManualBusiness"));
    const manual = {
      images,
      headlines,
      descriptions,
    };
    if (logos.length) manual.logos = logos;
    if (business) manual.business_name = business;
    result.manual = manual;
  }
  return result;
}

// ====== запуск создания (шаги 1..9) ======
function runCreate(isRetry = false){
  const pid = String(profileSel.value||"").trim();
  const url = String($("#url").value||"").trim();
  const budgetMinVal = $("#budgetMin").value;
  const budgetMaxVal = $("#budgetMax").value;
  const budgetMin = Number(String(budgetMinVal||"").trim());
  const budgetMax = Number(String(budgetMaxVal||"").trim());
  if(!Number.isFinite(budgetMin) || budgetMin <= 0){
    alert("Укажите минимальный бюджет (число больше 0)");
    return;
  }
  if(!Number.isFinite(budgetMax) || budgetMax <= 0){
    alert("Укажите максимальный бюджет (число больше 0)");
    return;
  }
  if(budgetMax < budgetMin){
    alert("Максимальный бюджет должен быть больше или равен минимальному");
    return;
  }
  const budgetMid = ((budgetMin + budgetMax) / 2).toFixed(2);
  const budget = String(budgetMid);
  const usp = String($("#usp").value||"").trim();
  const variantMeta = getCurrentVariant();
  const variantId = normalizeVariantId(variantMeta ? variantMeta.id : (typeSel ? typeSel.value : ""));
  const type = getChooseType(variantMeta);
  const variantLabel = (variantMeta && variantMeta.label) ? variantMeta.label : (DEFAULT_VARIANT && DEFAULT_VARIANT.label ? DEFAULT_VARIANT.label : "");
  const n_ads = String($("#n_ads").value||"3").trim();
  const locations = String($("#locations").value||"").trim();
  const languages = String($("#languages").value||"").trim();
  const headless = !!headlessChk.checked;
  const creative = collectCreativeConfig();

  if(!pid){ alert("Выберите профиль AdsPower"); return; }
  if(!url || !usp){ alert("URL и УТП обязательны"); return; }
  if (creative && creative.mode === "manual"){
    const manual = creative.manual || {};
    if (!manual.images || !manual.images.length){
      alert("В ручном режиме добавьте хотя бы одно изображение.");
      return;
    }
    if (!manual.headlines || !manual.headlines.length){
      alert("В ручном режиме добавьте минимум один заголовок.");
      return;
    }
    if (!manual.descriptions || !manual.descriptions.length){
      alert("В ручном режиме добавьте минимум одно описание.");
      return;
    }
  }

  publishFinished = false;
  if (publishRedirectTimer){ clearTimeout(publishRedirectTimer); publishRedirectTimer = null; }

  if (!isRetry){
    autoRestartBudget = 1;
  }

  // сбрасываем возможный результат прошлых запусков
  pendingRunId = null;
  createdCompanyId = null;
  currentRunId = null;

  startPreview();

  setBusy(true);
  profileSpan.textContent = pid;
  if (typeSpan && variantLabel) typeSpan.textContent = variantLabel;
  setProgress(0);
  setRunState('running');

  const qs = new URLSearchParams({
    profile_id: pid,
    url,
    budget,
    budget_min: budgetMin.toString(),
    budget_max: budgetMax.toString(),
    usp,
    type,
    variant: variantId,
    variant_label: variantLabel,
    headless: headless? "1":"0",
    n_ads,
  });
  if (locations) qs.set("locations", locations);
  if (languages) qs.set("languages", languages);
  if (creative){
    qs.set("creative_mode", creative.mode || "ai_only");
    if (creative.seed){
      qs.set("creative_seed", JSON.stringify(creative.seed));
    }
    if (creative.manual){
      qs.set("creative_manual", JSON.stringify(creative.manual));
    }
  }

  const es = new EventSource("/api/companies/run?" + qs.toString());
  let total = 0;
  let passed = 0;

  es.onmessage = (ev)=>{
    try{
      const msg = JSON.parse(ev.data||"{}");

      if (msg.event === "start"){
        total = (msg.steps||[]).length || 1;
        currentRunId = msg.run_id || currentRunId;
        enqueueTicker("Готовлю браузер…", 1400, true);
        setProgress(3);

      } else if (msg.event === "info"){
        if (msg.url){ urlSpan.textContent = msg.url; }
        if (msg.stage){ enqueueTicker(msg.stage, 1400); }

      } else if (msg.event === "comment"){
        if (typeof msg.text === "string" && msg.text.trim()){
          enqueueTicker(msg.text, 2000, true);
        }

      } else if (msg.event === "step_start"){
        const label = msg.label || ("Шаг #"+String(msg.number||""));
        enqueueTicker(label + "…", 1400);

      } else if (msg.event === "step_ok"){
        passed += 1;
        const p = Math.round((passed/Math.max(1,total))*100);
        setProgress(Math.min(100, Math.max(5,p)));

      } else if (msg.event === "step_fail"){
        if (msg.after_totp && autoRestartBudget > 0){
          autoRestartBudget -= 1;
          hideCodeModal();
          enqueueTicker("Извините, возникла ошибка, перезапускаем…", 2000, true);
          es.onerror = ()=>{};
          try{ es.close(); }catch(_){ }
          setTimeout(()=> runCreate(true), 1200);
          return;
        }
        enqueueTicker("Шаг упал: " + (msg.error||""), 2200, true);

      } else if (msg.event === "ready"){
        const rid = msg.run_id || currentRunId || null;
        if (rid){
          pendingRunId = rid;
          setRunState('publish');    // ← ПЕРЕКЛЮЧАЕМ КНОПКУ В РЕЖИМ ПУБЛИКАЦИИ
          enqueueTicker("Шаги завершены — можно публиковать", 2200, true);
        }

      } else if (msg.event === "summary"){
        const summary = msg.summary || {};
        if (typeSpan && typeof summary.campaign_variant_label === "string" && summary.campaign_variant_label.trim()){
          typeSpan.textContent = summary.campaign_variant_label.trim();
        } else if (typeSpan && typeof summary.campaign_type === "string" && summary.campaign_type.trim()){
          typeSpan.textContent = summary.campaign_type.trim();
        }
        const summaryMessages = [];
        const pushMessages = (label, reason)=>{
          const labelClean = (label||"").toString().trim();
          const reasonClean = (reason||"").toString().trim();
          if (labelClean) summaryMessages.push(labelClean);
          if (reasonClean) summaryMessages.push(reasonClean);
        };

        if (typeof summary.campaign_objective_reason === "string"){
          const objLabel = typeof summary.campaign_objective_label === "string" ? summary.campaign_objective_label.trim() : "";
          pushMessages(objLabel ? `Маркетинговая цель: ${objLabel}` : "", summary.campaign_objective_reason);
        }
        if (typeof summary.campaign_budget_reason === "string"){
          let budgetAmount = "";
          if (typeof summary.campaign_budget_amount === "number"){
            budgetAmount = summary.campaign_budget_amount.toString();
          } else if (summary.campaign_budget_amount){
            budgetAmount = String(summary.campaign_budget_amount);
          }
          pushMessages(budgetAmount ? `Бюджет: ${budgetAmount}` : "Бюджет обновлён", summary.campaign_budget_reason);
        }
        if (typeof summary.campaign_target_cpa_reason === "string"){
          const cpaEnabled = summary.campaign_target_cpa_enabled === true || summary.campaign_target_cpa_enabled === "true";
          let cpaValue = "";
          if (typeof summary.campaign_target_cpa === "number"){
            cpaValue = summary.campaign_target_cpa.toString();
          } else if (summary.campaign_target_cpa){
            cpaValue = String(summary.campaign_target_cpa);
          }
          pushMessages(cpaEnabled ? (cpaValue ? `Целевой CPA: ${cpaValue}` : "Целевой CPA") : "", summary.campaign_target_cpa_reason);
        }
        if (typeof summary.campaign_goal_reason === "string"){
          const label = typeof summary.campaign_goal_label === "string" ? summary.campaign_goal_label.trim() : "";
          pushMessages(label ? `Цель кампании: ${label}` : "", summary.campaign_goal_reason);
        }

        summaryMessages.forEach(text => enqueueTicker(text, 2200));

      } else if (msg.event === "code_request"){
        const rid = msg.run_id || currentRunId;
        if (rid) currentRunId = rid;
        const mode = msg.mode || "code";
        const prompt = msg.prompt || (mode === "otp_secret" ? "Укажите TOTP secret" : "Введите код подтверждения");
        showCodeModal(prompt, mode);

      } else if (msg.event === "end"){
        es.close(); setBusy(false);
        if (pendingRunId){
          enqueueTicker("Готово — ожидает публикации", 2200, true);
        } else {
          enqueueTicker("Готово", 2200, true);
        }
        setProgress(100);
        // ВАЖНО: превью НЕ выключаем — оставляем живым, чтобы пользователь видел экран
        if (!pendingRunId) setRunState('idle'); // без готового запуска не показываем publish
      }
    }catch(_){}
  };
  es.onerror = ()=>{ try{es.close()}catch(_){ } setBusy(false); enqueueTicker("Потеря соединения", 2200, true); setRunState('idle'); };
}

// ====== ПУБЛИКАЦИЯ (STEP 10) ======
function runPublish(){
  const pid = String(profileSel.value||"").trim();
  const headless = !!headlessChk.checked;
  const publishToken = pendingRunId ? String(pendingRunId).trim() : "";
  const existingId = createdCompanyId ? String(createdCompanyId).trim() : "";

  if(!pid){ alert("Выберите профиль AdsPower"); return; }
  if(!publishToken && !existingId){ alert("Нет готового запуска для публикации"); return; }

  publishFinished = false;
  if (publishRedirectTimer){ clearTimeout(publishRedirectTimer); publishRedirectTimer = null; }

  // если превью выключено (например, после ошибки) — включим
  if (!previewES){ startPreview(); }

  setRunState('publishing');

  setBusy(true);
  enqueueTicker("Публикую…", 1600, true);
  setProgress(0);

  const qs = new URLSearchParams({ profile_id: pid, headless: headless? "1":"0" });
  if (publishToken){
    qs.set("run_id", publishToken);
  } else if (existingId){
    qs.set("company_id", existingId);
  }
  const es = new EventSource("/api/companies/publish?" + qs.toString());

  es.onmessage = (ev)=>{
    try{
      const msg = JSON.parse(ev.data||"{}");
      if (msg.event === "step_start"){ enqueueTicker("Шаг 10 — публикация…", 1500); }
      else if (msg.event === "comment"){ if (msg.text) enqueueTicker(msg.text, 2000, true); }
      else if (msg.event === "step_ok"){ setProgress(80); }
      else if (msg.event === "publish_result"){
        setProgress(100);
        const statusText = msg.published ? "Опубликовано — открываю карточку…" : "Нет подтверждения — проверьте карточку";
        enqueueTicker(statusText, 2000);
        setBusy(false);
        const cid = msg.company_id || createdCompanyId;
        if (cid) createdCompanyId = cid;
        if (msg.published){ pendingRunId = null; }
        const redirected = redirectToCompany(cid);
        publishFinished = redirected;
        if (!redirected){
          setRunState(pendingRunId ? 'publish' : 'idle');
        }
      } else if (msg.event === "code_request"){
        const rid = msg.run_id || currentRunId;
        if (rid) currentRunId = rid;
        const mode = msg.mode || "code";
        const prompt = msg.prompt || (mode === "otp_secret" ? "Укажите TOTP secret" : "Введите код подтверждения");
        showCodeModal(prompt, mode);
      } else if (msg.event === "end"){
        es.close();
        if (!publishFinished){
          setBusy(false);
          setRunState(pendingRunId ? 'publish' : 'idle');
        }
        if (previewES){ try{ previewES.close(); }catch(_){ } previewES = null; }
        stopPull();
      }
    }catch(_){}
  };
  es.onerror = ()=>{ try{ es.close(); }catch(_){ } setBusy(false); setRunState('publish'); };
}

// Инициализируем стейт кнопки
setRunState('idle');

/* ===================== УПРАВЛЕНИЕ ИЗ ПРЕВЬЮ ===================== */
let ctrlActive = false;
controlBtn.addEventListener("click", ()=>{
  ctrlActive = !ctrlActive;
  controlBtn.textContent = ctrlActive ? "Отключить управление" : "Взять управление";
  card.classList.toggle("control-on", ctrlActive);
  if (ctrlActive) card.focus();
});

function coordsFromEvent(e){
  const rect = canvas.getBoundingClientRect();
  const s = Math.min(rect.width/(lastVW||1), rect.height/(lastVH||1));
  const dispW = (lastVW||1)*s;
  const dispH = (lastVH||1)*s;
  const left = rect.left + (rect.width - dispW)/2;
  const top = rect.top + (rect.height - dispH)/2;
  const x = e.clientX - left;
  const y = e.clientY - top;
  if (x < 0 || y < 0 || x > dispW || y > dispH) return null;
  const nx = x / dispW;
  const ny = y / dispH;
  const cssX = Math.round(nx * (lastVW || 1440));
  const cssY = Math.round(ny * (lastVH || 900));
  return { cssX, cssY };
}

async function sendMouse(type, payload){
  const pid = String(profileSel.value||"").trim();
  const headless = !!headlessChk.checked;
  if(!pid) return;
  const body = { profile_id: pid, headless: headless?1:0, type, ...payload };
  await fetch("/api/control/mouse", { method:"POST", headers:{ "Content-Type":"application/json" }, body: JSON.stringify(body) }).catch(()=>{});
}
async function sendText(text){
  const pid = String(profileSel.value||"").trim();
  const headless = !!headlessChk.checked;
  if(!pid) return;
  await fetch("/api/control/text", { method:"POST", headers:{ "Content-Type":"application/json" }, body: JSON.stringify({ profile_id: pid, headless: headless?1:0, text }) }).catch(()=>{});
}
async function sendKey(key){
  const pid = String(profileSel.value||"").trim();
  const headless = !!headlessChk.checked;
  if(!pid) return;
  await fetch("/api/control/key", { method:"POST", headers:{ "Content-Type":"application/json" }, body: JSON.stringify({ profile_id: pid, headless: headless?1:0, key }) }).catch(()=>{});
}

card.addEventListener("click", (e)=>{
  if(!ctrlActive) return;
  const c = coordsFromEvent(e);
  if(!c) return;
  sendMouse("click", { x: c.cssX, y: c.cssY, button: "left", count: 1 });
});

card.addEventListener("wheel", (e)=>{
  if(!ctrlActive) return;
  const c = coordsFromEvent(e);
  if(!c) return;
  sendMouse("wheel", { x: c.cssX, y: c.cssY, deltaX: e.deltaX||0, deltaY: e.deltaY||0 });
});

card.addEventListener("keydown", (e)=>{
  if(!ctrlActive) return;
  const special = ["Enter","Tab","Escape","Backspace","ArrowLeft","ArrowRight","ArrowUp","ArrowDown","Home","End","PageUp","PageDown","Delete"];
  if (special.includes(e.key)){
    e.preventDefault();
    sendKey(e.key);
    return;
  }
  if (e.key && e.key.length === 1 && !e.ctrlKey && !e.metaKey){
    e.preventDefault();
    sendText(e.key);
  }
});

/* ===================== 2FA MODAL (UI + POST) ===================== */
const codeModal = $("#codeModal");
const codeInput = $("#codeInput");
const codeConfirm = $("#codeConfirm");
const codeCancel = $("#codeCancel");
const codeDesc = $("#codeDesc");
let codeMode = "code";

function showCodeModal(desc, mode){
  codeMode = mode || "code";
  if (codeMode === "otp_secret"){
    codeDesc.textContent = desc || "Укажите TOTP secret (base32 или otpauth://...).";
    codeInput.type = "text";
    codeInput.placeholder = "Base32 секрет или otpauth://…";
    codeInput.removeAttribute("inputmode");
    codeInput.removeAttribute("pattern");
    codeConfirm.textContent = "Сохранить секрет";
    enqueueTicker("Требуется TOTP secret для 2FA", 2000, true);
  } else {
    codeDesc.textContent = desc || "Введите код подтверждения";
    codeInput.type = "text";
    codeInput.setAttribute("inputmode", "numeric");
    codeInput.setAttribute("pattern", "[0-9]*");
    codeInput.placeholder = "Код подтверждения";
    codeConfirm.textContent = "Подтвердить";
    enqueueTicker("Требуется подтверждение — введите код", 2000, true);
  }
  codeModal.style.display = 'flex';
  setTimeout(()=>{ codeInput.focus(); codeInput.select(); }, 60);
}
function hideCodeModal(){
  codeModal.style.display = 'none';
  codeInput.value = "";
  codeMode = "code";
}

async function submitCode(){
  const value = String(codeInput.value||"").trim();
  const rid = currentRunId;
  if (!rid){
    hideCodeModal();
    return;
  }
  if (!value){
    if (codeMode === "otp_secret"){
      codeInput.focus();
      return;
    }
    hideCodeModal();
    return;
  }
  try{
    const payload = { run_id: rid };
    if (codeMode === "otp_secret") payload.otp_secret = value; else payload.code = value;
    await fetch("/api/confirm/submit", {
      method: "POST",
      headers: { "Content-Type":"application/json" },
      body: JSON.stringify(payload)
    });
    const modeNow = codeMode;
    hideCodeModal();
    enqueueTicker(modeNow === "otp_secret" ? "Секрет сохранён, продолжаю…" : "Код отправлен, продолжаю…", 2000, true);
  }catch(_){
    hideCodeModal();
  }
}

codeConfirm.addEventListener("click", submitCode);
codeCancel.addEventListener("click", hideCodeModal);
codeInput.addEventListener("keydown", (e)=>{ if(e.key==="Enter"){ e.preventDefault(); submitCode(); }});
document.addEventListener("keydown", (e)=>{ if(e.key==="Escape"){ hideCodeModal(); }});
</script>
</body>
</html>
"""
PAGE_HTML = (
    PAGE_HTML
    .replace("__CAMPAIGN_TYPE_OPTIONS__", _CAMPAIGN_TYPE_OPTIONS_HTML)
    .replace("__CAMPAIGN_VARIANTS_JSON__", _CAMPAIGN_VARIANTS_JSON)
    .replace("__DEFAULT_CAMPAIGN_LABEL__", _DEFAULT_CAMPAIGN_LABEL_HTML)
)


# =============================================================================
#                                РЕГИСТРАЦИЯ РОУТОВ
# =============================================================================

def init_create_companies(app: Flask, settings: Settings) -> None:
    """
    Регистрирует:
      • /companies — UI
      • /api/adspower/profiles — список профилей AdsPower (поиск)
      • /api/preview — SSE превью (частые кадры, JPEG-приоритет, + мета vw/vh)
      • /api/status — статус драйвера (по профилю)
      • /api/companies/run — SSE шагов (выполняет все stepN по порядку)
      • /api/companies/publish — SSE публикации (только step10)
      • /api/companies — список сохранённых компаний
      • /api/companies/<id> — карточка компании (JSON, расширено google_tag)
      • /api/control/* — ручное управление превью
      • /api/confirm/submit — передача 2FA‑кода от пользователя
    """
    _db_init()  # гарантируем наличие БД

    # ====== локальный кэш карт profile_id -> google_email из campaigns.db (для UI списка профилей) ======
    _acc_cache: Dict[str, Any] = {
        "ts": 0.0,
        "map": {},
        "path": "",
        "email": "",
        "email_norm": "",
        "groups": [],
    }
    _run_meta: Dict[str, Dict[str, Any]] = {}
    _run_meta_lock = threading.Lock()

    def _normalize_otp_secret(value: Optional[str]) -> Optional[str]:
        if _cf_normalize_totp_secret:
            try:
                return _cf_normalize_totp_secret(value)
            except Exception:
                pass
        if value is None:
            return None
        raw = str(value).strip()
        if not raw:
            return None
        if raw.lower().startswith("otpauth://"):
            return raw
        secret_part, suffix = raw, ""
        if "|" in raw:
            secret_part, suffix = raw.split("|", 1)
        clean_secret = re.sub(r"[^A-Z2-7]", "", secret_part.upper())
        if not clean_secret:
            return None
        suffix_tokens = []
        if suffix:
            for token in re.split(r"[;,]", suffix):
                token = token.strip()
                if token:
                    suffix_tokens.append(token)
        if suffix_tokens:
            return clean_secret + "|" + ",".join(suffix_tokens)
        return clean_secret

    def _parse_totp_config(value: Optional[str]) -> Optional[Tuple[bytes, int, int]]:
        if value is None:
            return None
        raw = str(value).strip()
        if not raw:
            return None
        digits = 6
        period = 30
        secret_str = raw
        if raw.lower().startswith("otpauth://"):
            try:
                parsed = urllib.parse.urlparse(raw)
                params = urllib.parse.parse_qs(parsed.query or "")
                secret_param = (params.get("secret") or [""])[0].strip()
                if not secret_param:
                    return None
                secret_clean = re.sub(r"[^A-Z2-7]", "", secret_param.upper())
                if not secret_clean:
                    return None
                secret_str = secret_clean
                if "digits" in params:
                    try:
                        digits = int(params["digits"][0])
                    except Exception:
                        digits = 6
                if "period" in params:
                    try:
                        period = int(params["period"][0])
                    except Exception:
                        period = 30
            except Exception:
                return None
        else:
            suffix = ""
            if "|" in raw:
                secret_str, suffix = raw.split("|", 1)
            secret_clean = re.sub(r"[^A-Z2-7]", "", secret_str.upper())
            if not secret_clean:
                return None
            secret_str = secret_clean
            digits_set = False
            period_set = False
            if suffix:
                for token in re.split(r"[;,]", suffix):
                    token = token.strip()
                    if not token:
                        continue
                    low = token.lower()
                    if low.startswith("digits="):
                        try:
                            digits = int(low.split("=", 1)[1])
                            digits_set = True
                        except Exception:
                            continue
                    elif low.startswith("period=") or low.startswith("step=") or low.startswith("interval="):
                        try:
                            period = int(low.split("=", 1)[1])
                            period_set = True
                        except Exception:
                            continue
                    elif low.startswith("t="):
                        try:
                            period = int(low.split("=", 1)[1])
                            period_set = True
                        except Exception:
                            continue
                    elif low.isdigit():
                        try:
                            val = int(low)
                        except Exception:
                            continue
                        if not digits_set:
                            digits = val
                            digits_set = True
                        elif not period_set:
                            period = val
                            period_set = True
            # no suffix means defaults
        if digits not in (6, 7, 8):
            digits = 6
        if period <= 0:
            period = 30
        pad = (-len(secret_str)) % 8
        padded = secret_str + ("=" * pad)
        try:
            secret_bytes = base64.b32decode(padded, casefold=True)
        except Exception:
            return None
        if not secret_bytes:
            return None
        return secret_bytes, digits, period

    def _generate_totp_code(secret: Optional[str], now: Optional[float] = None) -> Optional[str]:
        if _cf_generate_totp_code:
            try:
                return _cf_generate_totp_code(secret, now=now)
            except Exception:
                pass
        cfg = _parse_totp_config(secret)
        if not cfg:
            return None
        key, digits, period = cfg
        ts = time.time() if now is None else float(now)
        counter = int(ts // period)
        msg = counter.to_bytes(8, "big")
        digest = hmac.new(key, msg, hashlib.sha1).digest()
        offset = digest[-1] & 0x0F
        code_int = int.from_bytes(digest[offset:offset + 4], "big") & 0x7FFFFFFF
        token = code_int % (10 ** digits)
        return str(token).zfill(digits)

    def _run_meta_set(run_id: str, info: Dict[str, Any]) -> None:
        with _run_meta_lock:
            _run_meta[run_id] = dict(info)

    def _run_meta_get(run_id: str) -> Optional[Dict[str, Any]]:
        with _run_meta_lock:
            meta = _run_meta.get(run_id)
            return dict(meta) if meta else None

    def _run_meta_update(run_id: str, **fields: Any) -> None:
        if not fields:
            return
        with _run_meta_lock:
            meta = _run_meta.get(run_id)
            if not meta:
                return
            meta.update({k: v for k, v in fields.items() if v is not None})

    def _run_meta_clear(run_id: str) -> None:
        with _run_meta_lock:
            _run_meta.pop(run_id, None)

    def _accounts_store_otp_secret(user_email: str, profile_id: str, otp_secret: str) -> None:
        normalized = _normalize_otp_secret(otp_secret)
        if not normalized:
            return
        path = _discover_campaigns_db()
        if not path or not os.path.exists(path):
            return
        conn: Optional[sqlite3.Connection] = None
        try:
            conn = sqlite3.connect(path, check_same_thread=False)
            with conn:
                conn.execute(
                    "UPDATE accounts SET otp_secret = ?, updated_at = ? WHERE user_email = ? AND profile_id = ?",
                    (normalized, time.time(), user_email, profile_id),
                )
        except Exception:
            pass
        finally:
            if conn:
                conn.close()
        if _acc_cache.get("email") == user_email:
            cached = _acc_cache["map"].get(profile_id)
            if cached is not None:
                cached["otp_secret"] = normalized

    def _install_wait_code(wait_fn: Callable[[float], Optional[str]]) -> None:
        try:
            conf_mod = importlib.import_module("examples.steps.code_for_confrim")
            setattr(conf_mod, "wait_code_from_env_or_file", wait_fn)
        except Exception:
            pass
        for name in ("examples.steps.step3", "examples.steps.step4", "examples.steps.step5"):
            try:
                mod = importlib.import_module(name)
                if hasattr(mod, "wait_code_from_env_or_file"):
                    setattr(mod, "wait_code_from_env_or_file", wait_fn)
            except Exception:
                continue

    def _build_wait_code_fn(
        run_id_local: str,
        *,
        user_email: str,
        profile_id: str,
        account_meta: Optional[Dict[str, Any]],
        context: Dict[str, Any],
    ) -> Callable[[float], Optional[str]]:
        meta = account_meta or {}
        otp_holder: Dict[str, Optional[str]] = {"secret": _normalize_otp_secret(meta.get("otp_secret"))}

        def _queue_event(evq: Any, payload: Dict[str, Any]) -> None:
            if not isinstance(evq, queue.Queue):
                return
            try:
                evq.put(payload)
            except Exception:
                pass

        def _wait(timeout_total: float = 180.0, prompt: str = "Введите код подтверждения") -> Optional[str]:
            deadline = time.time() + max(5.0, float(timeout_total))
            now = time.time()
            context["_totp_used"] = True
            context["_totp_last_time"] = now
            if context.get("_active_step") is not None:
                context["_totp_last_step"] = context.get("_active_step")
            _run_meta_update(run_id_local, last_totp_request=now)

            evq = getattr(_CODE_CTX, "evq", None)

            otp_secret = otp_holder["secret"]
            if not otp_secret:
                _queue_event(evq, {
                    "event": "code_request",
                    "run_id": run_id_local,
                    "prompt": "Укажите TOTP secret (он сохранится для профиля)",
                    "mode": "otp_secret",
                })
                remaining = max(1.0, deadline - time.time())
                value = _broker_pop_code(run_id_local, timeout=remaining)
                if not value:
                    return None
                value = str(value).strip()
                normalized = _normalize_otp_secret(value)
                if normalized:
                    otp_holder["secret"] = normalized
                    meta["otp_secret"] = normalized
                    _accounts_store_otp_secret(user_email, profile_id, normalized)
                    _run_meta_update(run_id_local, otp_secret=normalized)
                    otp_secret = normalized
                    _queue_event(evq, {"event": "comment", "text": "TOTP secret сохранён, генерирую код…"})
                else:
                    return value

            if otp_secret:
                _queue_event(evq, {"event": "comment", "text": "Генерирую код подтверждения через TOTP…"})
                code = _generate_totp_code(otp_secret)
                if code:
                    context["_totp_generated"] = True
                    ts = time.time()
                    context["_totp_last_time"] = ts
                    _run_meta_update(run_id_local, last_totp_success=ts)
                    return code
                context["_totp_failed"] = True
                _queue_event(evq, {"event": "comment", "text": "Не удалось сгенерировать код — прошу ввести вручную."})

            remaining = max(1.0, deadline - time.time())
            _queue_event(evq, {"event": "code_request", "run_id": run_id_local, "prompt": prompt, "mode": "code"})
            return _broker_pop_code(run_id_local, timeout=remaining)

        return _wait

    def _discover_campaigns_db() -> Optional[str]:
        p = (os.getenv("ADS_AI_CAMPAIGNS_DB") or "").strip()
        if p and os.path.exists(p):
            return p
        try:
            camp = importlib.import_module("ads_ai.web.camping")
            rp = getattr(camp, "_resolve_paths", None)
            if callable(rp):
                paths_obj = rp(settings)  # type: ignore
                dbf = getattr(paths_obj, "db_file", None)
                if dbf and os.path.exists(str(dbf)):
                    return str(dbf)
        except Exception:
            pass
        guess = os.path.join(os.getcwd(), "artifacts", "campaigns.db")
        return guess if os.path.exists(guess) else None

    def _read_user_groups(conn: Optional[sqlite3.Connection], email_norm: str) -> List[str]:
        if not conn or not email_norm:
            return []
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT group_ids FROM user_adspower_groups WHERE user_email=? LIMIT 1",
                (email_norm,),
            )
            row = cur.fetchone()
            if not row:
                return []
            raw_val = row["group_ids"] if isinstance(row, sqlite3.Row) else row[0]
            if not raw_val:
                return []
            data = json.loads(raw_val)
            if not isinstance(data, list):
                return []
            groups: List[str] = []
            seen: set[str] = set()
            for item in data:
                val = str(item or "").strip()
                if not val or val in seen:
                    continue
                seen.add(val)
                groups.append(val)
            return groups
        except Exception:
            return []

    def _accounts_index_cached(user_email: str) -> Dict[str, Dict[str, Any]]:
        """Карта profile_id -> {'google_email','google_name','id','name','created_at'} для пользователя (кэш 5с)."""
        path = _discover_campaigns_db()
        now = time.time()
        email_norm = str(user_email or "").strip().lower()
        if (
            _acc_cache["map"]
            and _acc_cache["path"] == (path or "")
            and _acc_cache.get("email_norm") == email_norm
            and (now - float(_acc_cache["ts"])) < 5.0
        ):
            return dict(_acc_cache["map"])

        index: Dict[str, Dict[str, Any]] = {}
        groups: List[str] = []
        if not path or not os.path.exists(path):
            _acc_cache.update({
                "ts": now,
                "map": index,
                "path": path or "",
                "email": user_email,
                "email_norm": email_norm,
                "groups": groups,
            })
            return index

        conn = None
        try:
            conn = sqlite3.connect(path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='accounts'")
            if not cur.fetchone():
                groups = _read_user_groups(conn, email_norm)
                _acc_cache.update({
                    "ts": now,
                    "map": index,
                    "path": path or "",
                    "email": user_email,
                    "email_norm": email_norm,
                    "groups": groups,
                })
                return index

            rows = conn.execute("SELECT * FROM accounts WHERE user_email = ?", (user_email,)).fetchall()
            for r in rows:
                row = dict(r)
                pid = str(row.get("profile_id") or "").strip()
                if not pid:
                    continue
                created_raw = row.get("created_at") or row.get("ts") or row.get("updated_at") or 0
                try:
                    created = float(created_raw)
                except Exception:
                    created = 0.0
                google_email = ""
                for k in (
                    "google_email",
                    "email",
                    "login",
                    "email_address",
                    "gmail",
                    "ga_email",
                    "account_email",
                ):
                    v = row.get(k)
                    if v:
                        google_email = str(v).strip()
                        break
                if not google_email:
                    name = str(row.get("name") or "")
                    if re.search(r"[\w\.\+\-]+@[\w\.\-]+\.\w+", name or ""):
                        google_email = name
                    else:
                        google_email = name
                if not _is_supported_google_email(google_email):
                    google_email = _NO_ACCOUNT_EMAIL
                google_name = str(row.get("google_name") or "").strip()
                if not google_name:
                    google_name = str(row.get("name") or "").strip()
                prev = index.get(pid)
                if (prev is None) or (created >= float(prev.get("created_at") or 0)):
                    index[pid] = {
                        "email": google_email,
                        "google_email": google_email,
                        "google_name": google_name,
                        "id": row.get("id"),
                        "name": row.get("name"),
                        "profile_id": pid,
                        "created_at": created,
                        "otp_secret": _normalize_otp_secret(row.get("otp_secret")),
                    }
            groups = _read_user_groups(conn, email_norm)
        except Exception:
            index = {}
            groups = []
        finally:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass

        _acc_cache.update({
            "ts": now,
            "map": index,
            "path": path or "",
            "email": user_email,
            "email_norm": email_norm,
            "groups": groups,
        })
        return index

    def _user_selected_groups(user_email: str) -> List[str]:
        email_norm = str(user_email or "").strip().lower()
        if not email_norm:
            return []
        cached_norm = str(_acc_cache.get("email_norm") or "")
        if cached_norm == email_norm:
            cached_groups = _acc_cache.get("groups")
            if isinstance(cached_groups, list):
                return [str(g) for g in cached_groups]
        _accounts_index_cached(user_email)
        cached_norm = str(_acc_cache.get("email_norm") or "")
        if cached_norm == email_norm:
            cached_groups = _acc_cache.get("groups")
            if isinstance(cached_groups, list):
                return [str(g) for g in cached_groups]
        return []

    def _user_profile_map(user_email: str) -> Dict[str, Dict[str, Any]]:
        return _accounts_index_cached(user_email)

    def _profile_allowed(user_email: str, profile_id: str) -> bool:
        profile_id = str(profile_id or "").strip()
        if not profile_id:
            return False
        return profile_id in _user_profile_map(user_email)

    @app.post("/api/manual-assets/upload")
    def api_manual_assets_upload() -> Response:
        try:
            email = _require_user_email()
        except PermissionError:
            return jsonify({"ok": False, "error": "unauthorized"}), 401

        file = request.files.get("file")
        if file is None or not file.filename:
            logger.warning("Manual upload rejected: missing_file")
            return jsonify({"ok": False, "error": "missing_file"}), 400

        filename = secure_filename(file.filename) or f"asset-{int(time.time()*1000)}"
        ext = os.path.splitext(filename)[1].lower()
        allowed_ext = {".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp", ".jfif", ".tif", ".tiff", ".avif", ".heic", ".heif"}
        mimetype_guess = ""
        if file.mimetype:
            mimetype_guess = file.mimetype.split("/")[-1].lower()
        if ext not in allowed_ext and mimetype_guess:
            mapped = {
                "jpeg": ".jpg",
                "pjpeg": ".jpg",
                "png": ".png",
                "gif": ".gif",
                "bmp": ".bmp",
                "webp": ".webp",
                "tiff": ".tiff",
                "x-tiff": ".tiff",
                "avif": ".avif",
            }.get(mimetype_guess)
            if mapped:
                ext = mapped
                filename = f"{os.path.splitext(filename)[0]}{ext}"
        if ext not in allowed_ext:
            logger.warning("Manual upload rejected: unsupported_type | file=%s mime=%s", filename, file.mimetype)
            return jsonify({"ok": False, "error": "unsupported_type"}), 400

        data = file.read()
        if not data:
            logger.warning("Manual upload rejected: empty_file | file=%s", filename)
            return jsonify({"ok": False, "error": "empty_file"}), 400
        if len(data) > _MANUAL_UPLOAD_MAX_BYTES:
            logger.warning(
                "Manual upload rejected: too_large | file=%s size=%s limit=%s",
                filename,
                len(data),
                _MANUAL_UPLOAD_MAX_BYTES,
            )
            return jsonify({"ok": False, "error": "too_large", "max_bytes": _MANUAL_UPLOAD_MAX_BYTES}), 413

        target_dir = _manual_upload_dir(email)
        unique_name = f"{int(time.time()*1000)}-{uuid.uuid4().hex[:6]}{ext}"
        target_path = os.path.join(target_dir, unique_name)
        try:
            with open(target_path, "wb") as fh:
                fh.write(data)
        except Exception as exc:
            logger.exception("Manual upload write_failed: file=%s err=%s", filename, exc)
            return jsonify({"ok": False, "error": f"write_failed: {exc}"}), 500

        return jsonify({"ok": True, "path": target_path, "name": unique_name, "size": len(data)})

    # ——————————————————— UI ———————————————————
    @app.route("/companies", methods=["GET"])
    @app.route("/companies/new", methods=["GET"])
    def companies_page() -> Response:
        return make_response(PAGE_HTML)

    # ——————————————————— AdsPower: список профилей ———————————————————
    @app.get("/api/adspower/profiles")
    def api_adspower_profiles() -> Response:
        q = (request.args.get("q") or "").strip()
        try:
            email = _require_user_email()
        except PermissionError:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        try:
            acc_map = _user_profile_map(email)
            selected_groups = _user_selected_groups(email)
            if not acc_map or not selected_groups:
                return jsonify({"ok": True, "items": [], "total": 0})

            raw = _list_adspower_profiles(q="", page=1, page_size=300, group_ids=selected_groups)
            ap_items = {str(it.get("profile_id") or ""): it for it in raw.get("items") or []}

            ql = q.lower()
            items: List[Dict[str, Any]] = []
            for pid, meta in acc_map.items():
                google_email = (meta.get("google_email") or meta.get("email") or "").strip()
                if (_is_no_account_value(google_email)
                        or not _is_supported_google_email(google_email)):
                    continue
                ap = ap_items.get(pid)
                if not ap:
                    continue
                name = ap.get("name") or meta.get("name") or ""
                google_name = (meta.get("google_name") or meta.get("name") or "").strip()
                record = {
                    "profile_id": pid,
                    "name": name,
                    "group_id": ap.get("group_id") or "",
                    "tags": ap.get("tags") or [],
                    "google_email": google_email,
                    "google_name": google_name,
                    "account_id": meta.get("id"),
                    "created_at": meta.get("created_at"),
                }
                if ql:
                    haystack = " ".join([
                        pid.lower(),
                        str(name).lower(),
                        str(record["google_email"]).lower(),
                        str(record["google_name"] or "").lower(),
                        str(record["group_id"]).lower(),
                    ])
                    if ql not in haystack:
                        continue
                items.append(record)

            return jsonify({"ok": True, "items": items, "total": len(items)})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    # ——————————————————— Статус (по профилю) ———————————————————
    @app.get("/api/status")
    def api_status() -> Response:
        try:
            email = _require_user_email()
        except PermissionError:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        pid = (request.args.get("profile_id") or "").strip()
        headless = (request.args.get("headless") or "").strip() in ("1", "true", "yes", "on")
        if not pid:
            if _local.profile_id and _local.user_email == email:
                pid = _local.profile_id
            else:
                return jsonify({"ok": True, "url": "", "title": "", "profile_id": ""})
        if not _profile_allowed(email, pid):
            return jsonify({"ok": False, "error": "forbidden"}), 403
        drv = _maybe_get_driver(pid, headless=headless, user_email=email)
        url = ""
        title = ""
        try:
            if drv:
                _ensure_big_viewport(drv)
            url = getattr(drv, "current_url", "") or ""
            title = getattr(drv, "title", "") or ""
        except Exception:
            pass
        return jsonify({"ok": True, "url": url, "title": title, "profile_id": pid})

    # ——————————————————— Превью (SSE) ———————————————————
    @app.route("/api/preview", methods=["GET"])
    def api_preview() -> Response:
        """
        Стримим частые кадры (стремимся к 10–15 fps).
        Параметры: profile_id (обязательно), headless=1|0
        Отправляем также мета: vw, vh, dpr, fmt — для маппинга кликов и отрисовки.
        Важно: этот эндпоинт НИКОГДА сам не поднимает браузер — только «подключается»,
        если драйвер уже существует.
        """
        try:
            email = _require_user_email()
        except PermissionError:
            return Response("unauthorized\n", status=401, mimetype="text/plain")
        pid = (request.args.get("profile_id") or "").strip()
        headless = (request.args.get("headless") or "").strip() in ("1", "true", "yes", "on")

        if not pid:
            def gen_empty():
                while True:
                    yield ":hb\n\n"; time.sleep(0.8)
            return Response(stream_with_context(gen_empty()), mimetype="text/event-stream")

        if not _profile_allowed(email, pid):
            return Response("forbidden\n", status=403, mimetype="text/plain")

        def gen():
            yield "retry: 600\n\n"
            while True:
                try:
                    drv = _maybe_get_driver(pid, headless=headless, user_email=email)  # ← не создаём!
                    if not drv:
                        yield ":hb\n\n"
                        time.sleep(0.25)
                        continue
                    _ensure_big_viewport(drv)
                    frame = _preview.capture_frame(drv, headless=headless)
                    if frame:
                        payload = json.dumps(frame)
                        yield "event: image\n"
                        yield f"data: {payload}\n\n"
                    else:
                        yield ":hb\n\n"
                    time.sleep(0.09)  # ~11 fps
                except GeneratorExit:
                    break
                except Exception:
                    time.sleep(0.15)
        resp = Response(stream_with_context(gen()), mimetype="text/event-stream")
        resp.headers["Cache-Control"] = "no-cache, no-transform"
        resp.headers["X-Accel-Buffering"] = "no"
        return resp

    # ——————————————————— Запуск (SSE) ———————————————————
    @app.route("/api/companies/run", methods=["GET"])
    def companies_run_stream() -> Response:
        """
        Исполняет ТОЛЬКО шаги 1..9 (step<N>, N<10) на выбранном профиле.
        Параметры (query): profile_id*, url*, budget*, usp*, locations, languages, n_ads, headless,
        variant?, type?
        ВАЖНО: драйвер ПОСЛЕ запуска НЕ закрывается автоматически — для шага публикации.
        """
        profile_id = (request.args.get("profile_id") or "").strip()
        url = (request.args.get("url") or "").strip()
        budget = (request.args.get("budget") or "").strip()
        budget_min = (request.args.get("budget_min") or "").strip()
        budget_max = (request.args.get("budget_max") or "").strip()
        usp = (request.args.get("usp") or "").strip()
        variant_id_raw = (request.args.get("variant") or "").strip()
        variant_label_raw = (request.args.get("variant_label") or "").strip()
        choose_type_raw = (request.args.get("type") or "").strip()
        variant_conf = _resolve_campaign_variant(variant_id=variant_id_raw, choose_type=choose_type_raw)
        steps_package = variant_conf.steps_package
        variant_id = variant_conf.variant_id
        variant_label = variant_label_raw or variant_conf.label
        choose_type = (choose_type_raw or variant_conf.choose_type or "").strip().upper() or variant_conf.choose_type
        n_ads = (request.args.get("n_ads") or "").strip()
        locations = _normalize_multi(request.args.get("locations", ""))
        languages = _normalize_multi(request.args.get("languages", ""))
        headless = (request.args.get("headless") or "").strip() in ("1", "true", "yes", "on")
        creative_mode_raw = (request.args.get("creative_mode") or "").strip().lower()
        creative_seed_raw = request.args.get("creative_seed") or ""
        creative_manual_raw = request.args.get("creative_manual") or ""

        def _parse_json_arg(raw: str) -> Any:
            if not raw:
                return None
            try:
                return json.loads(raw)
            except Exception:
                return None

        def _ensure_str_list(val: Any) -> List[str]:
            result: List[str] = []
            if isinstance(val, (list, tuple, set)):
                for item in val:
                    text = str(item or "").strip()
                    if text:
                        result.append(text)
            elif isinstance(val, str):
                text = val.strip()
                if text:
                    result.append(text)
            return result

        def _sanitize_seed_payload(payload: Any) -> Optional[Dict[str, Any]]:
            if not isinstance(payload, dict):
                return None
            result: Dict[str, Any] = {}
            for key in ("images", "headlines", "descriptions", "image_prompts"):
                items = _ensure_str_list(payload.get(key))
                if items:
                    result[key] = items
            note = payload.get("notes")
            if isinstance(note, str) and note.strip():
                result["notes"] = note.strip()
            return result or None

        def _sanitize_manual_payload(payload: Any) -> Optional[Dict[str, Any]]:
            if not isinstance(payload, dict):
                return None
            result: Dict[str, Any] = {
                "images": _ensure_str_list(payload.get("images")),
                "headlines": _ensure_str_list(payload.get("headlines")),
                "descriptions": _ensure_str_list(payload.get("descriptions")),
            }
            logos = _ensure_str_list(payload.get("logos"))
            if logos:
                result["logos"] = logos
            business = payload.get("business_name")
            if isinstance(business, str) and business.strip():
                result["business_name"] = business.strip()
            return result

        def _normalize_creative_mode(val: Optional[str]) -> Optional[str]:
            value = str(val or "").strip().lower()
            if value in {"ai_only", "inspired", "manual"}:
                return value
            return None

        is_demand_gen_variant = variant_id.upper() == "DEMAND_GEN"
        creative_mode_norm = _normalize_creative_mode(creative_mode_raw)
        if is_demand_gen_variant:
            creative_mode_norm = creative_mode_norm or "ai_only"
        creative_seed_payload = _sanitize_seed_payload(_parse_json_arg(creative_seed_raw)) if creative_mode_norm in {"inspired", "manual"} else None
        creative_manual_payload = _sanitize_manual_payload(_parse_json_arg(creative_manual_raw)) if creative_mode_norm == "manual" else None
        if creative_mode_norm not in {"inspired"}:
            creative_seed_payload = None
        if creative_mode_norm not in {"manual"}:
            creative_manual_payload = None

        if not profile_id or not url or not usp or not budget_min or not budget_max:
            msg = {"event": "error", "error": "Fields 'profile_id', 'url', 'budget_min', 'budget_max', 'usp' are required"}
            return Response("data: " + json.dumps(msg, ensure_ascii=False) + "\n\n",
                            mimetype="text/event-stream", status=400)

        try:
            user_email = _require_user_email()
        except PermissionError:
            msg = {"event": "error", "error": "unauthorized"}
            return Response("data: " + json.dumps(msg, ensure_ascii=False) + "\n\n",
                            mimetype="text/event-stream", status=401)

        if not _profile_allowed(user_email, profile_id):
            msg = {"event": "error", "error": "forbidden"}
            return Response("data: " + json.dumps(msg, ensure_ascii=False) + "\n\n",
                            mimetype="text/event-stream", status=403)

        try:
            n_ads_int = int(n_ads) if n_ads else 3
            if n_ads_int < 1:
                n_ads_int = 1
            if n_ads_int > 50:
                n_ads_int = 50
        except Exception:
            n_ads_int = 3

        cli_inputs: Dict[str, Any] = {
            "budget": budget,
            "budget_min": budget_min or None,
            "budget_max": budget_max or None,
            "url": url,
            "usp": usp,
            "type": choose_type,
            "variant": variant_id,
            "campaign_variant_label": variant_label,
            "locations": locations if locations else None,
            "languages": languages if languages else None,
            "n_ads": n_ads_int,
        }
        if creative_mode_norm:
            cli_inputs["creative_mode"] = creative_mode_norm
        if creative_seed_payload:
            cli_inputs["creative_seed_assets"] = creative_seed_payload
        if creative_manual_payload:
            cli_inputs["creative_provided_assets"] = creative_manual_payload

        def _yield(data: Dict[str, Any]) -> str:
            try:
                return "data: " + json.dumps(data, ensure_ascii=False) + "\n\n"
            except Exception:
                return "data: {\"event\":\"error\",\"error\":\"serialization_failed\"}\n\n"

        account_meta_raw = _user_profile_map(user_email).get(profile_id, {}) or {}
        account_meta = dict(account_meta_raw)

        # === run_id для текущего запуска (нужен для 2FA-модалки) ===
        run_id = f"run-{int(time.time()*1000)}-{os.getpid()}-{threading.get_ident()}"

        _run_meta_set(run_id, {
            "user_email": user_email,
            "profile_id": profile_id,
            "otp_secret": (account_meta or {}).get("otp_secret"),
        })

        def generate() -> Any:
            # Принудительно перезапускаем драйвер — предыдущие сессии могли оставить его в странном состоянии.
            _shutdown_driver("restart_before_run")

            # Гарантируем драйвер
            try:
                drv = _get_or_create_driver(profile_id, headless=headless, user_email=user_email)
                _ensure_big_viewport(drv)
                yield _yield({"event": "info", "stage": "Готовлю браузер…", "url": getattr(drv, "current_url", "") or ""})
            except Exception as e:
                yield _yield({"event": "error", "error": f"driver_failed: {e!r}"})
                yield _yield({"event": "end"})
                return

            # Поиск шагов
            try:
                steps_all = _discover_steps(steps_package)
                # Для классических кампаний (PMAX и др.) выполняем только подготовительные шаги (<10),
                # поскольку step10 отвечает за публикацию и запускается отдельным эндпоинтом.
                # Для Demand Gen нужен полный пайплайн (шаги 10–13), поэтому ничего не отфильтровываем.
                variant_upper = (variant_conf.variant_id or "").strip().upper()
                if variant_upper == "DEMAND_GEN":
                    steps = steps_all
                else:
                    steps = [s for s in steps_all if int(s.number) < 10]
                if not steps:
                    raise RuntimeError("Не найдены шаги для запуска (проверьте нумерацию step<N>)")
                steps_meta_run = [{"number": s.number, "module": s.module_name, "runner": s.runner_name, "label": s.label} for s in steps]
                steps_descr = ", ".join(f"{spec['number']}:{spec['module']}" for spec in steps_meta_run)
                yield _yield({"event": "comment", "text": f"Steps package: {steps_package}; найдено шагов: {len(steps_meta_run)} ({steps_descr})"})
            except Exception as e:
                yield _yield({"event": "error", "error": f"discover_failed: {e!r}"})
                yield _yield({"event": "end"})
                _run_meta_clear(run_id)
                return

            # старт + run_id для UI
            yield _yield({"event": "start", "steps": steps_meta_run, "run_id": run_id})

            # Общий контекст
            context: Dict[str, Any] = {}
            context["_totp_used"] = False
            context["campaign_variant"] = variant_id
            context["campaign_variant_label"] = variant_label
            context.setdefault("campaign_type", choose_type)
            context["steps_package"] = steps_package
            if budget_min:
                context["budget_min"] = budget_min
            if budget_max:
                context["budget_max"] = budget_max
            if locations:
                context["locations"] = locations
            if languages:
                context["languages"] = languages
            if creative_mode_norm:
                context["creative_mode"] = creative_mode_norm
            if creative_seed_payload:
                context["creative_seed_assets"] = creative_seed_payload
            if creative_manual_payload:
                context["creative_provided_assets"] = creative_manual_payload

            wait_fn = _build_wait_code_fn(
                run_id,
                user_email=user_email,
                profile_id=profile_id,
                account_meta=account_meta,
                context=context,
            )
            _install_wait_code(wait_fn)

            # Лок на driver (если консоль тоже может работать с ним)
            ext_state = _get_state_from_app()
            ext_lock = getattr(ext_state, "lock", None)
            primary_lock = _local.lock
            secondary_lock = ext_lock if ext_lock and ext_lock is not primary_lock else None

            all_ok = True
            steps_results: List[Dict[str, Any]] = []

            # заранее создадим слот ожидания кода
            _broker_get_or_create(run_id)

            try:
                for spec in steps:
                    yield _yield({"event": "step_start", "number": spec.number, "module": spec.module_name, "label": spec.label})
                    context["_active_step"] = spec.number

                    evq: "queue.Queue[Dict[str, Any]]" = queue.Queue()
                    done = threading.Event()
                    result_holder: Dict[str, Any] = {}

                    def emit(msg: str) -> None:
                        if isinstance(msg, str) and msg.strip():
                            evq.put({"event": "comment", "text": msg.strip(), "number": spec.number})

                    def worker():
                        setattr(_CODE_CTX, "run_id", run_id)
                        setattr(_CODE_CTX, "evq", evq)
                        try:
                            res = _call_step_with_injected_kwargs(spec, drv, cli_inputs, context, emit_cb=emit)
                            result_holder["ok"] = True
                            result_holder["res"] = res
                        except Exception as e:
                            result_holder["ok"] = False
                            result_holder["err"] = repr(e)
                        finally:
                            try:
                                delattr(_CODE_CTX, "run_id")
                                delattr(_CODE_CTX, "evq")
                            except Exception:
                                pass
                            done.set()

                    primary_acquired = False
                    secondary_acquired = False
                    secondary_warned = False
                    wait_until = time.time() + 45.0
                    while True:
                        primary_acquired = primary_lock.acquire(timeout=3.0)
                        if primary_acquired:
                            break
                        if time.time() >= wait_until:
                            break
                        yield _yield({"event": "comment", "text": "Драйвер занят другой операцией, ожидаю освобождения…"})

                    if not primary_acquired:
                        steps_results.append({"step": spec.number, "module": spec.module_name, "error": "driver_busy", "ok": False})
                        yield _yield({"event": "step_fail", "number": spec.number, "error": "driver_busy"})
                        all_ok = False
                        break

                    if secondary_lock:
                        try:
                            secondary_acquired = secondary_lock.acquire(timeout=0.5)
                        except Exception:
                            secondary_acquired = False
                        if not secondary_acquired and not secondary_warned:
                            yield _yield({"event": "comment", "text": "Консоль всё ещё читает экран — продолжаю без её блокировки."})
                            secondary_warned = True

                    try:
                        t = threading.Thread(target=worker, name=f"step{spec.number}-worker", daemon=True)
                        t.start()

                        while not done.is_set():
                            try:
                                evt = evq.get(timeout=0.15)
                                yield _yield(evt)
                            except queue.Empty:
                                pass
                            _ensure_big_viewport(drv)

                        while True:
                            try:
                                evt = evq.get_nowait()
                                yield _yield(evt)
                            except queue.Empty:
                                break

                        if result_holder.get("ok"):
                            res = result_holder.get("res")
                            steps_results.append({"step": spec.number, "module": spec.module_name, "result": res, "ok": True})
                            if isinstance(res, dict):
                                if isinstance(res.get("comment"), str):
                                    yield _yield({"event":"comment","number":spec.number,"text":res["comment"]})
                                if isinstance(res.get("comments"), list):
                                    for c in res["comments"]:
                                        if isinstance(c, str) and c.strip():
                                            yield _yield({"event":"comment","number":spec.number,"text":c.strip()})
                            _update_context_from_result(spec.number, res, context)

                            # Охранник после шага: вернуться в Ads при 2FA/reauth
                            try:
                                cur_url = getattr(drv, "current_url", "") or ""
                            except Exception:
                                cur_url = ""
                            if ("accounts.google.com" in (cur_url or "").lower()) and not ("ads.google.com" in (cur_url or "").lower()):
                                yield _yield({"event": "comment", "text": "Возвращаюсь в Google Ads после подтверждения…"})
                                ok_back = _ensure_back_to_ads(drv, max_wait=40.0)
                                if ok_back:
                                    yield _yield({"event": "comment", "text": "Вернулся в Google Ads, продолжаю."})
                                else:
                                    yield _yield({"event": "comment", "text": "Не удалось автоматически вернуться в Google Ads (продолжу)."})
                            yield _yield({"event": "step_ok", "number": spec.number})
                        else:
                            err = result_holder.get("err")
                            last_totp_ts = float(context.get("_totp_last_time") or 0.0)
                            after_totp = bool(last_totp_ts and (time.time() - last_totp_ts) < 180.0)
                            steps_results.append({
                                "step": spec.number,
                                "module": spec.module_name,
                                "error": err,
                                "ok": False,
                                "after_totp": after_totp,
                            })
                            yield _yield({"event": "step_fail", "number": spec.number, "error": err, "after_totp": after_totp})
                            all_ok = False
                            break

                    finally:
                        if secondary_acquired and secondary_lock:
                            try:
                                secondary_lock.release()
                            except Exception:
                                pass
                        if primary_acquired:
                            try:
                                primary_lock.release()
                            except Exception:
                                pass
                        context["_active_step"] = None

                # Итоговая сводка (для UI)
                summary_keys = [
                    "business_name",
                    "website_url",
                    "budget_clean",
                    "campaign_type",
                    "campaign_variant_label",
                    "campaign_objective_label",
                    "campaign_objective_reason",
                    "campaign_goal_label",
                    "campaign_goal_reason",
                    "campaign_budget_amount",
                    "campaign_budget_reason",
                    "campaign_target_cpa",
                    "campaign_target_cpa_reason",
                    "campaign_target_cpa_enabled",
                    "locations",
                    "languages",
                ]
                present = {k: v for k, v in context.items() if k in summary_keys and v}
                yield _yield({"event": "summary", "summary": present})

                # ---------- Подготовка к публикации ----------
                if all_ok:
                    try:
                        steps_meta_full = [
                            {"number": s.number, "module": s.module_name, "runner": s.runner_name, "label": s.label}
                            for s in steps
                        ]
                        record = _collect_company_record(
                            profile_id=profile_id,
                            headless=headless,
                            cli_inputs=cli_inputs,
                            context=context,
                            steps_meta=steps_meta_full,
                            driver=drv,
                            user_email=user_email,
                        )
                        record["status"] = "pending_publish"
                        pending = _PendingRun(
                            run_id=run_id,
                            user_email=user_email,
                            profile_id=profile_id,
                            headless=headless,
                            cli_inputs=copy.deepcopy(cli_inputs),
                            context=copy.deepcopy(context),
                            steps_meta=copy.deepcopy(steps_meta_full),
                            record=copy.deepcopy(record),
                            steps_results=copy.deepcopy(steps_results),
                            created_at=time.time(),
                            campaign_variant_id=variant_id,
                            steps_package=steps_package,
                        )
                        _pending_run_store(pending)
                        yield _yield({"event": "ready", "run_id": run_id})
                    except Exception as e:
                        yield _yield({"event": "comment", "text": f"Не удалось подготовить данные к публикации: {e!s}"})
                else:
                    yield _yield({"event": "comment", "text": "Рабочая сессия завершилась с ошибками — повторите запуск перед публикацией."})

            finally:
                with _CODE_BROKER_LOCK:
                    try:
                        _CODE_BROKER.pop(run_id, None)
                    except Exception:
                        pass
                _run_meta_clear(run_id)
                # ВАЖНО: драйвер НЕ закрываем — шаг публикации будет работать по текущему состоянию вкладки.
                yield _yield({"event": "end"})

        return Response(stream_with_context(generate()), mimetype="text/event-stream")

    # ——————————————————— Публикация (SSE) ———————————————————
    @app.route("/api/companies/publish", methods=["GET"])
    def companies_publish_stream() -> Response:
        """
        Выполняет ТОЛЬКО step10 — публикацию кампании.
        Параметры (query): profile_id*, headless, run_id? | company_id?
        Поведение:
          • отправляет «code_request» при 2FA (как и основной запуск);
          • по завершении кладёт google_tag в БД и статус {published|not_published};
          • закрывает драйвер после «end».
        """
        profile_id = (request.args.get("profile_id") or "").strip()
        headless = (request.args.get("headless") or "").strip() in ("1", "true", "yes", "on")
        run_token = (request.args.get("run_id") or "").strip()
        company_id_raw = (request.args.get("company_id") or "").strip()
        company_id = 0
        if company_id_raw:
            try:
                company_id = int(company_id_raw)
            except Exception:
                company_id = 0

        if not profile_id or (not run_token and not company_id):
            msg = {"event": "error", "error": "Fields 'profile_id' and either 'run_id' or 'company_id' are required"}
            return Response("data: " + json.dumps(msg, ensure_ascii=False) + "\n\n",
                            mimetype="text/event-stream", status=400)

        try:
            user_email = _require_user_email()
        except PermissionError:
            msg = {"event": "error", "error": "unauthorized"}
            return Response("data: " + json.dumps(msg, ensure_ascii=False) + "\n\n",
                            mimetype="text/event-stream", status=401)

        if not _profile_allowed(user_email, profile_id):
            msg = {"event": "error", "error": "forbidden"}
            return Response("data: " + json.dumps(msg, ensure_ascii=False) + "\n\n",
                            mimetype="text/event-stream", status=403)

        account_meta_raw = _user_profile_map(user_email).get(profile_id, {}) or {}
        account_meta = dict(account_meta_raw)

        pending_run = None
        context: Dict[str, Any] = {}
        cli_inputs: Dict[str, Any]
        record_payload: Optional[Dict[str, Any]] = None
        steps_package = _DEFAULT_CAMPAIGN_VARIANT.steps_package
        ctx_extra: Dict[str, Any] = {}

        def _load(val: Any) -> Any:
            try:
                return json.loads(val) if isinstance(val, (str, bytes)) else val
            except Exception:
                return val

        if run_token:
            pending_run = _pending_run_get(run_token, user_email=user_email)
            if not pending_run:
                msg = {"event": "error", "error": "pending_not_found"}
                return Response("data: " + json.dumps(msg, ensure_ascii=False) + "\n\n",
                                mimetype="text/event-stream", status=404)
            if pending_run.profile_id and pending_run.profile_id != profile_id:
                msg = {"event": "error", "error": "profile_mismatch"}
                return Response("data: " + json.dumps(msg, ensure_ascii=False) + "\n\n",
                                mimetype="text/event-stream", status=409)
            context = copy.deepcopy(pending_run.context)
            cli_inputs = copy.deepcopy(pending_run.cli_inputs or {})
            record_payload = copy.deepcopy(pending_run.record)
            if pending_run.record_id:
                company_id = pending_run.record_id
            if pending_run.steps_package:
                steps_package = pending_run.steps_package
        else:
            rec = _db_get_one(company_id, user_email=user_email)
            if not rec:
                msg = {"event": "error", "error": "not_found"}
                return Response("data: " + json.dumps(msg, ensure_ascii=False) + "\n\n",
                                mimetype="text/event-stream", status=404)

            rec_profile = str(rec.get("profile_id") or "").strip()
            if rec_profile and rec_profile != profile_id:
                msg = {"event": "error", "error": "profile_mismatch"}
                return Response("data: " + json.dumps(msg, ensure_ascii=False) + "\n\n",
                                mimetype="text/event-stream", status=409)

            ctx_extra = _load(rec.get("extra_json")) or {}
            if isinstance(ctx_extra, dict) and isinstance(ctx_extra.get("context"), dict):
                context.update(ctx_extra.get("context"))
            cli_inputs = {
                "budget": rec.get("budget_per_day"),
                "url": rec.get("site_url"),
                "usp": rec.get("usp"),
                "type": rec.get("campaign_type") or "UBERVERSAL",
                "locations": _load(rec.get("locations")) or None,
                "languages": _load(rec.get("languages")) or None,
                "n_ads": int(rec.get("n_ads") or 0) or 3,
                "variant": rec.get("campaign_variant") or "",
                "campaign_variant_label": rec.get("campaign_variant_label") or "",
            }
            if isinstance(ctx_extra, dict):
                cv_extra = ctx_extra.get("campaign_variant")
                if isinstance(cv_extra, dict):
                    cli_inputs.setdefault("variant", cv_extra.get("id") or "")
                    cli_inputs.setdefault("campaign_variant_label", cv_extra.get("label") or "")
                    context.setdefault("campaign_variant", cv_extra.get("id") or "")
                    context.setdefault("campaign_variant_label", cv_extra.get("label") or "")

        # если данных слишком мало — базовые значения по умолчанию
        cli_inputs.setdefault("locations", None)
        cli_inputs.setdefault("languages", None)
        if not cli_inputs.get("n_ads"):
            cli_inputs["n_ads"] = 3

        stored_steps_pkg = ""
        try:
            stored_steps_pkg = str((context or {}).get("steps_package") or "").strip()
        except Exception:
            stored_steps_pkg = ""
        if stored_steps_pkg:
            steps_package = stored_steps_pkg

        variant_id_val = str(cli_inputs.get("variant") or context.get("campaign_variant") or "").strip()
        variant_label_val = str(cli_inputs.get("campaign_variant_label") or context.get("campaign_variant_label") or "").strip()
        variant_conf = _resolve_campaign_variant(variant_id=variant_id_val, choose_type=cli_inputs.get("type"))
        if not pending_run:
            steps_package = variant_conf.steps_package
        elif not steps_package:
            steps_package = variant_conf.steps_package
        cli_inputs["variant"] = variant_conf.variant_id
        if not variant_label_val:
            variant_label_val = variant_conf.label
        cli_inputs["campaign_variant_label"] = variant_label_val
        choose_type_publish = (str(cli_inputs.get("type") or variant_conf.choose_type or "").strip().upper()
                               or variant_conf.choose_type)
        cli_inputs["type"] = choose_type_publish
        context.setdefault("campaign_variant", variant_conf.variant_id)
        context.setdefault("campaign_variant_label", variant_label_val)
        context.setdefault("campaign_type", choose_type_publish)
        context.setdefault("steps_package", steps_package)
        publish_steps_package = steps_package
        variant_upper_publish = (variant_conf.variant_id or "").strip().upper()
        if variant_upper_publish == "DEMAND_GEN":
            # Для Demand Gen публикуемся через стандартный step10 из examples.steps.
            publish_steps_package = _DEFAULT_CAMPAIGN_VARIANT.steps_package
        creative_mode_ctx = context.get("creative_mode")
        creative_seed_ctx = context.get("creative_seed_assets")
        creative_manual_ctx = context.get("creative_provided_assets")
        if isinstance(ctx_extra, dict):
            creative_extra = ctx_extra.get("creative")
            if isinstance(creative_extra, dict):
                if creative_mode_ctx is None and creative_extra.get("mode"):
                    creative_mode_ctx = creative_extra.get("mode")
                    context["creative_mode"] = creative_mode_ctx
                if creative_seed_ctx is None and creative_extra.get("seed_assets"):
                    creative_seed_ctx = creative_extra.get("seed_assets")
                    context["creative_seed_assets"] = creative_seed_ctx
                if creative_manual_ctx is None and creative_extra.get("provided_assets"):
                    creative_manual_ctx = creative_extra.get("provided_assets")
                    context["creative_provided_assets"] = creative_manual_ctx
        if creative_mode_ctx:
            cli_inputs["creative_mode"] = creative_mode_ctx
        if creative_seed_ctx:
            cli_inputs["creative_seed_assets"] = creative_seed_ctx
        if creative_manual_ctx:
            cli_inputs["creative_provided_assets"] = creative_manual_ctx

        # run_id для модалки 2FA
        run_id = f"pub-{int(time.time()*1000)}-{os.getpid()}-{threading.get_ident()}"

        _run_meta_set(run_id, {
            "user_email": user_email,
            "profile_id": profile_id,
            "otp_secret": (account_meta or {}).get("otp_secret"),
        })

        def _yield(data: Dict[str, Any]) -> str:
            try:
                return "data: " + json.dumps(data, ensure_ascii=False) + "\n\n"
            except Exception:
                return "data: {\"event\":\"error\",\"error\":\"serialization_failed\"}\n\n"

        def generate() -> Any:
            nonlocal company_id, record_payload
            try:
                drv = _get_or_create_driver(profile_id, headless=headless, user_email=user_email)
                _ensure_big_viewport(drv)
            except Exception as e:
                yield _yield({"event": "error", "error": f"driver_failed: {e!r}"})
                yield _yield({"event": "end"})
                _run_meta_clear(run_id)
                return

            # если это свежий запуск (run_id) — создадим запись в БД именно сейчас
            if pending_run and record_payload is not None and not pending_run.record_id:
                try:
                    record_payload["status"] = record_payload.get("status") or "pending_publish"
                    extra_payload = record_payload.get("extra") or {}
                    if isinstance(extra_payload, dict):
                        extra_payload["pending_run_id"] = run_token
                    record_payload["extra"] = extra_payload
                    new_id = _db_insert_company(record_payload)
                    company_id = new_id
                    pending_run.record_id = new_id
                    pending_run.record = copy.deepcopy(record_payload)
                    pending_run.created_at = time.time()
                    _pending_run_store(pending_run)
                except Exception as e:
                    yield _yield({"event": "error", "error": f"db_insert_failed: {e!r}"})
                    yield _yield({"event": "end"})
                    _run_meta_clear(run_id)
                    return

            # найдём именно step10
            try:
                steps = _discover_steps(publish_steps_package)
                spec10 = next((s for s in steps if s.number == 10), None)
                if not spec10:
                    raise RuntimeError("Шаг 10 (step10.py) не найден")
            except Exception as e:
                yield _yield({"event": "error", "error": f"discover_failed: {e!r}"})
                yield _yield({"event": "end"})
                return

            if "_totp_used" not in context:
                context["_totp_used"] = False
            wait_fn = _build_wait_code_fn(
                run_id,
                user_email=user_email,
                profile_id=profile_id,
                account_meta=account_meta,
                context=context,
            )
            _install_wait_code(wait_fn)

            context["_active_step"] = spec10.number
            yield _yield({"event": "step_start", "number": spec10.number, "module": spec10.module_name, "label": spec10.label, "run_id": run_id})

            evq: "queue.Queue[Dict[str, Any]]" = queue.Queue()
            done = threading.Event()
            result_holder: Dict[str, Any] = {}

            def emit(msg: str) -> None:
                if isinstance(msg, str) and msg.strip():
                    evq.put({"event": "comment", "text": msg.strip(), "number": 10})

            def worker():
                setattr(_CODE_CTX, "run_id", run_id)
                setattr(_CODE_CTX, "evq", evq)
                try:
                    res = _call_step_with_injected_kwargs(spec10, drv, cli_inputs, context, emit_cb=emit)
                    result_holder["ok"] = True
                    result_holder["res"] = res
                except Exception as e:
                    result_holder["ok"] = False
                    result_holder["err"] = repr(e)
                finally:
                    try:
                        delattr(_CODE_CTX, "run_id")
                        delattr(_CODE_CTX, "evq")
                    except Exception:
                        pass
                    done.set()

            ext_state = _get_state_from_app()
            ext_lock = getattr(ext_state, "lock", None)
            primary_lock = _local.lock
            secondary_lock = ext_lock if ext_lock and ext_lock is not primary_lock else None

            primary_acquired = False
            secondary_acquired = False
            secondary_warned = False
            wait_until = time.time() + 45.0
            while True:
                primary_acquired = primary_lock.acquire(timeout=3.0)
                if primary_acquired:
                    break
                if time.time() >= wait_until:
                    break
                yield _yield({"event": "comment", "text": "Драйвер занят другой операцией, ожидаю освобождения…"})

            if not primary_acquired:
                yield _yield({"event": "step_fail", "number": 10, "error": "driver_busy", "after_totp": False})
                yield _yield({"event": "publish_result", "company_id": company_id, "published": False, "google_tag": ""})
            else:
                if secondary_lock:
                    try:
                        secondary_acquired = secondary_lock.acquire(timeout=0.5)
                    except Exception:
                        secondary_acquired = False
                    if not secondary_acquired and not secondary_warned:
                        yield _yield({"event": "comment", "text": "Консоль всё ещё читает экран — продолжаю без её блокировки."})
                        secondary_warned = True

                try:
                    t = threading.Thread(target=worker, name="step10-worker", daemon=True)
                    t.start()

                    while not done.is_set():
                        try:
                            evt = evq.get(timeout=0.15)
                            yield _yield(evt)
                        except queue.Empty:
                            pass
                        _ensure_big_viewport(drv)

                    while True:
                        try:
                            evt = evq.get_nowait()
                            yield _yield(evt)
                        except queue.Empty:
                            break

                    # Сформируем результат публикации
                    published = False
                    google_tag = None
                    if result_holder.get("ok"):
                        res = result_holder.get("res") or {}
                        if isinstance(res, dict):
                            # контракт шага: published / google_tag опциональны
                            if isinstance(res.get("published"), bool):
                                published = bool(res.get("published"))
                            if isinstance(res.get("google_tag"), str) and res.get("google_tag").strip():
                                google_tag = res.get("google_tag").strip()
                    # при необходимости — эвристики DOM
                    if not isinstance(published, bool) or published is False:
                        try:
                            published = bool(_detect_published_ui(drv))
                        except Exception:
                            published = False
                    if not google_tag:
                        try:
                            google_tag = _extract_google_tag_snippet(drv) or None
                        except Exception:
                            google_tag = None

                    # Обновим БД
                    if company_id:
                        _db_update_publish(
                            company_id,
                            user_email=user_email,
                            status=("published" if published else "not_published"),
                            google_tag=google_tag,
                            publish_meta={"has_google_tag": bool(google_tag)}
                        )
                        if pending_run and bool(published):
                            _pending_run_pop(run_token)
                    else:
                        yield _yield({"event": "comment", "text": "Не удалось записать результат публикации: отсутствует ID компании."})

                    yield _yield({"event": "step_ok", "number": 10})
                    yield _yield({"event": "publish_result", "company_id": company_id, "published": bool(published), "google_tag": google_tag or ""})
                finally:
                    if secondary_acquired and secondary_lock:
                        try:
                            secondary_lock.release()
                        except Exception:
                            pass
                    if primary_acquired:
                        try:
                            primary_lock.release()
                        except Exception:
                            pass
                    context["_active_step"] = None
                with _CODE_BROKER_LOCK:
                    try:
                        _CODE_BROKER.pop(run_id, None)
                    except Exception:
                        pass
                try:
                    _run_meta_clear(run_id)
                    yield _yield({"event": "end"})
                finally:
                    _shutdown_driver("publish_end")

        return Response(stream_with_context(generate()), mimetype="text/event-stream")

    # ——————————————————— Снимок экрана (разово, для pull‑фолбэка/отладки) ———————————————————
    @app.get("/api/shot")
    def api_shot() -> Response:
        """
        ВАЖНО: не создаёт новый драйвер. Если драйвера нет — 204 (чтобы pull‑фолбэк не запускал браузер).
        """
        try:
            email = _require_user_email()
        except PermissionError:
            return make_response("unauthorized", 401)
        pid = (request.args.get("profile_id") or "").strip()
        headless = (request.args.get("headless") or "").strip() in ("1", "true", "yes", "on")
        if not pid:
            return make_response("profile_id required", 400)
        if not _profile_allowed(email, pid):
            return make_response("forbidden", 403)
        try:
            drv = _maybe_get_driver(pid, headless=headless, user_email=email)  # ← не создаём!
            if not drv:
                return make_response("", 204)
            _ensure_big_viewport(drv)
            try:
                res = drv.execute_cdp_cmd("Page.captureScreenshot",
                                          {"format": "jpeg", "quality": 85, "fromSurface": True})
                data = res.get("data")
                if data:
                    jpg = base64.b64decode(data)
                    return send_file(BytesIO(jpg), mimetype="image/jpeg")
            except Exception:
                pass
            png = drv.get_screenshot_as_png()
            return send_file(BytesIO(png), mimetype="image/png")
        except Exception as e:
            return make_response(f"no shot: {e}", 500)

    # ——————————————————— Ручное управление ———————————————————
    @app.post("/api/control/mouse")
    def api_control_mouse() -> Response:
        data = request.get_json(force=True, silent=True) or {}
        pid = str(data.get("profile_id") or "").strip()
        headless = str(data.get("headless") or "") in ("1","true","yes","on")
        if not pid:
            return jsonify({"ok": False, "error": "profile_id required"}), 400
        try:
            email = _require_user_email()
        except PermissionError:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        if not _profile_allowed(email, pid):
            return jsonify({"ok": False, "error": "forbidden"}), 403
        try:
            drv = _maybe_get_driver(pid, headless=headless, user_email=email)  # ← не создаём!
            if not drv:
                return jsonify({"ok": False, "error": "driver_not_running"}), 409
            _ensure_big_viewport(drv)
            x = float(data.get("x") or 0)
            y = float(data.get("y") or 0)
            typ = (data.get("type") or "click").lower()
            button = (data.get("button") or "left").lower()
            count = int(data.get("count") or 1)
            dx = float(data.get("deltaX") or 0)
            dy = float(data.get("deltaY") or 0)

            def cdp(method: str, params: Dict[str, Any]) -> Any:
                return drv.execute_cdp_cmd(method, params)

            try:
                cdp("Page.enable", {})
                cdp("Runtime.evaluate", {"expression": "window.focus();"})
            except Exception:
                pass

            if typ == "wheel":
                cdp("Input.dispatchMouseEvent", {"type": "mouseWheel", "x": x, "y": y, "deltaX": dx, "deltaY": dy, "modifiers": 0})
            elif typ == "move":
                cdp("Input.dispatchMouseEvent", {"type": "mouseMoved", "x": x, "y": y})
            elif typ == "down":
                cdp("Input.dispatchMouseEvent", {"type": "mousePressed", "x": x, "y": y, "button": button, "clickCount": count})
            elif typ == "up":
                cdp("Input.dispatchMouseEvent", {"type": "mouseReleased", "x": x, "y": y, "button": button, "clickCount": count})
            else:  # click
                cdp("Input.dispatchMouseEvent", {"type": "mouseMoved", "x": x, "y": y})
                cdp("Input.dispatchMouseEvent", {"type": "mousePressed", "x": x, "y": y, "button": button, "clickCount": count})
                cdp("Input.dispatchMouseEvent", {"type": "mouseReleased", "x": x, "y": y, "button": button, "clickCount": count})
            return jsonify({"ok": True})
        except Exception as e:
            return jsonify({"ok": False, "error": repr(e)}), 500

    @app.post("/api/control/text")
    def api_control_text() -> Response:
        data = request.get_json(force=True, silent=True) or {}
        pid = str(data.get("profile_id") or "").strip()
        headless = str(data.get("headless") or "") in ("1","true","yes","on")
        text = str(data.get("text") or "")
        if not pid:
            return jsonify({"ok": False, "error": "profile_id required"}), 400
        try:
            email = _require_user_email()
        except PermissionError:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        if not _profile_allowed(email, pid):
            return jsonify({"ok": False, "error": "forbidden"}), 403
        try:
            drv = _maybe_get_driver(pid, headless=headless, user_email=email)  # ← не создаём!
            if not drv:
                return jsonify({"ok": False, "error": "driver_not_running"}), 409
            _ensure_big_viewport(drv)
            drv.execute_cdp_cmd("Input.insertText", {"text": text})
            return jsonify({"ok": True})
        except Exception as e:
            return jsonify({"ok": False, "error": repr(e)}), 500

    @app.post("/api/control/key")
    def api_control_key() -> Response:
        data = request.get_json(force=True, silent=True) or {}
        pid = str(data.get("profile_id") or "").strip()
        headless = str(data.get("headless") or "") in ("1","true","yes","on")
        key = str(data.get("key") or "")
        if not pid or not key:
            return jsonify({"ok": False, "error": "profile_id and key required"}), 400
        try:
            email = _require_user_email()
        except PermissionError:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        if not _profile_allowed(email, pid):
            return jsonify({"ok": False, "error": "forbidden"}), 403
        try:
            drv = _maybe_get_driver(pid, headless=headless, user_email=email)  # ← не создаём!
            if not drv:
                return jsonify({"ok": False, "error": "driver_not_running"}), 409
            _ensure_big_viewport(drv)
            params = {"type": "keyDown", "key": key, "code": key, "windowsVirtualKeyCode": 0}
            drv.execute_cdp_cmd("Input.dispatchKeyEvent", params)
            params["type"] = "keyUp"
            drv.execute_cdp_cmd("Input.dispatchKeyEvent", params)
            return jsonify({"ok": True})
        except Exception as e:
            return jsonify({"ok": False, "error": repr(e)}), 500

    # ——————————————————— 2FA: приём кода от пользователя ———————————————————
    @app.post("/api/confirm/submit")
    def api_confirm_submit() -> Response:
        data = request.get_json(force=True, silent=True) or {}
        run_id = str(data.get("run_id") or "").strip()
        code = str(data.get("code") or "").strip()
        otp_secret_raw = str(data.get("otp_secret") or "").strip()
        if not run_id:
            return jsonify({"ok": False, "error": "run_id is required"}), 400
        if otp_secret_raw:
            normalized = _normalize_otp_secret(otp_secret_raw)
            if not normalized:
                return jsonify({"ok": False, "error": "invalid_otp_secret"}), 400
            meta = _run_meta_get(run_id)
            if meta and meta.get("user_email") and meta.get("profile_id"):
                try:
                    _accounts_store_otp_secret(str(meta["user_email"]), str(meta["profile_id"]), normalized)
                except Exception:
                    pass
                _run_meta_update(run_id, otp_secret=normalized)
            ok = _broker_set_code(run_id, normalized)
            return jsonify({"ok": bool(ok), "mode": "otp_secret"})
        if not code:
            return jsonify({"ok": False, "error": "code or otp_secret required"}), 400
        ok = _broker_set_code(run_id, code)
        return jsonify({"ok": bool(ok), "mode": "code"})

    # ——————————————————— Companies API ———————————————————
    @app.get("/api/companies")
    def api_companies_list() -> Response:
        try:
            email = _require_user_email()
        except PermissionError:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        try:
            limit = int(request.args.get("limit") or 50)
            offset = int(request.args.get("offset") or 0)
        except Exception:
            limit, offset = 50, 0
        items = _db_list(user_email=email, limit=limit, offset=offset)
        return jsonify({"ok": True, "items": items, "limit": limit, "offset": offset})

    @app.get("/api/companies/<int:rec_id>")
    def api_companies_get(rec_id: int) -> Response:
        try:
            email = _require_user_email()
        except PermissionError:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        item = _db_get_one(rec_id, user_email=email)
        if not item:
            return jsonify({"ok": False, "error": "not_found"}), 404

        def _load(val: Any) -> Any:
            try:
                return json.loads(val) if isinstance(val, (str, bytes)) else val
            except Exception:
                return val

        item["locations"] = _load(item.get("locations"))
        item["languages"] = _load(item.get("languages"))
        item["headlines_json"] = _load(item.get("headlines_json"))
        item["long_headlines_json"] = _load(item.get("long_headlines_json"))
        item["descriptions_json"] = _load(item.get("descriptions_json"))
        item["images_json"] = _load(item.get("images_json"))
        item["image_files_json"] = _load(item.get("image_files_json"))
        item["extra_json"] = _load(item.get("extra_json"))
        item["google_tags"] = _load(item.get("google_tags"))
        # новинка — гугл тег (как есть)
        item["google_tag"] = item.get("google_tag")
        return jsonify({"ok": True, "item": item})


# =============================================================================
#                      AdsPower API: список профилей (вспом.)
# =============================================================================

def _list_adspower_profiles(
    q: str = "",
    page: int = 1,
    page_size: int = 100,
    group_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    base, token = _adsp_env()
    headers = {"Authorization": token} if token else {}
    groups = [str(g or "").strip() for g in (group_ids or []) if str(g or "").strip()]
    item_map: Dict[str, Dict[str, Any]] = {}
    total = 0
    if groups:
        max_pages = 3
        for gid in groups:
            page_curr = 1
            while page_curr <= max_pages:
                url = (
                    f"{base}/api/v1/user/list?page={page_curr}&page_size={int(page_size)}"
                    f"&group_id={urllib.parse.quote(gid)}"
                )
                code, body = _http_get_json(url, headers=headers, timeout=6.0)
                if not code or not isinstance(body, dict) or str(body.get("code")) not in ("0", "200"):
                    break
                data = body.get("data") or {}
                lst = data.get("list") or []
                for it in lst:
                    name = it.get("name") or it.get("username") or it.get("remark") or ""
                    pid = it.get("user_id") or it.get("id") or it.get("profile_id") or it.get("profileId")
                    grp = it.get("group_id") or it.get("groupId") or gid
                    if not pid:
                        continue
                    pid_s = str(pid)
                    item_map[pid_s] = {
                        "profile_id": pid_s,
                        "name": str(name),
                        "group_id": str(grp or ""),
                        "tags": it.get("tags") or [],
                    }
                if len(lst) < page_size:
                    break
                page_curr += 1
        items = list(item_map.values())
        total = len(items)
    else:
        url = f"{base}/api/v1/user/list?page={int(page)}&page_size={int(page_size)}"
        code, body = _http_get_json(url, headers=headers, timeout=6.0)
        if code and isinstance(body, dict) and str(body.get("code")) in ("0", "200"):
            data = body.get("data") or {}
            lst = data.get("list") or []
            total = int(data.get("total") or len(lst) or 0)
            for it in lst:
                name = it.get("name") or it.get("username") or it.get("remark") or ""
                pid = it.get("user_id") or it.get("id") or it.get("profile_id") or it.get("profileId")
                grp = it.get("group_id") or it.get("groupId") or ""
                if not pid:
                    continue
                pid_s = str(pid)
                item_map[pid_s] = {
                    "profile_id": pid_s,
                    "name": str(name),
                    "group_id": str(grp or ""),
                    "tags": it.get("tags") or [],
                }
        items = list(item_map.values())
    ql = q.strip().lower()
    if ql:
        items = [
            x for x in items
            if ql in x["profile_id"].lower()
            or ql in (x["name"] or "").lower()
            or ql in (x["group_id"] or "").lower()
        ]
    return {"items": items, "total": total}
