# ads_ai/browser/adspower.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
import os
import re
import socket
import stat
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List

import requests
from requests import Response

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

__all__ = [
    "AdsPowerError",
    "start_adspower",
    "stop_adspower",
    # алиасы для совместимости с рефлексией
    "start",
    "stop",
]

# Логгер модуля
log = logging.getLogger("ads_ai.browser.adspower")

# Базовый локальный адрес AdsPower, если не переопределён
DEFAULT_BASE = "http://local.adspower.net:50325"


# ============================== Исключения ==============================

class AdsPowerError(RuntimeError):
    """Ошибки интеграции с AdsPower."""


# ============================== Модели ==============================

@dataclass
class _StartMeta:
    profile_id: str
    selenium_addr: str   # 127.0.0.1:61475
    devtools_ws: Optional[str]
    webdriver_path: str
    api_base: str


# ============================== Утилиты ==============================

def _mask_token(tok: Optional[str]) -> str:
    s = (tok or "").strip()
    if not s:
        return "-"
    if len(s) <= 8:
        head, tail = s[:2], s[-2:]
    else:
        head, tail = s[:4], s[-4:]
    import hashlib
    return f"{head}…{tail} (sha1:{hashlib.sha1(s.encode('utf-8')).hexdigest()[:8]})"


def _normalize_base(u: Optional[str]) -> str:
    """Приводит базовый URL к виду http(s)://host:port без завершающего слеша."""
    u = (u or "").strip() or os.getenv("ADSP_API_BASE") or os.getenv("ADSP_BASE") or DEFAULT_BASE
    if not re.match(r"^https?://", u, re.I):
        u = "http://" + u
    return u.rstrip("/")


def _strip_scheme(host: str) -> str:
    """127.0.0.1:12345/ws  ->  127.0.0.1:12345"""
    s = (host or "").strip()
    s = re.sub(r"^(ws|wss|http|https)://", "", s, flags=re.I)
    s = re.sub(r"/.*$", "", s)
    return s


def _split_host_port(addr: str) -> Tuple[str, int]:
    """'127.0.0.1:61475' -> ('127.0.0.1', 61475)"""
    addr = _strip_scheme(addr)
    host, _, port = addr.partition(":")
    try:
        return host, int(port)
    except Exception:
        return host, 0


def _short(obj: Any, limit: int = 500) -> str:
    try:
        s = json.dumps(obj, ensure_ascii=False) if not isinstance(obj, str) else obj
    except Exception:
        s = str(obj)
    return (s[:limit] + "…") if len(s) > limit else s


def _http(
    method: str,
    api_base: str,
    path: str,
    *,
    token: Optional[str],
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: float = 30.0,
    retries: int = 2,
    backoff: float = 0.35,
) -> Tuple[int, Dict[str, Any]]:
    """
    Унифицированный HTTP-вызов AdsPower (GET/POST) с ретраями.
    Возвращает (status_code, dict_body_or_wrapper).
    """
    base = _normalize_base(api_base)
    url = f"{base}{path}"
    q = dict(params or {})

    # На ряде сборок ожидают токен в query и/или в заголовке
    env_token = os.getenv("ADSP_API_TOKEN") or os.getenv("ADSP_TOKEN") or ""
    tok = (token or "").strip() or env_token
    if tok and "token" not in q:
        q["token"] = tok

    headers = {"Content-Type": "application/json"}
    if tok:
        headers["Authorization"] = tok
        headers["X-ADSPower-Token"] = tok
        headers["X-API-KEY"] = tok

    last_err = None
    for attempt in range(1, max(1, retries) + 2):
        t0 = time.perf_counter()
        try:
            r: Response = requests.request(
                method.upper(),
                url,
                params=q,
                json=json_body if method.upper() == "POST" else None,
                headers=headers,
                timeout=timeout,
            )
            ms = int((time.perf_counter() - t0) * 1000)
            try:
                body = r.json()
            except Exception:
                body = {"raw": r.text}
            # 5xx/599 — повод ретраить
            if (r.status_code >= 500 or r.status_code == 0) and attempt <= retries:
                log.warning(
                    "adspower http %s %s -> %s (retry %d/%d) %dms",
                    method.upper(), path, r.status_code, attempt, retries, ms
                )
                time.sleep(backoff * attempt)
                continue
            log.debug(
                "adspower http %s %s -> %s in %dms",
                method.upper(), path, r.status_code, ms
            )
            return r.status_code, body
        except Exception as e:
            last_err = f"{e.__class__.__name__}: {e}"
            log.warning(
                "adspower http err %s %s: %s (attempt %d/%d)",
                method.upper(), path, last_err, attempt, retries + 1
            )
            if attempt <= retries:
                time.sleep(backoff * attempt)
                continue
            return 599, {"error": last_err or "network_error"}

    return 599, {"error": last_err or "unknown_error"}


def _parse_start_payload(payload: Dict[str, Any], api_base: str, default_profile_id: Optional[str] = None) -> _StartMeta:
    """
    Универсальный разбор ответа /start (v1/v2).
    Пример v2:
      {"code":0,"data":{"ws":{"selenium":"127.0.0.1:XXXXX","devtools":"ws://..."},
                        "webdriver":"/path/chromedriver","user_id":"xxxx"}}
    """
    data = payload.get("data") or payload

    ws = data.get("ws") or {}
    selenium_addr = (
        ws.get("selenium")
        or ws.get("seleniumAddress")
        or data.get("selenium")
        or data.get("selenium_address")
    )
    webdriver_bin = (
        data.get("webdriver")
        or data.get("web_driver")
        or data.get("driver")
        or data.get("driver_path")
        or data.get("chromedriver")
    )
    devtools_ws = ws.get("devtools") or data.get("wsEndpoint") or data.get("webSocketDebuggerUrl")

    profile_id = (
        data.get("user_id")
        or data.get("id")
        or data.get("profile_id")
        or data.get("uid")
        or default_profile_id
        or ""
    )

    if not selenium_addr or not webdriver_bin:
        raise AdsPowerError(
            "Bad AdsPower start response (selenium/driver missing): "
            f"{_short(payload)}"
        )

    selenium_addr = _strip_scheme(str(selenium_addr)).strip()
    return _StartMeta(
        profile_id=str(profile_id),
        selenium_addr=selenium_addr,
        devtools_ws=str(devtools_ws) if devtools_ws else None,
        webdriver_path=_resolve_chromedriver_binary(str(webdriver_bin)),
        api_base=_normalize_base(api_base),
    )


def _resolve_chromedriver_binary(path: str) -> str:
    """
    AdsPower на macOS часто отдаёт *.app — дойдём до фактического бинаря.
    А ещё убедимся, что он исполняемый.
    """
    p = path
    # macOS bundle
    if p.endswith(".app") and os.path.exists(p):
        cand = os.path.join(p, "Contents", "MacOS", "chromedriver")
        if os.path.exists(cand):
            p = cand
    # Исполняемые права
    try:
        mode = os.stat(p).st_mode
        if not (mode & stat.S_IXUSR):
            os.chmod(p, mode | stat.S_IXUSR)
    except Exception:
        pass
    return p


def _wait_port_open(addr: str, timeout_sec: float = 8.0) -> bool:
    """Ждём, когда selenium-порт станет доступен (сразу после /start бывает пауза)."""
    host, port = _split_host_port(addr)
    if not host or not port:
        return False
    t_end = time.time() + timeout_sec
    while time.time() < t_end:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return True
        except OSError:
            time.sleep(0.25)
    return False


def _build_options(headless: bool, window_size: str) -> Options:
    """
    ChromeOptions, совместимые с Chrome/Driver 135+ при attach через debuggerAddress.
    ВАЖНО: без legacy-опций excludeSwitches/useAutomationExtension — они ломают Chrome 135/136.
    """
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")

    if window_size:
        opts.add_argument(f"--window-size={window_size}")

    # Надёжные базовые флаги
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-infobars")

    return opts


def _post_start_v2(api_base: str, profile: str, headless: bool, token: Optional[str]) -> Tuple[int, Dict[str, Any]]:
    """
    Старт через /api/v2/browser-profile/start (POST). На большинстве сборок —
    максимально стабильный путь. Возвращает (code, body).
    """
    la: List[str] = []

    # По умолчанию убираем системный прокси — это экономит время/убирает IP‑вкладки
    no_proxy = (os.getenv("ADSP_LAUNCH_NO_PROXY") or "1").strip() not in ("0", "false", "False")
    if no_proxy:
        la.append("--no-proxy-server")

    # снижает шум и мусорные попапы
    la.extend([
        "--disable-search-engine-choice-screen",
        "--disable-notifications",
        "--disable-features=Translate",
    ])

    body = {
        "profile_id": profile,
        "headless": "1" if headless else "0",
        "open_browser": "0" if headless else "1",
        "last_opened_tabs": "0",
        "proxy_detection": "0",
        "launch_args": la,
    }
    return _http("POST", api_base, "/api/v2/browser-profile/start", token=token, json_body=body, timeout=35.0)


def _get_active_list(api_base: str, token: Optional[str]) -> Tuple[int, Dict[str, Any]]:
    for path in ("/api/v2/browser-profile/active/list", "/api/v1/browser/active/list"):
        code, body = _http("GET", api_base, path, token=token, timeout=8.0, retries=0)
        if code == 200:
            return code, body
    return 404, {}


def _cleanup_tabs_and_window(driver: webdriver.Chrome, headless: bool, window_size: str) -> None:
    """Оставляем одну вкладку и выставляем размер окна (в видимом режиме)."""
    try:
        handles = driver.window_handles
        if len(handles) > 1:
            base = handles[0]
            for h in handles[1:]:
                try:
                    driver.switch_to.window(h)
                    driver.close()
                except Exception:
                    pass
            try:
                driver.switch_to.window(base)
            except Exception:
                pass
        if not headless and window_size:
            w, _, h = window_size.partition(",")
            try:
                driver.set_window_size(int(w), int(h or "800"))
            except Exception:
                pass
    except Exception:
        pass


# ============================== Публичный API ==============================

def start_adspower(
    profile: str,
    headless: bool,
    api_base: str,
    token: Optional[str],
    window_size: str = "1440,900",
    *,
    timeout: float = 30.0,
) -> webdriver.Chrome:
    """
    Запускает профиль AdsPower и коннектится к его Selenium endpoint.
    Возвращает готовый webdriver.Chrome.

    Аргументы:
      profile      — user_id / profile_id
      headless     — True/False; может быть перекрыт ENV ADS_AI_HEADLESS
      api_base     — базовый URL API AdsPower (или '', возьмём из ENV/DEFAULT)
      token        — API-токен (может быть пустым; возьмём из ENV)
      window_size  — размер окна для видимого режима
      timeout      — базовый таймаут HTTP-запросов (сек)

    Побочные эффекты:
      В driver._adspower пишутся метаданные: profile_id, selenium_addr, devtools_ws, webdriver_path, api_base, token.
    """
    # Перекрытие headless из ENV (если нужно)
    env_headless = os.getenv("ADS_AI_HEADLESS")
    if env_headless is not None:
        headless = env_headless.strip() not in ("0", "false", "False", "")

    base = _normalize_base(api_base)
    tok = (token or "").strip() or os.getenv("ADSP_API_TOKEN") or os.getenv("ADSP_TOKEN") or ""

    force_v1 = (os.getenv("ADSP_FORCE_V1") or "").strip() in ("1", "true", "True")
    force_v2 = (os.getenv("ADSP_FORCE_V2") or "").strip() in ("1", "true", "True")

    # Диагностика «уже запущен»
    try:
        code_act, body_act = _get_active_list(base, tok)
        if code_act == 200:
            log.debug("AdsPower active list: %s", _short(body_act, 400))
    except Exception:
        pass

    # --- 1) Пытаемся стартовать через v2 ---
    meta: Optional[_StartMeta] = None
    start_errors: List[str] = []

    if not force_v1:
        code_v2, body_v2 = _post_start_v2(base, profile, headless, tok)
        ok_v2 = code_v2 == 200 and isinstance(body_v2, dict) and str(body_v2.get("code")) in ("0", "200")
        try:
            meta = _parse_start_payload(body_v2, base, default_profile_id=profile) if ok_v2 else None
        except Exception as e:
            start_errors.append(f"v2 parse error: {e}")
            meta = None

        if not ok_v2:
            start_errors.append(f"v2 start http={code_v2} body={_short(body_v2)}")

    # --- 2) Фолбэк на v1, если нужно ---
    if meta is None and not force_v2:
        code_v1, body_v1 = _http(
            "GET", base, "/api/v1/browser/start",
            token=tok,
            params={"user_id": profile},
            timeout=max(10.0, timeout),
        )
        ok_v1 = code_v1 == 200
        try:
            meta = _parse_start_payload(body_v1, base, default_profile_id=profile) if ok_v1 else None
        except Exception as e:
            start_errors.append(f"v1 parse error: {e}")
            meta = None

        if not ok_v1:
            start_errors.append(f"v1 start http={code_v1} body={_short(body_v1)}")

    if meta is None:
        raise AdsPowerError(
            "AdsPower start failed. "
            f"base={base}, token={_mask_token(tok)}; details={'; '.join(start_errors) or 'no details'}"
        )

    # Ждём открытие порта
    if not _wait_port_open(meta.selenium_addr, timeout_sec=8.0):
        log.warning("Selenium port not ready yet: %s (continue attach)", meta.selenium_addr)

    # Готовим ChromeOptions + attach к debuggerAddress
    opts = _build_options(headless=headless, window_size=window_size)
    opts.add_experimental_option("debuggerAddress", meta.selenium_addr)

    # Запускаем Chromedriver, подключающийся к уже запущенному браузеру AdsPower
    try:
        drv = webdriver.Chrome(service=Service(meta.webdriver_path), options=opts)
    except Exception as e:
        raise AdsPowerError(
            f"Failed to attach ChromeDriver (path={meta.webdriver_path}, "
            f"addr={meta.selenium_addr}): {e}"
        )

    # Таймауты по умолчанию (без агрессивного implicit wait)
    try:
        drv.set_page_load_timeout(45)
        drv.set_script_timeout(35)
    except Exception:
        pass

    # Сохраняем метаданные — пригодится при остановке
    try:
        drv._adspower = {  # type: ignore[attr-defined]
            "profile_id": meta.profile_id or profile,
            "selenium_addr": meta.selenium_addr,
            "devtools_ws": meta.devtools_ws,
            "webdriver_path": meta.webdriver_path,
            "api_base": meta.api_base,
            "token": tok,
        }
    except Exception:
        pass

    # Анти‑залипание: оставляем одну вкладку и приводим окно к нужному размеру
    _cleanup_tabs_and_window(drv, headless=headless, window_size=window_size)

    log.info(
        "AdsPower attached: profile=%s addr=%s driver=%s",
        meta.profile_id or profile, meta.selenium_addr, meta.webdriver_path
    )
    return drv


def stop_adspower(
    driver: Optional[webdriver.Chrome] = None,
    *,
    profile: Optional[str] = None,
    profile_id: Optional[str] = None,
    api_base: Optional[str] = None,
    base: Optional[str] = None,
    token: Optional[str] = None,
    api_token: Optional[str] = None,
    timeout: float = 10.0,
) -> None:
    """
    Мягкая остановка профиля через AdsPower API + закрытие драйвера.
    Можно передать либо driver, либо profile_id (profile).
    Остальное (api_base, token) подтянется из driver._adspower при наличии.
    """
    # Извлекаем контекст
    _profile = (
        profile_id
        or profile
        or (getattr(getattr(driver, "_adspower", {}), "get", lambda *_: None)("profile_id") if driver else None)
    )
    _base = _normalize_base(
        api_base
        or base
        or (getattr(getattr(driver, "_adspower", {}), "get", lambda *_: None)("api_base") if driver else None)
        or os.getenv("ADSP_API_BASE")
        or os.getenv("ADSP_BASE")
        or DEFAULT_BASE
    )
    _token = (
        token
        or api_token
        or (getattr(getattr(driver, "_adspower", {}), "get", lambda *_: None)("token") if driver else None)
        or os.getenv("ADSP_API_TOKEN")
        or os.getenv("ADSP_TOKEN")
        or ""
    )

    # 1) Остановка через v2 (best-effort)
    try:
        code_v2, body_v2 = _http(
            "POST",
            _base,
            "/api/v2/browser-profile/stop",
            token=_token,
            json_body={"profile_id": _profile} if _profile else None,
            timeout=timeout,
            retries=1,
        )
        if code_v2 != 200 or (isinstance(body_v2, dict) and str(body_v2.get("code")) not in ("0", "200")):
            log.debug("AdsPower stop v2 non-ok %s: %s", code_v2, _short(body_v2))
    except Exception as e:
        log.debug("AdsPower stop v2 API error: %s", e)

    # 2) Фолбэк на v1
    try:
        if _profile:
            code_v1, body_v1 = _http(
                "GET",
                _base,
                "/api/v1/browser/stop",
                token=_token,
                params={"user_id": _profile},
                timeout=timeout,
                retries=1,
            )
            if code_v1 != 200:
                log.debug("AdsPower stop v1 non-200 %s: %s", code_v1, _short(body_v1))
    except Exception as e:
        log.debug("AdsPower stop v1 API error: %s", e)

    # 3) Закрываем драйвер
    try:
        if driver and hasattr(driver, "quit"):
            driver.quit()
            log.info("ChromeDriver quit (profile=%s)", _profile or "-")
    except Exception as e:
        log.debug("Driver quit error: %s", e)


# Алиасы для совместимости с рефлексией в вызывающем коде
def start(*args, **kwargs):
    return start_adspower(*args, **kwargs)


def stop(*args, **kwargs):
    return stop_adspower(*args, **kwargs)
