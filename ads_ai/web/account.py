# -*- coding: utf-8 -*-
"""
account.py — мастер добавления Google‑аккаунтов с идеальным UX и стабильным стартом AdsPower.

Ключевые моменты:
- Темизация: светлая по умолчанию; тёмная активируется только при наличии .dark/.theme-dark/.Dark
  либо [data-theme="dark"], [data-bs-theme="dark"], [data-mode="dark"], [theme="dark"].
- Асинхронный старт: тяжёлые шаги выполняются в фоне, страница мастера открывается мгновенно,
  прогресс — через SSE + /state‑поллинг. Кнопки включаются, когда драйвер готов.
- Надёжность: единый старт через start_adspower с fallback на /api/v2/browser-profile/start.
- Без дедлоков: лок профиля освобождается при любой ошибке; headless‑флаг восстанавливается.

Обновления дизайна:
- Единый «хром» страницы (фон, токены, сетка shell, панель-меню слева) — как в страницах компаний.
- Мастер входа/форма встроены в правую панель .stage; слева боковая панель с навигацией.
"""

from __future__ import annotations

import os
import json
import time
import threading
import hashlib
import socket
import re
import traceback
from dataclasses import dataclass, field
from io import BytesIO
import base64
import inspect
from typing import Any, Dict, Optional, List, Union

from flask import Response, request, jsonify, make_response, send_file, session, abort, url_for, redirect

# Selenium (нужен для fallback v2‑старта)
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService

# Единый надёжный старт AdsPower
from ads_ai.browser.adspower import start_adspower, AdsPowerError

# Мягкие импорты для вспомогательных функций
try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore

try:
    from ads_ai.browser import adspower as adspower_mod  # type: ignore
except Exception:  # pragma: no cover
    adspower_mod = None  # type: ignore

import urllib.request
import urllib.parse
import html as _html


# ============ Локальные общие хелперы (без зависимостей от create_companies) ============

def _escape(s: Any) -> str:
    return _html.escape("" if s is None else str(s), quote=True)


def _console(event_type: str, payload: Dict[str, Any], task_id: Optional[str] = None) -> None:
    line = {
        "ts": time.time(),
        "src": "accounts",
        "type": event_type,
        "task": task_id,
        "data": payload,
    }
    try:
        print(json.dumps(line, ensure_ascii=False), flush=True)
    except Exception:
        print(f"[{event_type}] {task_id} {payload}", flush=True)


def _read_csrf() -> str:
    tok = session.get("_csrf")
    if not tok:
        tok = base64.urlsafe_b64encode(os.urandom(24)).decode("ascii").rstrip("=")
        session["_csrf"] = tok
    return tok


def _check_csrf(value: Optional[str]) -> None:
    if not value or value != session.get("_csrf"):
        abort(400, description="CSRF token invalid")


def _require_user() -> str:
    email = session.get("user_email")
    if not email:
        abort(401)
    return str(email)


# ------------------------- Общий «хром» и лэйаут -------------------------

_CHROME_CSS = r"""
<style>
  :root{
    --bg:#eef2f7; --bg2:#f6f8fb; --text:#111827; --muted:#6b7280;
    --glass: rgba(255,255,255,.66); --glass-2: rgba(255,255,255,.5);
    --border: rgba(17,24,39,.08); --ring: rgba(17,24,39,.06);
    --neon1:#38bdf8; --neon2:#a78bfa; --ok:#16a34a; --err:#ef4444; --warn:#f59e0b;
    --radius:24px; --radius-sm:16px; --shadow: 0 10px 30px rgba(15,23,42,.12);
    --content-max: 1480px;
  }
  html[data-theme="dark"]{
    color-scheme: dark;
    --bg:#0b1220; --bg2:#0d1423; --text:#e5e7eb; --muted:#94a3b8;
    --glass: rgba(17,23,41,.55); --glass-2: rgba(17,23,41,.45);
    --border: rgba(255,255,255,.09); --ring: rgba(56,189,248,.15);
    --shadow: 0 10px 30px rgba(0,0,0,.35);
  }
  *{box-sizing:border-box}
  html,body{height:100%;margin:0;color:var(--text);
    font:14px/1.45 Inter,system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
    -webkit-font-smoothing:antialiased}
  body{
    background:
      radial-gradient(1200px 800px at 20% -10%, #ffffff 0%, var(--bg) 48%, var(--bg2) 100%),
      linear-gradient(180deg,#ffffff, var(--bg2));
  }
  html[data-theme="dark"] body{
    background:
      radial-gradient(1200px 800px at 20% -10%, #0e1527 0%, var(--bg) 40%, var(--bg2) 100%),
      linear-gradient(180deg,#0f172a, var(--bg2));
  }

  .shell{ display:grid; grid-template-columns: 300px minmax(0,1fr); gap:18px;
          min-height:100vh; padding:18px; max-width:var(--content-max); margin:0 auto; }
  .panel{ background:var(--glass); border:1px solid var(--border); border-radius:var(--radius);
          backdrop-filter: blur(12px) saturate(160%); box-shadow:var(--shadow); overflow:hidden; }
  .menu{ padding:18px; display:flex; flex-direction:column; gap:12px }
  .menu .head{ height:56px; display:flex; align-items:center; gap:10px; padding:0 6px; font-weight:700 }
  .mitem{ display:flex; align-items:center; gap:10px; padding:10px 12px; border-radius:14px; background:var(--glass-2); border:1px solid var(--border); text-decoration:none; color:inherit }
  .mitem:hover{ filter:brightness(1.02) }
  .mitem.active b{ font-weight:800 }
  .muted{ color:var(--muted) }
  .stage{ position:relative; display:grid; grid-template-rows: auto 1fr auto; gap:14px; padding:18px; }

  /* Небольшие утилиты для заголовков внутри stage */
  .stage .stage-title{ font-weight:800; letter-spacing:.2px; margin:6px 0 0 0 }
  .stage .stage-sub{ color:var(--muted); font-size:12px; margin-bottom:6px }
</style>
"""

def _sidebar_html(active: str = "") -> str:
    acc_list_active = " active" if active in ("accounts", "accounts_list") else ""
    cmp_create_active = ""  # не на этой странице
    cmp_list_active = ""

    return f"""
    <aside class="panel menu" aria-label="Меню">
      <div class="head">
        <div style="width:36px;height:36px;border-radius:12px;background:linear-gradient(135deg,var(--neon1),var(--neon2))"></div>
        <div>Меню</div>
      </div>
      <a class="mitem{cmp_create_active}" href="/companies">Создание компаний</a>
      <a class="mitem{cmp_list_active}" href="/companies/list">Список компаний</a>
      <a class="mitem{acc_list_active}" href="/accounts"><b>Список аккаунтов</b></a>
      <div style="margin-top:auto" class="muted">Powered by EasyByte</div>
    </aside>
    """

def _layout(title_right: str, inner: str, active: str = "accounts") -> str:
    """Единый каркас с боковой панелью и правой сценой."""
    return f"""<!doctype html>
<html lang="ru" data-theme="light">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>{_escape(title_right)} · Accounts</title>
  {_CHROME_CSS}
</head>
<body>
  <div class="shell">
    {_sidebar_html(active)}
    <section class="panel stage">
      {inner}
    </section>
  </div>
</body>
</html>"""


def _get_adspower_env() -> tuple[bool, str, str]:
    def _normalize_base(u: str) -> str:
        u = (u or "").strip()
        if not u:
            return "http://local.adspower.net:50325"
        if not re.match(r"^https?://", u, re.I):
            u = "http://" + u
        return u.rstrip("/")
    # Принудительно включаем headless, чтобы AdsPower-профили не открывали окна.
    os.environ["ADS_AI_HEADLESS"] = "1"
    os.environ.setdefault("ADSP_FORCE_V2", "1")
    headless = True
    api_base = os.getenv("ADSP_API_BASE") or os.getenv("ADSP_BASE") or "http://local.adspower.net:50325"
    api_base = _normalize_base(str(api_base))
    token = (os.getenv("ADSP_API_TOKEN") or os.getenv("ADSP_TOKEN") or "").strip()
    return headless, api_base, token


def _http_get_json(url: str, headers: Optional[Dict[str, str]] = None, timeout: float = 2.0) -> tuple[int, Dict[str, Any]]:
    try:
        if requests:
            r = requests.get(url, headers=headers or {}, timeout=timeout)  # type: ignore
            j = {}
            try:
                j = r.json() if r.content else {}
            except Exception:
                j = {}
            return int(r.status_code), j or {}
    except Exception:
        pass
    try:
        req = urllib.request.Request(url, headers=headers or {})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
            try:
                j = json.loads(data.decode("utf-8")) if data else {}
            except Exception:
                j = {}
            return int(resp.getcode() or 0), j or {}
    except Exception:
        return 0, {}


def _http_post_json(url: str, payload: Dict[str, Any], headers: Optional[Dict[str, str]] = None, timeout: float = 3.0) -> tuple[int, Dict[str, Any]]:
    body = json.dumps(payload).encode("utf-8")
    h = {"Content-Type": "application/json"}
    if headers:
        h.update(headers)
    try:
        if requests:
            r = requests.post(url, headers=h, data=body, timeout=timeout)  # type: ignore
            try:
                return int(r.status_code), (r.json() if r.content else {})
            except Exception:
                return int(r.status_code), {}
    except Exception:
        pass
    try:
        req = urllib.request.Request(url, data=body, headers=h, method="POST")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
            try:
                return int(resp.getcode() or 0), (json.loads(data.decode("utf-8")) if data else {})
            except Exception:
                return int(resp.getcode() or 0), {}
    except Exception:
        return 0, {}


def _stop_adspower_driver(driver: Any) -> None:
    if driver is None:
        return
    _headless, api_base, token = _get_adspower_env()
    for fn_name in ("stop_adspower", "stop"):
        try:
            fn = getattr(adspower_mod, fn_name, None) if adspower_mod else None
        except Exception:
            fn = None
        if not fn:
            continue
        try:
            params = inspect.signature(fn).parameters
        except Exception:
            params = {}
        pnames = set(params.keys()) if params else set()
        try:
            if pnames:
                kw: Dict[str, Any] = {}
                if "driver" in pnames:
                    kw["driver"] = driver
                if "token" in pnames:
                    kw["token"] = token
                elif "api_token" in pnames:
                    kw["api_token"] = token
                if "api_base" in pnames:
                    kw["api_base"] = api_base
                elif "base" in pnames:
                    kw["base"] = api_base
                if kw:
                    fn(**kw)  # type: ignore
                    _console(f"adspower:{fn_name}", {}, None)
                    return
        except Exception:
            pass
        try:
            if pnames:
                ordered: List[Any] = []
                for name in params.keys():
                    if name == "self":
                        continue
                    if name == "driver":
                        ordered.append(driver)
                    elif name in ("token", "api_token"):
                        ordered.append(token)
                    elif name in ("api_base", "base"):
                        ordered.append(api_base)
                if ordered:
                    fn(*ordered)  # type: ignore
                    _console(f"adspower:{fn_name}", {}, None)
                    return
        except Exception:
            pass
        try:
            fn(driver)  # type: ignore
            _console(f"adspower:{fn_name}", {}, None)
            return
        except Exception:
            pass
    try:
        if hasattr(driver, "quit") and callable(getattr(driver, "quit")):
            driver.quit()
            _console("webdriver:quit", {}, None)
    except Exception:
        pass


def _open_google_ads(driver, emit, stage) -> None:
    try:
        from ads_ai.browser.waits import ensure_ready_state, wait_url, wait_dom_stable
    except Exception:
        ensure_ready_state = lambda *_a, **_k: None  # type: ignore
        wait_url = lambda *_a, **_k: True          # type: ignore
        wait_dom_stable = lambda *_a, **_k: True   # type: ignore
    try:
        stage("ads:open", "start", url="https://ads.google.com/")
        emit("log", {"msg": "Навигация на https://ads.google.com/ ..."})
        driver.get("https://ads.google.com/")
        ensure_ready_state(driver, timeout=15.0)
        wait_dom_stable(driver, idle_ms=700, timeout_sec=12)
        emit("log", {"msg": "Открыта главная Google Ads"})
        driver.get("https://ads.google.com/aw/overview")
        ensure_ready_state(driver, timeout=15.0)
        ok = wait_url(driver, pattern="ads.google", timeout_sec=10, regex=False) or wait_url(driver, pattern="accounts.google.com", timeout_sec=10, regex=False)
        wait_dom_stable(driver, idle_ms=800, timeout_sec=12)
        stage("ads:open", "ok" if ok else "warn")
    except Exception as e:
        stage("ads:open", "warn", error=str(e))
        emit("log", {"msg": f"⚠️ Не удалось открыть Ads: {e}"})


_GOOGLE_CORE_NAMES = {
    "SID", "HSID", "SSID", "APISID", "SAPISID", "OSID",
    "__Secure-1PSID", "__Secure-3PSID", "__Secure-OSID",
}


def _check_ads_logged_in(driver) -> tuple[bool, str]:
    try:
        url = getattr(driver, "current_url", "") or ""
    except Exception:
        url = ""
    if "accounts.google.com" in url:
        return False, "redirected_to_login"
    if "ads.google.com" in url:
        return True, ""
    try:
        have = {str(ck.get("name") or "").strip() for ck in (driver.get_cookies() or [])}
        if any(n in have for n in _GOOGLE_CORE_NAMES):
            return True, ""
    except Exception:
        pass
    try:
        title = getattr(driver, "title", "") or ""
        if "Google Ads" in title or "Реклама Google" in title:
            return True, ""
    except Exception:
        pass
    return False, "unknown_state"


def _filter_google_cookies(raw: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    now = int(time.time())
    for c in (raw or []):
        name = c.get("name")
        value = c.get("value")
        if not (name and (value is not None)):
            continue
        domain = (c.get("domain") or "").strip().lower()
        if not domain:
            continue
        if not (domain == "google.com" or domain.endswith(".google.com")):
            continue
        item: Dict[str, Any] = {
            "name": str(name),
            "value": str(value),
            "path": c.get("path") or "/",
            "secure": bool(c.get("secure", True)),
            "httpOnly": bool(c.get("httpOnly", c.get("httponly", False))),
        }
        ss_raw = str(c.get("sameSite") or c.get("SameSite") or "").lower()
        if ss_raw in ("lax", "strict"):
            item["sameSite"] = ss_raw.capitalize()
        elif ss_raw in ("none", "no_restriction", "no-restriction", "unspecified"):
            item["sameSite"] = "None"; item["secure"] = True
        for k in ("expiry", "expirationDate", "expires"):
            if k in c and c[k]:
                try:
                    exp = int(float(c[k]))
                    if exp > now - 60:
                        item["expiry"] = exp
                except Exception:
                    pass
                break
        if domain:
            item["domain"] = domain if domain.startswith(".") else "." + domain
        out.append(item)
    return out


def _delete_adspower_profile(profile_id: str) -> bool:
    headless, api_base, token = _get_adspower_env()
    headers = {"Authorization": token} if token else {}
    urls = [
        f"{api_base}/api/v1/user/delete?user_id={urllib.parse.quote(profile_id)}",
        f"{api_base}/v1/api/user/delete?user_id={urllib.parse.quote(profile_id)}",
    ]
    for u in urls:
        code, body = _http_get_json(u, headers=headers)
        if code and code < 500 and isinstance(body, dict) and (body.get("code") in (0, "0")):
            return True
    posts = [
        (f"{api_base}/api/v1/user/delete", {"user_id": profile_id}),
        (f"{api_base}/v1/api/user/delete", {"user_id": profile_id}),
        (f"{api_base}/api/v1/user/batch_delete", {"user_ids": [profile_id]}),
    ]
    for u, payload in posts:
        code, body = _http_post_json(u, payload, headers=headers)
        if code and code < 500 and isinstance(body, dict) and (body.get("code") in (0, "0")):
            return True
    return False


# ============ Состояние мастера (in‑memory) ============

@dataclass
class _LoginWizard:
    id: str
    user_email: str
    name: str
    profile_id: Optional[str]          # может быть неизвестен на момент рендера
    created_profile: bool
    headless_before: Optional[str]
    created_at: float
    driver: Any = None
    closed: bool = False
    logs: List[str] = field(default_factory=list)
    status: str = "init"               # init -> booting -> ready|error|stopped
    last_error: Optional[str] = None
    otp_secret: Optional[str] = None


_WIZARDS: Dict[str, _LoginWizard] = {}
_WIZ_GUARD = threading.Lock()


def _wiz_get_by_user(email: str) -> Optional[_LoginWizard]:
    with _WIZ_GUARD:
        for w in _WIZARDS.values():
            if w.user_email == email and not w.closed:
                return w
    return None


# ============ Инициализация ============

def init_account_module(app, settings, db: Any, tm: Any) -> None:
    """Регистрация роутов мастера (самодостаточно, без зависимостей от campaigns.py)."""

    # ====== Утилиты ======

    class _ProfileCreateError(RuntimeError):
        def __init__(self, message: str, diag: Dict[str, Any]):
            super().__init__(message)
            self.diag = diag

    def _now_id(prefix: str = "wiz") -> str:
        import uuid
        return f"{prefix}_{uuid.uuid4().hex[:8]}_{int(time.time())}"

    def _wiz_get(wiz_id: str) -> Optional[_LoginWizard]:
        with _WIZ_GUARD:
            return _WIZARDS.get(wiz_id)

    def _wiz_put(wiz: _LoginWizard) -> None:
        with _WIZ_GUARD:
            _WIZARDS[wiz.id] = wiz

    def _wiz_del(wiz_id: str) -> None:
        with _WIZ_GUARD:
            _WIZARDS.pop(wiz_id, None)

    def _wiz_log(wiz: _LoginWizard, msg: str) -> None:
        line = f"{time.strftime('%H:%M:%S')}  {msg}"
        wiz.logs.append(line)
        if len(wiz.logs) > 800:
            wiz.logs[:] = wiz.logs[-800:]
        try:
            _console("wizard:log", {"wiz": wiz.id, "msg": msg})
        except Exception:
            pass

    def _mask_token(tok: str) -> str:
        if not tok:
            return ""
        h = hashlib.sha1(tok.encode("utf-8")).hexdigest()[:8]
        if len(tok) <= 8:
            return f"{tok[:2]}… (sha1:{h})"
        return f"{tok[:4]}…{tok[-4:]} (sha1:{h})"

    def _normalize_otp_secret(value: Optional[str]) -> Optional[str]:
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
        suffix_tokens = [tok.strip() for tok in re.split(r"[;,]", suffix) if tok.strip()]
        if suffix_tokens:
            return clean_secret + "|" + ",".join(suffix_tokens)
        return clean_secret

    def _short_json(data: Any, max_len: int = 600) -> Any:
        try:
            text = json.dumps(data, ensure_ascii=False)
            if len(text) <= max_len:
                return data
            return text[:max_len] + "…"
        except Exception:
            try:
                s = str(data)
                return s[:max_len] + "…" if len(s) > max_len else s
            except Exception:
                return data

    # --------- Разрешение group_id ---------

    def _extract_id(obj: Any, keys: List[str]) -> Optional[str]:
        if not isinstance(obj, dict):
            return None
        for k in keys:
            if k in obj and obj[k] not in (None, "", []):
                return str(obj[k])
        return None

    def _resolve_group_id(api_base: str, token: str) -> Optional[str]:
        headers = {"Authorization": token} if token else {}
        for p in [
            "/api/v1/group/list?page=1&page_size=100",
            "/v1/api/group/list?page=1&page_size=100",
            "/api/v1/group/index?page=1&page_size=100",
            "/v1/api/group/index?page=1&page_size=100",
        ]:
            url = f"{api_base}{p}"
            try:
                code, body = _http_get_json(url, headers=headers, timeout=6.0)
                _console("adspower:create:resolve-group:probe", {"url": url, "code": code})
                if isinstance(body, dict) and str(body.get("code", "")) in ("0", "200"):
                    data = body.get("data") or {}
                    items = data.get("list") or data.get("data") or data.get("items") or []
                    if isinstance(items, list) and items:
                        for it in items:
                            gid = _extract_id(it, ["group_id", "id", "groupId", "groupid"])
                            if gid:
                                _console("adspower:create:resolve-group:pick", {"group_id": gid, "source": "group_list"})
                                return gid
            except Exception as e:
                _console("adspower:create:resolve-group:probe", {"url": url, "error": str(e)})

        # fallback: из существующего профиля
        try:
            url = f"{api_base}/api/v1/user/list?page=1&page_size=1"
            code, body = _http_get_json(url, headers=headers, timeout=6.0)
            _console("adspower:create:resolve-group:userlist", {"url": url, "code": code})
            if isinstance(body, dict) and str(body.get("code", "")) in ("0", "200"):
                lst = (body.get("data") or {}).get("list") or []
                if isinstance(lst, list) and lst:
                    gid = _extract_id(lst[0], ["group_id", "groupId", "groupid"])
                    if gid:
                        _console("adspower:create:resolve-group:pick", {"group_id": gid, "source": "user_list"})
                        return gid
        except Exception as e:
            _console("adspower:create:resolve-group:userlist", {"error": str(e)})

        return None

    # --------- Диагностика ---------

    def _probe_adspower(api_base: str, token: str) -> List[Dict[str, Any]]:
        headers = {"Authorization": token} if token else {}
        results: List[Dict[str, Any]] = []
        for path in ["/status", "/api/v1/user/list?page=1&page_size=1"]:
            url = f"{api_base}{path}"
            try:
                code, body = _http_get_json(url, headers=headers, timeout=4.0)
                rec = {"url": url, "code": code, "body": _short_json(body)}
            except Exception as e:
                rec = {"url": url, "error": str(e)}
            results.append(rec)
            _console("adspower:create:probe", rec)
        return results

    # --------- Fingerprint и прокси ---------

    _DNT_ALLOWED = {"default", "true", "false"}

    def _sanitize_fingerprint(fp: Dict[str, Any]) -> Dict[str, Any]:
        clean = dict(fp or {})
        dnt = str(clean.get("do_not_track") or "").lower()
        if dnt not in _DNT_ALLOWED:
            clean["do_not_track"] = "default"

        if "language" in clean:
            clean["language_switch"] = "0"
        else:
            clean.setdefault("language_switch", "1")

        if str(clean.get("screen_resolution") or "") == "default":
            clean["screen_resolution"] = "none"

        for k in list(clean.keys()):
            if isinstance(clean[k], list) and not clean[k]:
                clean.pop(k, None)

        clean.setdefault("automatic_timezone", "1")
        clean.setdefault("webrtc", "disabled")
        clean.setdefault("canvas", "1")
        clean.setdefault("audio", "1")
        return clean

    def _default_fingerprint_config() -> Dict[str, Any]:
        base = {
            "automatic_timezone": "1",
            "webrtc": "disabled",
            "screen_resolution": "none",
            "language_switch": "0",
            "language": ["en-US", "en"],
            "canvas": "1",
            "audio": "1",
            "do_not_track": "default",
            "random_ua": {
                "ua_browser": ["chrome"],
                "ua_system_version": ["Windows 10", "Windows 11", "Mac OS X 12", "Mac OS X 13"]
            },
        }
        return _sanitize_fingerprint(base)

    def _parse_fp_json(text: Optional[str]) -> Optional[Dict[str, Any]]:
        if not text:
            return None
        try:
            obj = json.loads(text)
            if isinstance(obj, dict) and obj:
                return _sanitize_fingerprint(obj)
        except Exception:
            pass
        return None

    def _build_user_proxy_config(mode: str, manual: Dict[str, str]) -> Optional[Dict[str, Any]]:
        m = (mode or "").strip().lower()
        if m == "no_proxy":
            return {"proxy_soft": "no_proxy"}
        if m == "manual":
            ptype = (manual.get("proxy_type") or "socks5").lower()
            if ptype not in ("socks5", "http", "https"):
                ptype = "socks5"
            cfg = {
                "proxy_soft": "other",
                "proxy_type": ptype,
                "proxy_host": manual.get("proxy_host") or "",
                "proxy_port": manual.get("proxy_port") or "",
                "proxy_user": manual.get("proxy_user") or "",
                "proxy_password": manual.get("proxy_password") or "",
                "proxy_url": manual.get("proxy_url") or "",
                "proxy_partner": manual.get("proxy_partner") or "",
            }
            if not cfg["proxy_host"] or not cfg["proxy_port"]:
                return None
            return cfg
        return None  # proxyid

    # --------- Proxy‑лист AdsPower ---------

    def _query_proxy_list(page: int = 1, limit: int = 100, ids: Optional[List[str]] = None) -> Dict[str, Any]:
        _headless, api_base, token = _get_adspower_env()
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = token
        url = f"{api_base}/api/v2/proxy-list/list"
        body: Dict[str, Any] = {"page": int(page), "limit": int(limit)}
        if ids:
            body["Proxy_id"] = [str(x) for x in ids][:100]
        code, resp = _http_post_json(url, body, headers=headers, timeout=8.0)
        if code != 200 or not isinstance(resp, dict) or str(resp.get("code")) not in ("0", "200"):
            raise RuntimeError(f"Proxy list failed: code={code}, resp={_short_json(resp)}")
        data = (resp.get("data") or {})
        lst = data.get("list") or []
        total = int(data.get("total") or len(lst) or 0)
        page_size = int(data.get("page_size") or limit)
        return {"list": lst, "total": total, "page": int(page), "page_size": page_size}

    # --------- Создание профиля ---------

    def _create_adspower_profile(
        name: str,
        group_id: Optional[str] = None,
        tags: Optional[List[str]] = None,
        proxyid: Optional[Union[str, int]] = None,
        proxy_config: Optional[Dict[str, Any]] = None,
        fp_config: Optional[Dict[str, Any]] = None,
    ) -> str:
        _headless, api_base, token = _get_adspower_env()
        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = token

        probes = _probe_adspower(api_base, token)
        gid_raw = (group_id or "").strip()
        if not gid_raw:
            gid_raw = _resolve_group_id(api_base, token)
        if not gid_raw:
            diag = {
                "api_base": api_base,
                "token_present": bool(token),
                "token_masked": _mask_token(token),
                "reason": "group_id not determined",
                "probes": probes,
            }
            raise _ProfileCreateError("AdsPower: не удалось определить group_id (укажите вручную).", diag)
        gid = str(gid_raw).strip()
        if not gid:
            diag = {
                "api_base": api_base,
                "token_present": bool(token),
                "token_masked": _mask_token(token),
                "reason": "group_id empty after normalize",
                "probes": probes,
            }
            raise _ProfileCreateError("AdsPower: group_id пуст после нормализации.", diag)
        used_proxyid: Optional[Union[str, int]] = proxyid
        if isinstance(used_proxyid, str):
            cand = used_proxyid.strip()
            if not cand:
                used_proxyid = None
            elif cand.lower() == "true":
                # старые формы могли отправлять "true" вместо фактического ID — трактуем как random
                used_proxyid = "random"
            elif cand.lower() == "false":
                used_proxyid = None
            elif cand.lower() == "random":
                used_proxyid = "random"
            else:
                try:
                    used_proxyid = int(cand)
                except Exception:
                    used_proxyid = cand
        elif isinstance(used_proxyid, (int, float)):
            used_proxyid = int(used_proxyid)
        else:
            used_proxyid = None

        fp_raw = fp_config if (isinstance(fp_config, dict) and fp_config) else _default_fingerprint_config()
        fp = _sanitize_fingerprint(fp_raw)

        used_user_proxy_config = proxy_config
        if used_user_proxy_config is None and used_proxyid is None:
            used_user_proxy_config = {"proxy_soft": "no_proxy"}

        base_payload = {"name": name, "tags": tags or ["MissionControl"], "fingerprint_config": fp}
        payloads: List[Dict[str, Any]] = []
        p = dict(base_payload)
        p["group_id"] = gid  # API ожидает именно строковый group_id
        if used_user_proxy_config is not None:
            p["user_proxy_config"] = used_user_proxy_config
        if used_proxyid is not None:
            p["proxyid"] = used_proxyid  # поддерживает "random"
        compact = {k: v for k, v in p.items() if v not in (None, "", [], {})}
        payloads.append(compact)

        tries: List[Dict[str, Any]] = []
        last_code = None
        last_body = None

        url = f"{api_base}/api/v1/user/create"
        error_msgs: List[str] = []
        for payload in payloads:
            rec = {"url": url, "payload_keys": list(payload.keys())}
            _console("adspower:create:try", {**rec, "token_present": bool(token)})
            code, body = _http_post_json(url, payload, headers=headers, timeout=15.0)
            last_code, last_body = code, body
            rec.update({"code": code, "body": _short_json(body)})
            tries.append(rec)
            if isinstance(body, dict) and str(body.get("code", "")) in ("0", "200"):
                data = body.get("data") or {}
                cand = (data.get("user_id") or data.get("id") or data.get("profile_id") or data.get("profileId"))
                if cand:
                    _console("adspower:create:ok", {"url": url, "id": str(cand), "group_id": gid})
                    return str(cand)
            msg = ""
            if isinstance(body, dict):
                msg = str(body.get("msg") or body.get("message") or "")
            msg = msg.strip()
            if msg:
                if msg not in error_msgs:
                    error_msgs.append(msg)
            if msg and msg.lower() not in ("group_id is required",):
                # другие ошибки не зависят от смены ключа — прерываем цикл
                break

        diag = {
            "api_base": api_base,
            "token_present": bool(token),
            "token_masked": _mask_token(token),
            "group_id": gid,
            "endpoints_tried": [url],
            "payload_variants": [list(p.keys()) for p in payloads],
            "probe_results": probes,
            "tries": tries,
            "last_code": last_code,
            "last_body": last_body,
            "messages": error_msgs,
        }
        _console("adspower:create:error", {"message": "profile creation failed", "api_base": api_base, "last_code": last_code})
        msg = ""
        if error_msgs:
            msg = "; ".join(error_msgs)
        elif isinstance(last_body, dict):
            msg = str(last_body.get("msg") or last_body.get("message") or "")
        raise _ProfileCreateError(
            f"AdsPower create failed: status={last_code}, base={api_base}{(' — '+msg) if msg else ''}",
            diag=diag,
        )

    # --------- Хелперы ожидания selenium‑порта (используются в fallback) ---------

    def _strip_ws_scheme(addr: str) -> str:
        s = (addr or "").strip()
        s = re.sub(r"^(ws|wss|http|https)://", "", s, flags=re.I)
        if "/" in s:
            s = s.split("/", 1)[0]
        return s

    def _split_host_port(addr: str) -> tuple[str, int]:
        s = _strip_ws_scheme(addr)
        host, _, port_s = s.partition(":")
        try:
            return host, int(port_s)
        except Exception:
            return host, 0

    def _wait_port_open(addr: str, timeout_sec: float = 15.0) -> bool:
        """Ждём, когда selenium‑порт станет доступен (после /start бывает пауза)."""
        host, port = _split_host_port(addr)
        if not host or not port:
            return False
        t_end = time.time() + timeout_sec
        while time.time() < t_end:
            try:
                with socket.create_connection((host, port), timeout=1.2):
                    return True
            except OSError:
                time.sleep(0.25)
        return False

    # --------- Fallback‑старт драйвера через v2 (если start_adspower не сработал) ---------

    def _start_driver_v2_fallback(wiz: _LoginWizard, profile_id: str):
        """
        POST /api/v2/browser-profile/start — fallback для редких сборок AdsPower.
        Возвращает Selenium WebDriver, привязанный к AdsPower.
        """
        _headless, api_base, token = _get_adspower_env()
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = token

        launch_args = [
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-search-engine-choice-screen",
            "--disable-notifications",
            "--disable-features=Translate,OptimizationHints,AutofillKeychain,PasswordManagerOnboarding",
            "--disable-background-networking",
            "--disable-sync",
            "--no-service-autorun",
            "--no-experiments",
            "--disable-extensions",
            "--mute-audio",
            "--window-size=1600,900",
            "--no-proxy-server",
        ]
        try:
            hd = (os.getenv("ADS_AI_HEADLESS") or "").strip().lower() not in ("", "0", "false")
        except Exception:
            hd = False
        if hd:
            launch_args.append("--headless=new")
        body = {
            "profile_id": profile_id,
            "last_opened_tabs": "0",
            "proxy_detection": "0",
            "headless": "1" if hd else "0",
            "launch_args": launch_args,
        }

        last_err = None
        for attempt in range(1, 4):
            t0 = time.perf_counter()
            try:
                code, resp = _http_post_json(f"{api_base}/api/v2/browser-profile/start", body, headers=headers, timeout=30.0)
                if code == 200 and isinstance(resp, dict) and str(resp.get("code")) in ("0", "200"):
                    data = resp.get("data") or {}
                    webdriver_path = data.get("webdriver")
                    sel_addr = ((data.get("ws") or {}).get("selenium"))  # "127.0.0.1:xxxxx"
                    if not webdriver_path or not sel_addr:
                        raise RuntimeError("AdsPower: webdriver/ws not returned")

                    _wiz_log(wiz, f"Ожидаем доступность порта {sel_addr}…")
                    if not _wait_port_open(sel_addr, timeout_sec=15.0):
                        _wiz_log(wiz, f"⚠ Порт {sel_addr} не открылся за 15 секунд (пробуем attach всё равно).")
                    else:
                        _wiz_log(wiz, f"✓ Порт {sel_addr} доступен.")

                    service = ChromeService(executable_path=webdriver_path)
                    options = ChromeOptions()
                    options.add_experimental_option("debuggerAddress", sel_addr)
                    drv = webdriver.Chrome(service=service, options=options)

                    # Сохраним минимальные метаданные как в start_adspower
                    try:
                        meta_ws = (data.get("ws") or {}).get("devtools") or data.get("wsEndpoint") or data.get("webSocketDebuggerUrl")
                        drv._adspower = {  # type: ignore[attr-defined]
                            "profile_id": profile_id,
                            "selenium_addr": str(sel_addr),
                            "devtools_ws": str(meta_ws) if meta_ws else None,
                            "webdriver_path": str(webdriver_path),
                            "api_base": str(api_base),
                            "token": str(token or ""),
                        }
                    except Exception:
                        pass

                    # Санитария окон
                    try:
                        handles = drv.window_handles
                        if len(handles) > 1:
                            base = handles[0]
                            for h in handles[1:]:
                                drv.switch_to.window(h)
                                drv.close()
                            drv.switch_to.window(base)
                        drv.set_window_size(1600, 900)
                    except Exception:
                        pass

                    return drv
                last_err = RuntimeError(f"Open browser failed: code={code}, resp={_short_json(resp)}")
            except Exception as e:
                last_err = e

            time.sleep(1.2 * attempt + 0.3)
            _console("wizard:driver_retry", {
                "attempt": attempt,
                "elapsed_ms": int((time.perf_counter()-t0)*1000),
                "error": str(last_err)
            })

        raise last_err or RuntimeError("Open browser failed (fallback v2)")

    # ====== Общий CSS (темизация) и JS‑helpers) ======

    AWIZ_CSS = r"""
<style>
  /* Светлая тема по умолчанию (токены) */
  .awiz {
    --panel:#ffffff;
    --panel-2:#f8fafc;
    --line:#e5e7eb;
    --text:#0b1220;
    --muted:#64748b;
    --accent:#0ea5e9;
    --accent-2:#2dd4bf;
    --danger:#dc2626;
    --ok:#16a34a;
    --shadow:0 8px 24px rgba(2,6,23,.08);

    --btn-grad1:#f8fafc; --btn-grad2:#eef2f7; --btn-text:var(--text);
    --btnp1:#0ea5e9; --btnp2:#0284c7; --btnptext:#ffffff;

    --proxy-hover-bg:#f3f7fb;
    --proxy-selected-bg:#e6f6fe;
    --proxy-selected-outline:#bae6fd;

    --frame-bg:#f8fafc;
    --log-bg:#f8fafc;

    --skel-a:#f1f5f9; --skel-b:#e2e8f0;

    --scrollbar-thumb:#cbd5e1;
    --scrollbar-thumb-hover:#94a3b8;

    --tag-border:#d1d5db;

    --err-bg:#fff5f5; --err-border:#fecaca; --err-text:#7f1d1d;
  }

  /* Тёмная тема — активируется только явным индикатором предка */
  :where(html, body, #app, .app, :root):where(
      [data-theme="dark"],
      [data-theme="dark" i],
      [data-mode="dark"],
      [data-bs-theme="dark"],
      [theme="dark"],
      [data-color-mode="dark"],
      .theme-dark,
      .dark,
      .Dark
  ) .awiz {
    --panel:#0d0f12;
    --panel-2:#0f1216;
    --line:#1d232d;
    --text:#e8ecf1;
    --muted:#98a2b3;
    --accent:#6ee7ff;
    --accent-2:#2dd4bf;
    --danger:#ff4d4d;
    --ok:#22c55e;
    --shadow:0 8px 24px rgba(0,0,0,.25);

    --btn-grad1:#12161b; --btn-grad2:#0d1116; --btn-text:var(--text);
    --btnp1:#0ea5e9; --btnp2:#0891b2; --btnptext:#00131a;

    --proxy-hover-bg:#121821;
    --proxy-selected-bg:#0e1b26;
    --proxy-selected-outline:#1e3347;

    --frame-bg:#0b0e12;
    --log-bg:#0b0f13;

    --skel-a:#0b0e12; --skel-b:#11161d;

    --scrollbar-thumb:#1f2a37;
    --scrollbar-thumb-hover:#2a3a4c;

    --tag-border:#2a3442;

    --err-bg:#211114; --err-border:#4a2028; --err-text:#ffd8df;
  }

  .awiz * { box-sizing: border-box; }
  .awiz .card { background: var(--panel); border:1px solid var(--line); border-radius: 12px; box-shadow: var(--shadow); overflow: clip; }
  .awiz .head { padding: 12px 14px; border-bottom:1px solid var(--line); display:flex; align-items:center; justify-content:space-between; }
  .awiz .head .title { font-weight:700; color:var(--text); letter-spacing:.2px }
  .awiz .body { padding: 14px; color: var(--text); }
  .awiz .hr { height:1px; background:var(--line); margin:8px 0; }
  .awiz .grid { display:grid; gap:12px }
  .awiz .grid.cols-2 { grid-template-columns: 1.2fr .8fr; }
  .awiz .row { display:grid; grid-template-columns: 180px 1fr; gap:10px; align-items:center; margin:8px 0; }
  .awiz .row.full { grid-template-columns: 1fr; }
  .awiz .label { color:var(--muted); font-size:13px }
  .awiz .inp { width:100%; background:var(--panel-2); border:1px solid var(--line); border-radius:10px; padding:10px 12px; color:var(--text); outline:none; transition: border-color .18s ease, box-shadow .18s ease, transform .06s ease; will-change: transform }
  .awiz .inp:focus-visible { border-color: var(--accent); box-shadow: 0 0 0 3px rgba(14,165,233,.15) }
  .awiz .inp::placeholder { color:#94a3b8 }

  .awiz .btn {
    position:relative; display:inline-flex; align-items:center; justify-content:center;
    border:1px solid var(--line); background:linear-gradient(180deg, var(--btn-grad1), var(--btn-grad2));
    color:var(--btn-text); padding:8px 12px; border-radius:10px; cursor:pointer; user-select:none;
    transition: transform .06s ease, border-color .18s ease, opacity .2s ease; overflow:hidden;
  }
  .awiz .btn:hover { border-color:#cbd5e1 }
  .awiz .btn:active { transform: translateY(1px) }
  .awiz .btn.primary { background: linear-gradient(180deg, var(--btnp1), var(--btnp2)); border-color: var(--btnp1); color: var(--btnptext); font-weight:800 }
  .awiz .btn.primary:hover { filter: brightness(1.03) }
  .awiz .btn[disabled] { opacity:.6; cursor:not-allowed }
  .awiz .actions .btn { margin-right:8px; }

  .awiz .btn .ripple { position:absolute; border-radius:50%; transform: scale(0); animation: awiz-ripple .6s linear; background: rgba(255,255,255,.25); pointer-events:none; }
  @keyframes awiz-ripple { to { transform: scale(4); opacity: 0; } }

  .awiz .toasts { position:fixed; right:16px; bottom:16px; display:flex; flex-direction:column; gap:10px; z-index: 9999; }
  .awiz .toast { min-width: 260px; max-width: 480px; background: var(--panel-2); border:1px solid var(--line); border-left:4px solid var(--accent-2); color: var(--text); padding:10px 12px; border-radius:12px; box-shadow: var(--shadow); opacity:0; transform: translateY(10px); animation: awiz-appear .18s ease forwards; }
  .awiz .toast.err { border-left-color: var(--danger) }
  .awiz .toast.ok { border-left-color: var(--ok) }
  @keyframes awiz-appear { to { opacity:1; transform: translateY(0); } }

  .awiz .frame { border:1px solid var(--line); background:var(--frame-bg); display:flex; align-items:center; justify-content:center; border-radius: 12px; position:relative; overflow:hidden; }
  /* Увеличим высоту кадра для удобной работы */
  .awiz .frame { width: 720px; height: 460px; } /* ← фикс: явная высота, чтобы превью не схлопывалось */
  .awiz .frame .preview { width:100%; height:100%; object-fit:contain; display:block; }
  .awiz .frame .viewer { width:100%; height:100%; border:0; display:block; background:#000; }
  .awiz .frame .skeleton { position:absolute; inset:0; background: linear-gradient(90deg, var(--skel-a) 0%, var(--skel-b) 50%, var(--skel-a) 100%); background-size: 200% 100%; animation: awiz-skeleton 1.2s ease infinite; }
  @keyframes awiz-skeleton { 0%{ background-position: 0% 0 } 100%{ background-position: 200% 0 } }

  .awiz .remote-bar { margin-top:12px; display:flex; gap:8px; align-items:flex-start; }
  .awiz .remote-bar textarea { flex:1; min-height:46px; max-height:140px; resize:vertical; background:var(--panel-2); border:1px solid var(--line); border-radius:10px; padding:10px 12px; color:var(--text); outline:none; transition:border-color .18s ease, box-shadow .18s ease; }
  .awiz .remote-bar textarea:focus-visible { border-color: var(--accent); box-shadow: 0 0 0 3px rgba(14,165,233,.15); }
  .awiz .remote-bar .btn { white-space:nowrap; }
  .awiz .remote-hint { font-size:12px; color:var(--muted); margin-top:4px; }

  .awiz .proxy-panel { position:sticky; top:0; background: linear-gradient(180deg, rgba(255,255,255,.95), rgba(255,255,255,.88)); border-bottom:1px solid var(--line); padding:8px; display:flex; gap:8px; align-items:center; z-index:2; backdrop-filter:saturate(1.1) blur(6px); }
  :where(.dark, .theme-dark, [data-theme="dark"], [data-bs-theme="dark"]) .awiz .proxy-panel { background: linear-gradient(180deg, rgba(13,15,18,.95), rgba(13,15,18,.88)); }

  .awiz .proxy-list-wrap { border:1px solid var(--line); border-radius:12px; overflow:hidden; background: var(--panel-2); }
  .awiz .proxy-list { max-height: 280px; overflow:auto; overscroll-behavior: contain; scroll-behavior: smooth; }
  .awiz .proxy-item { display:grid; grid-template-columns:auto 1fr auto; gap:8px; align-items:center; padding:10px 12px; border-bottom:1px solid #e5e7eb20; cursor:pointer; transition: background .12s ease, transform .06s ease; contain: content; }
  .awiz .proxy-item:hover { background: var(--proxy-hover-bg) }
  .awiz .proxy-item.selected { background: var(--proxy-selected-bg); box-shadow: inset 0 0 0 1px var(--proxy-selected-outline); }
  .awiz .proxy-item[aria-selected="true"] { outline: 2px solid var(--accent); outline-offset:-2px; }
  .awiz .proxy-addr { font-weight:600; color:var(--text) }
  .awiz .proxy-remark { color:var(--muted) }
  .awiz .tag { font-size:11px; padding:2px 6px; border:1px solid var(--tag-border); border-radius:999px; color:var(--muted); }
  .awiz .badge.free { background:#10b981; color:#fff; padding:2px 6px; border-radius:4px; font-size:11px; }
  .awiz .muted { color:var(--muted); }
  .awiz .awiz-badge.err { background:var(--err-bg); color:var(--err-text); border:1px solid var(--err-border); border-left:4px solid var(--danger); padding:8px 10px; border-radius:10px; }

  .awiz #log { max-height:260px; min-height: 220px; overflow:auto; background: var(--log-bg); border:1px solid var(--line); border-radius: 12px; padding:8px; color: var(--text); }

  .awiz *::-webkit-scrollbar { width: 10px; height: 10px; }
  .awiz *::-webkit-scrollbar-thumb { background: var(--scrollbar-thumb); border-radius: 10px; border:2px solid var(--panel-2); }
  .awiz *::-webkit-scrollbar-thumb:hover { background: var(--scrollbar-thumb-hover); }

  .awiz input[type="radio"] { accent-color: var(--accent) }

  .awiz .card { opacity:0; transform: translateY(4px); animation: awiz-card-in .18s ease forwards; }
  @keyframes awiz-card-in { to { opacity:1; transform: translateY(0); } }

  @keyframes awiz-ripple { to { transform: scale(4); opacity: 0; } }
  @keyframes awiz-appear { to { opacity:1; transform: translateY(0); } }
  @keyframes awiz-skeleton { 0%{ background-position: 0% 0 } 100%{ background-position: 200% 0 } }

  @media (prefers-reduced-motion: reduce) {
    .awiz .btn, .awiz .proxy-item, .awiz .card, .awiz .toast { transition: none !important; animation: none !important; }
    .awiz .frame .skeleton { animation: none !important; }
  }
</style>
"""

    AWIZ_JS_HELPERS = r"""
<script>
  // === Общие утилиты (риппл, тосты, debounce) ===
  function awizRipple(e){
    const b=e.currentTarget; const r=document.createElement('span');
    const rect=b.getBoundingClientRect();
    const d=Math.max(rect.width, rect.height);
    r.className='ripple'; r.style.width=r.style.height=d+'px';
    r.style.left=(e.clientX-rect.left - d/2)+'px';
    r.style.top=(e.clientY-rect.top - d/2)+'px';
    b.appendChild(r); setTimeout(()=>r.remove(), 600);
  }
  function debounce(fn, ms){ let t; return (...a)=>{ clearTimeout(t); t=setTimeout(()=>fn(...a), ms||280); } }
  function esc(s){ const d=document.createElement('div'); d.innerText=(s==null?'':s); return d.innerHTML; }

  // Тосты
  const _awiz_toasts = [];
  function toast(msg, type){
    let box=document.querySelector('.awiz .toasts'); if(!box){ box=document.createElement('div'); box.className='toasts'; document.body.appendChild(box); }
    const el=document.createElement('div'); el.className='toast '+(type||''); el.setAttribute('role','status'); el.innerHTML=esc(msg);
    box.appendChild(el); _awiz_toasts.push(el);
    setTimeout(()=>{ el.style.opacity='0'; el.style.transform='translateY(10px)'; setTimeout(()=>el.remove(), 180); }, 3200);
  }

  // Антидубль сабмита + риппл на кнопках
  document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('.awiz .btn').forEach(b=> b.addEventListener('click', awizRipple));
    const form = document.getElementById('add-form');
    if(!form) return;
    form.addEventListener('submit', () => {
      const btn = form.querySelector('button[type=submit]');
      if(btn){ btn.disabled = true; btn.innerText = 'Запуск...'; }
    }, { once:true });
  });
</script>
"""

    # ====== Асинхронный bootstrap мастера ======

    def _boot_wizard_async(
        wiz: _LoginWizard,
        group_id: Optional[str],
        proxy_mode: str,
        proxyid_str: Optional[str],
        manual_proxy: Dict[str, str],
        fp_json_text: Optional[str],
        manual_profile_id: Optional[str],
    ) -> None:
        wiz.status = "booting"
        t0_all = time.perf_counter()
        lock_acquired = False
        profile_lock = None
        try:
            _wiz_log(wiz, "→ Bootstrap: старт мастера")
            _console("wizard:boot", {"wiz": wiz.id, "stage": "start", "user": wiz.user_email})

            # 0) Диагностика окружения AdsPower
            headless_env, api_base, token = _get_adspower_env()
            _wiz_log(wiz, f"Env: api_base={api_base}, token_present={bool(token)}, headless_env={headless_env}")

            # 1) Профиль
            if manual_profile_id:
                wiz.profile_id = manual_profile_id.strip()
                wiz.created_profile = False
                _wiz_log(wiz, f"✓ Использую существующий профиль: {wiz.profile_id}")
            else:
                # собираем параметры создания
                mode = (proxy_mode or "").lower()
                proxyid = None
                if mode == "proxyid":
                    proxyid = proxyid_str.strip() if proxyid_str else None  # допускается 'random'
                upc = _build_user_proxy_config(mode, manual_proxy)
                fp = _parse_fp_json(fp_json_text) or _default_fingerprint_config()

                _wiz_log(wiz, f"Параметры профиля: group_id={group_id or '(auto)'}; proxy_mode={mode}; "
                               f"proxyid={'random' if (proxyid and proxyid=='random') else bool(proxyid)}; "
                               f"user_proxy_config={'set' if upc else 'None'}; fp_keys={list(fp.keys())[:8]}…")

                t0 = time.perf_counter()
                try:
                    wiz.profile_id = _create_adspower_profile(
                        name=wiz.name,
                        group_id=(group_id or None),
                        tags=["MissionControl"],
                        proxyid=proxyid,
                        proxy_config=upc,
                        fp_config=fp,
                    )
                except _ProfileCreateError as e:
                    _wiz_log(wiz, f"✖ Создание профиля не удалось: {e}")
                    try:
                        _wiz_log(wiz, "Диагностика: " + json.dumps(_short_json(e.diag, 800), ensure_ascii=False))
                    except Exception:
                        pass
                    raise
                wiz.created_profile = True
                _wiz_log(wiz, f"✓ Профиль создан: {wiz.profile_id} ({int((time.perf_counter()-t0)*1000)} ms)")

            if not wiz.profile_id:
                raise RuntimeError("profile_id недоступен после шага создания/назначения")

            # 2) Фоновый режим (headless) на время мастера, чтобы окно AdsPower не крало фокус
            wiz.headless_before = os.getenv("ADS_AI_HEADLESS")
            os.environ["ADS_AI_HEADLESS"] = "1"
            _wiz_log(wiz, f"Headless: было={wiz.headless_before!r}, установлено='1' на время мастера (без фокуса окна)")

            # 3) Лок на профиль
            t0 = time.perf_counter()
            profile_lock = tm._profile_lock(wiz.profile_id)
            if not profile_lock.acquire(timeout=60.0):
                raise RuntimeError("Профиль занят (lock timeout)")
            lock_acquired = True
            _wiz_log(wiz, f"✓ Лок профиля получен ({int((time.perf_counter()-t0)*1000)} ms)")

            # 4) Старт драйвера — основной путь: start_adspower
            t0 = time.perf_counter()
            _wiz_log(wiz, "→ Запуск браузера через AdsPower API (start_adspower)…")
            try:
                wiz.driver = start_adspower(
                    profile=wiz.profile_id,
                    headless=False,          # мастер всегда видимый
                    api_base=api_base,
                    token=token,
                    window_size="1600,900",
                    timeout=45.0,
                )
                _wiz_log(wiz, f"✓ Драйвер готов ({int((time.perf_counter()-t0)*1000)} ms)")
            except AdsPowerError as e:
                _wiz_log(wiz, f"⚠ start_adspower не удался: {e}. Пробуем fallback v2…")
                # --- Fallback v2 ---
                t1 = time.perf_counter()
                wiz.driver = _start_driver_v2_fallback(wiz, wiz.profile_id)
                _wiz_log(wiz, f"✓ Драйвер готов через fallback v2 ({int((time.perf_counter()-t1)*1000)} ms)")

            # 5) Навигация
            try:
                wiz.driver.get("https://accounts.google.com/")
                time.sleep(0.5)
                wiz.driver.get("https://ads.google.com/aw/overview")
                _wiz_log(wiz, "✓ Открыл страницы входа Google/Ads — войдите и нажмите «Проверить и сохранить».")
            except Exception as e:
                _wiz_log(wiz, f"⚠ Навигация: {e}")

            wiz.status = "ready"
            _wiz_log(wiz, f"✔ Готово за {int((time.perf_counter()-t0_all)*1000)} ms")

        except Exception as e:
            wiz.status = "error"
            wiz.last_error = str(e)
            _wiz_log(wiz, f"✖ Ошибка bootstrap: {e}")
            _console("wizard:boot:error", {"wiz": wiz.id, "error": str(e), "trace": traceback.format_exc()})

            # ВАЖНО: чисто освобождаем ресурсы при ошибке
            try:
                if wiz.driver:
                    _stop_adspower_driver(wiz.driver)
            except Exception:
                pass
        finally:
            # Восстановить headless
            try:
                if wiz.headless_before is not None:
                    os.environ["ADS_AI_HEADLESS"] = wiz.headless_before
                else:
                    os.environ.pop("ADS_AI_HEADLESS", None)
            except Exception:
                pass
            # Освободить лок, если брали
            if lock_acquired and profile_lock:
                try:
                    profile_lock.release()
                    _wiz_log(wiz, "Лок профиля освобождён (finally).")
                except Exception:
                    pass

    # ====== UI helpers ======

    def _wizard_form_html(csrf: str, defaults: Dict[str, Any]) -> str:
        err = defaults.get("error")
        err_html = f'<div role="alert" class="awiz-badge err" style="margin-bottom:8px">{_escape(err)}</div>' if err else ""
        details = defaults.get("error_details")
        details_html = f'<details class="awiz-muted"><summary>Детали ошибки</summary><pre class="awiz-log" style="white-space:pre-wrap;margin-top:6px">{_escape(details)}</pre></details>' if details else ""

        defv = lambda k, v="": _escape(defaults.get(k, v))

        js_vars = f"""
<script>
  const __CSRF__ = {json.dumps(csrf)};
</script>
"""

        # Оборачиваем форму в собственную карточку; сам «хром» страницы даёт shell/aside.
        html_form = f"""
<div class="awiz">
  <div class="card">
    <div class="head"><div class="title">Добавить аккаунт через вход</div></div>
    <div class="body">
      {err_html}
      {details_html}
      <form id="add-form" method="post" action="/accounts/add/login" class="form" autocomplete="off" novalidate>
        <input type="hidden" name="_csrf" value="{_escape(csrf)}" />
        <div class="row"><label class="label" for="acc_name">Имя аккаунта</label>
          <input class="inp" id="acc_name" type="text" name="name"
                 value="{defv('name')}" placeholder="Напр. google-1" required/></div>

        <div class="row"><label class="label" for="profile_id">profile_id (если уже есть)</label>
          <input class="inp" id="profile_id" type="text" name="profile_id"
                 value="{defv('profile_id')}" placeholder="Если уже создан в AdsPower"/></div>

        <div class="row"><label class="label" for="group_id">group_id (опц.)</label>
          <input class="inp" id="group_id" type="text" name="group_id"
                 value="{defv('group_id')}" placeholder="Напр. 6750993"/></div>

        <div class="row"><label class="label" for="otp_secret">TOTP secret</label>
          <input class="inp" id="otp_secret" type="text" name="otp_secret"
                 value="{defv('otp_secret')}" placeholder="Base32 секрет или otpauth://…"/></div>
        <div class="row full awiz-muted" style="font-size:12px">
          <div>Секрет сохраняется для аккаунта и используется для генерации кодов подтверждения.</div>
        </div>

        <div class="hr"></div>
        <div class="row"><label class="label" for="proxy_mode">Прокси</label>
          <select class="inp" id="proxy_mode" name="proxy_mode" onchange="onProxyModeChange()" aria-label="Режим выбора прокси">
            <option value="no_proxy" {"selected" if defaults.get("proxy_mode","no_proxy")=="no_proxy" else ""}>Без прокси (no_proxy)</option>
            <option value="proxyid" {"selected" if defaults.get("proxy_mode")=="proxyid" else ""}>Из библиотеки (proxyid)</option>
            <option value="manual" {"selected" if defaults.get("proxy_mode")=="manual" else ""}>Ручной ввод</option>
          </select>
        </div>

        <div class="row full" id="row-proxyid" style="display:none">
          <input type="hidden" id="proxyid" name="proxyid" value="{defv('proxyid')}" />
          <div class="proxy-list-wrap" role="region" aria-label="Библиотека прокси">
            <div class="proxy-panel">
              <input class="inp" id="proxy_q" type="text" placeholder="Поиск: host / remark / tag (Ctrl+F)" style="min-width:260px" aria-label="Поиск по прокси" />
              <label style="display:flex;align-items:center;gap:6px"><input type="checkbox" id="proxy_free" /> <span class="muted">Только свободные</span></label>
              <label style="display:flex;align-items:center;gap:6px"><span class="muted">Сортировка</span>
                <select class="inp" id="proxy_sort" aria-label="Сортировка"><option value="pop">Свободные ↑</option><option value="alpha">А‑Я</option><option value="fav">Избранное ↑</option></select>
              </label>
              <button type="button" class="btn" id="proxy_reload" title="Обновить список (R)">Обновить</button>
              <button type="button" class="btn" id="proxy_random" title="Пусть AdsPower сам подберёт">Random</button>
              <button type="button" class="btn" id="proxy_test" title="Быстрый тест выбранного прокси (T)">Проверить прокси</button>
              <span class="muted" id="proxy_stats" aria-live="polite"></span>
            </div>
            <div class="proxy-list" id="proxy_list" role="listbox" aria-label="Proxy list" tabindex="0"></div>
            <div class="muted" style="padding:6px 8px">★ — локальное «Избранное». Навигация: ↑/↓ — выбор, Enter — применить, F — избранное, R — обновить, T — тест.</div>
          </div>
        </div>

        <div id="row-manual" style="display:none">
          <div class="row"><label class="label" for="proxy_type">proxy_type</label>
            <input class="inp" id="proxy_type" type="text" name="proxy_type" value="{defv('proxy_type','socks5')}" placeholder="socks5/http/https"/></div>
          <div class="row"><label class="label" for="proxy_host">proxy_host</label>
            <input class="inp" id="proxy_host" type="text" name="proxy_host" value="{defv('proxy_host')}" placeholder="host"/></div>
          <div class="row"><label class="label" for="proxy_port">proxy_port</label>
            <input class="inp" id="proxy_port" type="text" name="proxy_port" value="{defv('proxy_port')}" placeholder="port"/></div>
          <div class="row"><label class="label" for="proxy_user">proxy_user</label>
            <input class="inp" id="proxy_user" type="text" name="proxy_user" value="{defv('proxy_user')}" placeholder="user (опц.)"/></div>
          <div class="row"><label class="label" for="proxy_password">proxy_password</label>
            <input class="inp" id="proxy_password" type="password" name="proxy_password" value="{defv('proxy_password')}" placeholder="password (опц.)"/></div>
        </div>

        <details class="awiz-muted" style="margin:8px 0"><summary>Расширенные настройки (опц.)</summary>
          <div class="row"><label class="label" for="fp_json">fingerprint_config (JSON)</label>
            <textarea id="fp_json" class="inp" name="fp_json" rows="6" placeholder='Напр. {{"automatic_timezone":"1","webrtc":"disabled"}}'>{_escape(defaults.get("fp_json",""))}</textarea>
          </div>
        </details>

        <div class="row full" style="color:var(--muted);font-size:12px">
          <div>Если <b>group_id</b> не указан — мастер попытается определить его автоматически.</div>
          <div>Если нет прокси — используйте <b>Без прокси</b> (no_proxy).</div>
          <div>Далее откроется окно профиля: войдите в Google → нажмите «Проверить и сохранить».</div>
        </div>

        <div class="row full actions">
          <button class="btn primary" type="submit">Начать вход</button>
          <a class="btn" href="/accounts">Отмена</a>
        </div>
      </form>
    </div>
  </div>
  <div class="toasts" aria-live="polite" aria-atomic="true"></div>
</div>
"""

        # Логика списка прокси / хоткеи / тест — не f‑строка (без вмешательства Python)
        script_logic = """
<script>
  // ======== ProxyList state + логика ========
  const proxyState = {
    items: [],
    page: 1,
    limit: 100,
    total: 0,
    q: '',
    onlyFree: false,
    sort: 'pop', // pop|alpha|fav
    favs: new Set(JSON.parse(localStorage.getItem('adsp_proxy_favs')||'[]')),
    pick(id){
      localStorage.setItem('adsp_proxyid', id);
      document.getElementById('proxyid').value = id;
      toast('Прокси выбран: '+id, 'ok');
      renderProxyList();
    },
    isFav(id){ return this.favs.has(String(id)); },
    toggleFav(id){ const k=String(id); if(this.favs.has(k)) this.favs.delete(k); else this.favs.add(k); localStorage.setItem('adsp_proxy_favs', JSON.stringify([...this.favs])); renderProxyList(); },
    selectedIndex: -1,
  };
  function labelOf(it){ const remark = it.remark ? ` — ${it.remark}` : ''; return `${it.host}:${it.port} (${it.type})${remark}`; }
  function cmp(a,b){
    const s = proxyState.sort;
    if(s==='alpha'){ return labelOf(a).localeCompare(labelOf(b)); }
    if(s==='fav'){
      const af = proxyState.isFav(a.proxy_id)?1:0, bf = proxyState.isFav(b.proxy_id)?1:0;
      if(af!==bf) return bf-af;
    }
    const pa = parseInt(a.profile_count||'0')||0, pb = parseInt(b.profile_count||'0')||0;
    if(pa!==pb) return pa-pb;
    return labelOf(a).localeCompare(labelOf(b));
  }
  async function loadProxies(reset){
    const q = document.getElementById('proxy_q').value.trim();
    const onlyFree = document.getElementById('proxy_free').checked;
    const sort = document.getElementById('proxy_sort').value;
    proxyState.q = q; proxyState.onlyFree = onlyFree; proxyState.sort = sort;
    if(reset) proxyState.page = 1;

    const url = new URL('/integrations/adspower/proxy/list', window.location.origin);
    url.searchParams.set('page', String(proxyState.page));
    url.searchParams.set('limit', String(proxyState.limit));
    if(q) url.searchParams.set('q', q);
    if(onlyFree) url.searchParams.set('only_free', '1');

    // skeleton
    const list = document.getElementById('proxy_list');
    list.innerHTML = '';
    for(let i=0;i<6;i++){
      const sk=document.createElement('div');
      sk.className='proxy-item'; sk.style.opacity='.65';
      sk.innerHTML='<div class="skeleton" style="height:18px;width:18px;border-radius:50%"></div><div class="skeleton" style="height:14px;border-radius:6px"></div><div class="skeleton" style="height:14px;width:60px;border-radius:6px"></div>';
      list.appendChild(sk);
    }

    try{
      const r = await fetch(url, { headers: { 'X-CSRF': __CSRF__ } });
      if(!r.ok) throw new Error(await r.text());
      const j = await r.json();
      proxyState.items = j.items||[];
      proxyState.total = j.total||proxyState.items.length;
      proxyState.selectedIndex = -1;
      renderProxyList();
      toast('Загружено прокси: '+proxyState.items.length, 'ok');
    }catch(e){
      list.innerHTML=''; toast('Ошибка загрузки прокси: '+(e.message||e), 'err');
    }
  }
  function renderProxyList(){
    const box = document.getElementById('proxy_list'); box.innerHTML = '';
    const sel = (document.getElementById('proxyid').value||localStorage.getItem('adsp_proxyid')||'').trim();
    const items = [...proxyState.items].sort(cmp);
    let visibleIndex=0;
    for(const it of items){
      if(proxyState.onlyFree && String(it.profile_count||'0')!=='0') continue;
      const fav = proxyState.isFav(it.proxy_id) ? '★' : '☆';
      const remark = it.remark ? `<span class="proxy-remark"> — ${esc(it.remark)}</span>` : '';
      const free = String(it.profile_count||'0')==='0' ? '<span class="badge free">free</span>' : `<span class="muted">used:${esc(it.profile_count)}</span>`;
      const isSel = (String(it.proxy_id)===sel);
      const id = 'proxy_'+it.proxy_id;
      const div = document.createElement('div');
      div.className = 'proxy-item'+(isSel?' selected':'');
      div.setAttribute('role', 'option');
      div.setAttribute('id', id);
      div.setAttribute('aria-selected', isSel?'true':'false');
      div.setAttribute('tabindex', '0');
      div.innerHTML = `
        <input type="radio" name="proxyid_radio" ${isSel?'checked':''} aria-label="Выбрать прокси" />
        <div class="proxy-label">
          <div class="proxy-addr">${esc(it.host)}:${esc(it.port)} (${esc(it.type)}) ${remark}</div>
          <div class="muted" style="margin-top:2px; display:flex; gap:6px; flex-wrap:wrap">
            ${(it.proxy_tags||[]).map(t=>`<span class="tag">${esc(t.name||'tag')}</span>`).join('')}
          </div>
        </div>
        <div style="display:flex; gap:6px; align-items:center">
          ${free}
          <button type="button" class="btn" data-act="fav" title="Избранное (F)">${fav}</button>
        </div>
      `;
      div.addEventListener('click', (ev)=>{
        if(ev.target && ev.target.getAttribute('data-act')==='fav'){ ev.stopPropagation(); proxyState.toggleFav(it.proxy_id); return; }
        document.getElementById('proxyid').value = String(it.proxy_id);
        localStorage.setItem('adsp_proxyid', String(it.proxy_id));
        [...box.querySelectorAll('.proxy-item')].forEach(x=>{ x.classList.remove('selected'); x.setAttribute('aria-selected','false'); });
        div.classList.add('selected'); div.setAttribute('aria-selected','true');
        const radio = div.querySelector('input[type=radio]'); if(radio) radio.checked = true;
      });
      // клавиатурный Enter/F внутри фокуса
      div.addEventListener('keydown', (e)=>{
        if(e.key==='Enter'){ proxyState.pick(String(it.proxy_id)); e.preventDefault(); }
        if((e.key==='f' || e.key==='F')){ proxyState.toggleFav(String(it.proxy_id)); e.preventDefault(); }
      });
      box.appendChild(div);
      if(isSel) proxyState.selectedIndex = visibleIndex;
      visibleIndex++;
    }
    document.getElementById('proxy_stats').innerText = `${items.length} из ${proxyState.total}`;
  }

  async function testProxy(btn){
    const pid = (document.getElementById('proxyid').value||'').trim();
    if(!pid) { toast('Сначала выберите proxy', 'err'); return; }
    try {
      if(btn) btn.setAttribute('data-busy','1');
      const r = await fetch('/integrations/adspower/proxy/test', {
        method:'POST', headers:{'Content-Type':'application/json','X-CSRF':__CSRF__}, body: JSON.stringify({ proxy_id: pid })
      });
      const j = await r.json().catch(()=>({}));
      if(!r.ok || j.ok===false) throw new Error(j.error||'Тест не удался');
      toast(`OK: ${j.ip||'ip ?'} (latency ~${j.ms||'?'} ms)`, 'ok');
    } catch(e) {
      toast(String(e.message||e), 'err');
    } finally {
      if(btn) btn.removeAttribute('data-busy');
    }
  }

  function onProxyModeChange(){
    const mode = document.getElementById('proxy_mode').value;
    document.getElementById('row-proxyid').style.display = (mode === 'proxyid') ? '' : 'none';
    document.getElementById('row-manual').style.display = (mode === 'manual')  ? '' : 'none';
    if(mode==='proxyid'){ loadProxies(true).catch(err=>toast(String(err), 'err')); }
  }

  // Хоткеи и инициализация
  document.addEventListener('DOMContentLoaded', ()=>{
    onProxyModeChange();
    const last = localStorage.getItem('adsp_proxyid');
    if(last) document.getElementById('proxyid').value = last;

    const doLoad = debounce(()=>loadProxies(true), 280);
    const qEl = document.getElementById('proxy_q');
    qEl.addEventListener('input', doLoad);
    document.getElementById('proxy_free').addEventListener('change', ()=>loadProxies(true));
    document.getElementById('proxy_sort').addEventListener('change', ()=>loadProxies(false));
    document.getElementById('proxy_reload').addEventListener('click', ()=>loadProxies(true));
    document.getElementById('proxy_random').addEventListener('click', ()=>{
      document.getElementById('proxyid').value = 'random';
      localStorage.setItem('adsp_proxyid', 'random');
      toast('Выбран режим Random', 'ok');
      renderProxyList();
    });
    const testBtn = document.getElementById('proxy_test');
    testBtn.addEventListener('click', ()=>testProxy(testBtn));

    // Клавиатура: Ctrl+F, ↑/↓/Enter/F/R/T
    document.addEventListener('keydown', (e)=>{
      if((e.ctrlKey || e.metaKey) && e.key.toLowerCase()==='f'){ qEl.focus(); qEl.select(); e.preventDefault(); }
      const list = document.getElementById('proxy_list');
      if(document.getElementById('row-proxyid').style.display==='none') return;
      const items = [...list.querySelectorAll('.proxy-item')];
      if(!items.length) return;

      if(e.key==='ArrowDown' || e.key==='ArrowUp'){
        e.preventDefault();
        if(proxyState.selectedIndex<0){ proxyState.selectedIndex=0; }
        else { proxyState.selectedIndex += (e.key==='ArrowDown'?1:-1); }
        if(proxyState.selectedIndex<0) proxyState.selectedIndex=0;
        if(proxyState.selectedIndex>=items.length) proxyState.selectedIndex=items.length-1;
        items.forEach(x=>x.setAttribute('aria-selected','false'));
        const el = items[proxyState.selectedIndex];
        el.setAttribute('aria-selected','true'); el.focus({preventScroll:false});
        el.scrollIntoView({ block:'nearest', inline:'nearest' });
      }
      if(e.key==='Enter' && proxyState.selectedIndex>=0){
        const el = items[proxyState.selectedIndex];
        const id = el?.id?.replace('proxy_','');
        if(id){ proxyState.pick(id); }
      }
      if((e.key==='f' || e.key==='F') && proxyState.selectedIndex>=0){
        const el = items[proxyState.selectedIndex];
        const id = el?.id?.replace('proxy_','');
        if(id){ proxyState.toggleFav(id); }
      }
      if((e.key==='r' || e.key==='R') && !e.ctrlKey && !e.metaKey){ loadProxies(true); }
      if((e.key==='t' || e.key==='T') && !e.ctrlKey && !e.metaKey){ testProxy(testBtn); }
    });
  });
</script>
"""
        return AWIZ_CSS + AWIZ_JS_HELPERS + html_form + js_vars + script_logic

    def _wizard_page_html(wiz: _LoginWizard, csrf: str) -> str:
        js = f"""
<script>
  const __WIZ_ID__ = {json.dumps(wiz.id)};
  const __CSRF__ = {json.dumps(csrf)};

  // SSE‑логи
  const es = new EventSource('/accounts/wizard/'+__WIZ_ID__+'/events');
  es.addEventListener('open', ()=> line('SSE: подключено'));
  es.addEventListener('log',  (e)=>{{ let j={{}}; try{{ j=JSON.parse(e.data||'{{}}') }}catch(_e){{}}; if (j.msg) line(esc(j.msg)); }});
  es.addEventListener('hello',(_)=> line('Мастер готов к работе'));
  es.onerror = ()=>{{}};

  function esc(s){{ const d=document.createElement('div'); d.innerText=(s==null?'':s); return d.innerHTML; }}
  function line(s){{ const d=document.getElementById('log'); const x=document.createElement('div'); x.innerHTML=s; d.appendChild(x); d.scrollTop=d.scrollHeight; }}

  // скриншоты — двойная буферизация + skeleton
  let shotBusy = false;
  function refreshShot(){{
    if(shotBusy) return;
    shotBusy = true;
    const preload = new Image();
    preload.onload = () => {{ try {{ const img=document.getElementById('shot'); if(img) img.src = preload.src; const sk=document.querySelector('#viewer-box .skeleton'); if(sk) sk.style.display='none'; }} finally {{ shotBusy = false; }} }};
    preload.onerror = () => {{ shotBusy = false; }};
    preload.decoding = 'async';
    preload.src = '/accounts/wizard/'+__WIZ_ID__+'/shot?r=' + Date.now();
  }}
  // ~5–8 fps
  window.__shotTimer = setInterval(refreshShot, 200); refreshShot();

  // Метрики вьюпорта страницы профиля
  let __metrics = {{ w:1280, h:800, dpr:1 }};
  async function loadMetrics(){{
    try{{ const r = await fetch('/accounts/wizard/'+__WIZ_ID__+'/metrics'); const j = await r.json(); if(j && j.ok && j.metrics){{ __metrics = j.metrics; }} }}catch(_ ){{ }}
  }}
  setInterval(loadMetrics, 1000); loadMetrics();

  // Преобразование координат клика по изображению → координаты окна браузера (CSS px)
  function mapPoint(e){{
    const img = document.getElementById('shot');
    const rect = img.getBoundingClientRect();
    const cw = rect.width, ch = rect.height; const vw = __metrics.w || 1280, vh = __metrics.h || 800;
    const ratio = Math.min(cw/vw, ch/vh);
    const dw = vw*ratio, dh = vh*ratio;
    const offX = rect.left + (cw - dw)/2; const offY = rect.top + (ch - dh)/2;
    const x = (e.clientX - offX) / ratio; const y = (e.clientY - offY) / ratio;
    return {{ x: Math.max(0, Math.min(vw-1, Math.round(x))), y: Math.max(0, Math.min(vh-1, Math.round(y))) }};
  }}

  function modsFromEvent(e){{ return (e.altKey?1:0) | (e.ctrlKey?2:0) | (e.metaKey?4:0) | (e.shiftKey?8:0); }}

  // Отправка действий на сервер (CDP Input)
  async function sendInput(payload){{
    try{{ await fetch('/accounts/wizard/'+__WIZ_ID__+'/input', {{ method:'POST', headers:{{'Content-Type':'application/json','X-CSRF':__CSRF__}}, body: JSON.stringify(payload) }}) }}
    catch(_ ){{ }}
  }}

  // Навешиваем обработчики: клики, печать, колесо мыши (прокрутка)
  function enableRemoteControl(){{
    const img = document.getElementById('shot');
    if(!img || img.__rcEnabled) return; img.__rcEnabled = true;
    img.style.cursor = 'crosshair';
    img.addEventListener('click', (e)=>{{ const p = mapPoint(e); sendInput({{ type:'click', x:p.x, y:p.y, button:'left' }}); }});
    img.addEventListener('dblclick', (e)=>{{ const p = mapPoint(e); sendInput({{ type:'doubleClick', x:p.x, y:p.y, button:'left' }}); }});
    img.addEventListener('contextmenu', (e)=>{{ e.preventDefault(); const p = mapPoint(e); sendInput({{ type:'click', x:p.x, y:p.y, button:'right' }}); }});

    // Колесо мыши → реальная прокрутка в окне профиля (mouseWheel)
    img.addEventListener('wheel', (e)=>{{
      e.preventDefault();
      const p = mapPoint(e);
      const mods = modsFromEvent(e);
      const dx = Math.round(e.deltaX || 0);
      const dy = Math.round(e.deltaY || 0);
      sendInput({{ type:'scroll', x:p.x, y:p.y, deltaX:dx, deltaY:dy, modifiers:mods }});
    }}, {{ passive:false }});

    // Фокус на превью → отправляем клавиши
    let active = false;
    img.addEventListener('mouseenter', ()=>{{ active = true; }});
    img.addEventListener('mouseleave', ()=>{{ active = false; }});
    document.addEventListener('keydown', (e)=>{{
      if(!active) return;
      const k = e.key; const code = e.code || '';
      const mods = modsFromEvent(e);
      const special = ['Enter','Backspace','Tab','Escape','ArrowLeft','ArrowRight','ArrowUp','ArrowDown','Delete','Home','End','PageUp','PageDown'];
      // Ctrl/Cmd+V — вставка через insertText
      if((e.ctrlKey || e.metaKey) && (k==='v' || k==='V')){{
        try{{ navigator.clipboard.readText().then((t)=>{{ if(t) sendInput({{ type:'text', text:t }}); }}); }}catch(_ ){{}}
        e.preventDefault(); return;
      }}
      if(e.altKey || e.ctrlKey || e.metaKey || special.includes(k) || k.length>1){{
        sendInput({{ type:'key', key:k, code:code, modifiers:mods }}); e.preventDefault(); return;
      }}
      if(k.length===1){{ sendInput({{ type:'text', text:k }}); e.preventDefault(); }}
    }});
  }}

  const remoteText = document.getElementById('remoteText');
  const remoteTextSend = document.getElementById('remoteTextSend');
  if(remoteText && remoteTextSend){{
    const sendRemote = () => {{
      const val = remoteText.value;
      if(!val){{ remoteText.focus(); return; }}
      sendInput({{ type:'text', text:val }});
      remoteText.value = '';
      remoteText.focus();
      toast('Текст отправлен', 'ok');
    }};
    remoteTextSend.addEventListener('click', sendRemote);
    remoteText.addEventListener('keydown', (e)=>{{
      if(e.key==='Enter' && !e.shiftKey){{ e.preventDefault(); sendRemote(); }}
    }});
  }}

  // Попытка подключить DevTools‑viewer
  let viewerAttached = false;
  async function tryAttachViewer(){{
    if(viewerAttached) return;
    try{{
      const r = await fetch('/accounts/wizard/'+__WIZ_ID__+'/viewer-src');
      if(!r.ok) return;
      const j = await r.json();
      if(!j.ok || !j.url) return;
      const box = document.getElementById('viewer-box');
      if(!box) return;
      const iframe = document.createElement('iframe');
      iframe.className = 'viewer';
      iframe.setAttribute('allow', 'clipboard-read; clipboard-write');
      iframe.addEventListener('load', ()=>{{ const sk=document.querySelector('#viewer-box .skeleton'); if(sk) sk.style.display='none'; try{{ window.clearInterval(window.__shotTimer); }}catch(_ ){{ }} }});
      iframe.src = j.url;
      box.innerHTML = '';
      box.appendChild(iframe);
      viewerAttached = true;
    }} catch(_e){{}}
  }}

  async function post(url, data){{
    const r = await fetch(url, {{method:'POST', headers:{{'Content-Type':'application/json','X-CSRF':__CSRF__}}, body: JSON.stringify(data||{{}})}});
    if(!r.ok) throw new Error(await r.text());
    return r.json().catch(()=>({{}}));
  }}

  function setDisabled(dis){{
    document.getElementById('btn-open').disabled = dis;
    document.getElementById('btn-refresh').disabled = dis;
    document.getElementById('btn-save').disabled = dis;
    const bsUp = document.getElementById('btn-scroll-up'); if(bsUp) bsUp.disabled = dis;
    const bsDn = document.getElementById('btn-scroll-down'); if(bsDn) bsDn.disabled = dis;
  }}

  // Поллинг статуса
  async function pollState(){{
    try {{
      const r = await fetch('/accounts/wizard/'+__WIZ_ID__+'/state');
      const j = await r.json();
      if(j.ok){{
        document.getElementById('wiz_status').innerText = j.status.toUpperCase();
        document.getElementById('wiz_profile').innerText = j.profile_id || '—';
        setDisabled(!j.driver_ready);
        if(j.driver_ready){{ tryAttachViewer(); }}
        if(j.status==='error' && j.last_error){{ toast('Ошибка: '+j.last_error, 'err'); }}
      }}
    }} catch(_){{}}
  }}
  setInterval(pollState, 1000);
  pollState();
  enableRemoteControl();

  // Тосты
  function toast(msg, type){{
    let box=document.querySelector('.awiz .toasts'); if(!box){{ box=document.createElement('div'); box.className='toasts'; document.body.appendChild(box); }}
    const el=document.createElement('div'); el.className='toast '+(type||''); el.setAttribute('role','status'); el.innerHTML=msg;
    box.appendChild(el); setTimeout(()=>{{ el.style.opacity='0'; el.style.transform='translateY(10px)'; setTimeout(()=>el.remove(), 180); }}, 3200);
  }}

  function centerPoint(){{
    const vw = __metrics.w || 1280, vh = __metrics.h || 800;
    return {{ x: Math.round(vw/2), y: Math.round(vh/2) }};
  }}

  window._acc_actions = {{
    open_ads: async ()=>{{ try {{ await post('/accounts/wizard/'+__WIZ_ID__+'/nav', {{to:'ads'}}); refreshShot(); toast('Открыт Ads','ok'); }} catch(e){{ toast(e.message||e, 'err'); }} }},
    refresh:  async ()=>{{ try {{ await post('/accounts/wizard/'+__WIZ_ID__+'/nav', {{to:'refresh'}}); refreshShot(); toast('Страница обновлена','ok'); }} catch(e){{ toast(e.message||e, 'err'); }} }},
    save:     async ()=>{{ try {{ const j = await post('/accounts/wizard/'+__WIZ_ID__+'/save', {{}}); line('✔ Сохранено. Аккаунт: '+(j.account_id||'—')); window.location.href = '/accounts'; }} catch(e){{ toast(e.message||e, 'err'); }} }},
    stop:     async ()=>{{ const del = confirm('Удалить профиль AdsPower (если был создан в мастере)?\\nОК — удалить, Отмена — оставить.'); try {{ await post('/accounts/wizard/'+__WIZ_ID__+'/stop', {{ delete_profile: del }}); window.location.href = '/accounts'; }} catch(e){{ toast(e.message||e, 'err'); }} }},

    // Прокрутка ↑/↓ по центру окна профиля
    scroll:   async (delta)=>{{ try {{ const p = centerPoint(); await sendInput({{ type:'scroll', x:p.x, y:p.y, deltaX:0, deltaY:delta, modifiers:0 }}); }} catch(e){{ toast(e.message||e, 'err'); }} }},
  }};
</script>
"""
        html = f"""
<div class="awiz">
  <div class="grid">
    <div class="card">
      <div class="head">
        <div class="title">Вход в Google · {_escape(wiz.name)}</div>
        <div class="muted" style="display:flex;gap:8px;align-items:center">
          <span class="tag">Профиль: <b id="wiz_profile">{_escape(wiz.profile_id or '—')}</b></span>
          <span class="tag">Статус: <b id="wiz_status">{_escape(wiz.status.upper())}</b></span>
        </div>
      </div>
      <div class="body">
        <div class="actions" style="margin-bottom:8px">
          <button class="btn" id="btn-open" onclick="_acc_actions.open_ads()" disabled>Открыть Ads</button>
          <button class="btn" id="btn-refresh" onclick="_acc_actions.refresh()" disabled>Обновить вкладку</button>
          <button class="btn" id="btn-scroll-up" onclick="_acc_actions.scroll(-480)" disabled title="Прокрутка вверх (PgUp)">Прокрутка ↑</button>
          <button class="btn" id="btn-scroll-down" onclick="_acc_actions.scroll(480)" disabled title="Прокрутка вниз (PgDn)">Прокрутка ↓</button>
          <button class="btn primary" id="btn-save" onclick="_acc_actions.save()" style="font-weight:800" disabled>Проверить и сохранить</button>
          <button class="btn" onclick="_acc_actions.stop()">Отменить и закрыть</button>
        </div>
        <div class="grid cols-2">
          <div>
            <div class="frame">
              <div id="viewer-box" style="width:100%;height:100%">
                <img id="shot" class="preview" alt="скриншот" />
                <div class="skeleton" aria-hidden="true"></div>
              </div>
            </div>
            <div class="remote-bar">
              <textarea id="remoteText" class="inp" placeholder="Вставьте текст, который нужно отправить в активное поле"></textarea>
              <button type="button" class="btn" id="remoteTextSend">Вставить</button>
            </div>
            <div class="remote-hint">При наведении на превью можно печатать напрямую. Поле выше позволяет вставить большие куски текста.</div>
          </div>
          <div>
            <div class="title" style="margin-bottom:6px">Логи</div>
            <div class="log" id="log" aria-live="polite"></div>
            <div class="hr"></div>
            <div style="color:var(--muted);font-size:12px">
              Крутите колесо мыши по превью, используйте PgUp/PgDn или кнопки «Прокрутка ↑/↓».
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
  <div class="toasts" aria-live="polite" aria-atomic="true"></div>
</div>
"""
        return AWIZ_CSS + AWIZ_JS_HELPERS + html + js

    # ====== Роуты ======

    # Совместимость: /accounts/new → форма мастера входа
    @app.get("/accounts/new")
    def accounts_new_redirect():
        from flask import redirect, url_for
        return redirect(url_for("accounts_add_login_get"))

    @app.get("/accounts/add/login")
    def accounts_add_login_get() -> Response:
        try:
            _ = _require_user()
        except Exception:
            from flask import redirect, url_for
            return redirect(url_for("auth_login"))
        return redirect("/accounts")

    @app.post("/accounts/add/login")
    def accounts_add_login_post() -> Response:
        try:
            _ = _require_user()
        except Exception:
            from flask import redirect, url_for
            return redirect(url_for("auth_login"))
        return make_response("Добавление аккаунтов отключено", 410)

    # ====== API: список прокси и тест ======

    @app.get("/integrations/adspower/proxy/list")
    def integrations_adspower_proxy_list() -> Response:
        try:
            _ = _require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401

        page = max(1, int((request.args.get("page") or 1)))
        limit = min(200, max(1, int((request.args.get("limit") or 100))))
        q = (request.args.get("q") or "").strip().lower()
        only_free = (request.args.get("only_free") or "") in ("1", "true", "yes")

        try:
            data = _query_proxy_list(page=page, limit=limit)
            items = data.get("list") or []
            # серверная фильтрация
            if q:
                def _match(it: Dict[str, Any]) -> bool:
                    hay = [it.get("host",""), it.get("remark",""), " ".join([t.get("name","") for t in it.get("proxy_tags",[])])]
                    return any(q in str(x).lower() for x in hay if x)
                items = [it for it in items if _match(it)]
            if only_free:
                items = [it for it in items if str(it.get("profile_count","0")) == "0"]

            out = []
            for it in items:
                out.append({
                    "proxy_id": it.get("proxy_id"),
                    "type": it.get("type"),
                    "host": it.get("host"),
                    "port": it.get("port"),
                    "user": it.get("user"),
                    "password": it.get("password"),
                    "remark": it.get("remark"),
                    "proxy_tags": it.get("proxy_tags") or [],
                    "profile_count": it.get("profile_count") or "0",
                })
            return jsonify({"ok": True, "items": out, "total": int(data.get("total") or len(out)), "page": page, "limit": limit})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.post("/integrations/adspower/proxy/test")
    def integrations_adspower_proxy_test() -> Response:
        try:
            _ = _require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        _check_csrf(request.headers.get("X-CSRF"))

        payload = request.get_json(silent=True) or {}
        proxy_id = str(payload.get("proxy_id") or "").strip()
        if not proxy_id:
            return jsonify({"ok": False, "error": "proxy_id required"}), 400

        try:
            data = _query_proxy_list(page=1, limit=1, ids=[proxy_id])
            lst = data.get("list") or []
            if not lst:
                return jsonify({"ok": False, "error": "proxy not found"}), 404
            it = lst[0]
            import requests, time as _t
            proxies = {}
            proto = (it.get("type") or "http").lower()
            auth = ""
            if it.get("user") and it.get("password"):
                auth = f"{it['user']}:{it['password']}@"
            host = f"{auth}{it.get('host')}:{it.get('port')}"
            proxies["http"]  = f"{proto}://{host}"
            proxies["https"] = f"{proto}://{host}"
            t0 = _t.time()
            r = requests.get("https://api.ipify.org?format=json", timeout=5.0, proxies=proxies)
            ms = int((_t.time() - t0)*1000)
            if r.ok:
                j = {}
                try: j = r.json()
                except Exception: pass
                return jsonify({"ok": True, "ip": j.get("ip") or "ok", "ms": ms})
            return jsonify({"ok": False, "error": f"http {r.status_code}"}), 502
        except Exception as e:
            return jsonify({"ok": False, "error": f"connect failed: {e}"}), 502

    # ====== Сессия мастера ======

    @app.get("/accounts/wizard/<wiz_id>/state")
    def accounts_wizard_state(wiz_id: str) -> Response:
        try:
            email = _require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        wiz = _wiz_get(wiz_id)
        if not wiz or wiz.user_email != email:
            return jsonify({"ok": False, "error": "not_found"}), 404
        return jsonify({
            "ok": True,
            "wiz": wiz.id,
            "status": wiz.status,
            "profile_id": wiz.profile_id,
            "created_profile": bool(wiz.created_profile),
            "driver_ready": bool(wiz.driver),
            "logs_len": len(wiz.logs),
            "last_error": wiz.last_error,
        })

    @app.get("/accounts/wizard/<wiz_id>/events")
    def accounts_wizard_events(wiz_id: str) -> Response:
        try:
            email = _require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        wiz = _wiz_get(wiz_id)
        if not wiz or wiz.user_email != email:
            return jsonify({"ok": False, "error": "not_found"}), 404

        # Логируем подключение SSE — видно в системном логе, а в UI будет отдельная строка от клиента
        _wiz_log(wiz, "SSE: сервер подтвердил подключение клиента")

        def gen():
            yield "retry: 2000\n\n"
            yield "event: hello\n"
            yield "data: {}\n\n"
            last = 0
            while True:
                try:
                    lines = wiz.logs[last:]
                    if lines:
                        for ln in lines:
                            last += 1
                            payload = json.dumps({"msg": ln}, ensure_ascii=False)
                            yield "event: log\n"
                            yield f"data: {payload}\n\n"
                    time.sleep(0.6)
                except GeneratorExit:
                    break
                except Exception:
                    time.sleep(0.8)

        resp = Response(gen(), mimetype="text/event-stream")
        resp.headers["Cache-Control"] = "no-cache"
        resp.headers["X-Accel-Buffering"] = "no"
        return resp

    @app.get("/accounts/wizard/<wiz_id>/shot")
    def accounts_wizard_shot(wiz_id: str) -> Response:
        try:
            email = _require_user()
        except Exception:
            return make_response("unauthorized", 401)
        wiz = _wiz_get(wiz_id)
        if not wiz or wiz.user_email != email:
            return make_response("not found", 404)
        if not wiz.driver:
            # драйвер ещё не готов — даём 202, чтобы фронт продолжил попытки
            return make_response("pending", 202)
        # Сначала пробуем обычный метод Selenium
        try:
            png: bytes = wiz.driver.get_screenshot_as_png()
            buf = BytesIO(png)
            return send_file(buf, mimetype="image/png")
        except Exception:
            pass
        # Фолбэк: через CDP Page.captureScreenshot (устойчиво работает с attach‑драйвером)
        try:
            res = wiz.driver.execute_cdp_cmd("Page.captureScreenshot", {"format": "png", "fromSurface": True})
            data = res.get("data") if isinstance(res, dict) else None
            if data:
                png = base64.b64decode(data)
                buf = BytesIO(png)
                return send_file(buf, mimetype="image/png")
        except Exception:
            pass
        return make_response("no shot", 500)

    # Возвращает URL для DevTools‑viewer, если AdsPower предоставил devtools websocket
    @app.get("/accounts/wizard/<wiz_id>/viewer-src")
    def accounts_wizard_viewer_src(wiz_id: str) -> Response:
        try:
            email = _require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        wiz = _wiz_get(wiz_id)
        if not wiz or wiz.user_email != email or not wiz.driver:
            return jsonify({"ok": False, "error": "not_found"}), 404
        try:
            ws = None
            try:
                meta = getattr(wiz.driver, "_adspower", {})
                if isinstance(meta, dict):
                    ws = meta.get("devtools_ws")
            except Exception:
                ws = None
            if not ws:
                return jsonify({"ok": True, "url": None})
            ws = str(ws)
            # Преобразуем ws://host:port/devtools/... -> http://host:port/devtools/inspector.html?ws=host:port/devtools/...
            host = re.sub(r"^(ws|wss)://", "", ws, flags=re.I)
            http_base = "http://" + host.split("/", 1)[0]
            qs = host
            url = f"{http_base}/devtools/inspector.html?ws={qs}"
            return jsonify({"ok": True, "url": url})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.post("/accounts/wizard/<wiz_id>/nav")
    def accounts_wizard_nav(wiz_id: str) -> Response:
        try:
            email = _require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        _check_csrf(request.headers.get("X-CSRF"))
        wiz = _wiz_get(wiz_id)
        if not wiz or wiz.user_email != email or not wiz.driver:
            return jsonify({"ok": False, "error": "not_found"}), 404
        data = request.get_json(silent=True) or {}
        action = (data.get("to") or "").lower()
        try:
            if action == "ads":
                _wiz_log(wiz, "Навигация на Ads…")
                _open_google_ads(wiz.driver, lambda *_a, **_k: None, lambda *_a, **_k: None)
            elif action == "refresh":
                wiz.driver.refresh()
                _wiz_log(wiz, "Страница обновлена")
            else:
                return jsonify({"ok": False, "error": "unknown"}), 400
            return jsonify({"ok": True})
        except Exception as e:
            _wiz_log(wiz, f"⚠ Навигация: {e}")
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.post("/accounts/wizard/<wiz_id>/save")
    def accounts_wizard_save(wiz_id: str) -> Response:
        try:
            email = _require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        _check_csrf(request.headers.get("X-CSRF"))
        wiz = _wiz_get(wiz_id)
        if not wiz or wiz.user_email != email or not wiz.driver:
            return jsonify({"ok": False, "error": "not_found"}), 404
        try:
            _open_google_ads(wiz.driver, lambda *_a, **_k: None, lambda *_a, **_k: None)
            logged, reason = _check_ads_logged_in(wiz.driver)
            _wiz_log(wiz, f"Статус авторизации: {'OK' if logged else 'нет'}{(' ('+reason+')') if reason else ''}")

            raw = wiz.driver.get_cookies() or []
            _wiz_log(wiz, f"Получено cookies: {len(raw)} шт., фильтруем только google.com/ads.google.com…")
            filtered = _filter_google_cookies(raw)
            if not filtered:
                raise RuntimeError("Не удалось получить валидные cookies (google.com/ads.google.com)")

            acc_id = db.add_account(email, wiz.profile_id, wiz.name, filtered, otp_secret=wiz.otp_secret)
            _wiz_log(wiz, f"✔ Куки сохранены. ID аккаунта: {acc_id}")
            if wiz.otp_secret:
                _wiz_log(wiz, "TOTP secret сохранён для аккаунта.")

            _finish_wizard(wiz, delete_profile_if_created=False)
            return jsonify({"ok": True, "account_id": acc_id, "logged_in": bool(logged)})

        except Exception as e:
            _wiz_log(wiz, f"⚠ Сохранение не удалось: {e}")
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.get("/accounts/wizard/<wiz_id>/metrics")
    def accounts_wizard_metrics(wiz_id: str) -> Response:
        try:
            email = _require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        wiz = _wiz_get(wiz_id)
        if not wiz or wiz.user_email != email or not wiz.driver:
            return jsonify({"ok": False, "error": "not_found"}), 404
        try:
            m = wiz.driver.execute_script(
                "return {w:window.innerWidth||0,h:window.innerHeight||0,dpr:window.devicePixelRatio||1};"
            ) or {}
            return jsonify({"ok": True, "metrics": {"w": int(m.get("w") or 0), "h": int(m.get("h") or 0), "dpr": float(m.get("dpr") or 1.0)}})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.post("/accounts/wizard/<wiz_id>/input")
    def accounts_wizard_input(wiz_id: str) -> Response:
        # Управление мышью/клавиатурой через CDP: Input.dispatchMouseEvent / Input.dispatchKeyEvent / Input.insertText / mouseWheel
        try:
            email = _require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        _check_csrf(request.headers.get("X-CSRF"))
        wiz = _wiz_get(wiz_id)
        if not wiz or wiz.user_email != email or not wiz.driver:
            return jsonify({"ok": False, "error": "not_found"}), 404
        try:
            data = request.get_json(silent=True) or {}
            kind = str(data.get("type") or "").lower()

            if kind in ("click", "doubleclick", "down", "up", "move"):
                x = int(float(data.get("x") or 0))
                y = int(float(data.get("y") or 0))
                btn = str(data.get("button") or "left").lower()
                if btn not in ("left", "right", "middle", "none"):
                    btn = "left"
                mods = int(data.get("modifiers") or 0)
                tp = (
                    "mouseMoved"
                    if kind == "move"
                    else (
                        "mousePressed"
                        if kind in ("down",)
                        else ("mouseReleased" if kind in ("up",) else "mousePressed")
                    )
                )
                clicks = 2 if kind == "doubleclick" else 1
                if kind in ("click", "doubleclick"):
                    wiz.driver.execute_cdp_cmd("Input.dispatchMouseEvent", {"type": "mouseMoved", "x": x, "y": y, "button": "none", "modifiers": mods})
                    wiz.driver.execute_cdp_cmd("Input.dispatchMouseEvent", {"type": "mousePressed", "x": x, "y": y, "button": btn, "clickCount": clicks, "modifiers": mods})
                    wiz.driver.execute_cdp_cmd("Input.dispatchMouseEvent", {"type": "mouseReleased", "x": x, "y": y, "button": btn, "clickCount": clicks, "modifiers": mods})
                else:
                    wiz.driver.execute_cdp_cmd("Input.dispatchMouseEvent", {"type": tp, "x": x, "y": y, "button": btn, "clickCount": 1, "modifiers": mods})
                return jsonify({"ok": True})

            if kind in ("scroll", "wheel"):
                # Прокрутка через CDP mouseWheel
                x = int(float(data.get("x") or 0))
                y = int(float(data.get("y") or 0))
                dx = int(float(data.get("deltaX") or 0))
                dy = int(float(data.get("deltaY") or 0))
                mods = int(data.get("modifiers") or 0)
                # Безопасные пределы
                dx = max(-10000, min(10000, dx))
                dy = max(-10000, min(10000, dy))
                wiz.driver.execute_cdp_cmd("Input.dispatchMouseEvent", {
                    "type": "mouseWheel", "x": x, "y": y, "deltaX": dx, "deltaY": dy, "modifiers": mods
                })
                return jsonify({"ok": True})

            if kind == "text":
                text = str(data.get("text") or "")
                if text:
                    wiz.driver.execute_cdp_cmd("Input.insertText", {"text": text})
                return jsonify({"ok": True})

            if kind == "key":
                key = str(data.get("key") or "")
                code = str(data.get("code") or "")
                mods = int(data.get("modifiers") or 0)

                def _vk_for(k: str, c: str) -> int:
                    m = {
                        "Backspace": 8, "Tab": 9, "Enter": 13, "Escape": 27,
                        "ArrowLeft": 37, "ArrowUp": 38, "ArrowRight": 39, "ArrowDown": 40,
                        "Delete": 46, "Home": 36, "End": 35, "PageUp": 33, "PageDown": 34,
                    }
                    if k in m:
                        return m[k]
                    if c.startswith("Key") and len(c) == 4:
                        ch = c[-1].upper()
                        return ord(ch)
                    if c.startswith("Digit") and len(c) == 6 and c[-1].isdigit():
                        return ord(c[-1])
                    if len(k) == 1:
                        return ord(k.upper())
                    return 0

                vk = _vk_for(key, code)
                payload = {"type": "keyDown", "key": key or None, "code": code or None, "modifiers": mods}
                if len(key) == 1 and not (mods & 0b111):  # без модификаторов — можно передать text
                    payload["text"] = key
                    payload["unmodifiedText"] = key
                if vk:
                    payload["windowsVirtualKeyCode"] = vk
                    payload["nativeVirtualKeyCode"] = vk
                wiz.driver.execute_cdp_cmd("Input.dispatchKeyEvent", {k: v for k, v in payload.items() if v is not None})
                payload_up = dict(payload)
                payload_up["type"] = "keyUp"
                for k in ("text", "unmodifiedText"):
                    payload_up.pop(k, None)
                wiz.driver.execute_cdp_cmd("Input.dispatchKeyEvent", {k: v for k, v in payload_up.items() if v is not None})
                return jsonify({"ok": True})

            return jsonify({"ok": False, "error": "unknown_input"}), 400
        except Exception as e:
            _wiz_log(wiz, f"⚠ Input error: {e}")
            return jsonify({"ok": False, "error": str(e)}), 500

    def _finish_wizard(wiz: _LoginWizard, delete_profile_if_created: bool = False) -> None:
        # Восстановить headless
        if wiz.headless_before is not None:
            os.environ["ADS_AI_HEADLESS"] = wiz.headless_before
        else:
            os.environ.pop("ADS_AI_HEADLESS", None)

        # Закрыть драйвер
        try:
            _stop_adspower_driver(wiz.driver)
            _wiz_log(wiz, "Драйвер закрыт.")
        except Exception:
            pass
        wiz.driver = None
        wiz.closed = True
        wiz.status = "stopped"

        # Освободить лок профиля (на случай, если поток не дошёл до finally)
        try:
            if wiz.profile_id:
                tm._profile_lock(wiz.profile_id).release()
                _wiz_log(wiz, "Лок профиля освобождён (stop).")
        except Exception:
            pass

        # По запросу — удалить профиль, если создавали в мастере
        if delete_profile_if_created and wiz.created_profile and callable(_delete_adspower_profile):
            try:
                if not _delete_adspower_profile(wiz.profile_id):
                    _wiz_log(wiz, "⚠ Профиль AdsPower удалить не удалось (возможно, уже отсутствует).")
            except Exception as e:
                _wiz_log(wiz, f"⚠ Ошибка удаления профиля: {e}")

        _wiz_del(wiz.id)

    @app.post("/accounts/wizard/<wiz_id>/stop")
    def accounts_wizard_stop(wiz_id: str) -> Response:
        try:
            email = _require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        _check_csrf(request.headers.get("X-CSRF"))
        wiz = _wiz_get(wiz_id)
        if not wiz or wiz.user_email != email:
            return jsonify({"ok": False, "error": "not_found"}), 404
        delete_profile = bool((request.get_json(silent=True) or {}).get("delete_profile"))
        try:
            _finish_wizard(wiz, delete_profile_if_created=delete_profile)
            return jsonify({"ok": True})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500
