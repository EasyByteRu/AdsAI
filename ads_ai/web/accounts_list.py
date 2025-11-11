# -*- coding: utf-8 -*-
from __future__ import annotations

import html
import math
import json
import os
import re
import time
import urllib.parse
import threading
import uuid
import random
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterator, List, Optional, Tuple, Set

from flask import Flask, request, jsonify, make_response, redirect, url_for, Response
import sqlite3

# Конфиг
from ads_ai.config.settings import Settings

# Общие объекты/БД как в campaigns.py
from ads_ai.web.campaigns import CampaignDB, _start_adspower_driver

# Лейаут/защита/аутентификация/утилиты
from ads_ai.web.account import (
    _require_user,   # проверка логина
    _read_csrf,      # чтение CSRF
    _check_csrf,     # проверка CSRF
    _delete_adspower_profile,  # попытка удалить профиль из AdsPower (best-effort)
    _get_adspower_env,
    _stop_adspower_driver,
)

# ============================ Вспомогательные парсеры/HTTP ============================

def _http_get_json(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 6.0,
) -> Tuple[int, Dict[str, Any]]:
    try:
        import requests  # type: ignore
    except Exception:
        requests = None  # type: ignore

    if requests:
        try:
            resp = requests.get(url, headers=headers or {}, timeout=timeout)  # type: ignore
            body: Dict[str, Any] = {}
            try:
                body = resp.json() if resp.content else {}
            except Exception:
                body = {"raw": resp.text}
            return int(resp.status_code), body
        except Exception:
            return 0, {}

    import urllib.request
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # type: ignore
            data = resp.read()
            if data:
                try:
                    return int(resp.getcode() or 0), json.loads(data.decode("utf-8"))
                except Exception:
                    return int(resp.getcode() or 0), {"raw": data.decode("utf-8", "ignore")}
            return int(resp.getcode() or 0), {}
    except Exception:
        return 0, {}


def _safe_timestamp(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        txt = str(value).strip()
        if not txt:
            return 0.0
        if txt.isdigit():
            return float(int(txt))
        from datetime import datetime
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(txt, fmt).timestamp()
            except Exception:
                continue
        return 0.0
    except Exception:
        return 0.0


def _normalize_group_ids(raw: Any) -> List[str]:
    if raw is None:
        return []
    items: List[str] = []
    if isinstance(raw, str):
        tokens = re.split(r"[,\s;]+", raw)
    elif isinstance(raw, list):
        tokens = raw
    else:
        tokens = []
    seen = set()
    for tok in tokens:
        val = str(tok or "").strip()
        if not val or val in seen:
            continue
        seen.add(val)
        items.append(val)
    return items


_PROFILE_OTP_KEYS = (
    "otp_secret","otpSecret","otp_secret_key","otpSecretKey","otp_key","otpKey",
    "two_factor","twoFactor","two_factor_key","twoFactorKey","two_factor_code","twoFactorCode",
    "two_factor_secret","twoFactorSecret","two_fa","twoFa","twofa","totp","totp_secret","totpSecret",
    "google_otp","googleOtp","ga_otp","gaOtp","ga_code","gaCode","fakey","fa_key","fackey",
)

def _adspower_error_message(payload: Any) -> str:
    if isinstance(payload, dict):
        return str(payload.get("msg") or payload.get("message") or payload.get("error") or "adspower_unavailable")
    return str(payload or "adspower_unavailable")


# ============================ AdsPower API чтение ============================

def _adspower_get_with_retry(
    url: str,
    headers: Dict[str, str],
    timeout: float,
    context: str,
) -> Tuple[int, Any]:
    attempts = max(1, _ADSP_RATE_LIMIT_MAX_RETRIES)
    for attempt in range(attempts):
        code, body = _http_get_json(url, headers=headers, timeout=timeout)
        ok = code and isinstance(body, dict) and str(body.get("code")) in ("0", "200")
        if ok:
            return code, body
        message = _adspower_error_message(body)
        if _is_adspower_rate_limited(message) and attempt < attempts - 1:
            delay = max(0.5, _ADSP_RATE_LIMIT_BASE_DELAY * (attempt + 1))
            print(f"[accounts] AdsPower rate limit ({context}) — retry in {delay:.2f}s", flush=True)
            time.sleep(delay)
            continue
        return code, body
    return code, body


def _extract_profile_otp(payload: Any, depth: int = 0) -> Optional[str]:
    if depth > 4 or not isinstance(payload, dict):
        return None

    def _clean(val: Any) -> Optional[str]:
        if val is None:
            return None
        text = str(val).strip()
        return text or None

    for key in _PROFILE_OTP_KEYS:
        if key in payload:
            candidate = _clean(payload.get(key))
            if candidate:
                return candidate

    nested_keys = ("two_factor_info","twoFactorInfo","two_factor_data","twoFactorData",
                   "extensions","extension","ext","extra","extras","more","more_info","moreInfo")
    for nkey in nested_keys:
        nested = payload.get(nkey)
        if isinstance(nested, dict):
            value = _extract_profile_otp(nested, depth + 1)
            if value:
                return value

    for value in payload.values():
        if isinstance(value, dict):
            nested = _extract_profile_otp(value, depth + 1)
            if nested:
                return nested
        elif isinstance(value, str):
            candidate = _clean(value)
            if candidate and ("otpauth://" in candidate or len(candidate) >= 16):
                return candidate
    return None


def _fetch_adspower_profiles(group_ids: List[str]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Возвращает профили AdsPower, ограниченные указанными group_id."""
    _, api_base, token = _get_adspower_env()
    base = (api_base or "").rstrip("/")
    if not base:
        return [], "adspower_base_missing"
    groups = [g for g in group_ids if g]
    if not groups:
        return [], None
    headers = {"Authorization": token} if token else {}
    items: Dict[str, Dict[str, Any]] = {}
    error: Optional[str] = None
    page_size = 300
    max_pages = 3

    total_groups = len(groups)
    for idx, gid in enumerate(groups):
        page = 1
        while page <= max_pages:
            params = f"page={page}&page_size={page_size}"
            if gid:
                params += f"&group_id={urllib.parse.quote(gid)}"
            url = f"{base}/api/v1/user/list?{params}"
            context = f"group={gid} page={page}"
            code, body = _adspower_get_with_retry(url, headers=headers, timeout=8.0, context=context)
            if not code or not isinstance(body, dict) or str(body.get("code")) not in ("0", "200"):
                error = _adspower_error_message(body)
                break
            data = body.get("data") or {}
            lst = data.get("list") or []
            for it in lst:
                pid = it.get("user_id") or it.get("id") or it.get("profile_id") or it.get("profileId")
                if not pid:
                    continue
                pid_s = str(pid)
                try:
                    print("[accounts] adspower_profile_raw", pid_s, json.dumps(it, ensure_ascii=False)[:2000], flush=True)
                except Exception:
                    print(f"[accounts] adspower_profile_raw {pid_s} <unserializable>", flush=True)
                name = it.get("name") or it.get("username") or it.get("remark") or ""
                record = {
                    "id": pid_s,
                    "profile_id": pid_s,
                    "name": str(name or ""),
                    "group_id": str(it.get("group_id") or it.get("groupId") or gid or ""),
                    "tags": it.get("tags") or [],
                    "created_at": _safe_timestamp(it.get("create_time")),
                }
                otp_secret = _extract_profile_otp(it)
                if otp_secret:
                    record["otp_secret"] = otp_secret
                items[pid_s] = record
            has_more = len(lst) >= page_size
            if not has_more:
                break
            if _ADSP_PROFILE_FETCH_THROTTLE > 0:
                time.sleep(_ADSP_PROFILE_FETCH_THROTTLE)
            page += 1
        if error:
            break
        if _ADSP_PROFILE_FETCH_THROTTLE > 0 and total_groups > 1 and idx + 1 < total_groups:
            time.sleep(_ADSP_PROFILE_FETCH_THROTTLE)

    filtered = [it for it in items.values() if (not groups) or (it.get("group_id") in groups)]
    filtered.sort(key=lambda it: (it.get("name") or it.get("profile_id") or "").lower())
    return filtered, error


def _fetch_adspower_group_list() -> Tuple[List[Dict[str, str]], Optional[str]]:
    """Возвращает список групп AdsPower."""
    _, api_base, token = _get_adspower_env()
    base = (api_base or "").rstrip("/")
    if not base:
        return [], "adspower_base_missing"
    headers = {"Authorization": token} if token else {}
    paths = [
        "/api/v1/group/list?page=1&page_size=500",
        "/v1/api/group/list?page=1&page_size=500",
        "/api/v1/group/index?page=1&page_size=500",
        "/v1/api/group/index?page=1&page_size=500",
    ]
    last_error: Optional[str] = None
    groups: List[Dict[str, str]] = []
    for path in paths:
        code, body = _http_get_json(f"{base}{path}", headers=headers, timeout=8.0)
        if not code or not isinstance(body, dict):
            last_error = "adspower_unavailable"
            continue
        if str(body.get("code")) not in ("0", "200"):
            last_error = str(body.get("msg") or body.get("message") or "adspower_error")
            continue
        data = body.get("data") or {}
        raw_items = data.get("list") or data.get("data") or body.get("list") or body.get("items") or []
        if not isinstance(raw_items, list):
            continue
        for item in raw_items:
            gid = str(item.get("group_id") or item.get("id") or item.get("groupId") or "").strip()
            if not gid:
                continue
            name = str(item.get("name") or item.get("group_name") or item.get("remark") or "")
            groups.append({"id": gid, "name": name})
        break
    if not groups:
        return [], last_error or "adspower_groups_not_found"
    groups = sorted(groups, key=lambda it: (it.get("name") or it.get("id") or "").lower())
    return groups, None


# ============================ Конфиги/лимиты и адаптивные гейты ============================

def _parse_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        value = int(raw)
        return value if value > 0 else default
    except Exception:
        return default

def _parse_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        value = float(raw)
        return value if value > 0 else default
    except Exception:
        return default

# Воркеры и драйверы
_CPU = os.cpu_count() or 8
_EMAIL_WORKER_DEFAULT = max(1, min(4, _CPU))
_MAX_EMAIL_WORKERS = max(1, min(256, _parse_int_env("ADS_AI_EMAIL_WORKERS", _EMAIL_WORKER_DEFAULT)))
_ADSP_MAX_CONCURRENT_DRIVERS = _parse_int_env("ADS_AI_ADSPOWER_CONCURRENCY", 12)  # ускоряем дефолт
_ADSP_RATE_LIMIT_MAX_RETRIES = _parse_int_env("ADS_AI_ADSPOWER_RETRIES", 4)
_ADSP_RATE_LIMIT_BASE_DELAY = _parse_float_env("ADS_AI_ADSPOWER_BACKOFF", 1.0)
_ADSP_PROFILE_FETCH_THROTTLE = max(0.0, _parse_float_env("ADS_AI_ADSPOWER_THROTTLE", 0.25))
_ADSP_DRIVER_SEMAPHORE = threading.BoundedSemaphore(max(1, _ADSP_MAX_CONCURRENT_DRIVERS))
_ADSP_RATE_LIMIT_GLOBAL_BACKOFF = _parse_float_env("ADS_AI_ADSPOWER_GLOBAL_BACKOFF", 3.0)
_ADSP_THROTTLE_LOCK = threading.RLock()
_ADSP_THROTTLE_UNTIL = 0.0

# Batch/лимиты
_EMAIL_RECENT_LIMIT = 25
# 0 — без ограничения; задаётся через ADS_AI_EMAIL_BATCH_LIMIT
_EMAIL_BATCH_LIMIT = _parse_int_env("ADS_AI_EMAIL_BATCH_LIMIT", 0)
_EMAIL_RATE_LIMIT_QPS = max(0.0, _parse_float_env("ADS_AI_EMAIL_RATE_QPS", 1.0))
_EMAIL_RATE_LOCK = threading.Lock()
_EMAIL_RATE_NEXT_TS = 0.0

# Google fetch race/тайминги
_ADS_GA_FETCH_ATTEMPTS = _parse_int_env("ADS_AI_GA_FETCH_ATTEMPTS", 3)
_ADS_GA_FETCH_BACKOFF = _parse_float_env("ADS_AI_GA_FETCH_BACKOFF", 0.6)
_ADS_GA_FETCH_TIMEOUT_MS = _parse_int_env("ADS_AI_GA_FETCH_TIMEOUT_MS", 12000)
_ADS_GA_RACE_DELAY_MS = _parse_int_env("ADS_AI_GA_FETCH_RACE_DELAY_MS", 300)
_LAUNCH_JITTER_MS = _parse_int_env("ADS_AI_LAUNCH_JITTER_MS", 180)

# Динамический gate на одновременные фетчи к Google
_GOOGLE_CONCURRENCY_TARGET = _parse_int_env("ADS_AI_GOOGLE_CONCURRENCY", 16)
_GOOGLE_CONCURRENCY_MIN = _parse_int_env("ADS_AI_GOOGLE_CONCURRENCY_MIN", 2)
_GATE_RELAX_EVERY = _parse_int_env("ADS_AI_GATE_RELAX_EVERY", 10)

# Глобальный анти‑штраф после 429 (секунды)
_GA_RATE_LIMIT_GLOBAL_BACKOFF = _parse_float_env("ADS_AI_GA_GLOBAL_BACKOFF", 3.0)
_GA_THROTTLE_LOCK = threading.RLock()
_GA_THROTTLE_UNTIL = 0.0

def _note_ga_rate_limit():
    global _GA_THROTTLE_UNTIL
    with _GA_THROTTLE_LOCK:
        _GA_THROTTLE_UNTIL = max(_GA_THROTTLE_UNTIL, time.time() + _GA_RATE_LIMIT_GLOBAL_BACKOFF)

def _await_ga_throttle():
    with _GA_THROTTLE_LOCK:
        remain = _GA_THROTTLE_UNTIL - time.time()
    if remain > 0:
        time.sleep(min(remain, 1.5) + random.uniform(0.01, 0.12))

def _note_adspower_rate_limit():
    global _ADSP_THROTTLE_UNTIL
    with _ADSP_THROTTLE_LOCK:
        _ADSP_THROTTLE_UNTIL = max(_ADSP_THROTTLE_UNTIL, time.time() + _ADSP_RATE_LIMIT_GLOBAL_BACKOFF)

def _await_adspower_throttle():
    with _ADSP_THROTTLE_LOCK:
        remain = _ADSP_THROTTLE_UNTIL - time.time()
    if remain > 0:
        time.sleep(min(remain, 2.5) + random.uniform(0.02, 0.14))

def _await_email_rate_slot():
    global _EMAIL_RATE_NEXT_TS
    if _EMAIL_RATE_LIMIT_QPS <= 0:
        return
    interval = max(0.01, 1.0 / _EMAIL_RATE_LIMIT_QPS)
    while True:
        with _EMAIL_RATE_LOCK:
            now = time.time()
            if now >= _EMAIL_RATE_NEXT_TS:
                _EMAIL_RATE_NEXT_TS = now + interval
                return
            wait_for = max(0.0, _EMAIL_RATE_NEXT_TS - now)
        time.sleep(min(wait_for, 0.5))

_DEFAULT_LIST_ACCOUNTS_URLS = [
    "https://accounts.google.com/ListAccounts?gpsia=1&source=ChromiumBrowser&json=standard",
    "https://accounts.google.com/ListAccounts?gpsia=1&source=Chrome&json=standard",
    "https://accounts.google.com/ListAccounts?gpsia=1&json=standard",
]
def _listaccounts_urls() -> List[str]:
    raw = os.getenv("ADS_AI_GA_FETCH_URLS", "").strip()
    if not raw:
        return list(_DEFAULT_LIST_ACCOUNTS_URLS)
    parts = [p.strip() for p in re.split(r"[,\s]+", raw) if p.strip()]
    return parts or list(_DEFAULT_LIST_ACCOUNTS_URLS)

_LIST_ACCOUNTS_URL = _DEFAULT_LIST_ACCOUNTS_URLS[0]
_ACCOUNT_HOME_URL = "https://accounts.google.com/"
_ACCOUNT_CHOOSER_URL = (
    "https://accounts.google.com/AccountChooser?"
    "continue=https://accounts.google.com/&hl=en&flowName=GlifWebSignIn&flowEntry=AccountChooser"
)

_NO_ACCOUNT_EMAIL = "no_account"
_NO_ACCOUNT_ERROR_MARKERS = ("account_data_missing", "email_not_found")

def _is_no_account_error(message: Any) -> bool:
    text = str(message or "").strip().lower()
    return any(marker in text for marker in _NO_ACCOUNT_ERROR_MARKERS)

_LEGACY_DB_TABLE_CACHE: Dict[str, bool] = {}
_LEGACY_DB_WARNED: Set[str] = set()
_SECONDARY_DB_CANDIDATES = (
    os.path.join(os.getcwd(), "ads_ai_data", "companies.sqlite3"),
    os.path.join(os.getcwd(), "ads_ai_companies.sqlite3"),
)


def _legacy_secret_paths(primary_path: str) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []

    def _add(path: Optional[str]) -> None:
        if not path:
            return
        apath = os.path.abspath(path)
        if not os.path.exists(apath):
            return
        if primary_path and os.path.abspath(primary_path) == apath:
            return
        if apath in seen:
            return
        seen.add(apath)
        out.append(apath)

    _add(os.getenv("ADS_AI_CAMPAIGNS_DB", "").strip() or None)
    for cand in _SECONDARY_DB_CANDIDATES:
        _add(cand)
    return out


def _mirror_otp_secret_to_paths(paths: List[str], user_email: str, profile_id: str, otp_secret: Optional[str]) -> None:
    if not otp_secret or not paths:
        return
    payload = (otp_secret, time.time(), user_email, profile_id)
    for path in paths:
        cache_key = os.path.abspath(path)
        try:
            has_accounts = _LEGACY_DB_TABLE_CACHE.get(cache_key)
            conn = sqlite3.connect(path, check_same_thread=False)
            try:
                if has_accounts is None:
                    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='accounts'")
                    has_accounts = bool(cur.fetchone())
                    _LEGACY_DB_TABLE_CACHE[cache_key] = has_accounts
                if not has_accounts:
                    continue
                conn.execute(
                    "UPDATE accounts SET otp_secret = ?, updated_at = ? WHERE user_email = ? AND profile_id = ?",
                    payload,
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            if cache_key not in _LEGACY_DB_WARNED:
                print(f"[accounts] mirror otp_secret failed path={path} err={exc}", flush=True)
                _LEGACY_DB_WARNED.add(cache_key)

# -------- AdaptiveGate: динамический лимитер параллельных фетчей к Google --------
class AdaptiveGate:
    def __init__(self, initial: int, min_limit: int, max_limit: int, relax_every: int = 10):
        self._limit = max(min_limit, min(initial, max_limit))
        self._min = max(1, min_limit)
        self._max = max(self._min, max_limit)
        self._active = 0
        self._cv = threading.Condition()
        self._success = 0
        self._relax_every = max(1, relax_every)

    def acquire(self):
        with self._cv:
            while self._active >= self._limit:
                self._cv.wait(0.2)
            self._active += 1

    def release(self):
        with self._cv:
            self._active -= 1
            if self._active < 0:
                self._active = 0
            self._cv.notify()

    @contextmanager
    def slot(self):
        self.acquire()
        try:
            yield
        finally:
            self.release()

    def tighten(self, step: int = 1):
        with self._cv:
            new_limit = max(self._min, self._limit - max(1, step))
            if new_limit != self._limit:
                self._limit = new_limit
                print(f"[gate] tighten → {self._limit}", flush=True)
                self._cv.notify_all()

    def relax(self, step: int = 1):
        with self._cv:
            new_limit = min(self._max, self._limit + max(1, step))
            if new_limit != self._limit:
                self._limit = new_limit
                print(f"[gate] relax → {self._limit}", flush=True)
                self._cv.notify_all()

    def mark_success(self):
        with self._cv:
            self._success += 1
            if self._success >= self._relax_every:
                self._success = 0
                if self._limit < self._max:
                    self._limit += 1
                    print(f"[gate] auto-relax → {self._limit}", flush=True)
                    self._cv.notify_all()

_GOOGLE_GATE = AdaptiveGate(
    initial=min(_GOOGLE_CONCURRENCY_TARGET, _ADSP_MAX_CONCURRENT_DRIVERS * 2),
    min_limit=_GOOGLE_CONCURRENCY_MIN,
    max_limit=_GOOGLE_CONCURRENCY_TARGET,
    relax_every=_GATE_RELAX_EVERY,
)

# ============================ Email job state ============================

_EMAIL_JOBS: Dict[str, Dict[str, Any]] = {}
_EMAIL_JOB_LOCK = threading.RLock()

def _email_job_snapshot(job: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": job.get("id"),
        "status": job.get("status"),
        "total": job.get("total", 0),
        "done": job.get("done", 0),
        "success": job.get("success", 0),
        "failed": job.get("failed", 0),
        "error": job.get("error"),
        "started_at": job.get("created_at"),
        "finished_at": job.get("finished_at"),
        "recent": list(job.get("recent", [])),
    }

def _start_email_enrich_job(db: CampaignDB, user_email: str, accounts: List[Dict[str, Any]]) -> Dict[str, Any]:
    job_id = f"ainfo_{uuid.uuid4().hex[:10]}"
    job = {
        "id": job_id,
        "user": user_email,
        "status": "running",
        "total": len(accounts),
        "done": 0,
        "success": 0,
        "failed": 0,
        "created_at": time.time(),
        "finished_at": None,
        "error": None,
        "recent": [],
    }
    with _EMAIL_JOB_LOCK:
        _EMAIL_JOBS[job_id] = job

    print(f"[accounts] email job {job_id} start: user={user_email} targets={len(accounts)}", flush=True)
    t = threading.Thread(target=_run_email_job, name=f"account_email_job_{job_id}", args=(job_id, db, accounts), daemon=True)
    t.start()
    return job


# ============================ Selenium/driver helpers ============================

def _is_adspower_rate_limited(message: str) -> bool:
    msg = (message or "").lower()
    return (
        "too many request per second" in msg
        or "too many requests per second" in msg
        or ("rate limit" in msg and "adspower" in msg)
    )

@contextmanager
def _adspower_driver_slot() -> Iterator[None]:
    _ADSP_DRIVER_SEMAPHORE.acquire()
    try:
        yield
    finally:
        _ADSP_DRIVER_SEMAPHORE.release()

def _ensure_accounts_origin(driver, wait_timeout: float = 3.0) -> None:
    """Гарантируем, что вкладка открыта на accounts.google.com, чтобы JS-запросы шли same-origin."""
    try:
        current = driver.current_url or ""
    except Exception:
        current = ""
    if not (current.startswith(_ACCOUNT_HOME_URL) or current.startswith(_ACCOUNT_CHOOSER_URL)):
        driver.get(_ACCOUNT_CHOOSER_URL)
    deadline = time.time() + max(wait_timeout, 0.5)
    while time.time() < deadline:
        try:
            ready = driver.execute_script("return document.readyState") == "complete"
        except Exception:
            ready = True
        if ready:
            break
        time.sleep(0.2)

# ============================ Быстрый JS-мост: Fetch+XHR race ============================

def _fetch_list_accounts_text(driver, timeout: float = 12.0, urls: Optional[List[str]] = None) -> str:
    """
    Быстрый бридж: пробуем fetch; через raceDelay подключаем альтернативы и XHR.
    Возвращаем сырой текст.
    """
    urls = urls or _listaccounts_urls()
    race_delay = max(0, _ADS_GA_RACE_DELAY_MS)

    script = r"""
        const urls = arguments[0] || [];
        const timeoutMs = arguments[1] || 12000;
        const raceDelayMs = arguments[2] || 300;
        const done = arguments[arguments.length - 1];

        function finish(payload){ try{ done(payload); }catch(e){} }

        // Promise.any полифилл (на всякий)
        const promiseAny = (arr) => {
            if (Promise.any) return Promise.any(arr);
            return new Promise((resolve, reject)=>{
                let fails = 0; const errs = [];
                arr.forEach(p => p.then(resolve).catch(e => {
                    errs.push(e); fails += 1; if (fails === arr.length) reject(new Error('all_failed'));
                }));
            });
        };

        try{
            const host = (location && location.hostname) || '';
            if (!/(^|\.)accounts\.google\.com$/i.test(host)){
                return finish({ok:false, error:'list_accounts_fetch_error:not_on_accounts_origin:'+host});
            }
        }catch(_){}

        const controller = (typeof AbortController !== 'undefined') ? new AbortController() : null;
        const killer = setTimeout(()=>{
            if (controller) controller.abort();
            finish({ok:false, error:'list_accounts_fetch_error:timeout'});
        }, Math.max(500, Math.min(60000, timeoutMs)));

        function doFetch(url){
            return fetch(url, {
                credentials: 'include',
                cache: 'no-store',
                redirect: 'follow',
                mode: 'same-origin',
                headers: {'accept':'application/json,text/plain,*/*'},
                signal: controller ? controller.signal : undefined,
                keepalive: true,
                referrerPolicy: 'no-referrer-when-downgrade'
            }).then(async (resp) => {
                const text = await resp.text();
                return {ok:true, status: resp.status, body: text};
            }).catch(err => ({ok:false, error: 'fetch:'+ (err && (err.message||String(err)) || 'unknown')}));
        }

        function doXHR(url){
            return new Promise((resolve)=>{
                try{
                    const xhr = new XMLHttpRequest();
                    xhr.open('GET', url, true);
                    xhr.withCredentials = true;
                    xhr.setRequestHeader('Accept','application/json,text/plain,*/*');
                    let finished = false;
                    const t = setTimeout(()=>{
                        if (finished) return;
                        finished = true;
                        try{ xhr.abort(); }catch(_){}
                        resolve({ok:false, error:'xhr:timeout'});
                    }, Math.max(800, Math.min(60000, timeoutMs)));
                    xhr.onreadystatechange = function(){
                        if (xhr.readyState === 4 && !finished){
                            finished = true;
                            clearTimeout(t);
                            resolve({ok:true, status:xhr.status, body: xhr.responseText || ''});
                        }
                    };
                    xhr.onerror = function(){
                        if (finished) return;
                        finished = true;
                        clearTimeout(t);
                        resolve({ok:false, error:'xhr:error'});
                    };
                    xhr.send();
                }catch(e){
                    resolve({ok:false, error:'xhr_bridge:'+ (e && (e.message||String(e)) || 'unknown')});
                }
            });
        }

        async function tryOne(url){
            const f = doFetch(url);
            let raced = false;
            const waiter = new Promise((resolve)=>{
                setTimeout(()=>{ raced = true; doXHR(url).then(resolve); }, Math.max(0, raceDelayMs));
                f.then(resolve);
            });
            return await waiter;
        }

        (async ()=>{
            try{
                const ordered = Array.isArray(urls) ? urls.filter(Boolean) : [];
                if (!ordered.length){
                    clearTimeout(killer);
                    return finish({ok:false, error:'list_accounts_fetch_error:no_urls'});
                }

                let r = await tryOne(ordered[0]); // фора первому URL
                if (r && r.ok){
                    clearTimeout(killer);
                    if ((r.status||200) >= 400){
                        const err = (r.status === 429) ? 'too_many_requests' : ('http_'+String(r.status));
                        return finish({ok:false, error:'list_accounts_fetch_error:'+err, status:r.status, snippet: (r.body||'').slice(0,160)});
                    }
                    const body = String(r.body || '').trim();
                    if (!body) return finish({ok:false, error:'list_accounts_fetch_error:empty'});
                    return finish({ok:true, status: r.status || 200, body});
                }

                const tasks = ordered.slice(1,4).map(u => tryOne(u).then(res=>{
                    if (res && res.ok && (res.status||200) < 400 && String(res.body||'').trim()){
                        return res;
                    }
                    throw new Error(res && res.error ? res.error : 'fail');
                }));
                if (tasks.length){
                    const first = await promiseAny(tasks);
                    clearTimeout(killer);
                    return finish({ok:true, status: first.status || 200, body: String(first.body||'')});
                }

                clearTimeout(killer);
                return finish({ok:false, error:'list_accounts_fetch_error:unreachable'});
            }catch(e){
                clearTimeout(killer);
                const msg = (e && (e.message || String(e))) || 'unknown';
                return finish({ok:false, error:'list_accounts_fetch_error:js:'+msg});
            }
        })();
    """
    try:
        resp = driver.execute_async_script(
            script,
            urls,
            int(max(timeout, 1.0) * 1000),
            int(race_delay),
        )
    except Exception as exc:
        raise RuntimeError(f"list_accounts_fetch_error:bridge:{exc}")
    if not isinstance(resp, dict):
        raise RuntimeError("list_accounts_fetch_error:bridge")
    if not resp.get("ok"):
        err = resp.get("error") or "unknown"
        if "http_429" in err or "too_many_requests" in err:
            raise RuntimeError("too_many_requests")
        if "timeout" in err:
            raise RuntimeError("list_accounts_fetch_error:timeout")
        raise RuntimeError(str(err))
    status = int(resp.get("status") or 0)
    body = str(resp.get("body") or "")
    if status >= 400:
        snippet = body[:160].replace("\n", " ")
        print(f"[accounts] ListAccounts fetch status={status} snippet={snippet}", flush=True)
        if status == 429:
            raise RuntimeError("too_many_requests")
        raise RuntimeError(f"list_accounts_fetch_error:http_{status}")
    if not body.strip():
        raise RuntimeError("list_accounts_fetch_error:empty")
    return body


# ============================ Извлечение email из профиля ============================

def _extract_google_identity(profile_id: str) -> Tuple[str, Optional[str]]:
    driver = None
    _await_adspower_throttle()
    try:
        with _adspower_driver_slot():
            driver = _start_adspower_driver(str(profile_id))
            if driver is None:
                raise RuntimeError("driver_start_failed")
            result = _extract_identity_with_driver(driver, profile_id)
            try:
                _stop_adspower_driver(driver)
            except Exception:
                try:
                    driver.quit()
                except Exception:
                    pass
            driver = None
            return result
    finally:
        if driver:
            try:
                _stop_adspower_driver(driver)
            except Exception:
                try:
                    driver.quit()
                except Exception:
                    pass


def _extract_identity_with_driver(driver, profile_id: str) -> Tuple[str, Optional[str]]:
    """
    Супербыстрый путь: JS (Fetch+XHR Race, несколько URL) ➜ (если нужно) DOM‑фолбэк.
    С аккуратным бэкоффом и глобальным антиштрафом при 429.
    """
    page_text = ""
    last_error: Optional[Exception] = None

    try:
        try:
            driver.set_page_load_timeout(20)
        except Exception:
            pass

        _ensure_accounts_origin(driver)
        urls = _listaccounts_urls()
        wait = max(0.25, _ADS_GA_FETCH_BACKOFF)

        for attempt in range(_ADS_GA_FETCH_ATTEMPTS):
            try:
                raw = _fetch_list_accounts_text(driver, timeout=_ADS_GA_FETCH_TIMEOUT_MS / 1000.0, urls=urls)
                payload = _parse_list_accounts_payload(raw)
                account = _pick_google_account(payload) if payload else None
                if not account:
                    raise RuntimeError("account_data_missing")
                email = account.get("email") or account.get("gaiaEmail") or account.get("googleEmail")
                if not email:
                    raise RuntimeError("email_not_found")
                name = (
                    account.get("displayName")
                    or account.get("display_name")
                    or account.get("fullName")
                    or account.get("name")
                )
                print(f"[accounts] profile={profile_id} fetched(FETCH) email={email} name={name}", flush=True)
                return email, name
            except RuntimeError as fetch_err:
                last_error = fetch_err
                txt = str(fetch_err)
                if "too_many_requests" in txt and attempt + 1 < _ADS_GA_FETCH_ATTEMPTS:
                    _note_ga_rate_limit()
                    _GOOGLE_GATE.tighten(1)
                    sl = wait + random.uniform(0.05, 0.2)
                    print(f"[accounts] profile={profile_id} 429 → backoff {sl:.2f}s (attempt {attempt+1}/{_ADS_GA_FETCH_ATTEMPTS})", flush=True)
                    time.sleep(sl)
                    wait *= 1.8
                    continue
                # жёсткие ошибки → смысла долбить нет, уйдём в DOM быстрее
                if "http_400" in txt or "bridge" in txt or "parse_error" in txt or "unreachable" in txt:
                    break
                # мягко подождём и повторим, если есть попытки
                time.sleep(0.25 + random.uniform(0.02, 0.08))
                if attempt + 1 < _ADS_GA_FETCH_ATTEMPTS:
                    continue
                break
    except Exception as exc:
        last_error = exc

    # DOM фолбэк — быстрый
    try:
        _ensure_accounts_origin(driver)
        payload = _collect_accounts_from_dom(driver, wait_timeout=3.0)
        account = _pick_google_account(payload) if payload else None
        if not account:
            raise RuntimeError("account_data_missing")
        email = account.get("email") or account.get("gaiaEmail") or account.get("googleEmail")
        if not email:
            raise RuntimeError("email_not_found")
        name = (
            account.get("displayName")
            or account.get("display_name")
            or account.get("fullName")
            or account.get("name")
        )
        print(f"[accounts] profile={profile_id} fetched(DOM) email={email} name={name}", flush=True)
        return email, name
    except RuntimeError as dom_err:
        last_error = dom_err
        try:
            driver.get(_LIST_ACCOUNTS_URL)
            time.sleep(1.0)
            page_text = driver.page_source or ""
        except Exception:
            page_text = ""

    # если виден throttling на странице — поднимем 429
    if 'too many request' in (page_text or '').lower() or 'too many requests' in (page_text or '').lower():
        _note_ga_rate_limit()
        _GOOGLE_GATE.tighten(1)
        raise RuntimeError("too_many_requests")

    if last_error:
        raise last_error
    raise RuntimeError("account_data_missing")


def _parse_list_accounts_payload(text: str) -> Dict[str, Any]:
    body = (text or "").strip()
    if body.startswith(")]}'"):
        body = body[4:].strip()
    decoder = json.JSONDecoder()
    try:
        value, idx = decoder.raw_decode(body)
    except Exception as exc:
        snippet = body[:200].replace("\n", " ")
        print(f"[accounts] ListAccounts decode err: {exc} snippet={snippet}", flush=True)
        raise RuntimeError(f"list_accounts_parse_error:{exc}")
    remainder = body[idx:].strip()
    if remainder:
        print(f"[accounts] ListAccounts extra payload: {remainder[:120]}", flush=True)

    def _coerce_candidate(obj: Any) -> Optional[Dict[str, Any]]:
        if isinstance(obj, dict) and (obj.get("accounts") or obj.get("all_accounts")):
            return obj
        if isinstance(obj, str) and obj.strip().startswith("{"):
            try:
                parsed = json.loads(obj)
            except Exception:
                return None
            if isinstance(parsed, dict) and (parsed.get("accounts") or parsed.get("all_accounts")):
                return parsed
        return None

    candidate = _coerce_candidate(value)
    if candidate:
        return candidate

    if isinstance(value, list):
        for item in value:
            candidate = _coerce_candidate(item)
            if candidate:
                return candidate
            if isinstance(item, list):
                for sub in item:
                    candidate = _coerce_candidate(sub)
                    if candidate:
                        return candidate

    print(f"[accounts] ListAccounts unexpected payload structure: type={type(value)}", flush=True)
    raise RuntimeError("list_accounts_parse_error:unexpected_structure")


def _pick_google_account(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    accounts = payload.get("accounts") or payload.get("all_accounts") or []
    if not isinstance(accounts, list):
        return None
    def _is_default(entry: Dict[str, Any]) -> bool:
        return bool(
            entry.get("is_default")
            or entry.get("isDefault")
            or entry.get("is_selected")
            or entry.get("isSelected")
        )
    for entry in accounts:
        if _is_default(entry):
            return entry
    return accounts[0] if accounts else None


def _collect_accounts_from_dom(driver, wait_timeout: float = 10.0) -> Dict[str, Any]:
    """
    Считывает аккаунты Google из DOM (страница account chooser рендерится через JS).
    Возвращает payload, совместимый с _pick_google_account.
    """
    deadline = time.time() + max(wait_timeout, 1.0)
    last_error: Optional[Exception] = None
    while time.time() < deadline:
        try:
            data = driver.execute_script(
                """
                const nodes = Array.from(document.querySelectorAll('[data-identifier]'));
                return nodes.map((node) => {
                    const wrapper = node.closest('li') || node;
                    const dataset = node.dataset || {};
                    const emailAttr = node.getAttribute('data-identifier') || dataset.identifier || '';
                    const emailNode = node.querySelector('[data-email]');
                    const labelNode = node.querySelector('.yAlK0b');
                    const titleNode = node.querySelector('.pGzURd');
                    const fallbackEmail = emailNode ? (emailNode.getAttribute('data-email') || '') : (labelNode ? (labelNode.textContent || '') : '');
                    const displayName = titleNode ? (titleNode.textContent || '') : (labelNode ? (labelNode.textContent || '') : '');
                    const gaiaId = node.getAttribute('data-ogid') || '';
                    const authUser = node.getAttribute('data-authuser') || '';
                    const wrapperSelected = wrapper && (wrapper.classList.contains('ZRzegd') || wrapper.getAttribute('aria-selected') === 'true');
                    const isDefault = Boolean(wrapperSelected || authUser === '' || authUser === '0');
                    return {
                        email: (emailAttr || fallbackEmail || '').trim(),
                        displayName: (displayName || '').trim(),
                        gaiaId: (gaiaId || '').trim(),
                        isDefault: isDefault,
                    };
                });
                """
            )
        except Exception as exc:
            last_error = exc
            data = None
        accounts: List[Dict[str, Any]] = []
        if data:
            for entry in data:
                email = str(entry.get("email") or "").strip()
                if not email:
                    continue
                display_name = html.unescape(str(entry.get("displayName") or "").strip()) or None
                record: Dict[str, Any] = {
                    "email": email,
                    "googleEmail": email,
                    "gaiaEmail": email,
                }
                if display_name:
                    record["displayName"] = display_name
                    record["name"] = display_name
                gaia_id = str(entry.get("gaiaId") or entry.get("gaia_id") or "").strip()
                if gaia_id:
                    record["gaia_id"] = gaia_id
                if entry.get("isDefault") or entry.get("is_default"):
                    record["is_default"] = True
                accounts.append(record)
        if accounts:
            return {"accounts": accounts, "all_accounts": accounts}
        time.sleep(0.35)

    if last_error:
        raise RuntimeError(f"accounts_dom_error:{last_error}")
    raise RuntimeError("accounts_dom_timeout")


# ============================ Job runner (многопоточно + гейты) ============================

def _record_email_progress(job_id: str, result: Dict[str, Any]) -> None:
    with _EMAIL_JOB_LOCK:
        job = _EMAIL_JOBS.get(job_id)
        if not job:
            return
        job["done"] = job.get("done", 0) + 1
        status = result.get("status")
        if status in ("ok", "no_account"):
            job["success"] = job.get("success", 0) + 1
        else:
            job["failed"] = job.get("failed", 0) + 1
        recent = job.setdefault("recent", [])
        recent.append(result)
        if len(recent) > _EMAIL_RECENT_LIMIT:
            del recent[: len(recent) - _EMAIL_RECENT_LIMIT]


def _run_email_job(job_id: str, db: CampaignDB, accounts: List[Dict[str, Any]]) -> None:
    if not accounts:
        with _EMAIL_JOB_LOCK:
            job = _EMAIL_JOBS.get(job_id)
            if job:
                job["status"] = "completed"
                job["finished_at"] = time.time()
        print(f"[accounts] job {job_id}: nothing to process", flush=True)
        return

    max_workers = max(1, min(_MAX_EMAIL_WORKERS, len(accounts)))
    if _EMAIL_RATE_LIMIT_QPS > 0:
        rate_cap = max(1, int(math.ceil(_EMAIL_RATE_LIMIT_QPS)))
        max_workers = min(max_workers, rate_cap)
    print(f"[accounts] job {job_id}: workers={max_workers}, adsp_conc={_ADSP_MAX_CONCURRENT_DRIVERS}", flush=True)

    def _worker(account: Dict[str, Any]) -> Dict[str, Any]:
        # стартовый джиттер — снимает фронт‑ярус 429
        if _LAUNCH_JITTER_MS > 0:
            time.sleep(random.uniform(0, _LAUNCH_JITTER_MS / 1000.0))

        profile_id = account.get("profile_id")
        if not profile_id:
            raise RuntimeError("profile_id_missing")

        _await_email_rate_slot()
        # Глобальный throttling после 429, чтобы не долбить всем пулом
        _await_ga_throttle()

        # Гейт по Google‑параллельности (отдельно от драйверов)
        with _GOOGLE_GATE.slot():
            attempt = 0
            status = "ok"
            email: Optional[str] = None
            name: Optional[str] = None
            while True:
                try:
                    email, name = _extract_google_identity(profile_id)
                    # успех — даём гейту шанс немного расшириться
                    _GOOGLE_GATE.mark_success()
                except RuntimeError as exc:
                    text = str(exc)
                    if _is_no_account_error(text):
                        status = "no_account"
                        email = _NO_ACCOUNT_EMAIL
                        fallback_name = account.get("google_name") or account.get("name") or None
                        name = fallback_name
                        _GOOGLE_GATE.mark_success()
                        break
                    if "too_many_requests" in text:
                        _note_ga_rate_limit()
                        _GOOGLE_GATE.tighten(1)
                    # Не перезаписываем на "no_account" при сетевых/HTTP ошибках и мостовых проблемах
                    if "http_400" in text or "list_accounts_fetch_error" in text:
                        return {"_skip": True, "error": text}
                    if _is_adspower_rate_limited(text):
                        _note_adspower_rate_limit()
                        if attempt < _ADSP_RATE_LIMIT_MAX_RETRIES:
                            delay = _ADSP_RATE_LIMIT_BASE_DELAY * (attempt + 1)
                            print(f"[accounts] profile={profile_id} start rate-limited, retry in {delay:.2f}s", flush=True)
                            time.sleep(delay)
                            attempt += 1
                            continue
                    raise
                break

        if email is None:
            raise RuntimeError("email_fetch_failed")

        db.update_account_identity(account["id"], email, name)
        payload = {"email": email, "google_name": name}
        if status == "no_account":
            payload["status"] = status
        return payload

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_worker, acc): acc for acc in accounts}
            for fut in as_completed(futures):
                acc = futures[fut]
                result = {
                    "account_id": acc.get("id"),
                    "profile_id": acc.get("profile_id"),
                    "name": acc.get("name"),
                }
                try:
                    data = fut.result()
                    if data.get("_skip"):
                        result.update({"status": "skip", "error": data.get("error")})
                        print(f"[accounts] job {job_id}: profile={acc.get('profile_id')} SKIP ({data.get('error')})", flush=True)
                    else:
                        status = data.pop("status", "ok")
                        result.update({"status": status, **data})
                        if status == "no_account":
                            print(f"[accounts] job {job_id}: profile={acc.get('profile_id')} NO_ACCOUNT", flush=True)
                        else:
                            print(f"[accounts] job {job_id}: profile={acc.get('profile_id')} email={data.get('email')} name={data.get('google_name')}", flush=True)
                except Exception as exc:
                    result.update({"status": "error", "error": str(exc)})
                    print(f"[accounts] job {job_id}: profile={acc.get('profile_id')} ERROR {exc}", flush=True)
                _record_email_progress(job_id, result)
    except Exception as exc:
        with _EMAIL_JOB_LOCK:
            job = _EMAIL_JOBS.get(job_id)
            if job:
                job["status"] = "error"
                job["error"] = str(exc)
                job["finished_at"] = time.time()
        print(f"[accounts] job {job_id} fatal error: {exc}", flush=True)
        return

    with _EMAIL_JOB_LOCK:
        job = _EMAIL_JOBS.get(job_id)
        if job:
            job["status"] = "completed"
            job["finished_at"] = time.time()
    print(f"[accounts] job {job_id} completed", flush=True)


# ============================ Job state helpers ============================

def _get_email_job(job_id: str) -> Optional[Dict[str, Any]]:
    with _EMAIL_JOB_LOCK:
        job = _EMAIL_JOBS.get(job_id)
        if not job:
            return None
        return _email_job_snapshot(job)

def _find_running_email_job(user_email: str) -> Optional[Dict[str, Any]]:
    with _EMAIL_JOB_LOCK:
        for job in _EMAIL_JOBS.values():
            if job.get("user") == user_email and job.get("status") == "running":
                return _email_job_snapshot(job)
    return None


# ========================== HTML (лист аккаунтов) ============================

PAGE_HTML = """<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <title>HyperAI — Аккаунты</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="csrf" content="{{CSRF}}">
  <meta name="groups" content="{{GROUPS_META}}">
  <style>
    :root{
      --bg:#eef2f7; --bg2:#f6f8fb; --text:#111827; --muted:#6b7280;
      --glass: rgba(255,255,255,.66); --glass-2: rgba(255,255,255,.5);
      --border: rgba(17,24,39,.08); --ring: rgba(17,24,39,.06);
      --neon1:#38bdf8; --neon2:#a78bfa; --neon3:#34d399;
      --ok:#16a34a; --err:#ef4444; --warn:#f59e0b;
      --radius:24px; --radius-sm:16px;
      --shadow: 0 10px 30px rgba(15,23,42,.12);
      --content-max: 1480px;
    }
    *{box-sizing:border-box}
    html,body{height:100%;margin:0;color:var(--text);font:14px/1.45 Inter,system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;-webkit-font-smoothing:antialiased}
    body{
      background:
        radial-gradient(1200px 800px at 20% -10%, #ffffff 0%, var(--bg) 48%, var(--bg2) 100%),
        linear-gradient(180deg,#ffffff, var(--bg2));
    }
    .shell{ display:grid; grid-template-columns: 300px minmax(0,1fr); gap:18px;
            min-height:100vh; padding:18px; max-width:var(--content-max); margin:0 auto; }
    .panel{
      background:var(--glass); border:1px solid var(--border); border-radius:var(--radius);
      backdrop-filter: blur(12px) saturate(160%); box-shadow:var(--shadow); overflow:hidden;
    }
    .menu{ padding:18px; display:flex; flex-direction:column; gap:12px }
    .menu .head{ height:56px; display:flex; align-items:center; gap:10px; padding:0 6px; font-weight:700 }
    .mitem{ display:flex; align-items:center; gap:10px; padding:10px 12px; border-radius:14px; background:var(--glass-2); border:1px solid var(--border); text-decoration:none; color:inherit }
    .mitem.active{ outline:2px solid rgba(56,189,248,.25) }
    .muted{ color:var(--muted) }

    .stage{ position:relative; display:grid; grid-template-rows: auto auto 1fr auto; gap:14px; padding:18px; }

    .toolbar{
      display:flex; gap:12px; align-items:center; justify-content:space-between; padding:12px;
      border:1px solid var(--border); border-radius:16px;
      background: linear-gradient(135deg, rgba(255,255,255,.85), rgba(255,255,255,.7));
      backdrop-filter: blur(8px) saturate(160%);
      box-shadow: var(--shadow);
      flex-wrap:wrap;
    }
    .toolbar .left,.toolbar .right{ display:flex; gap:10px; align-items:center; flex-wrap:wrap }
    .btn{
      border:1px solid var(--border);
      background: linear-gradient(180deg, #fff, #f4f7fb);
      color:var(--text);
      border-radius: 999px;
      padding:9px 16px;
      cursor:pointer;
      transition: transform .08s ease, box-shadow .25s ease, opacity .2s ease
    }
    .btn:hover{ transform: translateY(-1px); box-shadow: 0 10px 30px rgba(15,23,42,.15) }
    .btn.primary{
      background: radial-gradient(100% 100% at 0% 0%, #67e8f9 0%, #38bdf8 40%, #a78bfa 100%);
      color:#021018; font-weight:800; letter-spacing:.2px;
      box-shadow: 0 12px 30px rgba(56,189,248,.35), inset 0 0 0 1px rgba(2,16,24,.1);
    }
    .btn.ghost{ background: transparent; border-color: var(--ring); }
    .btn[disabled]{ opacity:.55; cursor:not-allowed; filter:saturate(.7) grayscale(.06) }

    .group-editor{ display:flex; flex-direction:column; gap:4px; min-width:220px }
    .group-editor label{ font-size:12px; text-transform:uppercase; letter-spacing:.2px; color:var(--muted) }
    .group-editor input{
      border:1px solid var(--border); border-radius:12px; padding:8px 12px;
      font-size:13px; background:rgba(255,255,255,.92); color:var(--text);
    }

    .group-picker{
      display:flex; gap:8px; flex-wrap:wrap; padding:8px 6px 4px;
      border:1px solid var(--ring); border-radius:16px; background:rgba(255,255,255,.78); min-height:56px;
    }
    .group-picker .gitem{
      display:inline-flex; align-items:center; gap:6px;
      border:1px solid var(--border); border-radius:999px;
      padding:4px 10px; font-size:13px; height:40px; background:rgba(255,255,255,.95);
      cursor:pointer; transition:border-color .15s ease, box-shadow .15s ease;
    }
    .group-picker .gitem.selected{ border-color:var(--neon1); box-shadow:0 0 0 1px rgba(56,189,248,.25); }
    .group-picker .gitem input{ accent-color:var(--neon1); }
    .group-picker .gname{ font-weight:600; }
    .group-picker .gid{ font-size:11px; color:var(--muted); }
    .group-picker .empty-groups{ font-size:13px; color:var(--muted); padding:6px 4px; }

    .search{ display:flex; align-items:center; padding:8px 12px; border-radius:999px; background:rgba(255,255,255,.85); border:1px solid var(--border) }
    .search input{ width:220px; border:0; outline:none; background:transparent; color:var(--text) }

    .note{ border-radius:16px; border:1px solid var(--ring); padding:12px 14px;
           background:rgba(255,255,255,.82); color:var(--muted);
           transition: border-color .2s ease, color .2s ease; }
    .note.ok{ border-color:var(--ok); color:var(--ok); }
    .note.info{ border-color:var(--neon1); color:var(--text); }
    .note.warn{ border-color:var(--warn); color:var(--warn); }
    .note.err{ border-color:var(--err); color:var(--err); }

    .list{ display:flex; flex-direction:column; gap:10px; }
    .row{
      display:grid; grid-template-columns: auto 1fr auto; gap:14px; align-items:center;
      padding:12px 14px; border-radius:16px;
      background: rgba(255,255,255,.78); border:1px solid var(--ring); box-shadow: var(--shadow);
    }
    .row:hover{ box-shadow: 0 12px 30px rgba(15,23,42,.15) }
    .row .avatar{
      width:44px; height:44px; border-radius:14px; display:grid; place-items:center; font-weight:800;
      background: radial-gradient(90px 90px at 30% 30%, #fff, #e5e7eb);
      color:#0b1220; border:1px solid var(--border); font-size:18px;
    }
    .row .main{ display:flex; flex-direction:column; gap:6px; min-width:0 }
    .row .title{ font-weight:800; letter-spacing:.2px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis }
    .row .meta{ display:flex; gap:8px; align-items:center; flex-wrap:wrap }
    .pill{ font-size:12px; padding:3px 9px; border-radius:999px; border:1px solid var(--border);
           background: rgba(255,255,255,.9); }
    .pill.pill-no-account{ border-color: var(--warn); color: var(--warn); background: rgba(245,158,11,.12); }
    .tag{ font-size:11px; padding:2px 7px; border-radius:8px; background:rgba(56,189,248,.12); color:#0369a1; }
    .mono{ font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
    .actions{ display:flex; gap:8px; align-items:center; }

    .empty{ padding:18px; text-align:center; color:var(--muted);
            border:1px dashed var(--border); border-radius:16px; background: rgba(255,255,255,.55); }

    @media (max-width: 820px){
      .shell{ grid-template-columns: 1fr; }
      .row{ grid-template-columns: auto 1fr; }
      .row .actions{ grid-column: 1 / -1; justify-content:flex-start; }
      .search input{ width:160px; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <aside class="panel menu">
      <div class="head">
        <div style="width:36px;height:36px;border-radius:12px;background:linear-gradient(135deg,var(--neon1),var(--neon2))"></div>
        <div>Меню</div>
      </div>
      <a href="/" class="mitem">Главная</a>
      <a href="/companies/list" class="mitem">Компании</a>
      <a href="/accounts" class="mitem active">Аккаунты</a>
      <div style="margin-top:auto" class="muted">Powered by EasyByte</div>
    </aside>

    <section class="panel stage">
      <div class="toolbar">
        <div class="left">
          <div class="group-editor">
            <label for="groupInput">group_id (через запятую)</label>
            <input id="groupInput" placeholder="Напр. 6750993, 6750994" value="{{GROUPS_TEXT}}" />
          </div>
          <button class="btn primary" id="saveGroups">Сохранить</button>
          <button class="btn ghost" id="scanGroups">Сканировать группы</button>
          <button class="btn ghost" id="enrichEmails">Обновить почты</button>
          <button class="btn ghost" id="reloadBtn">Обновить профили</button>
        </div>
        <div class="right">
          <label class="search">
            <svg width="16" height="16" viewBox="0 0 24 24"><path d="m21 21-4.3-4.3M10 18a8 8 0 1 1 0-16 8 8 0 0 1 0 16Z" fill="none" stroke="currentColor"/></svg>
            <input id="q" placeholder="Поиск по имени/ID/группе"/>
          </label>
        </div>
      </div>

      <div class="note info" id="statusMsg">
        Укажите или выберите group_id и нажмите «Сохранить», чтобы увидеть профили из AdsPower.
      </div>

      <div class="group-picker" id="groupList" aria-label="Список групп"></div>

      <div id="list" class="list" role="list"></div>

      <div class="small" style="padding:0 6px 10px">Найдено: <b id="count">0</b></div>
    </section>
  </div>

<script>
const NO_ACCOUNT_MARKER = "{{NO_ACCOUNT}}";
const $ = (s)=>document.querySelector(s);
const listEl = $("#list"); const qinp = $("#q"); const countSpan = $("#count");
const statusMsg = $("#statusMsg"); const groupInput = $("#groupInput"); const groupList = $("#groupList");
const saveBtn = $("#saveGroups"); const scanBtn = $("#scanGroups"); const enrichBtn = $("#enrichEmails"); const reloadBtn = $("#reloadBtn");
const CSRF = (document.querySelector('meta[name="csrf"]')?.getAttribute('content')) || '';
const groupsMeta = document.querySelector('meta[name="groups"]');
if (groupInput && groupsMeta){ groupInput.value = groupsMeta.getAttribute('content') || ''; }

const parseGroupTokens = (value)=> (value||'').split(/[,\s]+/).map(t=>t.trim()).filter(Boolean);

let selectedGroups = new Set(parseGroupTokens(groupInput?.value || ''));
let availableGroups = []; let all = []; let filtered = [];
let emailJobTimer = null; let currentEmailJobId = null;

function esc(s){ const d=document.createElement('div'); d.innerText = (s==null?'':String(s)); return d.innerHTML; }
function isNoAccount(value){ return String(value || '').trim() === NO_ACCOUNT_MARKER; }
function displayEmail(value){ const raw = String(value == null ? '' : value); if (!raw) return '—'; return isNoAccount(raw) ? 'Нет аккаунта' : raw; }
function displayOtp(value){ const raw = String(value == null ? '' : value).trim(); return raw || '—'; }
function syncGroupInputFromSelection(){ if (!groupInput) return; groupInput.value = Array.from(selectedGroups).join(', '); }

function renderGroupPicker(){
  if (!groupList) return;
  if (!availableGroups.length){
    groupList.innerHTML = '<div class="empty-groups">Список групп пуст или недоступен.</div>'; return;
  }
  groupList.innerHTML = '';
  for (const group of availableGroups){
    const gid = String(group.id || '');
    const row = document.createElement('label');
    row.className = 'gitem' + (selectedGroups.has(gid) ? ' selected' : '');
    row.innerHTML = `
      <input type="checkbox" data-gid="${esc(gid)}" ${selectedGroups.has(gid)?'checked':''} />
      <span class="gname">${esc(group.name || 'Без имени')}</span>
      <span class="gid">#${esc(gid)}</span>`;
    const checkbox = row.querySelector('input');
    checkbox?.addEventListener('change', ()=>{
      if (!gid) return;
      if (checkbox.checked){ selectedGroups.add(gid); row.classList.add('selected'); }
      else{ selectedGroups.delete(gid); row.classList.remove('selected'); }
      syncGroupInputFromSelection();
    });
    groupList.appendChild(row);
  }
}
function updateSelectionFromInput(){ selectedGroups = new Set(parseGroupTokens(groupInput?.value || '')); renderGroupPicker(); }

function mergeSavedAccounts(savedList){
  if (!Array.isArray(savedList) || !savedList.length) return;
  const byProfile = new Map();
  for (const entry of savedList){
    const key = String(entry.profile_id || entry.profileId || ''); if (!key) continue; byProfile.set(key, entry);
  }
  let changed = false;
  for (const it of all){
    const saved = byProfile.get(String(it.profile_id || '')); if (!saved) continue;
    if (saved.google_email && it.google_email !== saved.google_email){ it.google_email = saved.google_email; changed = true; }
    if (saved.google_name && it.google_name !== saved.google_name){ it.google_name = saved.google_name; changed = true; }
    const savedOtp = saved.otp_secret || saved.otpSecret;
    if (savedOtp && it.otp_secret !== savedOtp){ it.otp_secret = savedOtp; changed = true; }
  }
  if (changed){ render(); }
}

function applyEmailResults(entries){
  if (!Array.isArray(entries) || !entries.length) return;
  let changed = false;
  for (const entry of entries){
    const pid = String(entry.profile_id || entry.profileId || '');
    const email = entry.email || entry.google_email; if (!pid || !email) continue;
    const target = all.find(it => String(it.profile_id) === pid); if (!target) continue;
    if (target.google_email !== email){ target.google_email = email; changed = true; }
    const gname = entry.google_name || entry.name;
    if (gname && target.google_name !== gname){ target.google_name = gname; changed = true; }
  }
  if (changed){ render(); }
}

function initial(name){ const s = String(name||'').trim(); return s ? s[0].toUpperCase() : '•'; }
function tagsTpl(tags){
  if (!Array.isArray(tags) || !tags.length) return '';
  return tags.slice(0,3).map(tag=>{
    if (tag && typeof tag === 'object'){ return `<span class="tag">${esc(tag.name||tag.remark||'tag')}</span>`; }
    return `<span class="tag">${esc(tag)}</span>`;
  }).join('');
}
function setStatus(text, tone){ if (!statusMsg) return; statusMsg.textContent = text; statusMsg.className = 'note ' + (tone||'info'); }
function renderEmpty(msg){ if (!listEl) return; listEl.innerHTML = `<div class="empty">${esc(msg||'Ничего не найдено')}</div>`; if (countSpan) countSpan.textContent = '0'; }
function render(){
  if (!listEl) return;
  if (!filtered.length){ renderEmpty('Профили не найдены'); return; }
  listEl.innerHTML = '';
  for (const it of filtered){
    const row = document.createElement('div'); row.className = 'row';
    const emailTags = [];
    const emailVal = displayEmail(it.google_email);
    const emailClass = 'pill mono' + (isNoAccount(it.google_email) ? ' pill-no-account' : '');
    emailTags.push(`<span class="${emailClass}" title="Google email">${esc(emailVal)}</span>`);
    if (it.google_name){ emailTags.push(`<span class="pill" title="Google name">${esc(it.google_name)}</span>`); }
    if (it.otp_secret){ emailTags.push(`<span class="pill mono" title="2FA код">${esc(displayOtp(it.otp_secret))}</span>`); }
    row.innerHTML = `
      <div class="avatar">${esc(initial(it.name || it.profile_id))}</div>
      <div class="main">
        <div class="title">${esc(it.name||'(без имени)')}</div>
        <div class="meta">
          <span class="pill mono" title="Profile ID">${esc(it.profile_id||'—')}</span>
          <span class="pill" title="Group ID">${esc(it.group_id||'—')}</span>
          ${tagsTpl(it.tags||[])}
          ${emailTags.join('')}
        </div>
      </div>
      <div class="actions">
        <button class="btn ghost small-copy" data-id="${esc(it.profile_id||'')}">Скопировать ID</button>
      </div>`;
    listEl.appendChild(row);
  }
  if (countSpan) countSpan.textContent = String(filtered.length);
  bindRowEvents();
}
function bindRowEvents(){
  if (!listEl) return;
  listEl.querySelectorAll('button.small-copy').forEach(btn=>{
    btn.addEventListener('click', async ()=>{
      try{
        await navigator.clipboard.writeText(btn.dataset.id||'');
        const old = btn.textContent; btn.textContent = '✓'; setTimeout(()=>btn.textContent = old, 600);
      }catch(_){ alert('Не удалось скопировать'); }
    });
  });
}
function applyFilter(){
  const q = (qinp?.value||'').trim().toLowerCase();
  if (!q){ filtered = all.slice(); }
  else{
    filtered = all.filter(it=>{
      const haystack = [it.name, it.profile_id, it.group_id, it.google_email, displayEmail(it.google_email), it.google_name, it.otp_secret];
      return haystack.some(val => String(val||'').toLowerCase().includes(q));
    });
  }
  render();
}

async function scanGroups(){
  if (scanBtn) scanBtn.disabled = true;
  try{
    setStatus('Сканируем группы…','info');
    const resp = await fetch('/api/accounts/groups/scan', { cache: 'no-store' });
    const data = await resp.json();
    if (!data.ok){ setStatus(data.error || 'Не удалось получить список групп', 'err'); return; }
    availableGroups = Array.isArray(data.groups) ? data.groups : [];
    const serverSelected = Array.isArray(data.selected) ? data.selected : parseGroupTokens(groupInput?.value || '');
    selectedGroups = new Set(serverSelected);
    syncGroupInputFromSelection(); renderGroupPicker();
    setStatus(`Найдено групп: ${availableGroups.length}`, 'ok');
  }catch(_){ setStatus('Не удалось получить список групп', 'err'); }
  finally{ if (scanBtn) scanBtn.disabled = false; }
}

function stopEmailJobPolling(){ if (emailJobTimer){ clearInterval(emailJobTimer); emailJobTimer = null; } }

async function startEmailJob(){
  if (enrichBtn?.disabled) return;
  if (!window.confirm('Запустить сбор почт только для профилей без сохранённого email?')) return;
  enrichBtn.disabled = true;
  try{
    const resp = await fetch('/api/accounts/enrich_emails', {
      method:'POST', headers:{'Content-Type':'application/json','X-CSRF':CSRF},
      body: JSON.stringify({ only_missing: true })
    });
    const data = await resp.json();
    if (!data.ok){
      setStatus(data.error || 'Не удалось запустить сбор почт', 'err');
      enrichBtn.disabled = false; return;
    }
    currentEmailJobId = data.job_id;
    if (data.already_running){ setStatus('Сканирование почт уже выполняется…', 'info'); }
    else{ setStatus(`Почты: 0/${data.total || 0} — запуск…`, 'info'); }
    pollEmailJob(currentEmailJobId);
  }catch(_){
    alert('Сеть недоступна'); enrichBtn.disabled = false;
  }
}

function pollEmailJob(jobId){
  stopEmailJobPolling();
  const tick = async ()=>{
    try{
      const resp = await fetch(`/api/accounts/enrich_emails/${encodeURIComponent(jobId)}`, { cache: 'no-store' });
      const data = await resp.json();
      if (!data.ok){
        setStatus(data.error || 'Не удалось получить статус', 'err');
        stopEmailJobPolling(); enrichBtn.disabled = false; return;
      }
      applyEmailResults(data.recent);
      const total = data.total || 0; const done = data.done || 0;
      const prefix = `Почты: ${done}/${total}`;
      if (data.status === 'completed'){
        setStatus(prefix + ' — готово', 'ok');
        stopEmailJobPolling(); enrichBtn.disabled = false; await load();
      }else if (data.status === 'error'){
        setStatus((data.error || 'Ошибка при сборе почт'), 'err');
        stopEmailJobPolling(); enrichBtn.disabled = false;
      }else{
        setStatus(prefix + ' …', 'info');
      }
    }catch(_){ setStatus('Не удалось получить статус задачи', 'err'); }
  };
  tick().catch(()=>{});
  emailJobTimer = setInterval(()=> tick().catch(()=>{}), 3000);
}

async function load(){
  try{
    setStatus('Загружаем профили…','info');
    const url = new URL('/api/accounts/query', location.origin);
    const q = (qinp?.value||'').trim(); if (q) url.searchParams.set('q', q);
    const resp = await fetch(url.toString(), { cache: 'no-store' });
    const data = await resp.json();
    if (!data.ok){ setStatus(data.error || 'Не удалось получить данные из AdsPower', 'err'); all=[]; renderEmpty('Ошибка загрузки'); return; }
    all = Array.isArray(data.items) ? data.items : [];
    const serverGroups = Array.isArray(data.groups) ? data.groups : [];
    if (groupInput){ groupInput.value = serverGroups.join(', '); }
    if (serverGroups.length){ selectedGroups = new Set(serverGroups); syncGroupInputFromSelection(); renderGroupPicker(); }
    if (!serverGroups.length){ setStatus('Добавьте group_id, чтобы показать профили.', 'warn'); }
    else if (!all.length){ setStatus('Профили для указанных group_id не найдены.', 'warn'); }
    else{ setStatus(`Загружено ${all.length} профилей`, 'ok'); }
    try{
      const savedResp = await fetch('/api/accounts/saved', { cache: 'no-store' });
      const savedData = await savedResp.json();
      if (savedData.ok){ mergeSavedAccounts(savedData.items || []); }
    }catch(_){ /* ignore */ }
    filtered = all.slice(); render();
  }catch(_){
    setStatus('Сеть недоступна', 'err'); all = []; renderEmpty('Сеть недоступна');
  }
}

let qTimer = null;
qinp?.addEventListener('input', ()=>{ clearTimeout(qTimer); qTimer = setTimeout(()=>{ 
  const q = (qinp?.value||'').trim().toLowerCase();
  if (!q){ filtered = all.slice(); } else{
    filtered = all.filter(it=>{
      const hs = [it.name, it.profile_id, it.group_id, it.google_email, (String(it.google_email||'')===NO_ACCOUNT_MARKER?'нет аккаунта':''), it.google_name, it.otp_secret];
      return hs.some(v => String(v||'').toLowerCase().includes(q));
    });
  }
  render();
}, 150); });

groupInput?.addEventListener('blur', updateSelectionFromInput);
groupInput?.addEventListener('change', updateSelectionFromInput);
scanBtn?.addEventListener('click', ()=> scanGroups());
enrichBtn?.addEventListener('click', ()=> startEmailJob());
saveBtn?.addEventListener('click', async ()=>{
  if (!groupInput) return;
  selectedGroups = new Set(parseGroupTokens(groupInput.value || ''));
  saveBtn.disabled = true;
  try{
    const resp = await fetch('/api/accounts/groups', {
      method:'POST', headers:{'Content-Type':'application/json','X-CSRF':CSRF},
      body: JSON.stringify({ group_ids: groupInput.value || '' })
    });
    const data = await resp.json();
    if (!data.ok){ alert(data.error || 'Не удалось сохранить группы'); return; }
    const savedGroups = Array.isArray(data.groups) ? data.groups : [];
    groupInput.value = savedGroups.join(', ');
    selectedGroups = new Set(savedGroups);
    syncGroupInputFromSelection(); renderGroupPicker();
    setStatus('Группы сохранены, обновляем список…','info');
    await load();
  }catch(_){ alert('Сеть недоступна'); }
  finally{ saveBtn.disabled = false; }
});
reloadBtn?.addEventListener('click', ()=> load());
scanGroups().catch(()=>{});
load().catch(()=>{});
</script>
</body>
</html>
"""

# ============================ Инициализация роутов ============================

def init_accounts_list(app: Flask, settings: Settings, db: CampaignDB) -> None:
    """
    Регистрация страницы /accounts и API:
      - GET  /accounts, /accounts/           — страница
      - GET  /api/accounts/query             — список (JSON)
      - GET  /api/accounts/groups/scan       — чтение доступных group_id из AdsPower
      - POST /api/accounts/enrich_emails     — запуск сбора почт по профилям
      - GET  /api/accounts/enrich_emails/<id> — статус задачи
      - POST /api/accounts/delete            — удалить один
      - POST /api/accounts/delete_bulk       — удалить пачку
    """
    primary_db_path = os.path.abspath(getattr(db, "path", "") or "")

    def _propagate_secret(user_email: str, profile_id: str, secret: Optional[str]) -> None:
        if not secret:
            return
        extra_paths = _legacy_secret_paths(primary_db_path)
        if not extra_paths:
            return
        _mirror_otp_secret_to_paths(extra_paths, user_email, profile_id, secret)

    def _save_and_mirror_secret(
        user_email: str,
        profile_id: str,
        account_id: Optional[str],
        candidate_secret: Optional[str],
    ) -> Optional[str]:
        normalized = db._normalize_otp_secret(candidate_secret)
        if not normalized:
            return None
        saved = normalized
        if account_id:
            saved = db.update_account_otp_secret(account_id, normalized) or normalized
        _propagate_secret(user_email, profile_id, saved)
        return saved

    # --- Страница /accounts ---
    @app.get("/accounts")
    def accounts_list_page() -> Response:
        try:
            email = _require_user()
        except Exception:
            return redirect(url_for("auth_login"))

        csrf = _read_csrf()
        groups = db.get_user_group_ids(email)
        group_text = ", ".join(groups)
        safe_group = html.escape(group_text)
        page_html = (
            PAGE_HTML
            .replace("{{CSRF}}", csrf)
            .replace("{{GROUPS_TEXT}}", safe_group)
            .replace("{{GROUPS_META}}", safe_group)
            .replace("{{NO_ACCOUNT}}", _NO_ACCOUNT_EMAIL)
        )
        return make_response(page_html)

    @app.get("/accounts/")
    def accounts_list_page_slash() -> Response:
        return redirect("/accounts", code=301)

    # --- API: список ---
    @app.get("/api/accounts/query")
    def accounts_query() -> Response:
        try:
            email = _require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401

        q = (request.args.get("q") or "").strip().lower()
        groups = db.get_user_group_ids(email)
        if not groups:
            return jsonify({"ok": True, "items": [], "groups": [], "total": 0})

        items, error = _fetch_adspower_profiles(groups)
        account_map = {str(it.get("profile_id")): it for it in db.list_accounts(email)}

        def _ensure_account_entry(profile: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            pid = str(profile.get("profile_id") or "").strip()
            if not pid:
                return None
            acc = account_map.get(pid)
            if acc:
                return acc
            try:
                acc_id = db.add_account(email, pid, profile.get("name") or pid, [], profile.get("otp_secret"))
            except Exception as exc:
                print(f"[accounts] failed to create account entry pid={pid} err={exc}", flush=True)
                return None
            secret_norm = db._normalize_otp_secret(profile.get("otp_secret"))
            acc = {
                "id": acc_id,
                "name": profile.get("name") or pid,
                "profile_id": pid,
                "created_at": time.time(),
                "google_email": None,
                "google_name": None,
                "info_updated_at": None,
                "otp_secret": secret_norm,
            }
            account_map[pid] = acc
            _propagate_secret(email, pid, secret_norm)
            return acc

        for it in items:
            pid = str(it.get("profile_id") or "").strip()
            if not pid:
                continue
            acc = account_map.get(pid)
            if not acc:
                acc = _ensure_account_entry(it)
            if not acc:
                continue
            account_id = acc.get("id")
            it["account_id"] = account_id
            it["google_email"] = acc.get("google_email")
            it["google_name"] = acc.get("google_name")
            it["info_updated_at"] = acc.get("info_updated_at")

            otp_db = db._normalize_otp_secret(acc.get("otp_secret"))
            otp_remote = db._normalize_otp_secret(it.get("otp_secret"))
            final_secret = otp_db
            if otp_remote and otp_remote != otp_db:
                saved = _save_and_mirror_secret(email, pid, account_id, otp_remote)
                if saved:
                    final_secret = saved
                    acc["otp_secret"] = saved
            elif final_secret:
                _propagate_secret(email, pid, final_secret)

            if not final_secret and otp_remote:
                final_secret = otp_remote
            if final_secret:
                it["otp_secret"] = final_secret

        if q:
            def _match_account(entry: Dict[str, Any]) -> bool:
                fields = [
                    str(entry.get("name", "")).lower(),
                    str(entry.get("profile_id", "")).lower(),
                    str(entry.get("group_id", "")).lower(),
                    str(entry.get("google_email", "")).lower(),
                    str(entry.get("google_name", "")).lower(),
                    str(entry.get("otp_secret") or "").lower(),
                ]
                if fields[3] == _NO_ACCOUNT_EMAIL:
                    fields.append("нет аккаунта")
                return any(q in val for val in fields)
            items = [it for it in items if _match_account(it)]

        ok = error is None
        return jsonify({"ok": ok, "items": items, "groups": groups, "total": len(items), "error": error})

    @app.post("/api/accounts/groups")
    def accounts_update_groups() -> Response:
        try:
            email = _require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        _check_csrf(request.headers.get("X-CSRF"))

        data = request.get_json(silent=True) or {}
        raw = data.get("group_ids")
        groups = _normalize_group_ids(raw)
        saved = db.set_user_group_ids(email, groups)
        return jsonify({"ok": True, "groups": saved})

    @app.get("/api/accounts/groups/scan")
    def accounts_scan_groups() -> Response:
        try:
            email = _require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401

        groups, error = _fetch_adspower_group_list()
        selected = db.get_user_group_ids(email)
        return jsonify({"ok": error is None, "groups": groups, "selected": selected, "error": error})

    @app.post("/api/accounts/enrich_emails")
    def accounts_enrich_emails() -> Response:
        try:
            email = _require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        _check_csrf(request.headers.get("X-CSRF"))

        running = _find_running_email_job(email)
        if running:
            return jsonify({
                "ok": True,
                "job_id": running["id"],
                "total": running.get("total", 0),
                "status": running.get("status"),
                "already_running": True,
            })

        groups = db.get_user_group_ids(email)
        if not groups:
            return jsonify({"ok": False, "error": "no_groups_configured"}), 400

        items, err = _fetch_adspower_profiles(groups)
        if err and not items:
            return jsonify({"ok": False, "error": err}), 400

        existing = {str(it.get("profile_id")): it for it in db.list_accounts(email)}
        accounts: List[Dict[str, Any]] = []
        for profile in items:
            pid = str(profile.get("profile_id") or "").strip()
            if not pid:
                continue
            acc = existing.get(pid)
            if not acc:
                acc_id = db.add_account(email, pid, profile.get("name") or pid, [], profile.get("otp_secret"))
                acc = {
                    "id": acc_id,
                    "name": profile.get("name") or pid,
                    "profile_id": pid,
                    "google_email": None,
                    "otp_secret": db._normalize_otp_secret(profile.get("otp_secret")),
                }
                existing[pid] = acc
            else:
                otp_remote = db._normalize_otp_secret(profile.get("otp_secret"))
                if otp_remote and otp_remote != db._normalize_otp_secret(acc.get("otp_secret")):
                    updated = db.update_account_otp_secret(acc.get("id"), otp_remote)
                    if updated:
                        acc["otp_secret"] = updated
            _propagate_secret(email, pid, acc.get("otp_secret"))
            accounts.append(acc)

        data = request.get_json(silent=True) or {}
        only_missing = bool(data.get("only_missing", False))
        if only_missing:
            accounts = [it for it in accounts if not it.get("google_email") or str(it.get("google_email")).strip() == _NO_ACCOUNT_EMAIL]

        if not accounts:
            return jsonify({"ok": False, "error": "no_accounts_for_enrich"}), 400

        total_candidates = len(accounts)

        # Подрежем батч, если слишком жирно
        if _EMAIL_BATCH_LIMIT > 0 and total_candidates > _EMAIL_BATCH_LIMIT:
            accounts = accounts[:_EMAIL_BATCH_LIMIT]
            print(
                f"[accounts] email job: candidates={total_candidates} clipped_to={_EMAIL_BATCH_LIMIT}",
                flush=True,
            )

        job = _start_email_enrich_job(db, email, accounts)
        response_payload = {
            "ok": True,
            "job_id": job["id"],
            "total": job["total"],
            "status": job["status"],
            "total_candidates": total_candidates,
        }
        if _EMAIL_BATCH_LIMIT > 0:
            response_payload["batch_limit"] = _EMAIL_BATCH_LIMIT
            response_payload["clipped"] = total_candidates > job["total"]
        print(f"[accounts] email job response: {response_payload}", flush=True)
        return jsonify(response_payload)

    @app.get("/api/accounts/enrich_emails/<job_id>")
    def accounts_enrich_status(job_id: str) -> Response:
        try:
            _ = _require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        job = _get_email_job(job_id)
        if not job:
            return jsonify({"ok": False, "error": "job_not_found"}), 404
        payload = {"ok": True, **job}
        print(f"[accounts] job status {job_id}: {payload}", flush=True)
        return jsonify(payload)

    @app.get("/api/accounts/saved")
    def accounts_saved() -> Response:
        try:
            email = _require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        rows = db.list_accounts(email)
        print(f"[accounts] /api/accounts/saved user={email} rows={len(rows)}", flush=True)
        return jsonify({"ok": True, "items": rows, "total": len(rows)})

    # --- Вспомогательная: удалить запись из таблицы accounts пользователя ---
    def _delete_from_db(acc_id: str, user_email: str) -> Tuple[bool, Optional[str]]:
        """
        Возвращает (ok, profile_id) — profile_id нужен, чтобы (по желанию) удалить и в AdsPower.
        """
        try:
            with db.conn:  # type: ignore[attr-defined]
                q = db.conn.execute("SELECT user_email, profile_id FROM accounts WHERE id=? LIMIT 1", (acc_id,))
                r = q.fetchone()
                if not r or str(r[0]) != str(user_email):
                    return False, None
                profile_id = str(r[1] or "")
                db.conn.execute("DELETE FROM accounts WHERE id=?", (acc_id,))
                return True, profile_id
        except Exception:
            return False, None

    @app.post("/api/accounts/delete")
    def accounts_delete_one() -> Response:
        try:
            email = _require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        _check_csrf(request.headers.get("X-CSRF"))

        data = request.get_json(silent=True) or {}
        acc_id = str(data.get("id") or "").strip()
        also_adsp = bool(data.get("adspower"))

        if not acc_id:
            return jsonify({"ok": False, "error": "empty_id"}), 400

        ok, profile_id = _delete_from_db(acc_id, email)
        adsp_ok = False
        if ok and also_adsp and profile_id:
            try:
                adsp_ok, _ = _delete_adspower_profile(profile_id)  # best-effort
            except Exception:
                adsp_ok = False
        return jsonify({"ok": ok, "adspower_removed": bool(adsp_ok)})

    @app.post("/api/accounts/delete_bulk")
    def accounts_delete_bulk() -> Response:
        try:
            email = _require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        _check_csrf(request.headers.get("X-CSRF"))

        data = request.get_json(silent=True) or {}
        ids = data.get("ids") or []
        also_adsp = bool(data.get("adspower"))

        if not isinstance(ids, list) or not ids:
            return jsonify({"ok": False, "error": "empty_ids"}), 400

        n_ok = 0
        n_adsp = 0
        for raw in ids:
            acc_id = str(raw or "").strip()
            if not acc_id:
                continue
            ok, profile_id = _delete_from_db(acc_id, email)
            if ok:
                n_ok += 1
                if also_adsp and profile_id:
                    try:
                        adsp_ok, _ = _delete_adspower_profile(profile_id)
                        if adsp_ok:
                            n_adsp += 1
                    except Exception:
                        pass

        return jsonify({"ok": True, "deleted": n_ok, "adspower_removed": n_adsp})
