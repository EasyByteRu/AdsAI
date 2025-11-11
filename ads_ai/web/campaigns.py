# -*- coding: utf-8 -*-
"""
ads_ai/web/camping.py

Production-модуль создания кампаний Google Ads.
— Только кампании. Аккаунты берём из account.py (таблица accounts), без своих CRUD.
— Глубокая интеграция с AdsPower (webdriver), VarStore, Trace/Artifacts, Runner, (опц.) Gemini.
— Пер-профильные блокировки, ретраи, стабильные автопереходы (новая кампания / цель), чек авторизации.
— Живые логи через SSE + быстрый канал превью (без мусора), скриншоты и HTML-снапшоты.
— Единый JSONL-лог LLM (prompt/response) на ран.
— Строгая типизация, dataclass'ы, чистый дизайн, минимальные зависимости.

Публичный контракт НЕ МЕНЯЛСЯ:
    def init_create_companies(app: Flask, settings: Settings) -> None
"""

from __future__ import annotations

import ast
import base64
import html
import inspect
import json
import logging
import os
import queue
import re
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from flask import (
    Flask, Response, abort, jsonify, make_response, redirect, request, send_file,
    session, url_for,
)

# =============================== ЛОГИРОВАНИЕ ===============================

logging.captureWarnings(True)
log = logging.getLogger("ads_ai.web.camping")
if not log.handlers:
    lvl = getattr(logging, os.getenv("ADS_AI_LOG", "INFO").upper(), logging.INFO)
    logging.basicConfig(level=lvl, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

# =============================== МЯГКИЕ ИМПОРТЫ ============================

from ads_ai.config.settings import Settings
from ads_ai.storage.vars import VarStore

try:
    from ads_ai.tracing.trace import make_trace  # type: ignore
except Exception:  # pragma: no cover
    make_trace = None  # type: ignore

try:
    from ads_ai.tracing.artifacts import Artifacts, take_screenshot, save_html_snapshot  # type: ignore
except Exception:  # pragma: no cover
    Artifacts = None  # type: ignore

    def take_screenshot(*_a, **_k):  # type: ignore
        raise RuntimeError("Artifacts not available")

    def save_html_snapshot(*_a, **_k):  # type: ignore
        raise RuntimeError("Artifacts not available")

try:
    from ads_ai.llm.gemini import GeminiClient  # type: ignore
except Exception:  # pragma: no cover
    GeminiClient = None  # type: ignore

try:
    from ads_ai.llm import prompts as llm_prompts  # type: ignore
except Exception:  # pragma: no cover
    llm_prompts = None  # type: ignore

try:
    from ads_ai.core.runner import Runner  # type: ignore
except Exception:  # pragma: no cover
    Runner = None  # type: ignore

try:
    from ads_ai.browser import adspower as adspower_mod  # type: ignore
except Exception:  # pragma: no cover
    adspower_mod = None  # type: ignore

try:
    from examples.steps.code_for_confrim import (  # type: ignore
        set_profile_totp_secret as _set_confirm_totp_secret,
        clear_profile_totp_secret as _clear_confirm_totp_secret,
    )
except Exception:  # pragma: no cover
    _set_confirm_totp_secret = None  # type: ignore
    _clear_confirm_totp_secret = None  # type: ignore

# Selenium soft-imports (только для ручных действий)
try:
    from selenium.webdriver.common.by import By  # type: ignore
    from selenium.common.exceptions import WebDriverException  # type: ignore
except Exception:  # pragma: no cover
    class _By:
        CSS_SELECTOR = "css selector"
        XPATH = "xpath"
        TAG_NAME = "tag name"
    By = _By()  # type: ignore
    WebDriverException = Exception  # type: ignore

# =============================== УТИЛИТЫ ===================================

def _json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return "{}"

def _escape(s: Any) -> str:
    return html.escape("" if s is None else str(s), quote=True)

def _now_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}_{int(time.time())}"

def _utc_ts() -> float:
    return time.time()

def _clip_text(s: Any, limit: int = 2000) -> str:
    try:
        t = str(s or "")
    except Exception:
        t = ""
    if len(t) > limit:
        return t[:limit] + "\n…[truncated]"
    return t

def _preview_json(obj: Any, limit: int = 2000) -> str:
    try:
        txt = json.dumps(obj, ensure_ascii=False)
    except Exception:
        try:
            txt = str(obj)
        except Exception:
            txt = ""
    return _clip_text(txt, limit)

# =============================== PATHS =====================================

@dataclass
class _Paths:
    artifacts: Path
    shots: Path
    html: Path
    traces: Path
    vars_file: Path
    db_file: Path

def _resolve_paths(settings: Settings) -> _Paths:
    ps = getattr(settings, "paths", None)

    def gp(name: str, fallback: Optional[Path] = None) -> Path:
        v = getattr(ps, name, None)
        if v:
            return Path(v)
        return fallback or Path.cwd()

    artifacts = gp("artifacts_root", Path.cwd() / "artifacts")
    shots_root = getattr(ps, "screenshots_root", None) or getattr(ps, "screenshots_dir", None)
    shots = Path(shots_root) if shots_root else artifacts / "screenshots"

    html_root = getattr(ps, "html_snaps_root", None) or getattr(ps, "html_snaps_dir", None)
    htmlp = Path(html_root) if html_root else (artifacts / "html_snaps")

    traces = getattr(ps, "traces_root", None) or getattr(ps, "traces_dir", None)
    traces = Path(traces) if traces else artifacts / "traces"

    vars_file = Path(getattr(ps, "vars_file", artifacts / "vars.json"))
    db_file = artifacts / "campaigns.db"

    for d in (artifacts, shots, htmlp, traces):
        d.mkdir(parents=True, exist_ok=True)

    return _Paths(artifacts=artifacts, shots=shots, html=htmlp, traces=traces, vars_file=vars_file, db_file=db_file)

# =============================== ДАННЫЕ/DB ==================================

@dataclass
class CampaignSpec:
    goal: str
    landing_url: str
    description: str
    budget_daily: float
    geo: str
    language: str
    profile_id: str
    campaign_type: str = "search"
    currency_sign: str = "₽"
    account_id: Optional[str] = None  # id из account.py

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)

class CampaignDB:
    """
    SQLite с WAL, отдельные таблицы: campaigns/events.
    Таблицу accounts создаёт master (account.py) — читаем её только read-only.
    """
    def __init__(self, path: Path):
        self.path = str(path)
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self._lock = threading.Lock()
        self._migrate()

    def _migrate(self) -> None:
        with self.conn:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS campaigns (
                    id TEXT PRIMARY KEY,
                    user_email TEXT NOT NULL,
                    spec_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    run_id TEXT NOT NULL,
                    profile_id TEXT NOT NULL,
                    error TEXT NOT NULL DEFAULT ''
                );
                CREATE INDEX IF NOT EXISTS idx_campaigns_user ON campaigns(user_email, created_at);

                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    ts REAL NOT NULL,
                    type TEXT NOT NULL,
                    data TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_events_task ON events(task_id, id);

                CREATE TABLE IF NOT EXISTS accounts (
                    id TEXT PRIMARY KEY,
                    user_email TEXT NOT NULL,
                    profile_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    cookies_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    otp_api_url TEXT,
                    otp_secret TEXT,
                    google_email TEXT,
                    google_name TEXT,
                    info_updated_at REAL
                );
                CREATE INDEX IF NOT EXISTS idx_acc_user_profile ON accounts(user_email, profile_id, created_at);

                CREATE TABLE IF NOT EXISTS user_adspower_groups (
                    user_email TEXT PRIMARY KEY,
                    group_ids TEXT NOT NULL,
                    updated_at REAL NOT NULL
                );
                """
            )
            self._ensure_column("accounts", "otp_api_url", "TEXT")
            self._ensure_column("accounts", "otp_secret", "TEXT")
            self._ensure_column("accounts", "google_email", "TEXT")
            self._ensure_column("accounts", "google_name", "TEXT")
            self._ensure_column("accounts", "info_updated_at", "REAL")

    # campaigns
    def create(self, email: str, spec: CampaignSpec, run_id: str) -> str:
        cid = _now_id("cmp")
        now = _utc_ts()
        with self.conn:
            self.conn.execute(
                "INSERT INTO campaigns (id,user_email,spec_json,status,created_at,updated_at,run_id,profile_id) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (cid, email, _json(spec.as_dict()), "queued", now, now, run_id, spec.profile_id),
            )
        return cid

    def get(self, task_id: str) -> Optional[Dict[str, Any]]:
        q = self.conn.execute("SELECT * FROM campaigns WHERE id=?", (task_id,))
        row = q.fetchone()
        if not row:
            return None
        keys = [d[0] for d in q.description]
        d = dict(zip(keys, row))
        d["spec"] = json.loads(d.pop("spec_json", "{}") or "{}")
        return d

    def list_for_user(self, email: str, limit: int = 100) -> List[Dict[str, Any]]:
        q = self.conn.execute(
            "SELECT id,status,created_at,updated_at,run_id,profile_id FROM campaigns "
            "WHERE user_email=? ORDER BY created_at DESC LIMIT ?",
            (email, int(limit)),
        )
        return [
            {
                "id": r[0], "status": r[1],
                "created_at": float(r[2]), "updated_at": float(r[3]),
                "run_id": r[4], "profile_id": r[5],
            }
            for r in q.fetchall()
        ]

    def update_status(self, task_id: str, status: str, error: str = "") -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE campaigns SET status=?, updated_at=?, error=? WHERE id=?",
                (status, _utc_ts(), error or "", task_id),
            )

    def delete(self, task_id: str, user_email: str) -> bool:
        with self.conn:
            q = self.conn.execute("SELECT user_email FROM campaigns WHERE id=? LIMIT 1", (task_id,))
            r = q.fetchone()
            if not r or r[0] != user_email:
                return False
            self.conn.execute("DELETE FROM events WHERE task_id=?", (task_id,))
            self.conn.execute("DELETE FROM campaigns WHERE id=?", (task_id,))
        return True

    # events
    def append_event(self, task_id: str, ev_type: str, payload: Dict[str, Any]) -> int:
        with self._lock:
            with self.conn:
                self.conn.execute(
                    "INSERT INTO events (task_id, ts, type, data) VALUES (?, ?, ?, ?)",
                    (task_id, _utc_ts(), ev_type, _json(payload)),
                )
                rowid = int(self.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                return rowid

    def events_since(self, task_id: str, last_id: int) -> List[Dict[str, Any]]:
        q = self.conn.execute(
            "SELECT id,ts,type,data FROM events WHERE task_id=? AND id>? ORDER BY id ASC",
            (task_id, int(last_id)),
        )
        out: List[Dict[str, Any]] = []
        for r in q.fetchall():
            try:
                payload = json.loads(r[3] or "{}")
            except Exception:
                payload = {}
            out.append({"id": int(r[0]), "ts": float(r[1]), "type": str(r[2]), "data": payload})
        return out

    # accounts
    @staticmethod
    def _normalize_otp_secret(raw: Optional[str]) -> Optional[str]:
        if raw is None:
            return None
        value = str(raw).strip()
        if not value:
            return None
        if value.lower().startswith("otpauth://"):
            return value
        secret_part, suffix = value, ""
        if "|" in value:
            secret_part, suffix = value.split("|", 1)
        clean_secret = re.sub(r"[^A-Z2-7]", "", secret_part.upper())
        if not clean_secret:
            return None
        suffix_tokens = [tok.strip() for tok in re.split(r"[;,]", suffix) if tok.strip()]
        if suffix_tokens:
            return clean_secret + "|" + ",".join(suffix_tokens)
        return clean_secret

    def _ensure_column(self, table: str, column: str, ddl_type: str) -> None:
        try:
            cur = self.conn.execute(f"PRAGMA table_info({table})")
            cols = {str(row[1]).lower() for row in cur.fetchall()}
            if column.lower() not in cols:
                with self.conn:
                    self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl_type}")
        except Exception:
            pass

    def add_account(
        self,
        email: str,
        profile_id: str,
        name: str,
        cookies: List[Dict[str, Any]],
        otp_secret: Optional[str] = None,
    ) -> str:
        email_s = str(email or "").strip()
        if not email_s:
            raise ValueError("email is required")
        profile_s = str(profile_id or "").strip()
        if not profile_s:
            raise ValueError("profile_id is required")
        name_s = str(name or "").strip() or profile_s
        cookies_json = _json(cookies or [])
        now = _utc_ts()
        otp_secret_norm = self._normalize_otp_secret(otp_secret)

        with self._lock:
            with self.conn:
                row = self.conn.execute(
                    "SELECT id, created_at FROM accounts WHERE user_email=? AND profile_id=? LIMIT 1",
                    (email_s, profile_s),
                ).fetchone()
                if row:
                    acc_id = str(row[0])
                    if otp_secret_norm is not None:
                        self.conn.execute(
                            "UPDATE accounts SET name=?, cookies_json=?, updated_at=?, otp_secret=? WHERE id=?",
                            (name_s, cookies_json, now, otp_secret_norm, acc_id),
                        )
                    else:
                        self.conn.execute(
                            "UPDATE accounts SET name=?, cookies_json=?, updated_at=? WHERE id=?",
                            (name_s, cookies_json, now, acc_id),
                        )
                    return acc_id

                acc_id = _now_id("acc")
                self.conn.execute(
                    "INSERT INTO accounts (id, user_email, profile_id, name, cookies_json, created_at, updated_at, otp_secret) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (acc_id, email_s, profile_s, name_s, cookies_json, now, now, otp_secret_norm),
                )
                return acc_id

    def list_accounts(self, email: str, profile_id: Optional[str] = None) -> List[Dict[str, Any]]:
        def _table_exists(name: str) -> bool:
            q = self.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
            return bool(q.fetchone())

        if not _table_exists("accounts"):
            return []

        base_columns = (
            "SELECT id,name,profile_id,created_at,google_email,google_name,info_updated_at,otp_secret "
            "FROM accounts "
        )
        if profile_id:
            q = self.conn.execute(
                base_columns + "WHERE user_email=? AND profile_id=? ORDER BY created_at DESC",
                (email, profile_id),
            )
        else:
            q = self.conn.execute(
                base_columns + "WHERE user_email=? ORDER BY created_at DESC",
                (email,),
            )
        return [
            {
                "id": r[0],
                "name": r[1],
                "profile_id": r[2],
                "created_at": float(r[3]),
                "google_email": r[4],
                "google_name": r[5],
                "info_updated_at": float(r[6]) if r[6] is not None else None,
                "otp_secret": self._normalize_otp_secret(r[7]) if len(r) > 7 else None,
            }
            for r in q.fetchall()
        ]

    def update_account_identity(self, account_id: str, email: Optional[str], name: Optional[str]) -> None:
        with self._lock:
            with self.conn:
                self.conn.execute(
                    "UPDATE accounts SET google_email=?, google_name=?, info_updated_at=? WHERE id=?",
                    (email, name, _utc_ts(), account_id),
                )
        print(f"[accounts] db identity saved id={account_id} email={email} name={name}", flush=True)

    def update_account_otp_secret(self, account_id: str, otp_secret: Optional[str]) -> Optional[str]:
        if not account_id:
            return None
        normalized = self._normalize_otp_secret(otp_secret)
        if not normalized:
            return None
        with self._lock:
            with self.conn:
                self.conn.execute(
                    "UPDATE accounts SET otp_secret=?, updated_at=? WHERE id=?",
                    (normalized, _utc_ts(), account_id),
                )
        return normalized

    def get_user_group_ids(self, email: str) -> List[str]:
        email_s = str(email or "").strip().lower()
        if not email_s:
            return []
        with self._lock:
            q = self.conn.execute(
                "SELECT group_ids FROM user_adspower_groups WHERE user_email=? LIMIT 1",
                (email_s,),
            )
            row = q.fetchone()
        if not row or row[0] in (None, ""):
            return []
        try:
            data = json.loads(row[0])
            if isinstance(data, list):
                groups = []
                seen = set()
                for item in data:
                    val = str(item or "").strip()
                    if not val or val in seen:
                        continue
                    seen.add(val)
                    groups.append(val)
                return groups
        except Exception:
            pass
        return []

    def set_user_group_ids(self, email: str, group_ids: Iterable[str]) -> List[str]:
        email_s = str(email or "").strip().lower()
        if not email_s:
            return []
        clean: List[str] = []
        seen = set()
        for gid in group_ids:
            val = str(gid or "").strip()
            if not val or val in seen:
                continue
            seen.add(val)
            clean.append(val)
        payload = json.dumps(clean, ensure_ascii=False)
        now = _utc_ts()
        with self._lock:
            with self.conn:
                self.conn.execute(
                    "INSERT INTO user_adspower_groups (user_email, group_ids, updated_at) VALUES (?, ?, ?) "
                    "ON CONFLICT(user_email) DO UPDATE SET group_ids=excluded.group_ids, updated_at=excluded.updated_at",
                    (email_s, payload, now),
                )
        return clean

    def get_account(self, acc_id: str, email: Optional[str] = None) -> Optional[Dict[str, Any]]:
        def _table_exists(name: str) -> bool:
            q = self.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
            return bool(q.fetchone())

        if not _table_exists("accounts"):
            return None

        if email:
            q = self.conn.execute("SELECT * FROM accounts WHERE id=? AND user_email=? LIMIT 1", (acc_id, email))
        else:
            q = self.conn.execute("SELECT * FROM accounts WHERE id=? LIMIT 1", (acc_id,))
        r = q.fetchone()
        if not r:
            return None
        keys = [d[0] for d in q.description]
        data = dict(zip(keys, r))
        raw = data.pop("cookies_json", "[]") or "[]"
        try:
            data["cookies"] = json.loads(raw)
        except Exception:
            data["cookies"] = []
        if data.get("otp_secret"):
            data["otp_secret"] = self._normalize_otp_secret(data["otp_secret"])
        return data

# =============================== КОНТРОЛЬ ЗАДАЧ =============================

@dataclass
class ControlState:
    paused: bool = False
    abort: bool = False
    manual_actions: "queue.Queue[Dict[str, Any]]" = field(default_factory=queue.Queue)
    preview_q: "queue.Queue[str]" = field(default_factory=lambda: queue.Queue(maxsize=12))
    preview_stop: threading.Event = field(default_factory=threading.Event)
    preview_thread: Optional[threading.Thread] = None

class TaskManager:
    def __init__(self, settings: Settings, db: CampaignDB, paths: _Paths):
        self.settings = settings
        self.db = db
        self.paths = paths

        from concurrent.futures import ThreadPoolExecutor
        workers = int(getattr(getattr(settings, "limits", object()), "concurrent_tasks", 3) or 3)
        self.pool = ThreadPoolExecutor(max_workers=max(1, workers), thread_name_prefix="camp")
        self.controls: Dict[str, ControlState] = {}
        self._lock = threading.Lock()
        self._plocks: Dict[str, threading.Lock] = {}
        self._plocks_guard = threading.Lock()

    def _profile_lock(self, profile_id: str) -> threading.Lock:
        with self._plocks_guard:
            lk = self._plocks.get(profile_id)
            if lk is None:
                lk = threading.Lock()
                self._plocks[profile_id] = lk
            return lk

    def control(self, task_id: str) -> ControlState:
        with self._lock:
            st = self.controls.get(task_id)
            if not st:
                st = ControlState()
                self.controls[task_id] = st
            return st

    def submit(self, task_id: str, user_email: str) -> None:
        with self._lock:
            self.controls[task_id] = ControlState()
        self.pool.submit(self._worker, task_id, user_email)

    # ------------------------ worker ------------------------------------

    def _worker(self, task_id: str, user_email: str) -> None:
        meta = self.db.get(task_id)
        if not meta:
            return

        spec = CampaignSpec(**meta["spec"])
        run_id: str = meta["run_id"]

        if _clear_confirm_totp_secret:
            try:
                _clear_confirm_totp_secret()
            except Exception:
                pass
        elif _set_confirm_totp_secret:
            try:
                _set_confirm_totp_secret(None)
            except Exception:
                pass

        trace = _make_trace_safe(self.paths.traces, run_id)
        arts = _make_artifacts_for_run(run_id, self.paths)
        llm_log = LLMJsonSink(self.paths.artifacts / run_id / "llm")

        def emit(ev: str, data: Dict[str, Any]) -> None:
            try:
                self.db.append_event(task_id, ev, data)
            except Exception as e:
                log.warning("events.append error: %s", e)

        def stage(name: str, status: str, **kw: Any) -> None:
            payload = {"stage": name, "status": status, **kw}
            try:
                trace.write({"event": "stage", "task": task_id, **payload})
            except Exception:
                pass
            emit("stage", payload)

        def info(msg: str, **kw: Any) -> None:
            payload = {"msg": msg, **kw}
            try:
                trace.write({"event": "info", "task": task_id, **payload})
            except Exception:
                pass
            emit("info", payload)

        def artifact(kind: str, path: Path) -> None:
            rel_url = f"/campaigns/artifact/{path.relative_to(self.paths.artifacts).as_posix()}"
            try:
                trace.write({"event": "artifact", "task": task_id, "kind": kind, "path": str(path), "url": rel_url})
            except Exception:
                pass
            emit("artifact", {"kind": kind, "url": rel_url})

        ctrl = self.control(task_id)

        def cooperate(label: str) -> None:
            emit("heartbeat", {"label": label})
            while True:
                if ctrl.abort:
                    raise RuntimeError("Остановлено пользователем")
                if ctrl.paused:
                    stage("paused", "waiting", label=label)
                    time.sleep(0.25)
                    continue
                break

        # профайловый лок
        plock = self._profile_lock(spec.profile_id)
        acquired = plock.acquire(timeout=180.0)
        if not acquired:
            self.db.update_status(task_id, "error", "Профиль занят")
            stage("profile:lock", "fail")
            return

        self.db.update_status(task_id, "running")
        stage("profile:lock", "acquired", profile_id=spec.profile_id)

        driver = None
        try:
            # 0) опц. LLM (ad texts)
            vstore = VarStore(self.paths.vars_file)
            g = _make_gemini_safe(self.settings)
            ai_texts = {"headlines": [], "descriptions": [], "keywords": []}
            if g is not None:
                info("Gemini: генерация ад-текстов…")
                prompt = (
                    f"Сформируй JSON {{headlines:<=8 x <=30сим, descriptions:<=4 x <=90сим, keywords:<=20}} "
                    f"для Google Ads. Язык={spec.language}; Валюта={spec.currency_sign}; "
                    f"Оффер={spec.description}; URL={spec.landing_url}; Цель={spec.goal}; Гео={spec.geo}."
                )
                llm_log.write("ad_texts", {"prompt": prompt})
                out = _gemini_plan_full(g, prompt)
                llm_log.write("ad_texts", {"response": out})
                ai_texts = _extract_ad_texts_from_any(out)
                emit("ai_texts", ai_texts)
            vstore.set("ad_headlines", ai_texts["headlines"])
            vstore.set("ad_descriptions", ai_texts["descriptions"])
            vstore.set("ad_keywords", ai_texts["keywords"])
            vstore.set("landing_url", spec.landing_url)
            vstore.set("budget_daily", spec.budget_daily)
            vstore.set("goal", spec.goal)
            vstore.set("geo", spec.geo)
            vstore.set("language", spec.language)

            # 1) AdsPower + cookies
            stage("adspower:start", "start", profile=spec.profile_id)
            driver = _start_adspower_driver(spec.profile_id)
            stage("adspower:start", "ok")

            if spec.account_id:
                acc = self.db.get_account(spec.account_id, email=user_email)
                if not acc or str(acc.get("profile_id")) != spec.profile_id:
                    raise RuntimeError("Account cookies не найдены/не принадлежат профилю")
                try:
                    driver.delete_all_cookies()
                except Exception:
                    pass
                _inject_cookies(driver, acc.get("cookies") or [], emit)
                info("Cookies применены", account_id=spec.account_id)
                if _set_confirm_totp_secret:
                    try:
                        _set_confirm_totp_secret(acc.get("otp_secret"))
                    except Exception as exc:
                        log.debug("Не удалось применить otp_secret для 2FA: %s", exc)

            # 2) Открытие Ads + проверка авторизации
            _open_google_ads(driver, emit, stage)
            logged, reason = _check_ads_logged_in(driver)
            emit("auth:status", {"logged_in": bool(logged), "reason": reason or ""})
            if not logged:
                raise RuntimeError("Требуется авторизация Google (cookies недействительны/истекли)")

            # 3) Быстрый превью-канал
            try:
                _start_preview_stream(driver, ctrl, fps=int(os.getenv("ADS_AI_PREVIEW_FPS", "20")))
            except Exception:
                pass
            _emit_live_preview(driver, emit)

            # 4) Попытка перейти к созданию кампании
            try:
                _open_new_campaign(driver, emit, stage)
            except Exception as e:
                info("ads:new_campaign: ошибка — продолжим", error=str(e))

            # 5) Главный цикл: LLM → Runner, микробатчи (по 1 шагу)
            engine = CampaignEngine(
                driver=driver,
                spec=spec,
                settings=self.settings,
                vstore=vstore,
                trace_writer=trace,
                artifacts=arts,
                emit=emit,
                stage=stage,
                artifact_cb=artifact,
                llm=g,
                llm_prompts=llm_prompts,
                llm_log=llm_log,
            )
            engine.run(cooperate)

            # 6) Финальные артефакты
            try:
                p1 = _safe_take_screenshot(driver, arts, "finish")
                artifact("screenshot", p1)
                p2 = _safe_save_html(getattr(driver, "page_source", "") or "", arts)
                artifact("html", p2)
            except Exception as e:
                info("Финальные артефакты не получены", error=str(e))

            self.db.update_status(task_id, "done")
            stage("finish", "ok")
        except Exception as e:
            log.exception("Ошибка кампании %s: %s", task_id, e)
            self.db.update_status(task_id, "error", str(e))
            try:
                stage("finish", "fail", error=str(e))
            except Exception:
                pass
        finally:
            if _clear_confirm_totp_secret:
                try:
                    _clear_confirm_totp_secret()
                except Exception:
                    pass
            elif _set_confirm_totp_secret:
                try:
                    _set_confirm_totp_secret(None)
                except Exception:
                    pass
            try:
                ctrl.preview_stop.set()
                th = ctrl.preview_thread
                if th and th.is_alive():
                    th.join(timeout=1.2)
            except Exception:
                pass
            try:
                _stop_adspower_driver(driver)
            except Exception:
                pass
            try:
                plock.release()
                stage("profile:lock", "released", profile_id=spec.profile_id)
            except Exception:
                pass

# =============================== LLM / TRACE / ARTIFACTS =====================

def _make_trace_safe(traces_root: Path, run_id: str):
    if make_trace is None:
        class _Dummy:
            def write(self, *_a, **_k): ...
        return _Dummy()
    try:
        tr, _ = make_trace(traces_root, run_id)
        return tr
    except Exception:
        class _Dummy:
            def write(self, *_a, **_k): ...
        return _Dummy()

class LLMJsonSink:
    """
    Единый JSONL-лог для LLM (prompt/response) + last.json.
    writes to: artifacts/<run_id>/llm/llm_log.jsonl and llm_last.json
    """
    def __init__(self, llm_dir: Path):
        self.dir = llm_dir
        self.dir.mkdir(parents=True, exist_ok=True)
        self.log_path = self.dir / "llm_log.jsonl"
        self.last_path = self.dir / "llm_last.json"

    def write(self, kind: str, content: Any, batch: Optional[int] = None, tag: Optional[str] = None) -> None:
        row = {
            "ts": int(time.time()),
            "kind": str(kind),
            "batch": int(batch) if batch is not None else None,
            "tag": str(tag) if tag is not None else None,
            "data": content,
        }
        safe_row = json.loads(json.dumps(row, ensure_ascii=False, default=str))
        try:
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(safe_row, ensure_ascii=False) + "\n")
            with open(self.last_path, "w", encoding="utf-8") as f:
                json.dump(safe_row, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log.warning("LLMJsonSink.write error: %s", e)

def _make_artifacts_for_run(run_id: str, paths: _Paths):
    if Artifacts is not None and hasattr(Artifacts, "for_run"):
        try:
            return Artifacts.for_run(run_id=run_id, base_screenshots=paths.shots, base_html_snaps=paths.html, per_run_subdir=True)  # type: ignore
        except Exception:
            pass
    # fallback-простейший контейнер
    class _Arts:
        def __init__(self, run: str):
            self.run_id = run
            self.shots = (paths.shots / run)
            self.html = (paths.html / run)
            self.shots.mkdir(parents=True, exist_ok=True)
            self.html.mkdir(parents=True, exist_ok=True)
    return _Arts(run_id)

def _safe_take_screenshot(driver: Any, arts: Any, label: str) -> Path:
    if Artifacts is not None:
        return take_screenshot(driver, arts, label)  # type: ignore
    # fallback
    fn = f"{int(time.time())}_{re.sub(r'[^a-zA-Z0-9_-]+','',label or 'shot')}.png"
    path = (arts.shots / fn)
    try:
        png: bytes = driver.get_screenshot_as_png()
        with open(path, "wb") as f:
            f.write(png)
    except Exception:
        # ничего — создадим заглушку
        with open(path, "wb") as f:
            f.write(b"")
    return path

def _safe_save_html(html_text: str, arts: Any) -> Path:
    if Artifacts is not None:
        return save_html_snapshot(html_text, arts)  # type: ignore
    path = arts.html / f"dom_{int(time.time())}.html"
    path.write_text(html_text or "", encoding="utf-8")
    return path

def _make_gemini_safe(settings: Settings) -> Optional[Any]:
    if GeminiClient is None:
        return None
    llm = getattr(settings, "llm", None)
    model = getattr(llm, "model", "models/gemini-2.0-flash")
    fallback_model = getattr(llm, "fallback_model", "models/gemini-2.0-flash")
    key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or getattr(settings, "gemini_api_key", None)

    attempts = [
        {"api_key": key, "model": model, "fallback_model": fallback_model},
        {"model": model, "fallback_model": fallback_model},
        (key,),
        tuple(),
    ]
    for args in attempts:
        try:
            if isinstance(args, dict):
                return GeminiClient(**{k: v for k, v in args.items() if v is not None})
            elif isinstance(args, tuple):
                return GeminiClient(*[x for x in args if x is not None])
        except Exception:
            continue
    return None

def _gemini_plan_full(g: Any, prompt: str):
    if g is None:
        return None
    try:
        sig = inspect.signature(g.plan_full)
        if len(sig.parameters) == 1:
            return g.plan_full(prompt)
        return g.plan_full(prompt, "", [], {})  # type: ignore[arg-type]
    except Exception:
        return None

# =============================== CAMPAIGN ENGINE ==============================

class CampaignEngine:
    """
    Инкрементальный мастер создания кампании: автопилоты + LLM-микробатчи (1 шаг).
    """
    def __init__(
        self,
        driver: Any,
        spec: CampaignSpec,
        settings: Settings,
        vstore: VarStore,
        trace_writer: Any,
        artifacts: Any,
        emit: Callable[[str, Dict[str, Any]], None],
        stage: Callable[[str, str], None],
        artifact_cb: Callable[[str, Path], None],
        llm: Optional[Any],
        llm_prompts: Optional[Any],
        llm_log: LLMJsonSink,
    ):
        self.driver = driver
        self.spec = spec
        self.settings = settings
        self.vstore = vstore
        self.trace = trace_writer
        self.arts = artifacts
        self.emit = emit
        self.stage = stage
        self.drop_artifact = artifact_cb
        self.llm = llm
        self.llm_prompts = llm_prompts
        self.llm_log = llm_log

        lim = getattr(getattr(settings, "limits", object()), "replans", 60) or 60
        self.max_batches: int = int(lim)
        self.max_steps_per_batch: int = int(getattr(getattr(settings, "limits", object()), "max_steps_per_batch", 1) or 1)
        self.pause_between_batches: float = float(getattr(getattr(settings, "humanize", object()), "micro_pause", 0.4) or 0.4)

        self.history_done: List[Dict[str, Any]] = []

    # ------------------------- публичный запуск ---------------------------

    def run(self, cooperate: Callable[[str], None]) -> None:
        published = False
        empty_steps_in_row = 0

        for batch_idx in range(1, self.max_batches + 1):
            cooperate("loop:pre")

            # 1) спец-экраны — один шаг за цикл
            try:
                if _detect_objective_screen(self.driver):
                    acted, details = _objective_autopilot_single_step(self.driver, self.spec)
                    self.emit("log", {"msg": f"[Objective] {details.get('msg','')}", "details": details})
                    self.stage("autopilot:objective", "step" if acted else "noop", **details)
                    self._pair_artifacts(f"auto_obj_{batch_idx:02d}")
                    if acted:
                        time.sleep(self.pause_between_batches)
                        continue
                else:
                    acted, details = _onboarding_url_single_step(self.driver, self.spec)
                    if details.get("seen"):
                        self.emit("log", {"msg": f"[Website URL] {details.get('msg','')}", "details": details})
                        self.stage("autopilot:onboarding", "step" if acted else "noop", **details)
                        self._pair_artifacts(f"auto_url_{batch_idx:02d}")
                        if acted:
                            time.sleep(self.pause_between_batches)
                            continue
            except Exception as e:
                self.emit("log", {"msg": "Автопилот: исключение", "error": str(e)})

            # 2) LLM планирование следующего микро-батча
            html_view, ui_meta = _get_llm_dom_view(self.driver, ctx={"goal": self.spec.goal})
            self.emit("ui:scan", {"batch": batch_idx, **ui_meta})
            if not (isinstance(html_view, str) and html_view.strip()):
                self.emit("info", {"msg": "LLM UI-view пуст — фоллбек на полный DOM"})
                html_view = _get_visible_dom(self.driver)

            steps: List[Dict[str, Any]] = []
            if self.llm is not None and self.llm_prompts is not None:
                try:
                    prompt = self.llm_prompts.campaign_next_steps_prompt(
                        html_view=html_view,
                        task=self._task_text(),
                        inputs=self._known_vars(),
                        done_history=self.history_done,
                        known_vars=self._known_vars(),
                        max_steps=self.max_steps_per_batch,
                    )
                except Exception as e:
                    self.stage("prompt:error", "fail", error=str(e))
                    break

                self._llm_emit_req("next_steps", batch_idx, prompt)
                out = _gemini_plan_full(self.llm, prompt)
                self._llm_emit_resp("next_steps", batch_idx, out)
                self.llm_log.write("next_steps", {"batch": batch_idx, "prompt": prompt, "response": out}, batch=batch_idx)

                steps = _gemini_steps_list(out)

            self.emit("plan", {"batch": batch_idx, "steps": steps[:20]})

            if not steps:
                empty_steps_in_row += 1
                # проверим статус
                hv = html_view if isinstance(html_view, str) and html_view.strip() else _get_visible_dom(self.driver)
                check = self._completion_status(hv, when="before", batch_idx=batch_idx)
                self.emit("status", {"batch": batch_idx, "check": check})

                status = (check.get("status") or "in_progress").lower()
                if status == "published":
                    published = True
                    self.stage("llm:status", "published", batch=batch_idx)
                    break
                if status == "ready_to_publish" and isinstance(check.get("next_steps"), list):
                    steps = list(check.get("next_steps") or [])
                    self.stage("llm:status", "next_steps_from_check", count=len(steps), batch=batch_idx)
                else:
                    acted, info_d = _heuristic_next(self.driver, self.emit, want_reason=True)
                    self.stage("heuristic:next", "clicked" if acted else "noop", **info_d)
                    time.sleep(self.pause_between_batches)
                    continue
            else:
                empty_steps_in_row = 0

            # 3) Исполнение шагов Runner'ом
            ok = _execute_steps_with_runner(
                steps=steps,
                settings=self.settings,
                artifacts=self.arts,
                vstore=self.vstore,
                trace_writer=self.trace,
                on_cooperate=cooperate,
            )
            self.history_done.extend(steps)
            self._pair_artifacts(f"batch_{batch_idx:02d}")

            # 4) Проверка статуса после действия
            html_view_after, ui_meta_after = _get_llm_dom_view(self.driver, ctx={"goal": self.spec.goal})
            self.emit("ui:scan", {"batch": batch_idx, "when": "after", **ui_meta_after})
            if not (isinstance(html_view_after, str) and html_view_after.strip()):
                html_view_after = _get_visible_dom(self.driver)

            check = self._completion_status(html_view_after, when="after", batch_idx=batch_idx)
            self.emit("status", {"batch": batch_idx, "check": check})
            status = (check.get("status") or "in_progress").lower()
            if status == "published":
                published = True
                self.stage("llm:status", "published", batch=batch_idx)
                break
            if status == "ready_to_publish" and isinstance(check.get("next_steps"), list):
                fin = list(check.get("next_steps") or [])
                if fin:
                    _execute_steps_with_runner(fin, self.settings, self.arts, self.vstore, self.trace, on_cooperate=cooperate)
                    self.history_done.extend(fin)
                    self._pair_artifacts(f"batch_{batch_idx:02d}_final")
                    hv_final, _ = _get_llm_dom_view(self.driver, ctx={"goal": self.spec.goal})
                    if not (isinstance(hv_final, str) and hv_final.strip()):
                        hv_final = _get_visible_dom(self.driver)
                    check2 = self._completion_status(hv_final, when="after", batch_idx=batch_idx)
                    if (check2.get("status") or "").lower() == "published":
                        published = True
                        self.stage("llm:status", "published", batch=batch_idx)
                        break

            time.sleep(self.pause_between_batches)

        if not published:
            self.stage("finish", "warn", reason="not_confirmed_published")

    # ------------------------- вспомогательные ---------------------------

    def _task_text(self) -> str:
        return (
            f"Мы создаём рекламную кампанию Google Ads: тип={self.spec.campaign_type}, цель={self.spec.goal}, "
            f"бюджет/день={self.spec.budget_daily}{self.spec.currency_sign}, гео={self.spec.geo}, язык={self.spec.language}, "
            f"лендинг={self.spec.landing_url}. Заполняй только то, что видно на экране мастера."
        )

    def _known_vars(self) -> Dict[str, Any]:
        host = re.sub(r"^https?://", "", self.spec.landing_url).strip().strip("/")
        host = host.split("/")[0] if host else "site"
        return {
            "campaign_type": self.spec.campaign_type,
            "goal": self.spec.goal,
            "landing_url": self.spec.landing_url,
            "budget_daily": self.spec.budget_daily,
            "geo": self.spec.geo,
            "language": self.spec.language,
            "campaign_name": f"{host} · {self.spec.goal}"[:40],
            "ad_headlines": self.vstore.get("ad_headlines") or [],
            "ad_descriptions": self.vstore.get("ad_descriptions") or [],
            "ad_keywords": self.vstore.get("ad_keywords") or [],
        }

    def _pair_artifacts(self, label: str) -> None:
        try:
            p1 = _safe_take_screenshot(self.driver, self.arts, label)
            self.drop_artifact("screenshot", p1)
        except Exception:
            pass
        try:
            p2 = _safe_save_html(getattr(self.driver, "page_source", "") or "", self.arts)
            self.drop_artifact("html", p2)
        except Exception:
            pass

    def _llm_emit_req(self, kind: str, batch: int, prompt: str) -> None:
        log.info("LLM[%s][b%02d] → prompt:\n%s", kind, batch, _clip_text(prompt, 4000))
        try:
            self.trace.write(
                {"event": "llm_request", "kind": kind, "batch": batch, "prompt_preview": _clip_text(prompt, 4000), "bytes": len(prompt)}
            )
        except Exception:
            pass
        self.emit("llm:request", {"kind": kind, "batch": batch, "prompt_preview": _clip_text(prompt, 1500)})
        self.stage(f"llm:{kind}", "request", batch=batch)

    def _llm_emit_resp(self, kind: str, batch: int, out: Any) -> None:
        log.info("LLM[%s][b%02d] ← response:\n%s", kind, batch, _preview_json(out, 4000))
        try:
            self.trace.write({"event": "llm_response", "kind": kind, "batch": batch, "raw_preview": _preview_json(out, 4000)})
        except Exception:
            pass
        self.emit("llm:response", {"kind": kind, "batch": batch, "raw_preview": _preview_json(out, 1500)})

    def _completion_status(self, html_view: str, when: str, batch_idx: int) -> Dict[str, Any]:
        if self.llm is None or self.llm_prompts is None:
            return {"status": "in_progress", "reason": "no_llm"}
        try:
            prompt = self.llm_prompts.campaign_completion_check_prompt(
                html_view=html_view,
                task=self._task_text(),
                inputs=self._known_vars(),
                done_history=self.history_done,
                known_vars=self._known_vars(),
            )
        except Exception:
            return {"status": "in_progress", "reason": "prompt_error"}
        self._llm_emit_req("completion_check", batch_idx, prompt)
        out = _gemini_plan_full(self.llm, prompt)
        self._llm_emit_resp("completion_check", batch_idx, out)
        self.llm_log.write("completion_check", {"batch": batch_idx, "when": when, "prompt": prompt, "response": out}, batch=batch_idx, tag=when)

        if isinstance(out, dict):
            return out
        if isinstance(out, str):
            try:
                return json.loads(out)
            except Exception:
                return {"status": "in_progress", "reason": "parse_error"}
        return {"status": "in_progress", "reason": "unknown"}

# =============================== LLM НОРМАЛИЗАЦИЯ ============================

def _gemini_steps_list(out: Any) -> List[Dict[str, Any]]:
    """
    Поддерживаем нормальные варианты:
      • list[step]
      • {"steps":[...]} / {"next_steps":[...]}
      • str(JSON) с одним из форматов
    """
    def _clean_list(lst: List[Any]) -> List[Dict[str, Any]]:
        cleaned: List[Dict[str, Any]] = []
        for s in lst:
            if not isinstance(s, dict):
                continue
            t = (s.get("type") or "").strip()
            if not t:
                continue
            cleaned.append(s)
        return cleaned[:20]

    if isinstance(out, list):
        return _clean_list(out)
    if isinstance(out, dict):
        if isinstance(out.get("steps"), list):
            return _clean_list(out["steps"])
        if isinstance(out.get("next_steps"), list):
            return _clean_list(out["next_steps"])
        return []
    if isinstance(out, str):
        try:
            data = json.loads(out)
        except Exception:
            return []
        if isinstance(data, list):
            return _clean_list(data)
        if isinstance(data, dict):
            if isinstance(data.get("steps"), list):
                return _clean_list(data["steps"])
            if isinstance(data.get("next_steps"), list):
                return _clean_list(data["next_steps"])
        return []
    return []

def _extract_ad_texts_from_any(out: Any) -> Dict[str, List[str]]:
    def _norm_lists(d: Dict[str, Any]) -> Dict[str, List[str]]:
        return {
            "headlines": [str(x)[:30] for x in (d.get("headlines") or [])][:8],
            "descriptions": [str(x)[:90] for x in (d.get("descriptions") or [])][:4],
            "keywords": [str(x) for x in (d.get("keywords") or [])][:20],
        }

    if isinstance(out, dict) and any(k in out for k in ("headlines", "descriptions", "keywords")):
        return _norm_lists(out)

    if isinstance(out, list):
        for s in out:
            if isinstance(s, dict) and (s.get("type") == "evaluate") and isinstance(s.get("script"), str):
                script = s["script"].strip()
                m = re.search(r"return\s*(\{.*\})\s*;?\s*$", script, flags=re.DOTALL)
                if not m:
                    continue
                body = m.group(1)
                data: Optional[Dict[str, Any]] = None
                try:
                    data = json.loads(body)
                except Exception:
                    try:
                        data = ast.literal_eval(body)
                    except Exception:
                        data = None
                if isinstance(data, dict):
                    return _norm_lists(data)
    return {"headlines": [], "descriptions": [], "keywords": []}

# =============================== DOM / UI MAP ================================

def _switch_to_default_content_safe(driver: Any) -> None:
    try:
        driver.switch_to.default_content()
    except Exception:
        pass

def _ensure_ready_state_local(driver: Any, timeout: float = 8.0) -> None:
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            st = (driver.execute_script("return document.readyState") or "").lower()
            if st in ("interactive", "complete"):
                break
        except Exception:
            break
        time.sleep(0.15)

def _get_visible_dom(driver: Any, limit: int = 140_000) -> str:
    _switch_to_default_content_safe(driver)
    _ensure_ready_state_local(driver, timeout=6.0)
    js = r"""
    (function(){
      try{
        const clone = document.documentElement.cloneNode(true);
        clone.querySelectorAll('script,style,link[rel="stylesheet"]').forEach(el=>el.remove());
        return '<!doctype html>\n'+clone.outerHTML;
      }catch(e){
        return document.documentElement ? document.documentElement.outerHTML : (document.body?document.body.outerHTML:'');
      }
    })();
    """
    html_view = ""
    try:
        html_view = driver.execute_script(js) or ""
    except Exception:
        try:
            html_view = getattr(driver, "page_source", "") or ""
        except Exception:
            html_view = ""
    if not isinstance(html_view, str):
        try:
            html_view = str(html_view)
        except Exception:
            html_view = ""
    if len(html_view) > limit:
        html_view = html_view[:limit] + "\n<!-- TRUNCATED -->"
    return html_view

def _get_llm_dom_view(driver: Any, ctx: Optional[Dict[str, Any]] = None, limit: int = 60_000) -> Tuple[str, Dict[str, Any]]:
    """
    Компактная UI‑карта: inputs/buttons/tabs/primary с устойчивыми селекторами.
    Если пусто — вызывающий код сделает фоллбек на _get_visible_dom().
    """
    _switch_to_default_content_safe(driver)
    _ensure_ready_state_local(driver, timeout=6.0)
    js = r"""
    (function(){
      function txt(s){ return (s==null?'':String(s)).replace(/\s+/g,' ').trim(); }
      function low(s){ return txt(s).toLowerCase(); }
      function visible(el){
        if(!(el instanceof Element)) return false;
        const st = getComputedStyle(el);
        if(st.display==='none' || st.visibility==='hidden' || st.opacity==='0') return false;
        const r = el.getBoundingClientRect();
        if(r.width<2 || r.height<2) return false;
        if(r.bottom < -100 || r.top > (innerHeight+2000)) return false;
        return true;
      }
      function inSearchContainer(el){
        let n=el;
        for(let i=0; i<6 && n; i++){
          try{
            if(n.classList && (n.classList.contains('universal-search-container') || n.classList.contains('search'))) return true;
          }catch(_){}
          n = (n.parentNode || (n.host||null));
        }
        return false;
      }
      function isSearchField(el){
        if(!el) return false;
        const t = low(el.getAttribute && el.getAttribute('type') || '');
        const role = low(el.getAttribute && el.getAttribute('role') || '');
        const al = low(el.getAttribute && el.getAttribute('aria-label') || '');
        const ph = low(el.getAttribute && el.getAttribute('placeholder') || '');
        const idtxt = low(el.id || '');
        const name = low(el.getAttribute && el.getAttribute('name') || '');
        const any = al+' '+ph+' '+idtxt+' '+name;
        if(t==='search') return true;
        if(role==='combobox' && (any.includes('search')||any.includes('поиск'))) return true;
        if(any.includes('search') || any.includes('поиск')) return true;
        if(inSearchContainer(el)) return true;
        return false;
      }
      function isEditable(el){
        if(!(el instanceof HTMLElement)) return false;
        const tag = el.tagName;
        if(tag==='TEXTAREA') return true;
        if(tag==='SELECT') return true;
        if(tag==='INPUT'){
          const t = low(el.getAttribute('type')||'text');
          if(['hidden','button','submit','reset','checkbox','radio','file','color','range','date','time','datetime-local','month','week'].includes(t)) return false;
          return true;
        }
        const names=['md-outlined-text-field','md-filled-text-field','md-text-field','material-input'];
        return names.some(n => el.matches && el.matches(n));
      }
      function isDisabled(el){
        if(!el) return true;
        if(el.disabled===true) return true;
        const ad = low(el.getAttribute('aria-disabled')||'');
        if(ad==='true') return true;
        const cl = low(el.className || '');
        if(cl.includes('disabled')) return true;
        return false;
      }
      function collectRoots(){
        const roots=[document.documentElement];
        let frames = 0;
        const ifrs = document.querySelectorAll('iframe');
        for(const fr of ifrs){
          try{
            const doc = fr.contentDocument || (fr.contentWindow && fr.contentWindow.document);
            if(doc && doc.documentElement){
              roots.push(doc.documentElement);
              frames++;
            }
          }catch(_){}
        }
        return {roots, frames};
      }
      function deepWalk(){
        const out=[];
        const {roots, frames} = collectRoots();
        const pushChildren = (root)=>{
          if(!root) return;
          let kids=[];
          try{ kids = root.children ? Array.from(root.children) : []; }catch(_){ kids = []; }
          for(const c of kids){ out.push(c); pushChildren(c); }
          try{
            if(root.shadowRoot){
              const sh = root.shadowRoot;
              const shKids = sh.children ? Array.from(sh.children) : [];
              for(const k of shKids){ out.push(k); pushChildren(k); }
            }
          }catch(_){}
        };
        for(const rt of roots){ pushChildren(rt); }
        return {nodes: out, frames};
      }
      function buildXPath(el){
        try{
          if(!el || el.nodeType!==1) return '';
          const parts=[];
          while(el && el.nodeType===1 && el!==document.body){
            let idx=1, sib=el;
            while((sib=sib.previousElementSibling)!=null){ if(sib.nodeName===el.nodeName) idx++; }
            parts.unshift(el.nodeName.toLowerCase()+'['+idx+']');
            el = el.parentElement;
          }
          return '//' + parts.join('/');
        }catch(_){ return ''; }
      }
      function uniqueSelector(el){
        const esc = (s)=> s==null?'':String(s).replace(/\\/g,'\\\\').replace(/"/g,'\\"');
        const q = (sel)=>{ try{ return document.querySelectorAll(sel).length; }catch(_){ return 0; } };
        if(!el) return '';
        const id = el.id;
        if(id && q('#'+id)===1) return '#'+esc(id);
        const rl = el.getAttribute('role');
        const al = el.getAttribute('aria-label');
        if(rl && al){
          const sel = `[role="${esc(rl)}"][aria-label="${esc(al)}"]`;
          if(q(sel)===1) return sel;
          const sel2 = `${el.tagName.toLowerCase()}${sel}`;
          if(q(sel2)===1) return sel2;
        }
        const dv = el.getAttribute('data-value');
        if(dv){
          const s1 = `[data-value="${esc(dv)}"]`;
          if(q(s1)===1) return s1;
          if(rl){
            const s2 = `[role="${esc(rl)}"][data-value="${esc(dv)}"]`;
            if(q(s2)===1) return s2;
          }
        }
        const name = el.getAttribute('name');
        if(name && el.tagName==='INPUT' && q(`input[name="${esc(name)}"]`)===1) return `input[name="${esc(name)}"]`;
        if(al && q(`[aria-label="${esc(al)}"]`)===1) return `[aria-label="${esc(al)}"]`;
        const cls = (el.className||'').split(/\s+/).filter(c=> c && !c.includes('_ngcontent') && !/^_?ng-/.test(c) && !/^\w+-\w+/.test(c)).slice(0,3);
        if(cls.length){
          const sel = el.tagName.toLowerCase()+'.'+cls.map(esc).join('.');
          if(q(sel)===1) return sel;
        }
        const rl2 = el.getAttribute('role');
        if(rl2 && q(`[role="${esc(rl2)}"]`)===1) return `[role="${esc(rl2)}"]`;
        let cur = el, built='';
        for(let i=0;i<3;i++){
          const p = cur.parentElement;
          if(!p) break;
          const idx = Array.prototype.indexOf.call(p.children, cur)+1;
          built = `${p.tagName.toLowerCase()}>${cur.tagName.toLowerCase()}:nth-child(${idx})`;
          if(q(built)===1) return built;
          cur = p;
        }
        return buildXPath(el) || '';
      }
      function pickLabel(el){
        if(!el) return '';
        const aria = el.getAttribute('aria-label');
        if(aria) return aria;
        const t = txt(el.innerText || el.textContent || '');
        if(t) return t;
        if(el.id){
          try{
            const lab = document.querySelector(`label[for="${el.id}"]`);
            if(lab){ const s=txt(lab.innerText||lab.textContent||''); if(s) return s; }
          }catch(_){}
        }
        return '';
      }
      const labelsPrimary = ['continue','next','продолжить','далее','save and continue','сохранить и продолжить','create campaign','создать кампанию','publish','готово','done'];
      const rolesClickable = new Set(['button','tab','option','menuitem','listitem','checkbox','radio','switch']);

      const {nodes, frames} = deepWalk();
      const ui = {inputs:[], buttons:[], tabs:[], primary:[], meta:{ignored_search:0, scanned:nodes.length, frames:frames, ts: Date.now(), url: location.href}};

      for(const el of nodes){
        if(!(el instanceof HTMLElement)) continue;
        try{
          const st = getComputedStyle(el);
          if(st.display==='none' || st.visibility==='hidden') continue;
        }catch(_){}

        // inputs
        if(visible(el) && isEditable(el) && !isDisabled(el)){
          let base = el;
          try{
            if(el.shadowRoot){
              const i = el.shadowRoot.querySelector('input,textarea,select');
              if(i) base = i;
            }else if(el.matches && el.matches('material-input')){
              const i = el.querySelector('input,textarea,select');
              if(i) base = i;
            }
          }catch(_){}
          if(!base) continue;
          if(isSearchField(base)) { ui.meta.ignored_search++; continue; }
          ui.inputs.push({
            tag: (base.tagName||'').toLowerCase(),
            role: base.getAttribute && base.getAttribute('role') || '',
            type: (base.getAttribute && (base.getAttribute('type')||'')).toLowerCase(),
            aria_label: pickLabel(el) || pickLabel(base) || (base.getAttribute && base.getAttribute('aria-label')) || '',
            placeholder: (base.getAttribute && base.getAttribute('placeholder')) || '',
            selector: uniqueSelector(base),
            xpath: buildXPath(base),
            value: (base.value || '')
          });
          continue;
        }

        // clickable
        const tag = el.tagName;
        const role = low(el.getAttribute('role')||'');
        const txtl = low(el.innerText || el.textContent || '');
        const isAnchor = (tag==='A' && el.hasAttribute('href'));
        const isButtonTag = tag==='BUTTON' || (tag==='INPUT' && ['button','submit','reset'].includes(low(el.getAttribute('type')||'')));
        const hasOnclick = el.getAttribute('onclick') != null;
        let pointer = false; try{ pointer = getComputedStyle(el).cursor==='pointer'; }catch(_){}
        const isClickable = isButtonTag || isAnchor || rolesClickable.has(role) || hasOnclick || pointer;
        if(!isClickable || !visible(el) || isSearchField(el)) continue;

        const cand = {
          tag: el.tagName.toLowerCase(),
          role: role,
          aria_label: el.getAttribute('aria-label')||'',
          text: txt(el.innerText || el.textContent || ''),
          data_value: el.getAttribute('data-value')||'',
          selector: uniqueSelector(el),
          xpath: buildXPath(el)
        };
        if(labelsPrimary.some(w=> txtl.includes(w))) ui.primary.push(cand);
        else if(role==='tab' || (el.className||'').includes('selection-item')) ui.tabs.push(cand);
        else ui.buttons.push(cand);
      }

      function dedup(arr){
        const seen = new Set(); const out=[];
        for(const it of arr){
          const key = (it.selector||'')+'|'+(it.aria_label||'')+'|'+(it.text||'');
          if(seen.has(key)) continue; seen.add(key); out.push(it);
        }
        return out;
      }
      ui.inputs = dedup(ui.inputs).slice(0, 60);
      ui.buttons = dedup(ui.buttons).slice(0, 60);
      ui.tabs = dedup(ui.tabs).slice(0, 40);
      ui.primary = dedup(ui.primary).slice(0, 16);
      ui.meta.counts = {inputs: ui.inputs.length, buttons: ui.buttons.length, tabs: ui.tabs.length, primary: ui.primary.length};
      return ui;
    })();
    """
    ui: Dict[str, Any] = {}
    try:
        raw = driver.execute_script(js)
        if isinstance(raw, dict):
            ui = raw
    except Exception:
        return "", {"error": "js_exec_failed"}

    meta = ui.get("meta", {}) if isinstance(ui.get("meta", {}), dict) else {}
    counts = meta.get("counts", {}) if isinstance(meta.get("counts", {}), dict) else {}
    scanned = int(meta.get("scanned", 0) or 0)
    frames = int(meta.get("frames", 0) or 0)

    try:
        html_view = _build_llm_ui_html(ui, ctx or {})
    except Exception:
        html_view = ""

    nothing_to_show = (scanned < 10) and all(int(counts.get(k, 0) or 0) == 0 for k in ("inputs", "buttons", "tabs", "primary"))
    if not html_view or nothing_to_show:
        return "", {"scanned": scanned, "frames": frames, "counts": counts, "why": "empty_ui_map"}

    if limit > 0 and len(html_view) > limit:
        html_view = html_view[:limit] + "\n<!-- TRUNCATED UI-VIEW -->"
    return html_view, {"scanned": scanned, "frames": frames, "counts": counts}

def _build_llm_ui_html(ui: Dict[str, Any], ctx: Dict[str, Any]) -> str:
    def esc(s: Any) -> str:
        return html.escape("" if s is None else str(s), quote=True)

    goal = esc((ctx or {}).get("goal", ""))

    def render_inputs(items: List[Dict[str, Any]]) -> str:
        out = []
        for it in items or []:
            lbl = it.get("aria_label") or it.get("placeholder") or it.get("text") or it.get("type") or ""
            lbl = esc(lbl)[:160]
            ds = esc(it.get("selector") or "")
            dx = esc(it.get("xpath") or "")
            typ = esc(it.get("type") or "")
            ph = esc(it.get("placeholder") or "")
            out.append(
                f'<div class="ui-row"><label class="ui-label">{lbl}</label>'
                f'<input class="ui-input" data-selector="{ds}" data-xpath="{dx}" aria-label="{lbl}" '
                f'type="{typ or "text"}" placeholder="{ph}" /></div>'
            )
        return "\n".join(out)

    def render_buttons(items: List[Dict[str, Any]], role_hint: str = "button") -> str:
        out = []
        for it in items or []:
            lbl = it.get("aria_label") or it.get("text") or it.get("data_value") or ""
            lbl = esc(lbl)[:160]
            ds = esc(it.get("selector") or "")
            dx = esc(it.get("xpath") or "")
            role = esc(it.get("role") or role_hint)
            out.append(
                f'<div class="ui-btn" role="{role}" aria-label="{lbl}" data-selector="{ds}" data-xpath="{dx}">{lbl}</div>'
            )
        return "\n".join(out)

    meta = ui.get("meta", {}) if isinstance(ui.get("meta", {}), dict) else {}
    m_ignored = int(meta.get("ignored_search", 0) or 0)
    m_scanned = int(meta.get("scanned", 0) or 0)
    m_frames = int(meta.get("frames", 0) or 0)

    css = """
    <style>
      .ui-wrap{font:14px/1.4 system-ui,Arial,sans-serif;color:#111}
      .ui-title{font-weight:700;margin:6px 0}
      .ui-note{font-size:12px;color:#555;margin:2px 0 10px}
      .ui-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px}
      .ui-block{border:1px solid #e5e7eb;border-radius:8px;padding:8px}
      .ui-block h3{margin:0 0 6px 0;font-size:13px;color:#222}
      .ui-row{display:grid;grid-template-columns:160px 1fr;gap:6px;align-items:center;margin:4px 0}
      .ui-label{font-size:12px;color:#374151}
      .ui-input{width:100%;padding:6px 8px;border:1px solid #e5e7eb;border-radius:6px}
      .ui-btn{display:inline-block;border:1px solid #d1d5db;border-radius:6px;padding:6px 10px;margin:3px 4px;background:#f9fafb}
      .muted{color:#6b7280}
    </style>
    """

    tabs = ui.get("tabs") or []
    prim = ui.get("primary") or []
    btns = ui.get("buttons") or []
    inps = ui.get("inputs") or []

    html_doc = [
        "<!doctype html>",
        "<html><head><meta charset='utf-8'/>",
        css,
        "</head><body>",
        "<div class='ui-wrap'>",
        f"<div class='ui-title'>LLM UI-View{(' · goal='+goal) if goal else ''}</div>",
        f"<div class='ui-note muted'>visible nodes scanned: {m_scanned}, frames: {m_frames}, ignored search widgets: {m_ignored}</div>",
        "<div class='ui-grid'>",
        "<section class='ui-block' data-block='tabs'><h3>Objective / Tabs</h3>",
        render_buttons(tabs, role_hint="tab") or "<div class='muted'>—</div>",
        "</section>",
        "<section class='ui-block' data-block='primary'><h3>Primary buttons</h3>",
        render_buttons(prim, role_hint="button") or "<div class='muted'>—</div>",
        "</section>",
        "<section class='ui-block' data-block='buttons'><h3>Buttons</h3>",
        render_buttons(btns, role_hint="button") or "<div class='muted'>—</div>",
        "</section>",
        "<section class='ui-block' data-block='inputs'><h3>Inputs</h3>",
        render_inputs(inps) or "<div class='muted'>—</div>",
        "</section>",
        "</div></div></body></html>",
    ]
    return "\n".join(html_doc)

# =============================== АВТОПИЛОТЫ ==================================

def _detect_objective_screen(driver: Any) -> bool:
    js = r"""
    (function(){
      try{
        const panel = document.querySelector('.panel--unified-goals-format');
        const hasTabs = !!document.querySelector('.panel--unified-goals-format [role="tablist"] .selection-item[role="tab"]');
        const label = (panel && (panel.innerText||'').toLowerCase()) || '';
        return !!(panel && hasTabs && (label.includes('choose your objective') || label.includes('выберите цель') || label.includes('select an objective')));
      }catch(e){ return false; }
    })();
    """
    try:
        return bool(driver.execute_script(js))
    except Exception:
        return False

def _goal_to_objective_label(goal: str) -> str:
    g = (goal or "").lower()
    if "траф" in g or "traffic" in g or "website" in g:
        return "Website traffic"
    if "лид" in g or "lead" in g:
        return "Leads"
    if "продаж" in g or "sale" in g:
        return "Sales"
    if "app" in g or "прилож" in g:
        return "App promotion"
    if "local" in g or "store" in g or "визит" in g:
        return "Local store visits and promotions"
    if "awareness" in g or "охват" in g or "consideration" in g:
        return "Awareness and consideration"
    return "Create a campaign without guidance"

def _objective_autopilot_single_step(driver: Any, spec: CampaignSpec) -> Tuple[bool, Dict[str, Any]]:
    target = _goal_to_objective_label(spec.goal)
    js = r"""
    (function(target){
      function txt(s){return (s||'').toString().trim().toLowerCase();}
      function isVisible(el){try{const r=el.getBoundingClientRect(); return r.width>1&&r.height>1;}catch(e){return false}}
      function deepFind(pred){
        const seen = new Set(); const stack=[document];
        while(stack.length){
          const root = stack.pop();
          let els=[]; try{els=root.querySelectorAll('*')}catch(e){}
          for(const el of els){
            if(seen.size>40000) break;
            if(seen.has(el)) continue; seen.add(el);
            try{ if(pred(el)) return el; }catch(e){}
            if(el.shadowRoot) stack.push(el.shadowRoot);
          }
        }
        return null;
      }
      var selected = deepFind(function(el){
        if(!(el instanceof HTMLElement)) return false;
        if(el.getAttribute('role')!=='tab') return false;
        if(txt(el.getAttribute('aria-selected'))==='true') return true;
        return false;
      });
      if(!selected){
        var card = deepFind(function(el){
          if(!(el instanceof HTMLElement)) return false;
          if(el.getAttribute('role')!=='tab') return false;
          const label = (el.getAttribute('aria-label')||'') + ' ' + (el.innerText||'');
          return txt(label).includes(txt(target));
        });
        if(card){
          try{ card.scrollIntoView({block:'center'}); }catch(_){}
          let ok=false, err='';
          try{ card.click(); ok=true; }catch(e){ err='click_error:'+String(e); }
          if(!ok){ try{ card.dispatchEvent(new MouseEvent('click',{bubbles:true,composed:true})); ok=true; }catch(e){ err=err||('dispatch_error:'+String(e)); } }
          return {acted: ok, step: 'click_card', msg: ok?'clicked card':'cannot click card', target: target, error: err||undefined};
        }else{
          return {acted:false, step:'card_not_found', msg:'Не найден видимый таб с aria-label', target: target};
        }
      }
      var btn = deepFind(function(el){
        if(!(el instanceof HTMLElement)) return false;
        const role=(el.getAttribute('role')||'').toLowerCase();
        const isBtn = el.tagName==='BUTTON' || role==='button' || (el.tagName==='A' && el.getAttribute('href'));
        if(!isBtn) return false;
        const t = txt((el.getAttribute('aria-label')||'')+' '+(el.innerText||el.textContent||''));
        return ['continue','next','продолжить','далее','save and continue','сохранить и продолжить'].some(w=>t.includes(w));
      });
      if(!btn){
        return {acted:false, step:'continue_not_found', msg:'Кнопка Continue/Next не найдена'};
      }
      var disabled = (btn.disabled===true) || (txt(btn.getAttribute('aria-disabled'))==='true') || btn.className.toLowerCase().includes('disabled');
      if(disabled){
        return {acted:false, step:'continue_disabled', msg:'Кнопка Continue неактивна', disabled:true};
      }
      try{ btn.scrollIntoView({block:'center'}); }catch(_){}
      let ok=false, err='';
      try{ btn.click(); ok=true; }catch(e){ err='click_error:'+String(e); }
      if(!ok){ try{ btn.dispatchEvent(new MouseEvent('click',{bubbles:true,composed:true})); ok=true; }catch(e){ err=err||('dispatch_error:'+String(e)); } }
      return {acted: ok, step: 'click_continue', msg: ok?'clicked Continue':'cannot click Continue', error: err||undefined};
    })(arguments[0]);
    """
    try:
        res = driver.execute_script(js, target) or {}
    except Exception as e:
        return False, {"msg": "JS error objective", "error": str(e), "target": target}
    acted = bool(res.get("acted"))
    if not acted and res.get("step") == "continue_disabled":
        res["hint"] = "Выберите карточку цели — Continue станет активной."
    return acted, res

def _onboarding_url_single_step(driver: Any, spec: CampaignSpec) -> Tuple[bool, Dict[str, Any]]:
    js = r"""
    (function(url){
      function txt(s){return (s||'').toString().trim().toLowerCase();}
      function deepFind(pred){
        const stack=[document], seen=new Set(); let limit=50000;
        while(stack.length){
          const root=stack.pop(); if(!root) break;
          let els=[]; try{els=root.querySelectorAll('*')}catch(_){}
          for(const el of els){
            if(seen.size>limit) break;
            if(seen.has(el)) continue; seen.add(el);
            try{ if(pred(el)) return el; }catch(_){}
            if(el.shadowRoot) stack.push(el.shadowRoot);
          }
        }
        return null;
      }
      var screen = deepFind(function(el){
        if(!(el instanceof HTMLElement)) return false;
        const t=txt((el.getAttribute('aria-label')||'')+' '+(el.innerText||el.textContent||''));
        if(el.tagName==='INPUT' && ['url','text','search'].includes((el.getAttribute('type')||'').toLowerCase())) return true;
        return t.includes('website')||t.includes('final url')||t.includes('веб-сайт')||t.includes('конечный url');
      });
      if(!screen) return {seen:false, acted:false, msg:'url_screen_not_detected'};

      var input = deepFind(function(el){
        if(!(el instanceof HTMLElement)) return false;
        if(el.tagName==='INPUT'){
          const t=(el.getAttribute('type')||'').toLowerCase();
          const ar=txt(el.getAttribute('aria-label'));
          const ph=txt(el.getAttribute('placeholder'));
          return ['url','text','search'].includes(t) || ar.includes('url') || ph.includes('url') || ar.includes('website') || ph.includes('website');
        }
        if(el.matches && el.matches('md-outlined-text-field,md-filled-text-field,md-text-field')){
          const sh=el.shadowRoot; if(sh){ const i=sh.querySelector('input'); if(i) return true; }
        }
        return false;
      });
      if(input && input.matches && !input.tagName){
        try{ input = input.shadowRoot.querySelector('input'); }catch(_){}
      }
      let cur = '';
      try{ cur = (input && input.value) ? String(input.value) : ''; }catch(_){ cur=''; }
      if(input && (!cur || cur.length<4)){
        try{
          input.focus(); input.value=url;
          input.dispatchEvent(new Event('input',{bubbles:true,composed:true}));
          input.dispatchEvent(new Event('change',{bubbles:true,composed:true}));
        }catch(e){ return {seen:true, acted:false, step:'type_url_fail', msg:'Не удалось ввести URL', error:String(e)}; }
        return {seen:true, acted:true, step:'type_url', msg:'Введён URL'};
      }
      var btn = deepFind(function(el){
        if(!(el instanceof HTMLElement)) return false;
        const role=(el.getAttribute('role')||'').toLowerCase();
        const isBtn = el.tagName==='BUTTON' || role==='button' || (el.tagName==='A' && el.getAttribute('href'));
        if(!isBtn) return false;
        const t = txt((el.getAttribute('aria-label')||'')+' '+(el.innerText||el.textContent||''));
        return ['continue','next','продолжить','далее','save and continue','сохранить и продолжить'].some(w=>t.includes(w));
      });
      if(!btn) return {seen:true, acted:false, step:'continue_not_found', msg:'Кнопка Continue не найдена'};
      var disabled = (btn.disabled===true) || (txt(btn.getAttribute('aria-disabled'))==='true') || btn.className.toLowerCase().includes('disabled');
      if(disabled) return {seen:true, acted:false, step:'continue_disabled', msg:'Кнопка Continue неактивна'};
      try{ btn.scrollIntoView({block:'center'}); }catch(_){}
      try{ btn.click(); }catch(e){ try{ btn.dispatchEvent(new MouseEvent('click',{bubbles:true,composed:true})) }catch(_){ return {seen:true, acted:false, step:'click_continue_fail', msg:'Клик по Continue не удался', error:String(e)}; } }
      return {seen:true, acted:true, step:'click_continue', msg:'Нажата Continue'};
    })(arguments[0]);
    """
    try:
        res = driver.execute_script(js, spec.landing_url or "https://example.com/")
    except Exception as e:
        return False, {"seen": False, "acted": False, "msg": "url_js_error", "error": str(e)}
    acted = bool(res.get("acted"))
    return acted, dict(res or {})

def _heuristic_next(driver: Any, emit: Callable[[str, Dict[str, Any]], None], want_reason: bool = False) -> Tuple[bool, Dict[str, Any]]:
    js = r"""
    (function(){
      function txt(s){return (s||'').toString().trim().toLowerCase();}
      function deepFind(pred){
        const stack=[document], seen=new Set(); let limit=50000;
        while(stack.length){
          const root=stack.pop(); if(!root) break;
          let els=[]; try{els=root.querySelectorAll('*')}catch(_){}
          for(const el of els){
            if(seen.size>limit) break;
            if(seen.has(el)) continue; seen.add(el);
            try{ if(pred(el)) return el; }catch(_){}
            if(el.shadowRoot) stack.push(el.shadowRoot);
          }
        }
        return null;
      }
      const labels = ['далее','продолжить','next','continue','save and continue','сохранить и продолжить','создать кампанию','publish','create campaign','готово','done'];
      const btn = deepFind(function(b){
        if(!(b instanceof HTMLElement)) return false;
        const role=(b.getAttribute('role')||'').toLowerCase();
        const isBtn = b.tagName==='BUTTON' || role==='button' || (b.tagName==='A' && b.getAttribute('href'));
        if(!isBtn) return false;
        const t=txt((b.getAttribute('aria-label')||'')+' '+(b.innerText||b.textContent||''));
        return labels.some(w=>t.includes(w));
      });
      if(!btn) return {acted:false, reason:'no_button'};
      const disabled = (btn.disabled===true) || (txt(btn.getAttribute('aria-disabled'))==='true') || btn.className.toLowerCase().includes('disabled');
      if(disabled){
        return {acted:false, reason:'button_disabled', ariaDisabled: txt(btn.getAttribute('aria-disabled')), disabledProp: btn.disabled===true};
      }
      try{ btn.scrollIntoView({block:'center'}); }catch(_){}
      let ok=false, err='';
      try{ btn.click(); ok=true; }catch(e){ err='click_error:'+String(e); }
      if(!ok){ try{ btn.dispatchEvent(new MouseEvent('click',{bubbles:true,composed:true})); ok=true; }catch(e){ err=err||('dispatch_error:'+String(e)); } }
      return {acted:ok, reason: ok?'clicked':'click_failed', error: err||undefined};
    })();
    """
    info: Dict[str, Any] = {}
    try:
        res = driver.execute_script(js) or {}
        if want_reason:
            info = dict(res or {})
        acted = bool(res.get("acted"))
    except Exception as e:
        acted = False
        info = {"reason": "js_error", "error": str(e)}
    if not want_reason:
        emit("log", {"msg": f"Эвристика Next: {'клик' if acted else 'нет/неактивна'}", "details": info})
    return acted, info

# =============================== AdsPower / OPEN ==============================

def _get_adspower_env() -> Tuple[bool, str, str]:
    def _normalize_base(u: str) -> str:
        u = (u or "").strip()
        if not u:
            return "http://local.adspower.net:50325"
        if not re.match(r"^https?://", u, re.I):
            u = "http://" + u
        return u.rstrip("/")

    # Насильно включаем headless, чтобы профили AdsPower не открывались визуально.
    os.environ["ADS_AI_HEADLESS"] = "1"
    os.environ.setdefault("ADSP_FORCE_V2", "1")
    headless = True
    api_base = _normalize_base(os.getenv("ADSP_API_BASE") or os.getenv("ADSP_BASE") or "http://local.adspower.net:50325")
    token = (os.getenv("ADSP_API_TOKEN") or os.getenv("ADSP_TOKEN") or "").strip()
    return headless, api_base, token

def _start_adspower_driver(profile_id: str):
    if not adspower_mod:
        raise RuntimeError("ads_ai.browser.adspower недоступен")

    start_fn = getattr(adspower_mod, "start_adspower", None) or getattr(adspower_mod, "start", None)
    if not start_fn:
        raise RuntimeError("В adspower нет start_adspower/start")

    params = {}
    try:
        params = inspect.signature(start_fn).parameters  # type: ignore
    except Exception:
        pass

    pnames = set(params.keys()) if params else set()
    headless, api_base, token = _get_adspower_env()

    kw: Dict[str, Any] = {}
    if "profile" in pnames:
        kw["profile"] = profile_id
    elif "profile_id" in pnames:
        kw["profile_id"] = profile_id
    elif "id" in pnames:
        kw["id"] = profile_id
    if "headless" in pnames:
        kw["headless"] = headless
    if "api_base" in pnames:
        kw["api_base"] = api_base
    elif "base" in pnames:
        kw["base"] = api_base
    if "token" in pnames:
        kw["token"] = token
    elif "api_token" in pnames:
        kw["api_token"] = token

    last_err: Optional[Exception] = None
    ap = None
    try:
        ap = start_fn(**kw) if kw else None  # type: ignore
    except Exception as e:
        last_err = e
        ap = None

    if ap is None and params:
        ordered: List[Any] = []
        for name in params.keys():
            if name == "self":
                continue
            if name in ("profile", "profile_id", "id"):
                ordered.append(profile_id)
            elif name == "headless":
                ordered.append(headless)
            elif name in ("api_base", "base"):
                ordered.append(api_base)
            elif name in ("token", "api_token"):
                ordered.append(token)
        try:
            ap = start_fn(*ordered)  # type: ignore
        except Exception as e:
            last_err = e
            ap = None

    if ap is None:
        raise TypeError(f"Не удалось запустить AdsPower: {last_err}")

    drv = None
    try:
        if hasattr(ap, "driver"):
            drv = getattr(ap, "driver")
        elif isinstance(ap, dict):
            drv = ap.get("driver") or ap.get("webdriver") or ap.get("wd")
            if drv is None and isinstance(ap.get("result"), dict):
                drv = ap["result"].get("driver")
        elif hasattr(ap, "get"):
            drv = ap
        if drv is None and hasattr(ap, "get_driver") and callable(getattr(ap, "get_driver")):
            drv = ap.get_driver()
    except Exception:
        drv = None

    if drv is None:
        raise RuntimeError("AdsPower не вернул WebDriver")

    try:
        drv.get("about:blank")
    except Exception:
        pass
    return drv

def _stop_adspower_driver(driver: Any) -> None:
    if driver is None:
        return
    try:
        if hasattr(driver, "quit") and callable(getattr(driver, "quit")):
            driver.quit()
    except Exception:
        pass

def _open_google_ads(driver, emit: Callable[[str, Dict[str, Any]], None], stage: Callable[[str, str], None]) -> None:
    try:
        from ads_ai.browser.waits import ensure_ready_state, wait_url, wait_dom_stable  # type: ignore
    except Exception:
        ensure_ready_state = lambda *_a, **_k: None  # type: ignore
        wait_url = lambda *_a, **_k: True  # type: ignore
        wait_dom_stable = lambda *_a, **_k: True  # type: ignore

    stage("ads:open", "start")
    try:
        driver.get("https://ads.google.com/")
        ensure_ready_state(driver, timeout=15.0)
        _ensure_ready_state_local(driver, timeout=8.0)
        wait_dom_stable(driver, idle_ms=700, timeout_sec=12)
        driver.get("https://ads.google.com/aw/overview")
        ensure_ready_state(driver, timeout=15.0)
        _ensure_ready_state_local(driver, timeout=8.0)
        wait_url(driver, pattern="ads.google", timeout_sec=10, regex=False)
        wait_dom_stable(driver, idle_ms=700, timeout_sec=12)
        stage("ads:open", "ok")
        _emit_live_preview(driver, emit)
    except Exception as e:
        stage("ads:open", "warn", error=str(e))
        emit("log", {"msg": f"⚠️ Не удалось открыть Ads: {e}"})

def _open_new_campaign(driver, emit: Callable[[str, Dict[str, Any]], None], stage: Callable[[str, str], None]) -> None:
    try:
        from ads_ai.browser.waits import ensure_ready_state, wait_dom_stable  # type: ignore
    except Exception:
        ensure_ready_state = lambda *_a, **_k: None  # type: ignore
        wait_dom_stable = lambda *_a, **_k: True  # type: ignore

    cur = getattr(driver, "current_url", "") or ""
    if ("/aw/signup/" in cur) or ("aboutyourbusiness" in cur):
        stage("ads:new_campaign", "skip", reason="onboarding")
        return

    stage("ads:new_campaign", "start")
    try:
        driver.get("https://ads.google.com/aw/campaigns/new")
        ensure_ready_state(driver, timeout=12.0)
        _ensure_ready_state_local(driver, timeout=6.0)
        wait_dom_stable(driver, idle_ms=600, timeout_sec=10)
        _emit_live_preview(driver, emit)
    except Exception:
        pass

    stage("ads:new_campaign", "ok", url=getattr(driver, "current_url", ""))

# =============================== AUTH / COOKIES ==============================

def _check_ads_logged_in(driver) -> Tuple[bool, str]:
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
        core = {"SID", "HSID", "SSID", "APISID", "SAPISID", "OSID", "__Secure-1PSID", "__Secure-3PSID", "__Secure-OSID"}
        if any(n in have for n in core):
            return True, ""
    except Exception:
        pass
    return False, "unknown_state"

def _inject_cookies(driver, cookies: List[Dict[str, Any]], emit: Callable[[str, Dict[str, Any]], None]) -> None:
    if not cookies:
        return

    def _norm_samesite(v: Any) -> Optional[str]:
        s = (str(v or "").strip().lower())
        if not s:
            return None
        if s in ("none", "no_restriction", "no-restriction", "unspecified"):
            return "None"
        if s in ("lax",):
            return "Lax"
        if s in ("strict",):
            return "Strict"
        return None

    def _pick_expiry(ck: Dict[str, Any]) -> Optional[int]:
        for key in ("expiry", "expirationDate", "expires"):
            if key in ck and ck[key]:
                try:
                    iv = int(float(ck[key]))
                    if iv > 0:
                        return iv
                except Exception:
                    continue
        return None

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    now = int(_utc_ts())
    for raw in cookies:
        name = raw.get("name")
        value = raw.get("value")
        if not (name and (value is not None)):
            continue
        domain_raw = raw.get("domain")
        path = raw.get("path") or "/"
        is_host_prefix = str(name).startswith("__Host-")
        is_secure_prefix = str(name).startswith("__Secure-")
        secure = bool(raw.get("secure", True) or is_host_prefix or is_secure_prefix)
        http_only = bool(raw.get("httpOnly", raw.get("httponly", False)))
        samesite = _norm_samesite(raw.get("sameSite") or raw.get("SameSite"))
        if samesite == "None":
            secure = True
        expiry = _pick_expiry(raw)
        if isinstance(expiry, int) and expiry > 0 and expiry < (now - 60):
            continue

        if is_host_prefix:
            host_to_visit = "accounts.google.com"
            domain_for_cookie: Optional[str] = None
            path = "/"
        else:
            domain_for_cookie = str(domain_raw) if domain_raw else None
            host_to_visit = domain_for_cookie.lstrip(".") if domain_for_cookie else "google.com"

        ck: Dict[str, Any] = {
            "name": str(name),
            "value": str(value),
            "path": path,
            "secure": bool(secure),
            "httpOnly": bool(http_only),
        }
        if samesite:
            ck["sameSite"] = samesite
        if expiry:
            ck["expiry"] = int(expiry)
        if domain_for_cookie and not is_host_prefix:
            ck["domain"] = domain_for_cookie

        grouped.setdefault(host_to_visit, []).append(ck)

    for host, items in grouped.items():
        ok_cnt, fail_cnt = 0, 0
        emit("log", {"msg": f"Инъекция cookies для {host} ({len(items)})"})
        try:
            driver.get(f"https://{host}/")
            time.sleep(0.25)
        except Exception:
            pass
        for c in items:
            added = False
            try:
                driver.add_cookie(dict(c))
                added = True
            except WebDriverException:
                try:
                    c2 = dict(c)
                    c2.pop("domain", None)
                    driver.add_cookie(c2)
                    added = True
                except WebDriverException:
                    added = False
            except Exception:
                added = False
            ok_cnt += 1 if added else 0
            fail_cnt += 0 if added else 1
        try:
            driver.refresh()
        except Exception:
            pass
        emit("log", {"msg": f"✔ Установлено {ok_cnt}/{len(items)} для {host}" + (f", ошибки: {fail_cnt}" if fail_cnt else "")})
    time.sleep(0.2)

# =============================== ПРЕВЬЮ / SSE ================================

def _emit_live_preview(driver, emit: Callable[[str, Dict[str, Any]], None]) -> None:
    try:
        png = driver.get_screenshot_as_png() or b""
        if png:
            emit("vision:image", {"data": base64.b64encode(png).decode("ascii")})
    except Exception:
        pass

def _start_preview_stream(driver, ctrl: ControlState, fps: int = 20) -> None:
    if ctrl.preview_thread and ctrl.preview_thread.is_alive():
        return
    try:
        fps = max(5, min(30, int(fps)))
    except Exception:
        fps = 20
    interval = 1.0 / float(fps)
    ctrl.preview_stop.clear()

    def _loop():
        last = ""
        while not ctrl.preview_stop.is_set():
            t0 = time.time()
            try:
                png = driver.get_screenshot_as_png()
            except Exception:
                break
            b64 = ""
            try:
                b64 = base64.b64encode(png).decode("ascii") if png else ""
            except Exception:
                b64 = ""
            if b64 and b64 != last:
                try:
                    if ctrl.preview_q.full():
                        try:
                            ctrl.preview_q.get_nowait()
                        except Exception:
                            pass
                    ctrl.preview_q.put_nowait(b64)
                    last = b64
                except Exception:
                    pass
            dt = time.time() - t0
            time.sleep(max(0.0, interval - dt))

    th = threading.Thread(target=_loop, name=f"preview-{id(driver)}", daemon=True)
    ctrl.preview_thread = th
    th.start()

# =============================== RUNNER ======================================

def _execute_steps_with_runner(
    steps: List[Dict[str, Any]],
    settings: Settings,
    artifacts: Any,
    vstore: VarStore,
    trace_writer: Any,
    on_cooperate: Callable[[str], None],
) -> bool:
    if not steps:
        return True
    if Runner is None:
        return True
    try:
        r = Runner(settings=settings, trace=trace_writer, artifacts=artifacts, var_store=vstore)  # type: ignore
        return bool(r.run(steps=steps, on_cooperate=on_cooperate))  # type: ignore
    except TypeError:
        r = Runner(settings)  # type: ignore
        return bool(r.run(steps=steps))  # type: ignore
    except Exception:
        return False

# =============================== РУЧНЫЕ ДЕЙСТВИЯ =============================

def _sel(sel: str) -> Tuple[Any, str]:
    s = (sel or "").strip()
    if s.startswith("//") or s.startswith("xpath="):
        return By.XPATH, (s[6:] if s.startswith("xpath=") else s)
    if s.startswith("css="):
        return By.CSS_SELECTOR, s[4:]
    return By.CSS_SELECTOR, s

def _execute_manual(driver, action: Dict[str, Any]) -> None:
    kind = (action.get("kind") or "click").lower()
    if kind == "click":
        by, sel = _sel(action.get("selector", ""))
        driver.find_element(by, sel).click()
    elif kind == "type":
        by, sel = _sel(action.get("selector", ""))
        el = driver.find_element(by, sel)
        try:
            el.clear()
        except Exception:
            pass
        el.send_keys(action.get("text", ""))
    else:
        raise ValueError(f"Неизвестное действие: {kind}")

# =============================== UI / HTML ===================================

_BASE_CSS = """
:root{--bg:#0b1220;--text:#e5e7eb;--muted:#94a3b8;--line:rgba(255,255,255,.12);--surface:#0f1829;--accent:#38bdf8;--ok:#16a34a;--err:#ef4444}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 Inter,system-ui,Arial}
.wrap{max-width:1120px;margin:0 auto;padding:20px}
.card{background:var(--surface);border:1px solid var(--line);border-radius:14px;box-shadow:0 10px 28px rgba(0,0,0,.35);overflow:hidden;margin-bottom:14px}
.card .head{padding:12px 14px;border-bottom:1px solid var(--line);display:flex;align-items:center;justify-content:space-between}
.title{font-weight:800;letter-spacing:.2px}
.body{padding:14px}
.form{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.row{display:grid;gap:6px}.row.full{grid-column:1/-1}
.label{font-size:12px;color:var(--muted)}
.inp,.sel,.ta{width:100%;padding:10px 12px;border:1px solid var(--line);border-radius:12px;background:#0d1526;color:#e5e7eb;outline:none}
.ta{min-height:84px;resize:vertical}
.actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:8px}
.btn{border:1px solid var(--line);border-radius:10px;padding:9px 12px;background:#0d1526;color:#e5e7eb;text-decoration:none;cursor:pointer;font-weight:700}
.btn.primary{background:linear-gradient(180deg,var(--accent),#60a5fa);color:#000;border:0}
.badge{padding:4px 8px;border-radius:999px;border:1px solid var(--line);font-size:12px}
.badge.ok{border-color:var(--ok);color:var(--ok)}.badge.err{border-color:var(--err);color:var(--err)}
.log{background:#0d1526;border:1px solid var(--line);border-radius:10px;padding:10px;max-height:420px;overflow:auto;font:12px/1.45 ui-monospace,Menlo,Consolas,monospace}
.frame{border:1px dashed var(--line);border-radius:12px;min-height:60vh;overflow:auto;background:#0b1324}
.preview{display:block;width:100%}
table{width:100%;border-collapse:collapse}th,td{padding:8px 6px;border-bottom:1px solid var(--line);text-align:left}
"""

def _layout(title: str, inner_html: str) -> str:
    return f"""<!doctype html>
<html lang="ru"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>{_escape(title)} · Campaigns</title>
<style>{_BASE_CSS}</style>
</head><body>
<div class="wrap">
  <div class="card"><div class="head"><div class="title">{_escape(title)}</div>
    <div class="actions"><a class="btn" href="/campaigns">Мои кампании</a><a class="btn primary" href="/campaigns/new">Новая</a></div>
  </div></div>
  {inner_html}
</div>
</body></html>"""

def _status_badge(status: str) -> str:
    s = (status or "").lower()
    cls = "badge ok" if s == "done" else "badge" if s in ("queued", "running") else "badge err"
    return f'<span class="{cls}">{_escape(status)}</span>'

def _csrf_read() -> str:
    tok = session.get("_csrf")
    if not tok:
        tok = base64.urlsafe_b64encode(os.urandom(24)).decode("ascii").rstrip("=")
        session["_csrf"] = tok
    return tok

def _csrf_check(value: Optional[str]) -> None:
    if not value or value != session.get("_csrf"):
        abort(400, description="CSRF token invalid")

def _require_user() -> str:
    email = session.get("user_email")
    if not email:
        abort(401)
    return str(email)

def _new_form(csrf: str, defaults: Dict[str, Any], accounts: List[Dict[str, Any]]) -> str:
    d = lambda k, v="": _escape(str(defaults.get(k, v)))
    cur_acc = str(defaults.get("account_id") or "")
    acc_opts = "".join(
        f'<option value="{_escape(a["id"])}" {"selected" if str(a["id"])==cur_acc else ""}>'
        f'{_escape(a["name"])} · профиль {_escape(a["profile_id"])}</option>'
        for a in accounts
    )
    ph = '<option value="" disabled selected>— выберите —</option>' if not cur_acc else ""
    return f"""
<div class="card"><div class="head"><div class="title">Новая кампания</div></div>
<div class="body">
  <form method="post" action="/campaigns/new" class="form" autocomplete="on" novalidate>
    <input type="hidden" name="_csrf" value="{_escape(csrf)}"/>
    <div class="row"><label class="label" for="goal">Цель</label>
      <select class="sel" id="goal" name="goal">
        <option value="Трафик" {"selected" if d("goal","Трафик")=="Трафик" else ""}>Трафик</option>
        <option value="Лиды" {"selected" if d("goal")=="Лиды" else ""}>Лиды</option>
        <option value="Продажи" {"selected" if d("goal")=="Продажи" else ""}>Продажи</option>
      </select>
    </div>
    <div class="row"><label class="label" for="geo">Гео</label><input class="inp" type="text" id="geo" name="geo" value="{d("geo","Россия")}"/></div>
    <div class="row"><label class="label" for="language">Язык</label><input class="inp" type="text" id="language" name="language" value="{d("language","ru")}"/></div>
    <div class="row"><label class="label" for="budget_daily">Бюджет/день</label><input class="inp" type="number" step="1" id="budget_daily" name="budget_daily" value="{d("budget_daily","1000")}"/></div>
    <div class="row full"><label class="label" for="landing_url">Лендинг (URL)</label><input class="inp" type="url" id="landing_url" name="landing_url" value="{d("landing_url","https://example.com")}" required/></div>
    <div class="row full"><label class="label" for="description">Описание оффера</label><textarea class="ta" id="description" name="description">{d("description")}</textarea></div>
    <div class="row"><label class="label" for="campaign_type">Тип</label>
      <select class="sel" id="campaign_type" name="campaign_type">
        <option value="search" {"selected" if d("campaign_type","search")=="search" else ""}>Поисковая</option>
        <option value="pmax" {"selected" if d("campaign_type")=="pmax" else ""}>Performance Max</option>
        <option value="display" {"selected" if d("campaign_type")=="display" else ""}>КМС</option>
      </select>
    </div>
    <div class="row"><label class="label" for="currency_sign">Валюта</label><input class="inp" type="text" id="currency_sign" name="currency_sign" value="{d("currency_sign","₽")}" maxlength="3"/></div>
    <div class="row"><label class="label" for="account_id">Google-аккаунт</label>
      <select class="sel" id="account_id" name="account_id" required>{ph}{acc_opts}</select>
    </div>
    <div class="row full actions">
      <button class="btn primary" type="submit">Создать</button>
      <a class="btn" target="_blank" rel="noopener" href="https://ads.google.com/aw/overview">Открыть Ads</a>
      <a class="btn" href="/accounts">Аккаунты</a>
    </div>
  </form>
</div></div>
"""

def _list_html(items: List[Dict[str, Any]], csrf: str) -> str:
    rows = "".join(
        f"<tr><td>{_escape(it['id'])}</td><td>{_status_badge(it['status'])}</td>"
        f"<td>{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(it['created_at']))}</td>"
        f"<td style='display:flex;gap:8px'><a class='btn' href='/campaigns/{_escape(it['id'])}'>Открыть</a>"
        f"<button class='btn' data-del='{_escape(it['id'])}' data-csrf='{_escape(csrf)}'>Удалить</button></td></tr>"
        for it in items
    )
    js = """
<script>
document.querySelectorAll('button[data-del]').forEach(btn=>{
  btn.addEventListener('click', async ()=>{
    const id=btn.getAttribute('data-del'); const tok=btn.getAttribute('data-csrf');
    if(!confirm('Удалить кампанию '+id+'?')) return;
    const r=await fetch('/campaigns/'+id+'/delete',{method:'POST',headers:{'Content-Type':'application/json','X-CSRF':tok},body:'{}'});
    if(r.ok) location.reload(); else alert(await r.text());
  });
});
</script>
"""
    return f"""
<div class="card"><div class="head"><div class="title">Мои кампании</div></div>
<div class="body">
  <table><thead><tr><th>ID</th><th>Статус</th><th>Создана</th><th></th></tr></thead>
  <tbody>{rows or '<tr><td colspan="4">Пусто</td></tr>'}</tbody></table>
</div></div>{js}
"""

def _demo_html(task_id: str, csrf: str, spec: Dict[str, Any], status: str) -> str:
    badge = _status_badge(status)
    script = f"""
<script>
const $log=document.getElementById('log');
const $prev=document.getElementById('preview');
const es=new EventSource('/campaigns/{task_id}/events');
let esPrev=null;
try{{ esPrev=new EventSource('/campaigns/{task_id}/preview'); }}catch(_){{}}
function esc(s){{const d=document.createElement('div'); d.innerText=(s==null?'':s); return d.innerHTML;}}
function line(kind, msg){{const d=document.createElement('div'); d.innerHTML=msg; $log.appendChild(d); $log.scrollTop=$log.scrollHeight;}}
es.addEventListener('hello', e=> line('info','SSE: готово'));
es.addEventListener('info', e=>{{ let j={{}}; try{{j=JSON.parse(e.data||'{{}}')}}catch(_){{}}; if(j.msg) line('info', esc(j.msg)); }});
es.addEventListener('stage', e=>{{ let j={{}}; try{{j=JSON.parse(e.data||'{{}}')}}catch(_){{}}; const s=(j.stage?('<b>'+esc(j.stage)+'</b>: '):'')+(j.status||''); line('stage', s + (j.error?(' — '+esc(j.error)):'') ); }});
es.addEventListener('log', e=>{{ let j={{}}; try{{j=JSON.parse(e.data||'{{}}')}}catch(_){{}}; if(j.msg) line('log', esc(j.msg)); }});
es.addEventListener('artifact', e=>{{ let j={{}}; try{{j=JSON.parse(e.data||'{{}}')}}catch(_){{}}; if(j.kind==='screenshot' && j.url){{ $prev.src=j.url+'?r='+Date.now(); }} }});
es.addEventListener('vision:image', e=>{{ let j={{}}; try{{j=JSON.parse(e.data||'{{}}')}}catch(_){{}}; if(j && j.data) $prev.src='data:image/png;base64,'+j.data; }});
es.addEventListener('ui:scan', e=>{{ let j={{}}; try{{j=JSON.parse(e.data||'{{}}')}}catch(_){{}}; if(j && j.counts) line('ui', 'UI-scan: '+JSON.stringify(j)); }});
if(esPrev) esPrev.addEventListener('preview:image', e=>{{ let j={{}}; try{{j=JSON.parse(e.data||'{{}}')}}catch(_){{}}; if(j && j.data) $prev.src='data:image/png;base64,'+j.data; }});
async function post(op, data){{ return fetch('/campaigns/{task_id}/control', {{ method:'POST', headers: {{'Content-Type':'application/json','X-CSRF':{json.dumps(csrf)} }}, body: JSON.stringify(Object.assign({{op}}, data||{{}})) }}); }}
document.getElementById('pause').onclick=()=>post('pause',{{}});
document.getElementById('resume').onclick=()=>post('resume',{{}});
document.getElementById('abort').onclick=()=>post('abort',{{}});
document.getElementById('delete').onclick=async()=>{{ if(!confirm('Удалить?')) return; const r=await fetch('/campaigns/{task_id}/delete', {{method:'POST',headers:{{'Content-Type':'application/json','X-CSRF':{json.dumps(csrf)}}}, body:'{{}}'}}); if(r.ok) window.location='/campaigns'; else alert(await r.text()); }};
</script>
"""
    return f"""
<div class="card"><div class="head"><div class="title">Кампания { _escape(task_id) }</div>{badge}</div>
<div class="body">
  <div class="actions" style="margin-bottom:8px">
    <button class="btn" id="pause">Пауза</button>
    <button class="btn" id="resume">Продолжить</button>
    <button class="btn" id="abort">Прервать</button>
    <button class="btn" id="delete">Удалить</button>
    <a class="btn" target="_blank" rel="noopener" href="https://ads.google.com/aw/overview">Ads</a>
  </div>
  <div class="frame"><img id="preview" class="preview" alt="скриншот"/></div>
</div></div>
<div class="card"><div class="head"><div class="title">Логи</div></div>
<div class="body"><div id="log" class="log"></div></div></div>
{script}
"""

# =============================== ПУБЛИЧНЫЙ КОНТРАКТ ==========================

def init_create_companies(app: Flask, settings: Settings) -> None:
    """
    Регистрация роутов кампаний (list/new/view/logs/sse/preview/control/artifacts).
    """
    # ENV для AdsPower
    try:
        browser = getattr(settings, "browser", None)
        headless_default = bool(getattr(browser, "headless_default", True))
        os.environ.setdefault("ADS_AI_HEADLESS", "1" if headless_default else "0")
        adsp_base = (
            os.getenv("ADSP_API_BASE")
            or getattr(browser, "adsp_api_base", None)
            or getattr(settings, "adsp_api_base", None)
            or "http://local.adspower.net:50325"
        )
        os.environ.setdefault("ADSP_API_BASE", str(adsp_base))
        adsp_token = (
            os.getenv("ADSP_API_TOKEN")
            or getattr(settings, "adsp_api_token", None)
            or getattr(browser, "adsp_api_token", None)
            or ""
        )
        if adsp_token:
            os.environ.setdefault("ADSP_API_TOKEN", str(adsp_token))
    except Exception:
        pass

    paths = _resolve_paths(settings)
    db = CampaignDB(paths.db_file)
    tm = TaskManager(settings, db, paths)

    # ---- Health ----
    @app.get("/_health")
    def _health() -> Response:
        return jsonify({"ok": True})

    # ---- Campaigns: list ----
    @app.get("/campaigns")
    def campaigns_list() -> Response:
        try:
            email = _require_user()
        except Exception:
            return redirect(url_for("auth_login"))
        items = db.list_for_user(email, limit=100)
        csrf = _csrf_read()
        return make_response(_layout("Кампании", _list_html(items, csrf)))

    @app.get("/campaigns/")
    def campaigns_list_slash() -> Response:
        return redirect("/campaigns", code=301)

    # ---- Campaigns: new ----
    @app.get("/campaigns/new")
    def campaigns_new_get() -> Response:
        try:
            email = _require_user()
        except Exception:
            return redirect(url_for("auth_login"))
        csrf = _csrf_read()
        accs = db.list_accounts(email)
        return make_response(_layout("Создание кампании", _new_form(csrf, {}, accs)))

    @app.post("/campaigns/new")
    def campaigns_new_post() -> Response:
        try:
            email = _require_user()
        except Exception:
            return redirect(url_for("auth_login"))
        _csrf_check(request.form.get("_csrf"))

        try:
            goal = (request.form.get("goal") or "Трафик").strip()
            landing_url = (request.form.get("landing_url") or "").strip()
            description = (request.form.get("description") or "").strip()
            budget_daily = float(request.form.get("budget_daily") or "1000")
            geo = (request.form.get("geo") or "Россия").strip()
            language = (request.form.get("language") or "ru").strip()
            campaign_type = (request.form.get("campaign_type") or "search").strip()
            currency_sign = ((request.form.get("currency_sign") or "₽").strip()[:3] or "₽")
            account_id = (request.form.get("account_id") or "").strip() or None
        except Exception as e:
            return make_response(f"Некорректные параметры: {e}", 400)

        if not landing_url or not account_id:
            csrf = _csrf_read()
            defaults = {
                "goal": goal,
                "landing_url": landing_url,
                "description": description,
                "budget_daily": budget_daily,
                "geo": geo,
                "language": language,
                "campaign_type": campaign_type,
                "currency_sign": currency_sign,
                "account_id": account_id or "",
            }
            html_doc = _new_form(csrf, {**defaults, "error": "URL и Google-аккаунт обязательны"}, db.list_accounts(email))
            return make_response(_layout("Создание кампании", html_doc), 400)

        acc = db.get_account(account_id, email=email)
        if not acc:
            csrf = _csrf_read()
            defaults = {
                "goal": goal,
                "landing_url": landing_url,
                "description": description,
                "budget_daily": budget_daily,
                "geo": geo,
                "language": language,
                "campaign_type": campaign_type,
                "currency_sign": currency_sign,
                "account_id": account_id or "",
            }
            html_doc = _new_form(csrf, {**defaults, "error": "Выбранный аккаунт недоступен"}, db.list_accounts(email))
            return make_response(_layout("Создание кампании", html_doc), 400)

        spec = CampaignSpec(
            goal=goal,
            landing_url=landing_url,
            description=description,
            budget_daily=budget_daily,
            geo=geo,
            language=language,
            profile_id=str(acc["profile_id"]),
            campaign_type=campaign_type,
            currency_sign=currency_sign,
            account_id=account_id,
        )
        run_id = _now_id("run")
        task_id = db.create(email, spec, run_id)
        tm.submit(task_id, email)
        return redirect(f"/campaigns/{task_id}")

    # ---- Campaign page ----
    @app.get("/campaigns/<task_id>")
    def campaign_demo(task_id: str) -> Response:
        try:
            _ = _require_user()
        except Exception:
            return redirect(url_for("auth_login"))
        meta = db.get(task_id)
        if not meta:
            return make_response("Задача не найдена", 404)
        csrf = _csrf_read()
        inner = _demo_html(task_id, csrf, meta.get("spec", {}), meta.get("status", "queued"))
        return make_response(_layout("Демонстрация", inner))

    # ---- Logs (simple view) ----
    @app.get("/campaigns/<task_id>/logs")
    def campaign_logs(task_id: str) -> Response:
        try:
            _ = _require_user()
        except Exception:
            return redirect(url_for("auth_login"))
        html_doc = """
<div class="card"><div class="head"><div class="title">Логи</div></div>
<div class="body"><div id="log" class="log"></div></div></div>
<script>
const $log=document.getElementById('log');
function esc(s){const d=document.createElement('div'); d.innerText=(s==null?'':s); return d.innerHTML;}
function line(msg){const d=document.createElement('div'); d.innerHTML=msg; $log.appendChild(d); $log.scrollTop=$log.scrollHeight;}
const es=new EventSource(location.pathname.replace('/logs','/events'));
es.addEventListener('info', e=>{ let j={}; try{j=JSON.parse(e.data||'{}')}catch(_){ } if(j.msg) line(esc(j.msg)); });
es.addEventListener('stage', e=>{ let j={}; try{j=JSON.parse(e.data||'{}')}catch(_){ } const s=(j.stage?('<b>'+esc(j.stage)+'</b>: '):'')+(j.status||''); line(s+(j.error?(' — '+esc(j.error)):'') ); });
es.addEventListener('ui:scan', e=>{ let j={}; try{j=JSON.parse(e.data||'{}')}catch(_){ } line('[ui] '+JSON.stringify(j)); });
</script>
"""
        return make_response(_layout("Логи", html_doc))

    # ---- Control ----
    @app.post("/campaigns/<task_id>/control")
    def campaign_control(task_id: str) -> Response:
        try:
            _ = _require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        _csrf_check(request.headers.get("X-CSRF"))
        data = request.get_json(silent=True) or {}
        op = (data.get("op") or "").lower()
        st = tm.control(task_id)
        if op == "pause":
            st.paused = True
            db.append_event(task_id, "info", {"msg": "⏸ Пауза"})
            return jsonify({"ok": True})
        if op == "resume":
            st.paused = False
            db.append_event(task_id, "info", {"msg": "▶ Продолжить"})
            return jsonify({"ok": True})
        if op == "abort":
            st.abort = True
            db.append_event(task_id, "info", {"msg": "■ Прервать"})
            return jsonify({"ok": True})
        if op == "manual":
            act = {
                "kind": (data.get("kind") or "click"),
                "selector": (data.get("selector") or ""),
                "text": (data.get("text") or ""),
            }
            try:
                st.manual_actions.put_nowait(act)
                db.append_event(task_id, "info", {"msg": f"🛠 Ручное: {act['kind']} {act['selector']}"})
                return jsonify({"ok": True})
            except Exception as e:
                return jsonify({"ok": False, "error": str(e)}), 400
        return jsonify({"ok": False, "error": "unknown_op"}), 400

    # ---- Delete ----
    @app.post("/campaigns/<task_id>/delete")
    def campaign_delete(task_id: str) -> Response:
        try:
            email = _require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        _csrf_check(request.headers.get("X-CSRF"))
        meta = db.get(task_id)
        if not meta:
            return jsonify({"ok": False, "error": "not_found"}), 404
        if str(meta.get("status", "")).lower() == "running":
            st = tm.control(task_id)
            st.abort = True
            db.append_event(task_id, "info", {"msg": "■ Прервать перед удалением"})
            return jsonify({"ok": False, "error": "running"}), 409
        ok = db.delete(task_id, user_email=email)
        if not ok:
            return jsonify({"ok": False, "error": "forbidden"}), 403
        # Частичная очистка артефактов по run_id
        try:
            run_id = meta.get("run_id") or ""
            for p in ((paths.shots / run_id), (paths.html / run_id), (paths.artifacts / run_id)):
                if p.exists() and p.is_dir():
                    import shutil
                    shutil.rmtree(p, ignore_errors=True)
        except Exception:
            pass
        return jsonify({"ok": True})

    # ---- SSE events ----
    @app.get("/campaigns/<task_id>/events")
    def campaign_events(task_id: str) -> Response:
        try:
            _ = _require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        try:
            lid0 = int(request.headers.get("Last-Event-ID") or "0")
        except Exception:
            lid0 = 0

        def gen():
            yield "retry: 2000\n\n"
            yield "event: hello\n"
            yield f"data: {json.dumps({'msg':'SSE connected'}, ensure_ascii=False)}\n\n"
            lid = lid0
            idle = 0
            while True:
                try:
                    rows = db.events_since(task_id, lid)
                    if rows:
                        for r in rows:
                            lid = r["id"]
                            if str(r.get("type", "")) == "heartbeat":
                                continue
                            payload = json.dumps(r["data"], ensure_ascii=False)
                            yield f"id: {r['id']}\n"
                            yield f"event: {r['type']}\n"
                            yield f"data: {payload}\n\n"
                        idle = 0
                    else:
                        idle += 1
                        if idle % 10 == 0:
                            yield ":hb\n\n"
                        time.sleep(0.6)
                except GeneratorExit:
                    break
                except Exception:
                    time.sleep(0.8)

        resp = Response(gen(), mimetype="text/event-stream")
        resp.headers["Cache-Control"] = "no-cache"
        resp.headers["X-Accel-Buffering"] = "no"
        return resp

    # ---- SSE preview ----
    @app.get("/campaigns/<task_id>/preview")
    def campaign_preview(task_id: str) -> Response:
        try:
            _ = _require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        st = tm.control(task_id)

        def gen():
            yield "retry: 40\n\n"
            i = 1
            while True:
                try:
                    b64 = st.preview_q.get(timeout=2.0)
                except queue.Empty:
                    if st.preview_stop.is_set():
                        break
                    yield ":hb\n\n"
                    continue
                payload = json.dumps({"data": b64}, ensure_ascii=False)
                yield f"id: {i}\n"
                yield "event: preview:image\n"
                yield f"data: {payload}\n\n"
                i += 1

        resp = Response(gen(), mimetype="text/event-stream")
        resp.headers["Cache-Control"] = "no-cache"
        resp.headers["X-Accel-Buffering"] = "no"
        return resp

    # ---- Артефакты ----
    @app.get("/campaigns/artifact/<path:rel>")
    def artifact_serve(rel: str) -> Response:
        root = paths.artifacts
        path = (root / rel).resolve()
        if not str(path).startswith(str(root.resolve())):
            return make_response("forbidden", 403)
        if not path.exists():
            return make_response("not found", 404)
        try:
            if path.suffix.lower() == ".html":
                return send_file(path, mimetype="text/html")
            if path.suffix.lower() == ".json":
                return send_file(path, mimetype="application/json")
            if path.suffix.lower() == ".jsonl":
                return send_file(path, mimetype="text/plain")
            if path.suffix.lower() in (".png", ".jpg", ".jpeg", ".webp"):
                return send_file(path)
            return send_file(path)
        except Exception:
            return make_response("error", 500)


# =============================== SHARED REGISTRAR ============================

def init_campaigns_module(
    app: Flask,
    *,
    db,
    tm,
    paths,
    layout,
    new_form_html,
    list_html,
    demo_html,
    read_csrf,
    check_csrf,
    require_user,
    now_id,
    spec_class,
) -> None:
    """Register campaign routes using external db/tm and UI helpers (Creation Center)."""

    @app.get("/campaigns")
    def _cc_campaigns_list() -> Response:
        try:
            email = require_user()
        except Exception:
            return redirect(url_for("auth_login"))
        items = db.list_campaigns(email, limit=100)
        csrf = read_csrf()
        return make_response(layout("Кампании", list_html(items, csrf), active="console"))

    @app.get("/campaigns/")
    def _cc_campaigns_list_slash() -> Response:
        return redirect("/campaigns", code=301)

    @app.get("/campaigns/new")
    def _cc_campaigns_new_get() -> Response:
        try:
            email = require_user()
        except Exception:
            return redirect(url_for("auth_login"))
        csrf = read_csrf()
        accs = db.list_accounts(email)
        return make_response(layout("Создать кампанию", new_form_html(csrf, {}, accs), active="console"))

    @app.post("/campaigns/new")
    def _cc_campaigns_new_post() -> Response:
        try:
            email = require_user()
        except Exception:
            return redirect(url_for("auth_login"))
        check_csrf(request.form.get("_csrf"))

        try:
            goal = (request.form.get("goal") or "Трафик").strip()
            landing_url = (request.form.get("landing_url") or "").strip()
            description = (request.form.get("description") or "").strip()
            budget_daily = float(request.form.get("budget_daily") or "1000")
            geo = (request.form.get("geo") or "Россия").strip()
            language = (request.form.get("language") or "ru").strip()
            campaign_type = (request.form.get("campaign_type") or "search").strip()
            currency_sign = ((request.form.get("currency_sign") or "₽").strip()[:3] or "₽")
            account_id = (request.form.get("account_id") or "").strip() or None
        except Exception as e:
            return make_response(f"Некорректные параметры: {e}", 400)

        if not landing_url or not account_id:
            csrf = read_csrf()
            defaults = {
                "goal": goal,
                "landing_url": landing_url,
                "description": description,
                "budget_daily": budget_daily,
                "geo": geo,
                "language": language,
                "campaign_type": campaign_type,
                "currency_sign": currency_sign,
                "account_id": account_id or "",
            }
            html_doc = new_form_html(csrf, {**defaults, "error": "URL и Google‑аккаунт обязательны"}, db.list_accounts(email))
            return make_response(layout("Создать кампанию", html_doc, active="console"), 400)

        acc = db.get_account(account_id, email=email) if account_id else None
        if not acc:
            csrf = read_csrf()
            defaults = {
                "goal": goal,
                "landing_url": landing_url,
                "description": description,
                "budget_daily": budget_daily,
                "geo": geo,
                "language": language,
                "campaign_type": campaign_type,
                "currency_sign": currency_sign,
                "account_id": account_id or "",
            }
            html_doc = new_form_html(csrf, {**defaults, "error": "Выбранный аккаунт недоступен"}, db.list_accounts(email))
            return make_response(layout("Создать кампанию", html_doc, active="console"), 400)

        spec = spec_class(
            goal=goal,
            landing_url=landing_url,
            description=description,
            budget_daily=budget_daily,
            geo=geo,
            language=language,
            profile_id=str(acc["profile_id"]),
            campaign_type=campaign_type,
            currency_sign=currency_sign,
            account_id=account_id,
        )

        run_id = now_id("run")
        task_id = db.new_campaign(email, spec, run_id)
        tm.submit(task_id, email)
        return redirect(f"/campaigns/{task_id}")

    @app.get("/campaigns/<task_id>")
    def _cc_campaign_view(task_id: str) -> Response:
        try:
            _ = require_user()
        except Exception:
            return redirect(url_for("auth_login"))
        meta = db.get_campaign(task_id)
        if not meta:
            return make_response("not found", 404)
        csrf = read_csrf()
        inner = demo_html(task_id, csrf, meta.get("spec", {}), meta.get("status", "queued"))
        return make_response(layout("Mission Control", inner, active="console"))

    @app.post("/campaigns/<task_id>/control")
    def _cc_campaign_control(task_id: str) -> Response:
        try:
            _ = require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        check_csrf(request.headers.get("X-CSRF"))
        data = request.get_json(silent=True) or {}
        op = (data.get("op") or "").lower()
        st = tm.control(task_id)
        if op == "pause":
            st.paused = True
            db.append_event(task_id, "info", {"msg": "⏸ Пауза"})
            return jsonify({"ok": True})
        if op == "resume":
            st.paused = False
            db.append_event(task_id, "info", {"msg": "▶ Продолжить"})
            return jsonify({"ok": True})
        if op == "abort":
            st.abort = True
            db.append_event(task_id, "info", {"msg": "■ Прервать"})
            return jsonify({"ok": True})
        if op == "manual":
            act = {
                "kind": (data.get("kind") or "click"),
                "selector": (data.get("selector") or ""),
                "text": (data.get("text") or ""),
            }
            try:
                st.manual_actions.put_nowait(act)
                db.append_event(task_id, "info", {"msg": f"🛠 Ручное: {act['kind']} {act['selector']}"})
                return jsonify({"ok": True})
            except Exception as e:
                return jsonify({"ok": False, "error": str(e)}), 400
        return jsonify({"ok": False, "error": "unknown_op"}), 400

    @app.post("/campaigns/<task_id>/delete")
    def _cc_campaigns_delete(task_id: str) -> Response:
        try:
            email = require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        check_csrf(request.headers.get("X-CSRF"))
        ok = db.delete_campaign(task_id, user_email=email)
        return jsonify({"ok": bool(ok)}) if ok else (jsonify({"ok": False, "error": "forbidden"}), 403)

    @app.get("/campaigns/<task_id>/events")
    def _cc_campaign_events(task_id: str) -> Response:
        try:
            _ = require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        try:
            lid0 = int(request.headers.get("Last-Event-ID") or "0")
        except Exception:
            lid0 = 0
        def gen():
            yield "retry: 2000\n\n"
            yield "event: hello\n"
            yield "data: {}\n\n"
            idle = 0
            lid = lid0
            while True:
                try:
                    rows = db.events_since(task_id, lid)
                    if rows:
                        for r in rows:
                            lid = r["id"]
                            if str(r.get("type", "")) == "heartbeat":
                                continue
                            payload = json.dumps(r["data"], ensure_ascii=False)
                            yield f"id: {r['id']}\n"
                            yield f"event: {r['type']}\n"
                            yield f"data: {payload}\n\n"
                        idle = 0
                    else:
                        idle += 1
                        if idle % 10 == 0:
                            yield ":hb\n\n"
                        time.sleep(0.6)
                except GeneratorExit:
                    break
                except Exception:
                    time.sleep(0.8)
        resp = Response(gen(), mimetype="text/event-stream")
        resp.headers["Cache-Control"] = "no-cache"
        resp.headers["X-Accel-Buffering"] = "no"
        return resp

    @app.get("/campaigns/<task_id>/preview")
    def _cc_campaign_preview(task_id: str) -> Response:
        try:
            _ = require_user()
        except Exception:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        st = tm.control(task_id)
        def gen():
            yield "retry: 40\n\n"
            i = 1
            while True:
                try:
                    b64 = st.preview_q.get(timeout=2.0)
                except queue.Empty:
                    if st.preview_stop.is_set():
                        break
                    yield ":hb\n\n"
                    continue
                payload = json.dumps({"data": b64}, ensure_ascii=False)
                yield f"id: {i}\n"
                yield "event: preview:image\n"
                yield f"data: {payload}\n\n"
                i += 1
        resp = Response(gen(), mimetype="text/event-stream")
        resp.headers["Cache-Control"] = "no-cache"
        resp.headers["X-Accel-Buffering"] = "no"
        return resp

    @app.get("/campaigns/artifact/<path:rel>")
    def _cc_artifact_serve(rel: str) -> Response:
        root = paths.artifacts
        path = (root / rel).resolve()
        if not str(path).startswith(str(root.resolve())):
            return make_response("forbidden", 403)
        if not path.exists():
            return make_response("not found", 404)
        try:
            if path.suffix.lower() == ".html":
                return send_file(path, mimetype="text/html")
            return send_file(path)
        except Exception:
            return make_response("error", 500)
