# -*- coding: utf-8 -*-
"""
Продакшен-авторизация/регистрация (без MFA) для Ads AI Agent.

Особенности:
- SQLite (WAL), миграции, индексы. Таблицы: users, login_attempts, kv.
- Регистрация по заявке: админ подтверждает в Telegram через inline-кнопки (callback_data).
- Без вебхуков: long polling бота (getUpdates), домен не требуется (вебхук удаляем на старте).
- Жёсткая сессия, строгие cookie-флаги, CSRF для POST, rate-limit логина.
- Аккуратный UI (light/dark), без эмодзи: минималистичная SVG-иконка показа пароля.
- Видимость ID аккаунта: показываем ID в профиле, в карточках Telegram и в ответах после регистрации.

ENV (или Settings.integrations):
  - TG_BOT_TOKEN=123456:ABC...
  - TG_ADMIN_CHAT_ID=987654321     # numeric ID (узнать через /whoami бота)
  - WEB_SECRET_KEY=<секрет для Flask-сессии>
"""

from __future__ import annotations

import os
import re
import hmac
import time
import json
import html
import base64
import hashlib
import secrets
import sqlite3
import threading
import logging
from dataclasses import dataclass
from typing import Optional, Tuple, Dict, Any
from string import Template

import requests
from flask import (
    Flask, request, Response, make_response, redirect, session,
    url_for, abort, jsonify
)

from ads_ai.config.settings import Settings

# ---------------- logging ----------------
log = logging.getLogger(__name__)
if not log.handlers:
    handler = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    handler.setFormatter(fmt)
    log.addHandler(handler)
log.setLevel(logging.INFO)
log.propagate = False  # исключаем дубли в корневой логгер

# ============================ Крипто / PBKDF2 ============================

_PBKDF2_ROUNDS = 200_000


def _pbkdf2(password: str, salt: bytes, rounds: int = _PBKDF2_ROUNDS) -> str:
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, rounds, dklen=32)
    return dk.hex()


def make_password(password: str) -> Tuple[str, str]:
    salt = secrets.token_bytes(16)
    return salt.hex(), _pbkdf2(password, salt)


def verify_password(password: str, salt_hex: str, hash_hex: str) -> bool:
    try:
        salt = bytes.fromhex(salt_hex)
        calc = _pbkdf2(password, salt)
        return hmac.compare_digest(calc, hash_hex)
    except Exception:
        return False


# ============================ Интеграции / конфиги ============================

def _get_integrations_value(settings: Settings, key: str) -> str:
    try:
        integ = getattr(settings, "integrations", None)
        if integ is None:
            return ""
        if hasattr(integ, key):
            return str(getattr(integ, key) or "")
        if isinstance(integ, dict):
            v = integ.get(key)
            return str(v or "")
    except Exception:
        pass
    return ""


def read_integrations(settings: Settings) -> Dict[str, str]:
    """Параметры из Settings.integrations с фоллбэком к ENV."""
    def get(k: str) -> str:
        return _get_integrations_value(settings, k) or os.environ.get(k, "") or ""
    return {
        "tg_bot_token": get("tg_bot_token") or os.environ.get("TG_BOT_TOKEN", ""),
        "tg_admin_chat_id": get("tg_admin_chat_id") or os.environ.get("TG_ADMIN_CHAT_ID", ""),
    }


# ============================ SQLite хранилище ============================

@dataclass
class User:
    id: int
    email: str
    name: str
    pw_salt_hex: str
    pw_hash_hex: str
    approved: bool
    created_at: float
    approval_token: str
    last_login_at: float


def _as_float(v: Any, default: float = 0.0) -> float:
    """Безопасное приведение в float — не падает на '', None, '0' и т.д."""
    try:
        if v is None:
            return default
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if not s:
            return default
        return float(s)
    except Exception:
        return default


class AuthDB:
    def __init__(self, path: str | os.PathLike[str]):
        self.path = str(path)
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._lock = threading.RLock()
        # isolation_level=None -> autocommit; check_same_thread=False -> общая коннекция
        self._conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._migrate()

    def _migrate(self) -> None:
        with self._conn:
            # Пользователи
            self._conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              email TEXT NOT NULL UNIQUE,
              name TEXT NOT NULL,
              pw_salt_hex TEXT NOT NULL,
              pw_hash_hex TEXT NOT NULL,
              approved INTEGER NOT NULL DEFAULT 0,
              created_at REAL NOT NULL,
              approval_token TEXT NOT NULL DEFAULT '',
              last_login_at REAL NOT NULL DEFAULT 0
            );
            """)
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_users_approval_token ON users(approval_token);")

            # Попытки входа (для rate-limit)
            self._conn.execute("""
            CREATE TABLE IF NOT EXISTS login_attempts (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ip TEXT NOT NULL,
              email TEXT NOT NULL,
              ts REAL NOT NULL,
              ok INTEGER NOT NULL
            );
            """)
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_attempts_ip_email ON login_attempts(ip, email, ts);")

            # Простой KV (под оффсеты poller'а и т.п.)
            self._conn.execute("""
            CREATE TABLE IF NOT EXISTS kv (
              k TEXT PRIMARY KEY,
              v TEXT NOT NULL
            );
            """)

            # --- Мягкая миграция «пустых» времён из старой схемы ('' -> 0)
            try:
                self._conn.execute("UPDATE users SET created_at = 0 WHERE created_at IS NULL OR created_at = ''")
                self._conn.execute("UPDATE users SET last_login_at = 0 WHERE last_login_at IS NULL OR last_login_at = ''")
            except Exception:
                pass

    # ---- KV

    def get_kv(self, key: str) -> Optional[str]:
        q = self._conn.execute("SELECT v FROM kv WHERE k=? LIMIT 1;", (key,))
        row = q.fetchone()
        return str(row[0]) if row else None

    def set_kv(self, key: str, value: str) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT INTO kv(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v;",
                (key, value)
            )

    # ---- Users

    def get_user_by_email(self, email: str) -> Optional[User]:
        q = self._conn.execute("SELECT * FROM users WHERE lower(email)=lower(?) LIMIT 1;", (email,))
        row = q.fetchone()
        return self._row_to_user(row) if row else None

    def get_user_by_approval_token(self, token: str) -> Optional[User]:
        q = self._conn.execute("SELECT * FROM users WHERE approval_token=? LIMIT 1;", (token,))
        row = q.fetchone()
        return self._row_to_user(row) if row else None

    def insert_pending_user(self, name: str, email: str, pw_salt_hex: str, pw_hash_hex: str, approval_token: str) -> int:
        with self._conn:
            cur = self._conn.execute("""
                INSERT INTO users (email, name, pw_salt_hex, pw_hash_hex, approved, created_at, approval_token)
                VALUES (?, ?, ?, ?, 0, ?, ?);
            """, (email, name, pw_salt_hex, pw_hash_hex, time.time(), approval_token))
            return int(cur.lastrowid)

    def approve_user_by_token(self, token: str, approve: bool) -> Optional[User]:
        with self._conn:
            u = self.get_user_by_approval_token(token)
            if not u:
                return None
            if approve:
                self._conn.execute("UPDATE users SET approved=1, approval_token='' WHERE id=?;", (u.id,))
            else:
                self._conn.execute("UPDATE users SET approval_token='' WHERE id=?;", (u.id,))
            return self.get_user_by_email(u.email)

    def update_last_login(self, user_id: int) -> None:
        with self._conn:
            self._conn.execute("UPDATE users SET last_login_at=? WHERE id=?;", (time.time(), user_id))

    # ---- Attempts (rate-limit)

    def add_login_attempt(self, ip: str, email: str, ok: bool) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT INTO login_attempts (ip, email, ts, ok) VALUES (?, ?, ?, ?);",
                (ip, email.lower(), time.time(), 1 if ok else 0)
            )

    def count_recent_failed(self, ip: str, email: str, window_sec: int) -> int:
        """
        Сколько было НЕуспешных попыток входа для ip+email за окно времени.
        """
        since = time.time() - window_sec
        q = self._conn.execute("""
            SELECT COUNT(*) FROM login_attempts
            WHERE ip = ? AND email = ? AND ts >= ? AND ok = 0;
        """, (ip, email.lower(), since))
        row = q.fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    # ---- Utils

    @staticmethod
    def _row_to_user(row: sqlite3.Row | tuple) -> User:
        return User(
            id=int(row[0]),
            email=str(row[1]),
            name=str(row[2]),
            pw_salt_hex=str(row[3]),
            pw_hash_hex=str(row[4]),
            approved=bool(row[5]),
            created_at=_as_float(row[6], 0.0),
            approval_token=str(row[7]),
            last_login_at=_as_float(row[8], 0.0),
        )


# ============================ Telegram (polling) — только заявки ============================

class TelegramPoller:
    """
    Фоновый long-polling бота:
    - отправляет администратору сообщение с кнопками "approve:<token>" / "deny:<token>";
    - обрабатывает callback_query и меняет статус заявки.

    ВАЖНО: перед стартом СБРАСЫВАЕМ вебхук, иначе getUpdates ничего не вернёт.
    """
    def __init__(self, db: AuthDB, bot_token: str, admin_chat_id: str):
        self.db = db
        self.bot_token = bot_token
        self.admin_chat_id = str(admin_chat_id or "").strip()
        self._stop = threading.Event()
        self._th: Optional[threading.Thread] = None
        self._session = requests.Session()
        off_str = db.get_kv("tg_update_offset") or "0"
        try:
            self._offset = int(off_str)
        except Exception:
            self._offset = 0

    # --- API helpers

    def _api_get(self, method: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        url = f"https://api.telegram.org/bot{self.bot_token}/{method}"
        try:
            r = self._session.get(url, params=params, timeout=65)
            return r.json()
        except Exception as e:
            log.error("TG GET %s failed: %s", method, e)
            return None

    def _api_post(self, method: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        url = f"https://api.telegram.org/bot{self.bot_token}/{method}"
        try:
            r = self._session.post(url, data=data, timeout=15)
            return r.json()
        except Exception as e:
            log.error("TG POST %s failed: %s", method, e)
            return None

    def _delete_webhook(self) -> None:
        resp = self._api_post("deleteWebhook", {"drop_pending_updates": "false"})
        if resp and resp.get("ok"):
            log.info("Telegram webhook deleted (ok).")
        else:
            log.warning("deleteWebhook did not confirm ok: %s", resp)

    def send_admin_request(self, name: str, email: str, approval_token: str, user_id: Optional[int] = None) -> bool:
        """Отправить админу карточку заявки с кнопками. Возвращает True при успехе."""
        if not self.admin_chat_id:
            log.warning("TG_ADMIN_CHAT_ID not set; admin notification skipped.")
            return False
        text = (
            "Новая заявка на доступ\n\n"
            f"ID: {user_id if user_id is not None else '—'}\n"
            f"Имя: {name}\n"
            f"Email: {email}\n\n"
            "Включить доступ?"
        )
        keyboard = {"inline_keyboard": [[
            {"text": "✅ Включить", "callback_data": f"approve:{approval_token}"},
            {"text": "❌ Отклонить", "callback_data": f"deny:{approval_token}"},
        ]]}
        resp = self._api_post("sendMessage", {
            "chat_id": self.admin_chat_id,
            "text": text,
            "reply_markup": json.dumps(keyboard, ensure_ascii=False),
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        })
        ok = bool(resp and resp.get("ok"))
        log.info("Admin request sent: %s", ok)
        if not ok:
            log.warning("Telegram sendMessage failed: %s", resp)
        return ok

    def start(self) -> None:
        if self._th and self._th.is_alive():
            return
        self._delete_webhook()  # <- критично для long polling
        self._th = threading.Thread(target=self._run, name="tg-poller", daemon=True)
        self._th.start()
        log.info("TelegramPoller started (offset=%s)", self._offset)

    def stop(self) -> None:
        self._stop.set()

    # --- Main loop

    def _run(self) -> None:
        allowed = json.dumps(["message", "callback_query"])  # message — для /whoami
        while not self._stop.is_set():
            try:
                resp = self._api_get("getUpdates", {
                    "timeout": 50,
                    "offset": self._offset,
                    "allowed_updates": allowed
                })
                if not resp or not resp.get("ok"):
                    time.sleep(2)
                    continue
                for upd in resp.get("result", []):
                    uid = int(upd.get("update_id", 0))
                    if uid >= self._offset:
                        self._offset = uid + 1
                        self.db.set_kv("tg_update_offset", str(self._offset))
                    if "callback_query" in upd:
                        self._process_callback(upd["callback_query"])
                    elif "message" in upd:
                        self._process_message(upd["message"])
            except Exception as e:
                log.error("Poller loop error: %s", e)
                time.sleep(2)

    def _answer_callback(self, callback_query_id: str, text: str = "", show_alert: bool = False) -> None:
        self._api_post("answerCallbackQuery", {
            "callback_query_id": callback_query_id,
            "text": text,
            "show_alert": "true" if show_alert else "false"
        })

    def _edit_message_text(self, chat_id: str, message_id: int, text: str) -> None:
        self._api_post("editMessageText", {
            "chat_id": str(chat_id),
            "message_id": message_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        })

    def _process_message(self, msg: Dict[str, Any]) -> None:
        text = (msg.get("text") or "").strip()
        chat = msg.get("chat") or {}
        chat_id = str(chat.get("id") or "")
        from_user = msg.get("from") or {}
        from_id = str(from_user.get("id") or "")

        # Диагностика: /whoami — покажет числовой ID (чтобы правильно задать TG_ADMIN_CHAT_ID)
        if text == "/whoami":
            self._api_post("sendMessage", {
                "chat_id": chat_id,
                "text": f"Ваш ID: <b>{html.escape(from_id)}</b>",
                "parse_mode": "HTML"
            })
            return

        # Мини-справка
        if text:
            self._api_post("sendMessage", {
                "chat_id": chat_id,
                "text": "HyperAI Bot:\n• /whoami — показать ваш numeric ID\n• Админ одобряет заявки кнопками под сообщением.",
                "parse_mode": "HTML",
            })

    def _process_callback(self, cq: Dict[str, Any]) -> None:
        data = (cq.get("data") or "").strip()
        qid = str(cq.get("id") or "")
        msg = cq.get("message") or {}
        m_chat = msg.get("chat") or {}
        chat_id = str(m_chat.get("id") or "")
        message_id = int(msg.get("message_id") or 0)
        from_user = cq.get("from") or {}
        from_id = str(from_user.get("id") or "")

        log.info("Callback received: data=%s from_id=%s chat_id=%s", data, from_id, chat_id)

        if not data:
            self._answer_callback(qid)
            return

        # Разрешаем клик только админу, если задан TG_ADMIN_CHAT_ID
        if self.admin_chat_id and from_id != self.admin_chat_id:
            self._answer_callback(qid, "Нет прав", show_alert=True)
            log.warning("Callback rejected: not admin (from_id=%s, admin=%s)", from_id, self.admin_chat_id)
            return

        if data.startswith("approve:") or data.startswith("deny:"):
            token = data.split(":", 1)[1]
            approve = data.startswith("approve:")
            u = self.db.approve_user_by_token(token, approve=approve)
            if u:
                status = "одобрена ✅" if approve else "отклонена ❌"
                if chat_id and message_id:
                    self._edit_message_text(chat_id, message_id, f"Заявка: {html.escape(u.email)} (ID {u.id}) — {status}")
                self._answer_callback(qid, "Готово")
                log.info("Request %s by %s (id=%s)", "approved" if approve else "denied", u.email, u.id)
            else:
                self._answer_callback(qid, "Токен не найден/исчерпан", show_alert=True)
                log.warning("Approve/deny failed: token not found or already used")
            return

        self._answer_callback(qid)
        return


# ============================ UI (Glass) ============================

BASE_CSS = """
:root{
  --text:#111827; --muted:#6b7280;
  --ok:#16a34a; --err:#ef4444; --warn:#f59e0b;
  --radius-xl:26px; --radius:18px;
  --shadow: 0 10px 30px rgba(15,23,42,.12);
  --shadow-big: 0 30px 80px rgba(15,23,42,.18);
  --content-max: 560px;

  --neon1:#60a5fa; --neon2:#a78bfa; --neon3:#34d399; --neon4:#38bdf8;

  --glass: rgba(255,255,255,.66);
  --glass-2: rgba(255,255,255,.52);
  --border: rgba(17,24,39,.10);
  --pill-bg: rgba(255,255,255,.85);
  --btn-grad-1:#ffffff; --btn-grad-2:#f6f8fb;
}
*{box-sizing:border-box}
html,body{height:100%;margin:0;color:var(--text);font:14px/1.45 Inter,system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;-webkit-font-smoothing:antialiased}
body{
  background:
    radial-gradient(1000px 600px at 10% -10%, #ffffff 0%, #f6f7fb 46%, #eef2f7 100%),
    linear-gradient(180deg, #fff 0%, #f3f6fb 100%);
}
html[data-theme="dark"]{
  color-scheme: dark;
  --text:#e5e7eb; --muted:#9aa5b1;
  --glass: rgba(17,23,41,.55);
  --glass-2: rgba(17,23,41,.42);
  --border: rgba(255,255,255,.08);
  --pill-bg: rgba(15,21,38,.72);
  --btn-grad-1:#141a2d; --btn-grad-2:#0e1527;
}
html[data-theme="dark"] body{
  background:
    radial-gradient(1400px 900px at 60% -20%, #0f172a 0%, #0b1020 42%, #0a1020 100%),
    linear-gradient(180deg,#0b1020 0%, #0a1020 100%);
}

a{text-decoration:none;color:inherit}
.wrap{ min-height:100%; display:grid; place-items:center; padding:28px; }
.card{
  width: min(92vw, var(--content-max));
  background: var(--glass); border:1px solid var(--border); border-radius: var(--radius-xl);
  box-shadow: var(--shadow-big); backdrop-filter: blur(14px) saturate(160%);
  overflow:hidden; position:relative;
}
.header{
  display:flex; align-items:center; justify-content:space-between; gap:12px;
  padding:14px 16px; border-bottom:1px solid var(--border);
  background: color-mix(in oklab, var(--glass) 82%, transparent);
}
.brand{ display:flex; align-items:center; gap:10px; font-weight:800; letter-spacing:.2px }
.logo{ width:28px;height:28px;border-radius:10px;
  
  box-shadow: 0 10px 36px #60a5fa66, inset 0 0 0 1px #ffffff55;
}
.switch{ display:inline-flex; align-items:center; gap:8px; background:var(--pill-bg); border:1px solid var(--border);
  border-radius:999px; padding:6px 10px; font-size:12px; cursor:pointer; user-select:none; }
.body{ padding:22px 20px 20px; }
.title{ font-size:22px; font-weight:900; margin:4px 0 2px }
.sub{ color:var(--muted); }
.form{ display:grid; gap:12px; margin-top:16px; }
.row{ display:grid; gap:6px }
.label{ font-weight:600; font-size:12px; color:var(--muted) }
.inp{
  width:100%; padding:12px 12px; border-radius:14px; border:1px solid var(--border);
  outline:none; background: rgba(255,255,255,.9); color:var(--text);
}
html[data-theme="dark"] .inp{ background: rgba(12,16,30,.86) }
.inp:focus{ box-shadow: 0 0 0 3px rgba(56,189,248,.18); }
.actions{ display:flex; gap:10px; align-items:center; margin-top:6px; flex-wrap:wrap }
.btn{
  border:1px solid var(--border); background: linear-gradient(180deg, var(--btn-grad-1), var(--btn-grad-2));
  color:var(--text); border-radius: 12px; padding:10px 14px; cursor:pointer; font-weight:700;
  transition: transform .08s ease, box-shadow .25s ease, opacity .2 ease;
}
.btn[disabled]{ opacity:.6; cursor:not-allowed }
.btn:hover{ transform: translateY(-1px); box-shadow: 0 12px 36px rgba(15,23,42,.18) }
.btn.primary{ position:relative; box-shadow: 0 14px 36px rgba(96,165,250,.25) }
.btn.primary::after{
  content:""; position:absolute; inset:-2px; border-radius:12px; pointer-events:none;
  background: linear-gradient(90deg, var(--neon1), var(--neon2), var(--neon4), var(--neon3));
  opacity:.45; filter: blur(12px);
}
.badge{display:inline-flex;gap:6px;align-items:center;padding:6px 10px;border:1px solid var(--border);border-radius:999px;background:var(--pill-bg);font-size:12px;color:var(--muted)}
.note{ color:var(--muted); font-size:12px; }
.err{ color:var(--err); font-weight:700; margin-top:2px }
.ok{ color:var(--ok); font-weight:700; margin-top:2px }
.footer{ padding:14px 16px; border-top:1px solid var(--border); display:flex; align-items:center; justify-content:space-between; color:var(--muted) }
.help{ font-size:12px; }

/* Поле пароля с минималистичной иконкой */
.pw-wrap{ position:relative }
.pw-toggle{
  position:absolute; right:10px; top:67%; transform: translateY(-50%);
  display:inline-flex; align-items:center; justify-content:center;
  border:0; background:transparent; padding:6px; margin:0; cursor:pointer; opacity:.8;
}
.pw-toggle:hover{ opacity:1 }
.pw-toggle svg{ width:18px; height:18px; stroke: currentColor; }
.hidden{ display:none }
.spinner{width:22px;height:22px;border-radius:999px;border:3px solid rgba(0,0,0,.12);border-top-color:#60a5fa;animation:spin 1s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
"""

THEME_JS = """
(function(){
  const saved = localStorage.getItem("hyperai_theme");
  const prefersDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
  const initial = saved || (prefersDark ? "dark" : "light");
  document.documentElement.setAttribute("data-theme", initial);
  window.__setTheme = function(mode){
    document.documentElement.setAttribute("data-theme", mode);
    localStorage.setItem("hyperai_theme", mode);
  };
})();
"""

def _escape(s: str) -> str:
    return html.escape(s, quote=True)

def _ui_header(title_right: str) -> str:
    tpl = Template("""
      <div class="header">
        <div class="brand"><div class="logo" aria-hidden="true"><svg viewBox="0 0 256 256" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <linearGradient id="rg" x1="0" y1="0" x2="256" y2="0">
      <stop offset="0%"  stop-color="#38BDF8"/><stop offset="50%" stop-color="#A78BFA"/><stop offset="100%" stop-color="#34D399"/>
    </linearGradient>
    <filter id="glow" x="-40%" y="-40%" width="180%" height="180%">
      <feGaussianBlur in="SourceGraphic" stdDeviation="3" result="b1"/>
      <feGaussianBlur in="SourceGraphic" stdDeviation="7" result="b2"/>
      <feMerge><feMergeNode in="b2"/><feMergeNode in="b1"/><feMergeNode in="SourceGraphic"/></feMerge>
    </filter>
  </defs>
  <rect x="8" y="8" width="240" height="240" rx="36" fill="url(#rg)" opacity=".06"/>
  <rect x="42" y="32" width="48" height="192" rx="24" fill="url(#rg)" stroke="#0b1020" stroke-opacity=".16" stroke-width="1.5" filter="url(#glow)"/>
  <rect x="166" y="32" width="48" height="192" rx="24" fill="url(#rg)" stroke="#0b1020" stroke-opacity=".16" stroke-width="1.5" filter="url(#glow)"/>
  <rect x="90" y="108" width="76" height="40" rx="20" transform="rotate(-20 128 128)"
        fill="url(#rg)" stroke="#0b1020" stroke-opacity=".16" stroke-width="1.5" filter="url(#glow)"/>
</svg>
</div><div>HyperAI / $TITLE_RIGHT</div></div>
        <label class="switch" title="Тёмная тема">
          <span>Dark</span><input type="checkbox" id="themeToggle" aria-label="Сменить тему">
        </label>
      </div>
    """)
    return tpl.substitute(TITLE_RIGHT=_escape(title_right))


# ============================ CSRF ============================

def _ensure_csrf() -> str:
    tok = session.get("_csrf")
    if not tok:
        tok = base64.urlsafe_b64encode(os.urandom(24)).decode("ascii").rstrip("=")
        session["_csrf"] = tok
    return tok

def _validate_csrf(form_value: str | None) -> None:
    token = session.get("_csrf") or ""
    if not form_value or not hmac.compare_digest(form_value, token):
        abort(400, description="CSRF token invalid")


# ============================ Страницы (HTML) ============================

def _login_html(csrf: str) -> str:
    tpl = Template("""<!doctype html>
<html lang="ru" data-theme="light">
<head>
  <meta charset="utf-8" />
  <title>Вход — HyperAI</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>$BASE_CSS</style>
  <script>$THEME_JS</script>
</head>
<body>
  <div class="wrap">
    <div class="card" role="dialog" aria-label="Вход в систему">
      $HEADER
      <div class="body">
        <div class="title">Добро пожаловать</div>
        <div class="sub">Введите e-mail и пароль, чтобы продолжить.</div>

        <form class="form" method="post" action="/auth/login" autocomplete="on" novalidate>
          <input type="hidden" name="_csrf" value="$CSRF" />
          <input type="hidden" name="next" id="nextField" />
          <div class="row">
            <label class="label" for="email">E-mail</label>
            <input class="inp" id="email" type="email" name="email" placeholder="you@company.com" required autocomplete="email" inputmode="email"/>
          </div>
          <div class="row pw-wrap">
            <label class="label" for="pw">Пароль</label>
            <input class="inp" id="pw" type="password" name="password" placeholder="••••••••" required autocomplete="current-password" minlength="1"/>
            <button type="button" class="pw-toggle" id="pwToggle" aria-label="Показать пароль" title="Показать пароль">
              <!-- eye -->
              <svg id="iconEyeOpen" viewBox="0 0 24 24" fill="none" stroke-width="1.8">
                <path d="M2 12s4-7 10-7 10 7 10 7-4 7-10 7S2 12 2 12Z"/>
                <circle cx="12" cy="12" r="3"/>
              </svg>
              <!-- eye-off -->
              <svg id="iconEyeOff" class="hidden" viewBox="0 0 24 24" fill="none" stroke-width="1.8">
                <path d="M2 12s4-7 10-7c2.1 0 4 .6 5.6 1.5M22 12s-4 7-10 7c-2.1 0-4-.6-5.6-1.5"/>
                <circle cx="12" cy="12" r="3"/>
                <path d="M3 3L21 21"/>
              </svg>
            </button>
          </div>
          <div id="msg" aria-live="polite"></div>
          <div class="actions">
            <button class="btn primary" id="submitBtn" type="submit">Войти</button>
            <a class="btn" href="/auth/register">Регистрация</a>
          </div>
          <div class="note">Слишком много ошибок подряд — временная блокировка.</div>
        </form>
      </div>
      <div class="footer">
        <div class="help">Нужен доступ? Оставьте заявку на регистрацию.</div>
        <div>© HyperAi</div>
      </div>
    </div>
  </div>
  <script>
    const q = new URLSearchParams(location.search);
    const msg = document.getElementById('msg');
    const err = q.get('err'), ok = q.get('ok');
    if (err) msg.innerHTML = '<div class="err">'+ decodeURIComponent(err) +'</div>';
    if (ok)  msg.innerHTML = '<div class="ok">'+ decodeURIComponent(ok)  +'</div>';
    document.getElementById('nextField').value = q.get('next') || '/';

    // Переключатель видимости пароля
    const pw = document.getElementById('pw');
    const pwToggle = document.getElementById('pwToggle');
    const eyeOpen = document.getElementById('iconEyeOpen');
    const eyeOff  = document.getElementById('iconEyeOff');
    pwToggle.addEventListener('click', ()=>{
      const show = (pw.type === 'password');
      pw.type = show ? 'text' : 'password';
      eyeOpen.classList.toggle('hidden', show);
      eyeOff.classList.toggle('hidden', !show);
      pwToggle.setAttribute('aria-label', show ? 'Скрыть пароль' : 'Показать пароль');
      pwToggle.setAttribute('title', show ? 'Скрыть пароль' : 'Показать пароль');
    });

    const form = document.querySelector('form');
    const submitBtn = document.getElementById('submitBtn');
    form.addEventListener('submit', ()=>{
      submitBtn.setAttribute('disabled','disabled');
      setTimeout(()=> submitBtn.removeAttribute('disabled'), 5000);
    });

    const tog = document.getElementById('themeToggle');
    if (tog){
      tog.checked = (document.documentElement.getAttribute('data-theme')==='dark');
      tog.addEventListener('change', ()=> window.__setTheme(tog.checked ? 'dark':'light'));
    }
  </script>
</body>
</html>
""")
    return tpl.substitute(BASE_CSS=BASE_CSS, THEME_JS=THEME_JS, HEADER=_ui_header("Вход"), CSRF=_escape(csrf))

def _register_html(csrf: str) -> str:
    tpl = Template("""<!doctype html>
<html lang="ru" data-theme="light">
<head>
  <meta charset="utf-8" />
  <title>Регистрация — HyperAI</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>$BASE_CSS</style>
  <script>$THEME_JS</script>
</head>
<body>
  <div class="wrap">
    <div class="card" role="dialog" aria-label="Заявка на доступ">
      $HEADER
      <div class="body">
        <div class="title">Заявка на доступ</div>
        <div class="sub">Укажите имя, почту и пароль — админ проверит и включит доступ.</div>

        <form class="form" method="post" action="/auth/register" autocomplete="on" novalidate>
          <input type="hidden" name="_csrf" value="$CSRF" />
          <div class="row"><label class="label" for="name">Имя</label>
            <input class="inp" id="name" type="text" name="name" placeholder="Иван" required autocomplete="name"/></div>
          <div class="row"><label class="label" for="email">E-mail</label>
            <input class="inp" id="email" type="email" name="email" placeholder="you@company.com" required autocomplete="email"/></div>
          <div class="row pw-wrap"><label class="label" for="pw">Пароль</label>
            <input class="inp" id="pw" type="password" name="password" placeholder="минимум 8 символов" minlength="8" required autocomplete="new-password"/>
            <button type="button" class="pw-toggle" id="pwToggle" aria-label="Показать пароль" title="Показать пароль">
              <svg id="iconEyeOpen" viewBox="0 0 24 24" fill="none" stroke-width="1.8">
                <path d="M2 12s4-7 10-7 10 7 10 7-4 7-10 7S2 12 2 12Z"/>
                <circle cx="12" cy="12" r="3"/>
              </svg>
              <svg id="iconEyeOff" class="hidden" viewBox="0 0 24 24" fill="none" stroke-width="1.8">
                <path d="M2 12s4-7 10-7c2.1 0 4 .6 5.6 1.5M22 12s-4 7-10 7c-2.1 0-4-.6-5.6-1.5"/>
                <circle cx="12" cy="12" r="3"/>
                <path d="M3 3L21 21"/>
              </svg>
            </button>
          </div>

          <div id="msg" aria-live="polite"></div>
          <div class="actions">
            <button class="btn primary" id="submitBtn" type="submit">Отправить заявку</button>
            <a class="btn" href="/auth/login">У меня есть аккаунт</a>
          </div>
        </form>
        <div class="note" style="margin-top:10px">После активации сможете войти. Админ получит кнопки одобрения в боте.</div>
      </div>
      <div class="footer"><div class="help">Связь — через администратора.</div><div>© HyperAi</div></div>
    </div>
  </div>
  <script>
    const q = new URLSearchParams(location.search);
    const msg = document.getElementById('msg');
    const err = q.get('err'), ok = q.get('ok');
    if (err) msg.innerHTML = '<div class="err">'+ decodeURIComponent(err) +'</div>';
    if (ok)  msg.innerHTML = '<div class="ok">'+ decodeURIComponent(ok)  +'</div>';

    const pw = document.getElementById('pw');
    const pwToggle = document.getElementById('pwToggle');
    const eyeOpen = document.getElementById('iconEyeOpen');
    const eyeOff  = document.getElementById('iconEyeOff');
    pwToggle.addEventListener('click', ()=>{
      const show = (pw.type === 'password');
      pw.type = show ? 'text' : 'password';
      eyeOpen.classList.toggle('hidden', show);
      eyeOff.classList.toggle('hidden', !show);
      pwToggle.setAttribute('aria-label', show ? 'Скрыть пароль' : 'Показать пароль');
      pwToggle.setAttribute('title', show ? 'Скрыть пароль' : 'Показать пароль');
    });

    const form = document.querySelector('form');
    const submitBtn = document.getElementById('submitBtn');
    form.addEventListener('submit', (event)=>{
      const name  = document.getElementById('name').value.trim();
      const email = document.getElementById('email').value.trim();
      const pwd   = document.getElementById('pw').value;
      if (!name || !email || !pwd || pwd.length < 8){
        msg.innerHTML = '<div class="err">Проверьте поля — пароль не короче 8 символов.</div>';
        event.preventDefault();
        return false;
      }
      submitBtn.setAttribute('disabled','disabled');
      setTimeout(()=> submitBtn.removeAttribute('disabled'), 5000);
    });

    const tog = document.getElementById('themeToggle');
    if (tog){
      tog.checked = (document.documentElement.getAttribute('data-theme')==='dark');
      tog.addEventListener('change', ()=> window.__setTheme(tog.checked ? 'dark':'light'));
    }
  </script>
</body>
</html>
""")
    return tpl.substitute(BASE_CSS=BASE_CSS, THEME_JS=THEME_JS, HEADER=_ui_header("Регистрация"), CSRF=_escape(csrf))

def _profile_html(user: User) -> str:
    tpl = Template("""<!doctype html>
<html lang="ru" data-theme="light">
<head>
  <meta charset="utf-8" />
  <title>Профиль — HyperAI</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>$BASE_CSS</style>
  <script>$THEME_JS</script>
</head>
<body>
  <div class="wrap">
    <div class="card" role="dialog" aria-label="Профиль">
      $HEADER
      <div class="body">
        <div class="title">Привет, $NAME</div>
        <div class="sub">ID: <b>$ID</b> · Почта: $EMAIL · Статус: <b>$STATUS</b></div>
        <div class="actions" style="margin-top:10px">
          <a class="btn" href="/auth/logout">Выйти</a>
        </div>
      </div>
      <div class="footer"><div class="help">Безопасность прежде всего.</div><div>© HyperAi</div></div>
    </div>
  </div>
  <script>
    const tog = document.getElementById('themeToggle');
    if (tog){
      tog.checked = (document.documentElement.getAttribute('data-theme')==='dark');
      tog.addEventListener('change', ()=> window.__setTheme(tog.checked ? 'dark':'light'));
    }
  </script>
</body>
</html>
""")
    status = "активирован ✅" if user.approved else "ожидает активации"
    return tpl.substitute(
        BASE_CSS=BASE_CSS, THEME_JS=THEME_JS, HEADER=_ui_header("Профиль"),
        NAME=_escape(user.name), EMAIL=_escape(user.email), STATUS=_escape(status), ID=str(user.id)
    )

def _simple_page(title: str, text_html: str, bad: bool=False) -> str:
    color = "var(--err)" if bad else "var(--ok)"
    tpl = Template("""<!doctype html>
<html lang="ru" data-theme="light">
<head>
  <meta charset="utf-8" />
  <title>$TITLE — HyperAI</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>$BASE_CSS</style>
  <script>$THEME_JS</script>
</head>
<body>
  <div class="wrap">
    <div class="card" role="dialog" aria-label="$TITLE">
      $HEADER
      <div class="body">
        <div class="title" style="color:$COLOR">$TITLE</div>
        <div class="sub">$TEXT_HTML</div>
        <div class="actions" style="margin-top:16px">
          <a class="btn primary" href="/auth/login">Ко входу</a>
        </div>
      </div>
      <div class="footer"><div class="help">Можно закрыть окно.</div><div>© HyperAi</div></div>
    </div>
  </div>
  <script>
    const tog = document.getElementById('themeToggle');
    if (tog){
      tog.checked = (document.documentElement.getAttribute('data-theme')==='dark');
      tog.addEventListener('change', ()=> window.__setTheme(tog.checked ? 'dark':'light'));
    }
  </script>
</body>
</html>
""")
    return tpl.substitute(
        TITLE=_escape(title), BASE_CSS=BASE_CSS, THEME_JS=THEME_JS, HEADER=_ui_header(title),
        COLOR=color, TEXT_HTML=text_html
    )


# ============================ Вспомогательные ============================

def _client_ip() -> str:
    fwd = request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
    return fwd or (request.remote_addr or "")

def _email_like(s: str) -> bool:
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", s or ""))

def _regenerate_session() -> None:
    keep = {k: v for k, v in session.items() if k.startswith("_")}
    session.clear()
    session.update(keep)

def _security_headers(resp: Response) -> Response:
    # CSP: в модуле есть inline-стили/скрипты → 'unsafe-inline'. Для прод — вынести в статику.
    resp.headers.setdefault("Content-Security-Policy", "default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; base-uri 'self'; frame-ancestors 'none'")
    resp.headers.setdefault("X-Frame-Options", "DENY")
    resp.headers.setdefault("Referrer-Policy", "no-referrer")
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("Permissions-Policy", "geolocation=(), camera=(), microphone=()")
    if request.is_secure or request.headers.get("X-Forwarded-Proto", "") == "https":
        resp.headers.setdefault("Strict-Transport-Security", "max-age=15552000; includeSubDomains")
    return resp


_TG_POLLER_STARTED = False  # sentinel


# ============================ Инициализация/маршруты ============================

def init_auth(app: Flask, settings: Settings) -> None:
    """
    Регистрирует SQLite, (опционально) фонового TelegramPoller и все маршруты /auth/*.
    Любой не-/auth запрос без сессии → редирект на /auth/login.
    """
    # Flask session secret
    app.secret_key = os.environ.get("WEB_SECRET_KEY") or app.secret_key or secrets.token_hex(32)

    # Cookie-флаги
    app.config.update(
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE=os.environ.get("WEB_SESSION_SAMESITE", "Lax"),
        SESSION_COOKIE_SECURE=(os.environ.get("WEB_SESSION_SECURE", "1") == "1"),
        PERMANENT_SESSION_LIFETIME=60*60*6,  # 6 часов
        SESSION_REFRESH_EACH_REQUEST=True,
    )

    # DB path
    db_path = settings.paths.artifacts_root / "auth.db"
    db = AuthDB(db_path)

    # интеграции
    integ = read_integrations(settings)
    bot_token = integ["tg_bot_token"]
    admin_chat_id = integ["tg_admin_chat_id"]

    # ---------------- Telegram long poller (если задан токен) ----------------
    global _TG_POLLER_STARTED
    if bot_token and not _TG_POLLER_STARTED:
        # не запускаем двойной поток при debug reloader
        if not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
            poller = TelegramPoller(db, bot_token=bot_token, admin_chat_id=admin_chat_id)
            poller.start()
            app.config["_tg_poller"] = poller
            _TG_POLLER_STARTED = True
            log.info("TG poller attached to app.")
    else:
        if not bot_token:
            log.info("TG_BOT_TOKEN not set — Telegram features disabled.")

    # ---------------- Security gate ----------------
    @app.before_request
    def _auth_gate() -> Optional[Response]:
        path = request.path or "/"
        public = path.startswith("/auth/") or path.startswith("/static/") or path in ("/favicon.ico", "/robots.txt")
        if public:
            return None
        if session.get("user_email"):
            return None
        nxt = request.full_path if request.query_string else request.path
        return redirect(url_for("auth_login", next=nxt))

    @app.after_request
    def _after(resp: Response) -> Response:
        return _security_headers(resp)

    # ---------------- Pages /auth ----------------

    @app.get("/auth/login")
    def auth_login() -> Response:
        csrf = _ensure_csrf()
        return make_response(_login_html(csrf))

    @app.post("/auth/login")
    def auth_login_post() -> Response:
        _validate_csrf(request.form.get("_csrf"))
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""
        next_url = request.form.get("next") or "/"
        ip = _client_ip()

        if not _email_like(email) or not password:
            return redirect(url_for("auth_login", err="Неверные данные", next=next_url))

        # rate-limit: 8 неудачных попыток за 10 минут → временная блокировка
        if db.count_recent_failed(ip, email, window_sec=600) >= 8:
            return redirect(url_for("auth_login", err="Слишком много попыток. Попробуйте позже.", next=next_url))

        u = db.get_user_by_email(email)
        if not u:
            db.add_login_attempt(ip, email, ok=False)
            return redirect(url_for("auth_login", err="Пользователь не найден", next=next_url))
        if not verify_password(password, u.pw_salt_hex, u.pw_hash_hex):
            db.add_login_attempt(ip, email, ok=False)
            return redirect(url_for("auth_login", err="Неверный пароль", next=next_url))
        if not u.approved:
            db.add_login_attempt(ip, email, ok=False)
            return redirect(url_for("auth_login", err="Доступ ещё не активирован", next=next_url))

        # OK — логиним
        _regenerate_session()
        session["_csrf"] = _ensure_csrf()
        session["user_id"] = u.id
        session["user_email"] = u.email
        session["user_name"] = u.name
        db.add_login_attempt(ip, email, ok=True)
        db.update_last_login(u.id)
        return redirect(next_url or "/")

    @app.get("/auth/logout")
    def auth_logout() -> Response:
        session.clear()
        return redirect(url_for("auth_login", ok="Вы вышли из системы"))

    @app.get("/auth/register")
    def auth_register() -> Response:
        csrf = _ensure_csrf()
        return make_response(_register_html(csrf))

    @app.post("/auth/register")
    def auth_register_post() -> Response:
        _validate_csrf(request.form.get("_csrf"))
        name = (request.form.get("name") or "").strip()
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""

        if not name or not _email_like(email) or len(password) < 8:
            return redirect(url_for("auth_register", err="Проверьте поля — пароль ≥ 8 символов"))

        existing = db.get_user_by_email(email)
        if existing:
            if existing.approved:
                return redirect(url_for("auth_login", ok=f"Аккаунт уже существует — войдите (ID {existing.id})", next="/"))
            else:
                return redirect(url_for("auth_login", ok=f"Заявка уже создана (ID {existing.id}). Ожидайте активации.", next="/"))

        salt_hex, hash_hex = make_password(password)
        approval_token = secrets.token_urlsafe(24)
        user_id = db.insert_pending_user(
            name=name,
            email=email,
            pw_salt_hex=salt_hex,
            pw_hash_hex=hash_hex,
            approval_token=approval_token
        )

        # Уведомим админа (если бот запущен)
        sent = False
        poller: Optional[TelegramPoller] = app.config.get("_tg_poller")  # type: ignore
        if poller:
            try:
                sent = bool(poller.send_admin_request(name=name, email=email, approval_token=approval_token, user_id=user_id))
            except Exception as e:
                log.error("Failed to send admin request: %s", e)

        if sent:
            ok_msg = f"Заявка создана (ID {user_id}) и отправлена администратору. Доступ будет активирован после проверки."
        else:
            ok_msg = (
                f"Заявка создана (ID {user_id}). "
                "Уведомление администратору не доставлено — проверьте TG_BOT_TOKEN/TG_ADMIN_CHAT_ID и напишите /whoami боту."
            )

        return redirect(url_for("auth_login", ok=ok_msg))

    @app.get("/auth/approve")
    def auth_approve() -> Response:
        # Доп. ручка: вдруг понадобится клик из браузера
        token = request.args.get("token") or ""
        u = db.approve_user_by_token(token, approve=True)
        if not u:
            return make_response(_simple_page("Подтверждение", "Токен не найден или уже использован.", bad=True))
        return make_response(_simple_page("Подтверждение", f"Пользователь <b>{_escape(u.email)}</b> (ID {u.id}) получил доступ ✅"))

    @app.get("/auth/deny")
    def auth_deny() -> Response:
        token = request.args.get("token") or ""
        u = db.approve_user_by_token(token, approve=False)
        if not u:
            return make_response(_simple_page("Отклонение", "Токен не найден или уже использован.", bad=True))
        return make_response(_simple_page("Отклонение", f"Заявка <b>{_escape(u.email)}</b> (ID {u.id}) отклонена ❌"))

    @app.get("/auth/profile")
    def auth_profile() -> Response:
        email = session.get("user_email") or ""
        u = db.get_user_by_email(email) if email else None
        if not u:
            return redirect(url_for("auth_login"))
        return make_response(_profile_html(u))

    # ---------------- Healthcheck / Debug / API ----------------
    @app.get("/auth/health")
    def auth_health() -> Response:
        return jsonify({"ok": True})

    @app.get("/auth/tg/status")
    def tg_status() -> Response:
        off = db.get_kv("tg_update_offset")
        return jsonify({
            "poller_started": bool(app.config.get("_tg_poller")),
            "update_offset": off,
            "admin_chat_id": admin_chat_id or "",
        })

    @app.get("/auth/me")
    def auth_me() -> Response:
        """Удобная JSON-ручка для фронта: возвращает текущего пользователя."""
        if not session.get("user_email"):
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        return jsonify({
            "ok": True,
            "id": session.get("user_id"),
            "email": session.get("user_email"),
            "name": session.get("user_name"),
        })
