# ads_ai/web/list_companies.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import importlib
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, List, Optional, Tuple

from flask import Flask, Response, jsonify, make_response, request, session

# Проектный Settings — мягкий импорт (как в create_companies.py)
try:
    from ads_ai.config.settings import Settings  # noqa: F401
except Exception:  # pragma: no cover
    class Settings:  # простая заглушка
        pass

# Удаление кампаний в GAds
from ads_ai.web.bulk_remove import remove_campaigns_by_names, init_bulk_remove  # type: ignore


# =============================================================================
#                                ПУТЬ К БД
# =============================================================================

def _db_path() -> str:
    """
    ДОЛЖЕН совпадать с create_companies.py:
      ADS_AI_DB или ./ads_ai_data/companies.sqlite3
    """
    path = (os.getenv("ADS_AI_DB") or "").strip()
    if not path:
        base = os.path.abspath(os.path.join(os.getcwd(), "ads_ai_data"))
        os.makedirs(base, exist_ok=True)
        path = os.path.join(base, "companies.sqlite3")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path


def _require_user_email() -> str:
    email = session.get("user_email")
    if not email:
        raise PermissionError("unauthorized")
    return str(email)


def _get_app_state() -> Any:
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


def _acquire_shared_driver(timeout: float = 25.0) -> Tuple[Optional[Tuple[Any, Any, Any]], Optional[str]]:
    """
    Пытаемся занять общий WebDriver из консоли (/console). Возвращаем (driver, state, lock).
    Вторая компонента — строковый код ошибки при неудаче.
    """
    state = _get_app_state()
    if not state:
        return None, "app_state_missing"
    driver = getattr(state, "driver", None)
    lock = getattr(state, "lock", None)
    if driver is None or lock is None:
        return None, "driver_unavailable"
    lock_wait = max(0.1, float(timeout))
    try:
        acquired = lock.acquire(timeout=lock_wait)
    except Exception:
        return None, "driver_lock_error"
    if not acquired:
        return None, "driver_lock_timeout"

    busy_wait_until = time.time() + min(lock_wait, 15.0)
    stale_after = getattr(state, "busy_stale_sec", 180.0)
    while getattr(state, "busy", False):
        busy_since = float(getattr(state, "busy_since", 0.0) or 0.0)
        now = time.time()
        if busy_since and stale_after > 0 and (now - busy_since) > stale_after:
            try:
                state.busy = False
                setattr(state, "busy_since", 0.0)
            except Exception:
                pass
            break
        if now >= busy_wait_until:
            try:
                lock.release()
            except Exception:
                pass
            return None, "driver_busy"
        remaining = max(0.05, busy_wait_until - now)
        try:
            lock.release()
        except Exception:
            pass
        time.sleep(min(0.4, remaining))
        try:
            reacquired = lock.acquire(timeout=remaining)
        except Exception:
            return None, "driver_lock_error"
        if not reacquired:
            return None, "driver_lock_timeout"
    try:
        state.busy = True
        setattr(state, "busy_since", time.time())
    except Exception:
        pass
    return (driver, state, lock), None


def _release_shared_driver(holder: Optional[Tuple[Any, Any, Any]]) -> None:
    if not holder:
        return
    _, state, lock = holder
    try:
        setattr(state, "busy", False)
        setattr(state, "busy_since", 0.0)
    except Exception:
        pass
    try:
        lock.release()
    except Exception:
        pass


# =============================================================================
#                                ДАННЫЕ / БД
# =============================================================================

@dataclass
class CompanyRow:
    id: int
    created_at: str
    status: str
    profile_id: str

    business_name: str
    website_url: str
    campaign_type: str
    budget_display: str

    locations: str
    languages: str
    n_ads: int

    creatives_summary: str
    google_account: str  # email или CID из extra_json (если найдено)

    # сырое содержимое для детальной/отладки (не рендерится в таблице)
    raw: Dict[str, Any]


class CompanyDB:
    """
    Читаем БД с той же схемой, что использует create_companies.py.
    Таблица: companies (см. _db_init() внутри create_companies.py).
    """
    def __init__(self, path: Optional[str] = None):
        self.path = path or _db_path()
        self._ensure_min_schema()

    def _connect(self) -> sqlite3.Connection:
        cx = sqlite3.connect(self.path, check_same_thread=False, timeout=30.0)
        cx.row_factory = sqlite3.Row
        return cx

    def _ensure_min_schema(self) -> None:
        """
        Не меняем существующую схему; создаём при отсутствии —
        совместимо с create_companies.py.
        """
        with self._connect() as cx:
            cx.execute("""
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
            cx.execute("CREATE INDEX IF NOT EXISTS idx_companies_created_at ON companies(created_at)")
            cx.execute("CREATE INDEX IF NOT EXISTS idx_companies_profile ON companies(profile_id)")
            cx.execute("CREATE INDEX IF NOT EXISTS idx_companies_user_email ON companies(user_email, created_at)")
            cx.commit()
            try:
                cols = {str(r[1]).lower() for r in cx.execute("PRAGMA table_info(companies)").fetchall()}
                if "user_email" not in cols:
                    cx.execute("ALTER TABLE companies ADD COLUMN user_email TEXT")
                if "google_tags" not in cols:
                    cx.execute("ALTER TABLE companies ADD COLUMN google_tags TEXT")
                if "google_tag" not in cols:
                    cx.execute("ALTER TABLE companies ADD COLUMN google_tag TEXT")
                cx.commit()
            except Exception:
                pass

    # ---------- utils ----------

    @staticmethod
    def _try_json(v: Any, default: Any) -> Any:
        if v is None:
            return default
        if isinstance(v, (list, dict)):
            return v
        if isinstance(v, (bytes, bytearray)):
            try:
                return json.loads(v.decode("utf-8"))
            except Exception:
                return default
        if isinstance(v, str):
            v2 = v.strip()
            if v2.startswith("{") or v2.startswith("["):
                try:
                    return json.loads(v2)
                except Exception:
                    return default
            return default
        return default

    @staticmethod
    def _list2str(vals: Any) -> str:
        if not vals:
            return "—"
        if isinstance(vals, str):
            return vals
        try:
            return ", ".join([str(x) for x in vals if x is not None])
        except Exception:
            return str(vals)

    @staticmethod
    def _extract_google_account(extra: Dict[str, Any]) -> str:
        """
        Выдёргиваем email/CID Google Ads из extra.context, если есть.
        Ключи перебираем широким списком, чтобы быть совместимыми с шагами.
        """
        ctx = extra.get("context") if isinstance(extra, dict) else {}
        candidates = []
        for k in (
            "google_email", "ga_email", "google_account_email", "account_email",
            "google_customer_id", "ga_customer_id", "customer_id", "customerId", "cid", "account_id"
        ):
            v = ctx.get(k)
            if isinstance(v, (str, int)):
                candidates.append(str(v))
        seen = set()
        out: List[str] = []
        for x in candidates:
            if x and x not in seen:
                seen.add(x)
                out.append(x)
        return " / ".join(out) if out else ""

    @staticmethod
    def _count(v: Any) -> int:
        if isinstance(v, (list, tuple)):
            return len(v)
        return 0

    def _compose(self, d: Dict[str, Any]) -> CompanyRow:
        # JSON-поля
        locations = self._try_json(d.get("locations"), [])
        languages = self._try_json(d.get("languages"), [])

        headlines = self._try_json(d.get("headlines_json"), [])
        long_headlines = self._try_json(d.get("long_headlines_json"), [])
        descriptions = self._try_json(d.get("descriptions_json"), [])
        images = self._try_json(d.get("images_json"), [])
        google_tags = self._try_json(d.get("google_tags"), [])

        extra = self._try_json(d.get("extra_json"), {}) or {}
        google_account = self._extract_google_account(extra)

        # Вычисляем креативную сводку
        crea = f"H{self._count(headlines)}/L{self._count(long_headlines)}/D{self._count(descriptions)}/IMG{self._count(images)}"

        # Бюджет показываем «как есть» (в БД он TEXT). При желании можно добавить валюту.
        budget_display = d.get("budget_per_day") or "—"

        return CompanyRow(
            id=int(d.get("id") or 0),
            created_at=str(d.get("created_at") or ""),
            status=str(d.get("status") or "—"),
            profile_id=str(d.get("profile_id") or ""),

            business_name=str(d.get("business_name") or "—"),
            website_url=str(d.get("site_url") or "—"),
            campaign_type=str(d.get("campaign_type") or "—"),
            budget_display=budget_display,

            locations=self._list2str(locations),
            languages=self._list2str(languages),
            n_ads=int(d.get("n_ads") or 0),

            creatives_summary=crea,
            google_account=google_account,

            raw={
                **d,
                "_parsed": {
                    "locations": locations,
                    "languages": languages,
                    "headlines": headlines,
                    "long_headlines": long_headlines,
                    "descriptions": descriptions,
                    "images": images,
                    "google_tags": google_tags,
                    "extra": extra,
                }
            }
        )

    # ---------- публичные методы ----------

    def get(self, company_id: int, user_email: str) -> Optional[CompanyRow]:
        with self._connect() as cx:
            cur = cx.execute(
                "SELECT * FROM companies WHERE id = ? AND user_email = ? LIMIT 1",
                (int(company_id), user_email),
            )
            r = cur.fetchone()
            return self._compose(dict(r)) if r else None

    def get_many(self, user_email: str, ids: Iterable[int]) -> List[CompanyRow]:
        ids_list = [int(x) for x in ids if str(x).strip()]
        if not ids_list:
            return []
        placeholders = ",".join(["?"] * len(ids_list))
        with self._connect() as cx:
            cur = cx.execute(
                f"SELECT * FROM companies WHERE user_email = ? AND id IN ({placeholders})",
                [user_email, *ids_list],
            )
            rows = cur.fetchall()
        return [self._compose(dict(r)) for r in rows]

    def query(
        self,
        user_email: str,
        *,
        q: str = "",
        page: int = 1,
        page_size: int = 50,
        sort: str = "created_at",
        direction: str = "desc",
        profile_id: Optional[str] = None,
        status: Optional[str] = None,
    ) -> Tuple[List[CompanyRow], int]:
        page = max(1, int(page))
        page_size = min(200, max(1, int(page_size)))
        offset = (page - 1) * page_size

        sort_allowed = {"created_at", "business_name", "site_url", "campaign_type", "n_ads", "status", "profile_id", "id"}
        sort_col = sort if sort in sort_allowed else "created_at"
        dir_sql = "ASC" if str(direction).lower() in ("asc", "up") else "DESC"

        where: List[str] = ["user_email = ?"]
        params: List[Any] = [user_email]

        if q:
            ql = f"%{q.lower().strip()}%"
            where.append(
                "(lower(coalesce(business_name,'')) LIKE ? OR lower(coalesce(site_url,'')) LIKE ? "
                "OR lower(coalesce(campaign_type,'')) LIKE ? OR lower(coalesce(profile_id,'')) LIKE ?)"
            )
            params += [ql, ql, ql, ql]

        if profile_id:
            where.append("profile_id = ?")
            params.append(profile_id.strip())

        if status:
            where.append("status = ?")
            params.append(status.strip())

        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        with self._connect() as cx:
            total = int(cx.execute(f"SELECT COUNT(*) FROM companies {where_sql}", params).fetchone()[0])
            cur = cx.execute(
                f"SELECT * FROM companies {where_sql} ORDER BY {sort_col} {dir_sql} LIMIT ? OFFSET ?",
                [*params, page_size, offset]
            )
            rows = [self._compose(dict(r)) for r in cur.fetchall()]
        return rows, total

    def delete_many(self, user_email: str, ids: Iterable[int]) -> int:
        ids = [int(x) for x in ids if str(x).strip()]
        if not ids:
            return 0
        placeholders = ",".join(["?"] * len(ids))
        with self._connect() as cx:
            cur = cx.execute(
                f"DELETE FROM companies WHERE user_email = ? AND id IN ({placeholders})",
                [user_email, *ids],
            )
            cx.commit()
            return int(cur.rowcount or 0)


def _derive_campaign_name(row: CompanyRow) -> str:
    raw = row.raw if isinstance(row.raw, dict) else {}
    parsed = raw.get("_parsed") if isinstance(raw, dict) else {}
    if not isinstance(parsed, dict):
        parsed = {}
    extra = parsed.get("extra") if isinstance(parsed, dict) else {}
    if not isinstance(extra, dict):
        extra = {}
    gads_import = extra.get("gads_import") if isinstance(extra, dict) else {}
    if not isinstance(gads_import, dict):
        gads_import = {}

    candidates = [
        getattr(row, "business_name", None),
        raw.get("asset_group_name"),
        gads_import.get("gads_campaign_name"),
        getattr(row, "website_url", None),
    ]
    for cand in candidates:
        name = str(cand or "").strip()
        if name:
            return name
    return ""


# =============================================================================
#                           ВСПОМ.: AdsPower профили
# =============================================================================

def _adsp_env() -> tuple[str, str]:
    base = (os.getenv("ADSP_API_BASE") or "http://local.adspower.net:50325").rstrip("/")
    token = os.getenv("ADSP_API_TOKEN") or ""
    if not base.startswith("http"):
        base = "http://" + base
    return base, token


def _http_get_json(url: str, headers: Optional[Dict[str, str]] = None, timeout: float = 5.0) -> Tuple[int, Dict[str, Any]]:
    try:
        import requests  # type: ignore
    except Exception:
        requests = None  # type: ignore
    if requests:
        try:
            r = requests.get(url, headers=headers or {}, timeout=timeout)  # type: ignore
            return int(r.status_code), (r.json() if r.content else {})
        except Exception:
            return 0, {}
    # stdlib fallback
    import urllib.request, json as _json  # noqa: E401
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
            return int(resp.getcode() or 0), (_json.loads(data.decode("utf-8")) if data else {})
    except Exception:
        return 0, {}


def _list_adspower_profiles(q: str = "", page: int = 1, page_size: int = 300) -> Dict[str, Any]:
    base, token = _adsp_env()
    headers = {"Authorization": token} if token else {}
    url = f"{base}/api/v1/user/list?page={int(page)}&page_size={int(page_size)}"
    code, body = _http_get_json(url, headers=headers, timeout=6.0)
    items: List[Dict[str, Any]] = []
    total = 0
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
            items.append({
                "profile_id": str(pid),
                "name": str(name),
                "group_id": str(grp or ""),
                "tags": it.get("tags") or [],
            })
    # Поиск по строке q
    ql = q.strip().lower()
    if ql:
        items = [x for x in items if ql in x["profile_id"].lower() or ql in (x["name"] or "").lower() or ql in (x["group_id"] or "").lower()]
    return {"items": items, "total": total}


# =============================================================================
#                                   HTML (UI)
# =============================================================================

PAGE_HTML = """<!doctype html>
<html lang="ru" data-theme="light">
<head>
  <meta charset="utf-8" />
  <title>HyperAI — Список компаний</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
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
    html,body{height:100%;margin:0;color:var(--text);font:14px/1.45 Inter,system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;-webkit-font-smoothing:antialiased}
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
    .mitem.active{ outline:2px solid rgba(56,189,248,.25) }
    .stage{ position:relative; display:grid; grid-template-rows: auto 1fr auto; gap:14px; padding:18px; }

    .toolbar{ display:flex; gap:10px; align-items:center; justify-content:space-between; padding:8px 12px 4px }
    .left, .right{ display:flex; gap:8px; align-items:center; flex-wrap:wrap }
    .inp,.sel{ padding:8px 12px; border-radius:12px; border:1px solid var(--border); background:rgba(255,255,255,.9); color:var(--text) }
    html[data-theme="dark"] .inp, html[data-theme="dark"] .sel{ background:rgba(13,18,30,.7) }
    .btn{ border:1px solid var(--border); background: linear-gradient(180deg, #fff, #f4f7fb);
          color:var(--text); border-radius: 999px; padding:8px 14px; cursor:pointer; transition: transform .08s ease, box-shadow .25s ease }
    .btn:hover{ transform: translateY(-1px); box-shadow: 0 10px 30px rgba(15,23,42,.15) }
    .btn.primary{ background: linear-gradient(180deg, var(--neon1), var(--neon2)); color:#00131a; font-weight:800 }
    .btn.ghost{ background:transparent }

    .table-wrap{ padding:0 12px 12px; overflow:auto }
    table{ width:100%; border-collapse:collapse; }
    th,td{ padding:10px 10px; border-bottom:1px solid var(--border); text-align:left; white-space:nowrap; }
    th{ font-size:12px; color:var(--muted); text-transform:uppercase; letter-spacing:.04em }
    td a{ color:inherit; text-decoration:none; }
    td a:hover{ text-decoration:underline; }

    .ctr{ text-align:center }
    .wmin{ width:34px }
    .muted{ color:var(--muted) }
    .status-ok{ color:var(--ok); font-weight:700 }
    .status-failed{ color:var(--err); font-weight:700 }
    .status-other{ color:var(--warn); font-weight:700 }

    .pager{ display:flex; gap:8px; align-items:center; justify-content:flex-end; padding:8px 12px 14px }
    .tiny{ font-size:12px; color:var(--muted) }
    .sync-overlay{ position:fixed; top:18px; right:18px; z-index:999; display:flex; align-items:flex-start; justify-content:flex-end; transition:opacity .25s ease, transform .25s ease; }
    html[data-theme="dark"] .sync-overlay{ background:transparent; }
    .sync-overlay.hide{ opacity:0; transform:translateY(-6px); }
    .sync-box{ pointer-events:auto; display:flex; flex-direction:column; align-items:flex-start; gap:10px; padding:18px 22px; border-radius:16px; border:1px solid var(--border); background:var(--glass); box-shadow:var(--shadow); min-width:240px; max-width:280px; }
    html[data-theme="dark"] .sync-box{ background:rgba(17,23,41,.82); }
    .sync-spinner{ width:32px; height:32px; border-radius:50%; border:3px solid rgba(0,0,0,.1); border-top-color:var(--neon1); animation:spin 1s linear infinite; }
    html[data-theme="dark"] .sync-spinner{ border:3px solid rgba(255,255,255,.08); border-top-color:var(--neon2); }
    .sync-title{ font-weight:700; font-size:14px; }
    .sync-note{ font-size:12px; color:var(--muted); }
    .sync-overlay.error .sync-title{ color:var(--err); }
    .sync-overlay.error .sync-spinner{ border-top-color:var(--err); }
    .sync-actions{ display:flex; gap:8px; }
    .btn.sm{ padding:6px 10px; font-size:12px; }
    @keyframes spin{ to{ transform:rotate(360deg); } }
  </style>
</head>
<body>
  <div id="syncOverlay" class="sync-overlay" role="status" aria-live="polite">
    <div class="sync-box">
      <div class="sync-spinner" aria-hidden="true"></div>
      <div class="sync-title" id="syncOverlayTitle">Синхронизирую данные, ожидайте…</div>
      <div class="sync-note" id="syncOverlayNote"></div>
      <div class="sync-actions">
        <button class="btn ghost sm" id="syncOverlayClose" type="button">Скрыть</button>
      </div>
    </div>
  </div>
  <div class="shell">
    <!-- LEFT -->
    <aside class="panel menu">
      <div class="head">
        <div style="width:36px;height:36px;border-radius:12px;background:linear-gradient(135deg,var(--neon1),var(--neon2))"></div>
        <div>Меню</div>
      </div>
      <a class="mitem" href="/">Главная</a>
      <a class="mitem active" href="/companies/list">Компании</a>
      <a class="mitem" href="/accounts">Аккаунты</a>
      <div style="margin-top:auto" class="muted">Powered by EasyByte</div>
    </aside>

    <!-- CENTER -->
    <section class="panel stage">
      <div class="toolbar">
        <div class="left">
          <input id="search" class="inp" placeholder="Поиск: название / URL / тип / профиль…" style="min-width:340px" />
          <select id="profileFilter" class="sel">
            <option value="">Все профили (AdsPower)</option>
          </select>
          <select id="statusFilter" class="sel">
            <option value="">Любой статус</option>
            <option value="ok">ok</option>
            <option value="failed">failed</option>
          </select>
          <button class="btn" id="searchBtn">Найти</button>
          <button class="btn ghost" id="resetBtn">Сброс</button>
        </div>
        <div class="right">
          <a class="btn primary" id="createBtn" href="/companies/new">Создать компанию</a>
          <button class="btn" id="refreshBtn">Обновить</button>
          <button class="btn primary" id="deleteBtn" disabled>Удалить выбранные</button>
        </div>
      </div>

      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th class="wmin"><input id="selAll" type="checkbox"/></th>
              <th>ID</th>
              <th>Название</th>
              <th>URL</th>
              <th>Тип</th>
              <th>Бюджет</th>
              <th>Локации</th>
              <th>Языки</th>
              <th class="ctr">Объявл.</th>
              <th>Креативы</th>
              <th>AdsPower</th>
              <th>Google Ads</th>
              <th>Создано</th>
              <th>Статус</th>
            </tr>
          </thead>
          <tbody id="tbody"></tbody>
        </table>
      </div>

      <div class="pager">
        <div class="tiny" id="count"></div>
        <div style="flex:1"></div>
        <button class="btn" id="prevBtn">Назад</button>
        <div class="tiny" id="pageInfo"></div>
        <button class="btn" id="nextBtn">Вперёд</button>
      </div>
    </section>
  </div>

<script>
const $ = (s)=>document.querySelector(s);
const tbody = $("#tbody");
const selAll = $("#selAll");
const deleteBtn = $("#deleteBtn");
const countEl = $("#count");
const pageInfo = $("#pageInfo");
const searchEl = $("#search");
const profileFilter = $("#profileFilter");
const statusFilter = $("#statusFilter");
const syncOverlay = $("#syncOverlay");
const syncOverlayTitle = $("#syncOverlayTitle");
const syncOverlayNote = $("#syncOverlayNote");
const syncOverlayClose = $("#syncOverlayClose");

let state = {
  q: "",
  page: 1,
  page_size: 20,
  sort: "created_at",
  dir: "desc",
  total: 0,
  items: [],
  selected: new Set(),          // набор выбранных ID
  selectedNames: new Map(),     // id -> name (имя кампании для GAds)
  selectedProfiles: new Map(),  // id -> profile_id (запоминается при выборе)
  nameById: {},                 // кэш имён по текущей странице
  profileById: {},              // кэш profile_id по текущей странице
  profile_id: "",
  status: "",
  profilesMap: {},              // profile_id -> name
};

let syncOverlayDismissed = false;
let syncInProgress = false;

function setSyncState(state, note){
  if(!syncOverlay || !syncOverlayTitle || !syncOverlayNote){
    return;
  }
  if(syncOverlayDismissed){
    return;
  }
  syncOverlay.classList.remove("hide");
  if(state !== "error"){
    syncOverlay.classList.remove("error");
  }
  if(state === "running"){
    syncOverlayTitle.textContent = "Синхронизирую данные, ожидайте…";
  } else if (state === "done"){
    syncOverlayTitle.textContent = "Синхронизация завершена";
  } else if (state === "error"){
    syncOverlay.classList.add("error");
    syncOverlayTitle.textContent = "Не удалось синхронизировать данные";
  }
  syncOverlayNote.textContent = note ? String(note) : "";
}

function hideSyncOverlay(delay = 900, force = false){
  if(!syncOverlay){
    return;
  }
  if(syncOverlayDismissed && !force){
    return;
  }
  window.setTimeout(()=>{
    syncOverlay.classList.add("hide");
  }, Math.max(0, delay));
}

if(syncOverlayClose){
  syncOverlayClose.addEventListener("click", ()=>{
    syncOverlayDismissed = true;
    hideSyncOverlay(0, true);
  });
}

async function runInitialSync(){
  syncInProgress = true;
  setSyncState("running", "");
  try{
    const resp = await fetch("/api/gads/campaigns/sync_all", { cache: "no-store" });
    if(!resp.ok){
      const text = await resp.text().catch(()=> "");
      throw new Error(text || ("HTTP " + resp.status));
    }
    let data = null;
    try{
      data = await resp.json();
    }catch(_){
      data = null;
    }
    if(!data || data.ok !== true){
      const errMsg = data && data.error ? data.error : "Сервер вернул ошибку";
      throw new Error(errMsg);
    }
    const profilesArr = Array.isArray(data.profiles) ? data.profiles : [];
    const total = typeof data.total === "number" ? data.total : profilesArr.length;
    const okCount = typeof data.ok_count === "number" ? data.ok_count : total;
    const resultsArr = Array.isArray(data.results) ? data.results : [];
    const failed = resultsArr.filter((it)=> it && it.ok === false);
    let note = "";
    if(total > 0){
      note = "Обновлено профилей: " + okCount + " из " + total;
      if(failed.length > 0){
        note += " (ошибки: " + failed.length + ")";
      }
    } else {
      note = "Нет профилей для синхронизации";
    }
    if(failed.length > 0){
      console.warn("[companies-list] sync_all partial errors", failed);
    }
    syncInProgress = false;
    setSyncState("done", note);
    hideSyncOverlay(1200);
  }catch(err){
    console.error("[companies-list] sync_all failed", err);
    const message = err && err.message ? err.message : "Неизвестная ошибка";
    syncInProgress = false;
    setSyncState("error", message);
    hideSyncOverlay(2500);
  }
}

function fmtDate(iso){
  if(!iso) return "—";
  const d = new Date(iso);
  if (isNaN(d.getTime())) return iso;
  return d.toLocaleString();
}

function esc(s){
  return String(s||"")
    .replaceAll("&","&amp;")
    .replaceAll("<","&lt;")
    .replaceAll(">","&gt;");
}

function render(){
  const rows = state.items || [];
  const map = state.profilesMap || {};
  state.nameById = {};     // перезаполняем для текущей страницы
  state.profileById = {};  // перезаполняем для текущей страницы
  tbody.innerHTML = "";
  for(const it of rows){
    const pid = it.profile_id || "";
    const pname = map[pid] ? map[pid] + " — " + pid : pid;
    const st = String(it.status||"").toLowerCase();
    const stClass = st === "ok" ? "status-ok" : (st === "failed" ? "status-failed" : "status-other");
    const campName = (it.campaign_name || it.business_name || "").trim();
    const idStr = String(it.id);

    state.nameById[idStr] = campName;
    state.profileById[idStr] = pid;

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td class="ctr"><input type="checkbox" class="rowchk" data-id="${it.id}"/></td>
      <td class="muted">${it.id}</td>
      <td><a href="/company/${it.id}"><b>${esc(it.business_name||"—")}</b></a></td>
      <td><span class="muted">${esc(it.website_url||"—")}</span></td>
      <td>${esc(it.campaign_type||"—")}</td>
      <td>${esc(it.budget_display||"—")}</td>
      <td>${esc(it.locations||"—")}</td>
      <td>${esc(it.languages||"—")}</td>
      <td class="ctr">${it.n_ads||0}</td>
      <td><span class="muted">${esc(it.creatives_summary||"")}</span></td>
      <td>${esc(pname||"—")}</td>
      <td>${esc(it.google_account||"")}</td>
      <td>${fmtDate(it.created_at)}</td>
      <td class="${stClass}">${esc(it.status||"—")}</td>
    `;
    tbody.appendChild(tr);
  }

  // селекты
  const chks = tbody.querySelectorAll(".rowchk");
  chks.forEach(ch=>{
    const id = String(ch.getAttribute("data-id"));
    ch.checked = state.selected.has(id);
  });

  // info / пагинация
  countEl.textContent = `${state.total} записей`;
  const pages = Math.max(1, Math.ceil(state.total / state.page_size));
  pageInfo.textContent = `${state.page} / ${pages}`;

  deleteBtn.disabled = state.selected.size === 0;
}

async function fetchProfiles(){
  try{
    const r = await fetch("/api/adspower/profiles");
    const j = await r.json();
    profileFilter.innerHTML = `<option value="">Все профили (AdsPower)</option>`;
    const map = {};
    for(const it of (j.items||[])){
      map[it.profile_id] = it.name || "";
      const opt = document.createElement("option");
      opt.value = it.profile_id;
      opt.textContent = (it.name||"(no name)") + " — " + it.profile_id;
      profileFilter.appendChild(opt);
    }
    state.profilesMap = map;
  }catch(e){
    // тихо
  }
}

async function fetchPage(){
  const url = new URL("/api/companies/query", location.origin);
  url.searchParams.set("page", state.page);
  url.searchParams.set("page_size", state.page_size);
  url.searchParams.set("sort", state.sort);
  url.searchParams.set("dir", state.dir);
  if(state.q) url.searchParams.set("q", state.q);
  if(state.profile_id) url.searchParams.set("profile_id", state.profile_id);
  if(state.status) url.searchParams.set("status", state.status);
  const r = await fetch(url);
  const j = await r.json();
  state.items = j.items || [];
  state.total = j.total || 0;
  render();
}

tbody.addEventListener("change", (e)=>{
  const el = e.target;
  if (!el || !el.classList || !el.classList.contains("rowchk")) return;
  const id = String(el.getAttribute("data-id"));
  const name = (state.nameById[id] || "").trim();
  const pid = String(state.profileById[id] || "");
  if (el.checked){
    state.selected.add(id);
    if (name) state.selectedNames.set(id, name);
    state.selectedProfiles.set(id, pid);
  } else {
    state.selected.delete(id);
    state.selectedNames.delete(id);
    state.selectedProfiles.delete(id);
  }
  deleteBtn.disabled = state.selected.size === 0;
});

selAll.addEventListener("change", ()=>{
  if(selAll.checked){
    for(const it of state.items){
      const id = String(it.id);
      const pid = String(it.profile_id||"");
      state.selected.add(id);
      const name = (state.nameById[id] || it.campaign_name || it.business_name || "").trim();
      if (name) state.selectedNames.set(id, name);
      state.selectedProfiles.set(id, pid);
    }
  } else {
    for(const it of state.items){
      const id = String(it.id);
      state.selected.delete(id);
      state.selectedNames.delete(id);
      state.selectedProfiles.delete(id);
    }
  }
  render();
});

$("#refreshBtn").addEventListener("click", ()=>{ fetchPage().catch(()=>{}); });
$("#prevBtn").addEventListener("click", ()=>{
  if(state.page>1){ state.page--; fetchPage().catch(()=>{}); }
});
$("#nextBtn").addEventListener("click", ()=>{
  const pages = Math.max(1, Math.ceil(state.total / state.page_size));
  if(state.page < pages){ state.page++; fetchPage().catch(()=>{}); }
});
$("#searchBtn").addEventListener("click", ()=>{
  state.q = String(searchEl.value||"").trim();
  state.page = 1;
  fetchPage().catch(()=>{});
});
$("#resetBtn").addEventListener("click", ()=>{
  searchEl.value = "";
  state.q = "";
  profileFilter.value = "";
  statusFilter.value = "";
  state.profile_id = "";
  state.status = "";
  state.page = 1;
  fetchPage().catch(()=>{});
});
profileFilter.addEventListener("change", ()=>{
  state.profile_id = String(profileFilter.value||"").trim();
  state.page = 1;
  fetchPage().catch(()=>{});
});
statusFilter.addEventListener("change", ()=>{
  state.status = String(statusFilter.value||"").trim();
  state.page = 1;
  fetchPage().catch(()=>{});
});

$("#deleteBtn").addEventListener("click", async ()=>{
  if(state.selected.size===0) return;

  // Собираем группы: profile_id -> { ids[], names(Set) }
  const groups = new Map();
  for(const id of state.selected){
    const idStr = String(id);
    const pid = String(state.selectedProfiles.get(idStr) || state.profileById[idStr] || "").trim();
    const nm = String(state.selectedNames.get(idStr) || "").trim();
    const key = pid || "_none";
    if(!groups.has(key)) groups.set(key, { pid, ids: [], names: new Set() });
    const g = groups.get(key);
    const n = parseInt(idStr,10);
    if(!isNaN(n)) g.ids.push(n);
    if(nm) g.names.add(nm);
  }

  if(groups.size===0){
    alert("Нет выбранных записей для удаления.");
    return;
  }

  if(!confirm(`Удалить ${Array.from(state.selected).length} выбранных записей?\nСначала удалим кампании в Google Ads (по профилям), затем — записи в БД.`)) return;

  deleteBtn.disabled = true;
  const prevText = deleteBtn.textContent;
  const allLogs = [];
  const failedKeys = new Set();

  try{
    // 1) Удаление в Google Ads — по группам профилей
    for(const [key, g] of groups){
      const names = Array.from(g.names);
      if(names.length === 0) continue;
      deleteBtn.textContent = g.pid ? `Удаление в Google Ads (профиль ${g.pid})…` : "Удаление в Google Ads…";
      const payload = g.pid ? { names, ids: g.ids, profile_id: g.pid } : { names, ids: g.ids };
      const r1 = await fetch("/api/gads/bulk_remove", {
        method: "POST",
        headers: { "Content-Type":"application/json" },
        body: JSON.stringify(payload)
      });
      const j1 = await r1.json();
      if (j1 && j1.bulk_remove_logs && Array.isArray(j1.bulk_remove_logs)) {
        allLogs.push(...j1.bulk_remove_logs);
      }
      if(!j1 || !j1.ok){
        failedKeys.add(key);
        console.warn("[bulk_remove] fail for profile:", g.pid, j1 && j1.error);
      }
    }
    if (allLogs.length) {
      console.log("[bulk_remove] logs:", allLogs);
    }

    // IDs, которые удалось удалить в GAds (по успешным группам)
    const idsOk = [];
    for(const [key, g] of groups){
      if(!failedKeys.has(key)){
        idsOk.push(...g.ids);
      }
    }

    // 2) Удаление строк из БД — только для idsOk
    if(idsOk.length > 0){
      deleteBtn.textContent = "Удаление из БД…";
      const r2 = await fetch("/api/companies/delete", {
        method:"POST",
        headers:{ "Content-Type":"application/json" },
        body: JSON.stringify({ids: idsOk, skip_gads: true})
      });
      const j2 = await r2.json();
      if(!j2.ok){
        alert("Ошибка удаления из БД: " + (j2.error || "unknown"));
        deleteBtn.textContent = prevText;
        deleteBtn.disabled = false;
        return;
      }
    }

    // Сброс выбора и обновление страницы
    state.selected.clear();
    state.selectedNames.clear();
    state.selectedProfiles.clear();
    selAll.checked = false;
    await fetchPage();

    if(failedKeys.size > 0){
      const failedCount = Array.from(failedKeys).reduce((acc,k)=>acc + (groups.get(k)?.ids.length||0), 0);
      alert(`Часть записей (${failedCount}) не удалена в Google Ads. Проверьте логи в консоли и повторите попытку по соответствующим профилям.`);
    }
  } catch(e){
    alert("Ошибка запроса: " + (e && e.message ? e.message : String(e)));
  } finally {
    deleteBtn.textContent = prevText;
    deleteBtn.disabled = state.selected.size === 0;
  }
});

runInitialSync();

Promise.all([fetchProfiles(), fetchPage()])
  .then(()=>{
    if(syncInProgress){
      setSyncState("running", "Синхронизация выполняется в фоне…");
    } else {
      hideSyncOverlay(600);
    }
  })
  .catch((err)=>{
    console.error("[companies-list] init failed", err);
    setSyncState("error", "Не удалось загрузить список компаний");
    syncOverlayDismissed = true;
    hideSyncOverlay(2500, true);
  });
</script>
</body>
</html>
"""


# =============================================================================
#                                РОУТЫ
# =============================================================================

def init_list_companies(app: Flask, settings: Settings) -> None:
    """
    Регистрирует:
      • /companies/list — страница списка
      • /api/companies/query — выдача (поиск/фильтры/пагинация/сортировка)
      • /api/companies/delete — массовое удаление
      • /api/adspower/profiles — список профилей (если не предоставлен модулем create_companies)
      • (авто) /api/gads/bulk_remove — если ещё не зарегистрирован
    """
    db = CompanyDB()

    # Автоподключение эндпоинта удаления в Google Ads (если не зарегистрирован)
    if "api_gads_bulk_remove" not in app.view_functions:
        try:
            init_bulk_remove(app, settings)  # type: ignore
        except Exception:
            # тихо — не мешаем списку компаний работать без bulk_remove
            pass

    @app.get("/companies/list")
    def companies_list_page() -> Response:
        return make_response(PAGE_HTML, 200)

    @app.get("/api/companies/query")
    def api_companies_query() -> Response:
        try:
            email = _require_user_email()
        except PermissionError:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        q = (request.args.get("q") or "").strip()
        profile_id = (request.args.get("profile_id") or "").strip()
        status = (request.args.get("status") or "").strip()
        try:
            page = int(request.args.get("page") or "1")
            size = int(request.args.get("page_size") or "20")
        except Exception:
            page, size = 1, 20
        sort = (request.args.get("sort") or "created_at").strip()
        direction = (request.args.get("dir") or "desc").strip()

        try:
            rows, total = db.query(
                user_email=email,
                q=q, page=page, page_size=size, sort=sort, direction=direction,
                profile_id=profile_id or None,
                status=status or None,
            )
            items: List[Dict[str, Any]] = []
            for r in rows:
                d = asdict(r)
                # Добавляем campaign_name для UI (используется при массовом удалении)
                d["campaign_name"] = _derive_campaign_name(r)
                items.append(d)
            return jsonify({"ok": True, "items": items, "total": total})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.post("/api/companies/delete")
    def api_companies_delete() -> Response:
        try:
            email = _require_user_email()
        except PermissionError:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        data = request.get_json(silent=True, force=True) or {}
        ids_raw = data.get("ids") or []
        skip_gads = bool(data.get("skip_gads") or False)
        if not isinstance(ids_raw, (list, tuple)):
            return jsonify({"ok": False, "error": "invalid_payload"}), 400
        try:
            ids = [int(x) for x in ids_raw if str(x).strip()]
        except Exception:
            return jsonify({"ok": False, "error": "invalid_ids"}), 400
        if not ids:
            return jsonify({"ok": False, "error": "empty_ids"}), 400

        rows = db.get_many(email, ids)
        found_ids = {row.id for row in rows}
        missing_ids = [i for i in ids if i not in found_ids]
        names_meta: List[Dict[str, Any]] = []
        dedup_names: List[str] = []
        seen_names: set[str] = set()
        for row in rows:
            item: Dict[str, Any] = {"id": row.id, "profile_id": row.profile_id}
            name = _derive_campaign_name(row)
            if name:
                item["name"] = name
                key = name.lower()
                if key not in seen_names:
                    seen_names.add(key)
                    dedup_names.append(name)
            names_meta.append(item)

        bulk_remove_logs: List[str] = []
        bulk_remove_result: Optional[Dict[str, Any]] = None
        bulk_remove_error: Optional[str] = None

        # Здесь оставляем прежнюю логику: удаление в GAds выполняется на клиенте (skip_gads=true),
        # иначе — пробуем общий драйвер.
        if dedup_names and not skip_gads:
            holder, acquire_err = _acquire_shared_driver(timeout=25.0)
            if not holder:
                bulk_remove_error = acquire_err
            else:
                driver = holder[0]

                def _emit(msg: str) -> None:
                    if isinstance(msg, str):
                        txt = msg.strip()
                        if txt:
                            bulk_remove_logs.append(txt)

                try:
                    bulk_remove_result = remove_campaigns_by_names(driver, dedup_names, emit=_emit)
                except Exception as e:
                    bulk_remove_error = f"{type(e).__name__}: {e}"
                finally:
                    _release_shared_driver(holder)

        try:
            deleted = db.delete_many(email, ids)
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

        resp: Dict[str, Any] = {
            "ok": True,
            "deleted": int(deleted),
            "ids": ids,
            "names": names_meta,
        }
        if missing_ids:
            resp["missing_ids"] = missing_ids
        if bulk_remove_result is not None:
            resp["bulk_remove"] = bulk_remove_result
        if bulk_remove_logs:
            resp["bulk_remove_logs"] = bulk_remove_logs
        if bulk_remove_error:
            resp["bulk_remove_error"] = bulk_remove_error
        return jsonify(resp)

    # Фолбэк на случай, если список профилей не зарегистрирован create_companies
    if "api_adspower_profiles" not in app.view_functions:
        @app.get("/api/adspower/profiles")
        def api_adspower_profiles_fallback() -> Response:  # pragma: no cover
            q = (request.args.get("q") or "").strip()
            try:
                data = _list_adspower_profiles(q=q, page=1, page_size=300)
                return jsonify({"ok": True, **data})
            except Exception as e:
                return jsonify({"ok": False, "error": str(e)}), 500
