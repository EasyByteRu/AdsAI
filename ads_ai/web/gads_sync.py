# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import json
import os
import re
import sqlite3
import threading
import time
import hashlib
import mimetypes
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Flask, jsonify, request, Response, session

# Мягкий импорт Settings
try:
    from ads_ai.config.settings import Settings  # noqa: F401
except Exception:  # pragma: no cover
    class Settings:
        pass


# =============================================================================
#                               ВСПОМОГАТЕЛЬНОЕ
# =============================================================================

def _now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _log(logs: List[str], msg: str) -> None:
    # Дёшево и безопасно (без форматирования больших структур)
    try:
        logs.append(f"[{time.strftime('%H:%M:%S')}] {msg}")
    except Exception:
        pass


def _parse_bool(val: Optional[str], default: bool = True) -> bool:
    if val is None or str(val).strip() == "":
        return bool(default)
    s = str(val).strip().lower()
    if s in ("1", "true", "yes", "y", "on"): return True
    if s in ("0", "false", "no", "n", "off"): return False
    return bool(default)


def _data_root() -> Path:
    """
    Корневая папка данных проекта:
      • ADS_AI_DATA, если задана;
      • ./ads_ai_data — по умолчанию.
    """
    base = (os.getenv("ADS_AI_DATA") or "").strip()
    p = Path(base).expanduser().resolve() if base else Path(os.getcwd()).joinpath("ads_ai_data").resolve()
    p.mkdir(parents=True, exist_ok=True)
    return p


# =============================================================================
#                       БАЗА ДАННЫХ: ОБЩИЕ ФУНКЦИИ (БЫСТРО!)
# =============================================================================

_DB_PATH_CACHED: Optional[str] = None
_DB_ONCE_LOCK = threading.Lock()
_DB_SCHEMA_ONCE = threading.Event()
_thread_local = threading.local()


def _companies_db_path() -> str:
    global _DB_PATH_CACHED
    if _DB_PATH_CACHED:
        return _DB_PATH_CACHED
    path = (os.getenv("ADS_AI_DB") or "").strip()
    if not path:
        base = os.path.abspath(os.path.join(os.getcwd(), "ads_ai_data"))
        os.makedirs(base, exist_ok=True)
        path = os.path.join(base, "companies.sqlite3")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    _DB_PATH_CACHED = path
    return path


def _configure_conn(cx: sqlite3.Connection) -> None:
    # Ускоряющие PRAGMA + предсказуемость конкуренции
    try:
        cx.execute("PRAGMA journal_mode=WAL;")
        cx.execute("PRAGMA synchronous=NORMAL;")
        cx.execute("PRAGMA foreign_keys=ON;")
        cx.execute("PRAGMA temp_store=MEMORY;")
        cx.execute("PRAGMA mmap_size=268435456;")  # 256 MB
        cx.execute("PRAGMA cache_size=-100000;")   # ~100MB cache
        cx.execute("PRAGMA busy_timeout=30000;")
    except Exception:
        pass


def _cx() -> sqlite3.Connection:
    """
    Быстрый доступ к БД: один коннект на поток с PRAGMA.
    Совместим со всеми 'with _cx() as cx' (commit/rollback на exit).
    """
    conn: Optional[sqlite3.Connection] = getattr(_thread_local, "cx", None)
    if conn is None:
        conn = sqlite3.connect(_companies_db_path(), check_same_thread=False, timeout=30.0)
        conn.row_factory = sqlite3.Row
        _configure_conn(conn)
        _thread_local.cx = conn
    return conn


# =============================================================================
#                       БАЗА ДАННЫХ: ТАБЛИЦА companies
# =============================================================================

def _db_ensure_companies_schema(logs: Optional[List[str]] = None) -> None:
    """
    Важно: схема совместима с /companies и /company/<id>.
    Поля ассетов — JSON-списки. UI ожидает 'images_json' как список dict со свойствами 'file' ИЛИ 'url'.
    """
    if _DB_SCHEMA_ONCE.is_set():
        return
    with _DB_ONCE_LOCK:
        if _DB_SCHEMA_ONCE.is_set():
            return
        with _cx() as cx:
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
            cx.execute("CREATE INDEX IF NOT EXISTS idx_companies_business_name ON companies(business_name)")
            cx.commit()
        _DB_SCHEMA_ONCE.set()
    if logs is not None:
        _log(logs, "Проверил/создал схему таблицы companies")


def _sql_like_json_pair(key: str, value: str) -> str:
    val = value.replace('"', '""')
    return f'%\"{key}\":\"{val}\"%'


def _merge_extra_json(old: Optional[str], patch: Dict[str, Any]) -> str:
    try:
        base = json.loads(old) if old else {}
        if not isinstance(base, dict): base = {}
    except Exception:
        base = {}

    def deep_merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
        for k, v in b.items():
            if isinstance(v, dict) and isinstance(a.get(k), dict):
                deep_merge(a[k], v)
            else:
                a[k] = v
        return a

    merged = deep_merge(base, patch)
    return json.dumps(merged, ensure_ascii=False, separators=(",", ":"))


def _db_find_company_by_gads_id_cx(
    cx: sqlite3.Connection, *, user_email: str, profile_id: str, gads_id: str
) -> Optional[int]:
    like_pat = _sql_like_json_pair("gads_campaign_id", gads_id)
    cur = cx.execute(
        "SELECT id FROM companies WHERE user_email=? AND profile_id=? AND extra_json LIKE ? LIMIT 1",
        (user_email, profile_id, like_pat),
    )
    r = cur.fetchone()
    return int(r["id"]) if r else None


def _db_find_company_by_gads_id(*, user_email: str, profile_id: str, gads_id: str) -> Optional[int]:
    with _cx() as cx:
        return _db_find_company_by_gads_id_cx(cx, user_email=user_email, profile_id=profile_id, gads_id=gads_id)


def _db_find_company_by_name_any_cx(
    cx: sqlite3.Connection, *, user_email: str, profile_id: str, name: str
) -> Optional[int]:
    cur = cx.execute(
        "SELECT id FROM companies WHERE user_email=? AND profile_id=? AND coalesce(business_name,'')=? LIMIT 1",
        (user_email, profile_id, name or ""),
    )
    r = cur.fetchone()
    return int(r["id"]) if r else None


def _db_find_company_by_name_any(*, user_email: str, profile_id: str, name: str) -> Optional[int]:
    with _cx() as cx:
        return _db_find_company_by_name_any_cx(cx, user_email=user_email, profile_id=profile_id, name=name)


def _db_insert_company_import(
    cx: sqlite3.Connection,
    *, user_email: str, profile_id: str, headless: bool,
    campaign_id: Optional[str], campaign_name: Optional[str],
    status_text: Optional[str], currency: Optional[str], budget: Optional[str],
    google_email: Optional[str], csv_path: Optional[str],
    csv_fieldnames: List[str], csv_row: Dict[str, Any], logs: Optional[List[str]] = None,
) -> int:
    created_at = _now_iso()
    extra = {
        "gads_import": {
            "source": "gads_sync",
            "downloaded_at": created_at,
            "csv_path": csv_path or "",
            "currency": (currency or ""),
            "daily_budget_raw": (budget or ""),
            "status_raw": (status_text or ""),
            "csv_columns": csv_fieldnames,
            "csv_row": csv_row,
        },
        "context": {
            "google_email": google_email or "",
        }
    }
    if campaign_id:
        extra["gads_import"]["gads_campaign_id"] = campaign_id
    if campaign_name:
        extra["gads_import"]["gads_campaign_name"] = campaign_name

    record = {
        "created_at": created_at,
        "status": "imported",
        "profile_id": profile_id,
        "user_email": user_email,
        "headless": 1 if headless else 0,
        "site_url": "",
        "budget_per_day": budget or "",
        "usp": "Imported from Google Ads",
        "campaign_type": "IMPORTED",
        "locations": json.dumps([], ensure_ascii=False),
        "languages": json.dumps([], ensure_ascii=False),
        "n_ads": 0,
        "business_name": (campaign_name or "").strip() or (campaign_id or "(no name)"),
        "asset_group_name": "",
        "headlines_json": json.dumps([], ensure_ascii=False),
        "long_headlines_json": json.dumps([], ensure_ascii=False),
        "descriptions_json": json.dumps([], ensure_ascii=False),
        "images_json": json.dumps([], ensure_ascii=False),
        "image_files_json": json.dumps([], ensure_ascii=False),
        "extra_json": json.dumps(extra, ensure_ascii=False, separators=(",", ":")),
        "google_tags": json.dumps(extra.get("keywords") or extra.get("search_themes") or [], ensure_ascii=False),
        "google_tag": None,
    }
    cur = cx.execute("""
    INSERT INTO companies(
      created_at,status,profile_id,user_email,headless,site_url,budget_per_day,usp,campaign_type,
      locations,languages,n_ads,business_name,asset_group_name,
      headlines_json,long_headlines_json,descriptions_json,images_json,image_files_json,extra_json,google_tags,google_tag
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        record["created_at"], record["status"], record["profile_id"], record["user_email"], record["headless"],
        record["site_url"], record["budget_per_day"], record["usp"], record["campaign_type"],
        record["locations"], record["languages"], record["n_ads"], record["business_name"], record["asset_group_name"],
        record["headlines_json"], record["long_headlines_json"], record["descriptions_json"],
        record["images_json"], record["image_files_json"], record["extra_json"], record["google_tags"], record["google_tag"],
    ))
    new_id = int(cur.lastrowid)
    if logs is not None:
        _log(logs, f"INSERT companies id={new_id} name={record['business_name']!r}")
    return new_id


def _db_update_company_import(
    cx: sqlite3.Connection,
    *, company_id: int, campaign_name: Optional[str], status_text: Optional[str],
    currency: Optional[str], budget: Optional[str],
    csv_path: Optional[str], csv_fieldnames: List[str], csv_row: Dict[str, Any],
    logs: Optional[List[str]] = None,
) -> None:
    cur = cx.execute("SELECT extra_json FROM companies WHERE id=? LIMIT 1", (company_id,))
    r = cur.fetchone()
    old_extra = r["extra_json"] if r else None
    patch = {
        "gads_import": {
            "last_sync": _now_iso(),
            "csv_path": csv_path or "",
            "status_raw": status_text or "",
            "daily_budget_raw": budget or "",
            "currency": currency or "",
            "gads_campaign_name": campaign_name or "",
            "csv_columns": csv_fieldnames,
            "csv_row": csv_row,
        }
    }
    new_extra = _merge_extra_json(old_extra, patch)

    cx.execute(
        """UPDATE companies 
           SET business_name=?, budget_per_day=?, status=?, extra_json=? 
           WHERE id=?""",
        (
            (campaign_name or "").strip() or f"Imported {company_id}",
            budget or "",
            "imported",
            new_extra,
            company_id
        ),
    )
    if logs is not None:
        _log(logs, f"UPDATE companies id={company_id} name='{campaign_name}'")


# =============================================================================
#                   БАЗА ДАННЫХ: ТАБЛИЦА СТАТИСТИКИ campaign_stats
# =============================================================================

def _db_ensure_campaign_stats_schema(logs: Optional[List[str]] = None) -> None:
    """Создает таблицу для хранения ежедневной статистики по кампаниям."""
    # Тоже под защищённым once — но без флага: дешёвая операция (CREATE IF NOT EXISTS)
    with _cx() as cx:
        cx.execute("""
        CREATE TABLE IF NOT EXISTS campaign_stats(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          company_id INTEGER NOT NULL,
          sync_date TEXT NOT NULL,
          clicks INTEGER,
          impressions INTEGER,
          ctr TEXT,
          avg_cpc REAL,
          cost REAL,
          conv_rate TEXT,
          conversions REAL,
          cost_per_conv REAL,
          raw_data_json TEXT,
          FOREIGN KEY (company_id) REFERENCES companies (id) ON DELETE CASCADE
        )
        """)
        cx.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_campaign_stats_company_date ON campaign_stats(company_id, sync_date)")
        cx.commit()
    if logs is not None:
        _log(logs, "Проверил/создал схему таблицы campaign_stats")


def _parse_stat_float(val: Optional[str]) -> Optional[float]:
    if val is None: return None
    try:
        s = str(val).strip().replace(",", ".").replace(" ", "").rstrip("%")
        return float(s)
    except (ValueError, TypeError):
        return None


def _parse_stat_int(val: Optional[str]) -> Optional[int]:
    if val is None: return None
    try:
        return int(str(val).strip().replace(",", "").replace(" ", ""))
    except (ValueError, TypeError):
        return None


def _db_log_campaign_stats(
    cx: sqlite3.Connection,
    company_id: int,
    csv_row: Dict[str, Any],
    logs: Optional[List[str]] = None,
) -> None:
    """
    UPSERT сегодняшней статистики.
    """
    sync_date = datetime.utcnow().strftime("%Y-%m-%d")
    def _pick(d: Dict[str, Any], *keys: str) -> Optional[str]:
        dk = {re.sub(r"[^a-z0-9]+", " ", (k or "").strip().lower()).strip(): k for k in d.keys()}
        for k in keys:
            nk = re.sub(r"[^a-z0-9]+", " ", (k or "").strip().lower()).strip()
            if nk in dk:
                v = d.get(dk[nk])
                return str(v).strip() if v is not None else None
        return None

    stats = {
        "company_id": company_id,
        "sync_date": sync_date,
        "clicks": _parse_stat_int(_pick(csv_row, "Clicks", "Клики")),
        "impressions": _parse_stat_int(_pick(csv_row, "Impr.", "Показы")),
        "ctr": _pick(csv_row, "CTR", "CTR"),
        "avg_cpc": _parse_stat_float(_pick(csv_row, "Avg. CPC", "Сред. цена за клик")),
        "cost": _parse_stat_float(_pick(csv_row, "Cost", "Стоимость")),
        "conv_rate": _pick(csv_row, "Conv. rate", "Коэф. конверсии"),
        "conversions": _parse_stat_float(_pick(csv_row, "Conversions", "Конверсии")),
        "cost_per_conv": _parse_stat_float(_pick(csv_row, "Cost / conv.", "Цена за конверсию")),
        "raw_data_json": json.dumps(csv_row, ensure_ascii=False),
    }

    cx.execute("""
    INSERT INTO campaign_stats (
      company_id, sync_date, clicks, impressions, ctr, avg_cpc, cost,
      conv_rate, conversions, cost_per_conv, raw_data_json
    ) VALUES (
      :company_id, :sync_date, :clicks, :impressions, :ctr, :avg_cpc, :cost,
      :conv_rate, :conversions, :cost_per_conv, :raw_data_json
    )
    ON CONFLICT(company_id, sync_date) DO UPDATE SET
      clicks = excluded.clicks,
      impressions = excluded.impressions,
      ctr = excluded.ctr,
      avg_cpc = excluded.avg_cpc,
      cost = excluded.cost,
      conv_rate = excluded.conv_rate,
      conversions = excluded.conversions,
      cost_per_conv = excluded.cost_per_conv,
      raw_data_json = excluded.raw_data_json
    """, stats)

    if logs is not None:
        _log(logs, f"UPSERT STATS: id={company_id} Date={sync_date} Clicks={stats['clicks']} Cost={stats['cost']}")


# =============================================================================
#                     СКАЧИВАНИЕ ИЗОБРАЖЕНИЙ + НОРМАЛИЗАЦИЯ ПУТЕЙ
# =============================================================================

def _company_images_dir(company_id: int) -> Path:
    """
    ADS_AI_DATA/companies/images/<company_id>/
    """
    base = _data_root().joinpath("companies", "images", str(company_id))
    base.mkdir(parents=True, exist_ok=True)
    return base


def _safe_filename_from_url(url: str) -> str:
    try:
        u = urlparse(url)
        name = unquote(os.path.basename(u.path)) or ""
    except Exception:
        name = ""
    if not name:
        name = hashlib.sha1(url.encode("utf-8", errors="ignore")).hexdigest()[:16]
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    if len(name) > 80:
        root, ext = os.path.splitext(name)
        name = root[:60] + "_" + hashlib.md5(name.encode()).hexdigest()[:8] + ext
    return name


def _ensure_extension(name: str, content_type: Optional[str]) -> str:
    root, ext = os.path.splitext(name)
    if ext and len(ext) <= 5:
        return name
    ext2 = None
    if content_type:
        ct = (content_type or "").lower().strip()
        if ct in ("image/jpeg", "image/jpg"): ext2 = ".jpg"
        elif ct == "image/png": ext2 = ".png"
        elif ct == "image/webp": ext2 = ".webp"
        elif ct == "image/gif": ext2 = ".gif"
        else:
            ext2 = mimetypes.guess_extension(ct) or ".jpg"
    return (root or "img") + (ext2 or ".jpg")


def _download_image_to(url: str, dest_dir: Path, logs: Optional[List[str]] = None, timeout: float = 15.0) -> Optional[Path]:
    """
    Качает один URL в dest_dir. Если файл уже существует — не перекачивает.
    Возвращает путь к локальному файлу или None при ошибке.
    """
    if not url or not re.match(r"^https?://", url, flags=re.I):
        return None

    fname = _safe_filename_from_url(url)
    content_type: Optional[str] = None
    data: Optional[bytes] = None

    # Попытка через requests (если установлен)
    try:
        import requests  # type: ignore
        try:
            headers = {"User-Agent": "Mozilla/5.0 (HyperAI GAdsSync) Chrome Safari"}
            with requests.get(url, headers=headers, timeout=timeout, stream=True) as r:  # type: ignore
                ct = r.headers.get("Content-Type") or r.headers.get("content-type")
                content_type = (ct or "").split(";")[0].strip() if ct else None
                chunks: List[bytes] = []
                size = 0
                max_size = 20 * 1024 * 1024  # 20 MB
                for chunk in r.iter_content(chunk_size=65536):  # крупнее чанки — меньше overhead
                    if not chunk:
                        continue
                    chunks.append(chunk)
                    size += len(chunk)
                    if size > max_size:
                        if logs is not None:
                            _log(logs, f"Превышен лимит размера картинки {url!r}")
                        return None
                data = b"".join(chunks)
        except Exception as e:
            if logs is not None:
                _log(logs, f"requests: ошибка скачивания {url!r}: {e!r}")
            data = None
    except Exception:
        data = None

    # Фолбэк — urllib
    if data is None:
        try:
            import urllib.request  # type: ignore
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (HyperAI GAdsSync) Chrome Safari"})  # type: ignore
            with urllib.request.urlopen(req, timeout=timeout) as resp:  # type: ignore
                hdrs = getattr(resp, "info", lambda: None)()
                if hdrs:
                    ct = hdrs.get("Content-Type") or hdrs.get("content-type")
                    content_type = (ct or "").split(";")[0].strip() if ct else None
                data = resp.read()
        except Exception as e:
            if logs is not None:
                _log(logs, f"urllib: ошибка скачивания {url!r}: {e!r}")
            return None

    fname = _ensure_extension(fname, content_type)
    dst = dest_dir.joinpath(fname)
    if not dst.exists():
        try:
            dst.write_bytes(data or b"")
        except Exception as e:
            if logs is not None:
                _log(logs, f"Ошибка записи файла {dst}: {e!r}")
            return None

    if logs is not None:
        _log(logs, f"Скачал изображение: {url} → {dst}")
    return dst


def _rel_to_data_root(path: Path) -> str:
    root = _data_root()
    try:
        rel = path.relative_to(root)
        return rel.as_posix()
    except Exception:
        s = path.as_posix()
        rs = root.as_posix()
        if s.startswith(rs):
            s = s[len(rs):].lstrip("/")
        return s


def _bulk_download_images(
    items: List[Dict[str, Any]],
    dest_dir: Path,
    logs: Optional[List[str]],
    max_workers: Optional[int] = None,
) -> Tuple[Dict[str, str], List[str], List[Dict[str, Any]]]:
    """
    Параллельная загрузка картинок.
    На входе items = [{"url": "...", "kind": "..."}].
    Возвращает:
      • url2rel: url -> относительный путь
      • rel_files: список относительных путей (порядок = входной порядок с успешной фильтрацией)
      • new_imgs_local: [{"file": rel, "kind": kind}, ...] (успешно скачанные)
    """
    # Уникализируем URL, но порядок для rel_files строим заново по исходным items
    uniq_urls: List[str] = []
    seen = set()
    for it in items:
        u = (it.get("url") or "").strip()
        if u and u not in seen:
            seen.add(u)
            uniq_urls.append(u)

    if not uniq_urls:
        return {}, [], []

    conc = max(2, min(32, int(os.getenv("ADS_AI_IMG_DL_CONCURRENCY", "8"))))
    if max_workers is None:
        max_workers = conc
    dest_dir.mkdir(parents=True, exist_ok=True)

    url2path: Dict[str, Optional[Path]] = {u: None for u in uniq_urls}

    def _task(u: str) -> Tuple[str, Optional[Path]]:
        p = _download_image_to(u, dest_dir, logs=logs)
        return u, p

    # Грузим параллельно
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_task, u) for u in uniq_urls]
        for f in as_completed(futures):
            try:
                u, p = f.result()
                url2path[u] = p
            except Exception:
                # уже залогировано внутри
                pass

    # Собираем выход
    url2rel: Dict[str, str] = {}
    rel_files: List[str] = []
    new_imgs_local: List[Dict[str, Any]] = []
    for it in items:
        u = (it.get("url") or "").strip()
        if not u:
            continue
        p = url2path.get(u)
        if not p:
            continue
        rel = url2rel.get(u)
        if not rel:
            rel = _rel_to_data_root(p)
            url2rel[u] = rel
        rel_files.append(rel)
        new_imgs_local.append({"file": rel, "kind": (it.get("kind") or "marketing")})
    return url2rel, rel_files, new_imgs_local


# =============================================================================
#                CSV → ЖЁСТКАЯ ЗАМЕНА АССЕТОВ + ЗАГРУЗКА ИЗОБРАЖЕНИЙ
# =============================================================================

def _db_replace_company_assets(
    *, company_id: int,
    agg: Dict[str, Any],
    csv_path: Optional[str],
    csv_fieldnames: List[str],
    logs: Optional[List[str]] = None,
) -> bool:
    """
    ЖЁСТКАЯ замена ассетов:
      - headlines_json, long_headlines_json, descriptions_json, images_json — ПОЛНОСТЬЮ заново из CSV;
      - image_files_json теперь заполняется локальными относительными путями;
      - asset_group_name — ставим первый из набора, если есть;
      - search_themes/audience_signals — в extra.gads_assets (+ алиас keywords).
    Возвращает True, если данные реально изменились.
    """
    with _cx() as cx:
        cur = cx.execute("""SELECT business_name, asset_group_name, headlines_json, long_headlines_json, descriptions_json,
                                   images_json, image_files_json, extra_json
                            FROM companies WHERE id=? LIMIT 1""", (company_id,))
        row = cur.fetchone()
        if not row:
            return False

        def _try_json(v: Any, dflt: Any) -> Any:
            try:
                if not v: return dflt
                if isinstance(v, (list, dict)): return v
                s = v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else str(v)
                s = s.strip()
                if s.startswith("{") or s.startswith("["):
                    return json.loads(s)
            except Exception:
                pass
            return dflt

        old = {
            "h": _try_json(row["headlines_json"], []),
            "lh": _try_json(row["long_headlines_json"], []),
            "d": _try_json(row["descriptions_json"], []),
            "img": _try_json(row["images_json"], []),
            "af": _try_json(row["image_files_json"], []),
            "agn": (row["asset_group_name"] or "").strip(),
            "extra": _try_json(row["extra_json"], {}),
        }

        # Новые значения из агрегата
        new_h  = list(agg.get("headlines") or [])
        new_lh = list(agg.get("long_headlines") or [])
        new_d  = list(agg.get("descriptions") or [])

        # Список исходных URL из CSV
        src_imgs: List[Dict[str, Any]] = []
        for itm in agg.get("images") or []:
            if isinstance(itm, dict):
                u = (itm.get("url") or itm.get("src") or "").strip()
                if u:
                    src_imgs.append({"url": u, "kind": (itm.get("kind") or "marketing")})

        aset = agg.get("asset_group_names")
        agn = ""
        if isinstance(aset, (set, list)) and aset:
            agn = sorted(list(aset))[0]

        # === ПАРАЛЛЕЛЬНО СКАЧИВАЕМ ИЗОБРАЖЕНИЯ ===
        img_dir = _company_images_dir(company_id)
        _, rel_files, new_imgs_local = _bulk_download_images(src_imgs, img_dir, logs=logs)

        # extra.gads_assets
        ga = {
            "source": "gads_sync",
            "last_sync": _now_iso(),
            "csv_path": csv_path or "",
            "csv_columns": csv_fieldnames,
            "asset_groups": sorted(list(agg.get("asset_group_names") or [])),
            "search_themes": sorted(list(agg.get("search_themes") or [])),
            "audience_signals": sorted(list(agg.get("audience_signals") or [])),
            "keywords": sorted(list(agg.get("search_themes") or [])),
            "images_source_urls": [x["url"] for x in src_imgs],
            "download_dir": _rel_to_data_root(img_dir),
        }
        new_extra = _merge_extra_json(row["extra_json"], {"gads_assets": ga})

        changed = (
            json.dumps(old["h"], ensure_ascii=False) != json.dumps(new_h, ensure_ascii=False) or
            json.dumps(old["lh"], ensure_ascii=False) != json.dumps(new_lh, ensure_ascii=False) or
            json.dumps(old["d"], ensure_ascii=False) != json.dumps(new_d, ensure_ascii=False) or
            json.dumps(old["img"], ensure_ascii=False) != json.dumps(new_imgs_local, ensure_ascii=False) or
            (agn and agn != old["agn"]) or
            json.dumps(old["extra"], ensure_ascii=False) != json.dumps(json.loads(new_extra), ensure_ascii=False)
        )

        # Обновляем — ПОЛНАЯ ПЕРЕЗАПИСЬ ассетов
        cx.execute(
            """UPDATE companies
               SET asset_group_name=?, headlines_json=?, long_headlines_json=?, descriptions_json=?,
                   images_json=?, image_files_json=?, extra_json=?
               WHERE id=?""",
            (
                agn or old["agn"],
                json.dumps(new_h, ensure_ascii=False),
                json.dumps(new_lh, ensure_ascii=False),
                json.dumps(new_d, ensure_ascii=False),
                json.dumps(new_imgs_local, ensure_ascii=False),
                json.dumps(rel_files, ensure_ascii=False),
                new_extra,
                company_id,
            )
        )
        cx.commit()
        if logs is not None:
            _log(logs, f"REPLACE assets id={company_id} H/L/D/IMG overwritten (images downloaded: {len(new_imgs_local)})")
        return changed


# =============================================================================
#                  ПРОФИЛИ ПОЛЬЗОВАТЕЛЯ (привязка аккаунтов)
# =============================================================================

@dataclass
class _AccountMeta:
    profile_id: str
    email: str
    name: str
    created_at: float


def _discover_campaigns_db(settings: Settings) -> Optional[str]:
    p = (os.getenv("ADS_AI_CAMPAIGNS_DB") or "").strip()
    if p and os.path.exists(p): return p
    try:
        import importlib  # noqa: WPS433
        camp = importlib.import_module("ads_ai.web.camping")
        rp = getattr(camp, "_resolve_paths", None)
        if callable(rp):
            obj = rp(settings)  # type: ignore
            dbf = getattr(obj, "db_file", None)
            if dbf and os.path.exists(str(dbf)): return str(dbf)
    except Exception:
        pass
    guess = os.path.join(os.getcwd(), "artifacts", "campaigns.db")
    return guess if os.path.exists(guess) else None


def _user_profiles_map(settings: Settings, user_email: str) -> Dict[str, _AccountMeta]:
    db = _discover_campaigns_db(settings)
    if not db or not os.path.exists(db): return {}
    index: Dict[str, _AccountMeta] = {}
    try:
        cx = sqlite3.connect(db, check_same_thread=False, timeout=15.0)
        cx.row_factory = sqlite3.Row
        cur = cx.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='accounts'")
        if not cur.fetchone(): return {}
        rows = cx.execute("SELECT * FROM accounts WHERE user_email = ?", (user_email,)).fetchall()
        for r in rows:
            d = dict(r)
            pid = str(d.get("profile_id") or "").strip()
            if not pid: continue
            created = d.get("created_at") or d.get("ts") or d.get("updated_at") or 0
            try: created = float(created)
            except Exception: created = 0.0
            email = ""
            for k in ("email", "login", "email_address", "gmail", "ga_email", "account_email"):
                v = d.get(k)
                if v: email = str(v).strip(); break
            name = str(d.get("name") or email or "")
            prev = index.get(pid)
            if (prev is None) or (created >= prev.created_at):
                index[pid] = _AccountMeta(profile_id=pid, email=email, name=name, created_at=float(created))
    except Exception:
        return {}
    finally:
        try: cx.close()  # type: ignore
        except Exception: pass
    return index


# =============================================================================
#                  CSV: устойчивый парсер отчётов Google Ads
# =============================================================================

def _norm_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (s or "").strip().lower()).strip()


def _pick(d: Dict[str, Any], *keys: str) -> Optional[str]:
    if not d: return None
    dk = {_norm_key(k): k for k in d.keys()}
    for k in keys:
        nk = _norm_key(k)
        if nk in dk:
            v = d.get(dk[nk])
            return str(v).strip() if v is not None else None
    return None


def _detect_delim(lines: List[str]) -> str:
    best_delim, best_score = ",", -1
    for delim in (",", ";", "\t"):
        score = sum(line.count(delim) for line in lines[:200])
        if score > best_score:
            best_delim, best_score = delim, score
    return best_delim


def _find_header_idx(lines: List[str], delim: str) -> int:
    for i, line in enumerate(lines[:400]):
        cols = [c.strip().strip('"') for c in line.split(delim)]
        if len(cols) < 2: continue
        norm = [_norm_key(c) for c in cols]
        if any(k in norm for k in ("campaign", "campaign id", "campaign status", "budget", "currency", "asset group", "headlines", "descriptions")):
            return i
    return 0


_TOTAL_RE = re.compile(r"^\s*(total|итог|всего)\s*:?", re.IGNORECASE)


def _row_is_campaign(r: Dict[str, Any]) -> bool:
    name = _pick(r, "Campaign", "Campaign name", "Название кампании", "Кампания")
    if not name: return False
    if name in ("—", "-", "--"): return False
    if _TOTAL_RE.match(name): return False
    cs = _pick(r, "Campaign status", "Состояние кампании", "Статус кампании")
    if cs and _norm_key(cs) in ("campaign status", "статус кампании", "состояние кампании"):
        return False
    return True


def _parse_gads_csv(path: Path, logs: Optional[List[str]] = None) -> Tuple[List[Dict[str, Any]], List[str]]:
    if logs is not None: _log(logs, f"Читаю CSV: {path}")
    raw = path.read_bytes()

    # Кодировка
    txt = None
    for enc in ("utf-8-sig", "utf-8", "cp1251", "latin-1", "utf-16", "utf-16le", "utf-16be"):
        try:
            txt = raw.decode(enc)
            if logs is not None: _log(logs, f"Кодировка CSV: {enc}")
            break
        except Exception:
            continue
    if txt is None:
        txt = raw.decode("utf-8", errors="ignore")
        if logs is not None: _log(logs, "Кодировка CSV: utf-8 (ignore errors)")

    lines = txt.splitlines()
    delim = _detect_delim(lines)
    hdr_idx = _find_header_idx(lines, delim)
    rdr = csv.DictReader(lines[hdr_idx:], delimiter=delim)
    fieldnames = list(rdr.fieldnames or [])
    rows_raw = [dict(r) for r in rdr]
    rows = [r for r in rows_raw if _row_is_campaign(r)]

    if logs is not None:
        _log(logs, f"CSV(campaigns): delimiter={repr(delim)} header_line={hdr_idx}")
        _log(logs, f"Колонок: {len(fieldnames)} — {fieldnames}")
        _log(logs, f"Всего строк: {len(rows_raw)}; после фильтра: {len(rows)}")
    return rows, fieldnames


def _extract_row_fields(row: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    cid = _pick(row, "Campaign ID", "ID кампании", "Идентификатор кампании", "Идентификатор", "ID")
    name = _pick(row, "Campaign", "Campaign name", "Название кампании", "Кампания")
    status = _pick(row, "Campaign status", "Status", "Campaign state", "Состояние", "Статус", "Состояние кампании")
    currency = _pick(row, "Currency", "Account currency", "Account currency code", "Currency code",
                     "Валюта", "Валюта аккаунта", "Код валюты")
    budget = _pick(row, "Daily budget", "Campaign daily budget", "Budget", "Бюджет", "Дневной бюджет")
    return cid or None, name or None, status or None, currency or None, budget or None


# ---------------- Asset Groups CSV ----------------

_URL_RE = re.compile(r"https?://[^\s,]+", re.IGNORECASE)

def _split_values(cell: Optional[str]) -> List[str]:
    if not cell:
        return []
    s = str(cell).strip()
    if s in ("—", "-", "--"):
        return []
    parts = [p.strip() for p in s.split(",")]
    out: List[str] = []
    seen: set[str] = set()
    for p in parts:
        if not p or p in ("—", "-", "--"):
            continue
        if p not in seen:
            out.append(p); seen.add(p)
    return out


def _extract_urls(cell: Optional[str]) -> List[str]:
    if not cell:
        return []
    return [m.group(0) for m in _URL_RE.finditer(cell)]


def _parse_assetgroup_csv(path: Path, logs: Optional[List[str]] = None) -> Tuple[List[Dict[str, Any]], List[str]]:
    if logs is not None: _log(logs, f"Читаю Asset Groups CSV: {path}")
    raw = path.read_bytes()
    txt = None
    for enc in ("utf-8-sig", "utf-8", "cp1251", "latin-1", "utf-16", "utf-16le", "utf-16be"):
        try:
            txt = raw.decode(enc)
            if logs is not None: _log(logs, f"Кодировка CSV: {enc}")
            break
        except Exception:
            continue
    if txt is None:
        txt = raw.decode("utf-8", errors="ignore")
        if logs is not None: _log(logs, "Кодировка CSV: utf-8 (ignore errors)")

    lines = txt.splitlines()
    delim = _detect_delim(lines)
    hdr_idx = _find_header_idx(lines, delim)
    rdr = csv.DictReader(lines[hdr_idx:], delimiter=delim)
    fieldnames = list(rdr.fieldnames or [])
    rows = [dict(r) for r in rdr]

    if logs is not None:
        _log(logs, f"CSV(assetgroups): delimiter={repr(delim)} header_line={hdr_idx}")
        _log(logs, f"Колонок: {len(fieldnames)} — {fieldnames}")
    #     _log(logs, f"Строк: {len(rows)}")
    return rows, fieldnames


def _aggregate_assets_by_campaign(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    map: campaign_name -> агрегированные ассеты и мета.
    """
    result: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        def _pick(d: Dict[str, Any], *keys: str) -> Optional[str]:
            dk = {re.sub(r"[^a-z0-9]+", " ", (k or "").strip().lower()).strip(): k for k in d.keys()}
            for k in keys:
                nk = re.sub(r"[^a-z0-9]+", " ", (k or "").strip().lower()).strip()
                if nk in dk:
                    v = d.get(dk[nk]); return str(v).strip() if v is not None else None
            return None

        camp = _pick(r, "Campaign", "Campaign name") or ""
        if not camp:
            continue
        ag_name = _pick(r, "Asset Group", "Asset group", "Asset group name") or ""

        headlines = _split_values(_pick(r, "Headlines"))
        long_hl   = _split_values(_pick(r, "Long Headlines", "Long headlines"))
        descs     = _split_values(_pick(r, "Descriptions"))
        imgs_m    = _extract_urls(_pick(r, "Marketing Images", "Images", "Image"))
        imgs_sq   = _extract_urls(_pick(r, "Square Marketing Images", "Square Images"))
        imgs_pr   = _extract_urls(_pick(r, "Portrait Marketing Images", "Portrait Images"))
        themes    = _split_values(_pick(r, "Search themes", "Search Themes", "Search terms", "Keywords"))
        aud       = _split_values(_pick(r, "Audience signal", "Audience", "Signals"))

        bucket = result.setdefault(camp, {
            "asset_group_names": set(),
            "headlines": set(),
            "long_headlines": set(),
            "descriptions": set(),
            "images": [],  # list of {"url":..., "kind":...}
            "search_themes": set(),
            "audience_signals": set(),
        })

        if ag_name: bucket["asset_group_names"].add(ag_name)
        for s in headlines: bucket["headlines"].add(s)
        for s in long_hl:   bucket["long_headlines"].add(s)
        for s in descs:     bucket["descriptions"].add(s)
        for u in imgs_m:    bucket["images"].append({"url": u, "kind": "marketing"})
        for u in imgs_sq:   bucket["images"].append({"url": u, "kind": "square"})
        for u in imgs_pr:   bucket["images"].append({"url": u, "kind": "portrait"})
        for s in themes:    bucket["search_themes"].add(s)
        for s in aud:       bucket["audience_signals"].add(s)

    for _, v in list(result.items()):
        v["asset_group_names"] = set(v["asset_group_names"])
        v["headlines"] = list(v["headlines"])
        v["long_headlines"] = list(v["long_headlines"])
        v["descriptions"] = list(v["descriptions"])
        v["images"] = v["images"]
        v["search_themes"] = set(v["search_themes"])
        v["audience_signals"] = set(v["audience_signals"])
    return result


# =============================================================================
#                           AdsPower / WebDriver
# =============================================================================

def _adsp_env() -> Tuple[str, str]:
    base = (os.getenv("ADSP_API_BASE") or "http://local.adspower.net:50325").rstrip("/")
    token = os.getenv("ADSP_API_TOKEN") or ""
    if not base.startswith("http"):
        base = "http://" + base
    return base, token


def _start_driver(profile_id: str, *, headless: bool, logs: List[str]):
    _log(logs, f"Стартую AdsPower профиль={profile_id} headless={headless}")
    try:
        import importlib  # noqa: WPS433
        adspower = importlib.import_module("ads_ai.browser.adspower")
        start_fn = getattr(adspower, "start_adspower", None) or getattr(adspower, "start", None)
        if not callable(start_fn):
            raise RuntimeError("start_adspower() is not available")
        api_base, token = _adsp_env()
        drv = start_fn(
            profile=profile_id,
            headless=headless,
            api_base=api_base,
            token=token,
            window_size="1600,1000",
        )
        try:
            drv.set_page_load_timeout(35)
            drv.set_script_timeout(20)
            # Понижаем частоту CDP-перерисовок (в headless это дешевле)
            drv.execute_cdp_cmd("Emulation.setDeviceMetricsOverride", {
                "mobile": False, "width": 1600, "height": 1000, "deviceScaleFactor": 1
            })
        except Exception as e:
            _log(logs, f"Предупреждение: не удалось применить CDP-метрики: {e!r}")
        _log(logs, "Драйвер готов")
        return drv
    except Exception as e:  # pragma: no cover
        _log(logs, f"Ошибка старта драйвера: {e!r}")
        raise RuntimeError(f"AdsPower driver failed: {e}")


def _stop_profile_safely(profile_id: str, logs: List[str]) -> None:
    try:
        import importlib  # noqa: WPS433
        adspower = importlib.import_module("ads_ai.browser.adspower")
        for name in ("stop_adspower", "stop", "close"):
            fn = getattr(adspower, name, None)
            if callable(fn):
                api_base, token = _adsp_env()
                fn(profile=str(profile_id), api_base=api_base, token=token)
                _log(logs, "Профиль AdsPower остановлен (best-effort)")
                break
    except Exception as e:
        _log(logs, f"Предупреждение: не удалось остановить профиль AdsPower: {e!r}")


def _close_driver_safely(drv, logs: List[str]) -> None:
    try:
        if drv:
            drv.quit()
            _log(logs, "Драйвер закрыт")
    except Exception as e:
        _log(logs, f"Предупреждение: ошибка при закрытии драйвера: {e!r}")


# =============================================================================
#     Google Ads → Campaigns/Asset groups CSV → INSERT/UPDATE + assets replace
# =============================================================================

def _enable_downloads(drv, target_dir: Path, logs: List[str]) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    _log(logs, f"Папка загрузок: {target_dir}")
    for method, params in [
        ("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": str(target_dir)}),
        ("Browser.setDownloadBehavior", {"behavior": "allow", "downloadPath": str(target_dir)}),
    ]:
        try:
            drv.execute_cdp_cmd(method, params)  # type: ignore
            _log(logs, f"CDP {method} применён")
        except Exception as e:
            _log(logs, f"CDP {method} не поддерживается: {e!r}")


def _go_to_campaigns(drv, logs: List[str], timeout: float = 55.0) -> bool:
    url = "https://ads.google.com/aw/campaigns?hl=en"
    try:
        drv.get(url)
        _log(logs, f"Открыл {url}")
    except Exception as e:
        _log(logs, f"Ошибка при открытии /aw/campaigns: {e!r}")

    end = time.time() + timeout
    while time.time() < end:
        try:
            cur = (drv.current_url or "").lower()
            if "/aw/campaigns" in cur and "assetgroup" not in cur:
                ok = drv.execute_script("""
                    const isVis=e=>{ if(!e) return false; const cs=getComputedStyle(e), r=e.getBoundingClientRect();
                      if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.25||cs.pointerEvents==='none') return false;
                      return r.width>24 && r.height>24 && r.right>0 && r.bottom>0; };
                    const toolbar = document.querySelector('.toolbar, .table-toolbar, [aria-label*="toolbar" i]');
                    const dl1 = [...document.querySelectorAll('button, material-button, [role=button]')].find(b=>{
                      const t=((b.innerText||'') + ' ' + (b.getAttribute('aria-label')||'')).toLowerCase();
                      if(t.includes('download')||t.includes('скачать')||t.includes('export')) return isVis(b);
                      const ic=b.querySelector('i.material-icon-i, i.material-icons, span.material-icons');
                      return ic && (ic.innerText||'').trim().toLowerCase().includes('file_download') && isVis(b);
                    });
                    const table = document.querySelector('[data-automation-id*="table" i], table');
                    return (toolbar && isVis(toolbar)) || (dl1 && isVis(dl1)) || (table && isVis(table));
                """)
                if ok:
                    _log(logs, "Таблица кампаний/тулбар готовы")
                    return True

            drv.execute_script("""
                const isVis=e=>{ if(!e) return false; const cs=getComputedStyle(e), r=e.getBoundingClientRect();
                  if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.25||cs.pointerEvents==='none') return false;
                  return r.width>20 && r.height>20 && r.right>0 && r.bottom>0; };
                const tryClick = (txts)=>{
                  const nodes=[...document.querySelectorAll('a, button, material-list-item, [role=menuitem]')];
                  for(const n of nodes){
                    const t=(n.innerText||n.textContent||'').trim().toLowerCase();
                    for(const tx of txts){
                      if(t===tx && isVis(n)){ try{ n.click(); return true; }catch(e){} }
                    }
                  }
                  return false;
                };
                tryClick(['campaigns','кампании']);
            """)
        except Exception:
            pass
        time.sleep(0.45)  # чуть реже — меньше нагрузка на CDP

    _log(logs, "Не дождался готовности страницы с кампаниями")
    return False


def _go_to_assetgroups(drv, logs: List[str], timeout: float = 55.0) -> bool:
    url = "https://ads.google.com/aw/assetgroup?hl=en"
    try:
        drv.get(url)
        _log(logs, f"Открыл {url}")
    except Exception as e:
        _log(logs, f"Ошибка при открытии /aw/assetgroup: {e!r}")

    end = time.time() + timeout
    while time.time() < end:
        try:
            cur = (drv.current_url or "").lower()
            if "/aw/assetgroup" in cur:
                ok = drv.execute_script("""
                    const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e), r=e.getBoundingClientRect();
                      if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.25||cs.pointerEvents==='none') return false;
                      return r.width>24 && r.height>24 && r.right>0 && r.bottom>0; };
                    const dl=[...document.querySelectorAll('button, material-button, [role=button]')].find(b=>{
                      const t=((b.innerText||'') + ' ' + (b.getAttribute('aria-label')||'')).toLowerCase();
                      if(t.includes('download')||t.includes('скачать')||t.includes('export')) return isVis(b);
                      const ic=b.querySelector('i.material-icon-i, i.material-icons, span.material-icons');
                      return ic && (ic.innerText||'').trim().toLowerCase().includes('file_download') && isVis(b);
                    });
                    return !!dl;
                """)
                if ok:
                    _log(logs, "Страница ассет‑групп готова (видна кнопка Download)")
                    return True
        except Exception:
            pass
        time.sleep(0.4)
    _log(logs, "Не дождался готовности страницы assetgroup")
    return False


def _click_download_csv(drv, logs: List[str], timeout: float = 30.0) -> bool:
    end = time.time() + timeout
    btn = None
    while time.time() < end and not btn:
        try:
            btn = drv.execute_script("""
                const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e), r=e.getBoundingClientRect();
                  if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.25||cs.pointerEvents==='none') return false;
                  return r.width>24 && r.height>24 && r.right>0 && r.bottom>0; };
                const nodes=[...document.querySelectorAll('button, material-button, [role=button]')].filter(isVis);
                for(const b of nodes){
                  const t=((b.innerText||'') + ' ' + (b.getAttribute('aria-label')||'')).toLowerCase();
                  if(t.includes('download')||t.includes('скачать')||t.includes('export')) return b;
                  const ic=b.querySelector('i.material-icon-i, i.material-icons, span.material-icons');
                  if(ic && (ic.innerText||'').trim().toLowerCase().includes('file_download')) return b;
                }
                const more=[...document.querySelectorAll('button[aria-label*="more" i], [aria-label*="другие" i]')].find(isVis);
                return more || null;
            """)
            if btn:
                try: btn.click()
                except Exception: drv.execute_script("arguments[0].click();", btn)
                break
        except Exception:
            pass
        time.sleep(0.25)

    if not btn:
        _log(logs, "Кнопка Download не найдена")
        return False

    end2 = time.time() + 14.0
    while time.time() < end2:
        try:
            ok = drv.execute_script("""
                const isVis=e=>{if(!e) return false; const cs=getComputedStyle(e), r=e.getBoundingClientRect();
                  if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.25||cs.pointerEvents==='none') return false;
                  return r.width>10 && r.height>10 && r.right>0 && r.bottom>0; };
                const items=[...document.querySelectorAll(
                  '.menu-container-group .menu-item, material-select-item.menu-item, [role=menuitem], li, a'
                )].filter(isVis);
                for(const it of items){
                  const t=(it.innerText||it.textContent||'').trim().toLowerCase();
                  if (t==='.csv' || t==='csv' || t.endsWith('\\n.csv') || t.includes('\\n.csv\\n')) {
                    try{ it.click(); }catch(e){ try{ it.querySelector('.menu-item-label')?.click(); }catch(e2){} }
                    return true;
                  }
                }
                for(const it of items){
                  const t=(it.innerText||it.textContent||'').trim().toLowerCase();
                  if(t.includes('.csv')){
                    try{ it.click(); }catch(e){ try{ it.querySelector('.menu-item-label')?.click(); }catch(e2){} }
                    return true;
                  }
                }
                const exp=[...items].find(x=>(x.innerText||'').toLowerCase().includes('export'));
                if(exp){ try{ exp.click(); }catch(e){} }
                return false;
            """)
            if ok:
                return True
        except Exception:
            pass
        time.sleep(0.25)

    _log(logs, "Меню .csv не появилось/не выбралось")
    return False


def _await_csv(download_dir: Path, t_start: float, logs: List[str], timeout: float = 120.0) -> Optional[Path]:
    end = time.time() + timeout
    seen = {p.name for p in download_dir.glob("**/*") if p.is_file()}
    # Быстрое ожидание стабильного размера (учитываем .crdownload в Chrome)
    while time.time() < end:
        for p in download_dir.glob("*.csv"):
            try:
                st = p.stat()
                if p.name in seen and st.st_mtime <= t_start:
                    continue
                sz1 = st.st_size
                time.sleep(0.5)
                st2 = p.stat()
                if st2.st_size > 0 and st2.st_size == sz1:
                    _log(logs, f"Получен CSV: {p} ({st2.st_size} байт)")
                    return p
            except Exception:
                continue
        time.sleep(0.35)
    _log(logs, "CSV не появился вовремя")
    return None


# =============================================================================
#                     ПАРАЛЛЕЛЬНЫЙ СКРАП СТАТУСОВ ИЗ БРАУЗЕРА (DOM)
# =============================================================================

def _scrape_campaigns_statuses_from_dom(drv, logs: List[str]) -> List[Dict[str, str]]:
    """
    Возвращает список словарей:
    [{ "name": "...", "campaign_id": "23109236877", "state": "Enabled|Paused|... (из aria-label)",
       "primary_status": "Eligible | Bid strategy learning | ..."}]
    Работает по текущей странице /aw/campaigns.
    """
    try:
        data = drv.execute_script("""
            const isVis = (e)=>{ if(!e) return false; const cs=getComputedStyle(e), r=e.getBoundingClientRect();
               if(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2||cs.pointerEvents==='none') return false;
               return r.width>10 && r.height>10 && r.right>0 && r.bottom>0; };

            const anchors = [...document.querySelectorAll('ess-cell[essfield="name"] a.ess-cell-link, a.ess-cell-link')]
              .filter(a => isVis(a) && (a.textContent||'').trim());

            const pickPrimary = (row)=>{
               // разные разметки: status-text | reasons-text | просто весь cell
               const cell = row && (row.querySelector('ess-cell[essfield="primary_status"]') || row.querySelector('[essfield="primary_status"]'));
               if(!cell) return '';
               const t1 = cell.querySelector('.status-text, .reasons-text, .reasons-container, .ps-cell') || cell;
               return (t1.innerText||t1.textContent||'').trim();
            };

            const getCampaignId = (href)=>{
               try{
                  const u = new URL(href, location.origin);
                  return (u.searchParams.get('campaignId')||'').trim();
               }catch(_){
                  const m=(href||'').match(/[?&]campaignId=(\\d+)/i);
                  return m?m[1]:'';
               }
            };

            const items = [];
            for(const a of anchors){
               const name = (a.innerText||a.textContent||'').trim();
               if(!name) continue;
               const row = a.closest('.particle-table-row,[role=row]') || document;
               const dot = row.querySelector('ess-cell[essfield="status"] .aw-status div, .aw-status div');
               let state = '';
               if (dot){
                  state = (dot.getAttribute('aria-label')||'').trim();
                  if(!state){
                      const cls = dot.className || '';
                      if(/enabled/i.test(cls)) state='Enabled';
                      else if(/paused/i.test(cls)) state='Paused';
                      else if(/removed|deleted/i.test(cls)) state='Removed';
                  }
               }
               const primary = pickPrimary(row);
               const href = a.getAttribute('href') || '';
               const cid = getCampaignId(href);
               items.push({ name, campaign_id: cid, state, primary_status: primary });
            }
            // Убираем дубликаты по имени+id (берём первый видимый)
            const seen = new Set();
            const out = [];
            for (const it of items){
               const key = (it.campaign_id||'') + '|' + it.name;
               if (!seen.has(key)){ seen.add(key); out.push(it); }
            }
            return out;
        """) or []
        rows: List[Dict[str, str]] = []
        for it in data:
            try:
                rows.append({
                    "name": str(it.get("name") or "").strip(),
                    "campaign_id": str(it.get("campaign_id") or "").strip(),
                    "state": str(it.get("state") or "").strip(),
                    "primary_status": str(it.get("primary_status") or "").strip(),
                })
            except Exception:
                continue
        _log(logs, f"DOM: собрано статусов {len(rows)}")
        return rows
    except Exception as e:
        _log(logs, f"DOM: ошибка чтения таблицы кампаний: {e!r}")
        return []


def _db_merge_ui_status(
    cx: sqlite3.Connection,
    *, company_id: int,
    campaign_id: Optional[str],
    state: Optional[str],
    primary_status: Optional[str],
    logs: Optional[List[str]] = None,
) -> None:
    """Подмешивает gads_ui в extra_json (state/primary_status + метка времени)."""
    cur = cx.execute("SELECT extra_json FROM companies WHERE id=? LIMIT 1", (company_id,))
    r = cur.fetchone()
    old_extra = r["extra_json"] if r else None
    patch = {
        "gads_ui": {
            "scraped_at": _now_iso(),
            "campaign_id": (campaign_id or ""),
            "state": (state or ""),
            "primary_status": (primary_status or ""),
        }
    }
    new_extra = _merge_extra_json(old_extra, patch)
    cx.execute("UPDATE companies SET extra_json=? WHERE id=?", (new_extra, company_id))
    if logs is not None:
        _log(logs, f"UI status merged for company_id={company_id}: state={state!r}; primary={primary_status!r}")


def _apply_ui_statuses_bulk(
    *, user_email: str, profile_id: str,
    scraped: List[Dict[str, str]],
    logs: Optional[List[str]] = None
) -> Tuple[int, Dict[str, Tuple[str, str]], Dict[str, Tuple[str, str]]]:
    """
    Пытается применить статусы к уже существующим компаниям (по gads_id и/или по имени).
    Возвращает: (updated_count, map_by_id, map_by_name)
      map_by_id:   { campaign_id: (state, primary) }
      map_by_name: { name: (state, primary) }
    """
    by_id: Dict[str, Tuple[str, str]] = {}
    by_name: Dict[str, Tuple[str, str]] = {}
    upd = 0
    if not scraped:
        return 0, by_id, by_name

    with _cx() as cx:
        name2id, gads2id = _prefetch_company_maps(cx, user_email=user_email, profile_id=profile_id)

        for it in scraped:
            nm = (it.get("name") or "").strip()
            cid = (it.get("campaign_id") or "").strip()
            st = (it.get("state") or "").strip()
            ps = (it.get("primary_status") or "").strip()

            if cid:
                by_id[cid] = (st, ps)
            if nm:
                by_name[nm] = (st, ps)

            company_id: Optional[int] = None
            if cid:
                company_id = gads2id.get(cid)
                if company_id is None:
                    company_id = _db_find_company_by_gads_id_cx(cx, user_email=user_email, profile_id=profile_id, gads_id=cid)
            if company_id is None and nm:
                company_id = name2id.get(nm)
                if company_id is None:
                    company_id = _db_find_company_by_name_any_cx(cx, user_email=user_email, profile_id=profile_id, name=nm)

            if company_id:
                _db_merge_ui_status(cx, company_id=company_id, campaign_id=cid or None, state=st, primary_status=ps, logs=logs)
                upd += 1

        cx.commit()

    if logs is not None:
        _log(logs, f"UI статусы применены к {upd} компаниям (по уже имеющимся в БД)")
    return upd, by_id, by_name


# =============================================================================
#                     ПРЕДВЫБОРКА МАПИНГОВ (сильно ускоряет UPSERT)
# =============================================================================

def _prefetch_company_maps(
    cx: sqlite3.Connection, *, user_email: str, profile_id: str
) -> Tuple[Dict[str, int], Dict[str, int]]:
    """
    Возвращает (name->id, gads_campaign_id->id) для данного пользователя/профиля.
    Это сильно уменьшает количество SELECT’ов в цикле.
    """
    name2id: Dict[str, int] = {}
    gads2id: Dict[str, int] = {}

    cur = cx.execute(
        "SELECT id, business_name, extra_json FROM companies WHERE user_email=? AND profile_id=?",
        (user_email, profile_id),
    )
    rows = cur.fetchall()
    for r in rows:
        cid = int(r["id"])
        name = (r["business_name"] or "").strip()
        if name and name not in name2id:
            name2id[name] = cid
        try:
            extra = json.loads(r["extra_json"] or "{}")
            gads_id = str(extra.get("gads_import", {}).get("gads_campaign_id") or "")
            if gads_id and gads_id not in gads2id:
                gads2id[gads_id] = cid
        except Exception:
            # упадём назад на LIKE при необходимости
            pass
    return name2id, gads2id


# =============================================================================
#                          СИНХРОННАЯ РАБОТА ПО ПРОФИЛЮ
# =============================================================================

@dataclass
class SyncResult:
    profile_id: str
    ok: bool
    error: Optional[str]
    downloaded: Optional[str]
    parsed_rows: int
    inserted: int
    skipped: int
    # --- assets part ---
    assets_csv: Optional[str]
    assets_parsed_rows: int
    assets_updated: int
    logs: List[str]
    logs_file: Optional[str] = None
    # --- ui statuses ---
    ui_seen: int = 0
    ui_applied: int = 0


def _write_log_file(base_downloads: Path, user_email: str, profile_id: str, logs: List[str]) -> Optional[str]:
    try:
        d = base_downloads.joinpath(user_email.replace("@", "_at_"), profile_id)
        d.mkdir(parents=True, exist_ok=True)
        name = f"sync-{time.strftime('%Y%m%d-%H%M%S')}.log"
        p = d / name
        p.write_text("\n".join(logs), encoding="utf-8")
        return str(p)
    except Exception:
        return None


def _sync_one_profile(
    *, user_email: str, profile_id: str, headless: bool,
    base_downloads: Path, google_email: Optional[str]
) -> SyncResult:
    logs: List[str] = []
    download_dir = base_downloads.joinpath(user_email.replace("@", "_at_"), profile_id)
    download_dir.mkdir(parents=True, exist_ok=True)

    drv = None
    t_start = time.time()
    inserted = 0
    skipped = 0
    parsed = 0
    downloaded: Optional[str] = None
    error: Optional[str] = None
    ok = False
    logs_file: Optional[str] = None

    # assets stage
    assets_csv: Optional[str] = None
    assets_parsed_rows = 0
    assets_updated = 0

    # ui status stats
    ui_seen = 0
    ui_applied = 0

    # карты статусов для последующего апсерта по CSV (чтобы новые компании тоже получили статус)
    scraped_by_id: Dict[str, Tuple[str, str]] = {}
    scraped_by_name: Dict[str, Tuple[str, str]] = {}

    _db_ensure_companies_schema(logs)
    _db_ensure_campaign_stats_schema(logs)
    _log(logs, f"Начинаю синхронизацию профиля {profile_id}")
    try:
        drv = _start_driver(profile_id, headless=headless, logs=logs)
        _enable_downloads(drv, download_dir, logs)

        # -------------------------- CAMPAIGNS CSV + UI STATUSES (ПАРАЛЛЕЛЬНО) --------------------------
        if not _go_to_campaigns(drv, logs=logs, timeout=55.0):
            error = "campaigns_page_unavailable"
        else:
            got_dl = _click_download_csv(drv, logs=logs, timeout=30.0)
            if not got_dl:
                error = "download_menu_unavailable"
                # даже если нет скачивания — статусы всё равно попробуем собрать
                scraped = _scrape_campaigns_statuses_from_dom(drv, logs=logs)
                ui_seen = len(scraped)
                if ui_seen:
                    applied, scraped_by_id, scraped_by_name = _apply_ui_statuses_bulk(
                        user_email=user_email, profile_id=profile_id, scraped=scraped, logs=logs
                    )
                    ui_applied += applied
            else:
                # кликнули .csv → параллельно ждём файл и в это время скрейпим DOM
                t_csv = time.time()

                csv_path_box: List[Optional[Path]] = [None]
                def _waiter():
                    csv_path_box[0] = _await_csv(download_dir, t_csv, logs=logs, timeout=120.0)

                waiter = threading.Thread(target=_waiter, name=f"await-csv-{profile_id}", daemon=True)
                waiter.start()

                # пока файл качается — читаем статусы из DOM и сразу применяем к БД
                scraped = _scrape_campaigns_statuses_from_dom(drv, logs=logs)
                ui_seen = len(scraped)
                if ui_seen:
                    applied, scraped_by_id, scraped_by_name = _apply_ui_statuses_bulk(
                        user_email=user_email, profile_id=profile_id, scraped=scraped, logs=logs
                    )
                    ui_applied += applied

                # дожимаем ожидание CSV
                waiter.join(timeout=130.0)
                csv_path = csv_path_box[0]
                if not csv_path:
                    # если файл так и не появился — но статусы мы уже импортировали
                    error = "csv_not_received"
                else:
                    downloaded = str(csv_path)
                    rows, fieldnames = _parse_gads_csv(csv_path, logs=logs)
                    parsed = len(rows)

                    with _cx() as cx:
                        # Предвыборка соответствий — ключевое ускорение
                        name2id, gads2id = _prefetch_company_maps(cx, user_email=user_email, profile_id=profile_id)

                        for r in rows:
                            cid_s, name, status_txt, curr, bud = _extract_row_fields(r)
                            existing_id = None

                            if name:
                                existing_id = name2id.get(name)

                            if existing_id is None and cid_s:
                                # быстрый путь — из предвыборки
                                existing_id = gads2id.get(cid_s)
                                # fallback — LIKE, если в extra_json не было гads_id
                                if existing_id is None:
                                    existing_id = _db_find_company_by_gads_id_cx(
                                        cx, user_email=user_email, profile_id=profile_id, gads_id=cid_s
                                    )

                            if existing_id:
                                _db_update_company_import(
                                    cx,
                                    company_id=existing_id,
                                    campaign_name=name,
                                    status_text=status_txt,
                                    currency=curr,
                                    budget=bud,
                                    csv_path=downloaded,
                                    csv_fieldnames=fieldnames,
                                    csv_row=r,
                                    logs=logs,
                                )
                                # подмешаем UI-статус, если он есть из DOM
                                st_tuple = None
                                if cid_s and cid_s in scraped_by_id:
                                    st_tuple = scraped_by_id.get(cid_s)
                                elif name and name in scraped_by_name:
                                    st_tuple = scraped_by_name.get(name)
                                if st_tuple:
                                    _db_merge_ui_status(cx, company_id=existing_id, campaign_id=cid_s, state=st_tuple[0], primary_status=st_tuple[1], logs=logs)

                                skipped += 1
                                company_db_id = existing_id
                            else:
                                company_db_id = _db_insert_company_import(
                                    cx,
                                    user_email=user_email,
                                    profile_id=profile_id,
                                    headless=headless,
                                    campaign_id=cid_s,
                                    campaign_name=name,
                                    status_text=status_txt,
                                    currency=curr,
                                    budget=bud,
                                    google_email=google_email,
                                    csv_path=downloaded,
                                    csv_fieldnames=fieldnames,
                                    csv_row=r,
                                    logs=logs,
                                )
                                inserted += 1
                                # обновим map’ы — последующие строки могут сослаться на ту же кампанию
                                if name:
                                    name2id.setdefault(name, company_db_id)
                                if cid_s:
                                    gads2id.setdefault(cid_s, company_db_id)

                                # сразу подмешаем DOM‑статус к новому инсерту, если распознали
                                st_tuple = None
                                if cid_s and cid_s in scraped_by_id:
                                    st_tuple = scraped_by_id.get(cid_s)
                                elif name and name in scraped_by_name:
                                    st_tuple = scraped_by_name.get(name)
                                if st_tuple:
                                    _db_merge_ui_status(cx, company_id=company_db_id, campaign_id=cid_s, state=st_tuple[0], primary_status=st_tuple[1], logs=logs)

                            if company_db_id:
                                _db_log_campaign_stats(cx, company_db_id, r, logs)

                        cx.commit()
                    _log(logs, f"CAMPAIGNS: inserted={inserted}, updated={skipped}. Stats logged.")

        # ------------------------ ASSET GROUPS CSV -------------------------
        # Даже если кампании упали, ассеты пробуем отдельно
        if _go_to_assetgroups(drv, logs=logs, timeout=40.0):
            t_assets = time.time()
            if _click_download_csv(drv, logs=logs, timeout=25.0):
                csv_ag = _await_csv(download_dir, t_assets, logs=logs, timeout=120.0)
                if csv_ag:
                    assets_csv = str(csv_ag)
                    ag_rows, ag_fields = _parse_assetgroup_csv(csv_ag, logs=logs)
                    assets_parsed_rows = len(ag_rows)
                    agg_map = _aggregate_assets_by_campaign(ag_rows)

                    # Быстрое сопоставление имён кампаний
                    with _cx() as cx:
                        name2id, _ = _prefetch_company_maps(cx, user_email=user_email, profile_id=profile_id)

                    for camp_name, agg in agg_map.items():
                        comp_id = name2id.get(camp_name)
                        if comp_id is None:
                            with _cx() as cx:
                                like_pat = _sql_like_json_pair("gads_campaign_name", camp_name)
                                r = cx.execute(
                                    "SELECT id FROM companies WHERE user_email=? AND profile_id=? AND extra_json LIKE ? LIMIT 1",
                                    (user_email, profile_id, like_pat)
                                ).fetchone()
                                comp_id = int(r["id"]) if r else None

                        if comp_id is not None:
                            if _db_replace_company_assets(
                                company_id=comp_id,
                                agg=agg,
                                csv_path=assets_csv,
                                csv_fieldnames=ag_fields,
                                logs=logs,
                            ):
                                assets_updated += 1
                        else:
                            _log(logs, f"Нет соответствия кампании {camp_name!r} в БД — ассеты пропущены")
                else:
                    _log(logs, "Asset groups CSV не получен (timeout)")
            else:
                _log(logs, "Меню Download/.csv на assetgroup не открылось")
        else:
            _log(logs, "Asset groups страница недоступна — пропускаю ассеты")

        ok = error is None

    except Exception as e:
        error = f"{e.__class__.__name__}: {e}"
        _log(logs, f"Критическая ошибка: {error}")
    finally:
        try: _close_driver_safely(drv, logs)
        finally:
            _stop_profile_safely(profile_id, logs)
            logs_file = _write_log_file(base_downloads, user_email, profile_id, logs)

    return SyncResult(
        profile_id=profile_id,
        ok=ok,
        error=error,
        downloaded=downloaded,
        parsed_rows=parsed,
        inserted=inserted,
        skipped=skipped,
        assets_csv=assets_csv,
        assets_parsed_rows=assets_parsed_rows,
        assets_updated=assets_updated,
        logs=logs,
        logs_file=logs_file,
        ui_seen=ui_seen,
        ui_applied=ui_applied,
    )


# =============================================================================
#                           КЭШ СИНХРОНИЗАЦИЙ (TTL)
# =============================================================================

# TTL по умолчанию: 15 мин (900 сек). Можно задать:
#  - GADS_SYNC_CACHE_TTL_SEC (приоритет),
#  - GADS_SYNC_CACHE_TTL_MIN (в минутах).
def _cache_ttl_sec() -> int:
    env_sec = os.getenv("GADS_SYNC_CACHE_TTL_SEC")
    if env_sec and env_sec.isdigit():
        return max(1, int(env_sec))
    env_min = os.getenv("GADS_SYNC_CACHE_TTL_MIN")
    if env_min and env_min.isdigit():
        return max(1, int(env_min)) * 60
    return 900

# Ключ кэша: (user_email, profile_id, headless)
_C_SYNC_LOCK = threading.Lock()
_C_SYNC: Dict[Tuple[str, str, bool], Tuple[float, SyncResult]] = {}

def _cache_key(user_email: str, profile_id: str, headless: bool) -> Tuple[str, str, bool]:
    return (str(user_email), str(profile_id), bool(headless))

def _cache_get(user_email: str, profile_id: str, headless: bool) -> Optional[Tuple[SyncResult, float, float]]:
    key = _cache_key(user_email, profile_id, headless)
    ttl = _cache_ttl_sec()
    now = time.time()
    with _C_SYNC_LOCK:
        rec = _C_SYNC.get(key)
        if not rec:
            return None
        ts, res = rec
        if now - ts < ttl:
            # вернуть (result, ts, ttl_left)
            return res, ts, max(0.0, ttl - (now - ts))
        else:
            # протух — удалим
            _C_SYNC.pop(key, None)
            return None

def _cache_put(user_email: str, profile_id: str, headless: bool, result: SyncResult) -> None:
    key = _cache_key(user_email, profile_id, headless)
    with _C_SYNC_LOCK:
        _C_SYNC[key] = (time.time(), result)

def _cache_invalidate_user(user_email: str) -> None:
    with _C_SYNC_LOCK:
        to_del = [k for k in _C_SYNC.keys() if k[0] == user_email]
        for k in to_del:
            _C_SYNC.pop(k, None)


# =============================================================================
#                           ПУБЛИЧНЫЕ HTTP-ЭНДПОИНТЫ
# =============================================================================

def _require_user_email() -> str:
    email = session.get("user_email")
    if not email:
        raise PermissionError("unauthorized")
    return str(email)


def init_gads_sync(app: Flask, settings: Settings) -> None:
    """
    Эндпоинты:
      • GET /api/gads/campaigns/sync_all?headless=on|off&concurrency=3[&force=1]
      • GET /api/gads/campaigns/sync?profile_id=...&headless=on|off[&force=1]
    Кэш: per-profile, TTL по умолчанию 15 минут.
    """
    _db_ensure_companies_schema()
    _db_ensure_campaign_stats_schema()

    def _result_to_json(r: SyncResult, *, cached: bool, cached_at_ts: Optional[float] = None, ttl_left: Optional[float] = None) -> Dict[str, Any]:
        out = {
            "profile_id": r.profile_id,
            "ok": r.ok,
            "error": r.error,
            "downloaded_csv": r.downloaded or "",
            "parsed_rows": r.parsed_rows,
            "inserted": r.inserted,
            "skipped": r.skipped,
            "assets_csv": r.assets_csv or "",
            "assets_parsed_rows": r.assets_parsed_rows,
            "assets_updated": r.assets_updated,
            "ui_seen": r.ui_seen,
            "ui_applied": r.ui_applied,
            "logs": r.logs,
            "logs_file": r.logs_file or "",
            "from_cache": bool(cached),
        }
        if cached and cached_at_ts is not None:
            out["cached_at"] = datetime.utcfromtimestamp(cached_at_ts).strftime("%Y-%m-%d %H:%M:%S")
            out["cache_ttl_left_sec"] = int(max(0, ttl_left or 0))
        return out

    @app.get("/api/gads/campaigns/sync_all")
    def api_sync_all() -> Response:
        try:
            email = _require_user_email()
        except PermissionError:
            return jsonify({"ok": False, "error": "unauthorized"}), 401

        acc_map = _user_profiles_map(settings, email)
        if not acc_map:
            return jsonify({"ok": True, "profiles": [], "total": 0, "note": "no_linked_profiles"})

        max_conc = int(request.args.get("concurrency") or 3)
        headless = _parse_bool(request.args.get("headless"), default=True)
        force = _parse_bool(request.args.get("force"), default=False)
        base_downloads = Path(os.getenv("ADS_AI_DATA") or (Path(os.getcwd()) / "ads_ai_data")).joinpath("downloads")

        # Разделим профили: уже в кэше и те, что нужно синкать
        cached_results: List[Dict[str, Any]] = []
        to_run_pids: List[str] = []
        for pid in acc_map.keys():
            if not force:
                got = _cache_get(email, pid, headless)
                if got:
                    res, ts, ttl_left = got
                    cached_results.append(_result_to_json(res, cached=True, cached_at_ts=ts, ttl_left=ttl_left))
                    continue
            to_run_pids.append(pid)

        # Выполняем для оставшихся
        sem = threading.Semaphore(max(1, max_conc))
        fresh_results: List[SyncResult] = []
        lock = threading.Lock()

        def worker(pid: str) -> None:
            nonlocal fresh_results
            meta = acc_map.get(pid)
            g_email = (meta.email if meta else "") or ""
            with sem:
                res = _sync_one_profile(
                    user_email=email, profile_id=pid, headless=headless,
                    base_downloads=base_downloads, google_email=g_email
                )
                _cache_put(email, pid, headless, res)  # кладём в кэш вне зависимости от статуса
                with lock:
                    fresh_results.append(res)

        threads: List[threading.Thread] = []
        for pid in to_run_pids:
            t = threading.Thread(target=worker, args=(pid,), name=f"gads-sync-{pid}", daemon=True)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Собираем финальные данные: сначала свежие, затем кэш
        data: List[Dict[str, Any]] = []
        for r in fresh_results:
            data.append(_result_to_json(r, cached=False))
        data.extend(cached_results)

        # Агрегаты по всем результатам (и свежим, и кэшированным)
        total = len(data)
        ok_n = sum(1 for it in data if it.get("ok"))
        ins = sum(int(it.get("inserted") or 0) for it in data)
        skp = sum(int(it.get("skipped") or 0) for it in data)
        upd = sum(int(it.get("assets_updated") or 0) for it in data)

        return jsonify({
            "ok": True,
            "headless": headless,
            "force": force,
            "cache_ttl_sec": _cache_ttl_sec(),
            "profiles": list(acc_map.keys()),
            "total": total,
            "ok_count": ok_n,
            "inserted": ins,
            "skipped": skp,
            "assets_updated": upd,
            "results": data
        })

    @app.get("/api/gads/campaigns/sync")
    def api_sync_one() -> Response:
        try:
            email = _require_user_email()
        except PermissionError:
            return jsonify({"ok": False, "error": "unauthorized"}), 401

        pid = (request.args.get("profile_id") or "").strip()
        if not pid:
            return jsonify({"ok": False, "error": "profile_id required"}), 400

        acc_map = _user_profiles_map(settings, email)
        if pid not in acc_map:
            return jsonify({"ok": False, "error": "forbidden"}), 403

        headless = _parse_bool(request.args.get("headless"), default=True)
        force = _parse_bool(request.args.get("force"), default=False)
        base_downloads = Path(os.getenv("ADS_AI_DATA") or (Path(os.getcwd()) / "ads_ai_data")).joinpath("downloads")
        g_email = (acc_map.get(pid).email if acc_map.get(pid) else "") or ""

        # Кэш
        if not force:
            got = _cache_get(email, pid, headless)
            if got:
                res, ts, ttl_left = got
                payload = _result_to_json(res, cached=True, cached_at_ts=ts, ttl_left=ttl_left)
                return jsonify({
                    "ok": res.ok,
                    "headless": headless,
                    "force": False,
                    "profile_id": res.profile_id,
                    **payload
                })

        # Свежий запуск
        res = _sync_one_profile(
            user_email=email, profile_id=pid, headless=headless,
            base_downloads=base_downloads, google_email=g_email
        )
        _cache_put(email, pid, headless, res)

        payload = _result_to_json(res, cached=False)
        return jsonify({
            "ok": res.ok,
            "headless": headless,
            "force": force,
            "profile_id": res.profile_id,
            **payload
        })


# =============================================================================
#                        CLI-friendly точка входа (необязательно)
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    app = Flask(__name__)
    app.secret_key = "dev"
    app.config["settings"] = Settings()  # type: ignore

    @app.before_request
    def _fake_login():
        session.setdefault("user_email", os.getenv("TEST_USER", "dev@example.com"))

    init_gads_sync(app, app.config["settings"])  # type: ignore
    app.run("0.0.0.0", 5068, debug=True)
