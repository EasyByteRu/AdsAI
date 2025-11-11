# ads_ai/web/company.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import inspect
import json
import mimetypes
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, Response, jsonify, make_response, request, session

# Мягкие зависимости на проектные Settings
try:
    from ads_ai.config.settings import Settings  # noqa: F401
except Exception:  # pragma: no cover
    class Settings:  # simple stub for standalone run
        pass


_SETTINGS_REF: Optional[Settings] = None


def _resolve_llm_model(default: str = "models/gemini-2.0-flash") -> str:
    """Пытается взять модель из переменной окружения или Settings."""
    env_model = (os.getenv("GEMINI_MODEL") or "").strip()
    if env_model:
        return env_model
    settings = _SETTINGS_REF
    try:
        llm = getattr(settings, "llm", None) if settings is not None else None
        model = getattr(llm, "model", None) if llm is not None else None
        if model:
            return str(model)
    except Exception:
        pass
    return default


def _resolve_llm_fallback(default: Optional[str] = None) -> Optional[str]:
    """Возвращает fallback-модель из окружения или Settings (если есть)."""
    env_fb = (os.getenv("GEMINI_FALLBACK_MODEL") or "").strip()
    if env_fb:
        return env_fb
    settings = _SETTINGS_REF
    try:
        llm = getattr(settings, "llm", None) if settings is not None else None
        fb = getattr(llm, "fallback_model", None) if llm is not None else None
        if fb:
            return str(fb)
    except Exception:
        pass
    return default


# -----------------------------------------------------------------------------
# Переиспользуем БД из списка компаний; если недоступен — даём читабельный fallback
# -----------------------------------------------------------------------------
try:
    # Основной контракт/схема — из списка компаний
    from ads_ai.web.list_companies import CompanyDB, CompanyRow  # type: ignore
except Exception:
    @dataclass
    class CompanyRow:  # type: ignore
        id: str
        created_at: str
        profile_id: str
        business_name: str
        website_url: str
        campaign_type: str
        budget_display: str
        locations: str
        languages: str
        n_ads: int
        status: str
        creatives_summary: str
        raw: Dict[str, Any]

    def _pick_db_path() -> str:
        env = (os.getenv("ADS_AI_DB") or "").strip()
        if env:
            return env
        # исторический фоллбек; основная версия хранит БД в ./ads_ai_data/*.sqlite3 (см. list_companies/gads_sync)
        base = os.path.abspath(os.path.join(os.getcwd(), "ads_ai_data"))
        os.makedirs(base, exist_ok=True)
        return os.path.join(base, "companies.sqlite3")

    class CompanyDB:  # type: ignore
        def __init__(self, db_path: Optional[str] = None):
            self.db_path = db_path or _pick_db_path()

        def _connect(self) -> sqlite3.Connection:
            cx = sqlite3.connect(self.db_path)
            cx.row_factory = sqlite3.Row
            return cx

        def get_company(self, cid: str) -> Optional[CompanyRow]:
            with self._connect() as cx:
                cur = cx.execute("SELECT * FROM companies WHERE id = ? LIMIT 1", (cid,))
                row = cur.fetchone()
                if not row:
                    return None
                raw: Dict[str, Any] = {}
                try:
                    # в «основной» версии сырые поля распакованы в _parsed.extra.*; тут храним 'data' как есть
                    raw = json.loads(row["data"] or "{}")
                except Exception:
                    # если в таблице companies «правильная» схема — соберём сырье из колонок
                    try:
                        raw = {
                            "headlines_json": row.get("headlines_json"),
                            "long_headlines_json": row.get("long_headlines_json"),
                            "descriptions_json": row.get("descriptions_json"),
                            "images_json": row.get("images_json"),
                            "image_files_json": row.get("image_files_json"),
                            "extra_json": row.get("extra_json"),
                        }
                    except Exception:
                        raw = {}
                return CompanyRow(
                    id=row["id"],
                    created_at=row["created_at"] or "",
                    profile_id=row["profile_id"] or "",
                    business_name=row["business_name"] or "",
                    website_url=row.get("site_url") or row.get("website_url") or "",
                    campaign_type=row["campaign_type"] or "",
                    budget_display=str(row.get("budget_per_day") or "—"),
                    locations=(row["locations"] or ""),
                    languages=(row["languages"] or ""),
                    n_ads=int(row["n_ads"] or 0),
                    status=(row["status"] or "done"),
                    creatives_summary="",
                    raw=raw,
                )


# --------------------------- AdsPower profile lookup ---------------------------

def _adsp_env() -> Tuple[str, str]:
    base = (os.getenv("ADSP_API_BASE") or "http://local.adspower.net:50325").rstrip("/")
    token = os.getenv("ADSP_API_TOKEN") or ""
    if not re.match(r"^https?://", base, flags=re.I):
        base = "http://" + base
    return base, token


def _http_get_json(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 4.5,
) -> Tuple[int, Dict[str, Any]]:
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
    import urllib.request  # type: ignore
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # type: ignore
            data = resp.read()
            try:
                j = json.loads(data.decode("utf-8")) if data else {}
            except Exception:
                j = {}
            return int(resp.getcode() or 0), j or {}
    except Exception:
        return 0, {}


def _get_adspower_profile(profile_id: str) -> Dict[str, Any]:
    """Возвращает краткую инфу по профилю AdsPower (name, group)."""
    if not profile_id:
        return {"profile_id": "", "name": "", "group_id": ""}
    base, token = _adsp_env()
    headers = {"Authorization": token} if token else {}
    url = f"{base}/api/v1/user/list?page=1&page_size=300"
    code, body = _http_get_json(url, headers=headers, timeout=6.0)
    if not code or not isinstance(body, dict) or str(body.get("code")) not in ("0", "200"):
        return {"profile_id": profile_id, "name": "", "group_id": "", "error": "adspower_api_unavailable"}
    data = body.get("data") or {}
    for it in data.get("list") or []:
        pid = it.get("user_id") or it.get("id") or it.get("profile_id") or it.get("profileId")
        if str(pid) == str(profile_id):
            return {
                "profile_id": str(profile_id),
                "name": str(it.get("name") or it.get("username") or it.get("remark") or ""),
                "group_id": str(it.get("group_id") or it.get("groupId") or ""),
                "tags": it.get("tags") or [],
            }
    return {"profile_id": profile_id, "name": "", "group_id": ""}


# ---------------------------- Вспомогательные утилиты --------------------------

def _require_user_email() -> str:
    email = session.get("user_email")
    if not email:
        raise PermissionError("unauthorized")
    return str(email)


def _db_get_company(db: Any, company_id: str | int) -> Optional[CompanyRow]:
    """
    Унифицированный доступ: если у БД есть get_company → используем его,
    иначе — get (как в list_companies.CompanyDB).
    """
    if hasattr(db, "get_company") and callable(getattr(db, "get_company")):
        return db.get_company(company_id)
    if hasattr(db, "get") and callable(getattr(db, "get")):
        get_fn = getattr(db, "get")
        try:
            cid = int(company_id)
        except Exception:
            cid = company_id  # на всякий
        expects_user = False
        try:
            sig = inspect.signature(get_fn)
            expects_user = "user_email" in sig.parameters
        except (TypeError, ValueError):
            expects_user = False
        if expects_user:
            user_email = _require_user_email()
            return get_fn(cid, user_email=user_email)
        return get_fn(cid)
    raise AttributeError("CompanyDB has neither get_company nor get")


def _by_path(obj: Any, path: str, default: Any = None) -> Any:
    try:
        cur = obj
        for k in path.split("."):
            if not k:
                continue
            cur = cur[k] if isinstance(cur, dict) else getattr(cur, k)
        return cur if cur is not None else default
    except Exception:
        return default


def _try_json(v: Any, default: Any) -> Any:
    """Безопасное преобразование JSON-строки в объект."""
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


def _images_list_from_raw(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Унифицированный список изображений (словарей вида {"file":..., "url":...}):
      1) _parsed.images
      2) images_json (строка/массив) — поддержка {"file"/"abs_path"} и/или {"url"/"src"}
      3) image_files_json (список строк/объектов)
      4) _parsed.extra.context.image_files / _parsed.extra.image_files
      5) raw.image_files (если вдруг есть)
    """
    # 1) основной — распакованный _parsed.images
    arr = _by_path(raw, "_parsed.images", [])
    if isinstance(arr, list) and arr:
        return [x for x in arr if isinstance(x, dict)]

    # 2) images_json (как строка/массив) → список словарей
    imgs = _try_json(raw.get("images_json"), [])
    if isinstance(imgs, list) and imgs:
        out = []
        for it in imgs:
            if not isinstance(it, dict):
                continue
            file_val = it.get("file") or it.get("path") or it.get("filename") or it.get("name") or it.get("abs_path")
            url_val = it.get("url") or it.get("src")
            if file_val or url_val:
                out.append({"file": str(file_val or "").strip(), "url": str(url_val or "").strip()})
        if out:
            return out

    # 3) image_files_json (как строка/массив) → список словарей
    files = _try_json(raw.get("image_files_json"), [])
    if isinstance(files, list) and files:
        out = []
        for it in files:
            if isinstance(it, str) and it.strip():
                out.append({"file": it.strip()})
            elif isinstance(it, dict) and (it.get("file") or it.get("url") or it.get("src") or it.get("abs_path")):
                out.append({"file": it.get("file") or it.get("abs_path") or "", "url": it.get("url") or it.get("src")})
        if out:
            return out

    # 4) из extra.context.image_files / extra.image_files
    ctx_files = _by_path(raw, "_parsed.extra.context.image_files", []) or _by_path(raw, "_parsed.extra.image_files", [])
    if isinstance(ctx_files, list) and ctx_files:
        out = []
        for it in ctx_files:
            if isinstance(it, str) and it.strip():
                out.append({"file": it.strip()})
            elif isinstance(it, dict) and (it.get("file") or it.get("url") or it.get("src") or it.get("abs_path")):
                out.append({"file": it.get("file") or it.get("abs_path") or "", "url": it.get("url") or it.get("src")})
        if out:
            return out

    # 5) raw.image_files
    raw_files = raw.get("image_files")
    if isinstance(raw_files, list) and raw_files:
        out = []
        for it in raw_files:
            if isinstance(it, str) and it.strip():
                out.append({"file": it.strip()})
        if out:
            return out

    return []


# ---------- РЕЗОЛВИНГ ПУТЕЙ К ФАЙЛАМ (логотип/изображения) ----------

_IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".webp", ".gif")

def _pick_data_root(db: Any) -> Path:
    """
    Базовая папка данных:
      1) ADS_AI_DATA (если задан);
      2) каталог, где лежит БД (list_companies.py кладёт её в ./ads_ai_data/*.sqlite3);
      3) ./ads_ai_data (фоллбек).
    """
    env = (os.getenv("ADS_AI_DATA") or "").strip()
    if env:
        base = Path(env).expanduser().resolve()
    else:
        base = None
        # list_companies.CompanyDB экспонирует path/db_path (см. исходники)
        for attr in ("path", "db_path"):
            try:
                val = getattr(db, attr, None)
                if val:
                    base = Path(val).resolve().parent
                    break
            except Exception:  # pragma: no cover
                pass
        if not base:
            base = Path(os.getcwd()).joinpath("ads_ai_data").resolve()
    base.mkdir(parents=True, exist_ok=True)
    return base

def _company_slug(name: str, fallback: str) -> str:
    s = (name or "").lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s or str(fallback)

def _extract_logo_hint(raw: Dict[str, Any]) -> str:
    """Путь к лого из raw, если есть (как есть, без резолвинга)."""
    # основной сценарий (создатель компаний сохраняет сюда)
    path = str(raw.get("logo_file") or "").strip()
    if path:
        return path
    # частые варианты в extra_json
    for p in (
        "_parsed.extra.context.logo_file",
        "_parsed.extra.logo_file",
        "extra_json.context.logo_file",  # вдруг сырое поле
    ):
        v = str(_by_path(raw, p, "")).strip()
        if v:
            return v
    return ""

def _join_if(base: Path, rel: Path) -> Path:
    if rel.is_absolute():
        return rel
    return base.joinpath(rel)

def _normalize_companies_path(data_root: Path, p: Path) -> Path:
    """
    Если в пути есть сегмент 'companies', всё что ПОСЛЕ него
    «перекидываем» под текущий data_root (миграция между машинами).
    """
    parts = list(p.parts)
    if "companies" in parts:
        i = parts.index("companies")
        return data_root.joinpath(*parts[i:])
    return p

def _resolve_company_file(data_root: Path, row: CompanyRow, src: str) -> Optional[Path]:
    """
    Пытаемся найти локальный файл, учитывая разные варианты формата src:
      • абсолютный путь (если файл существует);
      • относительный «companies/images/..» (привяжем к data_root);
      • просто имя файла (поищем в папке компании).
    """
    if not src:
        return None
    s = src.strip()
    # удалённые URL отдаёт фронт напрямую
    if re.match(r"^https?://", s, flags=re.I):
        return None

    p = Path(s)
    # 1) Абсолютный (как есть)
    if p.is_absolute() and p.exists():
        return p

    # 2) Относительный: пробуем «as-is» от корня данных + нормализуем старые пути
    cand = _normalize_companies_path(data_root, p)
    cand = _join_if(data_root, cand)
    if cand.exists():
        return cand

    # 3) По имени — в папке компании (slug и id)
    slug = _company_slug(getattr(row, "business_name", "") or "", str(getattr(row, "id", "")))
    base1 = data_root.joinpath("companies", "images", slug)
    base2 = data_root.joinpath("companies", "images", str(getattr(row, "id", "")))

    for base in (base1, base2):
        c1 = base.joinpath(p.name)
        if c1.exists():
            return c1
        c2 = base.joinpath(p)  # вдруг src имел подпапки
        if c2.exists():
            return c2

    return None

def _fallback_pick_by_index(data_root: Path, row: CompanyRow, idx: int) -> Optional[Path]:
    """
    Если 'file' в raw отсутствует/битый — берём idx-й файл из папки компании.
    Исключаем имена вроде 'logo.*'.
    """
    slug = _company_slug(getattr(row, "business_name", "") or "", str(getattr(row, "id", "")))
    for folder in (
        data_root.joinpath("companies", "images", slug),
        data_root.joinpath("companies", "images", str(getattr(row, "id", ""))),
    ):
        try:
            if not folder.exists():
                continue
            files = sorted(
                [p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in _IMAGE_EXTS and not p.name.lower().startswith("logo")],
                key=lambda x: x.name.lower(),
            )
            if files:
                return files[idx % len(files)]
        except Exception:
            continue
    return None


# ------------------------------- Статистика / LLM -----------------------------

def _companies_db_path_from(db: Any) -> str:
    """
    Унифицированное определение пути к SQLite с компаниями:
    • CompanyDB.path | CompanyDB.db_path | ADS_AI_DB | ./ads_ai_data/companies.sqlite3
    """
    for attr in ("path", "db_path"):
        try:
            v = getattr(db, attr, None)
            if v:
                return str(v)
        except Exception:
            continue
    env = (os.getenv("ADS_AI_DB") or "").strip()
    if env:
        return env
    base = os.path.abspath(os.path.join(os.getcwd(), "ads_ai_data"))
    os.makedirs(base, exist_ok=True)
    return os.path.join(base, "companies.sqlite3")


def _parse_pct_to_float(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    try:
        return float(str(s).strip().replace(",", ".").replace(" ", "").rstrip("%"))
    except Exception:
        return None


def _safe_int(v: Any) -> int:
    try:
        return int(v)
    except Exception:
        return 0


def _safe_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def _currency_from_raw(raw: Dict[str, Any]) -> str:
    """
    Извлекаем код валюты кампании (например, USD / RUB) из extra_json,
    распакованного в _parsed.extra.gads_import.currency (см. gads_sync). 
    """
    cur = _by_path(raw, "_parsed.extra.gads_import.currency", "")
    if not cur:
        # возможно, extra_json не распакован — попробуем как строку JSON
        extra = _try_json(raw.get("extra_json"), {}) or {}
        cur = _by_path(extra, "gads_import.currency", "") or ""
    return (str(cur).strip().upper() or "")


def _stats_query(db_path: str, company_id: int, days: int) -> Dict[str, Any]:
    """
    Возвращает агрегированные метрики и почасовую серию за период.
    Источник — таблица campaign_stats, которую пишет gads_sync при импорте CSV кампаний. 
    """
    series: List[Dict[str, Any]] = []
    sums = {"clicks": 0, "impressions": 0, "cost": 0.0, "conversions": 0.0}
    last_date: Optional[str] = None

    since = None
    if days and days > 0:
        # включаем текущий день: 30d → начиная с (UTC today - 29d)
        dt_from = (datetime.utcnow().date() - timedelta(days=days - 1)).strftime("%Y-%m-%d")
        since = dt_from

    with sqlite3.connect(db_path, check_same_thread=False, timeout=15.0) as cx:
        cx.row_factory = sqlite3.Row
        sql = (
            "SELECT sync_date, clicks, impressions, ctr, avg_cpc, cost, conv_rate, conversions, cost_per_conv "
            "FROM campaign_stats WHERE company_id = ?"
        )
        args: List[Any] = [int(company_id)]
        if since:
            sql += " AND sync_date >= ?"
            args.append(since)
        sql += " ORDER BY sync_date ASC"
        rows = cx.execute(sql, tuple(args)).fetchall()

    for r in rows:
        d = dict(r)
        sync_date = str(d.get("sync_date") or "")
        clicks = _safe_int(d.get("clicks"))
        impressions = _safe_int(d.get("impressions"))
        cost = _safe_float(d.get("cost"))
        conversions = _safe_float(d.get("conversions"))
        ctr = _parse_pct_to_float(d.get("ctr"))  # может прийти "1,23%"
        conv_rate = _parse_pct_to_float(d.get("conv_rate"))
        avg_cpc = _safe_float(d.get("avg_cpc"))
        cpa = _safe_float(d.get("cost_per_conv"))

        series.append({
            "date": sync_date,
            "clicks": clicks,
            "impressions": impressions,
            "cost": cost,
            "conversions": conversions,
            "ctr_pct": ctr,
            "cvr_pct": conv_rate,
            "avg_cpc": avg_cpc,
            "cpa": cpa,
        })

        sums["clicks"] += clicks
        sums["impressions"] += impressions
        sums["cost"] += cost
        sums["conversions"] += conversions
        last_date = sync_date

    # Эффективные агрегаты
    clicks = sums["clicks"]
    impr = sums["impressions"]
    cost = sums["cost"]
    conv = sums["conversions"]

    eff_ctr = (clicks / impr * 100.0) if impr > 0 else None
    eff_cvr = (conv / clicks * 100.0) if clicks > 0 else None
    eff_cpc = (cost / clicks) if clicks > 0 else None
    eff_cpm = (cost / impr * 1000.0) if impr > 0 else None
    eff_cpa = (cost / conv) if conv > 0 else None

    out = {
        "series": series,
        "summary": {
            "clicks": clicks,
            "impressions": impr,
            "cost": round(cost, 4),
            "conversions": round(conv, 4),
            "ctr_pct": None if eff_ctr is None else round(eff_ctr, 4),
            "cvr_pct": None if eff_cvr is None else round(eff_cvr, 4),
            "cpc": None if eff_cpc is None else round(eff_cpc, 4),
            "cpm": None if eff_cpm is None else round(eff_cpm, 4),
            "cpa": None if eff_cpa is None else round(eff_cpa, 4),
            "last_sync_date": last_date or "",
            "days_covered": len(series),
        }
    }
    return out


def _gemini_client_or_none():
    """Пытаемся получить готовый GeminiClient из приложения или создать новый."""
    client = None
    # 1) Попробуем достать из глобального состояния web.app (если есть)
    try:
        import importlib  # noqa: WPS433
        app_mod = importlib.import_module("ads_ai.web.app")
        _state = getattr(app_mod, "_state", None)
        if _state and getattr(_state, "ai", None):
            client = _state.ai
    except Exception:
        client = None
    if client:
        return client
    # 2) Создадим свой инстанс (строго текстовый вызов)
    try:
        from ads_ai.llm.gemini import GeminiClient  # type: ignore
    except Exception:
        GeminiClient = None  # type: ignore
    if GeminiClient is None:
        return None
    model = _resolve_llm_model()
    fallback_model = _resolve_llm_fallback(None)
    try:
        return GeminiClient(model=model, temperature=0.6, retries=1, fallback_model=fallback_model)  # type: ignore
    except Exception:
        return None


def _company_to_prompt_payload(row: CompanyRow, raw: Dict[str, Any], stats: Dict[str, Any]) -> Dict[str, Any]:
    # ассеты — только размеры, чтобы не утонуть в списках
    def _listlen(x: Any) -> int:
        if isinstance(x, list):
            return len(x)
        try:
            arr = json.loads(x) if isinstance(x, str) and x.strip().startswith("[") else []
            return len(arr) if isinstance(arr, list) else 0
        except Exception:
            return 0

    payload = {
        "company": {
            "id": getattr(row, "id", ""),
            "name": getattr(row, "business_name", ""),
            "site": getattr(row, "website_url", ""),
            "type": getattr(row, "campaign_type", ""),
            "status": getattr(row, "status", ""),
            "profile_id": getattr(row, "profile_id", ""),
            "languages": getattr(row, "languages", ""),
            "locations": getattr(row, "locations", ""),
            "budget_display": getattr(row, "budget_display", ""),
            "n_ads": getattr(row, "n_ads", 0),
            "created_at": getattr(row, "created_at", ""),
        },
        "assets_shape": {
            "headlines": _listlen(_by_path(raw, "_parsed.headlines", raw.get("headlines_json"))),
            "long_headlines": _listlen(_by_path(raw, "_parsed.long_headlines", raw.get("long_headlines_json"))),
            "descriptions": _listlen(_by_path(raw, "_parsed.descriptions", raw.get("descriptions_json"))),
            "images": _listlen(_by_path(raw, "_parsed.images", raw.get("images_json") or raw.get("image_files_json"))),
            "keywords": _listlen(_by_path(raw, "_parsed.extra.search_themes", [])),
        },
        "stats": stats or {},
        "extra": {
            "gads_import": _by_path(raw, "_parsed.extra.gads_import", _try_json(raw.get("extra_json"), {}).get("gads_import")),
            "gads_assets": _by_path(raw, "_parsed.extra.gads_assets", _try_json(raw.get("extra_json"), {}).get("gads_assets")),
            "context": _by_path(raw, "_parsed.extra.context", _try_json(raw.get("extra_json"), {}).get("context")),
        }
    }
    return payload


def _ai_insight_for_company(row: CompanyRow, raw: Dict[str, Any], stats: Dict[str, Any]) -> Dict[str, Any]:
    """
    Возвращает {"analysis_text": str, "topics": [...]}.
    Если LLM недоступна — вернём аккуратный фолбэк.
    """
    client = _gemini_client_or_none()
    payload = _company_to_prompt_payload(row, raw, stats)
    currency = _currency_from_raw(raw) or "—"

    if client is None:
        # фолбэк: минимальная аналитика на основе чисел
        s = stats.get("summary") or {}
        clicks = s.get("clicks", 0)
        impr = s.get("impressions", 0)
        conv = s.get("conversions", 0)
        cost = s.get("cost", 0.0)
        ctr = s.get("ctr_pct")
        cvr = s.get("cvr_pct")
        cpc = s.get("cpc")
        cpa = s.get("cpa")
        text = [
            f"Кампания: {row.business_name or '(без названия)'}",
            f"Период: {s.get('days_covered', 0)} дн.  ·  Валюта: {currency}",
            f"Показы: {impr:,}  ·  Клики: {clicks:,}  ·  Конверсии: {conv:,}",
            f"CTR: {ctr or '—'}%  ·  CR: {cvr or '—'}%  ·  CPC: {cpc or '—'}  ·  CPA: {cpa or '—'}  ·  Расход: {cost:.2f} {currency}",
            "",
            "LLM недоступна — показываю резюме по цифрам. Для идей экспериментов запустите синхронизацию и проверьте ключ API.",
        ]
        return {"analysis_text": "\n".join(text), "topics": ["fallback"], "model": "fallback"}

    # Формируем строгий JSON-ответ
    instruction = (
        "Ты — медиастратег и performance‑маркетолог. Тебе передан JSON с краткой информацией о кампании "
        "(company, assets_shape, stats.summary/series, extra). "
        "Сделай компактный аналитический обзор на русском языке. Ответ должен быть **чистым текстом**, без какой-либо Markdown-разметки (никаких **, #, *, -, 1.). "
        "Используй переносы строк для разделения пунктов и абзацев.\n"
        "Структура ответа:\n"
        "1. Описание кампании и ЦА: (твои гипотезы).\n"
        "2. Анализ метрик: (что видно по CTR, CPC, CPA, CR, CPM, приводи конкретные числа и выводы).\n"
        "3. Идеи для экспериментов: (3-6 идей по креативам/таргетингу/бюджету).\n"
        "4. Риски/узкие места: (твои наблюдения).\n"
        "5. Вердикт: (короткое резюме одним абзацем).\n"
        "Важно: никаких дисклеймеров, длинных вступлений. Только деловой и дружелюбный тон. "
        "Верни **строгий JSON** со структурой:\n"
        "{ \"analysis_text\": \"чистый текст анализа\", \"topics\": [\"краткие метки\"] }\n"
        "Не добавляй кодовые блоки и пояснения, только JSON."
    )
    try:
        # У GeminiClient в проекте уже есть удобный метод generate_json (см. step8 usage).
        # Мы отдаём единый текстовый промпт.
        from ads_ai.llm.gemini import GeminiClient  # type: ignore
        if not isinstance(client, GeminiClient):  # type: ignore
            # если client пришёл из _state.ai и это уже GeminiClient — пользуем его; иначе создадим локально
            model = _resolve_llm_model()
            fallback_model = _resolve_llm_fallback(None)
            client = GeminiClient(model=model, temperature=0.6, retries=1, fallback_model=fallback_model)  # type: ignore
        prompt = instruction + "\n\n---\nINPUT JSON:\n" + json.dumps(payload, ensure_ascii=False)
        raw_resp = client.generate_json(prompt)  # type: ignore
        print("[company.ai] Gemini raw response:", raw_resp)
        data = raw_resp if isinstance(raw_resp, dict) else {}
        if not data:
            # Попробуем извлечь JSON-объект из строки
            try:
                text = str(raw_resp)
                m = re.search(r"\{.*\}", text, flags=re.S)
                if m:
                    data = json.loads(m.group(0))
            except Exception:
                data = {}
        if not data:
            print("[company.ai] Gemini response parsed to empty dict; payload was:", payload)
        text_resp = str(data.get("analysis_text") or "").strip()
        topics = data.get("topics") or []
        if text_resp:
            return {"analysis_text": text_resp, "topics": topics, "model": "gemini"}
        # На всякий — короткий фолбэк
        return {"analysis_text": f"Нет ответа модели. Валюта: {currency}. Посмотрите цифры справа, они уже агрегированы.", "topics": ["no-llm-output"], "model": "gemini"}
    except Exception as exc:
        print("[company.ai] Gemini call failed:", repr(exc))
        return {"analysis_text": "Не удалось получить ответ от LLM. Показаны только числовые метрики.", "topics": ["llm-error"], "model": "error"}


# ------------------------------- HTML (detail) --------------------------------

DETAIL_HTML = """<!doctype html>
<html lang="ru" data-theme="light">
<head>
  <meta charset="utf-8" />
  <title>HyperAI — Компания</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    /* === Токены темы === */
    :root{
      --bg:#eef2f7; --bg2:#f6f8fb; --text:#111827; --muted:#6b7280;
      --glass: rgba(255,255,255,.66); --glass-2: rgba(255,255,255,.5);
      --border: rgba(17,24,39,.08); --ring: rgba(17,24,39,.06);
      --neon1:#38bdf8; --neon2:#a78bfa; --neon3:#34d399;
      --ok:#16a34a; --err:#ef4444; --warn:#f59e0b;
      --radius:24px; --radius-sm:16px; --gap:12px;
      --shadow: 0 10px 30px rgba(15,23,42,.12);
      --shadow-big: 0 30px 80px rgba(15,23,42,.18);
      --content-max: 1480px;
    }
    html[data-theme="dark"]{
      color-scheme: dark;
      --bg:#0b1220; --bg2:#0d1423; --text:#e5e7eb; --muted:#94a3b8;
      --glass: rgba(17,23,41,.55); --glass-2: rgba(17,23,41,.45);
      --border: rgba(255,255,255,.09); --ring: rgba(56,189,248,.15);
      --shadow: 0 10px 30px rgba(0,0,0,.35); --shadow-big: 0 30px 80px rgba(0,0,0,.45);
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
    .panel{
      background:var(--glass); border:1px solid var(--border); border-radius:var(--radius);
      backdrop-filter: blur(12px) saturate(160%); box-shadow:var(--shadow); overflow:hidden;
    }

    .menu{ padding:18px; display:flex; flex-direction:column; gap:12px }
    .menu .head{ height:56px; display:flex; align-items:center; gap:10px; padding:0 6px; font-weight:700 }
    .mitem{ display:flex; align-items:center; gap:10px; padding:10px 12px; border-radius:14px; background:var(--glass-2); border:1px solid var(--border); text-decoration:none; color:inherit }
    .mitem.active b{ font-weight:800 }
    .muted{ color:var(--muted) }

    .stage{ position:relative; display:grid; grid-template-rows: auto auto 1fr; gap:14px; padding:18px; }

    .hdr{
      position:relative; border-radius:22px; padding:18px; overflow:hidden;
      background: linear-gradient(135deg, rgba(56,189,248,.15), rgba(167,139,250,.18));
      border:1px solid var(--ring);
    }
    .hdr .top{ display:flex; gap:14px; align-items:center; }
    .logo{
      width:72px; height:72px; border-radius:18px; overflow:hidden;
      background: radial-gradient(circle at 30% 30%, rgba(255,255,255,.9), rgba(255,255,255,.35));
      display:grid; place-items:center; border:1px solid var(--border)
    }
    .logo img{ max-width:100%; max-height:100%; display:block }
    .ttl{ font-weight:800; font-size:20px; letter-spacing:.2px }
    .chips{ display:flex; align-items:center; gap:6px; flex-wrap:wrap }
    .chip{ font-size:12px; padding:4px 8px; border-radius:999px; border:1px solid var(--border); background:rgba(255,255,255,.7) }
    .hdr .meta{ display:flex; gap:14px; align-items:center; flex-wrap:wrap; margin-top:8px; color:var(--muted) }
    .hdr .meta .k{ font-size:12px }

    .grid{ display:grid; grid-template-columns: repeat(12,1fr); gap:12px; align-content:start }
    .x12{ grid-column:1 / -1 } .x8{ grid-column: span 8 } .x4{ grid-column: span 4 } .x6{ grid-column: span 6 } .x3{ grid-column: span 3 }
    @media (max-width: 1200px){ .x8{ grid-column:1/-1 } }
    .card{ padding:14px; border:1px solid var(--border); border-radius:18px; background:rgba(255,255,255,.75); box-shadow:var(--shadow) }
    html[data-theme="dark"] .card{ background:rgba(15,21,38,.65) }
    .card h3{ margin:0 0 8px; font-size:14px; letter-spacing:.3px }

    .kv{ display:grid; grid-template-columns: 1fr 2fr; gap:8px; }
    .kv .k{ color:var(--muted); font-size:12px }
    .kv .v{ font-weight:600; display:flex; align-items:center; gap:8px }

    .toolbar{ display:flex; gap:8px; align-items:center; justify-content:flex-end }
    .btn{ border:1px solid var(--border); background: linear-gradient(180deg, #fff, #f4f7fb); color:var(--text); border-radius: 999px; padding:8px 14px; cursor:pointer }
    .btn.primary{ background: linear-gradient(180deg, var(--neon1), var(--neon2)); color:#00131a; font-weight:800 }
    .btn.ghost{ background:transparent }
    .btn.danger{ background: linear-gradient(180deg, #fee2e2, #fecaca); color:#7f1d1d }
    .btn.smol{ padding:6px 10px; font-size:12px }
    .btn.tgl{ padding:6px 10px; font-size:12px; opacity:.75 }
    .btn.tgl.active{ opacity:1; box-shadow:0 0 0 2px var(--ring) inset }

    .placeholder{ border:1px dashed var(--border); padding:20px; border-radius:14px; text-align:center; color:var(--muted) }
    a{ color: inherit; }

    /* === Сетка креативов === */
    .creative-grid{ display:grid; grid-template-columns: repeat(12,1fr); gap:12px }
    .creative-col{ grid-column: span 4; }
    @media (max-width: 1280px){ .creative-col{ grid-column: span 6; } }
    @media (max-width: 820px){ .creative-col{ grid-column: span 12; } }

    .creative-card{ border:1px solid var(--border); border-radius:18px; overflow:hidden; background:rgba(255,255,255,.9); box-shadow: var(--shadow); display:flex; flex-direction:column; }
    html[data-theme="dark"] .creative-card{ background: rgba(15,21,38,.7) }

    .creative-hero{ position:relative; width:100%; aspect-ratio: 16/9; background:#e5e7eb; display:grid; place-items:center; overflow:hidden; }
    .creative-hero img{ width:100%; height:100%; object-fit:cover; display:block; filter: saturate(102%); }
    .creative-title{ font-weight:800; font-size:18px; letter-spacing:.2px; padding:10px 12px 6px 12px }
    .creative-body{ display:grid; grid-template-columns: auto 1fr auto; gap:10px; padding:8px 12px 12px; align-items:center }
    .creative-logo{ width:42px; height:42px; border-radius:10px; overflow:hidden; border:1px solid var(--border); background: radial-gradient(circle at 30% 30%, rgba(255,255,255,.9), rgba(255,255,255,.5)); display:grid; place-items:center; }
    .creative-logo img{ width:100%; height:100%; object-fit:cover; display:block }
    .creative-desc{ grid-column: 2 / span 1; color:var(--muted); line-height:1.35; font-size:13px }
    .creative-cta{ width:44px; height:44px; border-radius:999px; display:flex; align-items:center; justify-content:center; background: var(--text); color:#fff; border:1px solid var(--border); box-shadow: 0 10px 24px rgba(15,23,42,.15); }
    html[data-theme="dark"] .creative-cta{ background:#e5e7eb; color:#0b1220 }
    .creative-meta{ grid-column: 2 / span 1; font-size:12px; color:var(--muted); margin-top:4px }
    .creative-domain{ font-weight:600; color:inherit }

    .skeleton{ background:#eef2f7; border:1px solid var(--ring); border-radius:18px }
    html[data-theme="dark"] .skeleton{ background:#0f172a }

    /* === Keywords под сеткой === */
    .kw-wrap{ margin-top:12px; padding-top:10px; border-top:1px dashed var(--border); }
    .kw-title{ font-weight:800; font-size:13px; letter-spacing:.2px; margin-bottom:6px; }
    .kw-empty{ color:var(--muted); font-size:12px }

    /* === Статистика: AI + Метрики === */
    .ai-wrap{ display:grid; grid-template-columns: repeat(12,1fr); gap:12px; }
    .ai-pane{ grid-column: span 8; display:flex; flex-direction:column; gap:10px }
    .ai-box{
      border:1px solid var(--border); border-radius:18px; padding:14px;
      background:rgba(255,255,255,.85); box-shadow:var(--shadow); min-height:160px; position:relative;
    }
    html[data-theme="dark"] .ai-box{ background:rgba(15,21,38,.65) }
    .ai-text{ white-space:pre-wrap; line-height:1.48; font-size:14px; }
    .cursor{ display:inline-block; width:8px; background:currentColor; animation:blink 1s steps(1,end) infinite; margin-left:1px; }
    @keyframes blink { 50% { opacity: 0; } }

    .metrics-pane{ grid-column: span 4; display:flex; flex-direction:column; gap:10px }
    .metrics-grid{ display:grid; grid-template-columns: 1fr 1fr; gap:10px }
    .metric{ border:1px solid var(--border); border-radius:14px; padding:10px; background:rgba(255,255,255,.75); box-shadow:var(--shadow) }
    html[data-theme="dark"] .metric{ background:rgba(15,21,38,.6) }
    .metric .k{ color:var(--muted); font-size:12px }
    .metric .v{ font-weight:800; font-size:16px }

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
      <a class="mitem" href="/companies">Создание компаний</a>
      <a class="mitem" href="/companies/list">Список компаний</a>
      <a class="mitem active" href="#" onclick="return false"><b>Карточка компании</b></a>
      <div style="margin-top:auto" class="muted">Powered by EasyByte</div>
    </aside>

    <!-- CENTER -->
    <section class="panel stage">
      <div class="hdr">
        <div class="top">
          <div class="logo" id="logoBox"><div style="width:36px;height:36px;border-radius:12px;background:linear-gradient(135deg,var(--neon1),var(--neon2))"></div></div>
          <div>
            <div class="ttl" id="ttl">Компания</div>
            <div class="chips">
              <span class="chip" id="status">status</span>
              <span class="chip" id="ctype">type</span>
              <span class="chip" id="profile">profile</span>
            </div>
          </div>
          <div style="margin-left:auto" class="toolbar">
            <a class="btn ghost" id="backBtn" href="/companies/list">← Назад к списку</a>
            <button class="btn danger" id="delBtn">Удалить</button>
          </div>
        </div>
        <div class="meta">
          <span class="k">Создана: <b id="created">—</b></span>
          <span class="k">Бюджет: <b id="budget">—</b></span>
          <span class="k">URL: <a id="url" href="#" target="_blank">—</a></span>
        </div>
      </div>

      <div class="grid">
        <div class="card x4">
          <h3>Аккаунт</h3>
          <div class="kv">
            <div class="k">AdsPower профиль</div>
            <div class="v" id="acc_profile">—</div>

            <div class="k">Google Ads</div>
            <div class="v">
              <span id="acc_google">—</span>
              <button class="btn ghost" id="copyCidBtn" title="Скопировать" style="padding:4px 8px">⧉</button>
            </div>

            <div class="k">Языки</div>
            <div class="v"><span id="langs">—</span></div>

            <div class="к">География</div>
            <div class="v"><span id="locs">—</span></div>
          </div>
        </div>

        <div class="card x8">
          <h3>Основные параметры</h3>
          <div class="kv">
            <div class="k">Название</div>
            <div class="v" id="bizname">—</div>
            <div class="k">Тип</div>
            <div class="v" id="ctype2">—</div>
            <div class="k">Состояние</div>
            <div class="v" id="status2">—</div>
            <div class="k">Объявлений</div>
            <div class="v" id="adsN">—</div>
          </div>
        </div>

        <!-- Статистика: AI + Метрики -->
        <div class="card x12">
          <div style="display:flex;align-items:center; gap:10px; justify-content:space-between">
            <h3 style="margin:0">Статистика</h3>
            <div class="toolbar" id="statsToolbar">
              <button class="btn tgl" data-days="7">7 дн</button>
              <button class="btn tgl active" data-days="30">30 дн</button>
              <button class="btn tgl" data-days="90">90 дн</button>
              <button class="btn tgl" data-days="0">всё</button>
              <button class="btn smol" id="regenInsight">Перегенерировать</button>
            </div>
          </div>
          <div class="ai-wrap" style="margin-top:8px">
            <div class="ai-pane">
              <div class="ai-box">
                <div class="ai-text" id="aiText">Готовлю аналитику по кампании… <span class="cursor">&nbsp;</span></div>
              </div>
            </div>
            <div class="metrics-pane">
              <div class="metrics-grid" id="metricsGrid">
                <!-- заполняется динамически -->
              </div>
            </div>
          </div>
        </div>

        <div class="card x12">
          <h3>Креативы</h3>
          <div id="cardsView" class="creative-grid">
            <div class="creative-col"><div class="skeleton" style="aspect-ratio:16/9"></div><div class="skeleton" style="height:88px;margin-top:8px"></div></div>
            <div class="creative-col"><div class="skeleton" style="aspect-ratio:16/9"></div><div class="skeleton" style="height:88px;margin-top:8px"></div></div>
            <div class="creative-col"><div class="skeleton" style="aspect-ratio:16/9"></div><div class="skeleton" style="height:88px;margin-top:8px"></div></div>
          </div>
          <!-- Ключевые слова: ТОЛЬКО чипсы -->
          <div id="kwWrap" class="kw-wrap">
            <div class="kw-title">Ключевые слова</div>
            <div id="kwChips" class="chips"><span class="kw-empty">— нет данных —</span></div>
          </div>
        </div>
      </div>
    </section>
  </div>

  <div class="toasts" id="toasts" style="position:fixed; right:18px; top:18px; display:flex; flex-direction:column; gap:8px; z-index:9999"></div>

<script>
const state = { id: "", data: null, adsp: null, days: 30, typing: null };

/* === helpers === */
function esc(s){ return String(s||"").replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;").replaceAll('"',"&quot;"); }
function chipList(arr){
  const wrap = document.createElement('div'); wrap.className = 'chips';
  (arr||[]).forEach(t=>{ const span = document.createElement('span'); span.className = 'chip'; span.textContent = t; wrap.appendChild(span); });
  return wrap;
}
async function fetchJSON(url){ const r = await fetch(url); if(!r.ok) throw new Error('HTTP '+r.status); return await r.json(); }
function byPath(obj, path, dflt){ try{ let cur = obj; for(const k of path.split('.')){ if(!k) continue; cur = cur?.[k]; } return (cur===undefined||cur===null) ? dflt : cur; }catch(_){ return dflt; } }
function domainFromUrl(u){ try{ const {host} = new URL(u); return host.replace(/^www\\./,''); }catch(_){ return ''; } }
function pick(arr, i, fallback=''){ if(!Array.isArray(arr) || !arr.length) return fallback; return String(arr[i % arr.length]||'').trim() || fallback; }
function parseMaybeJSON(v, dflt){
  if (Array.isArray(v) || (v && typeof v==='object')) return v;
  if (typeof v === 'string'){
    const s = v.trim();
    if (s.startsWith('[') || s.startsWith('{')){ try{ return JSON.parse(s); }catch(_){ return dflt; } }
    if (s) return [s]; // строка → единичный элемент
  }
  return dflt;
}
function fmtNum(x, frac=2){
  if (x===null || x===undefined || isNaN(Number(x))) return '—';
  const n = Number(x);
  const f = (frac===0) ? 0 : Math.min(Math.max(frac, 0), 4);
  return n.toLocaleString('ru-RU', { minimumFractionDigits: (n<10&&f>0)?Math.min(2,f):0, maximumFractionDigits: f });
}

/* === toasts === */
function toast(msg, kind='ok'){
  const box = document.getElementById('toasts');
  const div = document.createElement('div');
  div.style.padding='10px 12px'; div.style.borderRadius='12px'; div.style.border='1px solid rgba(17,24,39,.12)';
  div.style.background='rgba(255,255,255,.96)'; div.style.boxShadow='0 10px 30px rgba(15,23,42,.12)';
  div.style.color = (kind==='err' ? '#991b1b' : '#065f46');
  div.textContent = msg;
  box.appendChild(div);
  setTimeout(()=> div.remove(), 1600);
}

/* === Cards builder === */
function gatherTexts(raw, keyParsed, keyJson){
  let arr = byPath(raw, keyParsed, []);
  arr = Array.isArray(arr) ? arr : [];
  if (!arr.length){
    const alt = parseMaybeJSON(byPath(raw, keyJson, []), []);
    if (Array.isArray(alt) && alt.length) arr = alt;
  }
  return arr;
}

function gatherImages(raw){
  // 1) _parsed.images как есть
  let IM = byPath(raw, '_parsed.images', []);
  if (Array.isArray(IM) && IM.length) return IM;

  // 2) images_json — строка/массив со словарями
  const imgsJson = parseMaybeJSON(byPath(raw, 'images_json', []), []);
  if (Array.isArray(imgsJson) && imgsJson.length){
    return imgsJson.map(x=>{
      if (typeof x !== 'object' || !x) return {};
      const file = String(x.file || x.path || x.filename || x.name || x.abs_path || '').trim();
      const url  = String(x.url  || x.src || '').trim();
      return { file, url };
    });
  }

  // 3) image_files_json — список строк или объектов
  const files = parseMaybeJSON(byPath(raw,'image_files_json', []), []);
  if (Array.isArray(files) && files.length){
    return files.map(x=>{
      if (typeof x === 'string') return { file: x };
      if (typeof x === 'object' && x) return { file: x.file || x.abs_path || '', url: x.url || x.src || '' };
      return {};
    });
  }

  // 4) контекстные списки
  const ctx = byPath(raw, '_parsed.extra.context.image_files', []) || byPath(raw, '_parsed.extra.image_files', []);
  if (Array.isArray(ctx) && ctx.length){
    return ctx.map(x=>{
      if (typeof x === 'string') return { file: x };
      if (typeof x === 'object' && x) return { file: x.file || x.abs_path || '', url: x.url || x.src || '' };
      return {};
    });
  }

  // 5) raw.image_files
  const rf = byPath(raw, 'image_files', []);
  if (Array.isArray(rf) && rf.length){
    return rf.map(x=> (typeof x==='string' ? {file:x} : (x||{})) );
  }

  return [];
}

function buildCards(){
  const row = state.data?.row || {};
  const raw = state.data?.raw || {};
  const cardsBox = document.querySelector('#cardsView');
  cardsBox.innerHTML = '';

  const H  = gatherTexts(raw, '_parsed.headlines', 'headlines_json');
  const LH = gatherTexts(raw, '_parsed.long_headlines', 'long_headlines_json');
  const D  = gatherTexts(raw, '_parsed.descriptions', 'descriptions_json');
  const IM = gatherImages(raw);

  // nAds: берём из row.n_ads, иначе максимум по длинам ассетов (≥1)
  const nAds = Math.max(1, Number(row.n_ads||0) || Math.max(H.length, LH.length, D.length, Array.isArray(IM)?IM.length:0, 1));

  const logoPath = byPath(raw, 'logo_file', '') 
                || byPath(raw, '_parsed.extra.context.logo_file', '')
                || byPath(raw, '_parsed.extra.logo_file', '');
  const logoURL = logoPath ? `/api/company/${encodeURIComponent(state.id)}/file?kind=logo` : '';

  const domain = domainFromUrl(row.website_url||'');

  if (!H.length && !LH.length && !D.length && (!Array.isArray(IM) || !IM.length)){
    const ph = document.createElement('div');
    ph.className = 'placeholder';
    ph.textContent = 'Нет ассетов для сборки карточек. Добавьте заголовки/описания и изображения.';
    cardsBox.appendChild(ph);
    return;
  }

  for(let i=0;i<nAds;i++){
    const title = pick(H.length?H:LH, i, 'Your headline');
    const desc  = pick(D, i, 'Short benefit-led description with CTA.');

    // Источник картинки
    let imgSrc = '';
    if (Array.isArray(IM) && IM.length){
      const it = IM[i % IM.length] || {};
      const hasLocal = String(it.file||'').trim().length>0;
      const remote = (it.url||it.src||'');
      imgSrc = hasLocal
        ? `/api/company/${encodeURIComponent(state.id)}/file?kind=image&idx=${(i % IM.length)}`
        : (remote||'');
    }

    const col = document.createElement('div'); col.className = 'creative-col';
    const card = document.createElement('div'); card.className = 'creative-card';

    const hero = document.createElement('div'); hero.className = 'creative-hero';
    if (imgSrc){
      const im = document.createElement('img'); im.loading='lazy'; im.src = imgSrc; im.alt = 'ad image'; hero.appendChild(im);
    }else{
      hero.innerHTML = '<svg viewBox="0 0 24 24" width="40" height="40" fill="none"><rect x="3" y="4" width="18" height="14" rx="2" stroke="#9ca3af" stroke-width="1.6"/><path d="M7 14l3-3 3 3 4-4 2 2" stroke="#9ca3af" stroke-width="1.6"/></svg>';
    }
    card.appendChild(hero);

    const t = document.createElement('div'); t.className='creative-title'; t.textContent = title;
    card.appendChild(t);

    const body = document.createElement('div'); body.className='creative-body';

    const l = document.createElement('div'); l.className='creative-logo';
    if (logoURL){
      const li = document.createElement('img'); li.loading='lazy'; li.src = logoURL; li.alt='logo'; l.appendChild(li);
    }else{
      l.innerHTML = '<svg viewBox="0 0 24 24" width="18" height="18" fill="none"><rect x="3" y="3" width="18" height="18" rx="4" stroke="#94a3b8"/></svg>';
    }

    const d = document.createElement('div'); d.className='creative-desc'; d.textContent = desc;
    const cta = document.createElement('div'); cta.className='creative-cta'; cta.innerHTML = '<svg viewBox="0 0 24 24" width="18" height="18" fill="none"><path d="M5 12h14M13 6l6 6-6 6" stroke="currentColor" stroke-width="2"/></svg>';

    const meta = document.createElement('div'); meta.className='creative-meta';
    meta.innerHTML = domain ? ('<span class="creative-domain">'+esc(domain)+'</span>  ·  Ad') : 'Ad';

    body.appendChild(l);
    body.appendChild(d);
    body.appendChild(cta);
    body.appendChild(document.createElement('div'));
    body.appendChild(meta);

    card.appendChild(body);
    col.appendChild(card);
    cardsBox.appendChild(col);
  }
}

/* === Keywords helpers (нормализация + сплит) === */
function _uniqueStrings(list){
  const seen = new Set(); const out = [];
  (list||[]).forEach(x=>{
    const t = String(x||'').trim();
    if (!t) return;
    const k = t.toLowerCase();
    if (!seen.has(k)){ seen.add(k); out.push(t); }
  });
  return out;
}

function _flattenKw(val, out){
  if (Array.isArray(val)){
    val.forEach(v=>_flattenKw(v, out));
    return;
  }
  if (val && typeof val === 'object'){
    const maybe = val.text || val.name || val.label || val.value || '';
    if (maybe) _flattenKw(String(maybe), out);
    return;
  }
  if (typeof val !== 'string') return;

  // NBSP → space, trim
  let s = val.replace(/\\u00a0/g, ' ').trim();
  if (!s) return;

  // Явные разделители: перевод строки, запятая, ;, |, •, ·
  let parts = s.split(/[\\n\\r;,|•·]+/g);
  if (parts.length === 1){
    parts = [s];
  }
  for (let p of parts){
    p = p.trim().replace(/\\s+/g, ' ');
    if (!p) continue;
    if (p.length > 110) continue; // отсечь явные «поросячьи хвосты»
    out.push(p);
  }
}

/** Глубокий сбор «search themes» и родственных списков из extra_json. */
function gatherKeywords(raw){
  const buf = [];

  // Предопределённые пути (быстрые)
  const fastPaths = [
    '_parsed.extra.keywords',
    'extra.keywords',
    '_parsed.keywords',
    'keywords',

    // search themes / topics
    '_parsed.extra.search_themes',
    'extra.search_themes',
    '_parsed.extra.topics',
    'extra.topics',

    // частые структуры из импорта GAds
    '_parsed.extra.gads_assets.search_themes',
    'gads_assets.search_themes',
    '_parsed.extra.gads_assets.keywords',
    'gads_assets.keywords',

    // ручной ввод тем во втором варианте: context.values
    '_parsed.extra.context.values',
    'extra.context.values',
    'values',
  ];
  for (const p of fastPaths){
    _flattenKw(byPath(raw, p, []), buf);
  }

  // Медленный «умный» обход: ищем по ключам в любом месте _parsed.extra
  const root = byPath(raw, '_parsed.extra', {});
  const wantKeys = new Set(['searchthemes','search_themes','values','keywords','topics','audience_signal','audiencesignal']);
  const stack = [{k:'', v:root, depth:0}];
  while (stack.length){
    const {k, v, depth} = stack.pop();
    if (depth > 6) continue; // ограничим глубину на всякий случай
    if (Array.isArray(v)){
      // если ключ «интересный» — воспринимаем как набор кандидатов
      if (wantKeys.has((k||'').toLowerCase().replace(/\\s+/g,''))){
        _flattenKw(v, buf);
      } else {
        v.forEach(x=> stack.push({k:'', v:x, depth:depth+1}));
      }
      continue;
    }
    if (v && typeof v === 'object'){
      // если значение-строка у интересного ключа
      if (wantKeys.has((k||'').toLowerCase().replace(/\\s+/g,''))){
        _flattenKw(v, buf);
      }
      for (const kk of Object.keys(v)){
        stack.push({k:kk, v:v[kk], depth:depth+1});
      }
    } else if (typeof v === 'string' && wantKeys.has((k||'').toLowerCase().replace(/\\s+/g,''))){
      _flattenKw(v, buf);
    }
  }

  return _uniqueStrings(buf);
}

function renderKeywords(){
  const raw = state.data?.raw || {};
  const wrap = document.getElementById('kwWrap');
  const box = document.getElementById('kwChips');
  if (!wrap || !box) return;

  // Полностью очищаем контейнер (никаких прилипших текстовых узлов)
  box.textContent = '';

  const kws = gatherKeywords(raw);
  if (!kws.length){
    const empty = document.createElement('span'); empty.className='kw-empty'; empty.textContent='— нет данных —';
    box.appendChild(empty);
    return;
  }
  for (const w of kws){
    const chip = document.createElement('span'); chip.className='chip'; chip.textContent = w;
    box.appendChild(chip);
  }
}

/* === AI typing === */
function typeText(elem, text){
  if (state.typing){ clearTimeout(state.typing); state.typing = null; }
  const speedBase = 14; // мс/символ
  const punctPause = { '.': 180, ',': 90, '!': 220, '?': 220, ';': 140, ':': 120, '\\n': 80 };
  const s = String(text||'').replace(/\\r/g,'');
  elem.textContent = '';
  const cursor = document.createElement('span'); cursor.className='cursor'; cursor.innerHTML='&nbsp;';
  elem.appendChild(cursor);
  let i = 0;
  function step(){
    if (i >= s.length){ cursor.remove(); return; }
    const ch = s[i++];
    cursor.insertAdjacentText('beforebegin', ch);
    let delay = speedBase + Math.random()*10;
    if (punctPause[ch] !== undefined) delay += punctPause[ch];
    if (ch === '\\n') delay += 80;
    state.typing = setTimeout(step, delay);
  }
  step();
}

/* === Stats (fetch + render) === */
function renderMetrics(summary, currency){
  const g = document.getElementById('metricsGrid');
  g.innerHTML = '';
  const pairs = [
    ['Период, дн', fmtNum(summary?.days_covered||0, 0)],
    ['Показы', fmtNum(summary?.impressions||0, 0)],
    ['Клики', fmtNum(summary?.clicks||0, 0)],
    ['CTR, %', fmtNum(summary?.ctr_pct, 2)],
    ['Конверсии', fmtNum(summary?.conversions||0, 2)],
    ['CR, %', fmtNum(summary?.cvr_pct, 2)],
    ['Расход, '+(currency||''), fmtNum(summary?.cost||0, 2)],
    ['CPC', fmtNum(summary?.cpc, 2)],
    ['CPA', fmtNum(summary?.cpa, 2)],
    ['CPM', fmtNum(summary?.cpm, 2)],
  ];
  for(const [k,v] of pairs){
    const box = document.createElement('div'); box.className='metric';
    const kk = document.createElement('div'); kk.className='k'; kk.textContent = k;
    const vv = document.createElement('div'); vv.className='v'; vv.textContent = v;
    box.appendChild(kk); box.appendChild(vv);
    g.appendChild(box);
  }
}

async function loadInsight(){
  const id = encodeURIComponent(state.id);
  const url = `/api/company/${id}/insight?days=${encodeURIComponent(String(state.days||30))}`;
  const data = await fetchJSON(url);
  const currency = data?.stats?.currency || '—';
  renderMetrics(data?.stats?.summary||{}, currency);
  const ai = data?.ai || {};
  const el = document.getElementById('aiText');
  const text = String(ai?.analysis_text || 'Нет данных по аналитике.');
  typeText(el, text);
}

/* === render === */
function chipListInline(csv){
  const arr = String(csv||'').split(',').map(s=>s.trim()).filter(Boolean);
  return chipList(arr);
}
function render(){
  const row = state.data?.row || {};
  const raw = state.data?.raw || {};
  document.title = "Компания — " + (row.business_name || row.website_url || row.id);

  document.querySelector('#ttl').textContent = row.business_name || '—';

  document.querySelector('#status').textContent = row.status || '—';
  document.querySelector('#ctype').textContent = row.campaign_type || '—';
  document.querySelector('#profile').textContent = row.profile_id || '—';

  document.querySelector('#created').textContent = row.created_at || '—';
  document.querySelector('#budget').textContent = row.budget_display || '—';
  const a = document.querySelector('#url'); a.textContent = row.website_url || '—'; if(row.website_url){ a.href = row.website_url; }

  document.querySelector('#bizname').textContent = row.business_name || '—';
  document.querySelector('#ctype2').textContent = row.campaign_type || '—';
  document.querySelector('#status2').textContent = row.status || '—';
  document.querySelector('#adsN').textContent = (row.n_ads||0).toString();

  const langs = document.querySelector('#langs'); langs.innerHTML=''; langs.appendChild(chipListInline(row.languages||''));
  const locs  = document.querySelector('#locs');  locs.innerHTML='';  locs.appendChild(chipListInline(row.locations||''));

  // AdsPower name
  const accProfile = document.querySelector('#acc_profile');
  const ap = state.adsp || {};
  let prof = row.profile_id || '';
  if(ap && ap.name){ prof = (ap.name || '(no name)') + ' — ' + (ap.profile_id || row.profile_id || ''); }
  accProfile.textContent = prof || '—';

  // Google Ads CID (из extra.context.*)
  const gads = byPath(raw, '_parsed.extra.context.google_customer_id', '')
            || byPath(raw, '_parsed.extra.context.customer_id', '')
            || byPath(raw, '_parsed.extra.context.cid', '');
  document.querySelector('#acc_google').textContent = gads || '—';

  // Логотип
  const logoBox = document.querySelector('#logoBox'); logoBox.innerHTML = '';
  const logo = byPath(raw,'logo_file','') || byPath(raw,'_parsed.extra.context.logo_file','') || byPath(raw,'_parsed.extra.logo_file','');
  if(logo){
    const img = document.createElement('img');
    img.src = `/api/company/${encodeURIComponent(state.id)}/file?kind=logo`;
    img.alt = 'logo'; logoBox.appendChild(img);
  }else{
    const ph = document.createElement('div');
    ph.style.width='36px'; ph.style.height='36px'; ph.style.borderRadius='12px';
    ph.style.background='linear-gradient(135deg,var(--neon1),var(--neon2))';
    logoBox.appendChild(ph);
  }

  // Карточки + ключевые слова
  buildCards();
  renderKeywords();

  // Статистика
  loadInsight().catch(()=>{ /* уже показан текст-заглушка */ });
}

/* === boot === */
async function boot(){
  try{
    const m = location.pathname.match(/\\/company\\/([^\\/?#]+)/);
    const id = decodeURIComponent(m ? m[1] : '');
    state.id = id;

    // Подгружаем данные и профиль параллельно
    const [data, adsp] = await Promise.all([
      fetchJSON(`/api/company/${encodeURIComponent(id)}`),
      fetchJSON(`/api/company/${encodeURIComponent(id)}/profile`)
    ]);
    state.data = data;
    state.adsp = adsp || {};
    render();
  }catch(e){
    document.body.innerHTML = '<div style="padding:40px;font:14px/1.45 sans-serif">Ошибка загрузки: '+ (e?.message||e) +'</div>';
  }
}

/* === UX actions === */
document.addEventListener('DOMContentLoaded', ()=>{
  document.querySelector('#delBtn').addEventListener('click', async ()=>{
    if(!confirm('Удалить эту компанию навсегда?')) return;
    const r = await fetch('/api/companies/delete', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ ids:[state.id] }) });
    const j = await r.json().catch(()=>({}));
    if(j?.ok){ 
      const box = document.getElementById('toasts');
      const div = document.createElement('div'); div.style.padding='10px 12px'; div.style.border='1px solid rgba(17,24,39,.12)'; div.style.borderRadius='12px';
      div.style.background='rgba(255,255,255,.96)'; div.textContent='Удалено'; box.appendChild(div); setTimeout(()=>location.href='/companies/list', 600);
    } else {
      toast('Ошибка удаления', 'err');
    }
  });

  document.querySelector('#copyCidBtn').addEventListener('click', async ()=>{
    const v = String(document.querySelector('#acc_google').textContent||'').trim();
    if(!v || v==='—'){ return; }
    try{ await navigator.clipboard.writeText(v); toast('CID скопирован'); }catch(_){ toast('Не удалось скопировать','err'); }
  });

  // Переключатели периода
  document.querySelectorAll('#statsToolbar .tgl').forEach(btn=>{
    btn.addEventListener('click', ()=>{
      document.querySelectorAll('#statsToolbar .tgl').forEach(b=>b.classList.remove('active'));
      btn.classList.add('active');
      const days = parseInt(btn.getAttribute('data-days')||'30', 10);
      state.days = days;
      loadInsight().catch(()=>{});
    });
  });

  document.querySelector('#regenInsight').addEventListener('click', ()=> loadInsight().catch(()=>{}));

  boot();
});
</script>
</body>
</html>
"""

# ------------------------------ Routes registration ----------------------------

def _safe_open(path: str) -> Optional[Tuple[bytes, str]]:
    """Открывает файл и возвращает (bytes, mimetype)."""
    if not path:
        return None
    p = Path(path)
    if not p.exists() or not p.is_file():
        return None
    try:
        data = p.read_bytes()
        mt, _ = mimetypes.guess_type(p.name)
        return data, mt or "application/octet-stream"
    except Exception:
        return None


def init_company(app: Flask, settings: Settings) -> None:
    """
    Регистрирует:
      • /company/<id> — детальная карточка
      • /api/company/<id> — JSON с полными данными (row + raw)
      • /api/company/<id>/profile — краткая инфа о профиле AdsPower
      • /api/company/<id>/file?kind=logo|image&idx=0 — безопасная раздача локальных артефактов
      • /api/company/<id>/stats?days=30 — агрегированные метрики кампании
      • /api/company/<id>/insight?days=30 — метрики + мнение LLM по данным кампании
    """
    global _SETTINGS_REF
    _SETTINGS_REF = settings

    db = CompanyDB()
    data_root = _pick_data_root(db)

    @app.get("/company/<company_id>")
    def company_page(company_id: str) -> Response:
        return make_response(DETAIL_HTML, 200)

    @app.get("/api/company/<company_id>")
    def api_company(company_id: str) -> Response:
        try:
            row = _db_get_company(db, company_id)
        except PermissionError:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        if not row:
            return jsonify({"ok": False, "error": "not_found"}), 404
        data = {
            "ok": True,
            "row": {
                "id": row.id,
                "created_at": row.created_at,
                "profile_id": row.profile_id,
                "business_name": row.business_name,
                "website_url": row.website_url,
                "campaign_type": row.campaign_type,
                "budget_display": row.budget_display,
                "locations": row.locations,
                "languages": row.languages,
                "n_ads": row.n_ads,
                "status": row.status,
                "creatives_summary": row.creatives_summary,
            },
            "raw": row.raw or {},
        }
        return jsonify(data)

    @app.get("/api/company/<company_id>/profile")
    def api_company_profile(company_id: str) -> Response:
        try:
            row = _db_get_company(db, company_id)
        except PermissionError:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        if not row:
            return jsonify({"ok": False, "error": "not_found"}), 404
        prof = _get_adspower_profile(row.profile_id or "")
        return jsonify(prof)

    @app.get("/api/company/<company_id>/file")
    def api_company_file(company_id: str) -> Response:
        """
        kind=logo|image
        idx=номер для собранного списка изображений (0..)
        """
        kind = (request.args.get("kind") or "").strip().lower()
        idx = request.args.get("idx")
        try:
            idx_i = int(idx) if idx is not None else 0
        except Exception:
            idx_i = 0

        try:
            row = _db_get_company(db, company_id)
        except PermissionError:
            return make_response("unauthorized", 401)
        if not row:
            return make_response("not_found", 404)

        raw = row.raw or {}
        src_path: Optional[Path] = None

        if kind == "logo":
            logo_src = _extract_logo_hint(raw)
            src_path = _resolve_company_file(data_root, row, logo_src) if logo_src else None
            # Доп. фоллбек: logo.* в папке компании
            if not src_path:
                slug = _company_slug(getattr(row, "business_name", "") or "", str(getattr(row, "id", "")))
                for base in (
                    data_root.joinpath("companies", "images", slug),
                    data_root.joinpath("companies", "images", str(getattr(row, "id", ""))),
                ):
                    if base.exists():
                        for ext in (".png", ".jpg", ".jpeg", ".webp"):
                            c = base.joinpath(f"logo{ext}")
                            if c.exists():
                                src_path = c
                                break
                    if src_path:
                        break

        elif kind == "image":
            images = _images_list_from_raw(raw)
            if 0 <= idx_i < len(images):
                it = images[idx_i] or {}
                # Приоритет: file/path/filename/name/abs_path
                for key in ("file", "path", "filename", "name", "abs_path"):
                    v = str(it.get(key) or "").strip()
                    if not v:
                        continue
                    src_path = _resolve_company_file(data_root, row, v)
                    if src_path:
                        break
            if not src_path:
                # Мягкий фоллбек — берём idx-й файл из папки компании
                src_path = _fallback_pick_by_index(data_root, row, idx_i)
        else:
            return make_response("bad kind", 400)

        if not src_path:
            return make_response("no file", 404)

        opened = _safe_open(str(src_path))
        if not opened:
            return make_response("no file", 404)
        data, mt = opened
        resp = make_response(data, 200)
        resp.headers["Content-Type"] = mt
        resp.headers["Cache-Control"] = "public, max-age=60"
        return resp

    # -------------------------- НОВОЕ: агрегированные метрики -----------------

    @app.get("/api/company/<company_id>/stats")
    def api_company_stats(company_id: str) -> Response:
        try:
            row = _db_get_company(db, company_id)
        except PermissionError:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        if not row:
            return jsonify({"ok": False, "error": "not_found"}), 404

        try:
            days = int(request.args.get("days") or 30)
            days = max(0, min(days, 3650))
        except Exception:
            days = 30

        db_path = _companies_db_path_from(db)
        stats = _stats_query(db_path, int(getattr(row, "id", company_id)), days)

        # добавим валюту из extra_json (см. gads_sync импорт)
        cur = _currency_from_raw(row.raw or {})
        stats["currency"] = cur or ""
        return jsonify({"ok": True, "company_id": row.id, "stats": stats})

    # --------------------- НОВОЕ: LLM‑инсайт + метрики вместе ------------------

    @app.get("/api/company/<company_id>/insight")
    def api_company_insight(company_id: str) -> Response:
        try:
            row = _db_get_company(db, company_id)
        except PermissionError:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        if not row:
            return jsonify({"ok": False, "error": "not_found"}), 404

        try:
            days = int(request.args.get("days") or 30)
            days = max(0, min(days, 3650))
        except Exception:
            days = 30

        db_path = _companies_db_path_from(db)
        stats = _stats_query(db_path, int(getattr(row, "id", company_id)), days)
        cur = _currency_from_raw(row.raw or {})
        stats["currency"] = cur or ""

        ai = _ai_insight_for_company(row, row.raw or {}, {"summary": stats.get("summary"), "series": stats.get("series"), "currency": cur})
        return jsonify({"ok": True, "company_id": row.id, "stats": stats, "ai": ai})
