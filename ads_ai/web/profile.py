# -*- coding: utf-8 -*-
"""
ads_ai/web/profile.py

Личный кабинет (Profile) для Ads AI Agent:

* Sidebar-меню (Главная, Mission Control, Артефакты, Настройки, Выход).
* /profile — Обзор (KPI + мини‑графики, inline SVG, без внешних зависимостей).
* /profile/settings — настройки профиля и смена пароля (валидация + CSRF).
* Хранилище: та же SQLite-база (auth.db), таблица metrics_daily для статистики.
* Безопасность: CSP, строгие кэши, HSTS (за HTTPS), сессионная защита.
* Шаблоны через string.Template (никаких f‑строк в HTML/JS).

ENV / Settings.integrations (не обязательно):
* CURRENCY_SIGN="₽"   # знак валюты в карточках KPI (по умолчанию — ₽)
"""

from __future__ import annotations

import os
import re
import hmac
import math
import time
import html
import base64
import hashlib
import secrets
import sqlite3
import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple
from string import Template

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


# ============================ Крипто / PBKDF2 (совместимо с auth.py) ============================

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


# ============================ Хелперы преобразования/форматирования ============================

def _escape(s: str) -> str:
    return html.escape(s, quote=True)


def _to_float(x: Any, default: float = 0.0) -> float:
    """Безопасное преобразование к float: '', None, 'nan' -> default."""
    try:
        if x is None:
            return default
        if isinstance(x, (bytes, bytearray)):
            x = x.decode("utf-8", "ignore")
        s = str(x).strip()
        if s == "":
            return default
        v = float(s)
        if not math.isfinite(v):
            return default
        return v
    except Exception:
        return default


def _to_bool(x: Any) -> bool:
    """0/1, '0'/'1', 'true'/'false' (без регистра)."""
    if x is None:
        return False
    if isinstance(x, (bytes, bytearray)):
        x = x.decode("utf-8", "ignore")
    s = str(x).strip().lower()
    if s in ("1", "true", "t", "yes", "y", "on"):
        return True
    if s in ("0", "false", "f", "no", "n", "off", ""):
        return False
    try:
        return int(s) != 0
    except Exception:
        return False


def _fmt_money(v: float, sign: str) -> str:
    # форматируем с пробелами для тысяч
    body = format(v, ",.2f").replace(",", " ")
    if body.endswith(".00"):
        body = body[:-3]
    return sign + body


def _fmt_int(n: int) -> str:
    return format(int(n), ",").replace(",", " ")


def _safe_ratio(num: float, den: float) -> float:
    return (num / den) if den > 0 else 0.0


def _date_range_days(days: int) -> Tuple[str, str]:
    import datetime as dt
    end = dt.date.today()
    start = end - dt.timedelta(days=days - 1)
    return start.isoformat(), end.isoformat()


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


def _security_headers(resp: Response) -> Response:
    # Контент + кэш
    resp.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; "
        "script-src 'self' 'unsafe-inline'; base-uri 'self'; frame-ancestors 'none'"
    )
    resp.headers.setdefault("X-Frame-Options", "DENY")
    resp.headers.setdefault("Referrer-Policy", "no-referrer")
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("Permissions-Policy", "geolocation=(), camera=(), microphone=()")
    resp.headers.setdefault("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
    resp.headers.setdefault("Pragma", "no-cache")
    if request.is_secure or request.headers.get("X-Forwarded-Proto", "") == "https":
        resp.headers.setdefault("Strict-Transport-Security", "max-age=15552000; includeSubDomains")
    return resp


# ============================ UI (layout + стили) ============================

BASE_CSS = """
:root{
  --text:#111827; --muted:#667085;
  --ok:#16a34a; --err:#ef4444; --warn:#f59e0b;

  --bg:#f6f7fb; --bg-2:#eef2f7; --panel:#ffffff;
  --radius-xxl:28px; --radius-xl:22px; --radius:14px;
  --shadow: 0 10px 32px rgba(15,23,42,.10);
  --shadow-big: 0 22px 80px rgba(15,23,42,.16);
  --content-max: 1200px;

  --neon1:#60a5fa; --neon2:#a78bfa; --neon3:#34d399; --neon4:#38bdf8;
  --stroke:#0b1020;

  --glass: rgba(255,255,255,.80);
  --glass-2: rgba(255,255,255,.68);
  --border: rgba(17,24,39,.10);
  --pill-bg: rgba(255,255,255,.92);
  --btn-grad-1:#ffffff; --btn-grad-2:#f6f8fb;

  --panel-bg: rgba(255,255,255,.86);
}
*{box-sizing:border-box}
html,body{height:100%;margin:0;color:var(--text);font:14px/1.55 Inter,system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;-webkit-font-smoothing:antialiased}
body{
  background:
    radial-gradient(1400px 800px at 40% -20%, #ffffff 0%, var(--bg) 46%, var(--bg-2) 100%),
    linear-gradient(180deg, #fff 0%, var(--bg) 100%);
}
html[data-theme="dark"]{
  color-scheme: dark;
  --text:#e5e7eb; --muted:#9aa5b1;
  --bg:#0e1426; --bg-2:#0b1120; --panel:#0f172a;
  --glass: rgba(19,25,46,.62);
  --glass-2: rgba(19,25,46,.52);
  --border: rgba(255,255,255,.08);
  --pill-bg: rgba(15,21,38,.72);
  --btn-grad-1:#141a2d; --btn-grad-2:#0e1527;
  --panel-bg: rgba(17,23,41,.55);
}
.page{
  min-height:100%;
  padding:24px;
  display:grid; grid-template-columns: 260px minmax(0,1fr); gap:20px;
  align-items:start;
}
@media (max-width: 980px){
  .page{ grid-template-columns: 1fr; padding:16px }
  .panel.menu{ position:static; }
}
.card{
  background: var(--glass);
  border:1px solid var(--border); border-radius: var(--radius-xxl);
  box-shadow: var(--shadow-big); backdrop-filter: blur(14px) saturate(160%); overflow:hidden;
}
.header{
  display:flex; align-items:center; justify-content:space-between; gap:12px;
  padding:14px 20px; border-bottom:1px solid var(--border);
  background: color-mix(in oklab, var(--glass) 86%, transparent);
}
.brand{ display:flex; align-items:center; gap:12px; font-weight:800; letter-spacing:.2px }
.logo{
  width:28px; height:28px; display:block; flex:0 0 auto;
  /* Прозрачный логотип — без фона и свечения */
}
.logo path, .logo rect{ vector-effect: non-scaling-stroke }
.switch{ display:inline-flex; align-items:center; gap:8px; background:var(--pill-bg); border:1px solid var(--border);
  border-radius:999px; padding:6px 10px; font-size:12px; cursor:pointer; user-select:none; }

.panel.menu{
  background: var(--panel); border:1px solid var(--border); border-radius: 20px;
  box-shadow: var(--shadow); padding:14px 12px;
  display:flex; flex-direction:column; min-height: 86vh; position:sticky; top:24px;
}
.panel.menu .head{ display:flex; align-items:center; gap:10px; padding:8px 10px; font-weight:800; letter-spacing:.2px; }
.panel.menu .head .logo{ width:32px; height:32px; }
.panel.menu nav{ display:flex; flex-direction:column; padding-top:6px }
.panel.menu .mitem{
  display:flex; gap:10px; align-items:center; padding:10px 12px; margin:3px 2px; border-radius:12px; cursor:pointer;
  color: var(--text); text-decoration:none; border:1px solid transparent;
}
.panel.menu .mitem:hover{ background: color-mix(in oklab, var(--glass-2) 52%, transparent); }
.panel.menu .mitem.active{
  background: linear-gradient(180deg, var(--btn-grad-1), var(--btn-grad-2));
  border-color: var(--border);
  box-shadow: 0 6px 18px rgba(96,165,250,.18);
}
.panel.menu .muted{ color: var(--muted); padding:10px 12px; font-size:12px; }

.content .section{ margin-bottom:16px }
.section .title{ font-size:16px; font-weight:800; }
.section .sub{ color:var(--muted); margin-top:4px }
.section .body{ padding:18px 16px 16px; }

/* Главная зона контента — явные паддинги */
.card .body{ padding:20px 22px 22px; }

.kpis{ display:grid; grid-template-columns: repeat(6, minmax(140px,1fr)); gap:12px; margin-top:12px }
@media (max-width: 1280px){ .kpis{ grid-template-columns: repeat(3, minmax(140px,1fr)); } }
@media (max-width: 640px){ .kpis{ grid-template-columns: repeat(2, minmax(140px,1fr)); } }
.kpi{
  border:1px solid var(--border); border-radius: 14px; padding:12px; background: rgba(255,255,255,.90);
}
html[data-theme="dark"] .kpi{ background: rgba(12,16,30,.86) }
.kpi .label{ font-size:12px; color: var(--muted) }
.kpi .value{ font-size:18px; font-weight:800; margin-top:4px }
.kpi .sub{ font-size:12px; color: var(--muted) }

.sparks{ display:grid; grid-template-columns: 1fr 1fr; gap:12px; margin-top:8px }
.spark{
  border:1px solid var(--border); border-radius: 14px; padding:10px 12px; background: rgba(255,255,255,.82);
}
html[data-theme="dark"] .spark{ background: rgba(12,16,30,.80) }
.spark .cap{ display:flex; justify-content:space-between; font-size:12px; color:var(--muted); margin-bottom:6px }
.spark svg{ width:100%; height:72px; display:block; }
.spark path[line]{ stroke: currentColor; stroke-width: 1.6; fill: none; vector-effect: non-scaling-stroke; stroke-linecap: round; }
.spark path[area]{ fill: currentColor; opacity: .12; }

.table{ width:100%; border-collapse: collapse; margin-top:10px }
.table th, .table td{ text-align:left; padding:12px 10px; border-bottom:1px solid var(--border); font-size:13px }

.form{ display:grid; gap:12px; max-width:560px; margin-top:10px }
.row{ display:grid; gap:6px }
.label{ font-weight:600; font-size:12px; color: var(--muted) }
.inp{
  width:100%; padding:12px 12px; border-radius:14px; border:1px solid var(--border);
  outline:none; background: rgba(255,255,255,.95); color:var(--text);
}
html[data-theme="dark"] .inp{ background: rgba(12,16,30,.90) }
.inp:focus{ box-shadow: 0 0 0 3px rgba(56,189,248,.18); }
.actions{ display:flex; gap:10px; align-items:center; margin-top:6px; flex-wrap:wrap }
.btn{
  border:1px solid var(--border); background: linear-gradient(180deg, var(--btn-grad-1), var(--btn-grad-2));
  color:var(--text); border-radius: 12px; padding:10px 14px; cursor:pointer; font-weight:700;
  transition: transform .08s ease, box-shadow .25s ease, opacity .2s ease; text-decoration:none; display:inline-block;
}
.btn[disabled]{ opacity:.6; cursor:not-allowed }
.btn:hover{ transform: translateY(-1px); box-shadow: 0 12px 36px rgba(15,23,42,.18) }
.btn.primary{ position:relative; box-shadow: 0 12px 32px rgba(96,165,250,.22) }
.btn.primary::after{
  content:""; position:absolute; inset:-2px; border-radius:12px; pointer-events:none;
  background: linear-gradient(90deg, var(--neon1), var(--neon2), var(--neon4), var(--neon3));
  opacity:.38; filter: blur(12px);
}
.note{ color:var(--muted); font-size:12px; }

.pw-wrap{ position:relative }
.pw-toggle{
  position:absolute; right:12px; top:50%; transform: translateY(-50%); cursor:pointer; opacity:.85; display:flex; align-items:center;
}
.pw-toggle svg{ width:18px; height:18px; display:block }
.ok{ color:var(--ok); font-weight:700; }
.err{ color:var(--err); font-weight:700; }
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

# Чистый, прозрачный SVG‑знак Hyper (две стойки + перемычка), без фона
LOGO_SVG = """
<svg class="logo" viewBox="0 0 256 256" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
  <defs>
    <linearGradient id="rg2" x1="0" y1="0" x2="256" y2="0">
      <stop offset="0%" stop-color="#38BDF8"/><stop offset="50%" stop-color="#A78BFA"/><stop offset="100%" stop-color="#34D399"/>
    </linearGradient>
  </defs>
  <rect x="44" y="28" width="48" height="200" rx="24" fill="url(#rg2)"/>
  <rect x="164" y="28" width="48" height="200" rx="24" fill="url(#rg2)"/>
  <rect x="92" y="108" width="72" height="40" rx="20" transform="rotate(-20 128 128)" fill="url(#rg2)"/>
</svg>
"""

def _layout(title_right: str, active: str, inner_html: str) -> str:
    """Общий лэйаут с боковым меню и хедером (прозрачный логотип, ровные паддинги)."""
    tpl = Template(
        "<!doctype html>\n"
        "<html lang=\"ru\" data-theme=\"light\">\n"
        "<head>\n"
        "  <meta charset=\"utf-8\" />\n"
        "  <title>$TITLE — HyperAI</title>\n"
        "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n"
        "  <style>$BASE_CSS</style>\n"
        "  <script>$THEME_JS</script>\n"
        "</head>\n"
        "<body>\n"
        "  <div class=\"page\">\n"
        "    <aside class=\"panel menu\">\n"
        "      <div class=\"head\">\n"
        "        $LOGO\n"
        "        <div>Навигация</div>\n"
        "      </div>\n"
        "      <nav>\n"
        "        <a class=\"mitem\" href=\"/\" title=\"Домой\">\n"
        "          <svg width=\"18\" height=\"18\" viewBox=\"0 0 24 24\" fill=\"none\" aria-hidden=\"true\"><path d=\"M3 10.5 12 3l9 7.5V21a1 1 0 0 1-1 1h-5v-6H9v6H4a1 1 0 0 1-1-1v-10.5Z\" stroke=\"currentColor\" stroke-width=\"1\"/></svg>\n"
        "          Главная\n"
        "        </a>\n"
        "        <a class=\"mitem\" href=\"/console\" title=\"Mission Control\">\n"
        "          <svg width=\"18\" height=\"18\" viewBox=\"0 0 24 24\" fill=\"none\" aria-hidden=\"true\"><path d=\"M3 12h18M12 3v18\" stroke=\"currentColor\" stroke-width=\"1\"/><circle cx=\"12\" cy=\"12\" r=\"9\" stroke=\"currentColor\" stroke-width=\"1\"/></svg>\n"
        "          Mission Control\n"
        "        </a>\n"
        "        <a class=\"mitem\" href=\"/artifacts\" title=\"Артефакты\">\n"
        "          <svg width=\"18\" height=\"18\" viewBox=\"0 0 24 24\" fill=\"none\" aria-hidden=\"true\"><path d=\"M4 7h16M4 12h16M4 17h16\" stroke=\"currentColor\" stroke-width=\"1.6\"/></svg>\n"
        "          Артефакты\n"
        "        </a>\n"
        "        <a class=\"mitem $A_SETTINGS\" href=\"/profile/settings\" title=\"Настройки\">\n"
        "          <svg width=\"18\" height=\"18\" viewBox=\"0 0 24 24\" fill=\"none\" aria-hidden=\"true\"><path d=\"M12 15a3 3 0 1 0 0-6 3 3 0 0 0 0 6Z\"/><path d=\"M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 1 1-4 0v-.09a1.65 1.65 0 0 0-1-1.51 1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 1 1 0-4h.09c.67 0 1.27-.39 1.51-1 .24-.61.11-1.3-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06c.52.44 1.21.57 1.82.33.61-.24 1-.84 1-1.51V3a2 2 0 1 1 4 0v.09c0 .67.39 1.27 1 1.51.61.24 1.3.11 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06c-.44.52-.57 1.21-.33 1.82.24-.61.84-1 1.51 1H21a2 2 0 1 1 0 4h-.09c-.67 0-1.27.39-1.51 1Z\" stroke=\"currentColor\" stroke-width=\"1\"/></svg>\n"
        "          Настройки\n"
        "        </a>\n"
        "      </nav>\n"
        "      <div style=\"margin-top:auto\" class=\"muted\">Powered by EasyByte</div>\n"
        "      <a class=\"mitem\" href=\"/auth/logout\" title=\"Выйти\">\n"
        "        <svg width=\"18\" height=\"18\" viewBox=\"0 0 24 24\" fill=\"none\" aria-hidden=\"true\"><path d=\"M10 6V4a2 2 0 0 1 2-2h6v20h-6a2 2 0 0 1-2-2v-2M7 16l-4-4 4-4M3 12h11\" stroke=\"currentColor\" stroke-width=\"1.2\"/></svg>\n"
        "        Выйти\n"
        "      </a>\n"
        "    </aside>\n"

        "    <main class=\"content\">\n"
        "      <div class=\"card\" style=\"max-width: var(--content-max); margin:0 auto;\">\n"
        "        <div class=\"header\">\n"
        "          <div class=\"brand\">$LOGO<div>HyperAI / $TITLE_RIGHT</div></div>\n"
        "          <label class=\"switch\" title=\"Тёмная тема\">\n"
        "            <span>Dark</span><input type=\"checkbox\" id=\"themeToggle\" aria-label=\"Сменить тему\">\n"
        "          </label>\n"
        "        </div>\n"
        "        <div class=\"body\">\n"
        "          $INNER\n"
        "        </div>\n"
        "      </div>\n"
        "    </main>\n"

        "  </div>\n"
        "  <script>\n"
        "    const tog = document.getElementById('themeToggle');\n"
        "    if (tog){\n"
        "      tog.checked = (document.documentElement.getAttribute('data-theme')==='dark');\n"
        "      tog.addEventListener('change', ()=> window.__setTheme(tog.checked ? 'dark':'light'));\n"
        "    }\n"
        "  </script>\n"
        "</body>\n"
        "</html>\n"
    )
    return tpl.substitute(
        TITLE=_escape(title_right),
        BASE_CSS=BASE_CSS,
        THEME_JS=THEME_JS,
        TITLE_RIGHT=_escape(title_right),
        INNER=inner_html,
        A_SETTINGS=("active" if active == "settings" else ""),
        LOGO=LOGO_SVG
    )


# ============================ UI-фрагменты страниц ============================

def _dashboard_html(
    kpis: Dict[str, str],
    range_days: int,
    spark_path_spend: str,
    spark_area_spend: str,
    spark_path_conv: str,
    spark_area_conv: str,
    rows: List[Dict[str, Any]]
) -> str:
    """KPI + спарклайны + таблица по дням (без f-строк в HTML)."""
    head_tpl = Template(
        "<div class=\"section\">"
        "<div class=\"title\">Обзор · последние $DAYS дн.</div>"
        "<div class=\"sub\">Ключевые показатели эффективности по рекламным активностям.</div>"
        "<div class=\"actions\" style=\"margin-top:10px\">"
        "<a class=\"btn $A7\" href=\"/profile?days=7\" aria-label=\"За 7 дней\">7 дн.</a>"
        "<a class=\"btn $A30\" href=\"/profile?days=30\" aria-label=\"За 30 дней\">30 дн.</a>"
        "<a class=\"btn $A90\" href=\"/profile?days=90\" aria-label=\"За 90 дней\">90 дн.</a>"
        "</div>"
        "</div>"

        "<div class=\"kpis\">"
        "<div class=\"kpi\"><div class=\"label\">Бюджет</div><div class=\"value\">$SPEND</div><div class=\"sub\">Суммарно</div></div>"
        "<div class=\"kpi\"><div class=\"label\">Выручка</div><div class=\"value\">$REV</div><div class=\"sub\">Суммарно</div></div>"
        "<div class=\"kpi\"><div class=\"label\">CTR</div><div class=\"value\">$CTR</div><div class=\"sub\">Клики / Показы</div></div>"
        "<div class=\"kpi\"><div class=\"label\">CVR</div><div class=\"value\">$CVR</div><div class=\"sub\">Конверсии / Клики</div></div>"
        "<div class=\"kpi\"><div class=\"label\">CPA</div><div class=\"value\">$CPA</div><div class=\"sub\">Цена за конверсию</div></div>"
        "<div class=\"kpi\"><div class=\"label\">ROAS</div><div class=\"value\">$ROAS</div><div class=\"sub\">Выручка / Бюджет</div></div>"
        "</div>"

        "<div class=\"sparks\">"

        "<div class=\"spark\">"
        "<div class=\"cap\"><span>Spend</span><span>$SPEND</span></div>"
        "<svg viewBox=\"0 0 100 30\" preserveAspectRatio=\"none\" aria-hidden=\"true\">"
        "<path area d=\"$SPARK_SPEND_AREA\" />"
        "<path line d=\"$SPARK_SPEND\" />"
        "</svg>"
        "</div>"

        "<div class=\"spark\">"
        "<div class=\"cap\"><span>Conversions</span><span>$CONV</span></div>"
        "<svg viewBox=\"0 0 100 30\" preserveAspectRatio=\"none\" aria-hidden=\"true\">"
        "<path area d=\"$SPARK_CONV_AREA\" />"
        "<path line d=\"$SPARK_CONV\" />"
        "</svg>"
        "</div>"

        "</div>"

        "<div class=\"section\" style=\"margin-top:10px\">"
        "<div class=\"title\">Динамика по дням</div>"
        "<div class=\"sub\">Без лишних подписей — только то, что действительно важно.</div>"
        "</div>"

        "<table class=\"table\">"
        "<thead><tr>"
        "<th>Дата</th><th>Показы</th><th>Клики</th><th>Конверсии</th><th>CTR</th><th>CVR</th><th>Spend</th><th>Revenue</th>"
        "</tr></thead>"
        "<tbody>\n"
        "$ROWS\n"
        "</tbody></table>"
    )

    # Табличные строки: строго через Template
    row_tpl = Template(
        "<tr>"
        "<td>$DATE</td>"
        "<td>$IMPR</td>"
        "<td>$CLICKS</td>"
        "<td>$CONV</td>"
        "<td>$CTR</td>"
        "<td>$CVR</td>"
        "<td>$SIGN$SPEND</td>"
        "<td>$SIGN$REV</td>"
        "</tr>"
    )
    rows_html: List[str] = []
    for r in rows:
        ctr = "{:.2f}%".format(_safe_ratio(r.get("clicks", 0), r.get("impressions", 0)) * 100.0)
        cvr = "{:.2f}%".format(_safe_ratio(r.get("conversions", 0), r.get("clicks", 0)) * 100.0)
        rows_html.append(row_tpl.substitute(
            DATE=_escape(str(r.get("date", ""))),
            IMPR=_escape(_fmt_int(int(r.get("impressions", 0)))),
            CLICKS=_escape(_fmt_int(int(r.get("clicks", 0)))),
            CONV=_escape(_fmt_int(int(r.get("conversions", 0)))),
            CTR=_escape(ctr),
            CVR=_escape(cvr),
            SIGN=_escape(kpis["sign"]),
            SPEND=_escape(format(float(r.get("spend", 0.0)), ",.2f").replace(",", " ")),
            REV=_escape(format(float(r.get("revenue", 0.0)), ",.2f").replace(",", " ")),
        ))
    return head_tpl.substitute(
        DAYS=str(range_days),
        A7=("primary" if range_days == 7 else ""),
        A30=("primary" if range_days == 30 else ""),
        A90=("primary" if range_days == 90 else ""),
        SPEND=_escape(kpis["spend"]),
        REV=_escape(kpis["rev"]),
        CTR=_escape(kpis["ctr"]),
        CVR=_escape(kpis["cvr"]),
        CPA=_escape(kpis["cpa"]),
        ROAS=_escape(kpis["roas"]),
        CONV=_escape(kpis["conv"]),
        SPARK_SPEND=_escape(spark_path_spend),
        SPARK_SPEND_AREA=_escape(spark_area_spend),
        SPARK_CONV=_escape(spark_path_conv),
        SPARK_CONV_AREA=_escape(spark_area_conv),
        ROWS="\n".join(rows_html)
    )


def _mission_html() -> str:
    return (
        "<div class=\"section\">"
        "<div class=\"title\">Mission Control</div>"
        "<div class=\"sub\">Центр управления кампаниями (плейбуки, расписания, авто‑бюджеты) — "
        "перемещён в /console.</div>"
        "</div>"
        "<div class=\"note\">Этот маршрут теперь редиректит. Используйте левое меню → Mission Control.</div>"
    )


def _settings_html(user: "User", csrf: str, note_ok: str = "", note_err: str = "") -> str:
    eye_svg = (
        "<svg viewBox=\"0 0 24 24\" aria-hidden=\"true\">"
        " <path d=\"M12 5C7 5 2.73 8.11 1 12c1.73 3.89 6 7 11 7s9.27-3.11 11-7c-1.73-3.89-6-7-11-7Z\" "
        "stroke=\"currentColor\" fill=\"none\" stroke-width=\"1.2\"/>"
        " <circle cx=\"12\" cy=\"12\" r=\"3.2\" stroke=\"currentColor\" fill=\"none\" stroke-width=\"1.2\"/>"
        "</svg>"
    )
    tpl = Template(
        "<div class=\"section\">"
        "<div class=\"title\">Настройки профиля</div>"
        "<div class=\"sub\">Измени отображаемое имя и пароль.</div>"
        "</div>"

        "${NOTE}"

        "<form class=\"form\" method=\"post\" action=\"/profile/account\" autocomplete=\"on\" novalidate>"
        "  <input type=\"hidden\" name=\"_csrf\" value=\"$CSRF\" />"
        "  <div class=\"row\"><label class=\"label\" for=\"name\">Имя</label>"
        "    <input class=\"inp\" id=\"name\" type=\"text\" name=\"name\" value=\"$NAME\" required autocomplete=\"name\"/></div>"
        "  <div class=\"row\"><label class=\"label\" for=\"email\">E-mail</label>"
        "    <input class=\"inp\" id=\"email\" type=\"email\" value=\"$EMAIL\" disabled/></div>"
        "  <div class=\"actions\"><button class=\"btn primary\" type=\"submit\">Сохранить</button></div>"
        "</form>"

        "<div class=\"section\" style=\"margin-top:12px\"><div class=\"title\">Смена пароля</div></div>"
        "<form class=\"form\" method=\"post\" action=\"/profile/password\" autocomplete=\"off\" novalidate>"
        "  <input type=\"hidden\" name=\"_csrf\" value=\"$CSRF\" />"
        "  <div class=\"row pw-wrap\">"
        "    <label class=\"label\" for=\"oldpw\">Текущий пароль</label>"
        "    <input class=\"inp\" id=\"oldpw\" type=\"password\" name=\"old_password\" placeholder=\"••••••••\" required autocomplete=\"current-password\"/>"
        "    <span class=\"pw-toggle\" data-target=\"oldpw\" title=\"Показать/скрыть\">$EYE</span>"
        "  </div>"
        "  <div class=\"row pw-wrap\">"
        "    <label class=\"label\" for=\"newpw\">Новый пароль</label>"
        "    <input class=\"inp\" id=\"newpw\" type=\"password\" name=\"new_password\" placeholder=\"минимум 8 символов\" minlength=\"8\" required autocomplete=\"new-password\"/>"
        "    <span class=\"pw-toggle\" data-target=\"newpw\" title=\"Показать/скрыть\">$EYE</span>"
        "  </div>"
        "  <div class=\"row pw-wrap\">"
        "    <label class=\"label\" for=\"newpw2\">Повторите новый пароль</label>"
        "    <input class=\"inp\" id=\"newpw2\" type=\"password\" name=\"new_password2\" placeholder=\"••••••••\" minlength=\"8\" required autocomplete=\"new-password\"/>"
        "    <span class=\"pw-toggle\" data-target=\"newpw2\" title=\"Показать/скрыть\">$EYE</span>"
        "  </div>"
        "  <div class=\"actions\"><button class=\"btn primary\" type=\"submit\">Обновить пароль</button></div>"
        "  <div class=\"note\">Пароль хранится с солью (PBKDF2‑SHA256, 200k раундов).</div>"
        "</form>"

        "<script>"
        "  document.querySelectorAll('.pw-toggle').forEach(function(el){"
        "    el.addEventListener('click', function(){"
        "      const id = el.getAttribute('data-target');"
        "      const inp = document.getElementById(id);"
        "      if (inp) inp.type = (inp.type==='password' ? 'text' : 'password');"
        "    });"
        "  });"
        "</script>"
    )
    note = ""
    if note_ok:
        note = '<div class="ok" style="margin:4px 0 6px">' + _escape(note_ok) + "</div>"
    if note_err:
        note = '<div class="err" style="margin:4px 0 6px">' + _escape(note_err) + "</div>"
    return tpl.substitute(
        CSRF=_escape(csrf),
        NAME=_escape(user.name),
        EMAIL=_escape(user.email),
        NOTE=note,
        EYE=eye_svg
    )


# ============================ Генерация "искр" (SVG path) ============================

def _spark_path(values: List[float]) -> str:
    """Строим path для мини‑графика (линия): координаты нормируем в 100x30."""
    if not values:
        return ""
    n = len(values)
    vmin = min(values)
    vmax = max(values)
    rng = max(vmax - vmin, 1e-9)
    pts = []
    for i, v in enumerate(values):
        x = 100.0 * i / max(n - 1, 1)
        y = 28.0 - 26.0 * ((v - vmin) / rng)  # 28..2
        pts.append((x, y))
    d = "M{:.2f},{:.2f} ".format(pts[0][0], pts[0][1]) + " ".join("L{:.2f},{:.2f}".format(p[0], p[1]) for p in pts[1:])
    return d


def _spark_area(values: List[float]) -> str:
    """Площадь под кривой для мягкого заполнения (минималистичный градиент без зависимостей)."""
    line = _spark_path(values)
    if not line:
        return ""
    # Превратим линию в area: замыкаем к низу (y=30)
    # line начинается с Mx,y далее Lx,y ... ; добавим L100,30 L0,30 Z
    return line + " L100,30 L0,30 Z"


# ============================ Хранилище (та же auth.db) ============================

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


class ProfileDB:
    """Хранилище профиля + метрики (в той же БД, что и auth)."""

    def __init__(self, db_path: str | os.PathLike[str]):
        self.path = str(db_path)
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._conn = sqlite3.connect(
            self.path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        # Производительность и целостность
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._migrate()

    def _migrate(self) -> None:
        with self._conn:
            # --- users (как в auth.py)
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

            # Санитаризация для устаревших инсталляций ('' -> 0)
            try:
                self._conn.execute("UPDATE users SET last_login_at=0 WHERE last_login_at IS NULL OR TRIM(last_login_at)='';")
            except sqlite3.OperationalError:
                pass
            try:
                self._conn.execute("UPDATE users SET approved=0 WHERE approved IS NULL OR TRIM(approved)='';")
            except sqlite3.OperationalError:
                pass

            # --- login_attempts (общая таблица)
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

            # --- kv (общая)
            self._conn.execute("""
            CREATE TABLE IF NOT EXISTS kv ( k TEXT PRIMARY KEY, v TEXT NOT NULL );
            """)

            # --- metrics_daily (новая таблица для дашборда)
            self._conn.execute("""
            CREATE TABLE IF NOT EXISTS metrics_daily (
              date TEXT NOT NULL,                 -- YYYY-MM-DD
              account TEXT NOT NULL DEFAULT '',
              campaign TEXT NOT NULL DEFAULT '',
              spend REAL NOT NULL DEFAULT 0,
              impressions INTEGER NOT NULL DEFAULT 0,
              clicks INTEGER NOT NULL DEFAULT 0,
              conversions INTEGER NOT NULL DEFAULT 0,
              revenue REAL NOT NULL DEFAULT 0,
              PRIMARY KEY(date, account, campaign)
            );
            """)
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_metrics_date ON metrics_daily(date);")

    # ---- Users

    def get_user_by_email(self, email: str) -> Optional[User]:
        q = self._conn.execute("SELECT * FROM users WHERE lower(email)=lower(?) LIMIT 1;", (email,))
        row = q.fetchone()
        return self._row_to_user(row) if row else None

    def update_user_name(self, user_id: int, new_name: str) -> None:
        with self._conn:
            self._conn.execute("UPDATE users SET name=? WHERE id=?;", (new_name.strip(), user_id))

    def update_user_password(self, user_id: int, new_salt_hex: str, new_hash_hex: str) -> None:
        with self._conn:
            self._conn.execute("UPDATE users SET pw_salt_hex=?, pw_hash_hex=? WHERE id=?;", (new_salt_hex, new_hash_hex, user_id))

    # ---- Metrics

    def seed_demo_if_empty(self, days: int = 30) -> None:
        """Если таблица пустая — заполним демо-данными, чтобы страница не выглядела пустой."""
        q = self._conn.execute("SELECT COUNT(*) AS c FROM metrics_daily;")
        cnt = int(q.fetchone()[0])
        if cnt > 0:
            return
        import random, datetime as dt
        today = dt.date.today()
        with self._conn:
            for i in range(days, 0, -1):
                d = today - dt.timedelta(days=i - 1)
                spend = max(0.0, random.gauss(120.0, 40.0))
                impr = max(0, int(random.gauss(60000, 12000)))
                clicks = max(0, int(spend * random.uniform(0.6, 1.1)))
                conv = max(0, int(clicks * random.uniform(0.02, 0.08)))
                rev = round(spend * random.uniform(1.1, 1.7), 2)
                self._conn.execute("""
                    INSERT OR REPLACE INTO metrics_daily(date, account, campaign, spend, impressions, clicks, conversions, revenue)
                    VALUES (?, '', '', ?, ?, ?, ?, ?);
                """, (d.isoformat(), round(spend, 2), impr, clicks, conv, rev))

    def get_range_agg(self, date_from: str, date_to: str) -> Dict[str, float]:
        q = self._conn.execute("""
            SELECT
              COALESCE(SUM(spend),0) AS s,
              COALESCE(SUM(impressions),0) AS i,
              COALESCE(SUM(clicks),0) AS c,
              COALESCE(SUM(conversions),0) AS v,
              COALESCE(SUM(revenue),0) AS r
            FROM metrics_daily
            WHERE date>=? AND date<=?;
        """, (date_from, date_to))
        s, i, c, v, r = q.fetchone()
        return dict(
            spend=_to_float(s, 0.0),
            impressions=int(i or 0),
            clicks=int(c or 0),
            conversions=int(v or 0),
            revenue=_to_float(r, 0.0),
        )

    def get_timeseries(self, date_from: str, date_to: str) -> List[Dict[str, Any]]:
        q = self._conn.execute("""
            SELECT date, SUM(spend), SUM(clicks), SUM(conversions), SUM(impressions), SUM(revenue)
            FROM metrics_daily
            WHERE date>=? AND date<=?
            GROUP BY date
            ORDER BY date ASC;
        """, (date_from, date_to))
        out: List[Dict[str, Any]] = []
        for row in q.fetchall():
            out.append(dict(
                date=str(row[0]),
                spend=_to_float(row[1], 0.0),
                clicks=int(row[2] or 0),
                conversions=int(row[3] or 0),
                impressions=int(row[4] or 0),
                revenue=_to_float(row[5], 0.0),
            ))
        return out

    # ---- Utils

    @staticmethod
    def _row_to_user(row: sqlite3.Row | tuple) -> "User":
        # Индексы соответствуют CREATE TABLE users
        return User(
            id=int(row[0]),
            email=str(row[1]),
            name=str(row[2]),
            pw_salt_hex=str(row[3]),
            pw_hash_hex=str(row[4]),
            approved=_to_bool(row[5]),
            created_at=_to_float(row[6], 0.0),
            approval_token=str(row[7]),
            last_login_at=_to_float(row[8], 0.0),
        )


# ============================ Роутинг/инициализация ============================

def init_profile(app: Flask, settings: Settings) -> None:
    """
    Регистрирует маршруты /profile/* и создаёт таблицы метрик (если отсутствуют).
    """
    currency = os.environ.get("CURRENCY_SIGN", "₽")
    db_path = settings.paths.artifacts_root / "auth.db"
    pdb = ProfileDB(db_path)
    pdb.seed_demo_if_empty(days=30)

    # Глобальный after_request (security headers + no-store)
    @app.after_request
    def _after(resp: Response) -> Response:
        return _security_headers(resp)

    # Общий гард: нужна активная сессия
    def _require_user() -> User:
        email = session.get("user_email") or ""
        u = pdb.get_user_by_email(email) if email else None
        if not u or not u.approved:
            raise PermissionError
        return u

    # ---------- Страницы

    @app.get("/profile")
    def profile_overview() -> Response:
        try:
            _ = _require_user()  # noqa: F841
        except PermissionError:
            return redirect(url_for("auth_login"))

        # диапазон
        days = int(request.args.get("days", "30"))
        days = 7 if days <= 7 else 30 if days <= 30 else 90
        d_from, d_to = _date_range_days(days)

        # агрегация
        agg = pdb.get_range_agg(d_from, d_to)
        kpi_spend = _fmt_money(agg["spend"], currency)
        kpi_rev = _fmt_money(agg["revenue"], currency)
        ctr = _safe_ratio(agg["clicks"], agg["impressions"]) * 100
        cvr = _safe_ratio(agg["conversions"], agg["clicks"]) * 100
        cpa = _fmt_money(_safe_ratio(agg["spend"], agg["conversions"]), currency)
        roas = _safe_ratio(agg["revenue"], agg["spend"])

        # timeseries + спарклайны (минималистично, без цифр на графике)
        ts = pdb.get_timeseries(d_from, d_to)
        spend_series = [float(row["spend"]) for row in ts] or [0.0]
        conv_series = [float(row["conversions"]) for row in ts] or [0.0]
        path_spend = _spark_path(spend_series)
        area_spend = _spark_area(spend_series)
        path_conv = _spark_path([float(x) for x in conv_series])
        area_conv = _spark_area([float(x) for x in conv_series])

        kpis = dict(
            spend=kpi_spend,
            rev=kpi_rev,
            ctr="{:.2f}%".format(ctr),
            cvr="{:.2f}%".format(cvr),
            cpa=cpa,
            roas="{:.2f}×".format(roas),
            conv=str(int(agg["conversions"])),
            sign=currency
        )
        inner = _dashboard_html(kpis, days, path_spend, area_spend, path_conv, area_conv, ts)
        html_doc = _layout("Профиль · Обзор", "overview", inner)
        resp = make_response(html_doc)
        return resp

    @app.get("/profile/mission")
    def profile_mission_redirect() -> Response:
        """Старый маршрут — показываем подсказку и редиректим на /console."""
        try:
            _ = _require_user()
        except PermissionError:
            return redirect(url_for("auth_login"))
        return redirect("/console", code=307)

    @app.get("/profile/settings")
    def profile_settings() -> Response:
        try:
            u = _require_user()
        except PermissionError:
            return redirect(url_for("auth_login"))
        csrf = _ensure_csrf()
        inner = _settings_html(u, csrf)
        return make_response(_layout("Профиль · Настройки", "settings", inner))

    # ---------- Действия

    @app.post("/profile/account")
    def profile_update_account() -> Response:
        try:
            u = _require_user()
        except PermissionError:
            return redirect(url_for("auth_login"))
        _validate_csrf(request.form.get("_csrf"))

        name = (request.form.get("name") or "").strip()
        # простая гигиена имени — не ломаем обратную совместимость, но не пускаем мусор
        if not (1 <= len(name) <= 120):
            csrf = _ensure_csrf()
            inner = _settings_html(u, csrf, note_err="Имя должно быть от 1 до 120 символов.")
            return make_response(_layout("Профиль · Настройки", "settings", inner))
        # уберём управляющие и злостные пробелы
        name = re.sub(r"[\u0000-\u001F\u007F]+", "", name)
        pdb.update_user_name(u.id, name)

        # отразим новое имя сразу
        u2 = pdb.get_user_by_email(u.email) or u
        csrf = _ensure_csrf()
        inner = _settings_html(u2, csrf, note_ok="Имя обновлено.")
        return make_response(_layout("Профиль · Настройки", "settings", inner))

    @app.post("/profile/password")
    def profile_update_password() -> Response:
        try:
            u = _require_user()
        except PermissionError:
            return redirect(url_for("auth_login"))
        _validate_csrf(request.form.get("_csrf"))

        old_pw = request.form.get("old_password") or ""
        new_pw = request.form.get("new_password") or ""
        new_pw2 = request.form.get("new_password2") or ""

        if not old_pw or not new_pw or not new_pw2:
            csrf = _ensure_csrf()
            inner = _settings_html(u, csrf, note_err="Заполните все поля.")
            return make_response(_layout("Профиль · Настройки", "settings", inner))

        if len(new_pw) < 8:
            csrf = _ensure_csrf()
            inner = _settings_html(u, csrf, note_err="Новый пароль должен быть не короче 8 символов.")
            return make_response(_layout("Профиль · Настройки", "settings", inner))

        if new_pw != new_pw2:
            csrf = _ensure_csrf()
            inner = _settings_html(u, csrf, note_err="Пароли не совпадают.")
            return make_response(_layout("Профиль · Настройки", "settings", inner))

        # проверим старый пароль
        if not verify_password(old_pw, u.pw_salt_hex, u.pw_hash_hex):
            csrf = _ensure_csrf()
            inner = _settings_html(u, csrf, note_err="Текущий пароль неверен.")
            return make_response(_layout("Профиль · Настройки", "settings", inner))

        salt_hex, hash_hex = make_password(new_pw)
        pdb.update_user_password(u.id, salt_hex, hash_hex)
        csrf = _ensure_csrf()
        inner = _settings_html(u, csrf, note_ok="Пароль успешно обновлён.")
        return make_response(_layout("Профиль · Настройки", "settings", inner))

    # ---------- API (виджеты/фронт)

    @app.get("/profile/api/stats")
    def api_stats() -> Response:
        try:
            _ = _require_user()
        except PermissionError:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        days = int(request.args.get("days", "30"))
        days = 7 if days <= 7 else 30 if days <= 30 else 90
        d_from, d_to = _date_range_days(days)
        agg = pdb.get_range_agg(d_from, d_to)
        ts = pdb.get_timeseries(d_from, d_to)
        return jsonify({"ok": True, "range_days": days, "agg": agg, "timeseries": ts})
