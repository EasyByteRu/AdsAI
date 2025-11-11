# -*- coding: utf-8 -*-
"""
Главная страница Web UI (Next-gen Glass UI) для Ads AI Agent.
Файл экспортирует HOME_HTML для встраивания в Flask (чтобы не раздувать app.py).

Интеграция в Flask (в ads_ai/web/app.py):
    from ads_ai.web.home import HOME_HTML

    @app.route("/", methods=["GET"])   # сделаем главную на корне
    def home_root() -> Response:
        return make_response(HOME_HTML)

    @app.route("/console", methods=["GET"])
    def console() -> Response:
        return make_response(INDEX_HTML)   # уже есть в app.py
"""
from __future__ import annotations

HOME_HTML: str = """<!doctype html>
<html lang="ru" data-theme="light">
<head>
  <meta charset="utf-8" />
  <title>HyperAI — Mission Hub</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="color-scheme" content="light dark" />
  <style>
    /* ================= THEME TOKENS ================= */
    :root{
      --bg:#0b1220; --bg2:#0d1423;
      --text:#111827; --muted:#6b7280;
      --glass: rgba(255,255,255,.66);
      --glass-2: rgba(255,255,255,.5);
      --border: rgba(17,24,39,.10);
      --ring: rgba(17,24,39,.06);
      --neon1:#60a5fa; --neon2:#a78bfa; --neon3:#34d399; --neon4:#38bdf8;
      --ok:#16a34a; --err:#ef4444; --warn:#f59e0b;
      --radius-xl:28px; --radius:22px; --radius-sm:14px;
      --shadow: 0 12px 36px rgba(15,23,42,.16);
      --shadow-big: 0 30px 80px rgba(15,23,42,.22);
      --content-max: 1240px;
      --kbd-bg:#f3f4f6; --kbd-text:#374151;
      --pill-bg: rgba(255,255,255,.82);
      --chip-bg: rgba(255,255,255,.9);
      --btn-grad-1:#ffffff; --btn-grad-2:#f5f7fb;
      --card-blur: 14px;
    }
    html[data-theme="dark"]{
      color-scheme: dark;
      --bg:#070c18; --bg2:#0a0f1e;
      --text:#e5e7eb; --muted:#9aa5b1;
      --glass: rgba(17,23,41,.55);
      --glass-2: rgba(17,23,41,.42);
      --border: rgba(255,255,255,.09);
      --ring: rgba(100,116,139,.18);
      --shadow: 0 12px 36px rgba(0,0,0,.42);
      --shadow-big: 0 30px 90px rgba(0,0,0,.5);
      --kbd-bg:#0f172a; --kbd-text:#e5e7eb;
      --pill-bg: rgba(15,21,38,.72);
      --chip-bg: rgba(12,16,30,.9);
      --btn-grad-1:#121a31; --btn-grad-2:#0c1428;
    }

    a{ text-decoration:none }

    *{box-sizing:border-box}
    html,body{height:100%;margin:0;color:var(--text);font:14px/1.45 Inter,ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;-webkit-font-smoothing:antialiased}
    body{ background: white; }
    html[data-theme="dark"] body{
      background:
        radial-gradient(1200px 800px at 20% -10%, #ffffff 0%, var(--bg) 48%, var(--bg2) 100%),
        linear-gradient(180deg,#ffffff 0%, var(--bg2) 100%);
      transition: background .45s ease, color .35s ease;
    }

    .bg-orbs{
      position:fixed; inset:-10%; pointer-events:none; z-index:-1; filter: blur(40px) saturate(140%);
      background:
        radial-gradient(1000px 600px at 20% 10%, rgba(96,165,250,.22), transparent 60%),
        radial-gradient(800px 520px at 80% 10%, rgba(167,139,250,.18), transparent 60%),
        radial-gradient(900px 600px at 50% 90%, rgba(56,189,248,.15), transparent 60%);
      animation: orbs 18s ease-in-out infinite alternate;
    }
    @keyframes orbs{ 0%{ transform: translateY(0)} 100%{ transform: translateY(-1.6%)} }

    /* ================= LAYOUT ================= */
    .wrap{max-width:var(--content-max); margin:0 auto; padding:24px 18px 34px;}
    .top{
      display:flex; align-items:center; gap:12px; justify-content:space-between;
      background:var(--glass); border:1px solid var(--border); border-radius:16px; padding:10px 12px;
      backdrop-filter: blur(12px) saturate(160%); box-shadow: var(--shadow);
    }
    .brand{display:flex; align-items:center; gap:12px; font-weight:800; letter-spacing:.3px}
    .logo{width:32px;height:32px;border-radius:12px; box-shadow: 0 10px 36px #60a5fa66, inset 0 0 0 1px #ffffff55;}
    .brand-sub{font-weight:600; opacity:.7}
    .right-ctrls{display:flex;align-items:center;gap:10px;}
    .switch{
      display:inline-flex; align-items:center; gap:8px; background:var(--pill-bg); border:1px solid var(--border);
      border-radius:999px; padding:6px 10px; font-size:12px; cursor:pointer; user-select:none;
    }

    /* ================= HERO ================= */
    .hero{
      margin-top:26px;
      border-radius: var(--radius-xl);
      background: color-mix(in oklab, var(--glass) 86%, transparent);
      border:1px solid var(--border);
      box-shadow: var(--shadow-big);
      overflow:hidden; position:relative;
    }
    .hero-deco{
      position:absolute; inset:-1px; pointer-events:none;
      background:
        conic-gradient(from 0deg, rgba(96,165,250,.25), rgba(167,139,250,.25), rgba(56,189,248,.2), rgba(52,211,153,.2), rgba(96,165,250,.25));
      -webkit-mask: linear-gradient(#000 0 0) content-box, linear-gradient(#000 0 0);
      -webkit-mask-composite: xor; mask-composite: exclude;
      padding:1.6px; border-radius: inherit; opacity:.7; filter: blur(.3px);
      animation: ringSpin 18s linear infinite;
    }
    @keyframes ringSpin{ to{ transform:rotate(360deg) } }
    .hero-inner{ padding:28px 26px 26px; display:grid; grid-template-columns: 1.2fr .8fr; gap:18px; align-items:center; }
    @media (max-width: 980px){ .hero-inner{ grid-template-columns: 1fr; } }

    .h-eyebrow{ font-size:12px; letter-spacing:.24em; text-transform:uppercase; color:var(--muted); }
    .h-title{
      margin:6px 0 0 0; font-weight:900; letter-spacing:.1px;
      font-size: clamp(28px, 6vw, 44px);
      background: linear-gradient(90deg, var(--neon1), var(--neon2), var(--neon4), var(--neon3));
      -webkit-background-clip: text; background-clip: text; color: transparent;
    }
    .h-sub{ color:var(--muted); margin-top:10px; max-width:720px; }
    .hero-cta{ display:flex; gap:12px; align-items:center; margin-top:18px; flex-wrap:wrap }
    .btn{
      border:1px solid var(--border); background: linear-gradient(180deg, var(--btn-grad-1), var(--btn-grad-2));
      color:var(--text); border-radius: 14px; padding:12px 16px; cursor:pointer; font-weight:600;
      transition: transform .08s ease, box-shadow .25s ease, outline-color .25s ease;
      outline: 0 solid transparent;
    }
    .btn:hover{ transform: translateY(-1px); box-shadow: 0 12px 36px rgba(15,23,42,.18) }
    .btn.primary{ position:relative; box-shadow: 0 14px 36px rgba(96,165,250,.25) }
    .btn.primary::after{
      content:""; position:absolute; inset:-2px; border-radius:14px; pointer-events:none;
      background: linear-gradient(90deg, var(--neon1), var(--neon2), var(--neon4), var(--neon3));
      opacity:.45; filter: blur(14px);
    }
    .btn.ghost{ background: transparent }

    .hero-mock{
      height: 220px; border-radius: 18px; border:1px solid var(--border);
      background:
        linear-gradient(180deg, rgba(255,255,255,.08), rgba(255,255,255,.02)),
        radial-gradient(600px 220px at 20% 0%, rgba(96,165,250,.2), transparent 60%),
        radial-gradient(600px 220px at 80% 100%, rgba(167,139,250,.16), transparent 60%);
      backdrop-filter: blur(var(--card-blur)) saturate(160%);
      box-shadow: var(--shadow);
      position:relative; overflow:hidden;
    }
    .hero-mock::before{
      content:""; position:absolute; inset:0;
      background:
        repeating-linear-gradient(90deg, rgba(255,255,255,.08) 0 1px, transparent 1px 6px);
      opacity:.4;
    }
    .hero-kpis{ position:absolute; bottom:12px; left:12px; right:12px; display:flex; gap:10px; flex-wrap:wrap }
    .kpi{
      background: var(--pill-bg); border:1px solid var(--border); border-radius: 12px; padding:8px 10px;
      display:flex; gap:8px; align-items:center; color:var(--text); font-weight:600; font-variant-numeric: tabular-nums;
    }
    .kpi .label{ color:var(--muted); font-weight:500; }

    /* ================= GRID CARDS ================= */
    .section-title{ margin:24px 6px 8px; font-weight:800; letter-spacing:.2px; }
    .grid{ visibility: hidden; display:grid; gap:16px; grid-template-columns: repeat( auto-fit, minmax(260px, 1fr) ); }
    .card{
      position:relative; overflow:hidden;
      background:var(--glass); border:1px solid var(--border); border-radius:var(--radius); padding:18px;
      min-height:122px; box-shadow: var(--shadow);
      transition: transform .12s ease, box-shadow .25s ease, background .25s ease;
      cursor:pointer;
      backdrop-filter: blur(var(--card-blur)) saturate(160%);
    }
    .card:hover{ transform: translateY(-2px); box-shadow: 0 16px 44px rgba(15,23,42,.20) }
    .card .ic{ width:38px;height:38px;border-radius:14px; display:flex;align-items:center;justify-content:center;
      background:#fff3; border:1px solid var(--border); margin-bottom:12px; font-size:18px; }
    .card b{ display:block; font-size:16px; }
    .card .muted{ color:var(--muted); margin-top:6px }
    .badge{
      position:absolute; top:12px; right:12px; font-size:11px; padding:4px 9px; border-radius:999px; border:1px solid var(--border);
      background:#f8fafc; color:#334155;
    }
    html[data-theme="dark"] .badge{ background:#0b1220; color:#cbd5e1 }
    .disabled{ opacity:.55; pointer-events:none }
    .soon{ background: #fde68a33 !important }

    /* ================= STATUS & FOOTER ================= */
    .status{
      margin-top:18px; display:flex; gap:10px; align-items:center; flex-wrap:wrap; color:var(--muted);
    }
    .pill{
      display:inline-flex; align-items:center; gap:8px; padding:7px 11px; background: var(--pill-bg); border:1px solid var(--border); border-radius:999px; font-size:12px;
    }
    .ok{ color:var(--ok) } .warn{ color:var(--warn) } .err{ color:var(--err) }

    footer{ margin:28px 0 8px; color:var(--muted); display:flex; align-items:center; gap:10px; justify-content:space-between; }
    .made{ display:flex; align-items:center; gap:8px; }
    .dot{ width:6px;height:6px;border-radius:50%; background:var(--neon2); box-shadow:0 0 12px var(--neon2); }

    /* Reduced Motion */
    @media (prefers-reduced-motion: reduce){
      .card, .btn{ transition:none!important }
      .bg-orbs{ animation:none!important }
    }
  </style>
</head>
<body>
  <div class="bg-orbs"></div>
  <div class="wrap">
    <!-- ===== TOP BAR ===== -->
    <div class="top">
      <div class="brand">
        <div class="logo"><svg viewBox="0 0 256 256" xmlns="http://www.w3.org/2000/svg">
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
</div>
        <div>
          HyperAI <span class="brand-sub">/ Главная</span>
        </div>
      </div>
      <div class="right-ctrls">
        <label class="switch" title="Тёмная тема">
          <span>Dark</span>
          <input type="checkbox" id="themeToggle" />
        </label>
      </div>
    </div>

    <!-- ===== HERO ===== -->
    <section class="hero">
      <div class="hero-deco"></div>
      <div class="hero-inner">
        <div>
          <div class="h-eyebrow">Google Ads · Автосоздание и аналитика</div>
          <h1 class="h-title">Запускайте умные кампании и следите за метриками в одном месте</h1>
          <div class="h-sub">
            Mission Hub — стартовая точка: создание кампаний, быстрый доступ к списку, отчётность и <b>Mission Control</b> (живой контроль браузера, план/шаги).
          </div>
          <div class="hero-cta">
            <a class="btn primary" href="/companies/new">Создать рекламную кампанию</a>
            <a class="btn" href="/companies/list">Кампании</a>
          </div>
        </div>
        <div class="hero-mock" aria-hidden="true">
          <div class="hero-kpis">
            <div class="kpi"><span class="label">Активных:</span> <span id="kpiActive">—</span></div>
            <div class="kpi"><span class="label">Бюджет сегодня:</span> <span id="kpiBudget">—</span></div>
            <div class="kpi"><span class="label">Конверсии 24ч:</span> <span id="kpiConv">—</span></div>
          </div>
        </div>
      </div>
    </section>

    <!-- ===== QUICK ACCESS CARDS ===== -->
    <h3 class="section-title" style="visibility: hidden;">Быстрый доступ</h3>
    <div class="grid">
      <a class="card" href="/campaigns/new">
        <div class="ic">🚀</div>
        <b>Создать кампанию</b>
        <div class="muted">Мастер создания: цели, аудитории, креативы, бюджеты</div>
      </a>

      <a class="card" href="/campaigns">
        <div class="ic">📋</div>
        <b>Кампании</b>
        <div class="muted">Список всех кампаний, статусы, быстрые действия</div>
      </a>

      <a class="card" href="/console">
        <div class="ic">🎛️</div>
        <b>Mission Control</b>
        <div class="muted">Живой контроль браузера: шаги агента, курсор, артефакты</div>
        <span class="badge" id="busyBadge">idle</span>
      </a>

      <a class="card" href="/analytics">
        <div class="ic">📈</div>
        <b>Отчёты и аналитика</b>
        <div class="muted">KPI, тренды, сравнение креативов (A/B)</div>
        <span class="badge">beta</span>
      </a>

      <a class="card" href="/audiences">
        <div class="ic">🎯</div>
        <b>Аудитории</b>
        <div class="muted">Сегменты, ретаргетинг, списки минус-слов</div>
        <span class="badge">beta</span>
      </a>

      <!-- ЗАМЕНА: вместо Артефактов/Переменных/Входа -->
      <a class="card" href="/account">
        <div class="ic">👤</div>
        <b>Личный кабинет</b>
        <div class="muted">Профиль, биллинг, API-токены</div>
      </a>

      <a class="card" href="/settings">
        <div class="ic">⚙️</div>
        <b>Настройки</b>
        <div class="muted">Профиль, лимиты, Guards/Humanize, интеграции</div>
      </a>
    </div>

    <!-- ===== STATUS ===== -->
    <div class="status" id="statusRow">
      <span class="pill">URL: <b id="stUrl" style="margin-left:6px">—</b></span>
      <span class="pill">etag: <b id="stEtag" style="margin-left:6px">0</b></span>
      <span class="pill">viewport: <b id="stVp" style="margin-left:6px">—</b></span>
      <span class="pill">shot: <b id="stSrc" style="margin-left:6px">—</b></span>
    </div>

    <footer>
      <div class="made"><span class="dot"></span> Powered by <b>EasyByte</b></div>
      <div class="muted">© HyperAI — Mission Hub</div>
    </footer>
  </div>

<script>
  /* ===== THEME ===== */
  const themeToggle = document.getElementById("themeToggle");
  (function initTheme(){
    const saved = localStorage.getItem("hyperai_theme");
    const prefersDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
    const initial = saved || (prefersDark ? "dark" : "light");
    applyTheme(initial);
    if (themeToggle) themeToggle.checked = (initial === "dark");
  })();
  function applyTheme(mode){
    document.documentElement.setAttribute("data-theme", mode);
    localStorage.setItem("hyperai_theme", mode);
  }
  if (themeToggle){
    themeToggle.addEventListener("change", ()=> applyTheme(themeToggle.checked ? "dark" : "light"));
  }

  /* ===== STATUS POLL (/api/status) ===== */
  const busyBadge = document.getElementById("busyBadge");
  const stUrl = document.getElementById("stUrl");
  const stEtag = document.getElementById("stEtag");
  const stVp = document.getElementById("stVp");
  const stSrc = document.getElementById("stSrc");

  async function updateStatus(){
    try{
      const r = await fetch("/api/status");
      const j = await r.json();
      stUrl.textContent = j?.url || "—";
      stEtag.textContent = j?.etag || "0";
      stVp.textContent = (j?.vp_w && j?.vp_h) ? (j.vp_w+"×"+j.vp_h+" @"+(j.dpr||1)) : "—";
      stSrc.textContent = j?.shot_src || "—";
      if (busyBadge){
        busyBadge.textContent = j?.busy ? "busy" : "idle";
        busyBadge.style.color = j?.busy ? "var(--warn)" : "inherit";
      }
    }catch(e){}
  }
  updateStatus();
  setInterval(updateStatus, 1200);

  /* ===== KPI placeholders (можно заменить на реальный /api/analytics/summary) ===== */
  const kpiActive = document.getElementById("kpiActive");
  const kpiBudget = document.getElementById("kpiBudget");
  const kpiConv   = document.getElementById("kpiConv");
  function setKpi(a="—", b="—", c="—"){ kpiActive.textContent=a; kpiBudget.textContent=b; kpiConv.textContent=c; }
  setKpi("—", "—", "—");
</script>
</body>
</html>
"""
