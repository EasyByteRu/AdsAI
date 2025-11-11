# ads_ai/web/app.py
from __future__ import annotations

import atexit
import base64
import json
import os
import queue
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple

from flask import Flask, request, jsonify, Response, make_response, stream_with_context, redirect

# Проектные зависимости/контракты
from ads_ai.config.settings import load_settings, Settings
from ads_ai.browser.adspower import start_adspower
from ads_ai.llm.gemini import GeminiClient
from ads_ai.plan.runtime import Runtime
from ads_ai.plan.repair import make_default_repairer
from ads_ai.tracing.trace import make_trace, JsonlTrace
from ads_ai.tracing.artifacts import Artifacts, take_screenshot, save_html_snapshot
from ads_ai.storage.vars import VarStore
from ads_ai.utils.ids import now_id
from ads_ai.web.home import HOME_HTML
from ads_ai.web.auth import init_auth
from ads_ai.web.profile import init_profile
from ads_ai.web.gads_sync import init_gads_sync


from ads_ai.web.create_companies import init_create_companies
from ads_ai.web.list_companies import init_list_companies
from ads_ai.web.company import init_company
from ads_ai.web.accounts_list import init_accounts_list  

# 🔹 ДОБАВЛЕНО: модуль аккаунтов + общие объекты из campaigns
from ads_ai.web.account import init_account_module  # аккаунты (мастер логина)
from ads_ai.web.campaigns import (                  # БД/диспетчер/пути для переиспользования
    _resolve_paths as _resolve_campaign_paths,
    CampaignDB,
    TaskManager,
)

# ------------------------------ Глобальное состояние ---------------------------

@dataclass
class AppState:
    settings: Settings
    driver: Any
    ai: GeminiClient
    vars: VarStore
    lock: threading.RLock

    # live-screenshot cache
    last_shot_png: Optional[bytes] = None
    last_shot_src: str = "none"             # 'cdp' | 'driver' | 'none'
    last_shot_ts: float = 0.0               # unix time (sec)
    etag: str = "0"                         # для клиентского сравнения

    # viewport metrics (для точного позиционирования курсора)
    last_vp_w: int = 0
    last_vp_h: int = 0
    last_dpr: float = 1.0
    last_scroll_x: float = 0.0
    last_scroll_y: float = 0.0

    # worker
    worker_thread: Optional[threading.Thread] = None
    worker_stop: threading.Event = field(default_factory=threading.Event)

    # status
    busy: bool = False
    busy_since: float = 0.0
    busy_stale_sec: float = 180.0


_state: Optional[AppState] = None


# ------------------------------ HTML (Light/Dark Glass UI) --------------------

INDEX_HTML = """<!doctype html>
<html lang="ru" data-theme="light">
<head>
  <meta charset="utf-8" />
  <title>HyperAI — Console</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    /* =======================================================================
       THEME TOKENS (light / dark)
    ======================================================================= */
    :root{
      --bg:#eef2f7; --bg2:#f6f8fb;
      --text:#111827; --muted:#6b7280;
      --glass: rgba(255,255,255,.66);
      --glass-2: rgba(255,255,255,.5);
      --border: rgba(17,24,39,.08);
      --ring: rgba(17,24,39,.06);
      --neon1:#38bdf8; --neon2:#a78bfa; --neon3:#34d399;
      --ok:#16a34a; --err:#ef4444; --warn:#f59e0b;
      --radius:24px; --radius-sm:16px;
      --shadow: 0 10px 30px rgba(15,23,42,.12);
      --shadow-big: 0 30px 80px rgba(15,23,42,.18);
      --content-max: 1680px;
      --chip-bg: rgba(255,255,255,.9);
      --kbd-bg:#f3f4f6; --kbd-text:#374151;
      --pill-bg: rgba(255,255,255,.8);
      --btn-grad-1:#ffffff; --btn-grad-2:#f4f7fb;
      --overlay-bg: rgba(255,255,255,.45);
      --cursor-main:#111827;
      --screen-border-radius:20px;
    }
    html[data-theme="dark"]{
      color-scheme: dark;
      --bg:#0b1220; --bg2:#0d1423;
      --text:#e5e7eb; --muted:#94a3b8;
      --glass: rgba(17,23,41,.55);
      --glass-2: rgba(17,23,41,.45);
      --border: rgba(255,255,255,.09);
      --ring: rgba(56,189,248,.15);
      --shadow: 0 10px 30px rgba(0,0,0,.35);
      --shadow-big: 0 30px 80px rgba(0,0,0,.45);
      --chip-bg: rgba(12,16,30,.9);
      --kbd-bg:#0f172a; --kbd-text:#e5e7eb;
      --pill-bg: rgba(15,21,38,.7);
      --btn-grad-1:#141b2f; --btn-grad-2:#0f172a;
      --overlay-bg: rgba(10,12,18,.45);
      --cursor-main:#e5e7eb;
    }

    *{box-sizing:border-box}
    html,body{height:100%;margin:0;color:var(--text);font:14px/1.45 Inter,system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;-webkit-font-smoothing:antialiased; text-rendering:optimizeLegibility;}
    html{ transition: background .35s ease, color .35s ease, filter .35s ease; }
    body{
      background:
        radial-gradient(1200px 800px at 20% -10%, #ffffff 0%, var(--bg) 48%, var(--bg2) 100%),
        linear-gradient(180deg,#ffffff, var(--bg2));
      transition: background .45s ease;
    }
    html[data-theme="dark"] body{
      background:
        radial-gradient(1200px 800px at 20% -10%, #0e1527 0%, var(--bg) 40%, var(--bg2) 100%),
        linear-gradient(180deg,#0f172a, var(--bg2));
    }

    /* =======================================================================
       SPLASH + "Powered by EasyByte"
    ======================================================================= */
    .splash{
      position:fixed; inset:0; z-index:9999; display:grid; place-items:center;
      background: radial-gradient(1400px 800px at 50% 0%, #fff 0%, #f6f8fb 40%, #eef2f7 100%);
      transition: opacity .45s ease, visibility .45s ease;
      overflow:hidden;
    }
    html[data-theme="dark"] .splash{
      background: radial-gradient(1400px 800px at 50% 0%, #0f172a 0%, #0d1423 40%, #0b1220 100%);
    }
    .splash.hide{ opacity:0; visibility:hidden; }
    .splash .skip{
      position:absolute; bottom:28px; right:28px; font-size:12px; color:#475569;
      background: rgba(255,255,255,.85); border:1px solid var(--border); padding:8px 12px; border-radius:999px;
      cursor:pointer; box-shadow: var(--shadow);
    }
    html[data-theme="dark"] .splash .skip{ color:#cbd5e1; background: rgba(17,23,41,.7); }
    .s-logo{ width:min(86vw,1200px); height:auto; display:block; overflow:visible; }
    .powered{
      margin-top:14px; font-size:14px; color:var(--muted);
      background: var(--glass); border:1px solid var(--border);
      padding:6px 12px; border-radius:999px; box-shadow: var(--shadow);
      text-shadow: 0 1px 0 rgba(255,255,255,.25);
    }
    html[data-theme="dark"] .powered{ text-shadow:none }

    .word{ transform-origin:50% 50%; will-change: transform, opacity, filter; pointer-events:none; }
    .stroke-base,.stroke-chase,.fill-cut{
      font-weight:900; letter-spacing:.02em;
      font-family: Inter,system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
      shape-rendering:geometricPrecision;
    }
    .stroke-base,.stroke-chase{
      fill:none; vector-effect:non-scaling-stroke;
      stroke-linecap:round; stroke-linejoin:round; paint-order:stroke;
      filter:url(#glow);
    }
    .stroke-base{
      stroke:url(#rg); stroke-width:2.6;
      stroke-dasharray:1400; stroke-dashoffset:1400;
      animation: draw 1.9s cubic-bezier(.2,.65,.2,1) forwards;
    }
    .stroke-chase{
      stroke:url(#rg); stroke-width:3.2; opacity:.95;
      stroke-dasharray: 40 340; stroke-dashoffset: 0;
      mask: url(#cutMask);
      animation: none;
    }
    .fill-cut{ fill:#0b1020; opacity:.04; filter:url(#soft); }

    .splash.trace .stroke-base{ animation: draw 1.9s cubic-bezier(.2,.65,.2,1) forwards; }
    .splash.chase .stroke-chase{ animation: dash .6s linear infinite; }
    .splash.fly .word{ animation: flyOut .8s cubic-bezier(.25,.9,.2,1.1) forwards; }

    @keyframes draw{ 0%{stroke-dashoffset:1400;opacity:.9} 100%{stroke-dashoffset:0;opacity:1} }
    @keyframes dash{ to{ stroke-dashoffset:-380 } }
    @keyframes flyOut{
      0%{ transform: translate3d(0,0,0) scale(1);   opacity:1;   filter:saturate(108%) }
      60%{transform: translate3d(0,-1.6vh,0) scale(4.6); opacity:.9; filter: blur(.4px) saturate(118%) }
      100%{transform: translate3d(0,-4vh,0) scale(10);opacity:0;  filter: blur(5px)  saturate(140%) }
    }

    .grain{
      position:absolute; inset:-20%; opacity:.06; pointer-events:none;
      background-image: radial-gradient(2px 2px at 20% 30%, #000 40%, transparent 40%),
                        radial-gradient(2px 2px at 80% 60%, #000 40%, transparent 40%);
      background-size: 180px 180px, 220px 220px;
      animation: grainMove 18s ease-in-out infinite alternate;
      filter: blur(.5px);
    }
    html[data-theme="dark"] .grain{ opacity:.1 }
    @keyframes grainMove{ 0%{transform:translateX(-4%) translateY(-3%)} 100%{transform:translateX(3%) translateY(4%)} }

    /* =======================================================================
       LAYOUT
    ======================================================================= */
    body:not(.ready) .shell{ opacity:0; transform: translateY(6px) scale(.985); filter: blur(6px); }
    body.ready .shell{ opacity:1; transform:none; filter:none; transition: opacity .45s ease, transform .55s cubic-bezier(.2,.9,.2,1), filter .45s ease; }

    .shell{
      display:grid; grid-template-columns: 260px minmax(0,1fr) 360px; gap:18px;
      min-height:100vh; padding:18px; max-width:var(--content-max); margin:0 auto;
    }
    .panel{
      background:var(--glass); border:1px solid var(--border); border-radius:var(--radius);
      backdrop-filter: blur(12px) saturate(160%); box-shadow:var(--shadow); overflow:hidden;
      transition: background .35s ease, border-color .35s ease, box-shadow .35s ease;
    }

    .menu{ display:flex; flex-direction:column; padding:18px; gap:14px; }
    .menu .head{ height:56px; display:flex; align-items:center; gap:10px; padding:0 6px; color:#374151; font-weight:600; }
    html[data-theme="dark"] .menu .head{ color:#cbd5e1 }
    .logo{
      width:36px;height:36px;border-radius:12px;
      
      box-shadow: 0 10px 40px #38bdf855, inset 0 0 0 1px #ffffff88;
    }
    .mitem{
      display:flex; align-items:center; gap:12px; padding:10px 12px; border-radius:14px; cursor:pointer; color:#374151;
      background: var(--glass-2); border:1px solid var(--border);
      transition: transform .09s ease, box-shadow .25s ease, background .25s ease;
    }
    html[data-theme="dark"] .mitem{ color:#e5e7eb }
    .mitem:hover{ transform: translateY(-1px); box-shadow: var(--shadow) }

    /* =================== STAGE + TOPBAR =================== */
    .stage{
      position:relative;
      display:grid; place-items:center;
      background: var(--glass);
      border:1px solid var(--border); border-radius:var(--radius);
      box-shadow:var(--shadow-big);
      padding: 56px 28px 96px;
      min-height: calc(100vh - 36px);
      width: 100%;
    }

    /* Верхняя плашка (над скриншотом) */
    .topbar{
      position:absolute; top:10px; left:50%; transform:translateX(-50%);
      display:flex; gap:8px; background: rgba(255,255,255,.72);
      border:1px solid var(--border); border-radius:999px; padding:6px 8px;
      backdrop-filter: blur(8px); box-shadow: var(--shadow); z-index:5;
    }
    html[data-theme="dark"] .topbar{ background: rgba(17,23,41,.6) }

    .pill{ display:inline-flex; align-items:center; gap:8px; padding:6px 10px; background: var(--pill-bg); border:1px solid var(--border); border-radius:999px; font-size:12px; color:#374151; }
    html[data-theme="dark"] .pill{ color:#e5e7eb }
    .pill.live::before{
      content:"•"; display:inline-block; margin-right:6px; font-size:16px; line-height:0;
      color:#ef4444; filter: drop-shadow(0 0 4px #ef4444aa);
      animation: livePulse 1.6s ease-in-out infinite;
    }
    @keyframes livePulse{
      0%{ opacity:.5; transform: scale(.9) }
      50%{ opacity:1; transform: scale(1.12) }
      100%{ opacity:.5; transform: scale(.9) }
    }

    .btn{
      border:1px solid var(--border); background: linear-gradient(180deg, var(--btn-grad-1), var(--btn-grad-2));
      color:var(--text); border-radius: 999px; padding:8px 14px; cursor:pointer;
      transition: transform .08s ease, box-shadow .25s ease, opacity .2s ease;
      position:relative; overflow:hidden;
    }
    .btn:hover{ transform: translateY(-1px); box-shadow: 0 10px 30px rgba(15,23,42,.15) }
    .btn:disabled{ opacity:.6; cursor:not-allowed }
    .btn .plane{ display:inline-block; transform: translateY(1px); }
    .btn.takeoff .plane{ animation: planeUp .45s cubic-bezier(.2,.9,.2,1) forwards; }
    @keyframes planeUp{
      0%{ transform: translate(0,1px) rotate(0) }
      60%{ transform: translate(10px,-8px) rotate(18deg) }
      100%{ transform: translate(24px,-12px) rotate(24deg); opacity:0 }
    }

    /* Карточка скрина */
    .shot-card{
      position:relative; display:inline-block;
      border-radius:28px; background:rgba(255,255,255,.72);
      border:1px solid var(--ring); box-shadow:0 10px 30px rgba(15,23,42,.12);
      backdrop-filter: blur(10px) saturate(160%);
      overflow:hidden;
    }
    html[data-theme="dark"] .shot-card{ background: rgba(15,21,38,.65) }
    .shot-card::before{
      content:""; position:absolute; inset:-1px; border-radius:inherit; padding:2px;
      background: linear-gradient(135deg, var(--neon1), var(--neon2), var(--neon3), var(--neon1));
      -webkit-mask: linear-gradient(#000 0 0) content-box, linear-gradient(#000 0 0);
      -webkit-mask-composite: xor; mask-composite: exclude;
      opacity:.55; filter: blur(.2px); pointer-events:none;
      animation: ringSpin 12s linear infinite;
    }
    .shot-card::after{
      content:""; position:absolute; inset:-3px; border-radius:inherit; pointer-events:none;
      background: radial-gradient(600px 300px at 20% 30%, #38bdf845, transparent 60%),
                  radial-gradient(600px 300px at 80% 70%, #a78bfa35, transparent 60%);
      filter: blur(22px); opacity:.7; mix-blend-mode: screen;
      animation: glowMove 14s ease-in-out infinite alternate;
    }
    @keyframes ringSpin{ to{ transform:rotate(360deg) } }
    @keyframes glowMove{ 0%{background-position: 10% 20%, 80% 70%} 100%{background-position: 30% 40%, 60% 60%} }

    .canvas{ position:relative; overflow:hidden; display:block; }
    .imgwrap{ position:relative; transform-origin: 0 0; will-change: transform; }
    .screen{ display:block; max-width:none; max-height:none; border-radius: var(--screen-border-radius); opacity:1; transition: opacity .18s ease; }

    /* Overlay загрузки */
    .overlay{ position:absolute; inset:0; display:none; align-items:center; justify-content:center; background: var(--overlay-bg); backdrop-filter: blur(2px); border-radius: inherit; }
    .overlay.show{ display:flex; }
    .spinner{ width:56px;height:56px;border:3px solid rgba(0,0,0,.08);border-top-color:var(--neon1);border-radius:50%; animation: spin 1s linear infinite; }
    @keyframes spin{to{transform:rotate(360deg)}}

    /* Ask bar */
    .askbar{
      position:absolute; left:50%; transform:translateX(-50%);
      bottom:24px; width:min(860px, 72%); display:grid; grid-template-columns: auto 1fr auto; gap:10px;
      background: rgba(255,255,255,.82); border:1px solid var(--border); border-radius:999px; padding:10px 12px;
      box-shadow:var(--shadow); backdrop-filter: blur(10px) saturate(140%); --spin:0deg;
    }
    html[data-theme="dark"] .askbar{ background: rgba(17,23,41,.7) }
    .askbar::before{
      content:""; position:absolute; inset:-2px; border-radius:999px; padding:2px;
      background: conic-gradient(from var(--spin), var(--neon1), var(--neon2), var(--neon3), var(--neon1));
      -webkit-mask: linear-gradient(#000 0 0) content-box, linear-gradient(#000 0 0);
      -webkit-mask-composite: xor; mask-composite: exclude;
      animation: spinGrad 8s linear infinite; opacity:.9; pointer-events:none; filter: blur(.2px);
    }
    .askbar::after{
      content:""; position:absolute; inset:-3px; border-radius:999px; pointer-events:none;
      background: radial-gradient(60% 80% at 50% 50%, rgba(56,189,248,.35), transparent 70%);
      filter: blur(16px); opacity:.6; mix-blend-mode: screen;
    }
    @keyframes spinGrad{ to{ --spin: 360deg } }
    .askbar .progress{ position:absolute; left:8px; right:8px; top:6px; height:3px; border-radius:999px; overflow:hidden; opacity:0; transition: opacity .2s ease; }
    .askbar .progress>i{ display:block; height:100%; width:40%; background: linear-gradient(90deg, var(--neon1), var(--neon2), var(--neon3)); animation: indet 1.4s ease-in-out infinite; }
    .askbar.sending { opacity: 1; }
    .askbar.sending .progress{ opacity:.8; }
    @keyframes indet{ 0%{ transform: translateX(-60%) } 100%{ transform: translateX(160%) } }

    .icon{ width:28px;height:28px;border-radius:999px; display:flex; align-items:center; justify-content:center; background:#ffffff; border:1px solid var(--border); box-shadow: 0 6px 16px rgba(15,23,42,.10); }
    html[data-theme="dark"] .icon{ background:#0b1220 }
    .textarea{
      resize:none; min-height:44px; max-height:220px; background: transparent; color:var(--text); border:0; outline:none; padding:8px 4px; font-size:14px; overflow:hidden;
    }

    /* RIGHT: Action Rail */
    .rail{ display:flex; flex-direction:column; padding:18px; gap:14px; }
    .rail .title{ font-weight:600; letter-spacing:.2px; color:var(--text); }
    .actions{ display:flex; flex-direction:column; gap:12px; overflow:auto; max-height: calc(100vh - 220px); scroll-behavior:smooth; }
    .chip{
      display:flex; align-items:center; gap:12px; padding:12px 14px;
      border-radius: 999px; background: var(--chip-bg);
      border:1px solid var(--border); color:var(--text);
      box-shadow: inset 0 0 0 1px #ffffff22, 0 8px 24px rgba(15,23,42,.10);
      transform-origin: 100% 50%;
      opacity:0; transform: translateX(24px) scale(.96);
      animation: slideIn .45s cubic-bezier(.18,.9,.2,1.1) forwards;
    }
    .chip .state{ margin-left:auto; font-size:12px; padding:2px 8px; border-radius:999px; border:1px solid var(--border); background:#f8fafc; color:#334155;}
    html[data-theme="dark"] .chip .state{ background:#0b1220; color:#cbd5e1 }
    .chip .state.ok{ color:var(--ok); border-color:#16a34a30; background:#f0fdf4 }
    .chip .state.fail{ color:var(--err); border-color:#ef444430; background:#fef2f2 }
    .chip.active{ box-shadow: 0 0 0 2px rgba(56,189,248,.35) inset, 0 10px 24px rgba(15,23,42,.08) }
    @keyframes slideIn{ 0%{opacity:0; transform: translateX(28px) scale(.94)} 60%{opacity:1; transform: translateX(-2px) scale(1.02)} 100%{opacity:1; transform: translateX(0) scale(1)} }

    .muted{color:var(--muted)}
    .kbd{ padding:2px 6px; border-radius:6px; background:var(--kbd-bg); border:1px solid var(--border); font:12px ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; color:var(--kbd-text);}

    /* ===== Overlay UI: курсор, клики, хайлайт, печать ===== */
    .overlay-ui{ position:absolute; left:0; top:0; width:0; height:0; pointer-events:none; }
    .cursor{
      position:absolute; width:18px; height:18px; border-radius:50%;
      background:var(--cursor-main); box-shadow: 0 0 0 2px #ffffff, 0 6px 16px rgba(15,23,42,.25);
      transform: translate3d(10px,10px,0);
      transition: transform .42s cubic-bezier(.22,.9,.2,1);
      will-change: transform;
    }
    html[data-theme="dark"] .cursor{ box-shadow: 0 0 0 2px #0b1220, 0 6px 16px rgba(0,0,0,.35); }
    .cursor > i{ display:block; width:100%; height:100%; border-radius:50%; background: radial-gradient(180px 180px at 30% 30%, var(--cursor-main), #1f2937); }
    html[data-theme="dark"] .cursor > i{ background: radial-gradient(180px 180px at 30% 30%, var(--cursor-main), #0b1220); }
    .click-ring{
      position:absolute; width:10px; height:10px; border-radius:50%; border:2px solid rgba(56,189,248,.9);
      transform: translate3d(-9999px,-9999px,0) scale(1); opacity:0; filter: blur(.2px);
    }
    .click-ring.show{ animation: ring .5s ease-out forwards; }
    @keyframes ring{
      0%{ opacity:1; transform: translate3d(var(--x),var(--y),0) scale(.9) }
      100%{ opacity:0; transform: translate3d(var(--x),var(--y),0) scale(3.2) }
    }
    .hl{
      position:absolute; border-radius:8px; border:2px solid rgba(167,139,250,.95);
      box-shadow: 0 0 0 4px rgba(167,139,250,.18), 0 8px 30px rgba(99,102,241,.25);
      transform: translate3d(-9999px,-9999px,0);
      transition: transform .18s ease, width .18s ease, height .18s ease, opacity .18s ease;
      opacity:0;
    }
    .hl.show{ opacity:1; }

    /* Пузырь "печати текста" */
    .type-ghost{
      position:absolute; max-width: 420px; min-width: 80px; white-space: pre-wrap; word-break: break-word;
      background: rgba(255,255,255,.95); border:1px solid var(--border); color:#0f172a;
      border-radius:12px; padding:8px 10px; box-shadow: var(--shadow);
      transform: translate3d(-9999px,-9999px,0);
      filter:none; opacity:0; transition: opacity .2s ease, transform .2s ease;
      font-size: 13px;
    }
    html[data-theme="dark"] .type-ghost{ background: rgba(17,23,41,.95); color:#e5e7eb }
    .shot-card { position: relative; }
    .overlay { z-index: 2; }
    .overlay-ui { z-index: 3; }

    /* Reduced motion */
    @media (prefers-reduced-motion: reduce){
      .stroke-base, .stroke-chase, .word, .shot-card::before, .shot-card::after, .askbar::before, .askbar::after{ animation:none !important }
      .screen{ transition:none !important }
      .cursor{ transition:none !important }
      .fly-ghost.animate{ transition:none !important }
    }
  </style>
</head>
<body>

<!-- ======== SPLASH OVERLAY ======== -->
<div class="splash trace" id="splash" aria-hidden="true">
  <div class="grain"></div>
  <svg class="s-logo" viewBox="0 0 1600 400" preserveAspectRatio="xMidYMid meet" overflow="visible" role="img" aria-label="HyperAI intro">
    <defs>
      <linearGradient id="rg" x1="0" y1="0" x2="1600" y2="0" gradientUnits="userSpaceOnUse">
        <stop offset="0%"  stop-color="#38bdf8"/>
        <stop offset="50%" stop-color="#a78bfa"/>
        <stop offset="100%" stop-color="#34d399"/>
      </linearGradient>
      <filter id="glow" x="-40%" y="-40%" width="180%" height="180%">
        <feGaussianBlur in="SourceGraphic" stdDeviation="2.2" result="b1"/>
        <feGaussianBlur in="SourceGraphic" stdDeviation="6"   result="b2"/>
        <feMerge><feMergeNode in="b2"/><feMergeNode in="b1"/><feMergeNode in="SourceGraphic"/></feMerge>
      </filter>
      <filter id="soft" x="-40%" y="-40%" width="180%" height="180%"><feGaussianBlur stdDeviation="1.1"/></filter>
      <mask id="cutMask" maskUnits="userSpaceOnUse">
        <g opacity=".95" filter="url(#soft)">
          <text x="50%" y="58%" text-anchor="middle" font-size="220" fill="#fff">HyperAI</text>
        </g>
      </mask>
    </defs>
    <g class="word">
      <text x="50%" y="58%" text-anchor="middle" class="stroke-base" font-size="220">HyperAI</text>
      <text x="50%" y="58%" text-anchor="middle" class="stroke-chase" font-size="220">HyperAI</text>
      <text x="50%" y="58%" text-anchor="middle" class="fill-cut"    font-size="220">HyperAI</text>
    </g>
  </svg>
  <div class="powered">Powered by <b>EasyByte</b></div>
  <div class="skip" id="splashSkip">Skip ⏭</div>
</div>

<div class="shell">
  <!-- LEFT MENU -->
  <aside class="panel menu">
    <div class="head">
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
      <div>Меню</div>
    </div>
    <div class="mitem" title="Артефакты">
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none"><path d="M4 7h16M4 12h16M4 17h16" stroke="currentColor" stroke-width="2"/></svg>
      Артефакты
    </div>
    <div class="mitem" title="Настройки">
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none"><path d="M12 15a3 3 0 1 0 0-6 3 3 0 0 0 0 6Z"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 1 1-4 0v-.09a1.65 1.65 0 0 0-1-1.51 1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 1 1 0-4h.09c.67 0 1.27-.39 1.51-1 .24-.61.11-1.3-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06c.52.44 1.21.57 1.82.33.61-.24 1-.84 1-1.51V3a2 2 0 1 1 4 0v.09c0 .67.39 1.27 1 1.51.61.24 1.3.11 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06c-.44.52-.57 1.21-.33 1.82.24-.61.84-1 1.51 1H21a2 2 0 1 1 0 4h-.09c-.67 0-1.27.39-1.51 1Z" stroke="currentColor" stroke-width="1"/></svg>
      Настройки
    </div>
    <div style="margin-top:auto" class="muted">Powered by EasyByte</div>
  </aside>

  <!-- CENTER STAGE -->
  <section class="panel stage" id="stage">
    <!-- TOPBAR над скриншотом -->
    <div class="topbar" id="topbar">
      <span class="pill live">Live <input type="checkbox" id="auto" checked style="margin-left:6px"></span>
      <span class="pill"><span id="fitlbl">Fit</span> <input type="checkbox" id="fit" checked></span>
      <span class="pill"><span id="themelbl">Dark</span> <input type="checkbox" id="themeToggle"></span>
      <button class="btn" id="refresh">Обновить</button>
    </div>

    <div class="shot-card" id="card">
      <div class="overlay" id="overlay"><div class="spinner"></div></div>

      <div class="canvas" id="canvas">
        <div class="imgwrap" id="wrap" style="transform: translate(0px,0px) scale(1);">
          <img id="screen" class="screen" src="/api/screenshot?ts=0" alt="screenshot"/>
          <div class="overlay-ui" id="overlayUi">
            <div class="cursor" id="cursor"><i></i></div>
            <div class="click-ring" id="clickRing"></div>
            <div class="hl" id="hl"></div>
            <div class="type-ghost" id="typeGhost"></div>
          </div>
        </div>
      </div>
    </div>

    <!-- Ask bar -->
    <div class="askbar" id="askbar">
      <div class="progress"><i></i></div>
      <div class="icon" title="Задание">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none"><path d="M4 7h16M4 12h16M4 17h10" stroke="#0f172a" stroke-width="2"/></svg>
      </div>
      <textarea id="inp" class="textarea" placeholder="Опиши задачу: «Открой google.com, найди weather Toronto, открой первый результат»" spellcheck="false"></textarea>
      <button class="btn" id="send" title="Отправить (Ctrl/Cmd+Enter)"><span class="plane">➤</span></button>
    </div>
  </section>

  <!-- RIGHT: ACTION RAIL -->
  <aside class="panel rail">
    <div class="title">Действия агента</div>
    <div class="muted" style="margin-top:-6px">Чипы появляются каскадом и обновляются по мере выполнения. Курсор показывает «куда» агент действует.</div>
    <div class="actions" id="actions"></div>
    <div style="margin-top:auto" class="muted">
      <div style="display:flex;gap:8px;align-items:center"><span class="kbd">URL</span><span id="url" style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:210px">—</span></div>
      <div style="margin-top:6px;display:flex;gap:8px;align-items:center"><span class="kbd">etag</span><span id="etag">0</span></div>
      <div style="margin-top:6px;display:flex;gap:8px;align-items:center"><span class="kbd">busy</span><span id="busyTxt">idle</span></div>
      <div style="margin-top:6px;display:flex;gap:8px;align-items:center"><span class="kbd">Hint</span>PgUp/PgDn — скролл страницы</div>
    </div>
  </aside>
</div>

<script>
const $ = (s)=>document.querySelector(s);

// DOM refs
const stage = $("#stage");
const card = $("#card");
const canvas = $("#canvas");
const wrap = $("#wrap");
const img = $("#screen");
const overlay = $("#overlay");
const fitChk = $("#fit"); const fitLbl = $("#fitlbl");
const autoChk = $("#auto");
const refreshBtn = $("#refresh");
const inp = $("#inp");
const send = $("#send");
const askbar = $("#askbar");
const actions = $("#actions");
const urlSpan = $("#url");
const etagSpan = $("#etag");
const busyTxt = $("#busyTxt");
const splash = $("#splash");
const splashSkip = $("#splashSkip");
const strokeBase = document.querySelector('.stroke-base');
// overlay UI
const overlayUi = $("#overlayUi");
const cursorEl = $("#cursor");
const clickRing = $("#clickRing");
const hl = $("#hl");
const typeGhost = $("#typeGhost");
// theme
const themeToggle = $("#themeToggle");
const themeLbl = $("#themelbl");

let auto = true;
let fit = true;                 // Fit по умолчанию
let zoom = 1.0;
let panX = 0, panY = 0, startX = 0, startY = 0, originX = 0, originY = 0, panning = false;
let lastEtag = "0";
let isTyping = false, lastKeyTs = 0;

let currentPlan = [];
let lastMetrics = { w: 1440, h: 900, dpr: 1, sx: 0, sy: 0 };
let curIX = 20, curIY = 20;

// ===== Theme init/persist =====
(function initTheme(){
  const saved = localStorage.getItem("hyperai_theme");
  const prefersDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
  const initial = saved || (prefersDark ? "dark" : "light");
  applyTheme(initial);
  themeToggle.checked = (initial === "dark");
  themeLbl.textContent = "Dark";
})();
function applyTheme(mode){
  document.documentElement.setAttribute("data-theme", mode);
  localStorage.setItem("hyperai_theme", mode);
}
themeToggle.addEventListener("change", ()=>{
  applyTheme(themeToggle.checked ? "dark" : "light");
});

// ===== Splash =====
(function initSplash(){
  const reduce = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  const skip = localStorage.getItem('hyperai_skip_splash')==='1';
  function endSplash(){
    document.body.classList.add('ready');
    splash?.classList.add('hide'); setTimeout(()=> splash?.remove(), 260);
  }
  if(!splash || reduce || skip){ endSplash(); return; }

  strokeBase?.addEventListener('animationend', (e)=>{
    if(e.animationName==='draw'){ splash.classList.add('chase'); startFly(); }
  }, {once:true});

  function startFly(){
    setTimeout(()=>{
      splash.classList.add('fly');
      splash.querySelector('.word')?.addEventListener('animationend', (ev)=>{
        if(ev.animationName==='flyOut'){ endSplash(); }
      }, {once:true});
    }, 380);
  }
  splash.addEventListener('dblclick', ()=>{ endSplash(); localStorage.setItem('hyperai_skip_splash','1'); });
  if (splashSkip){ splashSkip.addEventListener('click', ()=>{ endSplash(); localStorage.setItem('hyperai_skip_splash','1'); }); }
})();

/* ===== helpers ===== */
function setBusy(v){
  if(v){ overlay.classList.add("show"); busyTxt.textContent="busy"; askbar.classList.add('sending'); }
  else { overlay.classList.remove("show"); busyTxt.textContent="idle"; askbar.classList.remove('sending'); }
}
function escapeHtml(s){ return s.replace(/[&<>"]/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }

/* Ключевая функция: размеры карточки == размеры видимого скрина */
function layoutCard(){
  const nw = img.naturalWidth || 1024;
  const nh = img.naturalHeight || 768;

  // подогнать overlay UI к натуральному размеру скрина
  overlayUi.style.width = nw + "px";
  overlayUi.style.height = nh + "px";

  const st = stage.getBoundingClientRect();
  const availW = Math.max(120, st.width - 48);
  const availH = Math.max(120, st.height - 140); // запас на topbar

  if (fit){
    const scale = Math.min(availW / nw, availH / nh, 1);
    zoom = scale; panX = 0; panY = 0;

    wrap.style.transform = `translate(0px,0px) scale(${scale})`;
    const cw = Math.round(nw * scale);
    const ch = Math.round(nh * scale);
    canvas.style.width  = cw + "px";
    canvas.style.height = ch + "px";
    card.style.width    = cw + "px";
    card.style.height   = ch + "px";

    fitLbl.textContent = "Fit";
  }else{
    const scale = zoom;
    wrap.style.transform = `translate(${panX}px,${panY}px) scale(${scale})`;

    const vw = Math.min(nw, Math.floor(availW));
    const vh = Math.min(nh, Math.floor(availH));
    canvas.style.width  = vw + "px";
    canvas.style.height = vh + "px";
    card.style.width    = vw + "px";
    card.style.height   = vh + "px";

    fitLbl.textContent = "1:1";
  }
}

/* toggles */
fitChk.addEventListener("change", ()=>{
  fit = fitChk.checked;
  if (fit){ panX=panY=0; }
  layoutCard();
});
autoChk.addEventListener("change", ()=> { auto = autoChk.checked });

refreshBtn.addEventListener("click", ()=> refreshShot(true));

/* pan/zoom (только в 1:1 режиме) + прокрутка */
canvas.addEventListener("mousedown", (e)=>{
  if(fit) return;
  panning = true; originX=panX; originY=panY; startX=e.clientX; startY=e.clientY; canvas.style.cursor="grabbing";
});
window.addEventListener("mousemove", (e)=>{
  if(!panning) return;
  panX = originX + (e.clientX-startX);
  panY = originY + (e.clientY-startY);
  wrap.style.transform = `translate(${panX}px,${panY}px) scale(${zoom})`;
});
window.addEventListener("mouseup", ()=>{ panning=false; canvas.style.cursor="default"; });
canvas.addEventListener("wheel", (e)=>{
  if(e.ctrlKey && !fit){
    e.preventDefault();
    const prev = zoom;
    let z = zoom * (e.deltaY < 0 ? 1.08 : 0.92);
    z = Math.max(0.4, Math.min(3.0, z));
    // фокус-зум к курсору
    const rect = wrap.getBoundingClientRect();
    const cx = e.clientX - rect.left, cy = e.clientY - rect.top;
    const k = z/prev;
    panX = cx - (cx - panX)*k; panY = cy - (cy - panY)*k; zoom = z;
    layoutCard();
  }else{
    e.preventDefault();
    let dy = e.deltaY; if (e.shiftKey) dy *= 3;
    sendScroll(Math.trunc(dy));
  }
},{passive:false});

/* status / screenshot */
async function updateStatus(){
  try{
    const r = await fetch("/api/console_status");  // 🔁 было /api/status
    const j = await r.json();
    urlSpan.textContent = j.url || "—";
    etagSpan.textContent = j.etag || "0";
    if (j.vp_w && j.vp_h){ lastMetrics = { w: j.vp_w, h: j.vp_h, dpr: j.dpr || 1, sx: j.scrollX||0, sy: j.scrollY||0 }; }
    if (j.etag && j.etag !== lastEtag && auto){
      lastEtag = j.etag; refreshShot(true);
    }
    setBusy(!!j.busy);
  }catch(e){}
}
function refreshShot(cacheBust=false){
  const q = cacheBust ? ("e="+lastEtag+"&rnd="+(crypto?.randomUUID?.()||Date.now())) : ("ts="+Date.now());
  const url = "/api/screenshot?"+q;
  img.style.opacity = .35;
  img.onload = () => { img.style.opacity = 1; layoutCard(); };
  img.src = url;
}

/* typing pause (не дёргаем live во время ввода) */
inp.addEventListener("focus", ()=>{ isTyping=true; });
inp.addEventListener("blur",  ()=>{ isTyping=false; });
inp.addEventListener("keydown", ()=>{ lastKeyTs=Date.now(); });

/* polling */
setInterval(()=>{
  updateStatus();
  const typing = (Date.now()-lastKeyTs < 800);
  if (auto && !typing) refreshShot(true);
}, 1100);

/* keyboard scroll */
function throttle(fn, ms){ let last=0,t=null,q=null; return (...args)=>{ const now=Date.now(),left=ms-(now-last); if(left<=0){ last=now; fn(...args);} else { q=args; clearTimeout(t); t=setTimeout(()=>{ last=Date.now(); fn(...q); }, left);} }; }
const sendScroll = throttle(async(dy)=>{ try{ await fetch("/api/scroll",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({dy})}); }catch(e){} }, 120);
window.addEventListener("keydown",(e)=>{
  if (e.target===inp) {
    // Ctrl/Cmd + Enter — отправка
    if(e.key==='Enter' && (e.ctrlKey || e.metaKey)){ e.preventDefault(); send.click(); }
    return;
  }
  const H=700;
  if (["PageDown","ArrowDown"].includes(e.key)){ sendScroll(H); e.preventDefault(); }
  if (["PageUp","ArrowUp"].includes(e.key)){ sendScroll(-H); e.preventDefault(); }
  if (e.key==="Home"){ sendScroll(-9999999); e.preventDefault(); }
  if (e.key==="End"){  sendScroll(9999999); e.preventDefault(); }
  // Быстрый фокус на поле ввода
  if ((e.ctrlKey||e.metaKey) && e.key.toLowerCase()==='k'){ e.preventDefault(); inp.focus(); }
});

/* autosize + hotkeys */
function autosize(){ inp.style.height='auto'; inp.style.height=Math.min(220, Math.max(44, inp.scrollHeight))+'px'; }
inp.addEventListener('input', autosize);
setTimeout(autosize, 0);

/* ===== Overlay UI: геометрия и анимации ===== */
function imgScaleX(){ return img.naturalWidth / Math.max(1, lastMetrics.w); }
function imgScaleY(){ return img.naturalHeight / Math.max(1, lastMetrics.h); }

function moveCursorTo(ix, iy, ms=420){
  const reduce = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  if (reduce) ms = 0;
  cursorEl.style.transitionDuration = Math.max(0, ms) + "ms";
  cursorEl.style.transform = `translate3d(${ix}px,${iy}px,0)`;
  curIX = ix; curIY = iy;
}

function showClickRing(ix, iy){
  clickRing.style.setProperty('--x', ix+'px');
  clickRing.style.setProperty('--y', iy+'px');
  clickRing.classList.remove('show');
  void clickRing.offsetWidth; // перезапуск анимации
  clickRing.classList.add('show');
}

function showHighlight(l, t, w, h){
  hl.style.width = w + "px";
  hl.style.height = h + "px";
  hl.style.transform = `translate3d(${l}px,${t}px,0)`;
  hl.classList.add('show');
}
function hideHighlight(){ hl.classList.remove('show'); }

/* ===== Реал-тайм показ набора текста ===== */
let typeTimer = null;
let typeTextTarget = "";
let typeIndex = 0;
function startTypeEffect(rect, text){
  stopTypeEffect();
  if (!text) return;
  typeTextTarget = text;
  typeIndex = 0;
  const scaleX = imgScaleX();
  const scaleY = imgScaleY();
  const left = Math.max(6, rect.left*scaleX + 6);
  const top  = Math.max(6, (rect.top*scaleY) - 34); // над полем
  typeGhost.style.transform = `translate3d(${left}px,${top}px,0)`;
  typeGhost.textContent = "";
  typeGhost.style.opacity = 1;

  const baseDelay = 28; // мс/символ
  typeTimer = setInterval(()=>{
    if (typeIndex >= typeTextTarget.length){ stopTypeEffect(); return; }
    // имитируем «человечность»: иногда задержка на пробелах/знаках
    const ch = typeTextTarget[typeIndex];
    typeGhost.textContent = typeTextTarget.slice(0, typeIndex+1);
    typeIndex++;
    if (/[,.!? ]/.test(ch)){
      // короткая пауза
      const pause = 40 + Math.random()*120;
      clearInterval(typeTimer);
      typeTimer = setInterval(()=>{ /* продолжим на следующем тике */ }, baseDelay + pause);
    }
  }, baseDelay + Math.random()*35);
}
function stopTypeEffect(){
  if (typeTimer){ clearInterval(typeTimer); typeTimer = null; }
  typeGhost.style.opacity = 0;
  typeGhost.style.transform = `translate3d(-9999px,-9999px,0)`;
}

/* План → чипы → запуск (SSE) + курсор */
const STEP_ICONS = {
  click: '<svg width="16" height="16" viewBox="0 0 24 24" fill="none"><path d="m9 11 4 10 2-6 6-2-10-4-4-8v10h2Z" stroke="currentColor"/></svg>',
  input: '<svg width="16" height="16" viewBox="0 0 24 24" fill="none"><rect x="3" y="5" width="18" height="14" rx="3" stroke="currentColor"/><path d="M7 12h10" stroke="currentColor" /></svg>',
  navigate: '<svg width="16" height="16" viewBox="0 0 24 24" fill="none"><path d="M3 12h18M13 6l6 6-6 6" stroke="currentColor"/></svg>',
  wait: '<svg width="16" height="16" viewBox="0 0 24 24" fill="none"><circle cx="12" cy="12" r="9" stroke="currentColor"/><path d="M12 6v6l4 2" stroke="currentColor"/></svg>',
  select: '<svg width="16" height="16" viewBox="0 0 24 24" fill="none"><rect x="3" y="5" width="18" height="14" rx="3" stroke="currentColor"/><path d="M8 12l2 2 6-6" stroke="currentColor"/></svg>',
  default: '<svg width="16" height="16" viewBox="0 0 24 24" fill="none"><path d="M12 2l7 7-7 7-7-7 7-7Z" stroke="currentColor"/></svg>',
};
function chipHtml(step, idx){
  const t = (step.type||"").toLowerCase();
  const icon = STEP_ICONS[t] || STEP_ICONS.default;
  const label = (t||"step") + (step.selector ? ` · <span class="muted">${escapeHtml(String(step.selector)).slice(0,46)}</span>` : "");
  return `<div class="chip" data-idx="${idx}">${icon}<b>${label}</b><span class="state">pending</span></div>`;
}
function clearActions(){ actions.innerHTML = ""; currentPlan = []; }
function addAction(step, idx, delayMs){ setTimeout(()=>{ actions.insertAdjacentHTML('beforeend', chipHtml(step, idx)); if(idx===0){ actions.firstElementChild?.classList.add('active'); } }, delayMs); }
function addPlanChunk(steps){
  if (!Array.isArray(steps) || !steps.length) return;
  const startIdx = currentPlan.length;
  steps.forEach(s => currentPlan.push(s));
  steps.forEach((s,i)=> addAction(s, startIdx + i, i*90));
  // активируем первую новую чипу, если нет активной
  const activeIdx = getActiveChipIdx();
  if (activeIdx === -1 && steps.length){
    const chips = actions.querySelectorAll(".chip");
    chips[startIdx]?.classList.add('active');
    setTimeout(()=> animateToActive(), (steps.length*90)+180);
  }
}
function getActiveChipIdx(){
  const chips = actions.querySelectorAll(".chip");
  return Array.from(chips).findIndex(c=>c.classList.contains('active'));
}

async function locateWithRetry(selector, tries=5, baseDelay=140){
  for(let i=0;i<tries;i++){
    try{
      const r = await fetch("/api/locate", {
        method:"POST",
        headers:{ "Content-Type":"application/json" },
        body: JSON.stringify({ selector })
      });
      if (r.status === 409 || r.status === 423){ // лок занят — подождём и ретраим
        await new Promise(res => setTimeout(res, baseDelay * (i+1)));
        continue;
      }
      if (!r.ok){
        await new Promise(res => setTimeout(res, baseDelay));
        continue;
      }
      const j = await r.json();
      if (j && j.found && j.rect && j.viewport) return j;
    }catch(_){}
    await new Promise(res => setTimeout(res, baseDelay));
  }
  return null;
}

async function animateToActive(){
  const i = getActiveChipIdx();
  if (i<0 || !currentPlan[i]){ hideHighlight(); stopTypeEffect(); return; }
  const step = currentPlan[i];
  const sel = step.selector || "";
  if (!sel){ hideHighlight(); stopTypeEffect(); return; }

  const j = await locateWithRetry(sel);
  if (!j) { hideHighlight(); stopTypeEffect(); return; }

  lastMetrics = { w: j.viewport.w||lastMetrics.w, h: j.viewport.h||lastMetrics.h, dpr: j.viewport.dpr||lastMetrics.dpr, sx: j.viewport.sx||0, sy: j.viewport.sy||0 };
  const scaleX = img.naturalWidth / Math.max(1, lastMetrics.w);
  const scaleY = img.naturalHeight / Math.max(1, lastMetrics.h);
  const ix = j.rect.centerX * scaleX;
  const iy = j.rect.centerY * scaleY;
  moveCursorTo(ix, iy, 420);
  showHighlight(j.rect.left*scaleX, j.rect.top*scaleY, j.rect.width*scaleX, j.rect.height*scaleY);

  if ((step.type||'').toLowerCase()==='input'){
    startTypeEffect(j.rect, String(step.text||''));
  } else {
    stopTypeEffect();
  }
}
function markNext(kind){
  const chips = actions.querySelectorAll(".chip");
  const idx = Array.from(chips).findIndex(c=>c.classList.contains('active'));
  const i = idx === -1 ? 0 : idx;
  if (i >= chips.length) return;
  const ch = chips[i];
  const st = ch.querySelector(".state");
  st.textContent = kind;
  st.classList.add(kind === 'ok' ? 'ok' : 'fail');
  ch.classList.remove('active');

  // Завершили шаг: останавливаем печать
  stopTypeEffect();

  if (i + 1 < chips.length) {
    chips[i+1].classList.add('active');
    // поедем курсором к следующему шагу
    animateToActive();
    // если текущий был click — ринг клика
    const step = currentPlan[i];
    if (step && String(step.type||'').toLowerCase()==='click'){
      showClickRing(curIX, curIY);
    }
    // авто-прокрутка списка чипов
    chips[i+1].scrollIntoView({block:'nearest', inline:'nearest', behavior:'smooth'});
  }else{
    hideHighlight();
  }
}
function markDone(nOk, nTotal){
  const chips = actions.querySelectorAll(".chip");
  chips.forEach((ch,i)=>{
    const st = ch.querySelector(".state");
    if (st.textContent !== 'pending') return;
    if (i < nOk){ st.textContent = "ok"; st.classList.add("ok"); }
    else { st.textContent = "skip"; st.classList.add("fail"); }
    ch.classList.remove('active');
  });
  hideHighlight();
  stopTypeEffect();
}

/* ===== Анимация отправки: призрак текста + взлёт самолётика ===== */
function launchGhost(text){
  try{
    const r = inp.getBoundingClientRect();
    const rs = stage.getBoundingClientRect();
    const ghost = document.createElement('div');
    ghost.className = 'type-ghost';
    ghost.textContent = text;
    ghost.style.left = (r.left - rs.left + 46) + "px";   // чуть внутрь askbar
    ghost.style.top  = (r.top - rs.top - 6) + "px";
    ghost.style.maxWidth = (Math.min(860, rs.width*0.72) - 92) + "px";
    ghost.style.opacity = 1;
    ghost.style.transform = `translate3d(${(r.left - rs.left + 46)}px, ${(r.top - rs.top - 6)}px, 0)`;
    stage.appendChild(ghost);
    requestAnimationFrame(()=>{ ghost.style.opacity = 0; ghost.style.transform = `translate3d(${(r.left - rs.left + 46)}px, ${(r.top - rs.top - 66)}px, 0)`; });
    ghost.addEventListener('transitionend', ()=> ghost.remove(), {once:true});
  }catch(e){}
}

/* Отправка (SSE run) */
send.onclick = async ()=>{
  const text = inp.value.trim(); if (!text) return;

  // Анимации отправки
  launchGhost(text);
  send.classList.add('takeoff');
  setTimeout(()=> send.classList.remove('takeoff'), 600);

  // очищаем инпут
  inp.value = ""; autosize();

  send.disabled = true; askbar.classList.add("sending"); clearActions();

  // режим: по умолчанию используем incremental PE; оставим ?legacy для старого варианта
  const useLegacy = /#legacy$/i.test(location.hash);
  const es = new EventSource("/api/run_stream?mode=" + (useLegacy ? "legacy" : "pe") + "&task=" + encodeURIComponent(text));
  es.onmessage = (ev)=>{
    try{
      const msg = JSON.parse(ev.data || "{}");
      if (msg.event === "start" && Array.isArray(msg.plan)){
        // Стартовая пачка шагов
        currentPlan = msg.plan || [];
        msg.plan.forEach((s,i)=> addAction(s, i, i*90));
        setTimeout(()=> animateToActive(), (msg.plan.length*90)+180);
      } else if (msg.event === "plan_chunk" && Array.isArray(msg.plan)) {
        // Инкрементальное добавление шагов (следующие подцели/фиксы)
        addPlanChunk(msg.plan);
      } else if ((msg.event||"").match(/(step.*ok|action.*ok|_ok)$/i)) {
        markNext('ok');
      } else if ((msg.event||"").match(/(error|fail|skip)/i)) {
        markNext('fail');
      } else if (msg.event === "run_done"){
        const nOk = msg.stats?.ok_steps ?? (Array.isArray(msg.done_steps)? msg.done_steps.length : 0);
        const total = msg.stats?.total_steps ?? (msg.planned_total ?? 0);
        markDone(nOk, total);
      } else if (msg.event === "end"){
        es.close();
        askbar.classList.remove("sending");
        send.disabled = false;
        refreshShot(true); updateStatus();
      }
    }catch(e){}
  };
  es.onerror = ()=>{ try{es.close();}catch(e){} askbar.classList.remove("sending"); send.disabled=false; };
};

// init sizing
img.addEventListener("load", layoutCard);
window.addEventListener("resize", layoutCard);
fit = fitChk.checked; layoutCard();
updateStatus();

// Скролл списка чипов колесом с зажатым Alt — мелкая плюшка
actions.addEventListener("wheel", (e)=>{ if(!e.altKey) return; e.preventDefault(); actions.scrollTop += e.deltaY>0 ? 80 : -80; }, {passive:false});
</script>
</body>
</html>
"""


# ------------------------------ Вспомогательные функции -----------------------

def _ensure_env_keys(s: Settings) -> None:
    """Пробрасываем ключи в ENV, как в runner."""
    if s.gemini_api_key:
        os.environ.setdefault("GOOGLE_API_KEY", s.gemini_api_key)
    if s.adsp_api_token:
        os.environ.setdefault("ADSP_API_TOKEN", s.adsp_api_token)
    if s.runware.api_key:
        os.environ.setdefault("RUNWARE_API_KEY", s.runware.api_key)
    if s.runware.model_id:
        os.environ.setdefault("RUNWARE_MODEL_ID", s.runware.model_id)
    if s.runware.base_url:
        os.environ.setdefault("RUNWARE_URL", s.runware.base_url)
    os.environ.setdefault("ADSP_API_BASE", s.browser.adsp_api_base)


def _selector_to_str(sel: Any) -> str:
    """Превращаем объектный selector от LLM в строковый для нашего рантайма/UI."""
    if isinstance(sel, str):
        return sel.strip()
    if not isinstance(sel, dict):
        return str(sel or "").strip()

    if "css" in sel and sel["css"]:
        return f"{sel['css']}".strip()
    if "xpath" in sel and sel["xpath"]:
        return f"xpath={sel['xpath']}".strip()
    if "id" in sel and sel["id"]:
        return f"#{sel['id']}".strip()
    if "name" in sel and sel["name"]:
        v = str(sel["name"]).replace('"', '\\"')
        return f'[name="{v}"]'
    if "role" in sel and sel["role"]:
        r = str(sel["role"]).strip()
        nm = str(sel.get("name", "")).strip()
        return f'role={r}[name="{nm}"]' if nm else f"role={r}"
    if "aria" in sel and sel["aria"]:
        return f'aria={sel["aria"]}'.strip()
    if "ariaLabel" in sel and sel["ariaLabel"]:
        return f'aria={sel["ariaLabel"]}'.strip()
    if "text" in sel and sel["text"]:
        return f'text={sel["text"]}'.strip()
    return str(sel)


def _normalize_plan_steps(steps: List[Dict[str, Any]], current_url: str) -> List[Dict[str, Any]]:
    """Нормализуем LLM-план под наш StepType/схему и устойчивые селекторы."""
    out: List[Dict[str, Any]] = []
    cur = (current_url or "").lower()

    for st in steps or []:
        t = str(st.get("type") or "").strip().lower()
        if not t:
            continue
        if t == "type":
            t = "input"

        sel_raw = st.get("selector")
        sel = _selector_to_str(sel_raw) if sel_raw is not None else ""

        ns: Dict[str, Any] = {**st}
        ns["type"] = t
        if sel:
            ns["selector"] = sel

        if t == "input":
            val = st.get("text", None)
            if val is None:
                val = st.get("value", "")
            ns["text"] = "" if val is None else str(val)
            ns.pop("value", None)

        out.append(ns)

        press = bool(st.get("press_enter") or st.get("enter"))
        if press and "google." in cur:
            out.append({"type": "click", "selector": "input[name='btnK']"})

    return out


def _new_runtime(state: AppState) -> tuple[Runtime, JsonlTrace, Artifacts, str]:
    """Готовим разовый Runtime под конкретный запуск (run_id → отдельные артефакты/трейс)."""
    run_id = now_id("run")
    trace, _ctx = make_trace(state.settings.paths.traces_dir, run_id)
    artifacts = Artifacts.for_run(
        run_id=run_id,
        base_screenshots=state.settings.paths.screenshots_dir,
        base_html_snaps=state.settings.paths.html_snaps_dir,
        per_run_subdir=state.settings.paths.per_run_subdir,
    )
    rt = Runtime(
        driver=state.driver,
        settings=state.settings,
        artifacts=artifacts,
        trace=trace,
        run_id=run_id,
        var_store=state.vars,
        repairer=make_default_repairer(state.ai, trace),
        on_replan=None,
    )
    return rt, trace, artifacts, run_id


def _screenshot_png(driver) -> tuple[bytes, str]:
    """
    Безфокусный скрин через CDP (Page.captureScreenshot).
    Возвращает (png_bytes, source='cdp'|'driver').
    """
    try:
        if hasattr(driver, "execute_cdp_cmd"):
            res = driver.execute_cdp_cmd("Page.captureScreenshot", {"format": "png"})
            if isinstance(res, dict) and res.get("data"):
                return base64.b64decode(res["data"]), "cdp"
    except Exception:
        pass
    return driver.get_screenshot_as_png(), "driver"


def _update_shot_cache(state: AppState, data: bytes, src: str) -> None:
    state.last_shot_png = data
    state.last_shot_src = src
    state.last_shot_ts = time.time()
    state.etag = str(int(state.last_shot_ts * 1000))


def _start_shot_worker(state: AppState, interval_sec: float = 1.0) -> None:
    """Фоновый воркер, который периодически обновляет кадр и метрики без блокировок UI."""
    def _loop() -> None:
        while not state.worker_stop.is_set():
            t0 = time.time()
            acquired = state.lock.acquire(timeout=0.1)
            try:
                if acquired:
                    try:
                        data, src = _screenshot_png(state.driver)
                        _update_shot_cache(state, data, src)
                    except Exception:
                        pass
                    # viewport metrics
                    try:
                        m = state.driver.execute_script(
                            "return {w:window.innerWidth||0,h:window.innerHeight||0,dpr:window.devicePixelRatio||1,"
                            "sx:window.scrollX||0,sy:window.scrollY||0};"
                        )
                        if isinstance(m, dict):
                            state.last_vp_w = int(m.get("w", 0) or 0)
                            state.last_vp_h = int(m.get("h", 0) or 0)
                            state.last_dpr = float(m.get("dpr", 1) or 1)
                            state.last_scroll_x = float(m.get("sx", 0) or 0.0)
                            state.last_scroll_y = float(m.get("sy", 0) or 0.0)
                    except Exception:
                        pass
            finally:
                if acquired:
                    try:
                        state.lock.release()
                    except Exception:
                        pass
            dt = time.time() - t0
            delay = max(0.15, interval_sec - dt)
            state.worker_stop.wait(delay)

    th = threading.Thread(target=_loop, name="shot-worker", daemon=True)
    state.worker_thread = th
    th.start()


# --------- StreamTrace: обёртка, прокидывающая события в SSE очередь ----------

class _StreamTrace:
    """Оборачивает JsonlTrace и дублирует все .write(...) в очередь событий."""
    def __init__(self, base: JsonlTrace, q: "queue.Queue[dict]"):
        self._base = base
        self._q = q

    def write(self, data: Dict[str, Any]) -> None:
        # Не допускаем не-JSON сериализуемых полей
        try:
            _ = json.dumps(data, ensure_ascii=False)
        except Exception:
            # мягкая деградация: обернём в строку
            data = {"event": "trace_error", "payload": str(data)[:500]}
        self._base.write(data)
        try:
            self._q.put_nowait(data)
        except Exception:
            pass

    def __getattr__(self, name: str) -> Any:
        return getattr(self._base, name)


# ------------------------------ Flask приложение ------------------------------

def create_app(
    profile: str,
    start_url: str,
    *,
    headless: Optional[bool] = None,
    config_path: Optional[Path] = None,
    logging_path: Optional[Path] = None,
) -> Flask:
    """
    Flask-приложение с «живым» скриншотом, стримингом событий и двумя режимами выполнения:
      - legacy: единый план → run()
      - pe    : инкрементальный Plan-and-Execute (подцели/чанки плана)
    """
    global _state

    s = load_settings(config_path=config_path, logging_yaml=logging_path)
    if headless is not None:
        s.browser.headless_default = bool(headless)

    _ensure_env_keys(s)

    # стартуем браузер через AdsPower
    drv = start_adspower(
        profile=profile,
        headless=s.browser.headless_default,
        api_base=s.browser.adsp_api_base,
        token=s.adsp_api_token,
        window_size="1440,900",
    )

    # vars → артефакты
    vars_path = s.paths.artifacts_root / "vars.json"
    vstore = VarStore(vars_path)

    # LLM
    ai = GeminiClient(
        model=s.llm.model,
        temperature=s.llm.temperature,
        retries=s.llm.retries,
        fallback_model=s.llm.fallback_model,
    )

    # откроем начальную страницу
    try:
        drv.get(start_url)
    except Exception:
        pass

    _state = AppState(settings=s, driver=drv, ai=ai, vars=vstore, lock=threading.RLock())

    # первичный кадр (best-effort)
    try:
        data, src = _screenshot_png(drv)
        _update_shot_cache(_state, data, src)
        # первичные метрики
        try:
            m = drv.execute_script(
                "return {w:window.innerWidth||0,h:window.innerHeight||0,dpr:window.devicePixelRatio||1,"
                "sx:window.scrollX||0,sy:window.scrollY||0};"
            )
            if isinstance(m, dict):
                _state.last_vp_w = int(m.get("w", 0) or 0)
                _state.last_vp_h = int(m.get("h", 0) or 0)
                _state.last_dpr = float(m.get("dpr", 1) or 1)
                _state.last_scroll_x = float(m.get("sx", 0) or 0.0)
                _state.last_scroll_y = float(m.get("sy", 0) or 0.0)
        except Exception:
            pass
    except Exception:
        pass

    # фоновый воркер
    _start_shot_worker(_state, interval_sec=1.0)
    app = Flask(__name__)
    app.config['settings'] = s

    # --- Общие страницы/модули ---
    init_auth(app, s)
    init_profile(app, s)
    init_create_companies(app, s)
    init_list_companies(app, s)
    init_company(app, s)
    init_gads_sync(app, s)


    # --- 🔹 ДОБАВЛЕНО: инициализация модуля аккаунтов --------------------
    # Используем единые пути/БД/диспетчер из campaigns.py,
    # чтобы аккаунты писались в ту же SQLite (campaigns.db).
    paths = _resolve_campaign_paths(s)
    campaign_db = CampaignDB(paths.db_file)
    task_manager = TaskManager(s, campaign_db, paths)
    init_account_module(app, s, campaign_db, task_manager)  # регистрирует /accounts, /accounts/new и т.п.
    init_accounts_list(app, s, campaign_db)

    @app.route("/", methods=["GET"])
    def home_root() -> Response:
        return make_response(HOME_HTML)

    @app.route("/console", methods=["GET"])
    def console() -> Response:
        return make_response(INDEX_HTML)

    @app.route("/api/console_status", methods=["GET"])
    def console_status() -> Response:
        """
        Статус драйвера/шота для консоли (уникальный путь, чтобы не конфликтовать с /api/status из create_companies).
        """
        st: Dict[str, Any] = {
            "url": "",
            "title": "",
            "busy": False,
            "etag": "0",
            "shot_src": "none",
            "vp_w": 0,
            "vp_h": 0,
            "dpr": 1.0,
            "scrollX": 0.0,
            "scrollY": 0.0,
        }
        try:
            with _state.lock:  # type: ignore[union-attr]
                st["url"] = _state.driver.current_url or ""  # type: ignore[union-attr]
                st["title"] = _state.driver.title or ""      # type: ignore[union-attr]
            st["busy"] = bool(_state.busy)  # type: ignore[union-attr]
            st["etag"] = _state.etag        # type: ignore[union-attr]
            st["shot_src"] = _state.last_shot_src  # type: ignore[union-attr]
            st["vp_w"] = int(_state.last_vp_w)     # type: ignore[union-attr]
            st["vp_h"] = int(_state.last_vp_h)     # type: ignore[union-attr]
            st["dpr"] = float(_state.last_dpr)     # type: ignore[union-attr]
            st["scrollX"] = float(_state.last_scroll_x)  # type: ignore[union-attr]
            st["scrollY"] = float(_state.last_scroll_y)  # type: ignore[union-attr]
        except Exception:
            pass
        return jsonify(st)

    @app.route("/api/screenshot", methods=["GET"])
    def screenshot() -> Response:
        """Отдаём последний кэшированный PNG. Мгновенно и не блокируем драйвер."""
        try:
            data: Optional[bytes] = None
            src = "cache"
            etag = "0"
            try:
                data = _state.last_shot_png  # type: ignore[union-attr]
                etag = _state.etag           # type: ignore[union-attr]
                if data is None:
                    if _state.lock.acquire(timeout=0.1):  # type: ignore[union-attr]
                        try:
                            data, src = _screenshot_png(_state.driver)  # type: ignore[union-attr]
                            _update_shot_cache(_state, data, src)       # type: ignore[union-attr]
                            etag = _state.etag                          # type: ignore[union-attr]
                        finally:
                            _state.lock.release()  # type: ignore[union-attr]
            except Exception:
                pass

            if not data:
                return make_response("no screenshot", 503)

            resp = make_response(data)
            resp.headers["Content-Type"] = "image/png"
            resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            resp.headers["Pragma"] = "no-cache"
            resp.headers["Expires"] = "0"
            resp.headers["ETag"] = etag
            resp.headers["X-From"] = src  # 'cache' | 'cdp' | 'driver'
            return resp
        except Exception as e:
            return make_response(f"no screenshot: {e}", 500)

    @app.route("/api/scroll", methods=["POST"])
    def scroll() -> Response:
        """Скролл страницы из Web UI. Если драйвер занят — 409 (тихо)."""
        body = request.get_json(silent=True) or {}
        dy = int(body.get("dy", 0))
        if dy == 0:
            return jsonify({"ok": True, "skipped": True})
        acquired = _state.lock.acquire(timeout=0.15)  # type: ignore[union-attr]
        if not acquired or _state.busy:  # type: ignore[union-attr]
            return jsonify({"ok": False, "busy": True}), 409
        try:
            _state.driver.execute_script(  # type: ignore[union-attr]
                "try{window.scrollBy({top: arguments[0], behavior:'smooth'});}catch(e){window.scrollBy(0, arguments[0]);}", dy
            )
            try:
                data, src = _screenshot_png(_state.driver)  # type: ignore[union-attr]
                _update_shot_cache(_state, data, src)       # type: ignore[union-attr]
            except Exception:
                pass
            return jsonify({"ok": True})
        finally:
            try:
                _state.lock.release()  # type: ignore[union-attr]
            except Exception:
                pass

    @app.route("/api/plan", methods=["POST"])
    def plan_only() -> Response:
        """Быстрый превью плана без запуска рантайма."""
        body = request.get_json(silent=True) or {}
        task = str(body.get("task") or "").strip()
        if not task:
            return jsonify({"ok": False, "error": "empty task"}), 400
        try:
            with _state.lock:  # type: ignore[union-attr]
                rt, trace, _art, _run_id = _new_runtime(_state)  # type: ignore[arg-type]
                html_view = rt.guards.dom_snapshot()
                known_vars = _state.vars.vars
                plan: List[Dict[str, Any]] = _state.ai.plan_full(html_view, task, [], known_vars)  # type: ignore[union-attr]
                try:
                    cur = (_state.driver.current_url or "")  # type: ignore[union-attr]
                except Exception:
                    cur = ""
                plan_norm = _normalize_plan_steps(plan if isinstance(plan, list) else [], cur)
                trace.write({"event": "plan_preview", "steps": plan_norm})
                return jsonify({"ok": True, "plan": plan_norm})
        except Exception as e:
            return jsonify({"ok": False, "error": repr(e)}), 500

    # -------------------- Locate endpoint: координаты элемента -----------------
    @app.route("/api/locate", methods=["POST"])
    def locate() -> Response:
        """
        Возвращает геометрию элемента по селектору (поддержка: css | xpath=... | //... | text=... | aria=... | role=...).
        Формат: {rect:{left,top,width,height,centerX,centerY}, viewport:{w,h,dpr,sx,sy}}
        """
        body = request.get_json(silent=True) or {}
        sel_raw = body.get("selector")
        if not sel_raw:
            return jsonify({"ok": False, "error": "empty selector"}), 400

        selector = _selector_to_str(sel_raw)

        # ВАЖНО: не блокируем по busy; пытаемся быстро взять лок, иначе 409 (фронт ретраит)
        acquired = _state.lock.acquire(timeout=0.12)  # type: ignore[union-attr]
        if not acquired:
            return jsonify({"ok": False, "busy": True}), 409
        try:
            js = r"""
                var sel = arguments[0] || '';
                function escCss(s){ try{ if(window.CSS && CSS.escape) return CSS.escape(s); }catch(e){} return String(s).replace(/([#.;?+*^$\[\]\\(){}|\-])/g,'\\$1'); }
                function byXpath(xp){ try{ return document.evaluate(xp, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue; }catch(e){ return null; } }
                function byText(t){
                  t = String(t);
                  var nodes = document.querySelectorAll('body *');
                  var best=null, bl=1e12;
                  for(var i=0;i<nodes.length;i++){
                    var el = nodes[i]; var txt=(el.innerText||'').trim(); if(!txt) continue;
                    if(txt===t){ return el; }
                    if(txt.indexOf(t)>=0 && txt.length<bl){ best=el; bl=txt.length; }
                  }
                  return best;
                }
                function byRole(r, name){
                  r = String(r||'').toLowerCase();
                  var all = document.querySelectorAll('[role],button,input,select,textarea,a');
                  var cand=null, bl=1e18;
                  for(var i=0;i<all.length;i++){
                    var el = all[i];
                    var role = (el.getAttribute('role')||'').toLowerCase();
                    var tag = el.tagName.toLowerCase();
                    var match = (role===r) || (r==='button' && (tag==='button' || (tag==='a' && el.getAttribute('href'))));
                    if(!match) continue;
                    var label = (el.getAttribute('aria-label')||'') + ' ' + (el.getAttribute('aria-labelledby')||'') + ' ' + (el.innerText||'');
                    label = label.trim().toLowerCase();
                    if(name){ var nm = String(name).toLowerCase(); if(label.indexOf(nm)===-1) continue; }
                    var rect = el.getBoundingClientRect(); if(rect.width*rect.height===0) continue;
                    var area = rect.width*rect.height; if(area<bl){ cand=el; bl=area; }
                  }
                  return cand;
                }
                var el=null;
                if(sel.startsWith('xpath=')){ el = byXpath(sel.slice(6)); }
                else if(sel.startsWith('//') || sel.startsWith('(.') || sel.startsWith('(/')){ el = byXpath(sel); }
                else if(sel.startsWith('text=')){ el = byText(sel.slice(5)); }
                else if(sel.startsWith('aria=')){ var nm=sel.slice(5); el = document.querySelector('[aria-label="'+escCss(nm)+'"]') || byText(nm); }
                else if(sel.startsWith('role=')){
                  var rest=sel.slice(5);
                  var m = rest.match(/^([\w-]+)(\[name="([\s\S]*?)"\])?$/);
                  if(m){ el = byRole(m[1], m[3] || null); }
                }
                else { try { el = document.querySelector(sel); } catch(e) { el = null; } }
                if(!el){ return null; }
                try{ el.scrollIntoView({block:'center', inline:'center'}); }catch(e){}
                var r = el.getBoundingClientRect();
                var vp = {w: window.innerWidth||0, h: window.innerHeight||0, dpr: window.devicePixelRatio||1, sx: window.scrollX||0, sy: window.scrollY||0};
                return { rect: {left:r.left, top:r.top, width:r.width, height:r.height, centerX:r.left+r.width/2, centerY:r.top+r.height/2}, viewport: vp };
                """
            res = _state.driver.execute_script(js, selector)  # type: ignore[union-attr]
            if not res:
                return jsonify({"ok": True, "found": False})
            return jsonify({"ok": True, "found": True, "rect": res.get("rect"), "viewport": res.get("viewport")})
        finally:
            try:
                _state.lock.release()  # type: ignore[union-attr]
            except Exception:
                pass

    # ----------------------- SSE: запуск (legacy / pe) ------------------------
    @app.route("/api/run_stream", methods=["GET"])
    def run_stream() -> Response:
        task = (request.args.get("task") or "").strip()
        mode = (request.args.get("mode") or "pe").strip().lower()
        if not task:
            return Response('data: {"event":"error","error":"empty task"}\n\n', mimetype="text/event-stream", status=400)

        def _yield(data: Dict[str, Any]) -> str:
            # единая точка сериализации (на случай не-ASCII)
            try:
                return "data: " + json.dumps(data, ensure_ascii=False) + "\n\n"
            except Exception:
                return "data: " + json.dumps({"event": "error", "error": "serialization_failed"}) + "\n\n"

        def generate_legacy() -> Any:
            """Старый режим: единый план → run(). Оставлен без изменений."""
            with _state.lock:  # type: ignore[union-attr]
                rt, trace, artifacts, run_id = _new_runtime(_state)  # type: ignore[arg-type]
                q: "queue.Queue[dict]" = queue.Queue(maxsize=1000)
                strace = _StreamTrace(trace, q)
                rt.trace = strace

                html_view = rt.guards.dom_snapshot()
                known_vars = _state.vars.vars
                plan: List[Dict[str, Any]] = _state.ai.plan_full(html_view, task, [], known_vars)  # type: ignore[union-attr]
                try:
                    cur = (_state.driver.current_url or "")  # type: ignore[union-attr]
                except Exception:
                    cur = ""
                plan_norm = _normalize_plan_steps(plan if isinstance(plan, list) else [], cur)
                strace.write({"event": "plan_full", "steps": plan})
                strace.write({"event": "plan_normalized", "steps": plan_norm})

            # стартовая пачка чипов
            yield _yield({"event": "start", "run_id": run_id, "plan": plan_norm})

            # задний поток
            def _runner() -> None:
                _state.busy = True  # type: ignore[union-attr]
                _state.busy_since = time.time()  # type: ignore[union-attr]
                try:
                    rt.set_plan(plan_norm, task)
                    res = rt.run()
                    if _state.lock.acquire(timeout=0.5):  # type: ignore[union-attr]
                        try:
                            dom_path = str(save_html_snapshot(rt.guards.dom_snapshot(), artifacts))
                            shot_path = str(take_screenshot(_state.driver, artifacts, "final"))  # type: ignore[union-attr]
                            strace.write({
                                "event": "run_done",
                                "stats": res.stats.__dict__,
                                "done_steps": res.done_steps,
                                "planned_total": res.planned_total,
                                "artifacts": {"dom": dom_path, "screenshot": shot_path},
                                "run_id": run_id,
                            })
                            try:
                                data, src = _screenshot_png(_state.driver)  # type: ignore[union-attr]
                                _update_shot_cache(_state, data, src)  # type: ignore[union-attr]
                            except Exception:
                                pass
                        finally:
                            try:
                                _state.lock.release()  # type: ignore[union-attr]
                            except Exception:
                                pass
                except Exception as e:
                    strace.write({"event": "run_error", "error": repr(e)})
                finally:
                    _state.busy = False  # type: ignore[union-attr]
                    _state.busy_since = 0.0  # type: ignore[union-attr]
                    try:
                        q.put_nowait({"event": "__end__"})
                    except Exception:
                        pass

            threading.Thread(target=_runner, name=f"run-legacy-{run_id}", daemon=True).start()

            # основной цикл отдачи событий
            last_ping = time.time()
            while True:
                try:
                    ev = q.get(timeout=0.6)
                except queue.Empty:
                    if time.time() - last_ping > 1.0:
                        yield _yield({"event": "ping", "etag": _state.etag})
                        last_ping = time.time()
                    continue
                if ev.get("event") == "__end__":
                    yield _yield({"event": "end"})
                    break
                yield _yield(ev)

        def generate_pe() -> Any:
            """
            Инкрементальный режим Plan-and-Execute:
              - outline подцелей,
              - для каждой подцели получить план шагов,
              - для первой подцели отправить 'start', для остальных — 'plan_chunk' (append),
              - запустить run() в отдельном потоке и проксировать его события (step_result/ok/fail/run_done),
              - (опционально) один раунд verify_or_adjust с fix_steps (также чанк).
            """
            with _state.lock:  # type: ignore[union-attr]
                rt, trace, _art, run_id = _new_runtime(_state)  # type: ignore[arg-type]
                q: "queue.Queue[dict]" = queue.Queue(maxsize=1000)
                strace = _StreamTrace(trace, q)
                rt.trace = strace

            # --- подгружаем outline; если нет — fallback на единый план
            try:
                outline = _state.ai.plan_outline(task)  # type: ignore[union-attr]
            except Exception as e:
                outline = {"subgoals": []}
                strace.write({"event": "llm_error", "where": "plan_outline", "err": repr(e)})
            subgoals = outline.get("subgoals", []) if isinstance(outline, dict) else []
            strace.write({"event": "outline", "count": len(subgoals), "subgoals": (subgoals or [])[:6]})

            if not subgoals:
                # fallback → один план
                html_view = rt.guards.dom_snapshot()
                known_vars = _state.vars.vars
                plan = []
                try:
                    plan = _state.ai.plan_full(html_view, task, [], known_vars)  # type: ignore[union-attr]
                except Exception as e:
                    strace.write({"event": "llm_error", "where": "plan_full", "err": repr(e)})
                try:
                    cur = _state.driver.current_url  # type: ignore[union-attr]
                except Exception:
                    cur = ""
                plan_norm = _normalize_plan_steps(plan if isinstance(plan, list) else [], cur)
                strace.write({"event": "plan_full", "steps": plan})
                strace.write({"event": "plan_normalized", "steps": plan_norm})
                # стартуем legacy-образно
                yield _yield({"event": "start", "run_id": run_id, "plan": plan_norm})

                def _runner_one():
                    _state.busy = True  # type: ignore[union-attr]
                    _state.busy_since = time.time()  # type: ignore[union-attr]
                    try:
                        rt.set_plan(plan_norm, task)
                        _ = rt.run()
                    finally:
                        _state.busy = False  # type: ignore[union-attr]
                        _state.busy_since = 0.0  # type: ignore[union-attr]
                        try: q.put_nowait({"event": "__end__"})
                        except Exception: pass

                threading.Thread(target=_runner_one, name=f"run-pe-fallback-{run_id}", daemon=True).start()

                last_ping = time.time()
                while True:
                    try:
                        ev = q.get(timeout=0.6)
                    except queue.Empty:
                        if time.time() - last_ping > 1.0:
                            yield _yield({"event": "ping", "etag": _state.etag})
                            last_ping = time.time()
                        continue
                    if ev.get("event") == "__end__":
                        yield _yield({"event": "end"}); break
                    yield _yield(ev)
                return

            # --- основной цикл по подцелям
            first_chunk = True
            history_done: List[Dict[str, Any]] = []
            for idx, sg in enumerate(subgoals, start=1):
                # 1) план шагов подцели
                with _state.lock:  # безопасно берём актуальный DOM/URL
                    html_view = rt.guards.dom_snapshot()
                    try:
                        cur_url = _state.driver.current_url  # type: ignore[union-attr]
                    except Exception:
                        cur_url = ""
                known_vars = _state.vars.vars

                plan_for_sg: List[Dict[str, Any]] = []
                try:
                    plan_for_sg = _state.ai.plan_subgoal_steps(html_view, task, sg, history_done, known_vars, max_steps=6)  # type: ignore[union-attr]
                except Exception as e:
                    strace.write({"event": "llm_error", "where": "plan_subgoal_steps", "err": repr(e), "sg": sg})
                plan_norm = _normalize_plan_steps(plan_for_sg if isinstance(plan_for_sg, list) else [], cur_url)
                strace.write({"event": "subgoal_plan", "idx": idx, "count": len(plan_norm), "title": (sg.get('title') or sg.get('goal') or f'Subgoal {idx}')})

                # выдаём чипы в UI: стартовая пачка или догрузка
                if first_chunk:
                    first_chunk = False
                    yield _yield({"event": "start", "run_id": run_id, "plan": plan_norm})
                else:
                    yield _yield({"event": "plan_chunk", "plan": plan_norm})  # append

                # 2) запускаем run() этой пачки в отдельном потоке; стримим события
                def _runner_chunk(steps: List[Dict[str, Any]], title: str) -> None:
                    _state.busy = True  # type: ignore[union-attr]
                    _state.busy_since = time.time()  # type: ignore[union-attr]
                    try:
                        rt.set_plan(steps, f"{task} :: {title}")
                        res = rt.run()
                        # накапливаем историю в замыкании (без гонок на driver)
                        history_done.extend(res.done_steps)
                    except Exception as e:
                        strace.write({"event": "run_error", "error": repr(e)})
                    finally:
                        _state.busy = False  # type: ignore[union-attr]
                        _state.busy_since = 0.0  # type: ignore[union-attr]
                        try: q.put_nowait({"event": "__sub_end__"})
                        except Exception: pass

                title = str(sg.get("title") or sg.get("goal") or f"Subgoal {idx}")
                threading.Thread(target=_runner_chunk, args=(plan_norm, title), name=f"run-pe-{run_id}-sg{idx}", daemon=True).start()

                # 3) стримим события подзадачи до её окончания
                last_ping = time.time()
                while True:
                    try:
                        ev = q.get(timeout=0.6)
                    except queue.Empty:
                        if time.time() - last_ping > 1.0:
                            yield _yield({"event": "ping", "etag": _state.etag})
                            last_ping = time.time()
                        continue
                    if ev.get("event") == "__sub_end__":
                        break
                    yield _yield(ev)

                # 4) verify_or_adjust (один лёгкий раунд)
                try:
                    vr = _state.ai.verify_or_adjust(rt.guards.dom_snapshot(), task, sg, [], known_vars)  # type: ignore[union-attr]
                except Exception as e:
                    vr = {}
                    strace.write({"event": "llm_error", "where": "verify_or_adjust", "err": repr(e), "sg": sg})

                if isinstance(vr, dict) and vr.get("status") == "retry":
                    fix = vr.get("fix_steps") or []
                    try:
                        cur_url = _state.driver.current_url  # type: ignore[union-attr]
                    except Exception:
                        cur_url = ""
                    fix_norm = _normalize_plan_steps(fix if isinstance(fix, list) else [], cur_url)
                    if fix_norm:
                        # визуально догружаем фиксы
                        yield _yield({"event": "plan_chunk", "plan": fix_norm})

                        def _runner_fix(steps: List[Dict[str, Any]]) -> None:
                            _state.busy = True  # type: ignore[union-attr]
                            _state.busy_since = time.time()  # type: ignore[union-attr]
                            try:
                                rt.set_plan(steps, f"{task} :: {title} (fix)")
                                res = rt.run()
                                history_done.extend(res.done_steps)
                            finally:
                                _state.busy = False  # type: ignore[union-attr]
                                _state.busy_since = 0.0  # type: ignore[union-attr]
                                try: q.put_nowait({"event": "__sub_fix_end__"})
                                except Exception: pass

                        threading.Thread(target=_runner_fix, args=(fix_norm,), name=f"run-pe-fix-{run_id}-sg{idx}", daemon=True).start()

                        # стрим событий фикса
                        last_ping = time.time()
                        while True:
                            try:
                                ev = q.get(timeout=0.6)
                            except queue.Empty:
                                if time.time() - last_ping > 1.0:
                                    yield _yield({"event": "ping", "etag": _state.etag})
                                    last_ping = time.time()
                                continue
                            if ev.get("event") == "__sub_fix_end__":
                                break
                            yield _yield(ev)

            # завершение
            yield _yield({"event": "end"})

        # Выбор режима
        if mode == "legacy":
            return Response(stream_with_context(generate_legacy()), mimetype="text/event-stream")
        return Response(stream_with_context(generate_pe()), mimetype="text/event-stream")

    # --------------- Старый /api/chat (оставим) ---------------
    @app.route("/api/chat", methods=["POST"])
    def chat() -> Response:
        body = request.get_json(silent=True) or {}
        task = str(body.get("task") or "").strip()
        if not task:
            return jsonify({"ok": False, "error": "empty task"}), 400

        _state.busy = True  # type: ignore[union-attr]
        _state.busy_since = time.time()  # type: ignore[union-attr]
        try:
            with _state.lock:  # type: ignore[union-attr]
                rt, trace, artifacts, run_id = _new_runtime(_state)  # type: ignore[arg-type]

                html_view = rt.guards.dom_snapshot()
                known_vars = _state.vars.vars  # dict

                plan: List[Dict[str, Any]] = _state.ai.plan_full(html_view, task, [], known_vars)  # type: ignore[union-attr]
                try:
                    cur = (_state.driver.current_url or "")  # type: ignore[union-attr]
                except Exception:
                    cur = ""
                plan_norm = _normalize_plan_steps(plan if isinstance(plan, list) else [], cur)

                trace.write({"event": "plan_full", "steps": plan})
                trace.write({"event": "plan_normalized", "steps": plan_norm})

                rt.set_plan(plan_norm, task)
                res = rt.run()

                dom_path = str(save_html_snapshot(rt.guards.dom_snapshot(), artifacts))
                shot_path = str(take_screenshot(_state.driver, artifacts, "final"))  # type: ignore[union-attr]
                try:
                    data, src = _screenshot_png(_state.driver)  # type: ignore[union-attr]
                    _update_shot_cache(_state, data, src)       # type: ignore[union-attr]
                except Exception:
                    pass

                out = {
                    "ok": True,
                    "run_id": run_id,
                    "stats": res.stats.__dict__,
                    "done_steps": res.done_steps,
                    "planned_total": res.planned_total,
                    "replan_suggested": res.replan_suggested,
                    "plan": plan_norm,
                    "artifacts": {"dom": dom_path, "screenshot": shot_path},
                }
                return jsonify(out)
        except Exception as e:
            return jsonify({"ok": False, "error": repr(e)}), 500
        finally:
            _state.busy = False  # type: ignore[union-attr]
            _state.busy_since = 0.0  # type: ignore[union-attr]

    return app


# ------------------------------ Точка входа (CLI) -----------------------------

def _parse_args() -> Dict[str, Any]:
    import argparse
    p = argparse.ArgumentParser(description="Ads AI Agent — Web UI (Flask)")
    p.add_argument("--profile", required=True, help="AdsPower profile ID")
    p.add_argument("--url", required=True, help="Start URL (will be opened on launch)")
    p.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    p.add_argument("--port", type=int, default=5000, help="Port (default: 5000)")
    p.add_argument("--headless", action="store_true", help="Headless mode (override settings)")
    p.add_argument("--config", type=Path, help="Path to configs/config.yaml")
    p.add_argument("--logging", type=Path, help="Path to configs/logging.yaml")
    return vars(p.parse_args())


def _on_exit():
    # корректное закрытие драйвера и воркера при выходе
    try:
        if _state:
            try:
                _state.worker_stop.set()
            except Exception:
                pass
            if _state.driver:
                _state.driver.quit()
    except Exception:
        pass


def main() -> int:
    args = _parse_args()
    app = create_app(
        profile=args["profile"],
        start_url=args["url"],
        headless=args.get("headless") or False,
        config_path=args.get("config"),
        logging_path=args.get("logging"),
    )
    atexit.register(_on_exit)
    app.run(host=args["host"], port=int(args["port"]), debug=False, use_reloader=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
