# -*- coding: utf-8 -*-
"""
Google Ads: детерминированное создание кампании (Selenium через AdsPower).
FSM по экранам, устойчивые селекторы, контекст блоков, человекоподобные действия,
идемпотентные клики/ввод, анти‑застои, трейс JSONL + артефакты.

Ключевые фишки:
- Сканер UI понимает aria-labelledby / label[for] и поднимается по DOM, извлекая «человеческую» метку.
- Контекст блоков (fieldset/legend, панели, h2/h3) — в каждом контроле есть c["block"].
- Выбор плиток: гарантирую выбор «Create a campaign without guidance» и «Search» на одном экране; re‑assert после появления 2‑го блока.
- Ввод на интро‑шаге починен (business name / website URL).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ads_ai.plan.schema import StepType
from ads_ai.browser.actions import ACTIONS, ActionContext
from ads_ai.llm.gemini import GeminiClient
from ads_ai.config.settings import Settings
from ads_ai.browser.adspower import start_adspower
from ads_ai.storage.vars import VarStore
from ads_ai.tracing.trace import JsonlTrace
from ads_ai.tracing.artifacts import Artifacts

try:
    from ads_ai.browser.humanize import Humanizer  # type: ignore
except Exception:  # pragma: no cover
    Humanizer = None  # noqa: N816

logger = logging.getLogger("ads_ai.gads")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )


# ==========
# Модели
# ==========

@dataclass
class CampaignInput:
    budget: str
    site: str
    usp: str
    geo: str
    currency: str
    profile_id: Optional[str] = None


@dataclass
class CampaignCreatives:
    keywords_core: List[str]
    headlines: List[str]
    descriptions: List[str]


@dataclass
class CampaignResult:
    ok: bool
    published: bool
    name: str
    campaign_id: Optional[str]
    status: str
    account_currency: Optional[str]
    requested_currency: str
    currency_match: bool
    trace_path: Path
    artifacts_dir: Path
    errors: List[str]


class PlanExecutionError(Exception):
    pass


# ==========
# Утилиты
# ==========

def _now_id() -> str:
    return str(int(time.time()))

def _trim_list(items: List[str], n: int) -> List[str]:
    return [x.strip() for x in items if isinstance(x, str) and x.strip()][:n]

def _sanitize_budget(text: str) -> str:
    cleaned = re.sub(r"[^\d,\.]", "", text or "")
    cleaned = cleaned.replace(",", ".")
    m = re.match(r"^\d+(\.\d{1,2})?$", cleaned)
    return m.group(0) if m else re.sub(r"\D", "", cleaned)

def _parse_json_loose(text: str) -> Dict[str, Any]:
    if not text:
        return {}
    cb = re.search(r"```json\s*(\{.*?\})\s*```", text, flags=re.S)
    if cb:
        text = cb.group(1)
    try:
        return json.loads(text)
    except Exception:
        pass
    braces = re.search(r"\{.*\}", text, flags=re.S)
    if braces:
        try:
            return json.loads(braces.group(0))
        except Exception:
            return {}
    return {}

def _fingerprint(ui: Dict[str, Any]) -> str:
    """Хеш заголовка + 15 первых локаторов — для детекта смены экрана."""
    head = (ui.get("page_heading") or "").strip()
    locs: List[str] = []
    for c in (ui.get("controls") or [])[:15]:
        locs.extend((c.get("locators") or [])[:1])
    blob = head + "|" + "|".join(locs)
    return hashlib.sha1(blob.encode("utf-8", errors="ignore")).hexdigest()[:12]


# ======================
# Исполнитель шагов (обёртка над ACTIONS) + debounce
# ======================

class StepExecutor:
    def __init__(self, ctx: ActionContext, trace: JsonlTrace):
        self.ctx = ctx
        self.trace = trace
        self._counter = 0
        self._last_click: Dict[str, Any] = {"sel": None, "ts": 0.0, "fp": None}

    def _resolve_step_type(self, t: Any) -> StepType:
        if isinstance(t, StepType):
            return t
        if isinstance(t, str):
            key = t.upper()
            try:
                return StepType[key]
            except Exception:
                for m in StepType:
                    if getattr(m, "value", "").upper() == key:
                        return m
        raise KeyError(f"Unknown StepType: {t!r}")

    def run_one(self, raw: Dict[str, Any]) -> bool:
        self._counter += 1
        step = dict(raw)
        st = self._resolve_step_type(step.get("type"))
        step["type"] = st
        handler = ACTIONS.get(st)
        if not handler:
            raise PlanExecutionError(f"No handler for {st}")

        t0 = time.time()
        logger.info("[STEP %02d] %s", self._counter, self._short_repr(step))
        self.trace.write({"event": "exec_step", "idx": self._counter, "type": st.name, "step": self._redact(step)})
        ok = bool(handler(self.ctx, step))
        dt = int((time.time() - t0) * 1000)
        logger.info("[STEP %02d] result=%s (%d ms)", self._counter, "OK" if ok else "FAIL", dt)
        if dt > 3500:
            self.trace.write({"event": "slow_step", "idx": self._counter, "type": st.name, "duration_ms": dt})
            logger.warning("Slow step: %s took %d ms", st.name, dt)
        return ok

    def run_many(self, steps: List[Dict[str, Any]], stop_on_error: bool = True) -> None:
        for s in steps:
            ok = self.run_one(s)
            if not ok and stop_on_error:
                raise PlanExecutionError(f"Step failed: {s}")

    def debounce_would_skip(self, selector: str, fp: Optional[str], window_ms: int = 1500) -> bool:
        ts = time.time()
        last = self._last_click
        if selector and last.get("sel") == selector and last.get("fp") == fp and (ts - float(last.get("ts", 0))) * 1000 < window_ms:
            logger.info("Debounce: skip duplicate click on %s (same stage)", selector)
            self.trace.write({"event": "debounce_click", "selector": selector, "fp": fp})
            return True
        return False

    def mark_click(self, selector: str, fp: Optional[str]) -> None:
        self._last_click = {"sel": selector, "fp": fp, "ts": time.time()}

    @staticmethod
    def _short_repr(step: Dict[str, Any]) -> str:
        t = step.get("type")
        if t in (StepType.GOTO, "GOTO"):
            return f"GOTO {step.get('url')}"
        if t in (StepType.WAIT_URL, "WAIT_URL"):
            return f"WAIT_URL pattern={step.get('pattern')!r}"
        if t in (StepType.WAIT_VISIBLE, "WAIT_VISIBLE"):
            return f"WAIT_VISIBLE {step.get('selector')!r} timeout={step.get('timeout', '')}"
        if t in (StepType.WAIT_DOM_STABLE, "WAIT_DOM_STABLE"):
            return f"WAIT_DOM_STABLE ms={step.get('ms', '')} timeout={step.get('timeout', '')}"
        if t in (StepType.INPUT, "INPUT"):
            return f"INPUT {step.get('selector')!r} text=***"
        if t in (StepType.CLICK, "CLICK"):
            return f"CLICK {step.get('selector')!r} timeout={step.get('timeout', '')}"
        if t in (StepType.PRESS_KEY, "PRESS_KEY"):
            return f"PRESS_KEY {step.get('key')!r}"
        if t in (StepType.EVALUATE, "EVALUATE"):
            return "EVALUATE"
        if t in (StepType.EXTRACT, "EXTRACT"):
            return f"EXTRACT {step.get('selector')!r}"
        return f"{getattr(t, 'name', t)}"

    @staticmethod
    def _redact(step: Dict[str, Any]) -> Dict[str, Any]:
        safe = dict(step)
        for k in ("text", "value"):
            v = safe.get(k)
            if isinstance(v, str) and v:
                safe[k] = "***"
        return safe


# ==========================
# Основной Wizard (FSM)
# ==========================

@dataclass
class _Screen:
    heading: str
    controls: List[Dict[str, Any]]


class GoogleAdsCampaignWizard:
    def __init__(self, settings: Settings, run_dir: Optional[Path] = None):
        self.settings = settings
        self.run_id = _now_id()

        # Директории ранa
        paths = getattr(self.settings, "paths", None)
        base_runs = (Path(getattr(paths, "run_dir")) if getattr(paths, "run_dir", None)
                     else (Path(getattr(paths, "traces_dir")).parent if getattr(paths, "traces_dir", None)
                           else (Path(getattr(paths, "artifacts_root")) / "runs" if getattr(paths, "artifacts_root", None)
                                 else Path.cwd() / "artifacts" / "runs")))
        self.run_dir = base_runs / f"gads_{self.run_id}"
        self.run_dir.mkdir(parents=True, exist_ok=True)

        # Инфраструктура
        self.trace = JsonlTrace(self.run_dir / "trace.jsonl")
        try:
            self.artifacts = Artifacts.for_run(
                run_id=self.run_id,
                base_screenshots=self.settings.paths.screenshots_dir if getattr(self.settings, "paths", None) else (Path.cwd() / "artifacts" / "screenshots"),
                base_html_snaps=self.settings.paths.html_snaps_dir if getattr(self.settings, "paths", None) else (Path.cwd() / "artifacts" / "html_snaps"),
                per_run_subdir=getattr(getattr(self.settings, "paths", object()), "per_run_subdir", True),
            )
        except Exception:
            self.artifacts = Artifacts.for_run(
                run_id=self.run_id,
                base_screenshots=Path.cwd() / "artifacts" / "screenshots",
                base_html_snaps=Path.cwd() / "artifacts" / "html_snaps",
                per_run_subdir=True,
            )
        self.vars = VarStore(self.run_dir / "vars.json")

        # LLM только для креативов (1 вызов)
        try:
            api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY") or getattr(self.settings, "gemini_api_key", None)
            if api_key:
                os.environ.setdefault("GOOGLE_API_KEY", str(api_key))
        except Exception:
            pass
        llm_cfg = getattr(self.settings, "llm", None)
        model = getattr(llm_cfg, "model", "models/gemini-2.0-flash") if llm_cfg is not None else "models/gemini-2.0-flash"
        temperature = getattr(llm_cfg, "temperature", 0.1) if llm_cfg is not None else 0.1
        retries = getattr(llm_cfg, "retries", 1) if llm_cfg is not None else 1
        fallback = getattr(llm_cfg, "fallback_model", None) if llm_cfg is not None else None
        self.gemini = GeminiClient(model=model, temperature=float(temperature), retries=int(retries), fallback_model=fallback)

        self.driver = None
        self.humanizer = None
        if Humanizer:
            try:
                self.humanizer = Humanizer(getattr(self.settings, "humanize", None))
            except Exception:
                self.humanizer = None

        self.max_stages = 8
        self._last_fp: Optional[str] = None
        self._same_fp_in_a_row = 0

    # ---- Жизненный цикл ----

    def start(self, profile_id: Optional[str] = None) -> None:
        browser = getattr(self.settings, "browser", None)
        prof = profile_id or (getattr(browser, "profile_id", None) if browser is not None else None)
        headless = bool(getattr(browser, "headless_default", False)) if browser is not None else False
        api_base = str(getattr(browser, "adsp_api_base", "")) if browser is not None else ""
        token = getattr(self.settings, "adsp_api_token", None)
        self.driver = start_adspower(
            profile=str(prof or ""),
            headless=headless,
            api_base=api_base,
            token=token,
            window_size="1440,900",
        )
        try:
            self.driver.set_page_load_timeout(25)
            self.driver.set_script_timeout(15)
        except Exception:
            pass
        self.trace.write({"event": "driver_started", "profile_id": prof})
        logger.info("Driver started (profile=%s)", prof)

    def stop(self) -> None:
        try:
            if self.driver:
                self.driver.quit()
                logger.info("Driver stopped")
        except Exception:
            pass

    # ---- Публичный API ----

    def run(self, payload: CampaignInput) -> CampaignResult:
        assert self.driver is not None, "call .start() first"

        campaign_name = f"{payload.usp.strip()} — Search"
        self.vars.set("budget", _sanitize_budget(payload.budget))
        self.vars.set("site", payload.site.strip())
        self.vars.set("usp", payload.usp.strip())
        self.vars.set("geo", payload.geo.strip())
        self.vars.set("currency", payload.currency.strip().upper())
        self.vars.set("campaign_name", campaign_name)

        humanize_cfg = getattr(self.settings, "humanize", None)
        ctx = ActionContext(
            driver=self.driver,
            default_wait_sec=int(getattr(getattr(self.settings, "browser", object()), "default_wait_sec", 10)),
            step_timeout_sec=int(getattr(getattr(self.settings, "browser", object()), "step_timeout_sec", 25)),
            var_store=self.vars,
            humanizer=self.humanizer,
            post_wait_dom_idle_ms=int(getattr(humanize_cfg, "post_wait_dom_idle_ms", 120)) if humanize_cfg is not None else 120,
            trace=self.trace,
            artifacts=self.artifacts,
        )
        execu = StepExecutor(ctx, self.trace)

        errors: List[str] = []

        # 1) Креативы
        creatives = self._generate_creatives(payload)
        self.vars.set("keywords_core", "\n".join(creatives.keywords_core))
        self.vars.set("rsa_headlines", creatives.headlines)
        self.vars.set("rsa_descriptions", creatives.descriptions)
        logger.info("Creatives: %d keywords, %d headlines, %d descriptions",
                    len(creatives.keywords_core), len(creatives.headlines), len(creatives.descriptions))

        # 2) Валюта
        execu.run_many([
            {"type": StepType.GOTO, "url": "https://ads.google.com/aw/campaigns"},
            {"type": StepType.WAIT_URL, "pattern": "aw/campaigns", "timeout": 20},
            {"type": StepType.WAIT_DOM_STABLE, "ms": 250, "timeout": 8},
            {"type": StepType.EVALUATE,
             "script": ("const t=(document.body.innerText||'').match(/\\b(USD|EUR|RUB|UAH|KZT|GBP|TRY|PLN|CZK|GEL|AZN|AMD)\\b/);"
                        "return t ? t[0] : '';"),
             "var": "account_currency"},
        ])
        acc_curr = str(self.vars.get("account_currency") or "")
        req_curr = str(self.vars.get("currency") or "")
        currency_match = (acc_curr.upper() == req_curr.upper()) if acc_curr else False
        logger.info("Currency: account=%s requested=%s match=%s", acc_curr, req_curr, currency_match)
        self.trace.write({"event": "currency_check", "account_currency": acc_curr, "requested": req_curr, "match": currency_match})

        # 3) Мастер
        self._open_wizard(execu)

        # 4) FSM
        try:
            self._fsm_run(execu, creatives)
        except PlanExecutionError as e:
            errors.append(str(e))
            logger.error("FSM error: %s", e)
            self.trace.write({"event": "fsm_error", "error": str(e)})

        # 5) Паблиш/драфт
        finish_plan = self._plan_finish(currency_match)
        try:
            execu.run_many(finish_plan, stop_on_error=False)
        except Exception:
            pass

        # 6) Пост‑проверка
        status, cid = self._post_check(execu, campaign_name)

        result = CampaignResult(
            ok=(status != "UNKNOWN"),
            published=currency_match,
            name=campaign_name,
            campaign_id=cid,
            status=status or ("DRAFT" if not currency_match else "UNKNOWN"),
            account_currency=acc_curr or None,
            requested_currency=req_curr,
            currency_match=currency_match,
            trace_path=(self.trace.path or (self.run_dir / "trace.jsonl")),
            artifacts_dir=(getattr(self.settings, "paths", None).artifacts_root if getattr(self.settings, "paths", None) else (Path.cwd() / "artifacts")),
            errors=errors,
        )
        logger.info("Done: status=%s id=%s published=%s errors=%d", result.status, result.campaign_id, result.published, len(errors))
        return result

    # ---- Креативы ----

    def _generate_creatives(self, inp: CampaignInput) -> CampaignCreatives:
        prompt = {
            "task": "Generate Google Ads Search assets (keywords + RSA) in JSON only.",
            "site": inp.site, "usp": inp.usp, "geo": inp.geo,
            "language_hint": "ru" if re.search(r"[А-Яа-яЁё]", inp.usp) else "en",
            "constraints": {
                "rsa_headline_limit": 30, "rsa_description_limit": 90,
                "min_headlines": 10, "max_headlines": 12,
                "min_descriptions": 2, "max_descriptions": 3,
                "keywords_min": 8, "keywords_max": 24
            },
            "output_schema": {"keywords": ["..."], "headlines": ["..."], "descriptions": ["..."]}
        }
        try:
            raw = self.gemini.generate_json(json.dumps(prompt, ensure_ascii=False))
            data = raw if isinstance(raw, dict) else _parse_json_loose(str(raw))
        except Exception:
            data = {}
        kws = _trim_list(list((data or {}).get("keywords", [])), 24)
        hls = _trim_list(list((data or {}).get("headlines", [])), 12)
        dsc = _trim_list(list((data or {}).get("descriptions", [])), 3)

        def _clip(s: str, lim: int) -> str: return s[:lim]
        if not hls:
            hls = _trim_list([inp.usp, "Официальный сайт", "Быстрый запуск", "Гарантия", "Скидки сегодня"], 10)
        if not dsc:
            dsc = _trim_list([
                "Оставьте заявку на сайте — быстрый старт рекламы.",
                "Прозрачный бюджет и отчёты. Эффективность с первого дня."
            ], 3)
        hls = [_clip(s, 30) for s in hls]
        dsc = [_clip(s, 90) for s in dsc]
        if not kws:
            domain = re.sub(r"^https?://", "", inp.site).split("/")[0]
            kws = _trim_list([inp.usp, f"купить {domain}", f"{domain} цена", f"{domain} отзывы"], 10)
        return CampaignCreatives(keywords_core=kws, headlines=hls, descriptions=dsc)

    # ---- Открытие мастера ----

    def _open_wizard(self, execu: StepExecutor) -> None:
        candidates = [
            "https://ads.google.com/aw/campaigns/new/business",
            "https://ads.google.com/aw/campaigns/new",
        ]
        for url in candidates:
            try:
                execu.run_many([
                    {"type": StepType.GOTO, "url": url},
                    {"type": StepType.WAIT_URL, "pattern": "campaigns/new", "timeout": 20},
                    {"type": StepType.WAIT_DOM_STABLE, "ms": 250, "timeout": 8},
                ], stop_on_error=False)
                self._dismiss_popups_soft(execu)
                ui = self._scan_ui()
                if self._is_business_intro(ui) or self._is_general_like(ui):
                    logger.info("Wizard opened via %s", url)
                    self.trace.write({"event": "wizard_opened", "url": url, "page_heading": ui.get("page_heading", "")})
                    return
            except Exception:
                continue
        logger.info("Deeplink fallback by clicks")
        self._click_any(execu, [
            'text="New campaign"', 'text="New Campaign"', 'role=button["New campaign"]',
            'text="Создать кампанию"', 'text="Новая кампания"', 'role=button["Создать кампанию"]',
        ], cur_fp=None)
        self._click_any(execu, ['text="Search"', 'text="Поисковая"', 'text="Поиск"'], cur_fp=None)
        self._click_any(execu, ['text="Continue"', 'text="Продолжить"', 'text="Далее"'], cur_fp=None)
        execu.run_one({"type": StepType.WAIT_DOM_STABLE, "ms": 250, "timeout": 8})

    # ---- FSM ----

    def _fsm_run(self, execu: StepExecutor, cr: CampaignCreatives) -> None:
        for stage in range(1, self.max_stages + 1):
            ui = self._scan_ui()
            fp = _fingerprint(ui)
            url = self.driver.current_url if self.driver else ""
            heading = ui.get("page_heading") or ""
            top_controls = [(c.get("tag"), (c.get("label") or "") or (c.get("text") or ""), (c.get("locators") or [None])[0], c.get("block")) for c in (ui.get("controls") or [])[:6]]
            logger.info("[STAGE %d] url=%s | heading=%s | ctrls=%d | fp=%s", stage, url, heading, len(ui.get("controls") or []), fp)
            self.trace.write({"event": "stage_begin", "stage": stage, "url": url, "heading": heading, "fp": fp, "top_controls": top_controls})

            # защита от зацикливания
            if self._last_fp == fp:
                self._same_fp_in_a_row += 1
            else:
                self._same_fp_in_a_row = 0
            self._last_fp = fp
            if self._same_fp_in_a_row >= 2:
                try:
                    snap = self.artifacts.save_screenshot(self.driver, f"stuck_stage_{stage}")
                    html = self.artifacts.save_html(self.driver, f"stuck_stage_{stage}")
                    self.trace.write({"event": "stuck_snapshot", "screenshot": str(snap), "html": str(html)})
                    logger.warning("Stuck on the same stage 3x: screenshot=%s html=%s", snap, html)
                except Exception:
                    pass

            # Интро + выбор «A website» в радиогруппе (идемпотентно)
            if self._is_business_intro(ui):
                # выберем A website, если нужно
                self._ensure_card_by_label(execu, ui, "A website")
                filled = self._handle_business_intro(execu, ui)
                self.trace.write({"event": "stage_filled", "stage": stage, "filled": filled})
                if self._click_next(execu, cur_fp=fp):
                    continue

            # Спец-случай: экран из двух блоков (цели + тип кампании)
            if self._has_goal_and_type_blocks(ui):
                self._ensure_goal_and_type(execu, ui)  # безотказный выбор двух плиток
                if self._click_next(execu, cur_fp=fp):
                    continue

            # Аккуратный выбор очевидных плиток (идемпотентно)
            self._ensure_preferred_cards(execu, ui)

            # Review/обзор?
            if re.search(r"\b(review|обзор)\b", (heading or "").lower()):
                logger.info("Reached review stage — finishing")
                break

            # Общие поля
            filled: List[Tuple[str, str]] = []
            if self._is_general_like(ui):
                if self._fill_campaign_name(execu, ui, self.vars.get("campaign_name") or ""):
                    filled.append(("campaign_name", "ok"))
                if self._fill_budget(execu, ui, self.vars.get("budget") or ""):
                    filled.append(("budget", "ok"))

            if self._fill_final_url(execu, ui, self.vars.get("site") or ""):
                filled.append(("final_url", "ok"))
            if self._fill_keywords(execu, ui, self.vars.get("keywords_core") or ""):
                filled.append(("keywords", "ok"))
            if self._fill_headlines(execu, ui, cr.headlines):
                filled.append(("headlines", "ok"))
            if self._fill_descriptions(execu, ui, cr.descriptions):
                filled.append(("descriptions", "ok"))

            self.trace.write({"event": "stage_filled", "stage": stage, "filled": filled})

            self._click_next(execu, cur_fp=fp)
            execu.run_one({"type": StepType.WAIT_DOM_STABLE, "ms": 250, "timeout": 6})

    # ---- Определители экранов/блоков ----

    @staticmethod
    def _is_business_intro(ui: Dict[str, Any]) -> bool:
        h = (ui.get("page_heading") or "").strip().lower()
        blob = " ".join((((c.get("label") or c.get("text") or "") + " " + (c.get("block") or ""))).lower() for c in (ui.get("controls") or []))
        return (
            "tell us about your business" in h
            or "what's your business name" in blob
            or "enter a web page url" in blob
            or "where should people go" in blob
            or "введите адрес веб-страницы" in blob
            or "название вашего бизнеса" in blob
        )

    @staticmethod
    def _is_general_like(ui: Dict[str, Any]) -> bool:
        h = (ui.get("page_heading") or "").lower()
        if any(k in h for k in ("general", "settings", "campaign", "настрой", "общие")):
            return True
        text_blob = " ".join([(c.get("label") or "") + " " + (c.get("text") or "") + " " + (c.get("block") or "") for c in (ui.get("controls") or [])]).lower()
        return any(k in text_blob for k in ("website", "ваш сайт", "final url", "campaign name", "название кампании", "budget", "бюджет"))

    @staticmethod
    def _has_goal_and_type_blocks(ui: Dict[str, Any]) -> bool:
        blob = " ".join(((c.get("block") or "") + " " + (c.get("label") or "") + " " + (c.get("text") or "")).lower() for c in (ui.get("controls") or []))
        return ("goal" in blob or "кампанию без подсказок" in blob or "make this campaign successful" in blob) and \
               ("campaign type" in blob or "тип кампании" in blob)

    # ---- Intro: Business name + URL ----

    def _handle_business_intro(self, execu: StepExecutor, ui: Dict[str, Any]) -> List[Tuple[str, str]]:
        site = self.vars.get("site") or ""
        biz_name = (self.vars.get("campaign_name") or "").split("—")[0].strip() or (self.vars.get("usp") or "")
        done: List[Tuple[str, str]] = []

        # business name
        bn_locs_ui = self._locators(ui, want=("input",), labels=("What's your business name", "Enter your business name", "Название вашего бизнеса"))
        bn_locs_fb = [
            'css=input[aria-label*="your business name"]',
            'css=input[aria-label*="business name"]',
            'css=input[aria-labelledby]',  # общий fallback
        ]
        if self._input_first_idempotent(execu, bn_locs_ui or bn_locs_fb, biz_name, "business_name", timeout_visible=1.2):
            done.append(("business_name", "ok"))
        else:
            # JS‑фоллбек по тексту контейнера/label
            ok_bn = execu.run_one({
                "type": StepType.EVALUATE,
                "script": self._js_fill_input_by_label(),
                "args": [biz_name, ["what's your business name", "enter your business name", "название вашего бизнеса"]],
                "timeout": 6
            })
            if ok_bn:
                done.append(("business_name", "js_fallback"))

        # website url
        url_locs_ui = self._locators(ui, want=("input",), labels=("Enter a web page URL", "Введите адрес веб-страницы", "web page url"))
        url_locs_fb = [
            'css=input[aria-label*="web page url"]',
            'css=input[type="url"]',
            'css=input[aria-labelledby]',  # общий fallback
        ]
        if self._input_first_idempotent(execu, url_locs_ui or url_locs_fb, site, "website_url", timeout_visible=1.2):
            done.append(("website_url", "ok"))
        else:
            ok = execu.run_one({
                "type": StepType.EVALUATE,
                "script": self._js_fill_input_by_label(),
                "args": [site, ["enter a web page url", "введите адрес веб-страницы", "url"]],
                "timeout": 6
            })
            if ok:
                done.append(("website_url", "js_fallback"))

        logger.info("Intro filled: %s", done)
        return done

    # ---- Общие заполнители (с приоритом ui‑map, затем быстрый фоллбек) ----

    def _fill_campaign_name(self, execu: StepExecutor, ui: Dict[str, Any], name: str) -> bool:
        locs_ui = self._locators(ui, want=("input",), labels=("Campaign name", "Название кампании"))
        locs_fb = ['css=input[aria-label*="campaign name"]', 'css=input[aria-label*="название кампании"]', 'css=input[aria-labelledby]']
        ok = self._input_first_idempotent(execu, locs_ui or locs_fb, name, "campaign_name", timeout_visible=1.5)
        if not ok:
            return execu.run_one({"type": StepType.EVALUATE, "script": self._js_fill_input_by_label(),
                                  "args": [name, ["campaign name", "название кампании"]], "timeout": 6})
        return ok

    def _fill_budget(self, execu: StepExecutor, ui: Dict[str, Any], budget: str) -> bool:
        val = _sanitize_budget(budget)
        if not val:
            return False
        locs_ui = self._locators(ui, want=("input",), labels=("Budget", "Бюджет", "Average daily budget", "Ежедневный бюджет"))
        locs_fb = ['css=input[aria-label*="budget"]', 'css=input[aria-label*="бюджет"]', 'css=input[aria-labelledby]']
        ok = self._input_first_idempotent(execu, locs_ui or locs_fb, val, "budget", timeout_visible=1.5)
        if not ok:
            return execu.run_one({"type": StepType.EVALUATE, "script": self._js_fill_input_by_label(),
                                  "args": [val, ["budget", "бюджет"]], "timeout": 6})
        return ok

    def _fill_final_url(self, execu: StepExecutor, ui: Dict[str, Any], url: str) -> bool:
        locs_ui = self._locators(ui, want=("input",), labels=("Final URL", "Конечный URL", "Final Url", "Конечная ссылка"))
        locs_fb = ['css=input[aria-label*="final url"]', 'css=input[type="url"]', 'css=input[aria-labelledby]']
        ok = self._input_first_idempotent(execu, locs_ui or locs_fb, url, "final_url", timeout_visible=1.5)
        if not ok:
            return execu.run_one({"type": StepType.EVALUATE, "script": self._js_fill_input_by_label(),
                                  "args": [url, ["final url", "конечн"]], "timeout": 6})
        return ok

    def _fill_keywords(self, execu: StepExecutor, ui: Dict[str, Any], kws_block: str) -> bool:
        if not kws_block:
            return False
        locs_ui = self._locators(ui, want=("textarea", "input"), labels=("Keywords", "Ключевые слова", "Enter keywords"))
        locs_fb = ['css=textarea[aria-label*="keywords"]', 'css=input[aria-label*="keywords"]', 'css=textarea[aria-labelledby], css=input[aria-labelledby]']
        ok = self._input_first_idempotent(execu, locs_ui or locs_fb, kws_block, "keywords", timeout_visible=1.5)
        if not ok:
            return execu.run_one({"type": StepType.EVALUATE, "script": self._js_fill_input_by_label(),
                                  "args": [kws_block, ["keywords", "ключевые слова"]], "timeout": 6})
        return ok

    def _fill_headlines(self, execu: StepExecutor, ui: Dict[str, Any], headlines: List[str]) -> bool:
        locs = self._locators_many(ui, want=("input",), labels=("Headline", "Заголовок"))
        did = False
        for i, loc in enumerate(locs[: min(6, len(headlines))]):
            did |= self._input_exact_idempotent(execu, loc, headlines[i], f"headline[{i}]", timeout_visible=1.2)
        return did

    def _fill_descriptions(self, execu: StepExecutor, ui: Dict[str, Any], descs: List[str]) -> bool:
        locs = self._locators_many(ui, want=("textarea",), labels=("Description", "Описание"))
        did = False
        for i, loc in enumerate(locs[: min(2, len(descs))]):
            did |= self._input_exact_idempotent(execu, loc, descs[i], f"description[{i}]", timeout_visible=1.2)
        return did

    # ---- Выбор плиток/радио‑блоков (идемпотентно) ----

    def _ensure_goal_and_type(self, execu: StepExecutor, ui: Dict[str, Any]) -> None:
        """
        На экране «цель + тип кампании» гарантируем:
          1) Create a campaign without guidance (в первом блоке)
          2) Search (во втором блоке)
        """
        # 1) Цель: без подсказок (обычно уже выбрано, но перепроверим)
        self._ensure_card_by_label(execu, ui, "Create a campaign without guidance")
        # Ждём появления второго блока (типы кампании)
        t0 = time.time()
        while time.time() - t0 < 3.0:
            ui2 = self._scan_ui()
            if self._block_exists(ui2, ("Select a campaign type", "тип кампании")) or self._card_exists(ui2, "Search"):
                ui = ui2
                break
            time.sleep(0.15)
        # 2) Тип: Search
        self._ensure_card_by_label(execu, ui, "Search")
        # Re-assert цель (на случай перерендера после появления блока №2)
        self._ensure_card_by_label(execu, ui, "Create a campaign without guidance")

    @staticmethod
    def _block_exists(ui: Dict[str, Any], titles: Tuple[str, ...]) -> bool:
        want = [t.lower() for t in titles]
        for c in ui.get("controls") or []:
            b = (c.get("block") or "").lower()
            if any(w in b for w in want):
                return True
        return False

    @staticmethod
    def _card_exists(ui: Dict[str, Any], label_contains: str) -> bool:
        lab = label_contains.lower()
        for c in ui.get("controls") or []:
            if (c.get("role") or "").lower() == "tab":
                txt = ((c.get("label") or "") + " " + (c.get("text") or "")).lower()
                if lab in txt:
                    return True
        return False

    def _ensure_preferred_cards(self, execu: StepExecutor, ui: Dict[str, Any]) -> bool:
        """Мягкая попытка выбрать очевидные плитки: Website / Без подсказок / Search."""
        ok = False
        ok |= self._ensure_card_by_label(execu, ui, "A website")
        ok |= self._ensure_card_by_label(execu, ui, "Create a campaign without guidance")
        ok |= self._ensure_card_by_label(execu, ui, "Search")
        return ok

    def _ensure_card_by_label(self, execu: StepExecutor, ui: Dict[str, Any], label_contains: str) -> bool:
        """
        Находим лучшего кандидата по role=tab[...] с подписью, кликаем если не выбран.
        Не требуем уникальности — берём первый валидный role=tab.
        """
        lc = label_contains.lower()
        for c in (ui.get("controls") or []):
            if (c.get("role") or "").lower() != "tab":
                continue
            lbl = ((c.get("label") or "") + " " + (c.get("text") or "")).strip().lower()
            if lc not in lbl:
                continue
            # приоритет: role=... локатор
            locs = c.get("locators") or []
            role_loc = next((l for l in locs if isinstance(l, str) and l.startswith("role=")), None)
            css_loc = next((l for l in locs if isinstance(l, str) and l.startswith("css=")), None)
            if role_loc and self._ensure_block_selected_by_role(execu, role_loc, field_name=f"card:{label_contains}"):
                return True
            if css_loc and self._ensure_block_selected_css(execu, css_loc, field_name=f"card:{label_contains}"):
                return True
        return False

    def _ensure_block_selected_by_role(self, execu: StepExecutor, role_locator: str, field_name: str) -> bool:
        m = re.match(r'^role=([a-zA-Z0-9_-]+)\["(.+)"\]$', role_locator)
        if not m:
            return False
        role = m.group(1).lower()
        label = m.group(2).strip()

        var = f"sel_by_role_{int(time.time()*1e3)%100000}"
        js = """
        try{
          const role = arguments[0].toLowerCase();
          const label = arguments[1].toLowerCase();
          const nodes = [...document.querySelectorAll(`[role="${role}"]`)];
          const norm = s => (s||'').toLowerCase().replace(/\\s+/g,' ').trim();
          const cand = nodes.find(n=>{
            const t = norm(n.innerText);
            const a = norm(n.getAttribute('aria-label'));
            return (t && t.includes(label)) || (a && a.includes(label));
          });

          const isSelected = (n)=>{
            if(!n) return false;
            const ac=(n.getAttribute('aria-checked')||'').toLowerCase()==='true';
            const as=(n.getAttribute('aria-selected')||'').toLowerCase()==='true';
            const ap=(n.getAttribute('aria-pressed')||'').toLowerCase()==='true';
            const cls=(n.className||'').toLowerCase();
            const classOn=/(\\bselected\\b|\\bis-selected\\b|\\bactive\\b|\\bchecked\\b)/.test(cls);
            const descSel=!!n.querySelector('.item.selected,.is-selected,[aria-selected="true"],[aria-checked="true"]');
            return ac||as||ap||classOn||descSel;
          };

          if(!cand) return {ok:false, before:false, clicked:false, after:false};
          const before = isSelected(cand);
          if(!before){ cand.click(); }
          return {ok:true, before, clicked:!before, after:isSelected(cand)};
        }catch(e){
          return {ok:false, before:false, clicked:false, after:false};
        }
        """
        execu.run_one({"type": StepType.EVALUATE, "script": js, "args": [role, label], "var": var, "timeout": 5})
        res = execu.ctx.var_store.get(var) or {}
        self.trace.write({"event": "select_block_by_role", "locator": role_locator, "res": res, "field": field_name})
        return bool(res.get("ok") and (res.get("before") or res.get("after")))

    def _read_selected_flag_css(self, execu: StepExecutor, css_selector: str) -> bool:
        var = f"state_{int(time.time()*1000)%100000}"
        execu.run_one({
            "type": StepType.EVALUATE,
            "script": """
            try{
              const sel = arguments[0];
              const el = document.querySelector(sel);
              if(!el) return false;

              let root = el.closest('[role="tab"],[role="option"],[role="radio"],dynamic-component[role="tab"],.selection-item,selection-card,.item,.card,.card-wrapper') || el;

              const isSelected = (n) => {
                if(!n) return false;
                const ac = (n.getAttribute('aria-checked')||'').toLowerCase()==='true';
                const as = (n.getAttribute('aria-selected')||'').toLowerCase()==='true';
                const ap = (n.getAttribute('aria-pressed')||'').toLowerCase()==='true';
                const cls = (n.className||'').toLowerCase();
                const classOn = /(\\bselected\\b|\\bis-selected\\b|\\bactive\\b|\\bchecked\\b)/.test(cls);
                const inputChecked = n.matches('input[type=radio],input[type=checkbox]') ? !!n.checked :
                                     !!n.querySelector('input[type=radio]:checked, input[type=checkbox]:checked');
                const hasDescSel = !!n.querySelector('.item.selected, .is-selected, [aria-selected="true"], [aria-checked="true"]');
                return ac || as || ap || classOn || inputChecked || hasDescSel;
              };

              return isSelected(root) || isSelected(el) || !!(el.closest('.item.selected,.is-selected,[aria-selected="true"]'));
            }catch(e){return false;}
            """,
            "args": [css_selector[len("css="):]],
            "var": var,
            "timeout": 3
        })
        return bool(execu.ctx.var_store.get(var))

    def _ensure_block_selected_css(self, execu: StepExecutor, css_selector: str, field_name: str) -> bool:
        try:
            if self._read_selected_flag_css(execu, css_selector):
                logger.info("Idempotent skip: block already selected (%s)", css_selector)
                self.trace.write({"event": "select_block_skip", "selector": css_selector, "field": field_name})
                return True

            if not execu.run_one({"type": StepType.WAIT_VISIBLE, "selector": css_selector, "timeout": 2}):
                return False
            execu.run_one({"type": StepType.CLICK, "selector": css_selector, "retries": 0, "timeout": 2})
            execu.run_one({"type": StepType.WAIT_DOM_STABLE, "ms": 160, "timeout": 3})
            if self._read_selected_flag_css(execu, css_selector):
                self.trace.write({"event": "select_block_written", "selector": css_selector, "field": field_name, "via": "click"})
                return True

            execu.run_one({
                "type": StepType.EVALUATE,
                "script": "try{ const el=document.querySelector(arguments[0]); if(el){ el.click(); return true;} }catch(e){} return false;",
                "args": [css_selector[len("css="):]],
                "timeout": 2
            })
            execu.run_one({"type": StepType.WAIT_DOM_STABLE, "ms": 160, "timeout": 3})
            if self._read_selected_flag_css(execu, css_selector):
                self.trace.write({"event": "select_block_written", "selector": css_selector, "field": field_name, "via": "js_click"})
                return True

            execu.run_one({
                "type": StepType.EVALUATE,
                "script": """
                try{
                  const el = document.querySelector(arguments[0]);
                  if(!el) return false;
                  const root = el.closest('[role="tab"],[role="option"],[role="radio"],dynamic-component[role="tab"],.selection-item,selection-card,.item,.card,.card-wrapper') || el;
                  root.click(); return true;
                }catch(e){ return false; }
                """,
                "args": [css_selector[len("css="):]],
                "timeout": 2
            })
            execu.run_one({"type": StepType.WAIT_DOM_STABLE, "ms": 160, "timeout": 3})
            ok = self._read_selected_flag_css(execu, css_selector)
            self.trace.write({
                "event": "select_block_written",
                "selector": css_selector, "field": field_name, "via": "js_click_root", "ok": ok
            })
            return ok
        except Exception:
            return False

    # ---- Базовые действия ----

    def _click_next(self, execu: StepExecutor, *, cur_fp: Optional[str]) -> bool:
        return self._click_any(
            execu,
            [
                'text="Next"', 'text="Continue"', 'text="Save and continue"',
                'role=button["Next"]', 'role=button["Continue"]',
                'text="Продолжить"', 'text="Далее"'
            ],
            cur_fp=cur_fp
        )

    def _click_any(self, execu: StepExecutor, locators: List[str], *, cur_fp: Optional[str]) -> bool:
        for sel in locators:
            if execu.debounce_would_skip(sel, cur_fp):
                return True

            def _css_is_disabled(s: str) -> bool:
                if not s.startswith("css="):
                    return False
                v = f"dis_{int(time.time()*1e3)%100000}"
                execu.run_one({
                    "type": StepType.EVALUATE,
                    "script": """
                    try{
                      const el = document.querySelector(arguments[0]);
                      if(!el) return true;
                      const aria = (el.getAttribute('aria-disabled')||'').toLowerCase()==='true';
                      const dis  = el.hasAttribute('disabled');
                      const cls  = (el.className||'').toLowerCase();
                      const looksDisabled = /(\\bdisabled\\b|\\bis-disabled\\b|\\bmdc-button--disabled\\b)/.test(cls);
                      return aria || dis || looksDisabled;
                    }catch(e){return false;}
                    """,
                    "args": [s[len("css="):]],
                    "var": v,
                    "timeout": 2
                })
                return bool(execu.ctx.var_store.get(v))

            if _css_is_disabled(sel):
                self.trace.write({"event": "click_skip_disabled", "selector": sel})
                continue

            if execu.run_one({"type": StepType.WAIT_VISIBLE, "selector": sel, "timeout": 3}):
                execu.mark_click(sel, cur_fp)
                ok = execu.run_one({"type": StepType.CLICK, "selector": sel, "retries": 0, "timeout": 2})
                if ok:
                    old_fp = cur_fp
                    old_url = self.driver.current_url if self.driver else ""
                    t0 = time.time()
                    advanced = False
                    for _ in range(16):  # ~2.8s
                        execu.run_one({"type": StepType.WAIT_DOM_STABLE, "ms": 160, "timeout": 2})
                        ui2 = self._scan_ui()
                        fp2 = _fingerprint(ui2)
                        url2 = self.driver.current_url if self.driver else ""
                        if fp2 != old_fp or url2 != old_url:
                            advanced = True
                            break
                        time.sleep(0.16)
                    self.trace.write({"event": "after_click", "selector": sel, "advanced": advanced, "wait_ms": int((time.time()-t0)*1000)})
                    if not advanced:
                        try:
                            snap = self.artifacts.save_screenshot(self.driver, "next_ineffective")
                            html = self.artifacts.save_html(self.driver, "next_ineffective")
                            logger.warning("Next ineffective on fp=%s (snap=%s)", old_fp, snap)
                            self.trace.write({"event": "next_ineffective", "screenshot": str(snap), "html": str(html)})
                        except Exception:
                            pass
                    return True
        return False

    def _input_first_idempotent(self, execu: StepExecutor, locs: List[str], text: str, name: str, *, timeout_visible: float = 1.2) -> bool:
        for sel in locs:
            ok = execu.run_one({"type": StepType.WAIT_VISIBLE, "selector": sel, "timeout": timeout_visible})
            if not ok:
                continue
            return self._set_with_verify(execu, sel, text, name, precheck=True)
        return False

    def _input_exact_idempotent(self, execu: StepExecutor, locator: str, text: str, name: str, *, timeout_visible: float = 1.2) -> bool:
        ok = execu.run_one({"type": StepType.WAIT_VISIBLE, "selector": locator, "timeout": timeout_visible})
        if not ok:
            return False
        return self._set_with_verify(execu, locator, text, name, precheck=True)

    def _set_with_verify(self, execu: StepExecutor, sel: str, text: str, name: str, *, precheck: bool) -> bool:
        css_like = sel.startswith("css=")
        pre_val = None
        post_val = None
        if precheck and css_like:
            vname = f"pre_{name}"
            execu.run_one({
                "type": StepType.EVALUATE,
                "script": "try{ const el=document.querySelector(arguments[0]); return el?(el.value||''):'' }catch(e){return ''}",
                "args": [sel[len("css="):]],
                "var": vname,
                "timeout": 2
            })
            pre_val = execu.ctx.var_store.get(vname)
            if isinstance(pre_val, str) and pre_val.strip() == str(text).strip():
                logger.info("Idempotent skip: %s already set", name)
                self.trace.write({"event": "idempotent_skip", "field": name, "selector": sel})
                return True

        logger.info("Fill %s via %s", name, sel)
        ok = execu.run_one({"type": StepType.INPUT, "selector": sel, "text": str(text), "clear": True, "timeout": 12})
        if not ok:
            return False

        if css_like:
            vname2 = f"post_{name}"
            execu.run_one({
                "type": StepType.EVALUATE,
                "script": "try{ const el=document.querySelector(arguments[0]); return el?(el.value||''):'' }catch(e){return ''}",
                "args": [sel[len("css="):]],
                "var": vname2,
                "timeout": 2
            })
            post_val = execu.ctx.var_store.get(vname2)
            self.trace.write({"event": "field_written", "field": name, "selector": sel,
                              "before": pre_val if isinstance(pre_val, str) else "", "after": post_val if isinstance(post_val, str) else ""})
        return True

    # ---- Локаторы из ui_map ----

    @staticmethod
    def _locators(ui: Dict[str, Any], *, want: Tuple[str, ...], labels: Tuple[str, ...]) -> List[str]:
        out: List[str] = []
        for c in ui.get("controls") or []:
            tag = (c.get("tag") or "").lower()
            if tag not in want:
                continue
            lab = ((c.get("label") or "") + " " + (c.get("text") or "") + " " + (c.get("block") or "")).strip().lower()
            if any(x.lower() in lab for x in labels):
                locs = c.get("locators") or []
                if locs:
                    out.append(locs[0])
        return out

    @staticmethod
    def _locators_many(ui: Dict[str, Any], *, want: Tuple[str, ...], labels: Tuple[str, ...]) -> List[str]:
        out: List[str] = []
        for c in ui.get("controls") or []:
            tag = (c.get("tag") or "").lower()
            if tag not in want:
                continue
            lab = ((c.get("label") or "") + " " + (c.get("text") or "") + " " + (c.get("block") or "")).strip().lower()
            if any(x.lower() in lab for x in labels):
                locs = c.get("locators") or []
                if locs:
                    out.append(locs[0])
        return out

    # ---- Попапы ----

    def _dismiss_popups_soft(self, execu: StepExecutor) -> None:
        roots = ['css=[role="dialog"]', 'css=div[aria-modal="true"]', 'css=.mdc-dialog--open', 'css=[data-testid*="cookie"], css=[aria-label*="cookie"]']
        has_dialog = any(execu.run_one({"type": StepType.WAIT_VISIBLE, "selector": s, "timeout": 1}) for s in roots)
        if not has_dialog:
            return
        candidates = [
            'text="Accept all"', 'text="I agree"', 'text="Agree to all"',
            'css=button[aria-label="Accept all"]', 'text="Got it"', 'css=button[aria-label="Got it"]',
            'text="Принять все"', 'text="Я согласен"', 'text="Понятно"',
            'text="No thanks"', 'text="Не сейчас"',
        ]
        t0 = time.time()
        tried = 0
        for sel in candidates:
            if time.time() - t0 > 1.0:
                break
            tried += 1
            ok = execu.run_one({"type": StepType.WAIT_VISIBLE, "selector": sel, "timeout": 1})
            if not ok:
                continue
            clicked = execu.run_one({"type": StepType.CLICK, "selector": sel, "retries": 0, "timeout": 1})
            if clicked:
                execu.run_one({"type": StepType.WAIT_DOM_STABLE, "ms": 160, "timeout": 2})
                logger.info("Popup dismissed via %s", sel)
                break
        self.trace.write({"event": "popup_probe", "tried": tried, "window_ms": int((time.time()-t0)*1000)})

    # ---- Завершение и пост‑проверка ----

    def _plan_finish(self, allow_publish: bool) -> List[Dict[str, Any]]:
        if allow_publish:
            logger.info("Finish: Publish")
            return [
                {"type": StepType.WAIT_VISIBLE, "selector": 'text="Publish"', "timeout": 18},
                {"type": StepType.CLICK, "selector": 'text="Publish"', "retries": 1, "retry_pause_ms": 180, "timeout": 6},
                {"type": StepType.WAIT_DOM_STABLE, "ms": 350, "timeout": 10},
            ]
        else:
            logger.info("Finish: Save draft (currency mismatch)")
            return [
                {"type": StepType.WAIT_VISIBLE, "selector": 'text="Save draft"', "timeout": 18},
                {"type": StepType.CLICK, "selector": 'text="Save draft"', "retries": 1, "retry_pause_ms": 180, "timeout": 6},
                {"type": StepType.WAIT_DOM_STABLE, "ms": 350, "timeout": 10},
            ]

    def _post_check(self, execu: StepExecutor, camp_name: str) -> Tuple[str, Optional[str]]:
        try:
            execu.run_many([
                {"type": StepType.GOTO, "url": "https://ads.google.com/aw/campaigns"},
                {"type": StepType.WAIT_URL, "pattern": "aw/campaigns", "timeout": 20},
                {"type": StepType.WAIT_DOM_STABLE, "ms": 250, "timeout": 8},
                {"type": StepType.INPUT, "selector": 'css=input[aria-label="Search table"]', "text": camp_name, "clear": True},
                {"type": StepType.PRESS_KEY, "key": "ENTER"},
                {"type": StepType.WAIT_DOM_STABLE, "ms": 250, "timeout": 6},
                {"type": StepType.EXTRACT, "selector": f'text="{camp_name}"', "attr": "text", "var": "found_campaign", "all": False},
                {"type": StepType.EVALUATE,
                 "script": (
                     "const a=[...document.querySelectorAll('a[href*=\"campaigns/\"]')].map(x=>x.getAttribute('href')||'');"
                     "const m=a.map(x=>x.match(/campaigns\\/([0-9]+)/)).find(Boolean);"
                     "return m?m[1]:'';"
                 ),
                 "var": "campaign_id"},
                {"type": StepType.EVALUATE,
                 "script": (
                     "const row=(function(){"
                     "  const t=[...document.querySelectorAll('tr,div[role=row]')];"
                     "  const f=t.find(r=>(r.innerText||'').includes(arguments[0]));"
                     "  return f||null;"
                     "})();"
                     "if(!row) return '';"
                     "const txt=(row.innerText||'');"
                     "const m=txt.match(/(Eligible|Learning|Limited|Paused|Removed|Draft|Черновик|Готово|Идёт обучение)/i);"
                     "return m?m[1]:'';"
                 ),
                 "args": [camp_name],
                 "var": "campaign_status"}
            ], stop_on_error=False)
        except Exception:
            pass

        status = str(self.vars.get("campaign_status") or "").strip() or "UNKNOWN"
        cid = str(self.vars.get("campaign_id") or "").strip() or None
        logger.info("Post-check: status=%s id=%s", status, cid)
        return status.upper(), cid

    # ---- Сканер UI ----

    def _scan_ui(self) -> Dict[str, Any]:
        assert self.driver is not None
        try:
            data = self.driver.execute_script(self._scan_ui_map_js())
        except Exception as e:
            self.trace.write({"event": "ui_scan_error", "error": str(e)})
            data = {"page_heading": "", "controls": []}

        ctrls = data.get("controls") or []
        filt: List[Dict[str, Any]] = []
        for c in ctrls:
            score = 0
            tag = (c.get("tag") or "").lower()
            rl = (c.get("role") or "").lower()
            lbl_all = ((c.get("label") or "") + " " + (c.get("text") or "") + " " + (c.get("block") or "")).lower()

            if tag in ("input", "textarea", "select"):
                score += 3
            if rl in ("button", "option", "combobox", "listbox", "radio", "checkbox", "tab"):
                score += 2
            if any(k in lbl_all for k in (
                "campaign", "website", "web page url", "budget", "final url", "headline", "description", "keywords",
                "ключев", "заголовок", "описание", "конечный url", "бюджет", "название кампании", "ваш сайт",
                "what's your business name", "создать кампанию без подсказок", "select a campaign type", "search"
            )):
                score += 3
            if any(k in lbl_all for k in ("search table", "toolbar", "поиск", "найти в таблице")):
                score -= 5

            c["score"] = score
            if score >= 0:
                c["locators"] = list(dict.fromkeys(c.get("locators") or []))[:6]
                filt.append(c)
        filt = sorted(filt, key=lambda x: x["score"], reverse=True)[:120]
        return {"page_heading": data.get("page_heading") or "", "controls": filt}

    @staticmethod
    def _scan_ui_map_js() -> str:
        """
        Сбор интерактивных контролов в основной области (main/role=main) + нормализация метки:
        - aria-label,
        - aria-labelledby -> текст целевых узлов,
        - label[for=id], closest <label>, родительские .label/.input-header и т.п.
        Плюс вычисляем заголовок блока (fieldset/legend, панель, h2/h3).
        """
        return r"""
        const isVisibleBox = (r) => r && r.width > 8 && r.height > 8 &&
          r.top < window.innerHeight && r.left < window.innerWidth && r.bottom > 0 && r.right > 0;

        const styleOk = (el) => {
            const cs = window.getComputedStyle(el);
            return !(cs.visibility === 'hidden' || cs.display === 'none' || parseFloat(cs.opacity||'1') < 0.2 || cs.pointerEvents === 'none');
        };

        const notAriaHidden = (el) => {
            for (let p = el; p; p = p.parentElement) {
                if (p.hasAttribute('hidden') || p.getAttribute('aria-hidden') === 'true') return false;
                const cs = window.getComputedStyle(p);
                if (cs.visibility === 'hidden' || cs.display === 'none' || parseFloat(cs.opacity||'1') < 0.2) return false;
            }
            return true;
        };

        const notDisabled = (el) => {
            const aria = (el.getAttribute('aria-disabled')||'').toLowerCase()==='true';
            const dis  = el.hasAttribute('disabled');
            const cls  = (el.className||'').toLowerCase();
            const looksDisabled = /(\bdisabled\b|\bis-disabled\b|\bmdc-button--disabled\b)/.test(cls);
            return !(aria || dis || looksDisabled);
        };

        const hitTest = (el) => {
            const r = el.getBoundingClientRect();
            const pts = [
                [r.left+2,r.top+2],[r.right-2,r.top+2],
                [r.left+2,r.bottom-2],[r.right-2,r.bottom-2],
                [r.left+r.width/2,r.top+r.height/2]
            ];
            for (const [x,y] of pts) {
                if (x < 0 || y < 0 || x > window.innerWidth-1 || y > window.innerHeight-1) continue;
                const top = document.elementFromPoint(x, y);
                if (top && (top === el || el.contains(top) || top.contains(el))) return true;
            }
            return false;
        };

        const isInteractable = (el) => {
            if (!el) return false;
            const r = el.getBoundingClientRect();
            if (!isVisibleBox(r)) return false;
            if (!styleOk(el)) return false;
            if (!notAriaHidden(el)) return false;
            if (!notDisabled(el)) return false;
            if (el.closest('header,nav,aside,[role="navigation"],[role="toolbar"]')) return false;
            const t = (el.getAttribute('type')||'').toLowerCase();
            const a = (el.getAttribute('aria-label')||'').toLowerCase();
            const p = (el.getAttribute('placeholder')||'').toLowerCase();
            if (t === 'search' || a.includes('search table') || p.includes('search')) return false;
            if (!hitTest(el)) return false;
            return true;
        };

        const textOrEmpty = (n) => (n && (n.innerText || n.textContent) || '').trim();
        const roleOf = (el) => el.getAttribute('role') || '';
        const aria = (el) => el.getAttribute('aria-label') || '';
        const ph = (el) => el.getAttribute('placeholder') || '';
        const nameAttr = (el) => el.getAttribute('name') || '';
        const idAttr = (el) => el.getAttribute('id') || '';

        const getLabelledByText = (el) => {
            const ids = (el.getAttribute('aria-labelledby')||'').trim();
            if (!ids) return '';
            const out = [];
            for (const id of ids.split(/\s+/)) {
                const n = document.getElementById(id);
                if (n) out.push(textOrEmpty(n));
            }
            return out.join(' ').trim();
        };

        const labelByFor = (el) => {
            const id = idAttr(el);
            if (!id) return '';
            const lab = document.querySelector(`label[for="${id}"]`);
            return textOrEmpty(lab);
        };

        const closestLabelText = (el) => {
            const lab = el.closest('label');
            if (lab) return textOrEmpty(lab);
            return '';
        };

        const parentHints = (el) => {
            let p = el.parentElement, tries = 0;
            let acc = '';
            while (p && tries < 4) {
                const t = textOrEmpty(p);
                const a = (p.getAttribute && p.getAttribute('aria-label')) || '';
                const cls = (p.className||'').toLowerCase();
                if (t) acc += ' ' + t;
                if (a) acc += ' ' + a;
                if (/(input-header|editor-panel|material-input|gm-input|selection-card|card-wrapper|item)/.test(cls)) {
                    // сохраняем, но не бесконечно
                }
                p = p.parentElement; tries += 1;
            }
            return acc.trim();
        };

        const blockTitle = (el) => {
            const legend = el.closest('fieldset')?.querySelector('legend');
            if (legend) return textOrEmpty(legend);
            // панельные заголовки
            let cur = el.closest('section, .editor-panel, .panel-body, [role="region"], form, div');
            for (let i=0; i<4 && cur; i++) {
                const h = cur.querySelector(':scope > h2, :scope > h3, :scope > .input-header');
                if (h) return textOrEmpty(h);
                cur = cur.parentElement;
            }
            // ближайший предшествующий заголовок
            const hs = [...document.querySelectorAll('h2,h3')];
            let best = '';
            for (const h of hs) {
                const r = h.getBoundingClientRect();
                const er = el.getBoundingClientRect();
                if (r.top <= er.top + 2 && Math.abs(r.left - er.left) < 400) {
                    best = textOrEmpty(h);
                }
            }
            return best;
        };

        const toLocators = (el, t, a, rl) => {
            const L = [];
            const tag = el.tagName.toLowerCase();
            const id = idAttr(el);
            const labelledBy = (el.getAttribute('aria-labelledby')||'').trim();
            if (id) L.push(`css=#${id}`);
            if (nameAttr(el)) L.push(`css=${tag}[name="${nameAttr(el)}"]`);
            if (a) L.push(`css=${tag}[aria-label="${a}"]`);
            if (ph(el)) L.push(`css=${tag}[placeholder="${ph(el)}"]`);
            if (labelledBy) L.push(`css=${tag}[aria-labelledby="${labelledBy}"]`);
            if (t && (tag === 'button' || rl === 'button' || tag === 'a')) L.push(`text="${t}"`);
            if (rl) {
                const keyText = (t || a || ph(el) || '').trim();
                L.push(`role=${rl}["${keyText}"]`);
            }
            return Array.from(new Set(L)).slice(0, 6);
        };

        // Главный заголовок страницы — берём просто видимый h1/h2
        let page_heading = '';
        const hMain = document.querySelector('h1,h2');
        if (hMain) {
            const cs = window.getComputedStyle(hMain);
            if (!(cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2)) {
                page_heading = textOrEmpty(hMain);
            }
        }

        // Основная область
        let mainRoot = document.querySelector('main,[role="main"]') || document.body;

        const q = `
          input:not([type="hidden"]),
          textarea, select,
          button, [role="button"], a[role="button"],
          [role="combobox"], [role="listbox"], [role="option"], [role="radio"], [role="checkbox"], [role="tab"],
          [tabindex]:not([tabindex="-1"])
        `;
        const nodes = Array.from(mainRoot.querySelectorAll(q));
        const controls = [];
        for (const el of nodes) {
            const tag = el.tagName.toLowerCase();
            const rl = roleOf(el);
            const pointerish = (getComputedStyle(el).cursor||'').toLowerCase().includes('pointer');
            const isCtrl = tag==='input'||tag==='textarea'||tag==='select'||tag==='button'||rl==='button'||rl==='combobox'||rl==='listbox'||rl==='option'||rl==='radio'||rl==='checkbox'||rl==='tab'||pointerish;
            if (!isCtrl) continue;
            if (!isInteractable(el)) continue;

            // «Человеческая» метка
            const a = aria(el);
            const lb = getLabelledByText(el);
            const lf = labelByFor(el);
            const cl = closestLabelText(el);
            const phText = ph(el);
            const parentTxt = parentHints(el);

            // собираем метку приоритетно
            const labelText = (a || lb || lf || cl || '').trim();

            const t = (textOrEmpty(el) || '').trim();
            const locators = toLocators(el, t, a, rl);
            if (!locators.length) continue;

            const block = blockTitle(el);

            controls.push({
                tag, role: rl, text: t, label: labelText || a || lb || lf || cl,
                placeholder: phText, type: (el.getAttribute('type')||'').toLowerCase(),
                locators, block, raw_context: parentTxt
            });
            if (controls.length > 180) break;
        }

        return ({ page_heading, controls });
        """

    # ---- JS‑фоллбек: ввод по текстовой метке вокруг ----

    @staticmethod
    def _js_fill_input_by_label() -> str:
        return r"""
        try{
          const value = arguments[0];
          const wantedList = (arguments[1] || []).map(s => String(s||'').toLowerCase());
          const vis = (el)=>{if(!el) return false;const r=el.getBoundingClientRect();const cs=getComputedStyle(el);
            if (cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.2||cs.pointerEvents==='none') return false;
            if (el.hasAttribute('hidden')||el.getAttribute('aria-hidden')==='true') return false;
            return r.width>8 && r.height>8 && r.top<innerHeight && r.left<innerWidth && r.bottom>0 && r.right>0;
          };
          const hit = (el)=>{const r=el.getBoundingClientRect();
            const pts=[[r.left+2,r.top+2],[r.right-2,r.top+2],[r.left+2,r.bottom-2],[r.right-2,r.bottom-2],[r.left+r.width/2,r.top+r.height/2]];
            for(const [x,y] of pts){ if(x<0||y<0||x>innerWidth-1||y>innerHeight-1) continue;
              const a=document.elementFromPoint(x,y);
              if(a&&(a===el||el.contains(a)||a.contains(el))) return true;
            } return false;
          };
          const norm = s => (s||'').toLowerCase();

          const inputs=[...document.querySelectorAll('main input, [role=main] input, form input, section input, div input, textarea')]
            .filter(e=>vis(e)&&!e.readOnly&&!e.disabled&&hit(e));

          const score = (el) => {
            const a = norm(el.getAttribute('aria-label'));
            const lbIds = (el.getAttribute('aria-labelledby')||'').trim().split(/\s+/).filter(Boolean);
            let lbTxt = '';
            for(const id of lbIds){ const n=document.getElementById(id); lbTxt += ' ' + (n? (n.innerText||n.textContent||''):''); }
            const id = el.getAttribute('id')||'';
            const labFor = id ? (document.querySelector(`label[for="${id}"]`) || null) : null;
            const labForTxt = labFor ? (labFor.innerText||labFor.textContent||'') : '';
            const clab = el.closest('label'); const clTxt = clab ? (clab.innerText||clab.textContent||'') : '';
            let par = el.parentElement, hops=0, parTxt='';
            while(par && hops<3){ parTxt += ' ' + (par.innerText||par.textContent||'') + ' ' + (par.getAttribute('aria-label')||''); par=par.parentElement; hops++; }

            const hay = norm([a, lbTxt, labForTxt, clTxt, parTxt].join(' '));
            let s=0;
            for(const w of wantedList){ if(w && hay.includes(w)) s += 10; }
            if (a) s+=2;
            return s;
          };

          let best = null, bestScore=-1;
          for(const el of inputs){
            const sc = score(el);
            if (sc>bestScore){ best=el; bestScore=sc; }
          }

          if (!best || bestScore<=0) return false;
          if ((best.value||'') === String(value)) return true;
          best.focus();
          best.value = String(value);
          best.dispatchEvent(new Event('input',{bubbles:true}));
          best.dispatchEvent(new Event('change',{bubbles:true}));
          return true;
        }catch(e){ return false; }
        """

# ==========
# API
# ==========

def create_google_ads_campaign(settings: Settings, payload: CampaignInput) -> CampaignResult:
    wiz = GoogleAdsCampaignWizard(settings)
    wiz.start(profile_id=payload.profile_id)
    try:
        return wiz.run(payload)
    finally:
        wiz.stop()
