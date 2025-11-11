# ads_ai/core/runner.py
from __future__ import annotations

import logging
import os
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

from ads_ai.config.settings import load_settings, Settings
from ads_ai.tracing.trace import make_trace, JsonlTrace, TraceContext
from ads_ai.tracing.artifacts import Artifacts
from ads_ai.utils.ids import now_id
from ads_ai.browser.adspower import start_adspower
from ads_ai.llm.gemini import GeminiClient
from ads_ai.core.bot import Bot


log = logging.getLogger(__name__)


@dataclass
class RunnerArgs:
    profile: str
    url: str
    task: Optional[str] = None
    headless: Optional[bool] = None
    config_path: Optional[Path] = None
    logging_path: Optional[Path] = None


@dataclass
class RunContext:
    run_id: str
    settings: Settings
    trace: JsonlTrace
    trace_ctx: TraceContext
    artifacts: Artifacts


def _bootstrap(args: RunnerArgs) -> RunContext:
    """Поднимаем настройки, трейс и артефакты, генерим run_id."""
    settings = load_settings(config_path=args.config_path, logging_yaml=args.logging_path)

    # опционально переопределяем headless из CLI
    if args.headless is not None:
        settings.browser.headless_default = bool(args.headless)

    run_id = now_id("run")

    trace, trace_ctx = make_trace(settings.paths.traces_dir, run_id)
    artifacts = Artifacts.for_run(
        run_id=run_id,
        base_screenshots=settings.paths.screenshots_dir,
        base_html_snaps=settings.paths.html_snaps_dir,
        per_run_subdir=settings.paths.per_run_subdir,
    )

    # Легко диагностируем запуск
    trace.write({"event": "runner_start", "run_id": run_id, "args": {
        "profile": args.profile, "url": args.url, "task": bool(args.task),
        "headless": settings.browser.headless_default
    }})

    return RunContext(
        run_id=run_id,
        settings=settings,
        trace=trace,
        trace_ctx=trace_ctx,
        artifacts=artifacts,
    )


def _ensure_env_keys(s: Settings) -> None:
    """Подкладываем API-ключи в окружение для зависимых модулей, если они заданы."""
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
    # базовый URL AdsPower тоже держим в ENV (на случай старого вызова)
    os.environ.setdefault("ADSP_API_BASE", s.browser.adsp_api_base)


def _start_driver(ctx: RunContext, profile: str):
    """Стартуем браузер через AdsPower, отчитываемся в трейс."""
    headless = ctx.settings.browser.headless_default
    d = None
    try:
        d = start_adspower(
            profile=profile,
            headless=headless,
            api_base=ctx.settings.browser.adsp_api_base,
            token=ctx.settings.adsp_api_token,
            window_size="1440,900",
        )
        ctx.trace.write({"event": "driver_started", "profile": profile, "headless": headless})
        return d
    except Exception as e:
        ctx.trace.write({"event": "driver_start_failed", "err": repr(e), "trace": traceback.format_exc()})
        raise


def _start_llm(ctx: RunContext) -> GeminiClient:
    """Создаём клиента Gemini с ретраями; ключ берётся из ENV или settings."""
    s = ctx.settings.llm
    ai = GeminiClient(model=s.model, temperature=s.temperature, retries=s.retries, fallback_model=ctx.settings.llm.fallback_model)
    ctx.trace.write({"event": "llm_ready", "model": s.model, "fallback": ctx.settings.llm.fallback_model, "temp": s.temperature})
    return ai


def main(args: RunnerArgs) -> int:
    """
    Главный lifecycle:
    - bootstrap конфигов/трейсов/артефактов
    - старт драйвера через AdsPower
    - старт LLM
    - создание Bot и выполнение задачи (или чат)
    """
    ctx = _bootstrap(args)
    _ensure_env_keys(ctx.settings)

    driver = None
    exit_code = 0

    try:
        # 1) браузер
        driver = _start_driver(ctx, args.profile)

        # 2) LLM
        ai = _start_llm(ctx)

        # 3) Bot
        bot = Bot(
            driver=driver,
            ai=ai,
            settings=ctx.settings,
            artifacts=ctx.artifacts,
            trace=ctx.trace,
            run_id=ctx.run_id,
        )
        ctx.trace.write({"event": "bot_ready"})

        # 4) Начальный URL
        bot.go(args.url)

        # 5) Режим: одноразовая задача или интерактив
        if args.task:
            ctx.trace.write({"event": "task_start", "task": args.task})
            bot.run(args.task)
            ctx.trace.write({"event": "task_done"})
        else:
            ctx.trace.write({"event": "chat_start"})
            bot.chat()
            ctx.trace.write({"event": "chat_done"})

    except KeyboardInterrupt:
        log.warning("⛔ Прервано с клавиатуры.")
        ctx.trace.write({"event": "interrupted"})
        exit_code = 130  # стандартный код для SIGINT
    except Exception as e:
        log.error("Фатальная ошибка: %s", e)
        ctx.trace.write({"event": "fatal", "err": repr(e), "trace": traceback.format_exc()})
        exit_code = 1
    finally:
        try:
            if driver:
                driver.quit()
                ctx.trace.write({"event": "driver_quit"})
        except Exception:
            ctx.trace.write({"event": "driver_quit_error"})
        ctx.trace.write({"event": "runner_exit", "exit_code": exit_code})

    return exit_code


# Удобный шорткат для импорта в других местах
def run_agent(
    profile: str,
    url: str,
    task: Optional[str] = None,
    headless: Optional[bool] = None,
    config_path: Optional[Path] = None,
    logging_path: Optional[Path] = None,
) -> int:
    return main(RunnerArgs(
        profile=profile,
        url=url,
        task=task,
        headless=headless,
        config_path=config_path,
        logging_path=logging_path,
    ))
