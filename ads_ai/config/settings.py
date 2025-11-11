# ads_ai/config/settings.py
from __future__ import annotations

import logging
import logging.config
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple

from .env import load_env, getenv, getenv_bool, getenv_int, getenv_float
from ads_ai.utils.paths import project_root, ensure_dir


def _load_yaml(p: Path) -> Dict[str, Any]:
    """
    Мягкая загрузка YAML-конфига.
    Возвращает пустой dict при любой ошибке/несоответствии структуры.
    """
    if not p.exists():
        return {}
    try:
        import yaml  # PyYAML
        with p.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        if not isinstance(data, dict):
            return {}
        return data
    except Exception:
        return {}


# ---------------------------------- BLOCKS ---------------------------------- #

@dataclass
class Paths:
    """
    Пути артефактов. По умолчанию — ./artifacts/...
    """
    artifacts_root: Path = field(default_factory=lambda: project_root() / "artifacts")
    screenshots_dir: Path = field(default_factory=lambda: project_root() / "artifacts" / "screenshots")
    html_snaps_dir: Path = field(default_factory=lambda: project_root() / "artifacts" / "html_snaps")
    traces_dir: Path = field(default_factory=lambda: project_root() / "artifacts" / "traces")
    per_run_subdir: bool = True  # складывать артефакты в подпапку с run_id

    def ensure(self) -> "Paths":
        """Гарантируем наличие всех директорий."""
        ensure_dir(self.artifacts_root)
        ensure_dir(self.screenshots_dir)
        ensure_dir(self.html_snaps_dir)
        ensure_dir(self.traces_dir)
        return self


@dataclass
class LLM:
    """
    Параметры модели планирования/ремонта.
    """
    model: str = "models/gemini-2.0-flash"
    fallback_model: str = "models/gemini-2.0-flash"
    temperature: float = 0.15
    retries: int = 2


@dataclass
class Browser:
    """
    Настройки поведения браузера/ожиданий.
    """
    dom_scope: str = "full"  # viewport|full
    max_dom_chars: int = 200_000
    default_wait_sec: int = 12
    step_timeout_sec: int = 35
    headless_default: bool = False
    adsp_api_base: str = "http://local.adspower.net:50325"


@dataclass
class Limits:
    """
    Лимиты для рантайма (анти-зацикливание, ретраи, переплан).
    """
    max_steps_per_task: int = 80
    max_same_step: int = 3
    max_repairs_per_step: int = 3
    replan_after_repairs: int = 3
    replan_after_skips: int = 5


@dataclass
class Humanize:
    """
    Параметры «очеловечивания» действий.
    """
    enabled: bool = True
    typing_delay_min: float = 0.05
    typing_delay_max: float = 0.20
    mouse_move_enabled: bool = True
    mouse_steps: int = 12
    scroll_chunk_px: int = 280
    jitter_ms_min: int = 120
    jitter_ms_max: int = 480


@dataclass
class Guards:
    """
    Охранные эвристики (капча, стагнация DOM и т.п.).
    """
    loop_dom_hash_window: int = 6
    loop_dom_hash_trip_count: int = 5
    captcha_keywords: List[str] = field(default_factory=lambda: ["captcha", "hcaptcha", "recaptcha"])
    auto_refresh_on_stall: bool = True


@dataclass
class Integrations:
    """
    Внешние интеграции (уведомления/капчи и пр.).
    """
    captcha_provider: Optional[str] = None  # "2captcha"|None
    notify_telegram: bool = False
    notify_slack: bool = False
    runware_api_key: Optional[str] = None
    runware_model_id: Optional[str] = None
    runware_base_url: Optional[str] = None


@dataclass
class RunwareSettings:
    """
    Параметры генерации ассетов через Runware.
    """
    api_key: Optional[str] = None
    model_id: str = "runware:100@1"
    base_url: str = "https://api.runware.ai/v1"


# ------------------------------ NEW: Planner/Tracing ------------------------- #

@dataclass
class Planner:
    """
    Настройки Plan-and-Execute (инкрементальный режим).
    Не ломают совместимость: можно не использовать — всё останется как раньше.
    """
    enabled: bool = True                  # включить инкрементальный режим по умолчанию
    mode_default: str = "pe"              # "pe" | "legacy" (SSE /api/run_stream?mode=...)
    outline_max_subgoals: int = 8         # верхняя граница числа подцелей (LLM side)
    max_steps_per_subgoal: int = 6        # сколько шагов генерировать на подцель
    verify_rounds: int = 1                # количество микро-фиксов после подцели


@dataclass
class Tracing:
    """
    Параметры ротации jsonl-трейсов (синхронизированы с JsonlTrace).
    0 в полях — означает «выключено».
    """
    max_bytes: int = 5 * 1024 * 1024      # 5 MB
    max_backups: int = 3                  # хранить .1 .. .N


# -------------------------------- SETTINGS ----------------------------------- #

@dataclass
class Settings:
    paths: Paths = field(default_factory=Paths)
    llm: LLM = field(default_factory=LLM)
    browser: Browser = field(default_factory=Browser)
    limits: Limits = field(default_factory=Limits)
    humanize: Humanize = field(default_factory=Humanize)
    guards: Guards = field(default_factory=Guards)
    integrations: Integrations = field(default_factory=Integrations)
    runware: RunwareSettings = field(default_factory=RunwareSettings)

    # Новые блоки (необязательные, обратная совместимость сохранена)
    planner: Planner = field(default_factory=Planner)
    tracing: Tracing = field(default_factory=Tracing)

    # обязательные ENV (проверяются позже, когда реально понадобятся)
    gemini_api_key: Optional[str] = None
    adsp_api_token: Optional[str] = None

    def asdict(self) -> Dict[str, Any]:
        """
        Сериализация настроек в plain-dict для логов/отладки.
        Path → str, чтобы не ломать JSON.
        """
        d = asdict(self)
        # Path → str
        d["paths"]["artifacts_root"] = str(self.paths.artifacts_root)
        d["paths"]["screenshots_dir"] = str(self.paths.screenshots_dir)
        d["paths"]["html_snaps_dir"] = str(self.paths.html_snaps_dir)
        d["paths"]["traces_dir"] = str(self.paths.traces_dir)
        return d


# ------------------------------ LOADER / LOGGING ----------------------------- #

def load_settings(config_path: Optional[Path] = None, logging_yaml: Optional[Path] = None) -> Settings:
    """
    Загрузка настроек из .env, YAML и ENV-оверрайдов.
    Приоритет: YAML → ENV → дефолты.
    Все пути создаются, логирование настраивается мягко.
    """
    load_env()  # мягкая загрузка .env

    cfg_file = config_path or (project_root() / "configs" / "config.yaml")
    raw_cfg = _load_yaml(cfg_file)

    s = Settings()

    # ----- YAML-оверрайды (мягко, рекурсивно) -----
    def update_from(d: Dict[str, Any], obj: Any) -> None:
        for k, v in d.items():
            if hasattr(obj, k):
                cur = getattr(obj, k)
                # поддержка вложенных dataclass-блоков
                if isinstance(cur, (Paths, LLM, Browser, Limits, Humanize, Guards, Integrations, Planner, Tracing, RunwareSettings)) and isinstance(v, dict):
                    update_from(v, cur)
                else:
                    setattr(obj, k, v)

    update_from(raw_cfg, s)

    # ----- ENV-оверрайды -----
    # LLM
    s.llm.model = getenv("GEMINI_MODEL", s.llm.model) or s.llm.model
    s.llm.fallback_model = getenv("GEMINI_FALLBACK_MODEL", s.llm.fallback_model) or s.llm.fallback_model
    s.llm.temperature = getenv_float("GEMINI_TEMPERATURE", s.llm.temperature)
    s.llm.retries = getenv_int("GEMINI_RETRIES", s.llm.retries)

    # Browser
    s.browser.dom_scope = (getenv("DOM_SCOPE", s.browser.dom_scope) or s.browser.dom_scope).lower()
    s.browser.max_dom_chars = _clamp_int(getenv_int("MAX_DOM", s.browser.max_dom_chars), lo=50_000, hi=2_000_000)
    s.browser.default_wait_sec = _clamp_int(getenv_int("DEFAULT_WAIT", s.browser.default_wait_sec), lo=0, hi=120)
    s.browser.step_timeout_sec = _clamp_int(getenv_int("STEP_TIMEOUT_SEC", s.browser.step_timeout_sec), lo=1, hi=300)
    s.browser.headless_default = getenv_bool("HEADLESS_DEFAULT", s.browser.headless_default)
    s.browser.adsp_api_base = getenv("ADSP_API_BASE", s.browser.adsp_api_base) or s.browser.adsp_api_base

    # Limits
    s.limits.max_steps_per_task = _clamp_int(getenv_int("MAX_STEPS_PER_TASK", s.limits.max_steps_per_task), lo=1, hi=10_000)
    s.limits.max_same_step = _clamp_int(getenv_int("MAX_SAME_STEP", s.limits.max_same_step), lo=1, hi=100)
    s.limits.max_repairs_per_step = _clamp_int(getenv_int("MAX_REPAIRS_PER_STEP", s.limits.max_repairs_per_step), lo=0, hi=50)
    s.limits.replan_after_repairs = _clamp_int(getenv_int("REPLAN_AFTER_REPAIRS", s.limits.replan_after_repairs), lo=1, hi=50)
    s.limits.replan_after_skips = _clamp_int(getenv_int("REPLAN_AFTER_SKIPS", s.limits.replan_after_skips), lo=1, hi=50)

    # Planner (Plan-and-Execute)
    s.planner.enabled = getenv_bool("PE_ENABLED", s.planner.enabled)
    s.planner.mode_default = (getenv("PE_MODE_DEFAULT", s.planner.mode_default) or s.planner.mode_default).lower()
    s.planner.outline_max_subgoals = _clamp_int(getenv_int("PE_OUTLINE_MAX_SUBGOALS", s.planner.outline_max_subgoals), lo=1, hi=64)
    s.planner.max_steps_per_subgoal = _clamp_int(getenv_int("PE_MAX_STEPS_PER_SUBGOAL", s.planner.max_steps_per_subgoal), lo=1, hi=50)
    s.planner.verify_rounds = _clamp_int(getenv_int("PE_VERIFY_ROUNDS", s.planner.verify_rounds), lo=0, hi=10)

    # Runware (генерация ассетов)
    s.runware.api_key = getenv("RUNWARE_API_KEY", s.runware.api_key or s.integrations.runware_api_key) or s.runware.api_key
    s.runware.model_id = getenv("RUNWARE_MODEL_ID", s.runware.model_id or s.integrations.runware_model_id) or s.runware.model_id
    s.runware.base_url = getenv("RUNWARE_URL", s.runware.base_url or s.integrations.runware_base_url) or s.runware.base_url

    # Tracing (ротация логов)
    s.tracing.max_bytes = _clamp_int(getenv_int("TRACING_MAX_BYTES", s.tracing.max_bytes), lo=0, hi=1_000_000_000)
    s.tracing.max_backups = _clamp_int(getenv_int("TRACING_MAX_BACKUPS", s.tracing.max_backups), lo=0, hi=100)

    # Пути гарантированно существуют
    s.paths.ensure()

    # Ключи (могут быть пустыми — проверки позже, когда реально понадобятся)
    s.gemini_api_key = getenv("GEMINI_API_KEY", None)
    s.adsp_api_token = getenv("ADSP_API_TOKEN", None)

    # Логирование: читаем logging.yaml, иначе basicConfig
    setup_logging(logging_yaml or (project_root() / "configs" / "logging.yaml"))

    logging.getLogger(__name__).info("Settings loaded: %s", s.asdict())
    return s


def setup_logging(cfg_path: Path) -> None:
    """
    Настройка логирования: сначала пытаемся прочитать YAML-конфиг,
    при любой ошибке — fallback на базовый формат.
    """
    try:
        if cfg_path.exists():
            import yaml
            with cfg_path.open("r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
            logging.config.dictConfig(cfg)
        else:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                datefmt="%H:%M:%S",
            )
    except Exception:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        )


# --------------------------------- UTILS ------------------------------------- #

def _clamp_int(v: int, *, lo: int, hi: int) -> int:
    """
    Ограничение целочисленного значения в пределах [lo, hi].
    Если v не int — максимально мягко приводим и ограничиваем.
    """
    try:
        iv = int(v)
    except Exception:
        iv = lo
    if iv < lo:
        return lo
    if iv > hi:
        return hi
    return iv
