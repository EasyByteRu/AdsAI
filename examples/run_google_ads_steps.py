# -*- coding: utf-8 -*-
"""
Пошаговый раннер Google Ads Wizard:
- Автоматически обнаруживает и выполняет steps/step*.py в порядке номеров (step1, step2, ...).
- Не нужно вручную импортировать и вызывать каждый шаг.

Вход CLI:
  --profile         : AdsPower profile id (обязательно)
  --budget          : Daily budget (e.g. 3000 или 25.50) — пойдёт в step1 как budget_per_day
  --url             : Website URL (можно без схемы — добавим https://) — пойдёт в step1 как site_url
  --usp             : УТП/описание бизнеса — пойдёт в step1 как usp (LLM сгенерит business_name)
  --type            : Тип кампании (например UBERVERSAL, OWNED_AND_OPERATED) — влияет на выбор варианта
  --variant         : Вариант кампании (PMAX или DEMAND_GEN); если не задан, выбирается по --type
  --location(s)     : Целевые гео (имена/коды/ID); можно повторять ключ или перечислять через , или ;
  --language(s)     : Целевые языки (имена/ISO-коды); можно повторять ключ или перечислять через , или ;
  --headless        : Запуск браузера без UI
  --from-step / --to-step : Запустить диапазон шагов

Примеры:
  python examples/run_google_ads_steps.py \
    --profile k146rs7c \
    --budget 3000 \
    --url easy-byte.ru \
    --usp "Нейросети под ключ для e-commerce" \
    --location RU --location "United States" \
    --language ru,en

  python examples/run_google_ads_steps.py \
    --profile k146rs7c \
    --budget 25.50 \
    --url https://example.com/ \
    --usp "Premium oolong tea" \
    --type UBERVERSAL \
    --locations "US,CA" \
    --languages en

  python examples/run_google_ads_steps.py \
    --profile k146rs7c \
    --budget 40 \
    --url https://example.com/ \
    --usp "Seasonal apparel collection" \
    --variant DEMAND_GEN \
    --type OWNED_AND_OPERATED
"""

from __future__ import annotations

import argparse
import importlib
import inspect
import logging
import os
import pkgutil
import re
import sys
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple, Any

from selenium.webdriver.remote.webdriver import WebDriver

from ads_ai.browser.adspower import start_adspower

logger = logging.getLogger("ads_ai.gads.steps")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )


# ====== AdsPower driver bootstrap ======

def start_driver_via_adspower(profile: str, *, headless: bool = False) -> WebDriver:
    api_base = os.getenv("ADSP_API_BASE", "http://local.adspower.net:50325")
    token = os.getenv("ADSP_API_TOKEN")
    driver = start_adspower(
        profile=profile,
        headless=headless,
        api_base=api_base,
        token=token,
        window_size="1440,900",
    )
    try:
        driver.set_page_load_timeout(25)
        driver.set_script_timeout(15)
    except Exception:
        pass
    logger.info("AdsPower attached: profile=%s", profile)
    return driver


# ====== Steps discovery ======

@dataclass
class StepSpec:
    number: int
    module_name: str         # "examples.steps.step1"
    runner_name: str         # "run_step1" или "run_step"
    runner: Callable[..., Any]


@dataclass(frozen=True)
class CampaignVariant:
    variant_id: str
    label: str
    choose_type: str
    steps_package: str


_CAMPAIGN_VARIANTS: tuple[CampaignVariant, ...] = (
    CampaignVariant(
        variant_id="PMAX",
        label="Performance Max (PMax)",
        choose_type="UBERVERSAL",
        steps_package="examples.steps",
    ),
    CampaignVariant(
        variant_id="DEMAND_GEN",
        label="Demand Gen",
        choose_type="OWNED_AND_OPERATED",
        steps_package="examples.steps_demand_gen",
    ),
)

_VARIANTS_BY_ID = {v.variant_id.upper(): v for v in _CAMPAIGN_VARIANTS}
_VARIANTS_BY_TYPE = {v.choose_type.upper(): v for v in _CAMPAIGN_VARIANTS}


def _resolve_campaign_variant(*, variant_id: Optional[str], choose_type: Optional[str]) -> CampaignVariant:
    if variant_id:
        cand = _VARIANTS_BY_ID.get(str(variant_id).strip().upper())
        if cand:
            return cand
    if choose_type:
        cand = _VARIANTS_BY_TYPE.get(str(choose_type).strip().upper())
        if cand:
            return cand
    return _CAMPAIGN_VARIANTS[0]


def _discover_steps(steps_pkg_name: str) -> List[StepSpec]:
    """
    Ищет модули <steps_pkg_name>.step<N> и их run-функции.
    Возвращает отсортированный список по <N>.
    """
    importlib.invalidate_caches()
    try:
        steps_pkg = importlib.import_module(steps_pkg_name)
    except Exception as e:
        raise RuntimeError(f"Пакет {steps_pkg_name} недоступен: {e}")

    found: List[StepSpec] = []
    for m in pkgutil.iter_modules(steps_pkg.__path__):
        name = m.name  # например "step1"
        mnum = re.match(r"^step(\d+)$", name)
        if not mnum:
            continue
        n = int(mnum.group(1))
        full_mod = f"{steps_pkg_name}.{name}"
        try:
            mod = importlib.import_module(full_mod)
        except Exception as e:
            logger.warning("Не удалось импортировать %s: %s", full_mod, e)
            continue

        # runner: run_stepN или run_step
        fn_name = f"run_step{n}"
        fn = getattr(mod, fn_name, None)
        if not callable(fn):
            fn_name = "run_step"
            fn = getattr(mod, fn_name, None)
        if not callable(fn):
            logger.warning("В модуле %s нет функции %s/ run_step — пропуск", full_mod, f"run_step{n}")
            continue

        found.append(StepSpec(number=n, module_name=full_mod, runner_name=fn_name, runner=fn))

    found.sort(key=lambda s: s.number)
    if not found:
        raise RuntimeError(f"Не найдено ни одного шага в {steps_pkg_name} (ожидались step1.py, step2.py, ...)")
    return found


# ====== Helpers ======

def _normalize_multi(values: Optional[List[str]]) -> List[str]:
    """
    Принимает список строк от argparse (каждый элемент мог быть CSV/SSV).
    Возвращает нормализованный список уникальных значений в исходном порядке.
    """
    if not values:
        return []
    acc: List[str] = []
    for item in values:
        if not item:
            continue
        # Разделители: запятая или точка с запятой
        parts = [p.strip() for p in re.split(r"[;,]", item) if p and p.strip()]
        acc.extend(parts)

    seen: set[str] = set()
    out: List[str] = []
    for v in acc:
        key = v.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(v)
    return out


# ====== Smart kwargs injection ======

def _call_step_with_injected_kwargs(
    step: StepSpec,
    driver: WebDriver,
    cli_inputs: Dict[str, Any],
    context: Dict[str, Any],
) -> Any:
    """
    Вызывает функцию шага с подстановкой аргументов по именам.
    - Всегда передаём driver.
    - Из CLI: budget_per_day, site_url, usp, choose_type, locations/languages (если нужны).
    - Из контекста: то, что вернули предыдущие шаги (business_name, website_url, budget_clean, campaign_type, ...).
    """
    sig = inspect.signature(step.runner)
    params = sig.parameters

    kwargs: Dict[str, Any] = {}
    # обязателен driver
    if "driver" in params:
        kwargs["driver"] = driver

    # 1) CLI-аргументы (переимёнованные под шаги)
    mapping_cli_to_args = {
        "budget": "budget_per_day",
        "url": "site_url",
        "usp": "usp",
        "type": "choose_type",
        # новые пробросы:
        "locations": "locations",
        "languages": "languages",
        # число объявлений для шага 7
        "n_ads": "n_ads",
    }
    for cli_key, arg_name in mapping_cli_to_args.items():
        if arg_name in params and cli_key in cli_inputs and cli_inputs[cli_key] is not None:
            kwargs[arg_name] = cli_inputs[cli_key]

    # singular-fallback: если шаг просит единственное число
    if "location" in params and cli_inputs.get("locations"):
        kwargs["location"] = cli_inputs["locations"][0]
    if "language" in params and cli_inputs.get("languages"):
        kwargs["language"] = cli_inputs["languages"][0]

    # 2) Контекст от предыдущих шагов
    for k, v in context.items():
        if k in params and k not in kwargs:
            kwargs[k] = v

    # Безопасный дефолт для n_ads, если шаг его требует, но ни CLI, ни контекст не предоставили
    if "n_ads" in params and "n_ads" not in kwargs:
        kwargs["n_ads"] = 3
        logger.info("→ Параметр n_ads не задан — используем дефолт: 3")

    logger.info("→ Запуск %s.%s(%s)", step.module_name, step.runner_name, ", ".join(sorted(kwargs.keys())))
    return step.runner(**kwargs)


def _update_context_from_result(step_no: int, result: Any, ctx: Dict[str, Any]) -> None:
    """
    Нормализует результат шага в общий контекст:
    - dict -> ctx.update(...)
    - tuple/list известных форматов
    - str (step2) -> campaign_type
    - иначе — сохраняем в ctx[f"step{n}_result"]
    """
    if isinstance(result, dict):
        ctx.update(result)
        return

    # Специальные известные шаги (для обратной совместимости):
    if step_no == 1:
        # step1 мог возвращать (business_name, website_url, budget_clean)
        if isinstance(result, (tuple, list)) and len(result) == 3:
            ctx["business_name"] = result[0]
            ctx["website_url"] = result[1]
            ctx["budget_clean"] = result[2]
            return
    if step_no == 2:
        # step2 мог возвращать выбранный тип (строка)
        if isinstance(result, str) and result:
            ctx["campaign_type"] = result
            return

    # По умолчанию — складываем сырым
    ctx[f"step{step_no}_result"] = result


# ====== Main ======

def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Google Ads Wizard (auto steps runner)")
    parser.add_argument("--profile", required=True, help="AdsPower profile id")
    parser.add_argument("--budget", required=True, help="Daily budget (e.g. 3000 or 25.50)")
    parser.add_argument("--url", required=True, help="Website URL (with or without scheme)")
    parser.add_argument("--usp", required=True, help="USP / business description")
    parser.add_argument("--type", default="UBERVERSAL", help="Campaign type for steps that accept it (e.g. UBERVERSAL, SEARCH)")
    parser.add_argument(
        "--variant",
        choices=[v.variant_id for v in _CAMPAIGN_VARIANTS],
        help="Campaign variant shortcut (sets steps package automatically).",
    )
    parser.add_argument("--headless", action="store_true", default=False, help="Headless browser")
    parser.add_argument("--from-step", type=int, default=1, help="Start from step number (default: 1)")
    parser.add_argument("--to-step", type=int, default=10**6, help="Stop at step number (inclusive). Default: all")

    # Новые опции: Location/Language (поддерживаем и единств./множеств. ключи)
    parser.add_argument(
        "--location", "--locations",
        action="append", dest="locations", default=[],
        help="Target location(s): names/codes/geo IDs. Repeat flag or separate by ',' or ';'."
    )
    parser.add_argument(
        "--language", "--languages",
        action="append", dest="languages", default=[],
        help="Target language(s): names/ISO codes. Repeat flag or separate by ',' or ';'."
    )
    # Кол-во объявлений (используется шагом 7 для расчёта кол-ва заголовков/описаний и генерации изображений)
    parser.add_argument(
        "--n-ads", "--n_ads", "--ads", "--ads-count",
        type=int, dest="n_ads", default=3,
        help="Number of ads/variations to prepare (headlines/descriptions/images). Default: 3"
    )

    args = parser.parse_args(argv)

    variant_conf = _resolve_campaign_variant(variant_id=args.variant, choose_type=args.type)
    logger.info(
        "Использую вариант кампании: %s (%s) — пакет шагов %s",
        variant_conf.variant_id,
        variant_conf.label,
        variant_conf.steps_package,
    )

    # Нормализация многозначных аргументов
    norm_locations = _normalize_multi(args.locations)
    norm_languages = _normalize_multi(args.languages)

    # CLI inputs as a dict for injection
    cli_inputs: Dict[str, Any] = {
        "budget": args.budget,
        "url": args.url,
        "usp": args.usp,
        "type": args.type,
        "locations": norm_locations if norm_locations else None,
        "languages": norm_languages if norm_languages else None,
        "n_ads": int(args.n_ads) if args.n_ads is not None else None,
        "variant": variant_conf.variant_id,
        "campaign_variant_label": variant_conf.label,
    }

    # Discover steps
    steps = _discover_steps(variant_conf.steps_package)
    steps = [s for s in steps if args.from_step <= s.number <= args.to_step]
    if not steps:
        logger.error("Нет шагов для выполнения в заданном диапазоне (%s..%s)", args.from_step, args.to_step)
        return 2

    driver = start_driver_via_adspower(args.profile, headless=args.headless)
    context: Dict[str, Any] = {}

    # Сразу положим в контекст локации/языки, чтобы были доступны всем шагам
    if norm_locations:
        context["locations"] = norm_locations
    if norm_languages:
        context["languages"] = norm_languages
    context["campaign_variant"] = variant_conf.variant_id
    context["campaign_variant_label"] = variant_conf.label
    context["steps_package"] = variant_conf.steps_package

    try:
        for spec in steps:
            logger.info("===== STEP %d (%s.%s) =====", spec.number, spec.module_name, spec.runner_name)
            try:
                res = _call_step_with_injected_kwargs(spec, driver, cli_inputs, context)
                _update_context_from_result(spec.number, res, context)
                logger.info("STEP %d OK", spec.number)
            except Exception as e:
                logger.exception("STEP %d FAILED: %s", spec.number, e)
                return 2

        # Итог
        logger.info("Все шаги выполнены успешно.")
        # Небольшая сводка полезных значений (если есть)
        summary_keys = [
            "business_name",
            "website_url",
            "budget_clean",
            "campaign_type",
            "campaign_variant_label",
            "steps_package",
            "locations",
            "languages",
        ]
        present = {k: v for k, v in context.items() if k in summary_keys and v}
        if present:
            def _fmt(v: Any) -> str:
                if isinstance(v, list):
                    return "[" + ", ".join(repr(x) for x in v) + "]"
                return repr(v)
            logger.info("Summary: %s", ", ".join(f"{k}={_fmt(present[k])}" for k in summary_keys if k in present))
        return 0

    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
