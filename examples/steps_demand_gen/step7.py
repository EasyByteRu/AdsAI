# -*- coding: utf-8 -*-
"""
Шаг 7 (Demand Gen) - улучшенная версия:
- открывает раздел Ad schedule;
- запрашивает у LLM оптимальный график показов (максимум 7 записей);
- заполняет расписание напрямую в timezone аккаунта;
- упрощенная и ускоренная логика.
"""

from __future__ import annotations

import logging
import os
import re
import time
from typing import Any, Callable, Dict, Iterable, List, Optional

from selenium.webdriver.remote.webdriver import WebDriver

from examples.steps import step4 as base_step4  # type: ignore

try:
    from ads_ai.llm.gemini import GeminiClient  # type: ignore
except Exception:  # pragma: no cover
    GeminiClient = None  # type: ignore

logger = logging.getLogger("ads_ai.gads.step7.demand_gen")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

_emit = base_step4._emit  # type: ignore
_maybe_handle_confirm_its_you = base_step4._maybe_handle_confirm_its_you  # type: ignore
_ensure_panel_open = base_step4._ensure_panel_open  # type: ignore

AD_SCHEDULE_SYNS: List[str] = [
    "ad schedule",
    "schedule",
    "расписание показов",
    "расписание",
    "schedule for ads",
]

# Доступные дни недели в dropdown Google Ads
AVAILABLE_DAY_OPTIONS = [
    "All days",
    "Mondays - Fridays",
    "Saturdays - Sundays",
    "Mondays",
    "Tuesdays",
    "Wednesdays",
    "Thursdays",
    "Fridays",
    "Saturdays",
    "Sundays",
]

# Маппинг дней недели на варианты в dropdown
DAY_MAPPING = {
    "monday": "Mondays",
    "tuesday": "Tuesdays",
    "wednesday": "Wednesdays",
    "thursday": "Thursdays",
    "friday": "Fridays",
    "saturday": "Saturdays",
    "sunday": "Sundays",
    "all days": "All days",
    "weekdays": "Mondays - Fridays",
    "weekend": "Saturdays - Sundays",
}

TIME_12H_PATTERN = re.compile(r'^(\d{1,2}):(\d{2})\s*(AM|PM)$', re.IGNORECASE)


def _parse_12h_time(time_str: str) -> Optional[str]:
    """Парсит и валидирует время в формате 12-часов (например: '9:00 AM', '11:30 PM')."""
    time_str = time_str.strip()
    match = TIME_12H_PATTERN.match(time_str)
    if not match:
        return None

    hour = int(match.group(1))
    minute = int(match.group(2))
    period = match.group(3).upper()

    if not (1 <= hour <= 12) or not (0 <= minute <= 59):
        return None

    return f"{hour}:{minute:02d} {period}"


def _read_timezone_label(driver: WebDriver) -> str:
    """Читает label с часовым поясом из интерфейса."""
    try:
        label = driver.execute_script(
            """
            const tz = document.querySelector('ad-schedule-editor .time-zone');
            if (!tz) return '';
            return (tz.textContent || tz.innerText || '').trim();
            """
        )
        return str(label or "").strip()
    except Exception:
        return ""


def _decide_schedule_via_llm(
    *,
    business_name: Optional[str],
    usp: Optional[str],
    site_url: Optional[str],
    locations: Optional[Iterable[str]],
    languages: Optional[Iterable[str]],
    account_timezone: str,
) -> Dict[str, Any]:
    """
    Запрашивает у LLM оптимальное расписание показа рекламы.

    Формат ответа от LLM:
    {
        "schedules": [
            {"day": "Monday", "start": "9:00 AM", "end": "6:00 PM"},
            {"day": "Tuesday", "start": "9:00 AM", "end": "6:00 PM"},
            ...
        ],
        "reason": "Краткое объяснение"
    }

    Максимум 7 записей (по одной на каждый день недели).
    """
    fallback_schedules = [
        {"day": "All days", "start": "9:00 AM", "end": "6:00 PM"}
    ]
    fallback = {
        "source": "fallback",
        "reason": "LLM unavailable — using default 9:00 AM - 6:00 PM schedule.",
        "schedules": fallback_schedules,
    }

    if GeminiClient is None:
        logger.warning("GeminiClient not available, using fallback schedule")
        return fallback

    prompt = f"""You are planning an ad schedule for a Google Ads Demand Gen campaign.

**Business Context:**
- Business Name: {business_name or 'N/A'}
- Unique Selling Proposition: {usp or 'N/A'}
- Website: {site_url or 'N/A'}
- Target Locations: {', '.join(locations) if locations else 'N/A'}
- Languages: {', '.join(languages) if languages else 'N/A'}
- Account Timezone: {account_timezone}

**Task:**
Based on the business context, determine the OPTIMAL ad schedule for when ads should be shown.
Consider:
1. Target audience behavior and when they are most likely to engage
2. Business type (B2B vs B2C, service hours, etc.)
3. Time zone of the target audience
4. Industry best practices

**IMPORTANT FORMAT REQUIREMENTS:**
- Return a JSON object with two keys: "schedules" (array) and "reason" (string)
- "schedules" must contain 1-7 entries (each representing a time slot)
- Each schedule entry MUST have exactly these fields:
  * "day": Day of week as "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday", OR special values "All days", "Mondays - Fridays", "Saturdays - Sundays"
  * "start": Start time in 12-hour format with AM/PM (e.g., "9:00 AM", "2:30 PM")
  * "end": End time in 12-hour format with AM/PM (e.g., "6:00 PM", "11:30 PM")
- "reason": Brief explanation (max 160 characters) of why this schedule is optimal
- Use the account timezone ({account_timezone}) for all times
- DO NOT recommend 24/7 unless absolutely necessary for the business type
- Prefer specific time windows when the target audience is most active

**Example Response:**
{{
    "schedules": [
        {{"day": "Mondays - Fridays", "start": "9:00 AM", "end": "6:00 PM"}},
        {{"day": "Saturdays", "start": "10:00 AM", "end": "3:00 PM"}}
    ],
    "reason": "Business hours targeting for B2B audience in working days, limited weekend exposure"
}}

Return ONLY valid JSON, no other text."""

    try:
        client = GeminiClient(
            model=os.getenv("GEMINI_MODEL", "models/gemini-2.0-flash"),
            temperature=0.3,
            retries=2,
            fallback_model=None
        )
        raw = client.generate_json(prompt)
    except Exception as exc:
        logger.warning("LLM schedule decision failed: %s — using fallback", exc)
        return fallback

    if not isinstance(raw, dict):
        logger.warning("LLM returned non-dict response: %s", type(raw))
        return fallback

    schedules = raw.get("schedules", [])
    if not isinstance(schedules, list) or not schedules:
        logger.warning("LLM returned invalid schedules: %s", schedules)
        return fallback

    # Валидация и нормализация расписания
    validated_schedules = []
    for entry in schedules[:7]:  # Максимум 7 записей
        if not isinstance(entry, dict):
            continue

        day = str(entry.get("day", "")).strip()
        start = str(entry.get("start", "")).strip()
        end = str(entry.get("end", "")).strip()

        # Нормализуем день недели
        day_lower = day.lower()
        normalized_day = DAY_MAPPING.get(day_lower, day)

        # Проверяем, что день существует в dropdown
        if normalized_day not in AVAILABLE_DAY_OPTIONS:
            # Попробуем найти частичное совпадение
            found = False
            for available_day in AVAILABLE_DAY_OPTIONS:
                if day_lower in available_day.lower():
                    normalized_day = available_day
                    found = True
                    break
            if not found:
                logger.warning("Invalid day option: %s, skipping", day)
                continue

        # Валидируем время
        validated_start = _parse_12h_time(start)
        validated_end = _parse_12h_time(end)

        if not validated_start or not validated_end:
            logger.warning("Invalid time format: %s - %s, skipping", start, end)
            continue

        validated_schedules.append({
            "day": normalized_day,
            "start": validated_start,
            "end": validated_end,
        })

    if not validated_schedules:
        logger.warning("No valid schedules after validation, using fallback")
        return fallback

    reason = str(raw.get("reason", "")).strip()[:160] or "LLM-generated schedule"

    logger.info("LLM schedule decision: %d entries, reason: %s", len(validated_schedules), reason)
    return {
        "source": "llm",
        "reason": reason,
        "schedules": validated_schedules,
    }


def _get_schedule_row_count(driver: WebDriver) -> int:
    """Возвращает количество строк расписания на странице."""
    try:
        count = driver.execute_script(
            """
            return document.querySelectorAll('ad-schedule-editor-row .row').length || 0;
            """
        )
        return int(count or 0)
    except Exception:
        return 0


def _click_add_row(driver: WebDriver) -> bool:
    """Кликает кнопку Add для добавления новой строки расписания."""
    _maybe_handle_confirm_its_you(driver, emit=None)
    try:
        return bool(
            driver.execute_script(
                """
                const buttons = [...document.querySelectorAll('ad-schedule-editor material-button')];
                for (const btn of buttons) {
                    const text = (btn.textContent || '').trim().toLowerCase();
                    if (text === 'add') {
                        btn.click();
                        return true;
                    }
                }
                return false;
                """
            )
        )
    except Exception as exc:
        logger.debug("Add row click failed: %s", exc)
        return False


def _add_rows_if_needed(driver: WebDriver, target_count: int, *, timeout: float = 10.0) -> int:
    """
    Добавляет строки расписания до достижения нужного количества.
    Возвращает финальное количество строк.
    """
    target_count = max(1, int(target_count))
    deadline = time.time() + timeout

    logger.info("Adding rows to reach target count: %d", target_count)

    while time.time() < deadline:
        current_count = _get_schedule_row_count(driver)

        if current_count >= target_count:
            logger.info("Reached target row count: %d", current_count)
            return current_count

        logger.debug("Current rows: %d, target: %d — adding row", current_count, target_count)
        if not _click_add_row(driver):
            logger.warning("Failed to click Add button")
            break

        time.sleep(0.15)  # Небольшая пауза для загрузки новой строки

    final_count = _get_schedule_row_count(driver)
    if final_count < target_count:
        logger.warning("Could not reach target row count: have %d, want %d", final_count, target_count)

    return final_count


def _select_day_in_row(driver: WebDriver, row_index: int, day_label: str) -> bool:
    """
    Выбирает день недели в dropdown для указанной строки расписания.

    Args:
        row_index: Индекс строки (0-based)
        day_label: Название дня (например, "Mondays", "All days", "Saturdays - Sundays")
    """
    try:
        result = driver.execute_script(
            """
            const rowIndex = arguments[0];
            const targetLabel = String(arguments[1] || '').toLowerCase().trim();

            const rows = [...document.querySelectorAll('ad-schedule-editor-row .row')];
            const row = rows[rowIndex];
            if (!row) {
                console.log('Row not found:', rowIndex);
                return false;
            }

            const dropdown = row.querySelector('material-dropdown-select');
            if (!dropdown) {
                console.log('Dropdown not found in row:', rowIndex);
                return false;
            }

            const normalize = (text) => String(text || '').replace(/\\s+/g, ' ').trim().toLowerCase();

            // Функция для поиска и клика на нужный пункт меню
            const selectItem = (container) => {
                const items = [...container.querySelectorAll('material-select-dropdown-item')];
                for (const item of items) {
                    const itemText = normalize(item.textContent || item.innerText);
                    if (itemText === targetLabel) {
                        item.click();
                        return true;
                    }
                }
                return false;
            };

            // Сначала проверим, может dropdown уже открыт
            if (selectItem(document.body)) {
                return true;
            }

            // Если нет, откроем dropdown
            const button = dropdown.querySelector('.button');
            if (!button) {
                console.log('Button not found in dropdown');
                return false;
            }

            button.click();

            // Ждем появления popup и выбираем пункт
            return new Promise((resolve) => {
                setTimeout(() => {
                    resolve(selectItem(document.body));
                }, 100);
            });
            """,
            row_index,
            day_label.lower()
        )

        # Если вернулся Promise, подождем
        if isinstance(result, dict) and 'then' in str(result):
            time.sleep(0.2)
            return True

        return bool(result)
    except Exception as exc:
        logger.debug("Failed to select day in row %d: %s", row_index, exc)
        return False


def _set_time_in_row(driver: WebDriver, row_index: int, start_time: str, end_time: str) -> bool:
    """
    Устанавливает время начала и окончания для указанной строки расписания.

    Args:
        row_index: Индекс строки (0-based)
        start_time: Время начала (например, "9:00 AM")
        end_time: Время окончания (например, "6:00 PM")
    """
    try:
        result = driver.execute_script(
            """
            const rowIndex = arguments[0];
            const startTime = arguments[1];
            const endTime = arguments[2];

            const rows = [...document.querySelectorAll('ad-schedule-editor-row .row')];
            const row = rows[rowIndex];
            if (!row) return false;

            const startInput = row.querySelector('.start-time input');
            const endInput = row.querySelector('.end-time input');

            if (!startInput || !endInput) return false;

            const setInput = (input, value) => {
                input.focus();
                input.value = value;
                input.setAttribute('value', value);
                input.dispatchEvent(new Event('input', {bubbles: true}));
                input.dispatchEvent(new Event('change', {bubbles: true}));
                input.blur();
            };

            setInput(startInput, startTime);
            setInput(endInput, endTime);

            return true;
            """,
            row_index,
            start_time,
            end_time
        )
        return bool(result)
    except Exception as exc:
        logger.debug("Failed to set time in row %d: %s", row_index, exc)
        return False


def run_step7(
    driver: WebDriver,
    *,
    business_name: Optional[str] = None,
    site_url: Optional[str] = None,
    usp: Optional[str] = None,
    locations: Optional[Iterable[str]] = None,
    languages: Optional[Iterable[str]] = None,
    emit: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """
    Основная функция step7 - настройка расписания показов рекламы.

    Процесс:
    1. Открывает панель Ad schedule
    2. Читает timezone аккаунта
    3. Запрашивает у LLM оптимальное расписание (максимум 7 записей)
    4. Добавляет необходимое количество строк расписания
    5. Заполняет каждую строку: день недели, время начала, время окончания
    """
    logger.info("=" * 80)
    logger.info("DemandGen Step7 START")
    logger.info("Business: %s | Site: %s", business_name, site_url)
    if usp:
        logger.info("USP: %s", usp[:100])
    if locations:
        logger.info("Locations: %s", list(locations))
    if languages:
        logger.info("Languages: %s", list(languages))
    logger.info("=" * 80)

    t0 = time.time()

    # Обработка confirm
    _maybe_handle_confirm_its_you(driver, emit=emit)

    # Открываем панель расписания
    _emit(emit, "Открываю раздел Ad Schedule...")
    _ensure_panel_open(driver, AD_SCHEDULE_SYNS)
    time.sleep(0.2)
    logger.info("Ad schedule panel opened")

    # Читаем timezone аккаунта
    tz_label = _read_timezone_label(driver)
    logger.info("Account timezone: %s", tz_label or "N/A")
    _emit(emit, f"Часовой пояс аккаунта: {tz_label or 'не определен'}")

    # Запрашиваем расписание у LLM
    _emit(emit, "Запрашиваю оптимальное расписание у LLM...")
    decision = _decide_schedule_via_llm(
        business_name=business_name,
        usp=usp,
        site_url=site_url,
        locations=locations,
        languages=languages,
        account_timezone=tz_label,
    )

    schedules = decision.get("schedules", [])
    reason = decision.get("reason", "")
    source = decision.get("source", "unknown")

    logger.info("Schedule decision from %s: %d entries", source, len(schedules))
    logger.info("Reason: %s", reason)
    for i, sched in enumerate(schedules):
        logger.info("  [%d] %s: %s - %s", i, sched['day'], sched['start'], sched['end'])

    _emit(emit, f"LLM рекомендация: {reason}")

    if not schedules:
        logger.warning("No schedules to apply, skipping step")
        _emit(emit, "Нет расписания для применения")
        elapsed_ms = int((time.time() - t0) * 1000)
        return {
            "status": "skipped",
            "reason": "No valid schedules",
            "duration_ms": elapsed_ms,
            "account_timezone": tz_label,
        }

    # Добавляем нужное количество строк (у нас уже есть 1 строка по умолчанию)
    target_row_count = len(schedules)
    current_row_count = _get_schedule_row_count(driver)
    logger.info("Current rows: %d, target: %d", current_row_count, target_row_count)

    if current_row_count < target_row_count:
        _emit(emit, f"Добавляю {target_row_count - current_row_count} строк расписания...")
        final_row_count = _add_rows_if_needed(driver, target_row_count)
    else:
        final_row_count = current_row_count

    logger.info("Final row count: %d", final_row_count)

    # Заполняем каждую строку расписания
    _emit(emit, "Заполняю расписание...")
    applied_schedules = []

    for idx, schedule in enumerate(schedules):
        if idx >= final_row_count:
            logger.warning("Row %d exceeds available rows (%d), stopping", idx, final_row_count)
            break

        day = schedule["day"]
        start = schedule["start"]
        end = schedule["end"]

        logger.info("Filling row %d: %s %s - %s", idx, day, start, end)

        # Выбираем день недели
        day_ok = _select_day_in_row(driver, idx, day)
        if not day_ok:
            logger.warning("Failed to select day '%s' in row %d", day, idx)

        time.sleep(0.1)  # Небольшая пауза после выбора дня

        # Устанавливаем время
        time_ok = _set_time_in_row(driver, idx, start, end)
        if not time_ok:
            logger.warning("Failed to set time in row %d", idx)

        applied_schedules.append({
            "row": idx,
            "day": day,
            "start": start,
            "end": end,
            "day_ok": day_ok,
            "time_ok": time_ok,
        })

        time.sleep(0.1)  # Пауза между строками

    elapsed_ms = int((time.time() - t0) * 1000)

    logger.info("=" * 80)
    logger.info("DemandGen Step7 COMPLETED in %d ms", elapsed_ms)
    logger.info("Applied %d schedule entries", len(applied_schedules))
    logger.info("=" * 80)

    _emit(emit, f"Расписание заполнено: {len(applied_schedules)} записей")

    return {
        "status": "success",
        "account_timezone": tz_label,
        "schedule_source": source,
        "schedule_reason": reason,
        "schedules_requested": schedules,
        "schedules_applied": applied_schedules,
        "duration_ms": elapsed_ms,
    }
