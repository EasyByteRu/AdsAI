# -*- coding: utf-8 -*-
"""
Шаг 7 (Demand Gen):
- открывает раздел Ad schedule;
- учитывает часовой пояс аккаунта и конвертирует расписание под целевой GMT+3 (Москва);
- запрашивает у LLM оптимальный график показов с учётом бизнес-контекста;
- вписывает расписание по дням недели, добавляя строки при необходимости.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

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

DAY_ORDER: List[str] = [
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
]

DAY_NAME_TO_INDEX: Dict[str, int] = {
    "mon": 0,
    "monday": 0,
    "mondays": 0,
    "tue": 1,
    "tues": 1,
    "tuesday": 1,
    "tuesdays": 1,
    "wed": 2,
    "weds": 2,
    "wednesday": 2,
    "wednesdays": 2,
    "thu": 3,
    "thur": 3,
    "thurs": 3,
    "thursday": 3,
    "thursdays": 3,
    "fri": 4,
    "friday": 4,
    "fridays": 4,
    "sat": 5,
    "saturday": 5,
    "saturdays": 5,
    "sun": 6,
    "sunday": 6,
    "sundays": 6,
}

DAY_INDEX_TO_LABEL: List[str] = [
    "Mondays",
    "Tuesdays",
    "Wednesdays",
    "Thursdays",
    "Fridays",
    "Saturdays",
    "Sundays",
]

TARGET_TZ_OFFSET_MIN = 3 * 60  # GMT+3 (Москва)

TIME_PATTERN = re.compile(r"^(\d{1,2})(?::?(\d{2}))?$")
TZ_OFFSET_PATTERN = re.compile(r"GMT\s*([+-])?\s*(\d{1,2})(?::(\d{2}))?", re.IGNORECASE)


def _normalize_day(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    key = value.strip().lower()
    return DAY_NAME_TO_INDEX.get(key)


def _parse_time_to_minutes(value: str) -> Optional[int]:
    value = value.strip()
    match = TIME_PATTERN.match(value.replace(".", ":"))
    if not match:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2) or "0")
    if hour == 24 and minute == 0:
        return 24 * 60
    if not (0 <= hour < 24) or not (0 <= minute < 60):
        return None
    return hour * 60 + minute


def _format_minutes_12h(minutes: int, *, is_end: bool = False) -> str:
    minutes = max(0, min(minutes, 24 * 60))
    if is_end and minutes == 24 * 60:
        minutes = (24 * 60) - 1  # 23:59 -> 11:59 PM
    minutes %= 24 * 60
    hour = minutes // 60
    minute = minutes % 60
    suffix = "AM" if hour < 12 else "PM"
    hour12 = hour % 12
    if hour12 == 0:
        hour12 = 12
    return f"{hour12}:{minute:02d} {suffix}"


def _read_timezone_label(driver: WebDriver) -> str:
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


def _parse_gmt_offset_minutes(label: str) -> int:
    if not label:
        return 0
    match = TZ_OFFSET_PATTERN.search(label)
    if not match:
        if "gmt" in label.lower():
            return 0
        return 0
    sign = -1 if match.group(1) == "-" else 1
    hours = int(match.group(2))
    minutes = int(match.group(3) or "0")
    total = hours * 60 + minutes
    return sign * total


def _normalize_schedule_entries(raw_entries: Sequence[Dict[str, str]]) -> List[Dict[str, Any]]:
    grouped: Dict[int, List[Tuple[int, int]]] = {}
    for entry in raw_entries:
        day_raw = entry.get("day")
        start_raw = entry.get("start")
        end_raw = entry.get("end")
        idx = _normalize_day(day_raw)
        if idx is None or not start_raw or not end_raw:
            continue
        start_min = _parse_time_to_minutes(str(start_raw))
        end_min = _parse_time_to_minutes(str(end_raw))
        if start_min is None or end_min is None:
            continue
        if end_min == start_min:
            start_min = 0
            end_min = 24 * 60
        if end_min <= start_min:
            # не поддерживаем ночные отрезки — отбрасываем
            logger.debug("Skipping invalid interval (%s %s-%s)", day_raw, start_raw, end_raw)
            continue
        grouped.setdefault(idx, []).append((start_min, end_min))

    normalized: List[Dict[str, Any]] = []
    for idx in sorted(grouped):
        intervals = []
        for start_min, end_min in sorted(grouped[idx], key=lambda pair: pair[0]):
            if end_min - start_min <= 0:
                continue
            intervals.append({"start": start_min, "end": end_min})
        if intervals:
            normalized.append({"day_index": idx, "intervals": intervals})
    return normalized


def _decide_schedule_via_llm(
    *,
    business_name: Optional[str],
    usp: Optional[str],
    site_url: Optional[str],
    locations: Optional[Iterable[str]],
    languages: Optional[Iterable[str]],
) -> Dict[str, Any]:
    fallback_entries = [
        {"day": day, "start": "10:00", "end": "18:00"} for day in DAY_ORDER
    ]
    fallback_normalized = _normalize_schedule_entries(fallback_entries)
    fallback = {
        "source": "fallback",
        "reason": "LLM unavailable — using default 10:00-18:00 daily schedule.",
        "entries": fallback_normalized,
        "target_entries": fallback_entries,
    }

    if GeminiClient is None:
        return fallback

    payload = {
        "task": (
            "You are planning an ad schedule for a Google Ads Demand Gen campaign targeting Russia (GMT+3). "
            "Provide the best daily schedule based on the business context. "
            "Return ONLY JSON with keys: entries (list of objects with day, start, end in 24h HH:MM), "
            "and reason (<=160 chars). Use English day names (monday..sunday)."
        ),
        "context": {
            "business_name": (business_name or "").strip(),
            "usp": (usp or "").strip(),
            "site_url": (site_url or "").strip(),
            "locations": list(locations or []) if locations else [],
            "languages": list(languages or []) if languages else [],
            "target_timezone": "GMT+3",
        },
        "guidelines": [
            "Provide between 3 and 14 total intervals covering days with expected demand.",
            "Prefer working hours in Moscow time but adapt to business specifics.",
            "Avoid intervals shorter than 1 hour.",
        ],
        "output_schema": {
            "entries": [
                {"day": "string", "start": "HH:MM", "end": "HH:MM"}
            ],
            "reason": "string",
        },
    }

    try:
        client = GeminiClient(model=os.getenv("GEMINI_MODEL", "models/gemini-2.0-flash"), temperature=0.2, retries=1, fallback_model=None)  # type: ignore
        raw = client.generate_json(json.dumps(payload, ensure_ascii=False))
    except Exception as exc:  # pragma: no cover
        logger.warning("LLM schedule decision failed: %s — fallback schedule used.", exc)
        return fallback

    if not isinstance(raw, dict):
        return fallback

    raw_entries: List[Dict[str, str]] = []
    for item in raw.get("entries", []):
        if not isinstance(item, dict):
            continue
        day = str(item.get("day") or "").strip()
        start = str(item.get("start") or "").strip()
        end = str(item.get("end") or "").strip()
        if not (day and start and end):
            continue
        raw_entries.append({"day": day, "start": start, "end": end})

    normalized = _normalize_schedule_entries(raw_entries)
    if not normalized:
        return fallback

    reason = str(raw.get("reason") or "").strip() or "LLM schedule decision."
    return {
        "source": "llm",
        "reason": reason,
        "entries": normalized,
        "target_entries": raw_entries,
    }


def _merge_and_prepare_segments(segments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[int, List[Tuple[int, int]]] = {}
    for seg in segments:
        day_idx = seg["day_index"]
        start = max(0, min(int(seg["start"]), 24 * 60))
        end = max(0, min(int(seg["end"]), 24 * 60))
        if end <= start:
            continue
        grouped.setdefault(day_idx, []).append((start, end))

    results: List[Dict[str, Any]] = []
    for day_idx in sorted(grouped):
        intervals = sorted(grouped[day_idx], key=lambda pair: pair[0])
        merged: List[List[int]] = []
        for start, end in intervals:
            if not merged or start > merged[-1][1]:
                merged.append([start, end])
            else:
                merged[-1][1] = max(merged[-1][1], end)
        for start, end in merged:
            results.append(
                {
                    "day_index": day_idx,
                    "start": start,
                    "end": end,
                    "day_label": DAY_INDEX_TO_LABEL[day_idx],
                    "start_text": _format_minutes_12h(start, is_end=False),
                    "end_text": _format_minutes_12h(end, is_end=True),
                }
            )
    results.sort(key=lambda item: (item["day_index"], item["start"]))
    return results


def _convert_schedule_to_account_entries(
    normalized_entries: Sequence[Dict[str, Any]],
    delta_minutes: int,
) -> List[Dict[str, Any]]:
    if not normalized_entries:
        return []

    segments: List[Dict[str, Any]] = []
    for entry in normalized_entries:
        day_idx = entry.get("day_index")
        intervals = entry.get("intervals") or []
        if day_idx is None:
            continue
        for interval in intervals:
            start_min = int(interval.get("start", 0))
            end_min = int(interval.get("end", 0))
            duration = end_min - start_min
            if duration <= 0:
                continue
            if duration >= 24 * 60:
                segments.append({"day_index": day_idx, "start": 0, "end": 24 * 60})
                continue

            account_start = (start_min - delta_minutes) % (24 * 60)
            account_end = (end_min - delta_minutes) % (24 * 60)

            if account_start < account_end:
                segments.append(
                    {"day_index": day_idx, "start": account_start, "end": account_end}
                )
            else:
                segments.append(
                    {"day_index": day_idx, "start": account_start, "end": 24 * 60}
                )
                if account_end > 0:
                    segments.append(
                        {
                            "day_index": (day_idx + 1) % len(DAY_INDEX_TO_LABEL),
                            "start": 0,
                            "end": account_end,
                        }
                    )

    return _merge_and_prepare_segments(segments)


def _get_schedule_row_count(driver: WebDriver) -> int:
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
    _maybe_handle_confirm_its_you(driver, emit=None)
    try:
        return bool(
            driver.execute_script(
                """
                const buttons = [...document.querySelectorAll('ad-schedule-editor material-button')];
                for (const btn of buttons) {
                    const text = (btn.textContent || '').trim().toLowerCase();
                    if (text === 'add') {
                        try { btn.click(); return true; } catch (e) {
                            btn.dispatchEvent(new MouseEvent('click', {bubbles:true,cancelable:true,view:window}));
                            return true;
                        }
                    }
                }
                return false;
                """
            )
        )
    except Exception as exc:  # pragma: no cover
        logger.debug("Add row click failed: %s", exc)
        return False


def _click_remove_row(driver: WebDriver) -> bool:
    _maybe_handle_confirm_its_you(driver, emit=None)
    try:
        return bool(
            driver.execute_script(
                """
                const rows = [...document.querySelectorAll('ad-schedule-editor-row .row')];
                for (let i = rows.length - 1; i >= 0; i--) {
                    const remove = rows[i].querySelector('.remove-icon:not(.is-disabled)');
                    if (remove) {
                        try { remove.click(); return true; } catch (e) {
                            remove.dispatchEvent(new MouseEvent('click', {bubbles:true,cancelable:true,view:window}));
                            return true;
                        }
                    }
                }
                return false;
                """
            )
        )
    except Exception as exc:  # pragma: no cover
        logger.debug("Remove row click failed: %s", exc)
        return False


def _sync_row_count(driver: WebDriver, target: int, *, timeout: float = 6.0) -> int:
    target = max(1, int(target))
    deadline = time.time() + max(0.5, timeout)
    logger.info('DemandGen Step7: _sync_row_count start target=%d current=%d', target, _get_schedule_row_count(driver))
    while time.time() < deadline:
        count = _get_schedule_row_count(driver)
        if count == target:
            logger.info('DemandGen Step7: _sync_row_count reached target=%d', count)
            return count
        if count < target:
            logger.debug('DemandGen Step7: _sync_row_count adding row (have %d)', count)
            if not _click_add_row(driver):
                logger.warning('DemandGen Step7: add row click failed at count=%d', count)
                break
            time.sleep(0.12)
            continue
        if count > target:
            logger.debug('DemandGen Step7: _sync_row_count removing row (have %d)', count)
            if not _click_remove_row(driver):
                logger.warning('DemandGen Step7: remove row click failed at count=%d', count)
                break
            time.sleep(0.12)
            continue
    final = _get_schedule_row_count(driver)
    logger.info('DemandGen Step7: _sync_row_count end -> %d (target=%d)', final, target)
    return final

def _current_day_label(driver: WebDriver, row_index: int) -> str:
    try:
        value = driver.execute_script(
            """
            const rows = [...document.querySelectorAll('ad-schedule-editor-row .row')];
            const row = rows[arguments[0]];
            if (!row) return '';
            const btn = row.querySelector('material-dropdown-select .button');
            if (!btn) return '';
            return (btn.textContent || btn.innerText || '').replace(/\\s+/g,' ').trim().toLowerCase();
            """,
            row_index,
        )
        return str(value or '').strip().lower()
    except Exception:
        return ''

def _select_day_in_row(driver: WebDriver, row_index: int, day_label: str) -> bool:
    label = day_label.replace(" ", " ").strip().lower()
    try:
        return bool(
            driver.execute_script(
                """
                const index = arguments[0];
                const label = String(arguments[1] || '').toLowerCase();
                const rows = [...document.querySelectorAll('ad-schedule-editor-row .row')];
                const row = rows[index];
                if (!row) return false;
                const dropdown = row.querySelector('material-dropdown-select');
                if (!dropdown) return false;
                const normalize = (text) => String(text || '').replace(/\\s+/g,' ').trim().toLowerCase();
                const pick = (root) => {
                    if (!root) return false;
                    const items = [...root.querySelectorAll('material-select-dropdown-item')];
                    for (const item of items) {
                        if (normalize(item.textContent || item.innerText) === label) {
                            try { item.click(); } catch (e) {
                                item.dispatchEvent(new MouseEvent('click', {bubbles:true,cancelable:true,view:window}));
                            }
                            return true;
                        }
                    }
                    return false;
                };
                if (pick(dropdown)) return true;
                const button = dropdown.querySelector('.button');
                if (button) {
                    try { button.click(); } catch (err) {
                        button.dispatchEvent(new MouseEvent('click', {bubbles:true,cancelable:true,view:window}));
                    }
                    if (pick(document.body)) return true;
                }
                return false;
                """,
                row_index,
                label,
            )
        )
    except Exception as exc:  # pragma: no cover
        logger.debug("Select day failed (row %d): %s", row_index, exc)
        return False


def _set_time_field(driver: WebDriver, row_index: int, css_selector: str, value: str) -> bool:
    try:
        return bool(
            driver.execute_script(
                """
                const rows = [...document.querySelectorAll('ad-schedule-editor-row .row')];
                const row = rows[arguments[0]];
                if (!row) return false;
                const input = row.querySelector(arguments[1]);
                if (!input) return false;
                input.focus();
                input.value = arguments[2];
                input.setAttribute('value', arguments[2]);
                input.dispatchEvent(new Event('input', {bubbles:true}));
                input.dispatchEvent(new Event('change', {bubbles:true}));
                input.blur();
                return true;
                """,
                row_index,
                css_selector,
                value,
            )
        )
    except Exception as exc:  # pragma: no cover
        logger.debug("Time field set failed (row %d, %s): %s", row_index, css_selector, exc)
        return False


def _set_times_in_row(driver: WebDriver, row_index: int, start_text: str, end_text: str) -> bool:
    ok_start = _set_time_field(driver, row_index, ".start-time input", start_text)
    ok_end = _set_time_field(driver, row_index, ".end-time input", end_text)
    return bool(ok_start and ok_end)


def _read_schedule_summary(driver: WebDriver) -> str:
    try:
        summary = driver.execute_script(
            """
            const summaryNode = document.querySelector('div.summary-host .summary');
            if (!summaryNode) return '';
            return (summaryNode.textContent || summaryNode.innerText || '').replace(/\\s+/g,' ').trim();
            """
        )
        return str(summary or "").strip()
    except Exception:
        return ""


def run_step7(
    driver: WebDriver,
    *,
    business_name: Optional[str] = None,
    site_url: Optional[str] = None,
    usp: Optional[str] = None,
    locations: Optional[Iterable[str]] = None,
    languages: Optional[Iterable[str]] = None,
    timeout_total: float = 120.0,
    emit: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    logger.info('DemandGen Step7 start | business=%s site=%s usp=%s', business_name, site_url, (usp or '')[:80])
    if locations: logger.info('DemandGen Step7 locations=%s', list(locations))
    if languages: logger.info('DemandGen Step7 languages=%s', list(languages))
    t0 = time.time()
    stage_ts = t0
    stage_log: List[Tuple[str, float]] = []

    def _mark_stage(label: str) -> None:
        nonlocal stage_ts
        now = time.time()
        elapsed_stage = (now - stage_ts) * 1000.0
        elapsed_total = (now - t0) * 1000.0
        
        logger.info('DemandGen Step7 stage %s | %.1f ms (total %.1f ms) entries=%s', label, elapsed_stage, elapsed_total, len(stage_log))
        stage_log.append((label, elapsed_stage))
        stage_ts = now

    _maybe_handle_confirm_its_you(driver, emit=emit)
    _mark_stage("initial_confirm")

    _emit(emit, "Открываю раздел расписания показов")
    _ensure_panel_open(driver, AD_SCHEDULE_SYNS)
    time.sleep(0.1)
    _mark_stage("open_schedule_panel")

    tz_label = _read_timezone_label(driver)
    account_offset = _parse_gmt_offset_minutes(tz_label)
    delta_minutes = TARGET_TZ_OFFSET_MIN - account_offset
    _emit(
        emit,
        f"Часовой пояс аккаунта: {tz_label or 'не указан'} (GMT offset {account_offset:+} мин)",
    )
    _mark_stage("timezone_read")

    decision = _decide_schedule_via_llm(
        business_name=business_name,
        usp=usp,
        site_url=site_url,
        locations=locations,
        languages=languages,
    )
    logger.info('DemandGen Step7: schedule decision raw=%s', decision)
    _mark_stage("schedule_decision")
    _emit(emit, f"Расписание от LLM: {decision.get('reason')}")

    account_entries = _convert_schedule_to_account_entries(decision.get("entries") or [], delta_minutes)
    logger.info('DemandGen Step7: converted account entries=%s', account_entries)
    _mark_stage("convert_entries")
    if not account_entries:
        _emit(emit, "Не удалось преобразовать расписание — шаг пропущен")
        elapsed = int((time.time() - t0) * 1000)
        summary_text = _read_schedule_summary(driver)
        _mark_stage("summary_read")
        if stage_log:
            breakdown = ", ".join(f"{name}={dur:.0f}ms" for name, dur in stage_log)
            logger.info("step7 DemandGen breakdown: %s", breakdown)
        return {
            "account_timezone_label": tz_label,
            "account_timezone_offset_min": account_offset,
            "target_timezone_offset_min": TARGET_TZ_OFFSET_MIN,
            "schedule_target": decision.get("target_entries", []),
            "schedule_account": [],
            "schedule_reason": decision.get("reason"),
            "schedule_source": decision.get("source"),
            "duration_ms": elapsed,
            "summary": summary_text,
            "timing_breakdown": [{"stage": name, "duration_ms": dur} for name, dur in stage_log],
        }

    logger.info('DemandGen Step7: syncing rows target=%d current=%d', len(account_entries), _get_schedule_row_count(driver))
    target_rows = len(account_entries)
    final_rows = _sync_row_count(driver, target_rows)
    logger.info('DemandGen Step7: rows synced result=%d', final_rows)
    if final_rows < target_rows:
        logger.warning('Unable to reach desired row count: have %s want %s', final_rows, target_rows)
    _mark_stage('rows_synced')

    applied_rows: List[Dict[str, Any]] = []
    for idx, entry in enumerate(account_entries):
        logger.info('DemandGen Step7: target row %d -> %s %s-%s', idx, entry['day_label'], entry['start_text'], entry['end_text'])
        if idx >= final_rows:
            logger.warning('DemandGen Step7: row %d exceeds synced rows (%d) — stopping', idx, final_rows)
            break
        current_label = _current_day_label(driver, idx)
        logger.info('DemandGen Step7: current row %d label=%s', idx, current_label)
        desired_label = entry['day_label'].strip().lower()
        if current_label != desired_label:
            logger.info('DemandGen Step7: switching row %d to %s', idx, entry['day_label'])
            day_ok = _select_day_in_row(driver, idx, entry['day_label'])
        else:
            day_ok = True
            logger.info('DemandGen Step7: row %d already on desired day', idx)
        if not day_ok:
            logger.warning('Failed to set day for row %d (%s)', idx, entry['day_label'])
        time_ok = _set_times_in_row(driver, idx, entry['start_text'], entry['end_text'])
        logger.info('DemandGen Step7: row %d time set result=%s', idx, time_ok)
        if not time_ok:
            logger.warning('Failed to set time for row %d (%s %s-%s)', idx, entry['day_label'], entry['start_text'], entry['end_text'])
        applied_rows.append({
            'row': idx,
            'day': entry['day_label'],
            'start': entry['start_text'],
            'end': entry['end_text'],
        })
        logger.info('DemandGen Step7: row %d applied', idx)
    _mark_stage('apply_schedule')
    summary_text = _read_schedule_summary(driver)
    _mark_stage("summary_read")
    elapsed = int((time.time() - t0) * 1000)
    if stage_log:
        breakdown = ", ".join(f"{name}={dur:.0f}ms" for name, dur in stage_log)
        logger.info("step7 DemandGen breakdown: %s", breakdown)
    logger.info(
        "step7 DemandGen completed (%d ms). TZ=%s | rows=%d",
        elapsed,
        tz_label,
        len(applied_rows),
    )
    _emit(emit, "Расписание показов сохранено")

    return {
        "account_timezone_label": tz_label,
        "account_timezone_offset_min": account_offset,
        "target_timezone_offset_min": TARGET_TZ_OFFSET_MIN,
        "delta_minutes": delta_minutes,
        "schedule_target": decision.get("target_entries", []),
        "schedule_account": applied_rows,
        "schedule_reason": decision.get("reason"),
        "schedule_source": decision.get("source"),
        "duration_ms": elapsed,
        "summary": summary_text,
        "timing_breakdown": [{"stage": name, "duration_ms": dur} for name, dur in stage_log],
    }
