# ads_ai/browser/driver.py
from __future__ import annotations

import base64
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from selenium.common.exceptions import (
    JavascriptException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.remote.webdriver import WebDriver

__all__ = [
    "set_timeouts",
    "set_implicit_wait",
    "set_window",
    "safe_get",
    "stop_loading",
    "hard_refresh",
    "current_url_safe",
    "is_alive",
    "supports_cdp",
    "configure_download_dir",
    "safe_quit",
    "DriverInfo",
    "get_info",
    "full_page_screenshot",
]

log = logging.getLogger(__name__)


# --- Таймауты -----------------------------------------------------------------


def set_timeouts(d: WebDriver, page_load_sec: int = 45, script_sec: int = 30) -> None:
    """
    Базовая установка таймаутов страницы и скриптов.
    (Сигнатуру не меняем — совместимость с существующим кодом.)
    """
    d.set_page_load_timeout(page_load_sec)
    d.set_script_timeout(script_sec)


def set_implicit_wait(d: WebDriver, seconds: float = 0.0) -> None:
    """Имплиситный таймаут поиска элементов (0 — выкл.)."""
    d.implicitly_wait(seconds)


# --- Окно/фокус ----------------------------------------------------------------


def set_window(
    d: WebDriver,
    width: Optional[int] = None,
    height: Optional[int] = None,
    x: Optional[int] = None,
    y: Optional[int] = None,
    maximize: bool = False,
) -> None:
    """
    Настройка геометрии окна. Аккуратно работает как в обычном режиме, так и под AdsPower/Remote.
    Если maximize=True — пытаемся развернуть, иначе ставим размеры/позицию.
    """
    try:
        if maximize:
            d.maximize_window()
            return
        if width is not None and height is not None:
            d.set_window_size(width, height)
        if x is not None and y is not None:
            d.set_window_position(x, y)
    except WebDriverException as e:
        # В некоторых контейнерах/виртуалках изменение окна недоступно — это не крит.
        log.debug("set_window ignored: %s", e)


# --- Навигация/зависания -------------------------------------------------------


def _ensure_ready_state_fallback(d: WebDriver, timeout: float = 10.0) -> None:
    """
    Простейший локальный поллер readyState, если нет нашей реализованной waits.ensure_ready_state.
    """
    deadline = time.time() + timeout
    last_state = None
    while time.time() < deadline:
        try:
            state = d.execute_script("return document.readyState")
            if state != last_state:
                last_state = state
            if state == "complete":
                return
        except WebDriverException:
            # В процессе навигации текущий документ может быть недоступен — продолжаем.
            pass
        time.sleep(0.05)  # микропаузу оставляем маленькой
    # По таймауту выходим молча — наверху разрулят ретраи/гарды.


def safe_get(
    d: WebDriver,
    url: str,
    timeout_sec: int = 60,
    ensure_ready: bool = True,
    stop_on_timeout: bool = True,
) -> bool:
    """
    Переход по URL с мягкой обработкой зависаний.
    - Ловим TimeoutException от загрузки.
    - По желанию останавливаем загрузку через window.stop().
    - Дополнительно дожидаемся readyState=complete (если ensure_ready=True).
    Возвращает True, если навигация прошла без таймаута драйвера.
    """
    log.info("nav.get url=%s timeout=%ss", url, timeout_sec)
    ok = True
    # Установим page load timeout на текущий вызов (без гарантии восстановления; это норм).
    try:
        d.set_page_load_timeout(timeout_sec)
    except WebDriverException:
        pass

    try:
        d.get(url)
    except TimeoutException:
        ok = False
        log.warning("nav.get timeout after %ss: %s", timeout_sec, url)
        if stop_on_timeout:
            try:
                stop_loading(d)
            except WebDriverException as e:
                log.debug("window.stop failed: %s", e)
    except WebDriverException as e:
        ok = False
        log.warning("nav.get webdriver error: %s", e)

    if ensure_ready:
        try:
            # Пытаемся использовать проектный waits.ensure_ready_state; при отсутствии — локальный fallback.
            try:
                from .waits import ensure_ready_state  # type: ignore
            except Exception:  # noqa: BLE001
                ensure_ready_state = _ensure_ready_state_fallback  # type: ignore
            ensure_ready_state(d, timeout=max(3.0, min(10.0, timeout_sec / 3)))
        except WebDriverException as e:
            log.debug("ensure_ready_state skipped: %s", e)
    return ok


def stop_loading(d: WebDriver) -> None:
    """Остановить загрузку текущей страницы (полезно при зависаниях)."""
    d.execute_script("window.stop();")


def hard_refresh(d: WebDriver, ensure_ready: bool = True) -> None:
    """Жёсткое обновление страницы через JS (мимо кэша решается на уровне сервера)."""
    d.execute_script("location.reload()")
    if ensure_ready:
        try:
            try:
                from .waits import ensure_ready_state  # type: ignore
            except Exception:  # noqa: BLE001
                ensure_ready_state = _ensure_ready_state_fallback  # type: ignore
            ensure_ready_state(d, timeout=10.0)
        except WebDriverException:
            pass


def current_url_safe(d: WebDriver) -> str:
    """Безопасно получить текущий URL (не падаем на навигационных гонках)."""
    try:
        return d.current_url
    except WebDriverException:
        return ""


def is_alive(d: WebDriver) -> bool:
    """Проверка, что сессия жива (ping JS)."""
    try:
        return bool(d.execute_script("return true"))
    except WebDriverException:
        return False


# --- CDP / загрузки ------------------------------------------------------------


def supports_cdp(d: WebDriver) -> bool:
    """Есть ли у драйвера поддержка CDP (Chromium)?"""
    return hasattr(d, "execute_cdp_cmd")


def _cdp(d: WebDriver, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """Тонкая обёртка над execute_cdp_cmd с логом и безопасностью."""
    if not supports_cdp(d):
        raise WebDriverException("CDP not supported by this driver")
    log.debug("CDP %s %s", method, params)
    # type: ignore[attr-defined]
    return d.execute_cdp_cmd(method, params)  # pyright: ignore[reportAttributeAccessIssue]


# --- Full-page screenshot ---------------------------------------------------

def full_page_screenshot(d: WebDriver, *, img_format: str = "png", quality: Optional[int] = None) -> bytes:
    """
    Capture full-page screenshot via CDP. Returns PNG/JPEG bytes.

    Strategy:
      1) Try Page.captureScreenshot with captureBeyondViewport=True.
      2) Fallback: Page.getLayoutMetrics + Emulation.setDeviceMetricsOverride + clip.
      3) Final fallback: driver.get_screenshot_as_png() (viewport only).
    """
    # Use public helper; previous name caused NameError in some flows
    if not supports_cdp(d):  # viewport fallback
        try:
            return d.get_screenshot_as_png()
        except Exception:
            return b""

    # Attempt 1: captureBeyondViewport (modern Chrome)
    try:
        params: Dict[str, Any] = {"format": img_format, "fromSurface": True, "captureBeyondViewport": True}
        if img_format.lower() == "jpeg" and quality is not None:
            try:
                params["quality"] = int(quality)
            except Exception:
                pass
        res = _cdp(d, "Page.captureScreenshot", params)
        data = res.get("data") if isinstance(res, dict) else None
        if data:
            return base64.b64decode(data)
    except WebDriverException:
        pass
    except Exception:
        pass

    # Attempt 2: override device metrics to content size and capture with clip
    orig_overridden = False
    try:
        # Read content size
        width = height = None
        try:
            lm = _cdp(d, "Page.getLayoutMetrics", {})
            cs = (lm.get("contentSize") or lm.get("cssContentSize") or {})
            width = int(float(cs.get("width") or 0))
            height = int(float(cs.get("height") or 0))
        except Exception:
            pass
        if not width or not height:
            try:
                js = "return {w:document.documentElement.scrollWidth||0,h:document.documentElement.scrollHeight||0};"
                sz = d.execute_script(js) or {}
                width = int(float(sz.get("w") or 0))
                height = int(float(sz.get("h") or 0))
            except Exception:
                width = height = None
        if not width or not height:
            return d.get_screenshot_as_png()

        _cdp(
            d,
            "Emulation.setDeviceMetricsOverride",
            {"width": int(width), "height": int(height), "deviceScaleFactor": 1, "mobile": False},
        )
        orig_overridden = True

        res = _cdp(
            d,
            "Page.captureScreenshot",
            {
                "format": img_format,
                **({"quality": int(quality)} if (img_format.lower() == "jpeg" and quality is not None) else {}),
                "fromSurface": True,
                "clip": {"x": 0, "y": 0, "width": float(width), "height": float(height), "scale": 1},
            },
        )
        data = res.get("data") if isinstance(res, dict) else None
        if data:
            return base64.b64decode(data)
    except WebDriverException:
        pass
    except Exception:
        pass
    finally:
        if orig_overridden:
            try:
                _cdp(d, "Emulation.clearDeviceMetricsOverride", {})
            except Exception:
                pass

    # Final fallback
    try:
        return d.get_screenshot_as_png()
    except Exception:
        return b""


def configure_download_dir(d: WebDriver, directory: Path | str) -> bool:
    """
    Настроить каталог скачивания файлов для Chromium через CDP.
    Работает в т.ч. в headless (если версия драйвера поддерживает).
    Возвращает True при успешной установке.
    """
    path = Path(directory)
    path.mkdir(parents=True, exist_ok=True)
    try:
        # В некоторых версиях — Page.setDownloadBehavior, в других — Browser.setDownloadBehavior.
        # Пробуем оба.
        try:
            _cdp(
                d,
                "Page.setDownloadBehavior",
                {"behavior": "allow", "downloadPath": str(path)},
            )
            return True
        except WebDriverException:
            pass
        _cdp(
            d,
            "Browser.setDownloadBehavior",
            {"behavior": "allow", "downloadPath": str(path)},
        )
        return True
    except WebDriverException as e:
        log.debug("configure_download_dir failed: %s", e)
        return False


# --- Сеанс/завершение ----------------------------------------------------------


def safe_quit(d: Optional[WebDriver]) -> None:
    """Мягкое завершение сессии без падений тестов/рантайма."""
    if d is None:
        return
    try:
        d.quit()
    except WebDriverException as e:
        log.debug("driver.quit ignored: %s", e)


# --- Информация о драйвере -----------------------------------------------------


@dataclass(frozen=True)
class DriverInfo:
    name: str
    version: str
    platform: str
    user_agent: str
    headless: bool
    is_chromium: bool


def get_info(d: WebDriver) -> DriverInfo:
    """
    Собираем консистентную сводку о браузере: имя/версия/платформа/UA/headless.
    Полезно для трейсинга и диагностики.
    """
    caps: Dict[str, Any] = {}
    try:
        caps = dict(d.capabilities or {})
    except WebDriverException:
        pass

    name = str(caps.get("browserName", "") or "")
    version = str(caps.get("browserVersion", "") or caps.get("version", "") or "")
    platform = str(caps.get("platformName", "") or caps.get("platform", "") or "")

    # Headless признак пробуем вытащить из chromeOptions/args, а если не удалось — проверим по JS.
    headless = False
    try:
        chrome_opts = caps.get("goog:chromeOptions") or {}
        args = chrome_opts.get("args") or []
        headless = any("--headless" in str(a) for a in args)
    except Exception:  # noqa: BLE001
        headless = False

    # Chromium-детектируем через cap'ы.
    is_chrom = name.lower() in {"chrome", "chromium", "msedge", "edge"} or "chrome" in caps

    # UA получаем через JS (иногда в cap'ах неточный).
    ua = ""
    try:
        ua = str(d.execute_script("return navigator.userAgent || ''") or "")
    except (JavascriptException, WebDriverException):
        pass

    return DriverInfo(
        name=name,
        version=version,
        platform=platform,
        user_agent=ua,
        headless=headless,
        is_chromium=is_chrom,
    )
