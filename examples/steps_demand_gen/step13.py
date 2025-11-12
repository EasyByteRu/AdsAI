# -*- coding: utf-8 -*-
"""
examples/steps_demand_gen/step13.py

Шаг 13 (Demand Gen) — загрузка логотипов в UI Google Ads.

Функциональность:
- Открытие блока Media (если свёрнут)
- Удаление существующих логотипов (если есть)
- Нажатие кнопки Add в блоке Logos
- Переход на вкладку Upload в модальном окне
- Загрузка файлов логотипов через file input
- Ожидание активации и нажатие кнопки Save
- Ожидание закрытия модального окна

Использование:
    from examples.steps_demand_gen.step13 import run_step13

    run_step13(
        driver=driver,
        logo_paths=["/path/to/logo1.png", "/path/to/logo2.jpg"],
        logger=logger
    )
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
)
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# Импортируем функции генерации из step12
try:
    from examples.steps_demand_gen import step12
    _llm_generate_logo_prompts = step12._llm_generate_logo_prompts
    _generate_images_via_runware = step12._generate_images_via_runware
    _ensure_storage_dir = step12._ensure_storage_dir
    _dedupe_files = step12._dedupe_files
    RUNWARE_LOGO_SIZE = step12.RUNWARE_LOGO_SIZE
except ImportError:
    # Fallback если импорт не удался
    _llm_generate_logo_prompts = None
    _generate_images_via_runware = None
    _ensure_storage_dir = None
    _dedupe_files = None
    RUNWARE_LOGO_SIZE = 768


# --------------------------------------------------------------------------------------
#                                    ИСКЛЮЧЕНИЯ
# --------------------------------------------------------------------------------------


class LogoUploadError(Exception):
    """Базовое исключение для ошибок загрузки логотипов."""
    pass


class MediaPanelNotFoundError(LogoUploadError):
    """Блок Media не найден в интерфейсе."""
    pass


class LogoAddButtonNotFoundError(LogoUploadError):
    """Кнопка Add для логотипов не найдена."""
    pass


class UploadTabNotFoundError(LogoUploadError):
    """Вкладка Upload не найдена в модальном окне."""
    pass


class FileInputNotFoundError(LogoUploadError):
    """File input не найден для загрузки файлов."""
    pass


class SaveButtonTimeoutError(LogoUploadError):
    """Кнопка Save не активировалась в отведённое время."""
    pass


# --------------------------------------------------------------------------------------
#                                    КОНСТАНТЫ
# --------------------------------------------------------------------------------------

# Селекторы для основных элементов
MEDIA_PANEL_SELECTOR = 'expansion-panel[activityname="MediaOptions"]'
LOGO_PICKER_SELECTOR = 'multi-asset-picker[debugid="logo-picker"]'
LOGO_ADD_BUTTON_SELECTOR = 'multi-asset-picker[debugid="logo-picker"] material-button[debugid="add-asset"][aria-label="Add logos"]'

# Селекторы для модального окна
# Используем более специфичный селектор, чтобы избежать диалогов об ошибках
MODAL_SELECTOR = "slidealog-media-picker-wrapper"  # Основной контейнер медиа-пикера
MODAL_WRAPPER_SELECTOR = "slidealog-wrapper"  # Обёртка медиа-пикера
TAB_STRIP_SELECTOR = "div.navi-bar"
UPLOAD_TAB_SELECTOR = 'tab-button[aria-label="Upload"]'

# Селекторы для загрузки файлов
DROP_ZONE_SELECTOR = "drop-zone"
FILE_INPUT_SELECTOR = 'input[type="file"]'

# Селекторы для кнопки Save
SAVE_BUTTON_SELECTOR = 'material-button[data-test-id="confirm-button"]'
CANCEL_BUTTON_SELECTOR = "material-button.cancel-button"

# Таймауты (в секундах)
DEFAULT_TIMEOUT = 30
MODAL_OPEN_TIMEOUT = 10
TAB_SWITCH_TIMEOUT = 5
FILE_UPLOAD_TIMEOUT = 60
SAVE_ACTIVATION_TIMEOUT = 60

# Интервал проверки состояния (в секундах)
POLL_INTERVAL = 0.5

# Количество логотипов по умолчанию
DEFAULT_LOGO_COUNT = 2


# --------------------------------------------------------------------------------------
#                                    ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# --------------------------------------------------------------------------------------


def _wait_for_element(
    driver: WebDriver,
    selector: str,
    by: By = By.CSS_SELECTOR,
    timeout: float = DEFAULT_TIMEOUT,
    condition=EC.presence_of_element_located,
    description: str = "элемент",
    logger: Optional[logging.Logger] = None,
):
    """
    Ожидает появления элемента с указанным селектором.

    Args:
        driver: WebDriver instance
        selector: CSS селектор элемента
        by: Тип селектора (по умолчанию CSS_SELECTOR)
        timeout: Таймаут ожидания в секундах
        condition: Условие ожидания (по умолчанию presence_of_element_located)
        description: Описание элемента для логов
        logger: Logger instance

    Returns:
        Найденный WebElement

    Raises:
        TimeoutException: Если элемент не найден в течение timeout
    """
    if logger:
        logger.debug(f"Ожидание {description}: {selector}")

    try:
        element = WebDriverWait(driver, timeout, poll_frequency=POLL_INTERVAL).until(
            condition((by, selector))
        )
        if logger:
            logger.debug(f"✓ {description} найден")
        return element
    except TimeoutException:
        if logger:
            logger.error(f"✗ {description} не найден за {timeout}s: {selector}")
        raise


def _is_element_visible(element) -> bool:
    """
    Проверяет, видим ли элемент и доступен ли для взаимодействия.

    Args:
        element: WebElement для проверки

    Returns:
        True если элемент видим и доступен, False иначе
    """
    try:
        return (
            element.is_displayed()
            and element.is_enabled()
            and element.size.get("height", 0) > 0
            and element.size.get("width", 0) > 0
        )
    except (StaleElementReferenceException, NoSuchElementException):
        return False


def _safe_click(element, description: str = "элемент", logger: Optional[logging.Logger] = None):
    """
    Безопасный клик по элементу с логированием.

    Args:
        element: WebElement для клика
        description: Описание элемента для логов
        logger: Logger instance

    Raises:
        Exception: Если клик невозможен
    """
    try:
        if not _is_element_visible(element):
            raise Exception(f"{description} не видим или недоступен для клика")

        if logger:
            logger.debug(f"Клик по {description}")

        element.click()

        if logger:
            logger.debug(f"✓ Клик по {description} выполнен")

    except Exception as e:
        if logger:
            logger.error(f"✗ Не удалось кликнуть по {description}: {e}")
        raise


def _validate_file_paths(logo_paths: List[str], logger: Optional[logging.Logger] = None) -> List[str]:
    """
    Валидирует пути к файлам логотипов.

    Args:
        logo_paths: Список путей к файлам
        logger: Logger instance

    Returns:
        Список валидных путей

    Raises:
        LogoUploadError: Если нет валидных файлов
    """
    valid_paths = []

    for path_str in logo_paths:
        path = Path(path_str)

        if not path.exists():
            if logger:
                logger.warning(f"Файл не существует: {path}")
            continue

        if not path.is_file():
            if logger:
                logger.warning(f"Путь не является файлом: {path}")
            continue

        # Проверка расширения файла
        allowed_extensions = {".png", ".jpg", ".jpeg", ".gif", ".svg"}
        if path.suffix.lower() not in allowed_extensions:
            if logger:
                logger.warning(f"Неподдерживаемое расширение файла: {path}")
            continue

        valid_paths.append(str(path.absolute()))

    if not valid_paths:
        raise LogoUploadError("Нет валидных файлов логотипов для загрузки")

    if logger:
        logger.info(f"Валидировано {len(valid_paths)} файлов из {len(logo_paths)}")

    return valid_paths


# --------------------------------------------------------------------------------------
#                                    ОСНОВНЫЕ ФУНКЦИИ
# --------------------------------------------------------------------------------------


def _ensure_media_panel_open(driver: WebDriver, logger: Optional[logging.Logger] = None):
    """
    Проверяет наличие блока Media и блока Logos.

    Примечание: Не пытается разворачивать/сворачивать блок Media,
    так как он должен быть постоянно развёрнут на странице создания объявления.

    Args:
        driver: WebDriver instance
        logger: Logger instance

    Raises:
        MediaPanelNotFoundError: Если блок Media или блок Logos не найден
    """
    if logger:
        logger.info("Проверка наличия блока Media и блока Logos")

    try:
        # Проверяем наличие панели Media
        if logger:
            logger.debug(f"Поиск панели Media по селектору: {MEDIA_PANEL_SELECTOR}")

        _wait_for_element(
            driver,
            MEDIA_PANEL_SELECTOR,
            timeout=DEFAULT_TIMEOUT,
            description="блок Media",
            logger=logger,
        )

        if logger:
            logger.debug("✓ Блок Media найден")

        # Дожидаемся, что блок Logos видимый и доступен
        if logger:
            logger.debug(f"Ожидание появления блока Logos: {LOGO_PICKER_SELECTOR}")

        _wait_for_element(
            driver,
            LOGO_PICKER_SELECTOR,
            timeout=10,
            condition=EC.visibility_of_element_located,
            description="блок Logos",
            logger=logger,
        )

        if logger:
            logger.info("✓ Блок Media и блок Logos доступны")

    except TimeoutException as e:
        if logger:
            logger.error(f"Таймаут при поиске блока Media или Logos: {e}")
        raise MediaPanelNotFoundError("Блок Media или Logos не найден в интерфейсе")
    except Exception as e:
        if logger:
            logger.error(f"Ошибка при проверке блока Media: {e}")
        raise MediaPanelNotFoundError(f"Не удалось найти блок Media: {e}")


def _remove_existing_logos(driver: WebDriver, logger: Optional[logging.Logger] = None):
    """
    Удаляет все существующие логотипы из блока Logos.

    Находит все превью логотипов, наводит на них курсор (чтобы показать кнопку удаления)
    и кликает по кнопкам удаления (Cancel).

    Args:
        driver: WebDriver instance
        logger: Logger instance
    """
    if logger:
        logger.info("Проверка наличия и удаление существующих логотипов")

    try:
        # Находим блок logo-picker
        logo_picker = driver.find_element(By.CSS_SELECTOR, LOGO_PICKER_SELECTOR)

        # Ищем контейнер с превью логотипов
        try:
            preview_container = logo_picker.find_element(By.CSS_SELECTOR, "image-preview .preview-container")
        except NoSuchElementException:
            if logger:
                logger.info("✓ Нет существующих логотипов для удаления")
            return

        # Ищем все preview-item элементы (логотипы)
        preview_items = preview_container.find_elements(
            By.CSS_SELECTOR,
            "div.preview-item.cancellable"
        )

        if not preview_items:
            if logger:
                logger.info("✓ Нет существующих логотипов для удаления")
            return

        if logger:
            logger.info(f"Найдено {len(preview_items)} существующих логотипов, начинаем удаление")

        # Удаляем каждый логотип
        removed_count = 0
        for i in range(len(preview_items)):
            try:
                # Заново ищем preview items, так как DOM может измениться после удаления
                current_items = preview_container.find_elements(
                    By.CSS_SELECTOR,
                    "div.preview-item.cancellable"
                )

                if not current_items:
                    if logger:
                        logger.debug("Все логотипы удалены")
                    break

                # Берём первый элемент (так как после удаления индексы сдвигаются)
                item = current_items[0]

                if logger:
                    logger.debug(f"Удаление логотипа {i + 1}/{len(preview_items)}")

                # Наводим курсор на элемент, чтобы показать кнопку удаления
                try:
                    actions = ActionChains(driver)
                    actions.move_to_element(item).perform()

                    if logger:
                        logger.debug(f"  Навели курсор на логотип {i + 1}")

                    # Даём время на появление кнопки удаления (CSS transition)
                    time.sleep(0.3)
                except Exception as e:
                    if logger:
                        logger.debug(f"  Ошибка наведения курсора: {e}, пробуем найти кнопку напрямую")

                # Ищем кнопку удаления внутри preview-item
                try:
                    # Ждём появления кнопки удаления
                    cancel_button = WebDriverWait(item, 3, poll_frequency=0.1).until(
                        lambda el: el.find_element(
                            By.CSS_SELECTOR,
                            'material-button.cancel-button[aria-label="Cancel"]'
                        )
                    )
                except (NoSuchElementException, TimeoutException):
                    # Fallback: ищем любую кнопку с классом cancel-button
                    try:
                        cancel_button = item.find_element(
                            By.CSS_SELECTOR,
                            "material-button.cancel-button"
                        )
                    except NoSuchElementException:
                        if logger:
                            logger.warning(f"  Кнопка удаления не найдена для логотипа {i + 1}")
                        continue

                if logger:
                    logger.debug(f"  Найдена кнопка удаления для логотипа {i + 1}")

                # Проверяем, что кнопка видима
                if not cancel_button.is_displayed():
                    if logger:
                        logger.debug(f"  Кнопка удаления не видна, пробуем кликнуть через JavaScript")
                    # Альтернативный способ клика через JavaScript
                    driver.execute_script("arguments[0].click();", cancel_button)
                else:
                    # Обычный клик
                    cancel_button.click()

                if logger:
                    logger.debug(f"  Клик по кнопке удаления выполнен")

                # Ждём, пока элемент исчезнет из DOM
                def logo_removed(_driver):
                    try:
                        # Проверяем, что количество элементов уменьшилось
                        items = preview_container.find_elements(
                            By.CSS_SELECTOR,
                            "div.preview-item.cancellable"
                        )
                        return len(items) < len(current_items)
                    except (StaleElementReferenceException, NoSuchElementException):
                        return True

                WebDriverWait(driver, 5, poll_frequency=POLL_INTERVAL).until(logo_removed)

                removed_count += 1

                if logger:
                    logger.debug(f"✓ Логотип {i + 1} удалён")

                # Небольшая пауза между удалениями
                time.sleep(0.3)

            except (NoSuchElementException, TimeoutException) as e:
                if logger:
                    logger.warning(f"Не удалось удалить логотип {i + 1}: {e}")
                continue
            except StaleElementReferenceException:
                if logger:
                    logger.debug(f"Элемент {i + 1} стал stale, пропускаем")
                continue
            except Exception as e:
                if logger:
                    logger.warning(f"Неожиданная ошибка при удалении логотипа {i + 1}: {e}")
                continue

        if logger:
            logger.info(f"✓ Удалено {removed_count} логотипов")

        # Даём время на обновление UI после удаления всех логотипов
        time.sleep(0.5)

    except NoSuchElementException:
        if logger:
            logger.info("✓ Нет существующих логотипов для удаления")
    except Exception as e:
        if logger:
            logger.warning(f"Ошибка при удалении существующих логотипов: {e}")
        # Не прерываем выполнение, продолжаем с загрузкой новых логотипов


def _click_logo_add_button(driver: WebDriver, logger: Optional[logging.Logger] = None):
    """
    Находит и нажимает кнопку Add в блоке Logos.

    Args:
        driver: WebDriver instance
        logger: Logger instance

    Raises:
        LogoAddButtonNotFoundError: Если кнопка Add не найдена
    """
    if logger:
        logger.info("Поиск кнопки Add для логотипов")

    try:
        # Находим блок logo-picker (уже проверен в _ensure_media_panel_open)
        logo_picker = driver.find_element(By.CSS_SELECTOR, LOGO_PICKER_SELECTOR)

        if logger:
            logger.debug("Блок Logos найден, ищу кнопку Add внутри него")

        # Ищем кнопку Add внутри блока Logos с более надежным селектором
        # Используем поиск внутри logo_picker, чтобы гарантировать, что это именно Logos, а не Images
        add_button = None
        try:
            add_button = logo_picker.find_element(
                By.CSS_SELECTOR,
                'material-button[debugid="add-asset"][aria-label="Add logos"]'
            )
        except NoSuchElementException:
            # Fallback: ищем любую кнопку Add внутри logo-picker
            if logger:
                logger.debug("Кнопка с aria-label='Add logos' не найдена, пробую альтернативный селектор")
            add_button = logo_picker.find_element(
                By.CSS_SELECTOR,
                'material-button[debugid="add-asset"]'
            )

        if not add_button:
            raise LogoAddButtonNotFoundError("Кнопка Add не найдена в блоке Logos")

        # Дожидаемся, что кнопка кликабельна
        WebDriverWait(driver, 5).until(
            lambda d: add_button.is_displayed() and add_button.is_enabled()
        )

        if logger:
            logger.debug("✓ Кнопка Add найдена и готова к клику")

        # Кликаем
        _safe_click(add_button, "кнопка Add для логотипов", logger)

        if logger:
            logger.info("✓ Клик по кнопке Add выполнен")

        # Даём модальному окну 2 секунды на полную загрузку всех вкладок
        if logger:
            logger.debug("Ожидание загрузки модального окна (2 секунды)...")
        time.sleep(2)

    except (NoSuchElementException, TimeoutException) as e:
        if logger:
            logger.error(f"Кнопка Add для логотипов не найдена: {e}")
        raise LogoAddButtonNotFoundError("Кнопка Add для логотипов не найдена")
    except Exception as e:
        if logger:
            logger.error(f"Ошибка при нажатии кнопки Add: {e}")
        raise LogoAddButtonNotFoundError(f"Не удалось нажать кнопку Add: {e}")


def _wait_for_modal_and_switch_to_upload(driver: WebDriver, logger: Optional[logging.Logger] = None):
    """
    Переключается на вкладку Upload в модальном окне.

    Примечание: Модальное окно уже должно быть открыто после клика на Add.

    Args:
        driver: WebDriver instance
        logger: Logger instance

    Returns:
        WebElement модального окна (для последующего использования)

    Raises:
        UploadTabNotFoundError: Если вкладка Upload не найдена
    """
    if logger:
        logger.info("Поиск и переключение на вкладку Upload")

    try:
        # Проверяем наличие диалога об ошибке соединения
        if logger:
            logger.debug("Проверка на наличие диалога об ошибке соединения...")

        try:
            error_dialog = driver.find_element(By.CSS_SELECTOR, "material-dialog.heartbeat-dialog")
            if error_dialog and error_dialog.is_displayed():
                error_header = error_dialog.find_element(By.CSS_SELECTOR, "h2.heartbeat-dialog-header")
                error_text = error_header.text if error_header else "Unknown error"
                error_msg = (
                    f"❌ ОШИБКА: Обнаружен диалог об ошибке соединения!\n"
                    f"   Заголовок: '{error_text}'\n"
                    f"   Это не модальное окно загрузки логотипов.\n"
                    f"   Возможные причины:\n"
                    f"   1. Сессия Google Ads истекла\n"
                    f"   2. Потеря соединения с сервером\n"
                    f"   3. Требуется повторная авторизация\n"
                    f"   Решение: Перелогиньтесь в Google Ads и повторите попытку."
                )
                logger.error(error_msg)
                raise UploadTabNotFoundError(
                    f"Вместо окна загрузки открылся диалог об ошибке: '{error_text}'. "
                    "Проверьте соединение и авторизацию в Google Ads."
                )
        except NoSuchElementException:
            # Диалога об ошибке нет, это нормально
            pass
        except UploadTabNotFoundError:
            raise

        # Сначала проверяем наличие модального окна загрузки
        if logger:
            logger.debug(f"Проверка наличия модального окна: {MODAL_SELECTOR}")

        # Ищем все модальные окна медиа-пикера
        modal_wrappers = driver.find_elements(By.CSS_SELECTOR, MODAL_SELECTOR)

        if logger:
            logger.info(f"Найдено {len(modal_wrappers)} модальных окон медиа-пикера")

        # Ищем правильное окно для логотипов (не скрытое и с правильным заголовком)
        modal = None
        for i, wrapper in enumerate(modal_wrappers, 1):
            try:
                # Проверяем slidealog-wrapper внутри
                slidealog_wrapper = wrapper.find_element(By.CSS_SELECTOR, "slidealog-wrapper")

                # Проверяем, что окно не скрыто
                wrapper_classes = slidealog_wrapper.get_attribute("class") or ""
                is_hidden = "hidden" in wrapper_classes

                # Получаем aria-label для определения типа окна
                aria_label = slidealog_wrapper.get_attribute("aria-label") or ""

                if logger:
                    logger.info(f"  Окно {i}: aria-label='{aria_label}', hidden={is_hidden}")

                # Ищем окно для логотипов (не скрытое)
                if not is_hidden and "logo" in aria_label.lower():
                    if logger:
                        logger.info(f"✓ Найдено правильное окно для логотипов: '{aria_label}'")
                    modal = wrapper
                    break

            except NoSuchElementException:
                continue

        if not modal:
            # Fallback: берем первое видимое окно
            if logger:
                logger.warning("Не найдено окно с 'logos' в aria-label, использую первое видимое окно")

            for wrapper in modal_wrappers:
                try:
                    slidealog_wrapper = wrapper.find_element(By.CSS_SELECTOR, "slidealog-wrapper")
                    wrapper_classes = slidealog_wrapper.get_attribute("class") or ""
                    if "hidden" not in wrapper_classes:
                        modal = wrapper
                        if logger:
                            aria_label = slidealog_wrapper.get_attribute("aria-label") or ""
                            logger.info(f"Использую видимое окно: '{aria_label}'")
                        break
                except NoSuchElementException:
                    continue

        if not modal:
            raise UploadTabNotFoundError("Не найдено ни одного видимого модального окна загрузки")

        # Ищем вкладку Upload внутри модального окна
        if logger:
            logger.debug(f"Поиск вкладки Upload внутри модального окна: {UPLOAD_TAB_SELECTOR}")

        # Отобразим структуру модального окна для отладки
        if logger:
            try:
                modal_html = modal.get_attribute('outerHTML')
                logger.info("=" * 80)
                logger.info("ОТЛАДКА: Структура модального окна")
                logger.info("=" * 80)
                logger.info(f"HTML модального окна (первые 2000 символов):\n{modal_html[:2000]}")
                logger.info("=" * 80)
            except Exception as e:
                logger.debug(f"Не удалось получить HTML модального окна: {e}")

        # Ищем все возможные элементы вкладок
        possible_tab_selectors = [
            "tab-button",
            "material-tab",
            "[role='tab']",
            "button[role='tab']",
            ".tab",
            "div[class*='tab']",
            "button[class*='tab']"
        ]

        all_tabs_found = []
        if logger:
            logger.info("=" * 80)
            logger.info("ОТЛАДКА: Поиск всех возможных вкладок")
            logger.info("=" * 80)

        for selector in possible_tab_selectors:
            try:
                elements = modal.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    if logger:
                        logger.info(f"✓ Найдено {len(elements)} элементов по селектору '{selector}':")
                        for i, elem in enumerate(elements, 1):
                            try:
                                tag_name = elem.tag_name
                                aria_label = elem.get_attribute("aria-label") or ""
                                text = elem.text or ""
                                role = elem.get_attribute("role") or ""
                                classes = elem.get_attribute("class") or ""
                                logger.info(f"  [{i}] <{tag_name}> aria-label='{aria_label}' text='{text}' role='{role}' class='{classes}'")
                                all_tabs_found.append(elem)
                            except Exception as e:
                                logger.debug(f"  [{i}] Ошибка чтения атрибутов: {e}")
            except Exception as e:
                if logger:
                    logger.debug(f"Селектор '{selector}' не сработал: {e}")

        logger.info("=" * 80)

        # Ищем сначала все tab-button элементы
        tab_buttons = modal.find_elements(By.CSS_SELECTOR, "tab-button")
        if logger:
            logger.debug(f"Найдено {len(tab_buttons)} вкладок типа 'tab-button' в модальном окне")

        if len(tab_buttons) == 0:
            if logger:
                logger.warning("Не найдено ни одной вкладки типа 'tab-button' в модальном окне!")
                # Попробуем найти tab-strip
                try:
                    tab_strip = modal.find_element(By.CSS_SELECTOR, "material-tab-strip")
                    logger.debug(f"Найден material-tab-strip: {tab_strip.get_attribute('outerHTML')[:200]}")
                    tab_buttons = tab_strip.find_elements(By.CSS_SELECTOR, "tab-button")
                    logger.debug(f"Найдено {len(tab_buttons)} вкладок внутри material-tab-strip")
                except NoSuchElementException:
                    logger.error("material-tab-strip не найден в модальном окне")

                # Попробуем альтернативные селекторы
                logger.info("Пробую альтернативные селекторы для вкладок...")
                alternative_selectors = ["material-tab", "[role='tab']", "button[role='tab']"]
                for alt_selector in alternative_selectors:
                    try:
                        alt_tabs = modal.find_elements(By.CSS_SELECTOR, alt_selector)
                        if alt_tabs:
                            logger.info(f"✓ Найдено {len(alt_tabs)} вкладок по селектору '{alt_selector}'")
                            tab_buttons = alt_tabs
                            break
                    except Exception as e:
                        logger.debug(f"Селектор '{alt_selector}' не сработал: {e}")

        # Ищем нужную вкладку Upload
        upload_tab = None
        for tab in tab_buttons:
            try:
                aria_label = tab.get_attribute("aria-label")
                if logger:
                    logger.debug(f"  Вкладка: aria-label='{aria_label}'")
                if aria_label == "Upload":
                    upload_tab = tab
                    if logger:
                        logger.debug(f"✓ Найдена вкладка Upload")
                    break
            except StaleElementReferenceException:
                if logger:
                    logger.debug("  Пропуск stale элемента")
                continue

        if not upload_tab:
            # Fallback: ищем по полному селектору
            if logger:
                logger.debug("Пробую найти вкладку Upload по полному селектору...")
            try:
                upload_tab = modal.find_element(By.CSS_SELECTOR, UPLOAD_TAB_SELECTOR)
                if logger:
                    logger.debug("✓ Найдена вкладка Upload через fallback селектор")
            except NoSuchElementException:
                if logger:
                    logger.error(f"Вкладка Upload не найдена ни одним способом")

        if not upload_tab:
            raise UploadTabNotFoundError("Вкладка Upload не найдена в модальном окне")

        # Проверяем, активна ли уже вкладка Upload
        is_active = upload_tab.get_attribute("aria-selected") == "true"
        has_active_class = "active" in (upload_tab.get_attribute("class") or "")

        if logger:
            logger.debug(f"Вкладка Upload: aria-selected={upload_tab.get_attribute('aria-selected')}, has_active_class={has_active_class}")

        if not is_active and not has_active_class:
            if logger:
                logger.info("Переключение на вкладку Upload")

            _safe_click(upload_tab, "вкладка Upload", logger)

            # Ждём активации вкладки (проверяем внутри модального окна!)
            if logger:
                logger.debug("Ожидание активации вкладки Upload...")

            def is_upload_active(_driver):
                try:
                    # Ищем вкладку Upload внутри нашего модального окна
                    tabs = modal.find_elements(By.CSS_SELECTOR, "tab-button[aria-label='Upload']")
                    if not tabs:
                        return False

                    upload = tabs[0]
                    aria_selected = upload.get_attribute("aria-selected") == "true"
                    has_active = "active" in (upload.get_attribute("class") or "")

                    if logger and not (aria_selected or has_active):
                        logger.debug(f"Вкладка Upload пока не активна: aria-selected={aria_selected}, active={has_active}")

                    return aria_selected or has_active
                except (StaleElementReferenceException, NoSuchElementException):
                    return False

            WebDriverWait(driver, TAB_SWITCH_TIMEOUT, poll_frequency=POLL_INTERVAL).until(is_upload_active)

            if logger:
                logger.info("✓ Вкладка Upload активирована")

        else:
            if logger:
                logger.info("✓ Вкладка Upload уже активна")

        # Даём время на загрузку содержимого вкладки
        time.sleep(1)

        # Дожидаемся появления дроп-зоны (признак того, что вкладка загружена)
        # Ищем внутри модального окна, а не глобально!
        if logger:
            logger.debug("Ожидание загрузки дроп-зоны внутри модального окна...")

        def find_drop_zone(_driver):
            try:
                # Ищем drop-zone внутри модального окна
                drop_zones = modal.find_elements(By.CSS_SELECTOR, DROP_ZONE_SELECTOR)
                if drop_zones:
                    # Проверяем, что drop-zone видим
                    for dz in drop_zones:
                        if dz.is_displayed():
                            if logger:
                                logger.debug(f"✓ Найдена видимая drop-zone")
                            return dz

                # Альтернативный селектор: div.upload-tab
                upload_tabs = modal.find_elements(By.CSS_SELECTOR, "div.upload-tab")
                if upload_tabs and upload_tabs[0].is_displayed():
                    if logger:
                        logger.debug(f"✓ Найден контейнер upload-tab")
                    return upload_tabs[0]

                if logger:
                    logger.debug("drop-zone пока не найдена или не видима")
                return None
            except (StaleElementReferenceException, NoSuchElementException):
                return None

        WebDriverWait(driver, 10, poll_frequency=POLL_INTERVAL).until(find_drop_zone)

        if logger:
            logger.info("✓ Вкладка Upload полностью загружена")

        # Возвращаем модальное окно для использования в других функциях
        return modal

    except TimeoutException as e:
        if logger:
            logger.error(f"Таймаут при поиске вкладки Upload: {e}")
        raise UploadTabNotFoundError("Вкладка Upload не найдена")
    except Exception as e:
        if logger:
            logger.error(f"Ошибка при переключении на вкладку Upload: {e}")
        raise UploadTabNotFoundError(f"Не удалось переключиться на вкладку Upload: {e}")


def _upload_files_via_input(
    driver: WebDriver,
    file_paths: List[str],
    modal=None,
    logger: Optional[logging.Logger] = None,
):
    """
    Загружает файлы через file input в дроп-зоне.

    Args:
        driver: WebDriver instance
        file_paths: Список путей к файлам для загрузки
        modal: WebElement модального окна (для поиска внутри него)
        logger: Logger instance

    Raises:
        FileInputNotFoundError: Если file input не найден
    """
    if logger:
        logger.info(f"Загрузка {len(file_paths)} файлов через file input")

    try:
        # Определяем область поиска
        search_area = modal if modal is not None else driver

        # Ищем file input внутри модального окна или дроп-зоны
        # Обычно input скрыт, но доступен для send_keys
        if logger:
            logger.debug(f"Поиск file input по селектору: {FILE_INPUT_SELECTOR}")

        file_inputs = search_area.find_elements(By.CSS_SELECTOR, FILE_INPUT_SELECTOR)

        if logger:
            logger.debug(f"Найдено {len(file_inputs)} file input элементов")

        if not file_inputs:
            if logger:
                logger.error("File input не найден в модальном окне")
            raise FileInputNotFoundError("File input не найден в модальном окне")

        # Ищем file input с поддержкой multiple или загружаем по одному
        file_input = None
        supports_multiple = False

        for i, inp in enumerate(file_inputs):
            try:
                input_type = inp.get_attribute("type")
                has_multiple = inp.get_attribute("multiple") is not None

                if logger:
                    logger.debug(f"File input #{i+1}: type={input_type}, multiple={has_multiple}")

                # Даже если input скрыт, мы можем отправить в него путь
                if input_type == "file":
                    file_input = inp
                    supports_multiple = has_multiple
                    if logger:
                        logger.debug(f"Выбран file input #{i+1}, multiple={supports_multiple}")
                    # Предпочитаем input с multiple, если он есть
                    if supports_multiple:
                        break
            except StaleElementReferenceException:
                if logger:
                    logger.debug(f"File input #{i+1} стал stale, пропускаем")
                continue

        if not file_input:
            if logger:
                logger.error("Подходящий file input не найден среди найденных элементов")
            raise FileInputNotFoundError("Подходящий file input не найден")

        if logger:
            try:
                outer_html = file_input.get_attribute('outerHTML')
                logger.debug(f"File input найден: {outer_html[:200]}...")
            except:
                logger.debug("File input найден (не удалось получить outerHTML)")

        # Отправляем пути к файлам
        if supports_multiple and len(file_paths) > 1:
            # Selenium поддерживает отправку нескольких файлов через \n
            files_string = "\n".join(file_paths)

            if logger:
                logger.info(f"Отправка {len(file_paths)} файлов в input (multiple=true)")
                for i, path in enumerate(file_paths, 1):
                    logger.info(f"  Файл {i}/{len(file_paths)}: {Path(path).name}")

            file_input.send_keys(files_string)
        else:
            # Загружаем файлы по одному
            if logger:
                logger.info(f"Загрузка {len(file_paths)} файлов по одному (multiple=false)")

            for i, path in enumerate(file_paths, 1):
                if logger:
                    logger.info(f"  Загрузка файла {i}/{len(file_paths)}: {Path(path).name}")

                try:
                    # Для каждого файла находим свежий file input (может пересоздаваться)
                    if i > 1:
                        # После загрузки первого файла input может пересоздаться
                        time.sleep(0.5)
                        file_inputs = search_area.find_elements(By.CSS_SELECTOR, FILE_INPUT_SELECTOR)
                        if file_inputs:
                            # Ищем видимый или последний добавленный input
                            for inp in reversed(file_inputs):
                                if inp.get_attribute("type") == "file":
                                    file_input = inp
                                    break

                    file_input.send_keys(path)

                    if logger:
                        logger.debug(f"✓ Файл {i}/{len(file_paths)} отправлен")

                    # Даём время на обработку загрузки
                    time.sleep(0.5)

                except Exception as e:
                    if logger:
                        logger.error(f"✗ Ошибка загрузки файла {i}: {e}")
                    # Продолжаем с остальными файлами
                    continue

        if logger:
            logger.info("✓ Файлы отправлены в input, ожидание обработки загрузки")

    except FileInputNotFoundError:
        raise
    except Exception as e:
        if logger:
            logger.error(f"Неожиданная ошибка при загрузке файлов: {e}", exc_info=True)
        raise FileInputNotFoundError(f"Не удалось загрузить файлы через input: {e}")


def _wait_for_save_button_and_click(driver: WebDriver, modal=None, logger: Optional[logging.Logger] = None):
    """
    Ожидает активации кнопки Save и нажимает её.

    Args:
        driver: WebDriver instance
        modal: WebElement модального окна (для поиска внутри него)
        logger: Logger instance

    Raises:
        SaveButtonTimeoutError: Если кнопка Save не активировалась
    """
    if logger:
        logger.info("Ожидание активации кнопки Save")

    try:
        # Определяем область поиска
        search_area = modal if modal is not None else driver

        # Сначала ждём, пока загрузятся превью файлов (признак успешной загрузки)
        if logger:
            logger.debug("Ожидание появления превью загруженных файлов...")

        def files_preview_loaded(_driver):
            try:
                # Ищем превью внутри модального окна
                previews = search_area.find_elements(By.CSS_SELECTOR, "img.thumbnail, .file-preview, .upload-preview, canvas")
                if logger and len(previews) > 0:
                    logger.debug(f"Найдено {len(previews)} превью файлов")
                return len(previews) > 0
            except:
                return False

        try:
            WebDriverWait(driver, 10, poll_frequency=POLL_INTERVAL).until(files_preview_loaded)
            if logger:
                logger.debug("✓ Превью файлов загружены")
        except TimeoutException:
            if logger:
                logger.warning("Превью файлов не найдены, продолжаем ожидание активации Save")

        # Ждём появления кнопки Save внутри модального окна
        save_buttons = search_area.find_elements(By.CSS_SELECTOR, SAVE_BUTTON_SELECTOR)

        if logger:
            logger.debug(f"Найдено {len(save_buttons)} кнопок Save")

        if not save_buttons:
            raise SaveButtonTimeoutError("Кнопка Save не найдена в модальном окне")

        save_button = save_buttons[0]

        # Функция проверки, что кнопка активна
        def is_save_enabled(_driver):
            try:
                # Ищем кнопку Save внутри модального окна
                btns = search_area.find_elements(By.CSS_SELECTOR, SAVE_BUTTON_SELECTOR)
                if not btns:
                    return False

                btn = btns[0]

                # Проверяем различные признаки активности
                is_disabled_attr = btn.get_attribute("disabled") is not None
                is_aria_disabled = btn.get_attribute("aria-disabled") == "true"
                has_disabled_class = "is-disabled" in (btn.get_attribute("class") or "")

                is_enabled = not (is_disabled_attr or is_aria_disabled or has_disabled_class)

                if logger and not is_enabled:
                    logger.debug(f"Кнопка Save не активна: disabled={is_disabled_attr}, "
                               f"aria-disabled={is_aria_disabled}, has_disabled_class={has_disabled_class}")

                return is_enabled
            except (StaleElementReferenceException, NoSuchElementException):
                return False

        # Ждём активации кнопки
        if logger:
            logger.info(f"Ожидание активации Save (таймаут {SAVE_ACTIVATION_TIMEOUT}s)")

        start_time = time.time()
        WebDriverWait(driver, SAVE_ACTIVATION_TIMEOUT, poll_frequency=POLL_INTERVAL).until(is_save_enabled)

        elapsed = time.time() - start_time
        if logger:
            logger.info(f"✓ Кнопка Save активировалась за {elapsed:.1f}s")

        # Получаем свежую ссылку на кнопку и кликаем
        save_buttons = search_area.find_elements(By.CSS_SELECTOR, SAVE_BUTTON_SELECTOR)
        if save_buttons:
            save_button = save_buttons[0]
        else:
            raise SaveButtonTimeoutError("Кнопка Save исчезла после активации")

        _safe_click(save_button, "кнопка Save", logger)

        if logger:
            logger.debug("Ожидание закрытия модального окна...")

    except TimeoutException:
        if logger:
            logger.error("✗ Кнопка Save не активировалась в течение отведённого времени")

            # Пытаемся получить текущее состояние кнопки для диагностики
            try:
                btns = search_area.find_elements(By.CSS_SELECTOR, SAVE_BUTTON_SELECTOR)
                if btns:
                    btn = btns[0]
                    logger.error(f"Состояние кнопки Save внутри модального окна: "
                               f"disabled={btn.get_attribute('disabled')}, "
                               f"aria-disabled={btn.get_attribute('aria-disabled')}, "
                               f"class={btn.get_attribute('class')}")
                else:
                    logger.error("Кнопка Save не найдена в модальном окне")
            except:
                pass

        raise SaveButtonTimeoutError(
            f"Кнопка Save не активировалась за {SAVE_ACTIVATION_TIMEOUT}s. "
            "Возможно, файлы не загрузились или произошла ошибка при загрузке."
        )
    except Exception as e:
        raise SaveButtonTimeoutError(f"Не удалось нажать кнопку Save: {e}")


def _wait_for_modal_close(driver: WebDriver, logger: Optional[logging.Logger] = None):
    """
    Ожидает закрытия модального окна после нажатия Save.

    Args:
        driver: WebDriver instance
        logger: Logger instance
    """
    if logger:
        logger.info("Ожидание закрытия модального окна")

    try:
        # Ждём, пока модалка исчезнет (проверяем по slidealog с классом mat-drawer-expanded)
        WebDriverWait(driver, 10, poll_frequency=POLL_INTERVAL).until(
            EC.invisibility_of_element_located((By.CSS_SELECTOR, "slidealog material-drawer.mat-drawer-expanded"))
        )

        if logger:
            logger.info("✓ Модальное окно закрыто")

    except TimeoutException:
        if logger:
            logger.warning("Модальное окно не закрылось в течение 10s, но продолжаем")


def _verify_logos_uploaded(driver: WebDriver, expected_count: int, logger: Optional[logging.Logger] = None):
    """
    Проверяет, что логотипы действительно загружены (опционально).

    Args:
        driver: WebDriver instance
        expected_count: Ожидаемое количество логотипов
        logger: Logger instance
    """
    if logger:
        logger.info("Проверка загруженных логотипов")

    try:
        # Дожидаемся появления блока logo-picker
        logo_picker = _wait_for_element(
            driver,
            LOGO_PICKER_SELECTOR,
            timeout=5,
            condition=EC.presence_of_element_located,
            description="блок Logos для проверки",
            logger=logger,
        )

        # Даём время на отрисовку превью и ждём появления изображений
        def logos_loaded(d):
            try:
                picker = d.find_element(By.CSS_SELECTOR, LOGO_PICKER_SELECTOR)
                previews = picker.find_elements(By.CSS_SELECTOR, "img, .asset-preview, .preview-image, canvas")
                return len(previews) >= expected_count
            except:
                return False

        try:
            WebDriverWait(driver, 10, poll_frequency=POLL_INTERVAL).until(logos_loaded)
        except TimeoutException:
            if logger:
                logger.debug("Не все превью загрузились за отведённое время, проверяю текущее состояние")

        # Ищем превью изображений (различные возможные селекторы)
        previews = logo_picker.find_elements(By.CSS_SELECTOR, "img, .asset-preview, .preview-image, canvas")

        if logger:
            logger.info(f"✓ Найдено {len(previews)} превью логотипов (ожидалось {expected_count})")

        if len(previews) < expected_count:
            if logger:
                logger.warning(f"Загружено меньше логотипов, чем ожидалось: {len(previews)} < {expected_count}")

    except Exception as e:
        if logger:
            logger.warning(f"Не удалось проверить загруженные логотипы: {e}")


# --------------------------------------------------------------------------------------
#                                    ГЛАВНАЯ ФУНКЦИЯ
# --------------------------------------------------------------------------------------


def run_step13(
    driver: WebDriver,
    logo_paths: Optional[List[str]] = None,
    business_name: Optional[str] = None,
    usp: Optional[str] = None,
    site_url: Optional[str] = None,
    storage_dir: Optional[str] = None,
    desired_logo_count: int = DEFAULT_LOGO_COUNT,
    mode: str = "ai_only",
    logger: Optional[logging.Logger] = None,
    verify_upload: bool = True,
) -> None:
    """
    Генерирует и загружает логотипы в блок Logos через UI Google Ads.

    Шаги выполнения:
    0. Генерация логотипов (если logo_paths не предоставлены):
       - Генерация промптов через LLM
       - Генерация изображений через Runware
    1. Валидация путей к файлам
    2. Проверка наличия блока Media и блока Logos (без разворачивания/сворачивания)
    2.5. Удаление существующих логотипов (если есть)
    3. Нажатие кнопки Add в блоке Logos (модальное окно открывается автоматически)
    4-5. Переключение на вкладку Upload в модальном окне
    6. Загрузка файлов через file input
    7. Ожидание активации кнопки Save и её нажатие
    8. Ожидание закрытия модального окна
    9. Проверка загруженных логотипов (опционально)

    Args:
        driver: Selenium WebDriver с авторизованной сессией Google Ads
        logo_paths: Список путей к файлам логотипов (локальные абсолютные пути).
                   Если не предоставлен, логотипы генерируются автоматически.
        business_name: Название бизнеса для генерации промптов
        usp: УТП/описание бизнеса для генерации промптов
        site_url: URL сайта для определения storage_dir
        storage_dir: Директория для сохранения сгенерированных файлов
        desired_logo_count: Количество логотипов для генерации (по умолчанию 2)
        mode: Режим работы ("ai_only", "manual", "inspired")
        logger: Logger для записи логов (опционально)
        verify_upload: Проверять ли загруженные логотипы после загрузки

    Raises:
        LogoUploadError: Базовое исключение при любых ошибках загрузки
        MediaPanelNotFoundError: Блок Media не найден
        LogoAddButtonNotFoundError: Кнопка Add не найдена
        UploadTabNotFoundError: Вкладка Upload не найдена
        FileInputNotFoundError: File input не найден
        SaveButtonTimeoutError: Кнопка Save не активировалась

    Example:
        >>> from selenium import webdriver
        >>> import logging
        >>>
        >>> logger = logging.getLogger(__name__)
        >>> driver = webdriver.Chrome()
        >>>
        >>> # ... авторизация и открытие страницы создания объявления ...
        >>>
        >>> run_step13(
        ...     driver=driver,
        ...     logo_paths=[
        ...         "/Users/user/logos/logo1.png",
        ...         "/Users/user/logos/logo2.jpg"
        ...     ],
        ...     logger=logger
        ... )
    """
    if logger is None:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        # Предотвращаем дублирование логов через родительские логгеры
        logger.propagate = False
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
            )
            logger.addHandler(handler)

    start_time = time.time()

    logger.info("=" * 80)
    logger.info("Начало шага 13: Загрузка логотипов")
    logger.info("=" * 80)

    # Определяем storage_dir
    if storage_dir is None and _ensure_storage_dir is not None:
        storage_dir = str(_ensure_storage_dir(business_name, site_url))
        logger.info(f"Storage directory: {storage_dir}")
    elif storage_dir is None:
        storage_dir = "/tmp/ads_ai_logos"
        logger.warning(f"Используется временная директория: {storage_dir}")

    # Генерируем логотипы, если не предоставлены
    if logo_paths is None or len(logo_paths) == 0:
        logger.info("Логотипы не предоставлены, начинаем генерацию")

        # Проверяем доступность функций генерации
        if _llm_generate_logo_prompts is None or _generate_images_via_runware is None:
            logger.warning("=" * 80)
            logger.warning("⚠ Шаг 13: функции генерации недоступны")
            logger.warning("⚠ Пропускаем загрузку логотипов (можно загрузить позже вручную)")
            logger.warning("=" * 80)
            return

        # Генерируем промпты для логотипов
        logger.info(f"Генерация {desired_logo_count} промптов для логотипов")
        try:
            logo_prompts = _llm_generate_logo_prompts(
                count=desired_logo_count,
                business_name=business_name,
                usp=usp,
                seed_notes=None
            )
            logger.info(f"✓ Сгенерировано {len(logo_prompts)} промптов")
            for i, prompt in enumerate(logo_prompts, 1):
                logger.debug(f"  Промпт {i}: {prompt[:100]}...")
        except Exception as e:
            logger.error(f"✗ Ошибка генерации промптов: {e}")
            logger.warning("Пропускаем загрузку логотипов")
            return

        # Генерируем логотипы через Runware
        logger.info(f"Генерация {len(logo_prompts)} логотипов через Runware")
        try:
            logo_paths = _generate_images_via_runware(
                prompts=logo_prompts,
                dest_dir=Path(storage_dir),
                label_prefix="demandgen-logo",
                width=RUNWARE_LOGO_SIZE,
                height=RUNWARE_LOGO_SIZE,
            )
            logger.info(f"✓ Сгенерировано {len(logo_paths)} логотипов")
        except Exception as e:
            logger.error(f"✗ Ошибка генерации логотипов через Runware: {e}")
            logger.warning("Пропускаем загрузку логотипов")
            return

        # Дедупликация файлов
        if _dedupe_files is not None:
            logo_paths = _dedupe_files(logo_paths)
            logger.info(f"После дедупликации: {len(logo_paths)} логотипов")

        # Проверяем, что получили файлы
        if not logo_paths:
            logger.warning("=" * 80)
            logger.warning("⚠ Шаг 13: не удалось сгенерировать логотипы")
            logger.warning("⚠ Пропускаем загрузку логотипов (можно загрузить позже вручную)")
            logger.warning("=" * 80)
            return

    try:
        # Шаг 1: Валидация файлов
        logger.info(f"Шаг 1/9: Валидация {len(logo_paths)} файлов")
        valid_paths = _validate_file_paths(logo_paths, logger)
        logger.info(f"✓ Валидировано {len(valid_paths)} файлов")

        # Шаг 2: Проверка блока Media и Logos
        logger.info("Шаг 2/9: Проверка блока Media и блока Logos")
        _ensure_media_panel_open(driver, logger)

        # Шаг 2.5: Удаление существующих логотипов (если есть)
        logger.info("Шаг 2.5/9: Удаление существующих логотипов")
        _remove_existing_logos(driver, logger)

        # Шаг 3: Нажатие кнопки Add для Logos
        logger.info("Шаг 3/9: Нажатие кнопки Add для логотипов")
        _click_logo_add_button(driver, logger)

        # Шаг 4-5: Переключение на вкладку Upload (модалка уже открыта после клика Add)
        logger.info("Шаг 4-5/9: Переключение на вкладку Upload")
        modal = _wait_for_modal_and_switch_to_upload(driver, logger)

        # Шаг 6: Загрузка файлов
        logger.info(f"Шаг 6/9: Загрузка {len(valid_paths)} файлов")
        _upload_files_via_input(driver, valid_paths, modal=modal, logger=logger)

        # Шаг 7: Ожидание активации Save и клик
        logger.info("Шаг 7/9: Ожидание активации и нажатие кнопки Save")
        _wait_for_save_button_and_click(driver, modal=modal, logger=logger)

        # Шаг 8: Ожидание закрытия модального окна
        logger.info("Шаг 8/9: Ожидание закрытия модального окна")
        _wait_for_modal_close(driver, logger)

        # Шаг 9: Проверка загруженных логотипов (опционально)
        if verify_upload:
            logger.info("Шаг 9/9: Проверка загруженных логотипов")
            _verify_logos_uploaded(driver, len(valid_paths), logger)

        elapsed = time.time() - start_time
        logger.info("=" * 80)
        logger.info(f"✓ Шаг 13 завершён успешно за {elapsed:.1f}s")
        logger.info(f"✓ Загружено логотипов: {len(valid_paths)}")
        logger.info("=" * 80)

    except LogoUploadError:
        elapsed = time.time() - start_time
        logger.error("=" * 80)
        logger.error(f"✗ Шаг 13 зав��ршился с ошибкой за {elapsed:.1f}s")
        logger.error("=" * 80)
        raise

    except Exception as e:
        elapsed = time.time() - start_time
        logger.error("=" * 80)
        logger.error(f"✗ Неожиданная ошибка на шаге 13 за {elapsed:.1f}s: {e}")
        logger.error("=" * 80)
        raise LogoUploadError(f"Неожиданная ошибка при загрузке логотипов: {e}") from e


# --------------------------------------------------------------------------------------
#                                    LEGACY COMPATIBILITY
# --------------------------------------------------------------------------------------


def run(driver: WebDriver, **kwargs) -> None:
    """
    Точка входа для обратной совместимости со старым интерфейсом.

    Args:
        driver: WebDriver instance
        **kwargs: Дополнительные параметры (logo_paths, logger и т.д.)
    """
    logo_paths = kwargs.get("logo_paths", [])
    logger = kwargs.get("logger")
    verify_upload = kwargs.get("verify_upload", True)

    run_step13(driver, logo_paths, logger, verify_upload)


# --------------------------------------------------------------------------------------
#                                    CLI ИНТЕРФЕЙС
# --------------------------------------------------------------------------------------


if __name__ == "__main__":
    import argparse
    import sys
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.chrome.service import Service as ChromeService

    # Настройка аргументов командной строки
    parser = argparse.ArgumentParser(
        description="Шаг 13: Загрузка логотипов в Google Ads",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

  # С автоматической генерацией логотипов:
  python step13.py --url "https://ads.google.com/..." --business "Моя компания" --usp "Лучший сервис"

  # С предоставленными логотипами:
  python step13.py --url "https://ads.google.com/..." --logos logo1.png logo2.jpg

  # С использованием существующей сессии (профиль Chrome):
  python step13.py --url "https://ads.google.com/..." --profile "/path/to/chrome/profile" --logos logo1.png

  # Указать путь к chromedriver:
  python step13.py --url "https://ads.google.com/..." --chromedriver "/path/to/chromedriver" --logos logo1.png
        """,
    )

    parser.add_argument(
        "--url",
        required=True,
        help="URL страницы Google Ads (страница создания объявления)",
    )

    parser.add_argument(
        "--logos",
        nargs="+",
        help="Пути к файлам логотипов (если не указаны - будут сгенерированы автоматически)",
    )

    parser.add_argument(
        "--business",
        help="Название бизнеса (для автогенерации логотипов)",
    )

    parser.add_argument(
        "--usp",
        help="УТП/описание бизнеса (для автогенерации логотипов)",
    )

    parser.add_argument(
        "--site-url",
        help="URL сайта (для определения директории хранения)",
    )

    parser.add_argument(
        "--storage-dir",
        help="Директория для сохранения сгенерированных файлов",
    )

    parser.add_argument(
        "--logo-count",
        type=int,
        default=DEFAULT_LOGO_COUNT,
        help=f"Количество логотипов для генерации (по умолчанию: {DEFAULT_LOGO_COUNT})",
    )

    parser.add_argument(
        "--profile",
        help="Путь к профилю Chrome (для использования существующей сессии)",
    )

    parser.add_argument(
        "--chromedriver",
        help="Путь к chromedriver (если не в PATH)",
    )

    parser.add_argument(
        "--headless",
        action="store_true",
        help="Запустить в headless режиме",
    )

    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="Не проверять загруженные логотипы после загрузки",
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Включить DEBUG логирование",
    )

    args = parser.parse_args()

    # Настройка логирования
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    logger = logging.getLogger(__name__)
    logger.propagate = False

    # Настройка Chrome опций
    chrome_options = ChromeOptions()

    if args.headless:
        chrome_options.add_argument("--headless")

    if args.profile:
        chrome_options.add_argument(f"user-data-dir={args.profile}")
        logger.info(f"Используется профиль Chrome: {args.profile}")

    # Общие полезные опции
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)

    # Инициализация драйвера
    try:
        if args.chromedriver:
            service = ChromeService(executable_path=args.chromedriver)
            driver = webdriver.Chrome(service=service, options=chrome_options)
            logger.info(f"Запущен ChromeDriver: {args.chromedriver}")
        else:
            driver = webdriver.Chrome(options=chrome_options)
            logger.info("Запущен ChromeDriver (из PATH)")

        # Переход на указанную страницу
        logger.info(f"Переход на страницу: {args.url}")
        driver.get(args.url)

        # Даём время на загрузку страницы
        logger.info("Ожидание загрузки страницы (5 секунд)...")
        time.sleep(5)

        # Запуск step13
        run_step13(
            driver=driver,
            logo_paths=args.logos,
            business_name=args.business,
            usp=args.usp,
            site_url=args.site_url,
            storage_dir=args.storage_dir,
            desired_logo_count=args.logo_count,
            logger=logger,
            verify_upload=not args.no_verify,
        )

        logger.info("✓ Скрипт выполнен успешно!")

        # Оставляем браузер открытым на 10 секунд для проверки результата
        logger.info("Браузер будет открыт ещё 10 секунд для проверки результата...")
        time.sleep(10)

    except KeyboardInterrupt:
        logger.warning("Прервано пользователем (Ctrl+C)")
        sys.exit(1)

    except Exception as e:
        logger.error(f"✗ Ошибка выполнения: {e}", exc_info=True)
        sys.exit(1)

    finally:
        if "driver" in locals():
            logger.info("Закрытие браузера...")
            driver.quit()
