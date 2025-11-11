# ads_ai/browser/humanize.py
from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from typing import Tuple, Optional

from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.common.exceptions import (
    WebDriverException,
    StaleElementReferenceException,
    MoveTargetOutOfBoundsException,
)

from ads_ai.config.settings import Humanize as HumanizeCfg
from ads_ai.utils.time import jitter_ms


@dataclass
class Humanizer:
    """
    Мини‑уровень «очеловечивания» браузерных действий: печать текста, наведение, скролл.
    Никакой «магии» — только устойчивые, предсказуемые приёмы с мягкими паузами.
    """
    driver: WebDriver
    cfg: HumanizeCfg = field(default_factory=HumanizeCfg)  # избегаем мутабельного дефолта

    # ------------------------------------------------------------------ #
    # БАЗОВЫЕ ПАУЗЫ / ДЖИТТЕР                                            #
    # ------------------------------------------------------------------ #

    def tiny_pause(self) -> None:
        """Короткая микропаузa с джиттером из конфига. Никогда не кидает исключений."""
        if not getattr(self.cfg, "enabled", True):
            return
        try:
            time.sleep(jitter_ms(
                getattr(self.cfg, "jitter_ms_min", 10),
                getattr(self.cfg, "jitter_ms_max", 40),
            ))
        except Exception:
            time.sleep(0.02)

    # ------------------------------------------------------------------ #
    # ВВОД ТЕКСТА                                                        #
    # ------------------------------------------------------------------ #

    def type_text(
        self,
        el: WebElement,
        text: str,
        per_char_override: Optional[Tuple[float, float]] = None,
    ) -> None:
        """
        Посимвольный ввод текста (без обязательного clear), с небольшими паузами между символами.
        Устойчив к недавнему clear(): делаем короткую паузу, аккуратно фокусируем элемент.
        Никаких исключений наружу не утекает: максимум — частичное введение текста.
        """
        if not text:
            return

        # Если «человечность» выключена — просто отправляем строку целиком.
        if not getattr(self.cfg, "enabled", True):
            try:
                el.send_keys(text)
            except WebDriverException:
                pass
            return

        # 1) гарантируем видимость + фокус (без агрессии)
        self.scroll_into_view_center(el)
        if not self._has_focus(el):
            self._focus_element(el)
            self.tiny_pause()

        # 2) задержки между символами
        dmin, dmax = per_char_override or (
            float(getattr(self.cfg, "typing_delay_min", 0.03)),
            float(getattr(self.cfg, "typing_delay_max", 0.08)),
        )
        dmin = max(0.0, dmin)
        dmax = max(dmin, dmax)

        # «дыхание» на границах слов/пунктуации
        breath_marks = {",", ".", ";", ":", "!", "?", " "}
        # редкий «перевод духа» раз в N символов
        long_breath_every = random.randint(25, 45)

        sent = 0
        for idx, ch in enumerate(str(text)):
            # попытка обычного ввода
            if not self._send_keys_safe(el, ch):
                # пробуем восстановить фокус и кликнуть по элементу
                try:
                    ActionChains(self.driver).move_to_element(el).click().pause(0.02).perform()
                except Exception:
                    pass
                # вторая попытка
                if not self._send_keys_safe(el, ch):
                    # JS‑фоллбек как крайняя мера для простых input/textarea
                    if not self._js_insert_char(el, ch):
                        # выходим, чтобы не зациклиться — лучше частичный ввод, чем падение
                        break

            sent += 1

            # базовая задержка
            time.sleep(random.uniform(dmin, dmax))

            # редкие микро‑брейки по пунктуации/пробелам
            if ch in breath_marks and random.random() < 0.08:
                self.tiny_pause()

            # более длинная пауза раз в N символов
            if sent % long_breath_every == 0 and random.random() < 0.5:
                self.tiny_pause()

        # финальная микропаузa
        self.tiny_pause()

    # ------------------------------------------------------------------ #
    # ДВИЖЕНИЕ КУРСОРА / HOVER                                           #
    # ------------------------------------------------------------------ #

    def hover(self, el: WebElement) -> None:
        """Наводим курсор на элемент плавно, с лёгким «тремором» у цели."""
        self.scroll_into_view_center(el)
        self.move_mouse_to_element(el, steps=getattr(self.cfg, "mouse_steps", 6), wiggle=True)

    def move_mouse_to_element(self, el: WebElement, steps: Optional[int] = None, wiggle: bool = True) -> None:
        """
        Плавно подводим курсор к элементу серией малых перемещений внутри его прямоугольника.
        Точки траектории — по квадратичной Безье. Если humanize выключен — простой move_to_element.
        """
        if not getattr(self.cfg, "enabled", True):
            try:
                ActionChains(self.driver).move_to_element(el).perform()
            except Exception:
                pass
            return

        steps = max(4, int(steps or getattr(self.cfg, "mouse_steps", 6)))

        rect = self._rect(el)
        w = max(2.0, rect["w"])
        h = max(2.0, rect["h"])

        # Целевая точка — центр с небольшим шумом
        cx = w * 0.5 + random.uniform(-min(6.0, w * 0.08), min(6.0, w * 0.08))
        cy = h * 0.5 + random.uniform(-min(6.0, h * 0.08), min(6.0, h * 0.08))

        # Старт — ближе к углу (внутри прямоугольника)
        sx = max(1.0, min(w - 1.0, w * 0.2 + random.uniform(-w * 0.05, w * 0.05)))
        sy = max(1.0, min(h - 1.0, h * 0.2 + random.uniform(-h * 0.05, h * 0.05)))

        # Контрольная точка — на пути к центру
        kx = (sx + cx) / 2.0 + random.uniform(-w * 0.05, w * 0.05)
        ky = (sy + cy) / 2.0 + random.uniform(-h * 0.05, h * 0.05)

        chain = ActionChains(self.driver)
        for i in range(1, steps + 1):
            t = i / steps
            # квадратичная кривая Безье
            x = (1 - t) ** 2 * sx + 2 * (1 - t) * t * kx + t ** 2 * cx
            y = (1 - t) ** 2 * sy + 2 * (1 - t) * t * ky + t ** 2 * cy
            ox = int(round(max(1.0, min(w - 1.0, x))))
            oy = int(round(max(1.0, min(h - 1.0, y))))
            try:
                chain.move_to_element_with_offset(el, ox, oy).pause(0.02)
            except (WebDriverException, MoveTargetOutOfBoundsException):
                try:
                    chain.move_to_element(el).pause(0.02)
                except Exception:
                    break

        try:
            chain.perform()
        except Exception:
            pass

        if wiggle and getattr(self.cfg, "enabled", True):
            # лёгкий «тремор» курсора у цели (без выхода за экран)
            try:
                ActionChains(self.driver) \
                    .move_by_offset(random.randint(-2, 2), random.randint(-2, 2)).pause(0.03) \
                    .move_by_offset(random.randint(-2, 2), random.randint(-2, 2)).pause(0.03) \
                    .perform()
            except Exception:
                pass

        self.tiny_pause()

    # ------------------------------------------------------------------ #
    # ПАРКОВКА КУРСОРА                                                   #
    # ------------------------------------------------------------------ #

    def park_mouse(self, corner: str = "top-left") -> None:
        """Отводим курсор в нейтральную область окна, чтобы не подсвечивать элементы.

        По умолчанию — верхний левый угол DOM (`html`), со смещением 5×5 px.
        Без падений: любые ошибки внутри поглощаются.
        """
        try:
            root = self.driver.find_element(By.TAG_NAME, "html")
        except Exception:
            return
        try:
            w = root.size.get("width", 1024) or 1024
            h = root.size.get("height", 768) or 768
            if corner == "top-left":
                ox, oy = 5, 5
            elif corner == "top-right":
                ox, oy = max(5, w - 10), 5
            elif corner == "bottom-left":
                ox, oy = 5, max(5, h - 10)
            else:  # bottom-right
                ox, oy = max(5, w - 10), max(5, h - 10)
            ActionChains(self.driver).move_to_element_with_offset(root, int(ox), int(oy)).pause(0.05).perform()
        except Exception:
            try:
                ActionChains(self.driver).move_to_element(root).pause(0.05).perform()
            except Exception:
                pass
        self.tiny_pause()

    # ------------------------------------------------------------------ #
    # СКРОЛЛ                                                             #
    # ------------------------------------------------------------------ #

    def smooth_scroll_by(self, amount_px: int | float) -> None:
        """
        Плавный скролл вертикально на указанное количество пикселей.
        Делим движение на чанки (cfg.scroll_chunk_px), применяем easing и джиттер пауз.
        Если humanize выключен — один прямой scrollBy.
        """
        if not amount_px:
            return

        # Без очеловечивания — прямой скролл
        if not getattr(self.cfg, "enabled", True):
            try:
                self.driver.execute_script("window.scrollBy(0, arguments[0]);", float(amount_px))
            except Exception:
                pass
            return

        total = float(amount_px)
        direction = 1.0 if total > 0 else -1.0
        chunk = max(1.0, float(getattr(self.cfg, "scroll_chunk_px", 120)))
        steps = max(1, int(abs(total) // chunk)) or 1

        # гладкая ease-in-out (cubic)
        def ease(t: float) -> float:
            return 4 * t * t * t if t < 0.5 else 1 - pow(-2 * t + 2, 3) / 2

        moved = 0.0
        for i in range(1, steps + 1):
            t0 = (i - 1) / steps
            t1 = i / steps
            w = ease(t1) - ease(t0)
            dy = direction * w * abs(total)
            if i == steps:
                dy = total - moved  # компенсируем накопленную погрешность
            self._scroll_chunk(dy)
            moved += dy

    def scroll_into_view_center(self, el: WebElement) -> None:
        """Прокручивает элемент к центру вьюпорта (без нативной «плавности»), затем микропаузa."""
        try:
            self.driver.execute_script(
                "try{arguments[0].scrollIntoView({block:'center', inline:'center', behavior:'instant'});}catch(_){"
                "  try{arguments[0].scrollIntoView({block:'center', inline:'center'});}catch(__){}"
                "}", el
            )
        except Exception:
            pass
        self.tiny_pause()

    # ------------------------------------------------------------------ #
    # ВНУТРЕННИЕ ХЕЛПЕРЫ                                                 #
    # ------------------------------------------------------------------ #

    def _scroll_chunk(self, dy: float) -> None:
        try:
            self.driver.execute_script("window.scrollBy(0, arguments[0]);", float(dy))
        except Exception:
            pass
        self.tiny_pause()

    def _rect(self, el: WebElement) -> dict:
        """Безопасно получает {x,y,w,h} через getBoundingClientRect(), с запасным значением."""
        try:
            r = self.driver.execute_script(
                "const b=arguments[0].getBoundingClientRect();"
                "return {x:b.left, y:b.top, w:b.width, h:b.height};", el
            )
            if isinstance(r, dict):
                return {
                    "x": float(r.get("x", 0.0)),
                    "y": float(r.get("y", 0.0)),
                    "w": float(r.get("w", 0.0)),
                    "h": float(r.get("h", 0.0)),
                }
        except Exception:
            pass
        # запасной усреднённый прямоугольник (не позволяет делить на ноль)
        return {"x": 0.0, "y": 0.0, "w": 10.0, "h": 10.0}

    def _has_focus(self, el: WebElement) -> bool:
        try:
            return bool(self.driver.execute_script("return document.activeElement===arguments[0];", el))
        except Exception:
            return False

    def _focus_element(self, el: WebElement) -> None:
        """
        Мягко фокусируем элемент: навели → кликнули; если не вышло — прямой click(); если нет — JS focus().
        """
        try:
            ActionChains(self.driver).move_to_element(el).click().pause(0.03).perform()
            self.tiny_pause()
            return
        except Exception:
            pass
        try:
            el.click()
            self.tiny_pause()
            return
        except Exception:
            pass
        try:
            self.driver.execute_script("try{ arguments[0].focus(); }catch(e){}", el)
        except Exception:
            pass

    def _send_keys_safe(self, el: WebElement, s: str) -> bool:
        """
        Безопасная отправка фрагмента текста в элемент.
        Возвращает True при успехе. Никогда не кидает исключений наружу.
        """
        try:
            el.send_keys(s)
            return True
        except (StaleElementReferenceException, WebDriverException):
            # В редких случаях элемент «распался», даём одну повторную попытку.
            try:
                el.send_keys(s)
                return True
            except Exception:
                return False

    def _js_insert_char(self, el: WebElement, ch: str) -> bool:
        """
        Крайний фоллбек для input/textarea: вставка символа через JS с диспатчем событий.
        Это НЕ делает поведение менее «человечным», т.к. используется только при сбоях.
        """
        try:
            return bool(self.driver.execute_script(
                """
                try {
                  const el = arguments[0];
                  const ch = String(arguments[1] || "");
                  if (!el) return false;
                  const isInput = el.tagName && /^(INPUT|TEXTAREA)$/i.test(el.tagName);
                  if (!isInput) return false;

                  const start = el.selectionStart ?? el.value.length;
                  const end   = el.selectionEnd   ?? el.value.length;
                  const before = el.value.slice(0, start);
                  const after  = el.value.slice(end);
                  el.value = before + ch + after;

                  const pos = start + ch.length;
                  try { el.setSelectionRange(pos, pos); } catch(e) {}

                  const evts = ["input", "change", "keyup"];
                  evts.forEach(e => { try{ el.dispatchEvent(new Event(e, {bubbles:true})); }catch(_){ } });
                  return true;
                } catch(e) { return false; }
                """,
                el, ch
            ))
        except Exception:
            return False

    # (опционально) — пригодится для будущих действий
    def _select_all_and_clear(self, el: WebElement) -> None:
        """Надёжное очищение: Cmd/Ctrl+A → Backspace, без выброса исключений наружу."""
        try:
            mod = Keys.COMMAND if self._is_macos() else Keys.CONTROL
            el.send_keys(mod, "a")
            time.sleep(0.02)
            el.send_keys(Keys.BACK_SPACE)
        except Exception:
            try:
                el.clear()
            except Exception:
                pass

    def _is_macos(self) -> bool:
        try:
            platform = (self.driver.capabilities or {}).get("platformName", "") or \
                       (self.driver.capabilities or {}).get("platform", "")
            return isinstance(platform, str) and platform.lower().startswith("mac")
        except Exception:
            return False
