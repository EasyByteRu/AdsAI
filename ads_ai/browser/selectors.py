# ads_ai/browser/selectors.py
from __future__ import annotations

import re
from functools import lru_cache
from typing import Optional, Tuple, List, Literal

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    InvalidSelectorException,
    WebDriverException,
    StaleElementReferenceException,
    NoSuchElementException,
)
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement

__all__ = [
    "normalize_selector",
    "find",
    "exists",
    "find_all",
]

# ---------------------------------------------------------------------------
# Евристики определения типа селектора
# ---------------------------------------------------------------------------

# Символы, при которых строка, скорее всего, — CSS/XPath (а не «просто текст» ссылки)
_CSS_SIGNS = set("#.:[]=>,+~*(){}|$^\"'\\/@")

# Подсказки, что это XPath
_XPATH_HINTS = ("//", "(.//", "(/", "/html", "descendant::", "self::", "parent::", "contains(")

def _looks_like_xpath(s: str) -> bool:
    s = s.strip()
    if s.startswith(("//", ".//", "(.//", "(/", "/")):
        return True
    if any(h in s for h in _XPATH_HINTS):
        return True
    return False

def _looks_like_plain_text(s: str) -> bool:
    """Эвристика: строка похожа на «просто текст» (без специальных символов)."""
    return bool(s) and not any(ch in _CSS_SIGNS for ch in s)


# ---------------------------------------------------------------------------
# Case-insensitive для XPath: латиница + кириллица (RU/UA/BE + часть KZ)
# ---------------------------------------------------------------------------

_ASCII_UP = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
_ASCII_LO = "abcdefghijklmnopqrstuvwxyz"

# Русский алфавит (включая Ё/ё)
_RU_UP = "АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ"
_RU_LO = "абвгдеёжзийклмнопрстуфхцчшщъыьэюя"

# Украинский / белорусский дополнения
_UA_BE_UP = "ІЇЄҐЎ"
_UA_BE_LO = "іїєґў"

# Казахский (частые буквы в интерфейсах)
_KZ_UP = "ӘҒҚҢӨҰҮҺІ"
_KZ_LO = "әғқңөүұһі"

_UPPER = _ASCII_UP + _RU_UP + _UA_BE_UP + _KZ_UP
_LOWER = _ASCII_LO + _RU_LO + _UA_BE_LO + _KZ_LO


def _collapse_spaces(s: str) -> str:
    """Нормализуем подряд идущие пробелы в один."""
    return re.sub(r"\s+", " ", s or "").strip()


def _xpath_literal(s: str) -> str:
    """Безопасная упаковка Python-строки в XPath literal."""
    s = s or ""
    if "'" not in s:
        return f"'{s}'"
    if '"' not in s:
        return f'"{s}"'
    parts = s.split("'")
    # concat('ab', "'", 'cd', "'", 'ef')
    return "concat(" + ", ".join([f"'{p}'" for p in parts[:-1]] + ['"\'"', f"'{parts[-1]}'"]) + ")"


def _xp_ci(expr: str) -> str:
    """translate(expr, UPPER, LOWER)"""
    return f"translate({expr}, '{_UPPER}', '{_LOWER}')"


def _xp_ci_contains(expr: str, raw: str) -> str:
    """Case-insensitive contains для XPath (с ASCII/RU/UA/BE/KZ)."""
    lit = _xpath_literal(_collapse_spaces(raw))
    return f"contains({_xp_ci(expr)}, {_xp_ci(lit)}) or contains({expr}, {lit})"


def _xp_ci_starts_with(expr: str, raw: str) -> str:
    """Case-insensitive starts-with."""
    lit = _xpath_literal(_collapse_spaces(raw))
    return f"starts-with({_xp_ci(expr)}, {_xp_ci(lit)}) or starts-with({expr}, {lit})"


def _xp_ci_ends_with(expr: str, raw: str) -> str:
    """Case-insensitive ends-with для XPath 1.0."""
    lit = _xpath_literal(_collapse_spaces(raw))
    # substring(expr, string-length(expr)-string-length(lit)+1) = lit
    a = f"substring({_xp_ci(expr)}, string-length({_xp_ci(expr)}) - string-length({_xp_ci(lit)}) + 1)"
    b = f"substring({expr}, string-length({expr}) - string-length({lit}) + 1)"
    return f"{a} = {_xp_ci(lit)} or {b} = {lit}"


def _xp_ci_word(expr: str, raw: str) -> str:
    """Case-insensitive word-match: ' ... word ... ' (обрамляем пробелами)."""
    lit = _xpath_literal(_collapse_spaces(raw))
    # contains(concat(' ', expr, ' '), ' word ')
    ex_ci = f"concat(' ', {_xp_ci(expr)}, ' ')"
    lt_ci = f"concat(' ', {_xp_ci(lit)}, ' ')"
    ex = f"concat(' ', {expr}, ' ')"
    lt = f"concat(' ', {lit}, ' ')"
    return f"contains({ex_ci}, {lt_ci}) or contains({ex}, {lt})"


# ---------------------------------------------------------------------------
# Конструкторы XPath-селекторов
# ---------------------------------------------------------------------------

def _xp_text_clickables(text: str, mode: Literal["contains","prefix","suffix","word"]="contains") -> str:
    """
    XPath, отдающий «кликабельные» элементы первыми, затем — любой элемент.
    Поддерживает режимы сравнения по тексту/aria-label.
    """
    t = _collapse_spaces(text)
    expr_t = "normalize-space(string(.))"
    expr_aria = "normalize-space(@aria-label)"

    if mode == "prefix":
        cond_t = _xp_ci_starts_with(expr_t, t)
        cond_aria = _xp_ci_starts_with(expr_aria, t)
    elif mode == "suffix":
        cond_t = _xp_ci_ends_with(expr_t, t)
        cond_aria = _xp_ci_ends_with(expr_aria, t)
    elif mode == "word":
        cond_t = _xp_ci_word(expr_t, t)
        cond_aria = _xp_ci_word(expr_aria, t)
    else:
        cond_t = _xp_ci_contains(expr_t, t)
        cond_aria = _xp_ci_contains(expr_aria, t)

    # 1) Кликабельные (a/button/summary/input[type=button|submit]/role)
    clickables = (
        "("
        f"//a[{cond_t} or {cond_aria}]"
        " | "
        f"//button[{cond_t} or {cond_aria}]"
        " | "
        f"//summary[{cond_t} or {cond_aria}]"
        " | "
        f"//input[(translate(@type,'{_UPPER}','{_LOWER}')='button' "
        f"       or translate(@type,'{_UPPER}','{_LOWER}')='submit')]"
        f"      [contains(@value, {_xpath_literal(t)}) or {cond_aria}]"
        " | "
        f"//*[@role='button' or @role='link' or @role='tab' or @role='menuitem' or @role='option']"
        f"[{cond_t} or {cond_aria}]"
        ")"
    )
    # 2) Любой узел с текстом
    any_node = f"(//*[ {cond_t} ])"
    return f"{clickables} | {any_node}"


def _xp_role(role: str, name: Optional[str]) -> str:
    """
    XPath для `role=...` c опциональным [name="..."].
    Для role=button/link/textbox расширяем семантику нативными тегами.
    """
    r = (role or "").strip()
    if not r:
        return "//*[@role]"

    base: str
    rl = r.lower()
    if rl == "button":
        base = "//*[@role='button' or self::button or (self::a and @href)]"
    elif rl == "link":
        base = "//*[@role='link' or (self::a and @href)]"
    elif rl == "textbox":
        base = "//*[@role='textbox' or self::input or self::textarea]"
    else:
        base = f"//*[@role={_xpath_literal(r)}]"

    if not name:
        return base

    expr_t = "normalize-space(string(.))"
    expr_aria = "normalize-space(@aria-label)"
    cond = f"{_xp_ci_contains(expr_t, name)} or {_xp_ci_contains(expr_aria, name)}"
    return f"{base}[{cond}]"


# ---------------------------------------------------------------------------
# Сахар-парсеры (role/text/aria)
# ---------------------------------------------------------------------------

_ROLE_RE = re.compile(
    r"""^role\s*=\s*([a-zA-Z0-9_-]+)
        (?:\s*\[\s*(?:name|text)?\s*=\s*(?:"([^"]+)"|'([^']+)'|([^\]]+))\s*\])?
        |^role\s*=\s*([a-zA-Z0-9_-]+)\s*\[\s*"([^"]+)"\s*\]
    $""",
    re.X,
)

def _parse_role_selector(s: str) -> Optional[Tuple[str, str]]:
    m = _ROLE_RE.match(s)
    if not m:
        return None
    role = m.group(1) or m.group(5)
    name = m.group(2) or m.group(3) or (m.group(4).strip() if m.group(4) else "") or m.group(6)
    xp = _xp_role(role, name or None)
    return xp, "xpath"


def _parse_text_selector(s: str) -> Optional[Tuple[str, str]]:
    low = s.lower()
    if low.startswith("text="):
        v = s[5:].strip().strip('"').strip("'")
        return _xp_text_clickables(v, "contains"), "xpath"
    if low.startswith("text^="):
        v = s[6:].strip().strip('"').strip("'")
        return _xp_text_clickables(v, "prefix"), "xpath"
    if low.startswith("text$="):
        v = s[6:].strip().strip('"').strip("'")
        return _xp_text_clickables(v, "suffix"), "xpath"
    if low.startswith("text~="):
        v = s[6:].strip().strip('"').strip("'")
        return _xp_text_clickables(v, "word"), "xpath"
    return None


def _css_attr_contains(attr: str, value: str) -> str:
    v = (value or "").replace('"', '\\"')
    return f'[{attr}*="{v}"]'

def _css_attr_prefix(attr: str, value: str) -> str:
    v = (value or "").replace('"', '\\"')
    return f'[{attr}^="{v}"]'

def _css_attr_suffix(attr: str, value: str) -> str:
    v = (value or "").replace('"', '\\"')
    return f'[{attr}$="{v}"]'

def _parse_aria_selector(s: str) -> Optional[Tuple[str, str]]:
    low = s.lower()
    for op, builder in (("aria^=", _css_attr_prefix), ("aria$=", _css_attr_suffix), ("aria~=", None), ("aria=", _css_attr_contains)):
        if low.startswith(op):
            v = s[len(op):].strip().strip('"').strip("'")
            # CSS: aria-label, title, alt, placeholder (частый дублирующий UX)
            if op == "aria~=":
                # «по слову» — делаем через XPath, т.к. CSS нет word-contains
                xp = (
                    "//*[" +
                    _xp_ci_word("normalize-space(@aria-label)", v) + " or " +
                    _xp_ci_word("normalize-space(@title)", v) + " or " +
                    _xp_ci_word("normalize-space(@alt)", v) + " or " +
                    _xp_ci_word("normalize-space(@placeholder)", v) +
                    "]"
                )
                return xp, "xpath"
            css = ",".join([
                builder("aria-label", v),
                builder("title", v),
                builder("alt", v),
                builder("placeholder", v),
            ])
            return css, "css"
    return None


# ---------------------------------------------------------------------------
# Нормализация селектора (кэшируется)
# ---------------------------------------------------------------------------

@lru_cache(maxsize=2048)
def normalize_selector(sel: str) -> Tuple[str, str]:
    """
    Преобразует произвольный селектор к кортежу (query, kind), где kind ∈ {'css','xpath'}.

    Поддерживаемый сахар (совместим с UI/LLM-промптами):
      - css=... | css:...      → CSS
      - xpath=... | xpath:...  → XPath
      - text=...               → XPath (contains, RU/EN ci)
      - text^=... / text$=... / text~=... → XPath (prefix/suffix/word)
      - aria=... / aria^=... / aria$=... / aria~=... → CSS/XPath (aria-label/title/alt/placeholder)
      - role=button / role=button[name="..."] / role=button["..."] → XPath
      - Мягкий сахар: id=..., name=..., testid=..., data-test=..., data-testid=..., placeholder=...

    Без префикса:
      - Если похоже на XPath — возвращаем XPath.
      - Иначе — считаем CSS. Если строка «похожа на текст», будет доп. fallback по ссылкам.
    """
    s = (sel or "").strip()
    if not s:
        return "", "css"

    low = s.lower()

    # Явные префиксы CSS/XPath
    if low.startswith(("css=", "css:")):
        body = s.split("=", 1)[-1] if "=" in s else s
        body = body.split(":", 1)[-1] if ":" in body else body
        return body.strip(), "css"
    if low.startswith(("xpath=", "xpath:")):
        body = s.split("=", 1)[-1] if "=" in s else s
        body = body.split(":", 1)[-1] if ":" in body else body
        return body.strip(), "xpath"

    # Явный XPath по началу
    if _looks_like_xpath(s):
        return s, "xpath"

    # text= / text^= / text$= / text~=
    parsed = _parse_text_selector(s)
    if parsed:
        return parsed

    # aria= / aria^= / aria$= / aria~=
    parsed = _parse_aria_selector(s)
    if parsed:
        return parsed

    # role=...
    parsed = _parse_role_selector(s)
    if parsed:
        return parsed

    # Мягкий сахар по атрибутам
    if low.startswith("id="):
        return f"#{s[3:].strip()}", "css"
    if low.startswith("name="):
        v = s[5:].strip().strip('"').strip("'").replace('"', '\\"')
        return f'[name="{v}"]', "css"
    if low.startswith("testid=") or low.startswith("data-testid="):
        v = s.split("=", 1)[1].strip().strip('"').strip("'").replace('"', '\\"')
        return f'[data-testid="{v}"]', "css"
    if low.startswith("data-test="):
        v = s.split("=", 1)[1].strip().strip('"').strip("'").replace('"', '\\"')
        return f'[data-test="{v}"]', "css"
    if low.startswith("placeholder="):
        v = s.split("=", 1)[1].strip().strip('"').strip("'").replace('"', '\\"')
        return f'[placeholder="{v}"]', "css"

    # По умолчанию — CSS
    return s, "css"


# ---------------------------------------------------------------------------
# Ранжирование «кликабельности» и фильтрация
# ---------------------------------------------------------------------------

def _score_clickability(el: WebElement) -> int:
    """
    Эвристический «вес кликабельности»:
      +3: href
      +2: role ∈ {button, link, tab, menuitem, option}
      +1: tag ∈ {a, button, input, summary, label}
      +1: enabled
    """
    score = 0
    try:
        tag = (el.tag_name or "").lower()
        role = (el.get_attribute("role") or "").lower()
        href = el.get_attribute("href")
        if href:
            score += 3
        if role in {"button", "link", "tab", "menuitem", "option"}:
            score += 2
        if tag in {"a", "button", "input", "summary", "label"}:
            score += 1
        if el.is_enabled():
            score += 1
    except WebDriverException:
        pass
    return score


def _pick_best(candidates: List[WebElement], require_visible: bool) -> Optional[WebElement]:
    """
    Из списка кандидатов выбираем реалистичные (видимые при флаге и с положительной площадью),
    затем ранжируем по кликабельности.
    """
    ranked: List[tuple[int, WebElement]] = []
    for el in candidates:
        try:
            if require_visible and not el.is_displayed():
                continue
            r = el.rect or {}
            if (r.get("width", 0) or 0) <= 0 or (r.get("height", 0) or 0) <= 0:
                continue
            ranked.append((_score_clickability(el), el))
        except (StaleElementReferenceException, WebDriverException):
            continue

    if not ranked:
        return None
    ranked.sort(key=lambda t: t[0], reverse=True)
    return ranked[0][1]


def _as_locator(query: str, kind: str) -> tuple[str, str]:
    return (By.CSS_SELECTOR, query) if kind == "css" else (By.XPATH, query)


# ---------------------------------------------------------------------------
# Ожидания и fallback-стратегии
# ---------------------------------------------------------------------------

def _wait_pick_with_locator(
    driver: WebDriver,
    locator: tuple[str, str],
    *,
    visible: bool,
    timeout_sec: int,
) -> Optional[WebElement]:
    """
    Ждём коллекцию и выбираем лучший элемент по эвристикам.
    Используем пользовательский предикат, чтобы иметь доступ к списку.
    """
    def _predicate(_driver: WebDriver) -> Optional[WebElement] | bool:
        try:
            els = _driver.find_elements(*locator)
        except InvalidSelectorException:
            return False
        except WebDriverException:
            return False

        best = _pick_best(els, require_visible=visible)
        return best or False

    try:
        return WebDriverWait(
            driver,
            timeout_sec,
            poll_frequency=0.25,
            ignored_exceptions=(NoSuchElementException, StaleElementReferenceException),
        ).until(_predicate)
    except TimeoutException:
        return None


def _try_link_fallback(
    driver: WebDriver,
    q: str,
    visible: bool,
    timeout_sec: int,
) -> Optional[WebElement]:
    """
    Ссылочные fallback’и — LINK_TEXT, затем PARTIAL_LINK_TEXT.
    Таймаут берём усечённый, т.к. это запасной путь.
    """
    cond = EC.visibility_of_element_located if visible else EC.presence_of_element_located
    short = max(1, min(2, timeout_sec))
    try:
        return WebDriverWait(driver, short, poll_frequency=0.25).until(cond((By.LINK_TEXT, q)))
    except (TimeoutException, InvalidSelectorException, WebDriverException):
        pass
    try:
        return WebDriverWait(driver, short, poll_frequency=0.25).until(cond((By.PARTIAL_LINK_TEXT, q)))
    except (TimeoutException, InvalidSelectorException, WebDriverException):
        return None


# ---------------------------------------------------------------------------
# Публичное API
# ---------------------------------------------------------------------------

def find(
    driver: WebDriver,
    selector: str,
    *,
    visible: bool = False,
    timeout_sec: int = 12,
) -> Optional[WebElement]:
    """
    Находит ОДИН «лучший» элемент по универсальному селектору, дожидаясь появления/видимости.
    Возвращает WebElement или None.

    Алгоритм:
      1) normalize_selector → (query, kind ∈ {css|xpath});
      2) Ждём коллекцию и ранжируем кликабельность (href/role/tag/enabled/площадь);
      3) Если kind == 'css' и строка похожа на «просто текст» — доп. fallback по ссылкам.

    Устойчив к InvalidSelectorException / WebDriverException.
    Не выполняет scroll-into-view — этим занимается слой действий (Humanizer/Actions).
    """
    q, kind = normalize_selector(selector)
    if not q:
        return None

    locator = _as_locator(q, kind)
    el = _wait_pick_with_locator(driver, locator, visible=visible, timeout_sec=timeout_sec)
    if el is not None:
        return el

    if kind == "css" and _looks_like_plain_text(q):
        return _try_link_fallback(driver, q, visible, timeout_sec)

    # В редком случае: пользователь дал CSS, который на деле XPath — аккуратный авто-ретрай
    if kind == "css" and _looks_like_xpath(q):
        try:
            return _wait_pick_with_locator(driver, (By.XPATH, q), visible=visible, timeout_sec=max(1, timeout_sec // 2))
        except Exception:
            return None

    return None


def exists(
    driver: WebDriver,
    selector: str,
    *,
    visible: bool = False,
    timeout_sec: int = 12,
) -> bool:
    """Проверка существования (и видимости при флаге) хотя бы одного подходящего элемента."""
    return find(driver, selector, visible=visible, timeout_sec=timeout_sec) is not None


def find_all(
    driver: WebDriver,
    selector: str,
    *,
    visible: bool = False,
    timeout_sec: int = 12,
) -> List[WebElement]:
    """
    Возвращает список элементов по селектору.
    Сначала ждём хотя бы один (через find) — чтобы не гонять пустые выборки,
    затем забираем коллекцию и при необходимости фильтруем по видимости.
    """
    q, kind = normalize_selector(selector)
    if not q:
        return []

    # Прогреем ожидание (и синхронизацию DOM)
    first = find(driver, selector, visible=visible, timeout_sec=timeout_sec)
    if not first:
        return []

    by, term = _as_locator(q, kind)
    try:
        els = driver.find_elements(by, term)
    except (InvalidSelectorException, WebDriverException):
        # Если селектор сломан и строка «похожа на текст» — соберём ссылки PARTIAL_LINK_TEXT.
        if kind == "css" and _looks_like_plain_text(q):
            try:
                return driver.find_elements(By.PARTIAL_LINK_TEXT, q)
            except Exception:
                return []
        return []

    if visible:
        out: List[WebElement] = []
        for e in els:
            try:
                if e.is_displayed():
                    out.append(e)
            except (StaleElementReferenceException, WebDriverException):
                continue
        return out

    return els
