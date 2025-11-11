# ads_ai/browser/guards.py
from __future__ import annotations

import collections
from dataclasses import dataclass, field
from typing import Iterable, List, Optional

from selenium.webdriver.remote.webdriver import WebDriver

from ads_ai.config.settings import Guards as GuardsCfg, Browser as BrowserCfg
from ads_ai.utils.ids import sha1


def _full_dom(d: WebDriver) -> str:
    try:
        return d.execute_script(
            "return document.documentElement?.outerHTML || document.body?.outerHTML || ''"
        ) or ""
    except Exception:
        return ""


def _viewport_dom(d: WebDriver) -> str:
    js = """
    const H = window.innerHeight || 800;
    const V = (n) => {
      if (!n) return false;
      const r = n.getBoundingClientRect();
      return r && r.top < H && r.bottom > 0 && r.width > 0 && r.height > 0;
    };
    const out = [];
    (function walk(node){
       if (!node) return;
       if (V(node) && node.outerHTML) out.push(node.outerHTML);
       const kids = node.children || [];
       for (const c of kids) walk(c);
    })(document.body || document.documentElement);
    return out.join('\\n');
    """
    try:
        return d.execute_script(js) or ""
    except Exception:
        return ""


@dataclass
class LoopGuardState:
    hashes: collections.deque[str] = field(default_factory=lambda: collections.deque(maxlen=8))
    trips: int = 0  # сколько раз сработал триггер


@dataclass
class Guards:
    driver: WebDriver
    guards_cfg: GuardsCfg = field(default_factory=GuardsCfg)     # FIX: mutable default -> default_factory
    browser_cfg: BrowserCfg = field(default_factory=BrowserCfg)   # FIX: mutable default -> default_factory
    state: LoopGuardState = field(default_factory=LoopGuardState)

    def dom_snapshot(self) -> str:
        if str(self.browser_cfg.dom_scope).lower() == "viewport":
            return _viewport_dom(self.driver)
        return _full_dom(self.driver)

    def loop_guard_update(self, recent_actions: Iterable[dict]) -> bool:
        """
        Добавляет хеш DOM снапшота и проверяет «ступор».
        Возвращает True, если DOM не меняется в окне наблюдения и были действия типа click/input/select.
        """
        html = self.dom_snapshot()
        h = sha1((html or "")[:50000])  # 50кб достаточно для сигнатуры
        self.state.hashes.append(h)

        window = self.guards_cfg.loop_dom_hash_window
        trip_count = self.guards_cfg.loop_dom_hash_trip_count  # оставлено для будущих эвристик

        if len(self.state.hashes) < window:
            return False

        recent = list(self.state.hashes)[-window:]
        stalled = len(set(recent)) == 1
        acted = any((a.get("type") or "").lower() in {"click", "input", "select"}
                    for a in list(recent_actions)[-window:])

        if stalled and acted:
            self.state.trips += 1
            return True
        return False

    def detect_captcha(self, keywords: Optional[List[str]] = None) -> bool:
        """
        Примитивная проверка на капчу: по ключевым словам в HTML.
        """
        keys = [k.lower() for k in (keywords or self.guards_cfg.captcha_keywords or [])]
        if not keys:
            return False
        try:
            html = (self.driver.page_source or "").lower()
        except Exception:
            html = ""
        return any(k in html for k in keys)

    def auto_refresh_on_stall(self) -> None:
        if not self.guards_cfg.auto_refresh_on_stall:
            return
        try:
            self.driver.refresh()
        except Exception:
            pass
