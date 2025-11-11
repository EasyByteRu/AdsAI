# ads_ai/tracing/artifacts.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

from ads_ai.utils.ids import now_id
from ads_ai.utils.paths import ensure_dir


@dataclass
class Artifacts:
    run_id: str
    screenshots_dir: Path
    html_snaps_dir: Path

    @classmethod
    def for_run(cls, run_id: str, base_screenshots: Path, base_html_snaps: Path, per_run_subdir: bool = True) -> "Artifacts":
        screenshots = base_screenshots / run_id if per_run_subdir else base_screenshots
        html_snaps = base_html_snaps / run_id if per_run_subdir else base_html_snaps
        ensure_dir(screenshots)
        ensure_dir(html_snaps)
        return cls(run_id=run_id, screenshots_dir=screenshots, html_snaps_dir=html_snaps)

    def screenshot_path(self, label: str) -> Path:
        return self.screenshots_dir / f"{now_id(label)}.png"

    def html_snap_path(self) -> Path:
        return self.html_snaps_dir / f"{now_id('dom')}.html"


def save_html_snapshot(html: str, artifacts: Artifacts) -> Path:
    path = artifacts.html_snap_path()
    try:
        with path.open("w", encoding="utf-8") as f:
            f.write(html or "")
    except Exception:
        # не паникуем — просто возвращаем путь, даже если пусто
        pass
    return path


def take_screenshot(driver, artifacts: Artifacts, label: str) -> Path:
    path = artifacts.screenshot_path(label)
    try:
        driver.save_screenshot(str(path))
    except Exception:
        pass
    return path
