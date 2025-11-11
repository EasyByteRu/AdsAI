# ads_ai/cli/main.py
from __future__ import annotations

import argparse
from pathlib import Path

from ads_ai.core.runner import run_agent


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Ads AI Agent (runner)")
    p.add_argument("--profile", required=True, help="AdsPower profile ID")
    p.add_argument("--url", required=True, help="Start URL")
    p.add_argument("--task", help="One-shot task text (otherwise interactive chat in Bot)")
    p.add_argument("--headless", action="store_true", help="Headless mode")
    p.add_argument("--config", type=Path, help="Path to configs/config.yaml")
    p.add_argument("--logging", type=Path, help="Path to configs/logging.yaml")
    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return run_agent(
        profile=args.profile,
        url=args.url,
        task=args.task,
        headless=args.headless,
        config_path=args.config,
        logging_path=args.logging,
    )


if __name__ == "__main__":
    raise SystemExit(main())
