#!/bin/bash
set -e

# создаём директории
mkdir -p configs \
         artifacts/{screenshots,html_snaps,traces} \
         scripts \
         ads_monster/{cli,config,tracing,storage,utils,browser,plan,llm,integrations/{captcha,notify},plugins/steps,core} \
         tests

# создаём файлы верхнего уровня
touch pyproject.toml requirements.txt .env.example README.md

# конфиги
touch configs/config.yaml configs/logging.yaml

# скрипты
touch scripts/run_agent.sh scripts/dev_check.sh

# Python пакеты
touch ads_monster/__init__.py
touch ads_monster/cli/__init__.py ads_monster/cli/main.py
touch ads_monster/config/__init__.py ads_monster/config/settings.py ads_monster/config/env.py
touch ads_monster/tracing/__init__.py ads_monster/tracing/trace.py ads_monster/tracing/artifacts.py ads_monster/tracing/metrics.py
touch ads_monster/storage/__init__.py ads_monster/storage/vars.py ads_monster/storage/session.py
touch ads_monster/utils/__init__.py ads_monster/utils/json_tools.py ads_monster/utils/ids.py ads_monster/utils/time.py ads_monster/utils/paths.py
touch ads_monster/browser/__init__.py ads_monster/browser/adspower.py ads_monster/browser/driver.py ads_monster/browser/waits.py ads_monster/browser/selectors.py ads_monster/browser/humanize.py ads_monster/browser/actions.py ads_monster/browser/guards.py
touch ads_monster/plan/__init__.py ads_monster/plan/schema.py ads_monster/plan/compiler.py ads_monster/plan/repair.py ads_monster/plan/runtime.py
touch ads_monster/llm/__init__.py ads_monster/llm/gemini.py ads_monster/llm/prompts.py
touch ads_monster/integrations/__init__.py
touch ads_monster/integrations/captcha/__init__.py ads_monster/integrations/captcha/base.py ads_monster/integrations/captcha/twocaptcha.py
touch ads_monster/integrations/notify/__init__.py ads_monster/integrations/notify/base.py ads_monster/integrations/notify/telegram.py ads_monster/integrations/notify/slack.py
touch ads_monster/plugins/__init__.py ads_monster/plugins/registry.py
touch ads_monster/plugins/steps/__init__.py
touch ads_monster/core/__init__.py ads_monster/core/bot.py ads_monster/core/runner.py

# тесты
touch tests/test_schema.py tests/test_selectors.py tests/test_actions.py tests/test_runtime.py

echo "✅ Структура проекта успешно создана в $(pwd)"
