# -*- coding: utf-8 -*-
"""
ads_ai/llm/prompts.py

Промпты для планирования шагов/ремонта в браузерном агенте и специализированные
промпты для мастера создания кампаний Google Ads.

Ключевое требование из задачи:
- Во ВСЕх промптах, где модель планирует или ремонтирует действия, встраиваем
  системные правила `sys_rules()` с перечислением допустимых типов шагов.

Контракты/имена функций сохранены.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional


# ============================== СИСТЕМНЫЕ ПРАВИЛА ==============================

def sys_rules() -> str:
    """
    Базовые правила генерации пошаговых действий для веб-агента.

    ВАЖНО:
      - Всегда возвращай СТРОГО JSON (без Markdown, без пояснений).
      - Не добавляй поля, которых нет в схеме.
      - Не придумывай элементы, которых нет в DOM-снимке (VISIBLE_DOM).
    """
    return (
        "Ты — надёжный веб-бот. Верни ТОЛЬКО JSON (без Markdown, без комментариев).\n"
        "\n"
        "Правила селекторов (устойчивость и совместимость):\n"
        "  1) В приоритете стабильные атрибуты и роли: [data-testid], [data-qa], [aria-label], [role], [name], [id].\n"
        "  2) Разрешён сахар text=\"...\" — клик/поиск по видимому тексту (я конвертирую в XPath).\n"
        "  3) Можно явный xpath=//... (я распознаю префикс xpath=). Если текст уникальный — лучше text=\"...\".\n"
        "  4) НЕ используй :has(...) и случайные :nth-child(...) — они ломкие и не всегда стабильны в Selenium.\n"
        "  5) НЕ используй селектор 'body' и сверхобщие классы без контекста.\n"
        "  6) Если в DOM есть кликабельная ссылка — используй click, НЕ делай navigate/goto на тот же URL.\n"
        "  7) Для ввода текста выбирай ТОЛЬКО редактируемые поля: добавляй фильтр\n"
        "     :not([readonly]):not([disabled]):not([aria-readonly='true']):not([aria-disabled='true']).\n"
        "     Игнорируй элементы с tabindex='-1' и индикацией автодополнения поиска ([role='combobox'] с [aria-autocomplete]).\n"
        "  8) НИКОГДА не вводи данные в глобальные поля поиска/фильтрации со словами\n"
        "     (Search|Поиск|Find|Filter|Lookup|Quick search|Global search|Search for ...),\n"
        "     а также если aria-label/placeholder/name содержит эти маркеры.\n"
        "\n"
        "ДОПУСТИМЫЕ ТИПЫ ШАГОВ (белый список):\n"
        "  click | type | input | sleep | wait | navigate | goto | screenshot |\n"
        "  wait_visible | wait_url | wait_dom | wait_dom_stable | hover | select |\n"
        "  file_upload | switch_to_frame | switch_to_default | new_tab | switch_to_tab | close_tab |\n"
        "  scroll | scroll_to | scroll_to_element | extract | assert_text | evaluate | pause_for_human\n"
        "\n"
        "Схемные подсказки (минимум полей, алиасы):\n"
        "  - click        : {\"type\":\"click\", \"selector\":\"...\"}\n"
        "  - type (alias input): {\"type\":\"type\", \"selector\":\"...\", \"text\":\"...\", \"clear\":true|false}\n"
        "  - sleep        : {\"type\":\"sleep\", \"ms\": 800}\n"
        "  - wait         : {\"type\":\"wait\", \"seconds\": 0.8}\n"
        "  - wait_visible : {\"type\":\"wait_visible\", \"selector\":\"...\", \"timeout\": 12}\n"
        "  - wait_url     : {\"type\":\"wait_url\", \"pattern\":\"/path\", \"regex\": false, \"timeout\": 12}\n"
        "  - wait_dom / wait_dom_stable: {\"type\":\"wait_dom_stable\", \"ms\": 1000, \"timeout\": 12}\n"
        "  - navigate     : {\"type\":\"navigate\", \"url\":\"https://...\"}\n"
        "  - goto (alias) : {\"type\":\"goto\", \"url\":\"https://...\"}\n"
        "  - screenshot   : {\"type\":\"screenshot\", \"label\":\"...\"}\n"
        "  - hover        : {\"type\":\"hover\", \"selector\":\"...\"}\n"
        "  - scroll       : {\"type\":\"scroll\", \"direction\":\"down|up\", \"amount\": 600}\n"
        "  - scroll_to    : {\"type\":\"scroll_to\", \"to\":\"top|bottom\"}\n"
        "  - scroll_to_element: {\"type\":\"scroll_to_element\", \"selector\":\"...\"}\n"
        "  - select       : {\"type\":\"select\", \"selector\":\"...\", \"by\":\"text|value|index\", \"value\":\"...\"}\n"
        "  - file_upload  : {\"type\":\"file_upload\", \"selector\":\"input[type=file]\", \"path\":\"/abs/path\"}\n"
        "  - switch_to_frame: {\"type\":\"switch_to_frame\", \"selector\":\"iframe[...]\"} ИЛИ {\"index\": 0}\n"
        "  - switch_to_default: {\"type\":\"switch_to_default\"}\n"
        "  - new_tab      : {\"type\":\"new_tab\", \"url\":\"about:blank\", \"foreground\": true}\n"
        "  - switch_to_tab: {\"type\":\"switch_to_tab\", \"by\":\"index|url_contains|title_contains\", \"value\":\"...\"}\n"
        "  - close_tab    : {\"type\":\"close_tab\"} ИЛИ {\"type\":\"close_tab\", \"index\": 0}\n"
        "  - extract      : {\"type\":\"extract\", \"selector\":\"...\", \"attr\":\"text|html|outer_html|any_attr\", \"var\":\"name\", \"all\": false}\n"
        "  - assert_text  : {\"type\":\"assert_text\", \"selector\":\"...\", \"attr\":\"text|html|...\", \"match\":\"contains|equals|regex|...\", \"value\":\"...\"}\n"
        "  - evaluate     : {\"type\":\"evaluate\", \"script\":\"return ...\", \"args\": [...], \"var\":\"name\"}\n"
        "  - pause_for_human: {\"type\":\"pause_for_human\", \"reason\":\"...\"}\n"
        "\n"
        "Поддержка переменных: строки могут содержать ${var} и ${var:-fallback}.\n"
        "Контекст: у тебя есть TASK (цель), HISTORY_DONE (выполненные шаги), KNOWN_VARS (значения), VISIBLE_DOM (видимый HTML).\n"
        "Поведение:\n"
        "  - Планируй строго под текущий DOM; после навигации/крупного клика — добавляй соответствующие ожидания.\n"
        "  - Не повторяй уже сделанные шаги из HISTORY_DONE.\n"
        "  - Игнорируй поисковые/readonly/disabled поля.\n"
    )


# ========================= БАЗОВЫЕ (СОВМЕСТИМЫЕ) ПРОМПТЫ ======================

def plan_prompt(html_view: str, task: str, done_history: List[Dict[str, Any]], known_vars: Dict[str, Any]) -> str:
    """
    Планирование массива шагов под текущий видимый DOM и цель (совместимо с текущим рантаймом).
    Возвращать ТОЛЬКО JSON-массив шагов.
    """
    return (
        "[SYS_RULES]\n" + sys_rules() + "\n\n"
        f"TASK:\n{task}\n\n"
        f"KNOWN_VARS:\n{json.dumps(known_vars, ensure_ascii=False)}\n\n"
        f"HISTORY_DONE:\n{json.dumps(done_history, ensure_ascii=False)}\n\n"
        f"VISIBLE_DOM:\n```html\n{html_view}\n```"
    )


def repair_prompt(
    html_view: str,
    task: str,
    history: List[Dict[str, Any]],
    failing_step: Dict[str, Any],
    known_vars: Dict[str, Any],
) -> str:
    """
    Ремонт одного шага, который не выполнился (или выглядит поломанным).
    Вернуть ТОЛЬКО JSON-объект одного шага (замена).
    """
    sys = (
        "Исправь ОДИН следующий шаг под текущий DOM. Верни ТОЛЬКО JSON-объект шага.\n"
        "Сохрани исходный смысл, но сделай валидный, устойчивый selector/действие (см. SYS_RULES)."
    )
    return (
        "[SYS_RULES]\n" + sys_rules() + "\n\n"
        f"{sys}\n\n"
        f"TASK:\n{task}\n\n"
        f"KNOWN_VARS:\n{json.dumps(known_vars, ensure_ascii=False)}\n\n"
        f"HISTORY_DONE:\n{json.dumps(history, ensure_ascii=False)}\n\n"
        f"FAILING_STEP:\n{json.dumps(failing_step, ensure_ascii=False)}\n\n"
        f"VISIBLE_DOM:\n```html\n{html_view}\n```"
    )


# ========================= ДОПОЛНИТЕЛЬНЫЕ ПРОМПТЫ (P&E) =======================

def outline_prompt(task: str) -> str:
    """
    Разложить большую цель на подцели. Возврат ТОЛЬКО JSON-объектом.
    """
    rules = (
        "Разбей цель на последовательность компактных подцелей. Верни ТОЛЬКО JSON-объект.\n"
        "Строгая схема:\n"
        "{\n"
        "  \"subgoals\": [\n"
        "    {\"id\":\"sg1\",\"title\":\"краткий заголовок\",\"goal\":\"описание намерения\",\n"
        "     \"done_when\":\"критерий достижения\",\"notes\":\"важные оговорки/синонимы (опц.)\"}\n"
        "  ]\n"
        "}\n"
        "Никаких Markdown/текста вне JSON."
    )
    payload = {
        "task": task,
        "guidelines": [
            "Подцели должны вести к конечной цели шаг за шагом.",
            "Каждая подцель — выполнима за 1–5 действий в браузере.",
            "Заголовки — нейтральные и проверяемые."
        ],
        "example": {
            "subgoals": [
                {
                    "id": "sg1",
                    "title": "Открыть поиск и найти нужный запрос",
                    "goal": "Открыть поисковик и выполнить запрос пользователя",
                    "done_when": "Отображены результаты поиска по запросу",
                    "notes": "Если задан конкретный сайт — поиск по сайту"
                }
            ]
        }
    }
    return (
        "[SYS_RULES]\n" + sys_rules() + "\n\n"
        f"{rules}\n\n"
        f"TASK:\n{task}\n\n"
        f"CONTEXT:\n{json.dumps(payload, ensure_ascii=False)}"
    )


def subgoal_steps_prompt(
    html_view: str,
    task: str,
    subgoal: Dict[str, Any],
    done_history: List[Dict[str, Any]],
    known_vars: Dict[str, Any],
    max_steps: int = 6,
) -> str:
    """
    Шаги ТОЛЬКО для конкретной подцели. Возврат — JSON-массив шагов.
    """
    header = (
        "Сгенерируй шаги ТОЛЬКО для указанной подцели. Верни ТОЛЬКО JSON-массив шагов.\n"
        f"Ограничение: не более {int(max_steps)} шагов. Учитывай HISTORY_DONE.\n"
        "Если нужен переход/ожидание — добавь соответствующие wait_*.\n"
        "Не повторяй goto, если уже на нужной странице; не дублируй сделанные шаги."
    )
    return (
        "[SYS_RULES]\n" + sys_rules() + "\n\n"
        f"{header}\n\n"
        f"GLOBAL_TASK:\n{task}\n\n"
        f"SUBGOAL:\n{json.dumps(subgoal, ensure_ascii=False)}\n\n"
        f"KNOWN_VARS:\n{json.dumps(known_vars, ensure_ascii=False)}\n\n"
        f"HISTORY_DONE:\n{json.dumps(done_history, ensure_ascii=False)}\n\n"
        f"VISIBLE_DOM:\n```html\n{html_view}\n```"
    )


def verify_or_adjust_prompt(
    html_view: str,
    task: str,
    subgoal: Dict[str, Any],
    last_steps: List[Dict[str, Any]],
    known_vars: Dict[str, Any],
) -> str:
    """
    Верификация выполнения подцели и микро-ремонт. Возврат — JSON-объект {status, reason, fix_steps}.
    """
    header = (
        "Проверь, достигнута ли подцель по текущему DOM. При необходимости — предложи 1–3 корректирующих шага.\n"
        "Ответ ТОЛЬКО JSON-объектом {\"status\":..., \"reason\":..., \"fix_steps\":[...]}. Без Markdown."
    )
    skeleton = {
        "status": "retry",
        "reason": "Нужен дополнительный клик по разделу ...",
        "fix_steps": [
            {"type": "wait_dom_stable", "ms": 800, "timeout": 8},
            {"type": "click", "selector": "text=\"Ингредиенты\""}
        ]
    }
    return (
        "[SYS_RULES]\n" + sys_rules() + "\n\n"
        f"{header}\n\n"
        f"GLOBAL_TASK:\n{task}\n\n"
        f"SUBGOAL:\n{json.dumps(subgoal, ensure_ascii=False)}\n\n"
        f"LAST_EXECUTED_STEPS:\n{json.dumps(last_steps, ensure_ascii=False)}\n\n"
        f"KNOWN_VARS:\n{json.dumps(known_vars, ensure_ascii=False)}\n\n"
        f"VISIBLE_DOM:\n```html\n{html_view}\n```\n\n"
        f"RETURN_SCHEMA_EXAMPLE:\n{json.dumps(skeleton, ensure_ascii=False)}"
    )


# ====================== CAMPINGS (Создание рекламной кампании) =================
# Специализированные промпты под «мастер» создания кампаний.

def _search_field_markers() -> List[str]:
    """
    Синонимы/маркеры, по которым распознаём 'поисковые' поля — запрещённые для ввода данных кампании.
    """
    return [
        "search", "поиск", "найти", "find", "filter", "lookup", "global search",
        "quick search", "search for", "jump to", "search campaigns", "поиск по",
        "search for a page", "search for a page or campaign"
    ]


def campaign_rules() -> str:
    """
    Доп. правила для мастера создания рекламной кампании.
    """
    markers = ", ".join(_search_field_markers())
    return (
        "РЕЖИМ: СОЗДАНИЕ РЕКЛАМНОЙ КАМПАНИИ (wizard).\n"
        "Работай инкрементально: выдай 1–6 следующих шагов, которые можно выполнить СЕЙЧАС.\n"
        "НЕ строй полную цепочку заранее.\n"
        "НЕ делай произвольных переходов (navigate/goto) — переходи по кнопкам 'Далее/Next/Continue/Сохранить и продолжить',\n"
        "только когда текущие обязательные поля заполнены.\n"
        "Если 'Далее' неактивна — найди и заполни обязательные поля текущего шага.\n"
        "Избегай 'Пропустить/Skip/Напомнить позже/No thanks', если это не требуется явно входными данными.\n"
        "Для полей: кликни по полю, затем ввози значение; для селектов — раскрой, затем выбери опцию по text/value.\n"
        "Стабильные селекторы: [aria-label], [name], [role], [data-testid], text=\"...\"; избегай случайных nth-child.\n"
        "После значимых действий добавляй ожидания: wait_dom_stable (600–1200ms) и/или wait_visible.\n"
        "При переходе между шагами — wait_url или wait_visible заголовка следующего шага.\n"
        "Используй только то, что реально есть в VISIBLE_DOM; HISTORY_DONE не дублируй.\n"
        "\n"
        "АНТИ-ЦЕЛИ ДЛЯ ВВОДА (строго запрещено):\n"
        f"  • Глобальные поля поиска/фильтрации и командные палитры — если label/name/placeholder содержит: {markers}.\n"
        "  • Элементы с role='searchbox' или role='combobox' вместе с aria-autocomplete.\n"
        "  • Поля с readonly/aria-readonly='true'/disabled/aria-disabled='true' или tabindex='-1'.\n"
        "Всегда выбирай редактируемые профильные поля этапа (Campaign name, Final URL, Budget и т.п.),\n"
        "а для input добавляй фильтр :not([readonly]):not([disabled]):not([aria-readonly='true']):not([aria-disabled='true']).\n"
        "\n"
        "В конце, если видишь кнопку 'Publish/Create campaign/Создать кампанию' — кликни её с корректными ожиданиями."
    )


def campaign_vocab_hint() -> Dict[str, List[str]]:
    """
    Справочник синонимов для кнопок/полей.
    """
    return {
        "next_button": [
            "Далее", "Продолжить", "Next", "Continue", "Save and continue", "Сохранить и продолжить"
        ],
        "back_button": ["Назад", "Back", "Previous"],
        "campaign_name": ["Название кампании", "Campaign name", "Имя кампании"],
        "final_url": ["Конечный URL", "Окончательный URL", "Целевая страница", "Final URL", "Website", "Веб-сайт"],
        "budget": ["Бюджет", "Среднесуточный бюджет", "Daily budget", "Budget"],
        "bidding": ["Стратегия ставок", "Bidding", "Bidding strategy", "Ставки"],
        "locations": ["Местоположения", "Locations", "География показа", "Target locations", "Геотаргетинг"],
        "languages": ["Языки", "Languages"],
        "keywords": ["Ключевые слова", "Keywords", "Search keywords"],
        "headlines": ["Заголовки", "Headlines"],
        "descriptions": ["Описания", "Descriptions"],
        "phone": ["Телефон", "Phone", "Phone number"],
        "save": ["Сохранить", "Save"],
        "publish": ["Опубликовать", "Publish", "Create campaign", "Создать кампанию"],
        "close_tips": ["Не сейчас", "Later", "Maybe later", "Напомнить позже", "Пропустить", "Skip", "No thanks", "Понятно", "Got it"],
        # Анти-таргет для поиска
        "search_field": _search_field_markers(),
    }


def campaign_outline_prompt(task: str, inputs: Dict[str, Any]) -> str:
    """
    План этапов мастера. Возвращается ТОЛЬКО JSON-объект.
    """
    rules = (
        "Сформируй список этапов мастера для создания рекламной кампании. Верни ТОЛЬКО JSON-объект.\n"
        "Строгая схема:\n"
        "{ \"stages\": [ {\"id\":\"intro\",\"title\":\"Старт мастера\",\"required\":[],\"optional\":[],"
        "\"notes\":\"например: выбор цели/типа кампании\",\"next_texts\":[\"Далее\",\"Next\",\"Continue\"]} ] }"
    )
    context = {
        "inputs": inputs,
        "vocab_hint": campaign_vocab_hint(),
        "avoid_fields": _search_field_markers(),
        "safe_input_guard": ":not([readonly]):not([disabled]):not([aria-readonly='true']):not([aria-disabled='true'])",
        "suggested_stage_order": [
            {"id": "intro", "title": "Старт мастера / Тип кампании"},
            {"id": "base", "title": "Базовые настройки: Название и сайт"},
            {"id": "budget_bidding", "title": "Бюджет и стратегия ставок"},
            {"id": "targeting", "title": "Геотаргетинг и языки"},
            {"id": "keywords", "title": "Ключевые слова (если релевантно)"},
            {"id": "ads", "title": "Объявления: заголовки/описания/URL"},
            {"id": "extensions", "title": "Расширения (телефон и др.)"},
            {"id": "review", "title": "Проверка и публикация"}
        ]
    }
    return (
        "[SYS_RULES]\n" + sys_rules() + "\n\n"
        "Верни план этапов мастера кампании.\n"
        f"{json.dumps(rules, ensure_ascii=False)}\n\n"
        f"TASK:\n{task}\n\n"
        f"INPUTS:\n{json.dumps(inputs, ensure_ascii=False)}\n\n"
        f"CONTEXT:\n{json.dumps(context, ensure_ascii=False)}"
    )


def campaign_stage_steps_prompt(
    html_view: str,
    inputs: Dict[str, Any],
    stage: Dict[str, Any],
    done_history: List[Dict[str, Any]],
    known_vars: Dict[str, Any],
    max_steps: int = 8,
) -> str:
    """
    Шаги ТОЛЬКО для текущего этапа мастера (если используется staging).
    Возвращается ТОЛЬКО JSON-массив шагов.
    """
    header = (
        "Создание рекламной кампании — этап мастера. Верни ТОЛЬКО JSON-массив шагов.\n"
        f"Ограничение: не более {int(max_steps)} шагов. Учитывай HISTORY_DONE.\n"
        "НЕ делай произвольных navigate/goto; переходи по валидной кнопке 'Далее/Next/Continue'.\n"
        "Сначала заполни все требуемые поля этапа, затем переход.\n"
        "Не вводи в поля поиска/фильтрации и не редактируемые поля (см. дисциплину)."
    )
    payload = {
        "mode": "campaign_wizard_stage",
        "inputs": inputs,
        "stage": stage,
        "vocab_hint": campaign_vocab_hint(),
        "avoid_fields": _search_field_markers(),
        "safe_input_guard": ":not([readonly]):not([disabled]):not([aria-readonly='true']):not([aria-disabled='true'])",
        "discipline": campaign_rules(),
        "selector_rules": "Смотри базовые правила в sys_rules()."
    }
    return (
        "[SYS_RULES]\n" + sys_rules() + "\n" + campaign_rules() + "\n\n"
        f"{header}\n"
        f"KNOWN_VARS:\n{json.dumps(known_vars, ensure_ascii=False)}\n\n"
        f"HISTORY_DONE:\n{json.dumps(done_history, ensure_ascii=False)}\n\n"
        f"STAGE:\n{json.dumps(stage, ensure_ascii=False)}\n\n"
        f"INPUTS (значения для полей):\n{json.dumps(inputs, ensure_ascii=False)}\n\n"
        f"GUIDE:\n{json.dumps(payload, ensure_ascii=False)}\n\n"
        f"VISIBLE_DOM:\n```html\n{html_view}\n```"
    )


def campaign_repair_step_prompt(
    html_view: str,
    inputs: Dict[str, Any],
    stage: Dict[str, Any],
    history: List[Dict[str, Any]],
    failing_step: Dict[str, Any],
    known_vars: Dict[str, Any],
) -> str:
    """
    Ремонт ОДНОГО шага в контексте мастера кампании (staging).
    Верни ТОЛЬКО JSON-объект шага.
    """
    advice = (
        "Ремонт в режиме мастера кампании: сохрани исходное намерение.\n"
        "Если опция не сработала — используй более устойчивый якорь (text=\"...\"/[aria-label]/[name]).\n"
        "Для type/input используй ТОЛЬКО редактируемые поля (не readonly/disabled) и избегай поисковых полей.\n"
        "После клика по 'Далее' — добавь wait_url или wait_visible заголовка следующего шага."
    )
    guide = {
        "avoid_fields": _search_field_markers(),
        "safe_input_guard": ":not([readonly]):not([disabled]):not([aria-readonly='true']):not([aria-disabled='true'])",
    }
    return (
        "[SYS_RULES]\n" + sys_rules() + "\n" + campaign_rules() + "\n\n"
        f"{advice}\n\n"
        f"STAGE:\n{json.dumps(stage, ensure_ascii=False)}\n\n"
        f"INPUTS:\n{json.dumps(inputs, ensure_ascii=False)}\n\n"
        f"HISTORY_DONE:\n{json.dumps(history, ensure_ascii=False)}\n\n"
        f"FAILING_STEP:\n{json.dumps(failing_step, ensure_ascii=False)}\n\n"
        f"KNOWN_VARS:\n{json.dumps(known_vars, ensure_ascii=False)}\n\n"
        f"GUIDE:\n{json.dumps(guide, ensure_ascii=False)}\n\n"
        f"VISIBLE_DOM:\n```html\n{html_view}\n```"
    )


def campaign_verify_or_advance_prompt(
    html_view: str,
    inputs: Dict[str, Any],
    stage: Dict[str, Any],
    last_steps: List[Dict[str, Any]],
    known_vars: Dict[str, Any],
) -> str:
    """
    Проверка, что этап завершён (staging). Возврат — JSON-объект {status, reason, fix_steps}.
    """
    header = (
        "Верификация этапа мастера кампании. Если всё заполнено — можно перейти далее.\n"
        "При status=retry предложи до 3-х шагов (например: заполнить пропущенное поле, закрыть подсказку, кликнуть 'Далее').\n"
        "Ответ строго объектом JSON. Без Markdown."
    )
    example = {
        "status": "retry",
        "reason": "Кнопка 'Далее' неактивна — не заполнен 'Название кампании'.",
        "fix_steps": [
            {"type": "click", "selector": "text=\"Название кампании\""},
            {"type": "type", "selector": "[aria-label=\"Название кампании\"]:not([readonly]):not([disabled]):not([aria-readonly='true']):not([aria-disabled='true'])", "text": "${campaign_name}"},
            {"type": "click", "selector": "text=\"Далее\""}
        ]
    }
    guide = {
        "avoid_fields": _search_field_markers(),
        "safe_input_guard": ":not([readonly]):not([disabled]):not([aria-readonly='true']):not([aria-disabled='true'])",
    }
    return (
        "[SYS_RULES]\n" + sys_rules() + "\n" + campaign_rules() + "\n\n"
        f"{header}\n\n"
        f"STAGE:\n{json.dumps(stage, ensure_ascii=False)}\n\n"
        f"INPUTS:\n{json.dumps(inputs, ensure_ascii=False)}\n\n"
        f"LAST_EXECUTED_STEPS:\n{json.dumps(last_steps, ensure_ascii=False)}\n\n"
        f"KNOWN_VARS:\n{json.dumps(known_vars, ensure_ascii=False)}\n\n"
        f"GUIDE:\n{json.dumps(guide, ensure_ascii=False)}\n\n"
        f"VISIBLE_DOM:\n```html\n{html_view}\n```\n\n"
        f"RETURN_SCHEMA_EXAMPLE:\n{json.dumps(example, ensure_ascii=False)}"
    )


# ===================== REAL-TIME режим для кампаний (микро-батчи) ==============

def campaign_next_steps_prompt(
    html_view: str,
    task: str,
    inputs: Dict[str, Any],
    done_history: List[Dict[str, Any]],
    known_vars: Dict[str, Any],
    max_steps: int = 6,
) -> str:
    """
    Инкрементальное планирование: сгенерируй ТОЛЬКО следующие 1–N (до max_steps) шагов,
    которые можно выполнить прямо сейчас для продолжения создания рекламной кампании.
    НЕ строй всю цепочку. Учитывай уже выполненные шаги (HISTORY_DONE) и текущий DOM.
    Возврат — ТОЛЬКО JSON-массив шагов.
    """
    header = (
        "Реальное время — следующий микро-батч шагов. Верни ТОЛЬКО JSON-массив.\n"
        f"Сгенерируй 1–{int(max_steps)} шагов максимум. Не дублируй HISTORY_DONE. "
        "Фокус: заполнить видимые обязательные поля текущего шага мастера и перейти далее, "
        "если кнопка 'Далее/Next' доступна.\n"
        "Если есть модальные подсказки — закрой их. Если 'Далее' неактивна — найди и заполни недостающее.\n"
        "Строго избегай поисковых полей и не редактируемых инпутов."
    )
    guidance = {
        "discipline": campaign_rules(),
        "vocab_hint": campaign_vocab_hint(),
        "avoid_fields": _search_field_markers(),
        "safe_input_guard": ":not([readonly]):not([disabled]):not([aria-readonly='true']):not([aria-disabled='true'])",
        "selector_rules": "используй [aria-label]/[name]/[role]/[data-testid]/text=\"...\"; избегай nth-child",
        "recommendations": [
            "После кликов и вводов добавляй wait_dom_stable 600–1200ms.",
            "После клика 'Далее' — wait_url или wait_visible заголовка следующего экрана.",
            "Для селекта: click по полю -> click по опции text=\"...\"."
        ]
    }
    return (
        "[SYS_RULES]\n" + sys_rules() + "\n" + campaign_rules() + "\n\n"
        f"{header}\n\n"
        f"TASK:\n{task}\n\n"
        f"INPUTS (значения/инварианты кампании):\n{json.dumps(inputs, ensure_ascii=False)}\n\n"
        f"KNOWN_VARS:\n{json.dumps(known_vars, ensure_ascii=False)}\n\n"
        f"HISTORY_DONE (уже выполнено):\n{json.dumps(done_history, ensure_ascii=False)}\n\n"
        f"GUIDE:\n{json.dumps(guidance, ensure_ascii=False)}\n\n"
        f"VISIBLE_DOM:\n```html\n{html_view}\n```"
    )


def campaign_next_repair_prompt(
    html_view: str,
    task: str,
    inputs: Dict[str, Any],
    history: List[Dict[str, Any]],
    failing_step: Dict[str, Any],
    known_vars: Dict[str, Any],
) -> str:
    """
    REAL-TIME ремонт одного шага в процессе создания кампании.
    Верни ТОЛЬКО JSON-объект заменяющего шага.
    """
    advice = (
        "Ремонт одного шага в реальном времени. Сохрани намерение: заполнить поле/выбрать опцию/перейти далее.\n"
        "Если опция не найдена — попробуй text=\"...\" с ближайшим синонимом, либо [aria-label]/[name].\n"
        "НИКОГДА не вводи данные в поля поиска/фильтрации и элементы с readonly/disabled/aria-readonly/aria-disabled.\n"
        "Для 'Далее' — проверь активность кнопки и добавь корректные ожидания (wait_url/wait_visible/wait_dom_stable)."
    )
    guide = {
        "avoid_fields": _search_field_markers(),
        "safe_input_guard": ":not([readonly]):not([disabled]):not([aria-readonly='true']):not([aria-disabled='true'])",
    }
    return (
        "[SYS_RULES]\n" + sys_rules() + "\n" + campaign_rules() + "\n\n"
        f"{advice}\n\n"
        f"TASK:\n{task}\n\n"
        f"INPUTS:\n{json.dumps(inputs, ensure_ascii=False)}\n\n"
        f"HISTORY_DONE:\n{json.dumps(history, ensure_ascii=False)}\n\n"
        f"FAILING_STEP:\n{json.dumps(failing_step, ensure_ascii=False)}\n\n"
        f"KNOWN_VARS:\n{json.dumps(known_vars, ensure_ascii=False)}\n\n"
        f"GUIDE:\n{json.dumps(guide, ensure_ascii=False)}\n\n"
        f"VISIBLE_DOM:\n```html\n{html_view}\n```"
    )


def campaign_completion_check_prompt(
    html_view: str,
    task: str,
    inputs: Dict[str, Any],
    done_history: List[Dict[str, Any]],
    known_vars: Dict[str, Any],
) -> str:
    """
    Проверка статуса завершения процесса кампании в реальном времени.
    Верни ТОЛЬКО JSON-объект:
      { "status": "in_progress|ready_to_publish|published|blocked",
        "reason": "кратко",
        "next_steps": [ ... ]  // 0–3 шага для продвижения/завершения
      }
    """
    header = (
        "Определи текущий статус мастера кампании по DOM: идёт процесс / готово к публикации / опубликовано / блокировка.\n"
        "Если не завершено — предложи до 3-х шагов (next_steps) для продвижения. Ответ ТОЛЬКО JSON-объектом."
    )
    example = {
        "status": "ready_to_publish",
        "reason": "Виден обзор и активна кнопка 'Создать кампанию'",
        "next_steps": [
            {"type": "click", "selector": "text=\"Создать кампанию\""},
            {"type": "wait_url", "pattern": "/campaigns", "regex": False, "timeout": 20}
        ]
    }
    guide = {
        "avoid_fields": _search_field_markers(),
        "safe_input_guard": ":not([readonly]):not([disabled]):not([aria-readonly='true']):not([aria-disabled='true'])",
    }
    return (
        "[SYS_RULES]\n" + sys_rules() + "\n" + campaign_rules() + "\n\n"
        f"{header}\n\n"
        f"TASK:\n{task}\n\n"
        f"INPUTS:\n{json.dumps(inputs, ensure_ascii=False)}\n\n"
        f"KNOWN_VARS:\n{json.dumps(known_vars, ensure_ascii=False)}\n\n"
        f"HISTORY_DONE:\n{json.dumps(done_history, ensure_ascii=False)}\n\n"
        f"GUIDE:\n{json.dumps(guide, ensure_ascii=False)}\n\n"
        f"VISIBLE_DOM:\n```html\n{html_view}\n```\n\n"
        f"RETURN_SCHEMA_EXAMPLE:\n{json.dumps(example, ensure_ascii=False)}"
    )


# ============================= ВСПОМОГАТЕЛЬНЫЕ УТИЛЫ ===========================

def make_json_note(obj: Any) -> str:
    """
    Безопасно сериализуем объект в JSON для подсказок в промпте (чтобы избежать поломок из-за символов).
    Не используется моделью напрямую, только как часть текста промпта.
    """
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return "{}"
