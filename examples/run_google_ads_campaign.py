# examples/run_google_ads_campaign.py
import os, sys
from pathlib import Path

# --- bootstrap PYTHONPATH (запускать из корня репо) ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# опционально подхватить .env
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

from ads_ai.config.settings import Settings  # ваша реализация
from ads_ai.flows.google_ads_campaign import create_google_ads_campaign, CampaignInput


def _safe_load_settings() -> Settings:
    """
    Универсальная загрузка Settings:
    - Пробуем from_env / load / from_yaml / from_file — что найдётся,
    - Иначе — конструктор с дефолтами.
    Ничего в проекте не ломаем.
    """
    # 1) Популярные фабрики
    for meth in ("from_env", "load", "from_yaml", "from_file"):
        if hasattr(Settings, meth):
            fn = getattr(Settings, meth)
            # from_yaml / from_file могут ожидать путь к конфигу
            if meth in ("from_yaml", "from_file"):
                cfg = os.getenv("ADS_AI_CONFIG") or os.getenv("CONFIG") or ""
                if not cfg:
                    # попробуем стандартные места
                    for candidate in ("settings.yaml", "settings.yml", "config.yaml", "config.yml"):
                        p = Path(candidate)
                        if p.exists():
                            cfg = str(p)
                            break
                if cfg:
                    return fn(cfg)  # type: ignore[misc]
                # если файла нет — пропускаем эту фабрику
                continue
            # from_env / load
            return fn()  # type: ignore[misc]

    # 2) Прямой конструктор (если у класса есть дефолты)
    try:
        return Settings()  # type: ignore[call-arg]
    except TypeError:
        # 3) Последняя попытка: минимальный «заглушечный» конструктор,
        # если Settings — dataclass c обязательными полями. Менять контракт мы НЕ будем —
        # просто подскажем, что нужен корректный фабричный метод.
        raise RuntimeError(
            "Не найден подходящий фабричный метод Settings (from_env/load/from_yaml/from_file), "
            "и конструктор без аргументов недоступен. "
            "Добавь один из фабричных методов в ads_ai/config/settings.py или укажи ADS_AI_CONFIG=path/to/settings.yaml."
        )


def main() -> None:
    settings = _safe_load_settings()

    payload = CampaignInput(
        budget=os.getenv("GADS_BUDGET", "20.00"),
        site=os.getenv("GADS_SITE", "https://easy-byte.ru/"),
        usp=os.getenv("GADS_USP", "Разработка нейросетей для бизнеса"),
        geo=os.getenv("GADS_GEO", "Russia, Moscow"),
        currency=os.getenv("GADS_CURRENCY", "RUB"),
        profile_id=os.getenv("ADSP_PROFILE_ID", "k146rs7c")  # можно задать через .env
    )

    result = create_google_ads_campaign(settings, payload)
    print(result)


if __name__ == "__main__":
    main()
