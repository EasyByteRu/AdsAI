# gem_http_test.py
# pip install requests
import os
import json
import requests

MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")  # можно поменять на свою модель

BASE_URL_TEMPLATE = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
HEADERS = {"Content-Type": "application/json; charset=utf-8"}

class QuotaDisabled(Exception): pass
class MissingApiKey(Exception): pass

def _get_api_key() -> str:
    key = (os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or "").strip()
    if not key:
        raise MissingApiKey("Укажи переменную окружения GEMINI_API_KEY (или GOOGLE_API_KEY).")
    return key

def _post(payload: dict) -> dict:
    api_key = _get_api_key()
    url = BASE_URL_TEMPLATE.format(model=MODEL, api_key=api_key)
    r = requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=60)
    if r.status_code == 429:
        # различаем «rate limit» и «limit: 0», если прилетит текст ошибки
        msg = r.text
        if "limit: 0" in msg:
            raise QuotaDisabled("Gemini quota disabled (limit:0). Проверь биллинг/квоты.")
        raise QuotaDisabled("Gemini rate limit (429). Притормози запросы.")
    r.raise_for_status()
    return r.json()

def ask_text(prompt: str, max_tokens: int = 512) -> str:
    payload = {
        "contents": [
            {"role": "user", "parts": [{"text": prompt}]}
        ],
        "generationConfig": {
            "maxOutputTokens": max_tokens
        }
    }
    data = _post(payload)
    # Ответ обычно в candidates[0].content.parts[0].text
    try:
        return data["candidates"][0]["content"]["parts"][0].get("text", "").strip()
    except Exception:
        return json.dumps(data, ensure_ascii=False)  # на крайний случай

def ask_json(prompt: str, max_tokens: int = 256) -> dict:
    """
    Просим строго JSON без схемы (универсально).
    Если модель добавит мусор — пытаемся распарсить первое JSON-подобное.
    """
    payload = {
        "contents": [
            {"role": "user", "parts": [{
                "text": (
                    "Верни ТОЛЬКО валидный JSON без комментариев и пояснений.\n"
                    "Никакого текста до или после JSON.\n\n"
                    + prompt
                )
            }]}
        ],
        "generationConfig": {
            "maxOutputTokens": max_tokens,
            "responseMimeType": "application/json"
        }
    }
    data = _post(payload)
    txt = ""
    try:
        txt = data["candidates"][0]["content"]["parts"][0].get("text", "").strip()
        return json.loads(txt)
    except Exception:
        # грубый парсер: вытащить первый JSON-блок из текста
        import re
        m = re.search(r"\{.*\}|\[.*\]", txt, flags=re.S)
        if m:
            return json.loads(m.group(0))
        raise

if __name__ == "__main__":
    try:
        print("=== TEXT ===")
        print(ask_text("Ответь ровно словом: Готово."))

        print("\n=== JSON ===")
        result = ask_json('Сформируй {"items":["слоган1","слоган2","слоган3"]} для бренда EasyByte. Коротко, ≤30 символов.')
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except MissingApiKey as e:
        print("LLM OFF:", e)
    except QuotaDisabled as q:
        print("LLM OFF:", q)
    except requests.HTTPError as e:
        print("HTTP ERROR:", e, "\nBody:", getattr(e.response, "text", ""))
    except Exception as e:
        print("ERROR:", e)
