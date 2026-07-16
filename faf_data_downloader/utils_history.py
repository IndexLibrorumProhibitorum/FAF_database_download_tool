import json
from datetime import datetime, timedelta

from config import SETTINGS_FILE, HISTORY_FILE, RATING_HISTORY_FILE


def load_settings() -> dict:
    try:
        if SETTINGS_FILE.exists():
            return json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def save_settings(data: dict) -> None:
    try:
        SETTINGS_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception:
        pass


def load_history() -> list:
    try:
        if HISTORY_FILE.exists():
            return json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return []


def append_history(entry: dict) -> None:
    try:
        history = load_history()
        history.insert(0, entry)
        history = history[:50]   # keep last 50 entries
        HISTORY_FILE.write_text(json.dumps(history, indent=2), encoding="utf-8")
    except Exception:
        pass


def load_rating_history() -> list:
    try:
        if RATING_HISTORY_FILE.exists():
            return json.loads(RATING_HISTORY_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return []


def save_rating_history(history: list) -> None:
    try:
        RATING_HISTORY_FILE.write_text(json.dumps(history, indent=2), encoding="utf-8")
    except Exception:
        pass


def append_rating_history(entry: dict) -> None:
    try:
        history = load_rating_history()
        history.insert(0, entry)
        save_rating_history(history[:100])
    except Exception:
        pass


def clear_rating_history() -> None:
    save_rating_history([])


def prune_rating_history(max_age_days: int) -> None:
    if max_age_days <= 0:
        return
    cutoff = datetime.now() - timedelta(days=max_age_days)
    kept = []
    for entry in load_rating_history():
        try:
            entry_time = datetime.strptime(entry.get("time", ""), "%Y-%m-%d %H:%M")
        except (TypeError, ValueError):
            kept.append(entry)
            continue
        if entry_time >= cutoff:
            kept.append(entry)
    save_rating_history(kept)


def format_duration(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    else:
        h = seconds // 3600
        m = (seconds % 3600) // 60
        return f"{h}h {m}m"
