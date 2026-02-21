from datetime import date, datetime

def build_filter(predicates: list[str], extra: str) -> dict:
    """
    filter=field=ge="2025-01-01T00:00:00Z";field=le="2025-10-31T00:00:00Z"
    Extra RSQL predicates from the user are appended with ;
    """
    all_parts = list(predicates)
    raw = extra.strip()
    if raw:
        all_parts.extend(p.strip() for p in raw.split(";") if p.strip())
    if not all_parts:
        return {}
    return {"filter": ";".join(all_parts)}


def date_to_filter_value(d: date) -> str:
    return f'"{datetime(d.year, d.month, d.day).strftime("%Y-%m-%dT%H:%M:%SZ")}"'
