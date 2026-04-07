from __future__ import annotations


def normalize_symbol(symbol: str, provider: str = "yahoo") -> str:
    raw = symbol.strip()
    lowered = raw.lower()

    if provider != "yahoo":
        return raw

    aliases = {
        "btcusd": "BTC-USD",
        "ethusd": "ETH-USD",
        "solusd": "SOL-USD",
        "^spx": "^GSPC",
        "spx": "^GSPC",
    }
    if lowered in aliases:
        return aliases[lowered]

    if lowered.endswith(".us"):
        base = raw[:-3]
        return base.upper().replace(".", "-")

    if lowered.endswith("usd") and "-" not in raw and len(raw) >= 6:
        asset = raw[:-3].upper()
        return f"{asset}-USD"

    return raw.upper().replace(".", "-")


_INTERVAL_ALIASES = {
    "d": "1d",
    "1d": "1d",
    "day": "1d",
    "daily": "1d",
    "wk": "1wk",
    "w": "1wk",
    "1wk": "1wk",
    "mo": "1mo",
    "m": "1mo",
    "1mo": "1mo",
    "1h": "1h",
    "60m": "60m",
    "30m": "30m",
    "15m": "15m",
    "5m": "5m",
    "1m": "1m",
}


def normalize_interval(interval: str, provider: str = "yahoo") -> str:
    if provider != "yahoo":
        return interval
    return _INTERVAL_ALIASES.get(interval.lower(), interval)
