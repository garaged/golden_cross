from __future__ import annotations

from datetime import datetime
from typing import Iterable, Optional

import pandas as pd

from golden_cross.models import MAConfig, MAReport


def normalize_ohlc(df: pd.DataFrame, date_col: str, close_col: str) -> pd.DataFrame:
    if date_col not in df.columns:
        raise ValueError(f"Missing {date_col!r} column. Found: {list(df.columns)}")
    if close_col not in df.columns:
        raise ValueError(f"Missing {close_col!r} column. Found: {list(df.columns)}")

    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col])
    out = out.sort_values(date_col).reset_index(drop=True)
    out = out.dropna(subset=[close_col]).reset_index(drop=True)

    for col in ["Open", "High", "Low", close_col, "Volume"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def add_sma_columns(df: pd.DataFrame, periods: Iterable[int], close_col: str) -> pd.DataFrame:
    out = df.copy()
    for period in sorted({int(value) for value in periods}):
        out[f"SMA{period}"] = out[close_col].rolling(period).mean()
    return out


def detect_cross_events(
    df: pd.DataFrame,
    date_col: str,
    close_col: str,
    above: pd.Series,
    up_type: str,
    down_type: str,
    extra_cols: Optional[list[str]] = None,
) -> pd.DataFrame:
    extra_cols = extra_cols or []
    above_clean = above.dropna()
    if above_clean.empty:
        return pd.DataFrame(columns=["Date", "Close", "type", *extra_cols])

    cross = above_clean.astype("int8").diff()
    up_idx = cross[cross == 1].index
    dn_idx = cross[cross == -1].index

    cols = [date_col, close_col, *extra_cols]
    up = df.loc[up_idx, cols].copy()
    up["type"] = up_type

    dn = df.loc[dn_idx, cols].copy()
    dn["type"] = down_type

    events = pd.concat([up, dn], ignore_index=True)
    events = events.sort_values(date_col).reset_index(drop=True)
    return events.rename(columns={date_col: "Date", close_col: "Close"})

def compute_ma_report(
    df: pd.DataFrame,
    symbol: str,
    interval: str,
    cfg: MAConfig,
) -> MAReport:
    df0 = normalize_ohlc(df, cfg.date_col, cfg.close_col)

    needed_mas = {cfg.short, cfg.long, *cfg.price_cross_mas}
    df1 = add_sma_columns(df0, needed_mas, cfg.close_col)

    short_col = f"SMA{cfg.short}"
    long_col = f"SMA{cfg.long}"
    event_context_cols = [f"SMA{period}" for period in sorted(needed_mas)]

    ma_events = detect_cross_events(
        df=df1,
        date_col=cfg.date_col,
        close_col=cfg.close_col,
        above=df1[short_col] > df1[long_col],
        up_type="GOLDEN_CROSS",
        down_type="DEATH_CROSS",
        extra_cols=event_context_cols,
    )

    price_events: list[pd.DataFrame] = []
    for period in cfg.price_cross_mas:
        sma_col = f"SMA{period}"
        event_df = detect_cross_events(
            df=df1,
            date_col=cfg.date_col,
            close_col=cfg.close_col,
            above=df1[cfg.close_col] > df1[sma_col],
            up_type=f"PRICE_CROSS_UP_SMA{period}",
            down_type=f"PRICE_CROSS_DN_SMA{period}",
            extra_cols=event_context_cols,
        )
        price_events.append(event_df)

    if price_events:
        events = pd.concat([ma_events, *price_events], ignore_index=True)
        events = events.sort_values("Date").reset_index(drop=True)
    else:
        events = ma_events

    valid = df1.dropna(subset=[short_col, long_col])
    if valid.empty:
        latest = {
            "date": None,
            "close": None,
            "sma_short": None,
            "sma_long": None,
            "regime": "INSUFFICIENT_DATA",
            "days_since_last_event": None,
        }
    else:
        last = valid.iloc[-1]
        last_date = pd.to_datetime(last[cfg.date_col])
        regime = "BULLISH" if float(last[short_col]) > float(last[long_col]) else "BEARISH"
        latest = {
            "date": last_date.date(),
            "close": float(last[cfg.close_col]),
            "sma_short": float(last[short_col]),
            "sma_long": float(last[long_col]),
            "regime": regime,
            "days_since_last_event": None,
        }
        if not events.empty:
            latest["days_since_last_event"] = (datetime.now() - pd.to_datetime(events["Date"].iloc[-1])).days

    return MAReport(symbol=symbol, interval=interval, cfg=cfg, df=df1, events=events, latest=latest)
