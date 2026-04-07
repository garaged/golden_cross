from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from golden_cross.analysis import compute_ma_report
from golden_cross.data.cache import OHLCVCache
from golden_cross.data.providers.yahoo import YahooFinanceProvider
from golden_cross.data.service import MarketDataService, RefreshPolicy
from golden_cross.models import MAConfig, MAReport
from golden_cross.plotting import plot_ma_report
from golden_cross.symbols import normalize_interval, normalize_symbol


def format_report_summary(report: MAReport) -> str:
    latest = report.latest
    if latest["date"] is None:
        lines = [f"Latest: insufficient data to compute SMA{report.cfg.short}/SMA{report.cfg.long}"]
    else:
        lines = [
            f"Latest: {latest['date']} close={latest['close']:.2f} "
            f"SMA{report.cfg.short}={latest['sma_short']:.2f} "
            f"SMA{report.cfg.long}={latest['sma_long']:.2f} => {latest['regime']}"
        ]

    if latest.get("days_since_last_event") is not None:
        lines.append(f"{latest['days_since_last_event']} days since last event")
    elif report.events.empty:
        lines.append("No crossover events detected in the available data")

    return "\n".join(lines)


def ma_cross_report(
    symbol: str = "btcusd",
    interval: str = "1d",
    cfg: MAConfig = MAConfig(),
    df: Optional[pd.DataFrame] = None,
    plot: bool = True,
    print_events: int = 10,
    start: str | None = None,
    end: str | None = None,
    cache_dir: str | Path | None = None,
    force_refresh: bool = False,
) -> MAReport:
    provider = YahooFinanceProvider()
    cache = OHLCVCache(root=cache_dir)
    data_service = MarketDataService(provider=provider, cache=cache, refresh_policy=RefreshPolicy())

    normalized_symbol = normalize_symbol(symbol, provider=provider.name)
    normalized_interval = normalize_interval(interval, provider=provider.name)

    if df is None:
        df = data_service.get_ohlcv(
            symbol=symbol,
            interval=interval,
            start=start,
            end=end,
            force_refresh=force_refresh,
        )

    report = compute_ma_report(
        df=df,
        symbol=normalized_symbol,
        interval=normalized_interval,
        cfg=cfg,
    )

    print(format_report_summary(report))

    if not report.events.empty and print_events > 0:
        print(f"\nLast {min(print_events, len(report.events))} events:")
        print(report.events.tail(print_events).to_string(index=False))

    if plot:
        plot_ma_report(report)

    return report
