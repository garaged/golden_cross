from __future__ import annotations

import pandas as pd

from .base import DataRequest


class YahooFinanceProvider:
    name = "yahoo"

    def fetch(self, request: DataRequest) -> pd.DataFrame:
        kwargs: dict[str, object] = {
            "tickers": request.symbol,
            "interval": request.interval,
            "auto_adjust": False,
            "progress": False,
            "threads": False,
        }

        if request.period is not None and request.start is None and request.end is None:
            kwargs["period"] = request.period
        else:
            if request.start is not None:
                kwargs["start"] = request.start
            if request.end is not None:
                kwargs["end"] = request.end

        import yfinance as yf

        df = yf.download(**kwargs)
        if df.empty:
            raise ValueError(
                "No data returned from Yahoo Finance for "
                f"symbol={request.symbol!r}, interval={request.interval!r}, start={request.start!r}, end={request.end!r}, period={request.period!r}"
            )

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df = df.reset_index()
        if "Datetime" in df.columns and "Date" not in df.columns:
            df = df.rename(columns={"Datetime": "Date"})

        required = ["Date", "Open", "High", "Low", "Close", "Volume"]
        missing = [col for col in required if col not in df.columns]
        if missing:
            raise ValueError(f"Missing expected OHLCV columns: {missing}. Found: {list(df.columns)}")

        out = df[required].copy()
        out["Date"] = pd.to_datetime(out["Date"])
        if hasattr(out["Date"].dt, "tz") and out["Date"].dt.tz is not None:
            out["Date"] = out["Date"].dt.tz_localize(None)

        for col in ["Open", "High", "Low", "Close", "Volume"]:
            out[col] = pd.to_numeric(out[col], errors="coerce")

        return out.sort_values("Date").drop_duplicates(subset=["Date"], keep="last").reset_index(drop=True)
