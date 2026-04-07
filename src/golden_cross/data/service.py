from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from golden_cross.data.cache import OHLCVCache, merge_ohlcv_frames
from golden_cross.data.providers.base import DataRequest, OHLCVProvider
from golden_cross.symbols import normalize_interval, normalize_symbol


@dataclass(frozen=True)
class RefreshPolicy:
    daily_tail_days: int = 5
    weekly_tail_days: int = 21
    monthly_tail_days: int = 62
    intraday_tail_days: int = 7

    def tail_days(self, interval: str) -> int:
        key = interval.lower()
        if key.endswith("mo"):
            return self.monthly_tail_days
        if key.endswith("wk"):
            return self.weekly_tail_days
        if key.endswith("d"):
            return self.daily_tail_days
        return self.intraday_tail_days


class MarketDataService:
    def __init__(
        self,
        provider: OHLCVProvider,
        cache: OHLCVCache,
        refresh_policy: RefreshPolicy | None = None,
    ) -> None:
        self.provider = provider
        self.cache = cache
        self.refresh_policy = refresh_policy or RefreshPolicy()

    def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        start: str | None = None,
        end: str | None = None,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        normalized_symbol = normalize_symbol(symbol, provider=self.provider.name)
        normalized_interval = normalize_interval(interval, provider=self.provider.name)

        metadata = None if force_refresh else self.cache.get_metadata(self.provider.name, normalized_symbol, normalized_interval)
        cached = None if force_refresh else self.cache.load(self.provider.name, normalized_symbol, normalized_interval)
        start_ts = pd.to_datetime(start) if start is not None else None
        end_ts = pd.to_datetime(end) if end is not None else None
        wants_unbounded_history = start is None and end is None

        if cached is None or cached.empty:
            request = DataRequest(
                symbol=normalized_symbol,
                interval=normalized_interval,
                start=start,
                end=end,
                period="max" if wants_unbounded_history else None,
            )
            fetched = self.provider.fetch(request)
            self.cache.save(
                self.provider.name,
                normalized_symbol,
                normalized_interval,
                fetched,
                full_history=wants_unbounded_history,
            )
            return self._slice(fetched, start_ts, end_ts)

        # Self-heal older partial caches: if the caller wants the full range and the
        # metadata does not confirm a full-history snapshot, fetch max once and mark it.
        if wants_unbounded_history and not (metadata.full_history if metadata is not None else False):
            full = self.provider.fetch(
                DataRequest(
                    symbol=normalized_symbol,
                    interval=normalized_interval,
                    period="max",
                )
            )
            cached = merge_ohlcv_frames(cached, full)
            self.cache.save(
                self.provider.name,
                normalized_symbol,
                normalized_interval,
                cached,
                full_history=True,
            )
            return self._slice(cached, start_ts, end_ts)

        cached_min = pd.to_datetime(cached["Date"].min())
        cached_max = pd.to_datetime(cached["Date"].max())

        if start_ts is not None and start_ts < cached_min:
            left = self.provider.fetch(
                DataRequest(
                    symbol=normalized_symbol,
                    interval=normalized_interval,
                    start=start_ts.strftime("%Y-%m-%d"),
                    end=cached_min.strftime("%Y-%m-%d"),
                )
            )
            cached = merge_ohlcv_frames(cached, left)
            cached_min = pd.to_datetime(cached["Date"].min())
            cached_max = pd.to_datetime(cached["Date"].max())

        tail_days = self.refresh_policy.tail_days(normalized_interval)
        refresh_start_ts = cached_max - pd.Timedelta(days=tail_days)
        should_refresh_right = end_ts is None or end_ts >= refresh_start_ts

        if should_refresh_right:
            right = self.provider.fetch(
                DataRequest(
                    symbol=normalized_symbol,
                    interval=normalized_interval,
                    start=refresh_start_ts.strftime("%Y-%m-%d"),
                    end=end_ts.strftime("%Y-%m-%d") if end_ts is not None else None,
                )
            )
            cached = merge_ohlcv_frames(cached, right)

        self.cache.save(
            self.provider.name,
            normalized_symbol,
            normalized_interval,
            cached,
            full_history=bool(metadata.full_history if metadata is not None else False),
        )
        return self._slice(cached, start_ts, end_ts)

    @staticmethod
    def _slice(df: pd.DataFrame, start: pd.Timestamp | None, end: pd.Timestamp | None) -> pd.DataFrame:
        out = df.copy()
        out["Date"] = pd.to_datetime(out["Date"])
        if start is not None:
            out = out[out["Date"] >= start]
        if end is not None:
            out = out[out["Date"] <= end]
        return out.sort_values("Date").reset_index(drop=True)
