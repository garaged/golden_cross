from pathlib import Path

import pandas as pd

from golden_cross.data.cache import OHLCVCache
from golden_cross.data.providers.base import DataRequest
from golden_cross.data.service import MarketDataService, RefreshPolicy


class StubProvider:
    name = "stub"

    def __init__(self, frames: list[pd.DataFrame]) -> None:
        self.frames = frames
        self.calls: list[DataRequest] = []

    def fetch(self, request: DataRequest) -> pd.DataFrame:
        self.calls.append(request)
        if not self.frames:
            raise AssertionError("No stub frame available for fetch")
        return self.frames.pop(0).copy()



def test_cache_round_trip(tmp_path: Path) -> None:
    cache = OHLCVCache(root=tmp_path, storage_format="csv")
    df = pd.DataFrame(
        {
            "Date": pd.date_range("2024-01-01", periods=3, freq="D"),
            "Open": [1.0, 2.0, 3.0],
            "High": [1.0, 2.0, 3.0],
            "Low": [1.0, 2.0, 3.0],
            "Close": [1.0, 2.0, 3.0],
            "Volume": [10, 20, 30],
        }
    )

    meta = cache.save("yahoo", "BTC-USD", "1d", df)
    loaded = cache.load("yahoo", "BTC-USD", "1d")

    assert meta is not None
    assert loaded is not None
    assert len(loaded) == 3
    assert loaded["Close"].tolist() == [1.0, 2.0, 3.0]
    assert meta.full_history is False



def test_unbounded_request_self_heals_partial_cache(tmp_path: Path) -> None:
    cache = OHLCVCache(root=tmp_path, storage_format="csv")
    partial = pd.DataFrame(
        {
            "Date": pd.date_range("2024-03-01", periods=3, freq="D"),
            "Open": [3.0, 4.0, 5.0],
            "High": [3.0, 4.0, 5.0],
            "Low": [3.0, 4.0, 5.0],
            "Close": [3.0, 4.0, 5.0],
            "Volume": [30, 40, 50],
        }
    )
    cache.save("stub", "BTC-USD", "1d", partial, full_history=False)

    full_dates = pd.to_datetime([
        "2024-01-01",
        "2024-01-02",
        "2024-01-03",
        "2024-03-01",
        "2024-03-02",
        "2024-03-03",
    ])
    full = pd.DataFrame(
        {
            "Date": full_dates,
            "Open": [1.0, 2.0, 3.0, 3.0, 4.0, 5.0],
            "High": [1.0, 2.0, 3.0, 3.0, 4.0, 5.0],
            "Low": [1.0, 2.0, 3.0, 3.0, 4.0, 5.0],
            "Close": [1.0, 2.0, 3.0, 3.0, 4.0, 5.0],
            "Volume": [10, 20, 30, 30, 40, 50],
        }
    )
    provider = StubProvider([full])
    service = MarketDataService(provider=provider, cache=cache, refresh_policy=RefreshPolicy())

    out = service.get_ohlcv("BTC-USD", "1d")
    meta = cache.get_metadata("stub", "BTC-USD", "1d")

    assert len(provider.calls) == 1
    assert provider.calls[0].period == "max"
    assert provider.calls[0].start is None
    assert len(out) == 6
    assert out["Date"].min() == pd.Timestamp("2024-01-01")
    assert meta is not None
    assert meta.full_history is True
