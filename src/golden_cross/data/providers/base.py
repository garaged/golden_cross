from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import pandas as pd


@dataclass(frozen=True)
class DataRequest:
    symbol: str
    interval: str
    start: str | None = None
    end: str | None = None
    period: str | None = None


class OHLCVProvider(Protocol):
    name: str

    def fetch(self, request: DataRequest) -> pd.DataFrame:
        ...
