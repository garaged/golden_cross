from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd


@dataclass(frozen=True)
class MAConfig:
    short: int = 50
    long: int = 200
    price_cross_mas: tuple[int, ...] = ()
    plot_days: Optional[int] = 3650
    figsize: tuple[int, int] = (12, 6)
    title: Optional[str] = None
    date_col: str = "Date"
    close_col: str = "Close"


@dataclass(frozen=True)
class MAReport:
    symbol: str
    interval: str
    cfg: MAConfig
    df: pd.DataFrame
    events: pd.DataFrame
    latest: dict


@dataclass(frozen=True)
class CacheMetadata:
    provider: str
    symbol: str
    interval: str
    min_date: Optional[str]
    max_date: Optional[str]
    row_count: int
    stored_at: str
    storage_format: str
    data_path: Path
    full_history: bool = False
