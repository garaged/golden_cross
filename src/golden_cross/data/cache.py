from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from golden_cross.models import CacheMetadata


class OHLCVCache:
    def __init__(self, root: str | Path | None = None, storage_format: str = "auto") -> None:
        if root is None:
            root = Path.home() / ".cache" / "golden_cross"
        self.root = Path(root)
        self.storage_format = storage_format

    def _safe_part(self, value: str) -> str:
        return re.sub(r"[^A-Za-z0-9._-]+", "_", value)

    def _symbol_dir(self, provider: str, symbol: str) -> Path:
        return self.root / "bars" / self._safe_part(provider) / self._safe_part(symbol)

    def _selected_storage_format(self) -> str:
        if self.storage_format != "auto":
            return self.storage_format
        try:
            import pyarrow  # noqa: F401

            return "parquet"
        except Exception:
            return "csv"

    def _data_path(self, provider: str, symbol: str, interval: str, storage_format: str | None = None) -> Path:
        fmt = storage_format or self._selected_storage_format()
        ext = "parquet" if fmt == "parquet" else "csv"
        return self._symbol_dir(provider, symbol) / f"{self._safe_part(interval)}.{ext}"

    def _meta_path(self, provider: str, symbol: str, interval: str) -> Path:
        return self._symbol_dir(provider, symbol) / f"{self._safe_part(interval)}.meta.json"

    def get_metadata(self, provider: str, symbol: str, interval: str) -> CacheMetadata | None:
        meta_path = self._meta_path(provider, symbol, interval)
        if not meta_path.exists():
            return None

        payload = json.loads(meta_path.read_text())
        return CacheMetadata(
            provider=payload["provider"],
            symbol=payload["symbol"],
            interval=payload["interval"],
            min_date=payload.get("min_date"),
            max_date=payload.get("max_date"),
            row_count=int(payload.get("row_count", 0)),
            stored_at=payload["stored_at"],
            storage_format=payload["storage_format"],
            data_path=Path(payload["data_path"]),
            full_history=bool(payload.get("full_history", False)),
        )

    def load(self, provider: str, symbol: str, interval: str) -> pd.DataFrame | None:
        metadata = self.get_metadata(provider, symbol, interval)
        if metadata is None:
            return None

        path = metadata.data_path
        if not path.exists():
            return None

        if metadata.storage_format == "parquet":
            df = pd.read_parquet(path)
        else:
            df = pd.read_csv(path)

        df["Date"] = pd.to_datetime(df["Date"])
        return df.sort_values("Date").drop_duplicates(subset=["Date"], keep="last").reset_index(drop=True)

    def save(
        self,
        provider: str,
        symbol: str,
        interval: str,
        df: pd.DataFrame,
        *,
        full_history: bool = False,
    ) -> CacheMetadata:
        if df.empty:
            raise ValueError("Refusing to cache an empty dataframe")

        previous = self.get_metadata(provider, symbol, interval)
        storage_format = self._selected_storage_format()
        data_path = self._data_path(provider, symbol, interval, storage_format=storage_format)
        meta_path = self._meta_path(provider, symbol, interval)
        data_path.parent.mkdir(parents=True, exist_ok=True)

        out = df.copy()
        out["Date"] = pd.to_datetime(out["Date"])
        out = out.sort_values("Date").drop_duplicates(subset=["Date"], keep="last").reset_index(drop=True)

        if storage_format == "parquet":
            out.to_parquet(data_path, index=False)
        else:
            out.to_csv(data_path, index=False)

        stored_at = datetime.now(timezone.utc).isoformat()
        payload = {
            "provider": provider,
            "symbol": symbol,
            "interval": interval,
            "min_date": out["Date"].min().isoformat() if not out.empty else None,
            "max_date": out["Date"].max().isoformat() if not out.empty else None,
            "row_count": int(len(out)),
            "stored_at": stored_at,
            "storage_format": storage_format,
            "data_path": str(data_path),
            "full_history": bool(full_history or (previous.full_history if previous is not None else False)),
        }
        meta_path.write_text(json.dumps(payload, indent=2))
        return self.get_metadata(provider, symbol, interval)  # type: ignore[return-value]


def merge_ohlcv_frames(old: pd.DataFrame | None, new: pd.DataFrame | None) -> pd.DataFrame:
    if old is None or old.empty:
        return new.copy() if new is not None else pd.DataFrame()
    if new is None or new.empty:
        return old.copy()

    merged = pd.concat([old, new], ignore_index=True)
    merged["Date"] = pd.to_datetime(merged["Date"])
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors="coerce")
    return merged.sort_values("Date").drop_duplicates(subset=["Date"], keep="last").reset_index(drop=True)
